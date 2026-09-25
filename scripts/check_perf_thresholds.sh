#!/usr/bin/env bash
# Runs the hot-path microbenchmarks and gates them against
# testing/perf_thresholds.json.
#
# Absolute-latency and recorded-baseline gating belong on a controlled host.
# Run locally and retain the report with the merge request. `--ratios-only`
# is a diagnostic mode for other hosts: every fresh measurement must exist,
# while absolute thresholds and recorded-baseline comparisons are skipped.
# Every complete, readable full run with recording provenance deposits its
# measurements under target/perf-samples. Other ordinary runs keep their gate
# verdict and report why no sample was retained; ratios-only never deposits.
# `--record` rewrites testing/perf_baseline.json whole from the median of the
# distinct runs at the current revision in the same environment, and refuses
# with fewer than three; it is the only supported way to recalibrate (#114).
# Mirrors ferro-risk's gate: stale Criterion output is wiped first and a
# freshness marker rejects anything the current run did not produce.
set -euo pipefail

cd "$(dirname "$0")/.."

CRITERION_ROOT="target/criterion"
MARKER="target/.perf-gate-fresh-run"
REPORT="reports/perf_gate_report.json"
BASELINE="testing/perf_baseline.json"
# A build artifact, like the criterion output it summarises: samples describe
# this host and are never checked in.
SAMPLES="target/perf-samples"
# A bounded window of past readable runs, kept across calibration series, from
# which --record derives each row's allowance (#141). --fresh-samples clears
# the samples of the series being recorded, never this.
HISTORY="target/perf-history"
gate_args=(--baseline "$BASELINE" --samples "$SAMPLES" --run-history "$HISTORY")
if [ "${1:-}" = "--ratios-only" ]; then
    REPORT="reports/perf_ratio_gate_report.json"
    gate_args=(--ratios-only "${gate_args[@]}")
    shift
fi
if [ "${1:-}" = "--record" ]; then
    gate_args+=(--record "$BASELINE")
    shift
fi
if [ "${1:-}" = "--fresh-samples" ]; then
    # Start a new calibration series, e.g. after changing the code being
    # measured. Samples from another revision or environment are ignored anyway;
    # this also keeps the directory from growing.
    rm -rf "$SAMPLES"
    shift
fi
if [ "$#" -ne 0 ]; then
    printf 'usage: %s [--ratios-only] [--record] [--fresh-samples]\n' "$0" >&2
    exit 2
fi

# Wipe only the groups this gate owns, so unrelated criterion output (if any)
# cannot satisfy the checker.
rm -rf "$CRITERION_ROOT/cost_table" "$CRITERION_ROOT/snapshot" "$CRITERION_ROOT/lease" \
       "$CRITERION_ROOT/reservation" "$CRITERION_ROOT/admission" "$CRITERION_ROOT/capacity" \
       "$CRITERION_ROOT/credential" "$CRITERION_ROOT/managed_credential" \
       "$CRITERION_ROOT/credential_digest"
mkdir -p "$(dirname "$MARKER")"
# The marker's mtime is also the stable benchmark-run identity. Checker retries
# must reuse it; only starting a new benchmark suite advances it.
touch "$MARKER"

cargo bench --locked -p tollgate-core --bench core_hot_path
cargo bench --locked -p tollgate-admission --bench admission_hot_path
# Every id in the manifest must be produced by a benchmark this script runs, or
# `evaluate` reports it missing-or-stale and the gate fails whatever the code
# does. Adding a manifest entry without a run here is how that happens (#2).
cargo bench --locked -p tollgate-auth --bench credential_verification
cargo bench --locked -p tollgate-client --bench managed_credentials
cargo bench --locked -p tollgate-client --bench usage_queue

# Read the environment here rather than in the gate binary: there is no
# portable way to ask std for a load average or a CPU model, and capturing the
# environment is the wrapper's job while judging it is the checker's. Recorded
# straight after the benchmarks, so it reflects the machine that produced these
# numbers.
load_average() {
    if [ -r /proc/loadavg ]; then
        cut -d' ' -f1-3 /proc/loadavg
    elif command -v sysctl > /dev/null 2>&1; then
        sysctl -n vm.loadavg 2> /dev/null | tr -d '{}' | awk '{ print $1, $2, $3 }'
    fi
}

# Swap pressure and free memory. A host that is paging produces wide
# confidence intervals while its load average and CPU idle still look healthy,
# which is how #114 mistook a swapping machine for a threshold problem.
memory_state() {
    if [ -r /proc/meminfo ]; then
        awk '/^(MemAvailable|SwapFree|SwapTotal):/ { printf "%s %s kB; ", $1, $2 }' /proc/meminfo
    elif command -v sysctl > /dev/null 2>&1; then
        printf 'swap %s; free pages %s' \
            "$(sysctl -n vm.swapusage 2> /dev/null | tr -s ' ')" \
            "$(vm_stat 2> /dev/null | awk '/Pages free/ { gsub(/\./, "", $3); print $3 }')"
    fi
}

cpu_model() {
    if [ -r /proc/cpuinfo ]; then
        awk -F': ' '/^model name/ { print $2; exit }' /proc/cpuinfo
    elif command -v sysctl > /dev/null 2>&1; then
        sysctl -n machdep.cpu.brand_string 2> /dev/null
    fi
}

logical_cpus() {
    getconf _NPROCESSORS_ONLN 2> /dev/null || echo "?"
}

os_description() {
    if command -v sw_vers > /dev/null 2>&1; then
        printf '%s %s (%s)' "$(sw_vers -productName)" "$(sw_vers -productVersion)" "$(uname -sr)"
    else
        uname -sr
    fi
}

# A revision the gate can refuse. `--record` rejects anything that is not a
# committed 40-hex sha, so uncommitted work cannot be stamped onto a baseline
# as if it were a checkout-able state.
gate_revision() {
    local head
    head="$(git rev-parse HEAD 2> /dev/null || echo unknown)"
    if [ -n "$(git status --porcelain 2> /dev/null)" ]; then
        printf '%s-dirty' "$head"
    else
        printf '%s' "$head"
    fi
}

TOLLGATE_GATE_LOAD="$(load_average)" \
    TOLLGATE_GATE_MEMORY="$(memory_state)" \
    TOLLGATE_GATE_REVISION="$(gate_revision)" \
    TOLLGATE_GATE_TARGET="$(rustc -vV | awk '/^host: / { print $2 }')" \
    TOLLGATE_GATE_RUSTC="$(rustc -V)" \
    TOLLGATE_GATE_CPU="$(cpu_model) ($(logical_cpus) logical CPUs)" \
    TOLLGATE_GATE_OS="$(os_description)" \
    exec cargo run --locked -p tollgate-perf-gate --bin check_benchmark_thresholds -- \
    "${gate_args[@]}" testing/perf_thresholds.json "$CRITERION_ROOT" "$REPORT" "$MARKER"
