#!/usr/bin/env bash
# Runs the hot-path microbenchmarks and gates them against
# testing/perf_thresholds.json.
#
# Absolute-latency and recorded-baseline gating belong on a controlled host.
# `--ratios-only` is the required shared-CI mode: every fresh measurement must
# exist, while only same-run ratios decide the performance verdict.
# Mirrors ferro-risk's gate: stale Criterion output is wiped first and a
# freshness marker rejects anything the current run did not produce.
set -euo pipefail

cd "$(dirname "$0")/.."

CRITERION_ROOT="target/criterion"
MARKER="target/.perf-gate-fresh-run"
REPORT="reports/perf_gate_report.json"
gate_args=(--baseline testing/perf_baseline.json)
if [ "${1:-}" = "--ratios-only" ]; then
    REPORT="reports/perf_ratio_gate_report.json"
    gate_args=(--ratios-only "${gate_args[@]}")
    shift
fi
if [ "$#" -ne 0 ]; then
    printf 'usage: %s [--ratios-only]\n' "$0" >&2
    exit 2
fi

# Wipe only the groups this gate owns, so unrelated criterion output (if any)
# cannot satisfy the checker.
rm -rf "$CRITERION_ROOT/cost_table" "$CRITERION_ROOT/snapshot" "$CRITERION_ROOT/lease" \
       "$CRITERION_ROOT/reservation" "$CRITERION_ROOT/admission" "$CRITERION_ROOT/credential" \
       "$CRITERION_ROOT/credential_digest"
mkdir -p "$(dirname "$MARKER")"
touch "$MARKER"

cargo bench --locked -p tollgate-core --bench core_hot_path
cargo bench --locked -p tollgate-admission --bench admission_hot_path
# Every id in the manifest must be produced by a benchmark this script runs, or
# `evaluate` reports it missing-or-stale and the gate fails whatever the code
# does. Adding a manifest entry without a run here is how that happens (#2).
cargo bench --locked -p tollgate-auth --bench credential_verification

# Read the load average here rather than in the gate binary: there is no
# portable way to ask for it from std, and capturing the environment is the
# wrapper's job while judging it is the checker's. Recorded straight after the
# benchmarks, so it reflects the machine that produced these numbers.
load_average() {
    if [ -r /proc/loadavg ]; then
        cut -d' ' -f1-3 /proc/loadavg
    elif command -v sysctl > /dev/null 2>&1; then
        sysctl -n vm.loadavg 2> /dev/null | tr -d '{}' | awk '{ print $1, $2, $3 }'
    fi
}

TOLLGATE_GATE_LOAD="$(load_average)" \
    exec cargo run --locked -p tollgate-perf-gate --bin check_benchmark_thresholds -- \
    "${gate_args[@]}" testing/perf_thresholds.json "$CRITERION_ROOT" "$REPORT" "$MARKER"
