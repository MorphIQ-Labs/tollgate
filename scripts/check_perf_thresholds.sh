#!/usr/bin/env bash
# Runs the hot-path microbenchmarks and gates them against
# testing/perf_thresholds.json.
#
# Absolute-latency gating belongs on a controlled host; on shared CI this
# script's role is limited to proving the benches and the checker still run.
# Mirrors ferro-risk's gate: stale Criterion output is wiped first and a
# freshness marker rejects anything the current run did not produce.
set -euo pipefail

cd "$(dirname "$0")/.."

CRITERION_ROOT="target/criterion"
MARKER="target/.perf-gate-fresh-run"
REPORT="reports/perf_gate_report.json"

# Wipe only the groups this gate owns, so unrelated criterion output (if any)
# cannot satisfy the checker.
rm -rf "$CRITERION_ROOT/cost_table" "$CRITERION_ROOT/snapshot" "$CRITERION_ROOT/lease" \
       "$CRITERION_ROOT/admission"
mkdir -p "$(dirname "$MARKER")"
touch "$MARKER"

cargo bench --locked -p tollgate-core --bench core_hot_path
cargo bench --locked -p tollgate-admission --bench admission_hot_path

exec cargo run --locked -p tollgate-perf-gate --bin check_benchmark_thresholds -- \
    testing/perf_thresholds.json "$CRITERION_ROOT" "$REPORT" "$MARKER"
