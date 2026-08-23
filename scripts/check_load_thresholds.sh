#!/usr/bin/env bash
# Loopback load gate: pricing-api with admission on vs off, under the
# production profile (fat LTO, codegen-units 1 — the deployed configuration).
# Absolute numbers gate on a controlled host; the sequential and configured
# same-account-concurrency overhead ratios are portable like-for-like.
set -euo pipefail

cd "$(dirname "$0")/.."

# Same environment capture as the perf gate: read here because std offers no
# portable load average, and recorded so a borderline ratio can be diagnosed
# rather than only re-run (#49).
load_average() {
    if [ -r /proc/loadavg ]; then
        cut -d' ' -f1-3 /proc/loadavg
    elif command -v sysctl > /dev/null 2>&1; then
        sysctl -n vm.loadavg 2> /dev/null | tr -d '{}' | awk '{ print $1, $2, $3 }'
    fi
}

TOLLGATE_GATE_LOAD="$(load_average)" \
    exec cargo run --locked --profile production -p pricing-api --bin load_gate -- \
    testing/load_thresholds.json reports/load_gate_report.json
