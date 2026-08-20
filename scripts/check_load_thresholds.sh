#!/usr/bin/env bash
# Loopback load gate: pricing-api with admission on vs off, under the
# production profile (fat LTO, codegen-units 1 — the deployed configuration).
# Absolute numbers gate on a controlled host; the overhead ratio is
# meaningful anywhere.
set -euo pipefail

cd "$(dirname "$0")/.."

exec cargo run --locked --profile production -p pricing-api --bin load_gate -- \
    testing/load_thresholds.json reports/load_gate_report.json
