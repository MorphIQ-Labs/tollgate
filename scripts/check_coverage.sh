#!/usr/bin/env sh
# Measures which lines and regions the workspace tests, the PostgreSQL suite and
# the end-to-end scenarios execute (GH-91). It reports; it does not set a bar:
# a covered line is not a pinned one, which is what mutation testing measures.
#
# TOLLGATE_PG_URL must name a PostgreSQL database whose role may create
# databases. Reports are written under reports/coverage/.
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

if [ -z "${TOLLGATE_PG_URL:-}" ]; then
  echo "coverage: TOLLGATE_PG_URL is required, or the backend reads as uncovered" >&2
  exit 2
fi
required=$(cat .cargo/llvm-cov-version)
installed=$(cargo llvm-cov --version 2>/dev/null || true)
if [ "$installed" != "cargo-llvm-cov $required" ]; then
  echo "cargo-llvm-cov $required is required; install it with:" >&2
  echo "  cargo install --locked cargo-llvm-cov --version $required" >&2
  exit 127
fi

# The environment form, so the processes the end-to-end scenarios spawn are
# instrumented too. A process a scenario kills reports nothing.
environment=$(cargo llvm-cov show-env --sh)
eval "$environment"
cargo llvm-cov clean --workspace
TOLLGATE_REQUIRE_PG=1 cargo test --locked --workspace --all-features -- --test-threads=1
./scripts/with_tls_postgres.sh cargo test --locked -p tollgate-store-postgres --test tls
./scripts/check_e2e.sh

mkdir -p reports/coverage
cargo llvm-cov report --json --output-path reports/coverage/coverage.json
cargo llvm-cov report --show-missing-lines >reports/coverage/summary.txt
cargo llvm-cov report --html --output-dir reports/coverage
tail -n 1 reports/coverage/summary.txt
echo "coverage: reports/coverage/summary.txt lists each file's uncovered lines"
