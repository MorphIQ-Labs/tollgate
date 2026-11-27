#!/usr/bin/env sh
# Runs the end-to-end scenarios: a PostgreSQL-backed tollgate-server and
# pricing-api instances as separate processes (crates/tollgate-e2e).
#
# TOLLGATE_PG_URL must name a PostgreSQL database whose role may create
# databases. Each scenario creates its own and drops it afterwards.
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

if [ -z "${TOLLGATE_PG_URL:-}" ]; then
  echo "e2e: TOLLGATE_PG_URL is required (docker compose up -d starts one)" >&2
  exit 2
fi

# Built here, every time, so a scenario never runs against a stale binary.
cargo build --locked -p tollgate-server -p pricing-api --bins
target=$(cargo metadata --locked --no-deps --format-version 1 |
  sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')

TOLLGATE_REQUIRE_E2E=1 \
  TOLLGATE_E2E_SERVER_BIN="$target/debug/tollgate-server" \
  TOLLGATE_E2E_APP_BIN="$target/debug/pricing-api" \
  cargo test --locked -p tollgate-e2e "$@"
