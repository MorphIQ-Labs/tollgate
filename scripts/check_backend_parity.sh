#!/usr/bin/env sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
exec cargo run --locked --quiet -p tollgate-repo-check --bin check_backend_parity -- "$@"
