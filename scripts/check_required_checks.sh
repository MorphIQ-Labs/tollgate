#!/usr/bin/env sh
# Fails if a job in ci.yml is not listed as a required check, or a listed
# check has no job (GH-88). See crates/tollgate-repo-check/src/required_checks.rs.
set -eu
ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
exec cargo run --locked --quiet -p tollgate-repo-check --bin check_required_checks -- "$@"
