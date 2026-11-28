#!/usr/bin/env sh
# Witness mutants for the background mechanisms: each entry of the catalogue
# disables one (a sweep, a reload, a retry, an enumeration, a push, a bound on a
# hung call), and the tests it names must then fail. See
# crates/tollgate-repo-check/src/witness_mutants/. PostgreSQL witnesses need
# TOLLGATE_PG_URL.
set -eu
ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
exec cargo run --locked --quiet -p tollgate-repo-check --bin check_witness_mutants -- "$@"
