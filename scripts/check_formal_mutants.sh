#!/usr/bin/env sh
# Mutation testing for the Lean models: every transition definition is
# mutated, one change at a time, and some theorem must fail for each. See
# crates/tollgate-repo-check/src/lean_mutants.rs. Needs the package built
# first (./scripts/check_formal.sh builds it).
set -eu
ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
exec cargo run --locked --quiet -p tollgate-repo-check --bin check_lean_mutants -- "$@"
