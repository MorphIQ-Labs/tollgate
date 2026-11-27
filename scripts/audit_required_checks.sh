#!/usr/bin/env sh
# Compares the checks `main` requires on GitHub with
# .github/required-checks.txt (GH-88). Needs `gh` signed in with permission to
# read branch protection, which the workflow token lacks, so this runs from an
# administrator's checkout. It fails if the setting cannot be read.
set -eu
ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
if ! protection=$(gh api 'repos/{owner}/{repo}/branches/main/protection/required_status_checks'); then
  echo "required checks audit: cannot read branch protection; nothing was audited" >&2
  exit 1
fi
printf '%s' "$protection" |
  cargo run --locked --quiet -p tollgate-repo-check --bin audit_required_checks
