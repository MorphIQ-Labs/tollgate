#!/usr/bin/env sh
# Runs the long end-to-end scenario: sustained load across lease renewal,
# snapshot refresh and budget-period boundaries. Local only, and not part of the
# pull-request gate.
#
# TOLLGATE_PG_URL as for ./scripts/check_e2e.sh. TOLLGATE_E2E_SOAK_SECS sets how
# long load is sustained (default 600).
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
exec "$ROOT/scripts/check_e2e.sh" --test soak -- --ignored --nocapture "$@"
