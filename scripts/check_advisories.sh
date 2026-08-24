#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
VERSION_FILE="$ROOT/.cargo/audit-version"
IGNORED_ADVISORY=RUSTSEC-2023-0071
IGNORED_PACKAGE=rsa

required=$(cat "$VERSION_FILE")
installed=$(cargo-audit --version 2>/dev/null || true)
if [ "$installed" != "cargo-audit $required" ]; then
  echo "cargo-audit $required is required; install it with:" >&2
  echo "  cargo install --locked --no-default-features cargo-audit --version $required" >&2
  exit 127
fi

cd "$ROOT"

# RUSTSEC-2023-0071 affects rsa 0.9.10 and has no fixed release. The package is
# present only because Cargo.lock records sqlx-macros-core's optional MySQL
# dependency graph; no workspace target or feature compiles it. The exception
# stops being safe immediately if that reachability fact changes.
reachable=$(cargo tree --quiet --locked --workspace --all-features --target all \
  --edges normal,build,dev --prefix none -i "$IGNORED_PACKAGE")
if [ -n "$reachable" ]; then
  echo "advisory gate: $IGNORED_PACKAGE became reachable; $IGNORED_ADVISORY may not be ignored" >&2
  printf '%s\n' "$reachable" >&2
  exit 1
fi

# Vulnerabilities are errors by default; --deny warnings also blocks yanked,
# unmaintained, and unsound dependencies. This is the sole exception.
cargo audit --deny warnings --ignore "$IGNORED_ADVISORY"

# Prove the exception is still necessary against the same freshly fetched
# advisory database. If the unignored audit passes, the advisory no longer
# applies and retaining the escape hatch is itself a gate failure.
set +e
unignored_output=$(cargo audit --no-fetch --deny warnings 2>&1)
unignored_status=$?
set -e
case "$unignored_status" in
  0)
    echo "advisory gate: $IGNORED_ADVISORY no longer applies; remove its exception" >&2
    exit 1
    ;;
  1)
    # The ignored audit already proved there are no other findings.
    ;;
  *)
    printf '%s\n' "$unignored_output" >&2
    echo "advisory gate: could not verify whether $IGNORED_ADVISORY is still necessary" >&2
    exit "$unignored_status"
    ;;
esac

echo "advisory gate: OK ($IGNORED_ADVISORY is isolated to an unreachable lockfile package)"
