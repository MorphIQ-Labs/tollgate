#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
VERSION_FILE="$ROOT/.cargo/audit-version"
# Each exception is an advisory and the package it is filed against, written
# "RUSTSEC-YYYY-NNNN:package" and separated by spaces, with the reason above the
# list. One is tolerated only while no workspace target compiles that package
# and the audit still fails without it. There are none.
EXCEPTIONS=""

required=$(cat "$VERSION_FILE")
installed=$(cargo-audit --version 2>/dev/null || true)
if [ "$installed" != "cargo-audit $required" ]; then
  echo "cargo-audit $required is required; install it with:" >&2
  echo "  cargo install --locked --no-default-features cargo-audit --version $required" >&2
  exit 127
fi

cd "$ROOT"

# An exception stops being safe immediately if its reachability fact changes.
for exception in $EXCEPTIONS; do
  advisory=${exception%%:*}
  package=${exception#*:}
  if ! grep -qx "name = \"$package\"" Cargo.lock; then
    echo "advisory gate: $package is not in Cargo.lock; remove the exception for $advisory" >&2
    exit 1
  fi
  reachable=$(cargo tree --quiet --locked --workspace --all-features --target all \
    --edges normal,build,dev --prefix none -i "$package")
  if [ -n "$reachable" ]; then
    echo "advisory gate: $package became reachable; $advisory may not be ignored" >&2
    printf '%s\n' "$reachable" >&2
    exit 1
  fi
done

# Every exception except the one named, as cargo-audit arguments.
ignore_all_but() {
  for exception in $EXCEPTIONS; do
    if [ "${exception%%:*}" != "$1" ]; then
      printf -- '--ignore %s ' "${exception%%:*}"
    fi
  done
}

# Vulnerabilities are errors by default; --deny warnings also blocks yanked,
# unmaintained, and unsound dependencies. These exceptions are the only ones.
# shellcheck disable=SC2046 # the arguments are split on purpose
cargo audit --deny warnings $(ignore_all_but none)

# Prove each exception is still necessary against the same freshly fetched
# advisory database. If the audit passes with only that one unignored, the
# advisory no longer applies and retaining it is itself a gate failure.
for exception in $EXCEPTIONS; do
  advisory=${exception%%:*}
  set +e
  # shellcheck disable=SC2046
  output=$(cargo audit --no-fetch --deny warnings $(ignore_all_but "$advisory") 2>&1)
  status=$?
  set -e
  case "$status" in
    0)
      echo "advisory gate: $advisory no longer applies; remove its exception" >&2
      exit 1
      ;;
    1)
      # The ignored audit already proved there are no other findings.
      ;;
    *)
      printf '%s\n' "$output" >&2
      echo "advisory gate: could not verify whether $advisory is still necessary" >&2
      exit "$status"
      ;;
  esac
done

if [ -z "$EXCEPTIONS" ]; then
  echo "advisory gate: OK (no exceptions)"
else
  echo "advisory gate: OK (every exception is isolated to an unreachable lockfile package: $EXCEPTIONS)"
fi
