#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
VERSION_FILE="$ROOT/.cargo/mutants-version"
NEXTEST_VERSION_FILE="$ROOT/.cargo/nextest-version"
OUTPUT="$ROOT/reports/mutations"
JOBS=${MUTANTS_JOBS:-4}

required=$(cat "$VERSION_FILE")
installed=$(cargo mutants --version 2>/dev/null || true)
if [ "$installed" != "cargo-mutants $required" ]; then
  echo "cargo-mutants $required is required; install it with:" >&2
  echo "  cargo install --locked cargo-mutants --version $required" >&2
  exit 127
fi

required_nextest=$(cat "$NEXTEST_VERSION_FILE")
installed_nextest=$(cargo nextest --version 2>/dev/null | sed -n '1s/^cargo-nextest \([^ ]*\).*/\1/p')
if [ "$installed_nextest" != "$required_nextest" ]; then
  echo "cargo-nextest $required_nextest is required; install it with:" >&2
  echo "  cargo install --locked cargo-nextest --version $required_nextest" >&2
  exit 127
fi

cd "$ROOT"
rm -rf "$OUTPUT"
mkdir -p "$(dirname "$OUTPUT")"

case "${1:-}" in
  --diff)
    base=${2:-main}
    merge_base=$(git merge-base HEAD "$base")
    diff=$(mktemp)
    trap 'rm -f "$diff"' EXIT
    git diff --src-prefix=a/ --dst-prefix=b/ "$merge_base" -- '*.rs' > "$diff"
    if [ ! -s "$diff" ]; then
      echo "mutation gate: no Rust changes against $base"
      exit 0
    fi
    if grep -q 'crates/tollgate-store-postgres/' "$diff" \
      && [ -z "${TOLLGATE_PG_URL:-}" ]; then
      echo "mutation gate: Postgres changed; set TOLLGATE_PG_URL so backend mutants are exercised" >&2
      exit 1
    fi
    cargo mutants --workspace -j "$JOBS" --line-col true --in-diff "$diff" --output "$OUTPUT"
    ;;
  "")
    if [ -z "${TOLLGATE_PG_URL:-}" ]; then
      echo "mutation gate: full sweep requires TOLLGATE_PG_URL" >&2
      exit 1
    fi
    cargo mutants --workspace -j "$JOBS" --line-col true --output "$OUTPUT"
    ;;
  *)
    echo "usage: $0 [--diff [base-ref]]" >&2
    exit 2
    ;;
esac

echo "mutation gate: OK (report: reports/mutations)"
