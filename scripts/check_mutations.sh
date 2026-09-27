#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
VERSION_FILE="$ROOT/.cargo/mutants-version"
NEXTEST_VERSION_FILE="$ROOT/.cargo/nextest-version"
OUTPUT="$ROOT/reports/mutations"
# Each cargo-mutants worker starts its own nextest process. Nextest test groups
# can serialize PostgreSQL tests within one process, but not across workers;
# parallel workers would therefore truncate the same database underneath one
# another and could turn unrelated failures into false "caught" mutants.
PARALLEL=${MUTANTS_JOBS:-4}
if [ -n "${TOLLGATE_PG_URL:-}" ]; then
  if [ "${MUTANTS_JOBS:-1}" != "1" ]; then
    echo "mutation gate: MUTANTS_JOBS must be 1 when TOLLGATE_PG_URL is set; PostgreSQL tests share one database" >&2
    exit 2
  fi
  JOBS=1
else
  JOBS=$PARALLEL
fi
# `set -u` is on, so the verdict below needs this defined even when the run
# never reaches cargo-mutants.
status=0

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
    if grep -q 'crates/tollgate-store-postgres/' "$diff"; then
      if [ -z "${TOLLGATE_PG_URL:-}" ]; then
        echo "mutation gate: Postgres changed; set TOLLGATE_PG_URL so backend mutants are exercised" >&2
        exit 1
      fi
    elif [ -n "${TOLLGATE_PG_URL:-}" ] && [ -z "${MUTANTS_JOBS:-}" ]; then
      # The shared database is the only reason a diff run is serial, and a diff
      # that cannot change backend behaviour does not need it. Dropping it lets
      # the backend suite skip instead of race, and the gate use every worker.
      #
      # This trades away the backend suite's ability to kill a mutant in
      # another crate. That direction is safe: a mutant that only a PostgreSQL
      # test would have caught is now reported MISSED, which fails the gate and
      # is read by a person -- it never turns a missed mutant into a pass. It
      # also keeps cargo-mutants' timeout meaningful, because the suite each
      # mutant runs no longer takes longer than the gate's own floor.
      #
      # GL-67 is why this exists: 159 mutants at one worker ran past the job's
      # 90-minute limit, and a gate that cannot finish reports nothing at all.
      echo "mutation gate: no PostgreSQL change in this diff; the backend suite skips and the run uses $PARALLEL workers"
      unset TOLLGATE_PG_URL
      JOBS=$PARALLEL
    fi
    cargo mutants --workspace -j "$JOBS" --line-col true --in-diff "$diff" --output "$OUTPUT" \
      || status=$?
    ;;
  # One crate's whole surface, rather than only what a branch touched. The
  # diff mode protects code as it is written; this is how code written before
  # the gate existed gets measured at all (GL-43). `test_workspace` stays on, so
  # a mutant here is still allowed to die to any test in the workspace.
  --package)
    package=${2:-}
    if [ -z "$package" ]; then
      echo "usage: $0 --package <crate>" >&2
      exit 2
    fi
    if [ "$package" = "tollgate-store-postgres" ] && [ -z "${TOLLGATE_PG_URL:-}" ]; then
      echo "mutation gate: $package needs TOLLGATE_PG_URL, or its tests skip and every mutant reads as missed" >&2
      exit 1
    fi
    cargo mutants -p "$package" -j "$JOBS" --line-col true --output "$OUTPUT" || status=$?
    ;;
  "")
    if [ -z "${TOLLGATE_PG_URL:-}" ]; then
      echo "mutation gate: full sweep requires TOLLGATE_PG_URL" >&2
      exit 1
    fi
    cargo mutants --workspace -j "$JOBS" --line-col true --output "$OUTPUT" || status=$?
    ;;
  *)
    echo "usage: $0 [--diff [base-ref] | --package <crate>]" >&2
    exit 2
    ;;
esac

# State the verdict, not only the evidence. cargo-mutants' own last line is
# "N mutants tested: 2 missed, 34 caught, 11 unviable" — which reads as a
# result unless you already know that a missed mutant is fatal. Letting
# `set -e` abort here printed nothing at all on failure, so a caller skimming
# the tail of the output, or piping it (a shell pipeline reports only its
# last command's status), saw a run that looked clean. Both sibling gates
# print PASS or FAIL explicitly; this one now does too, and exits with the
# status it was given.
if [ "$status" -ne 0 ]; then
  echo "mutation gate: FAILED (cargo-mutants exit $status; report: $OUTPUT)" >&2
  exit "$status"
fi

echo "mutation gate: OK (report: reports/mutations)"
