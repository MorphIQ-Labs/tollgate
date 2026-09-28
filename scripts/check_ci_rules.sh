#!/usr/bin/env sh
# Fails if a blocking gate's triggers depend on where a change is routed.
#
# The assurance jobs were once restricted to merge requests targeting the
# default branch (GL-106). That is not a skipped job with a visible state: the
# jobs are never created, so the checks page shows a complete, green run with
# the whole column missing, and a change could reach the default branch having
# never been mutation-tested while nothing anywhere said so.
#
# Whether a gate runs is a property of the change, not of its routing. This
# checks that mechanically against the GitHub Actions workflow, because the
# failure it guards against is invisible by construction: there is no red job
# to notice.
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
CONFIG="$ROOT/.github/workflows/ci.yml"
WORKFLOWS="$ROOT/.github/workflows"

# Blocking gates. Each must run on every pull request, whatever it targets.
BLOCKING_GATES="formal mutation-shard"

# Required checks that summarise a sharded gate, as `<job>:<gate>`. Each runs
# under `always()` and fails unless the gate's result is `success`, because
# GitHub reports a job skipped behind a failed dependency as passing: an
# aggregate without `always()` would go green exactly when a shard failed.
AGGREGATES="mutation:mutation-shard"

# Timed measurements run locally. Compilation and allocation assertions still
# belong in CI; a remote performance job must not silently restore the policy
# that made unchanged release builds fail different ratios (GL-113).
LOCAL_ONLY="perf-ratios perf-thresholds load-thresholds"

status=0

fail() {
  printf 'ci rules: %s\n' "$1" >&2
  status=1
}

if [ ! -f "$CONFIG" ]; then
  fail ".github/workflows/ci.yml is missing; the gates it carries run nowhere"
  exit 1
fi

# The block of one job: its lines under `jobs:`, from `  <job>:` to the next
# two-space key.
job_block() {
  awk -v job="  $1:" '
    /^jobs:$/ { in_jobs = 1; next }
    in_jobs && $0 == job { inside = 1; next }
    in_jobs && /^  [^ #]/ { inside = 0 }
    /^[^ #]/ && !/^jobs:$/ { in_jobs = 0; inside = 0 }
    inside
  ' "$CONFIG"
}

# 1. `pull_request` must be unfiltered. A `branches`, `paths`, or `types`
#    filter decides by routing or file set whether every job runs at all.
pr_filters=$(awk '
  /^on:$/ { in_on = 1; next }
  /^[^ #]/ { in_on = 0 }
  in_on && /^  pull_request:/ { in_pr = 1; next }
  in_on && /^  [^ ]/ { in_pr = 0 }
  in_on && in_pr && /^    [^ #]/ { print }
' "$CONFIG")
if [ -n "$pr_filters" ]; then
  fail "the pull_request trigger must not be filtered; every gate would silently stop running for some pull requests (GL-106): $pr_filters"
fi
if ! grep -qE '^  pull_request:[[:space:]]*$' "$CONFIG"; then
  fail "ci.yml must trigger on pull_request"
fi

# 2. Each blocking gate exists, runs on every pull request, and blocks.
for job in $BLOCKING_GATES; do
  block=$(job_block "$job")
  if [ -z "$block" ]; then
    fail "$job is not a job in ci.yml; this list and the workflow have drifted"
    continue
  fi
  condition=$(printf '%s\n' "$block" | sed -n 's/^    if:[[:space:]]*//p')
  if [ "$condition" != "github.event_name == 'pull_request'" ]; then
    fail "$job must run on every pull request (if: github.event_name == 'pull_request'), found: ${condition:-no condition}"
  fi
  if printf '%s\n' "$block" | grep -qE '^[[:space:]]+continue-on-error:[[:space:]]*true'; then
    fail "$job must remain a blocking gate; continue-on-error turns a failure green"
  fi
done

# 2b. Each aggregate runs on every pull request even after a failure, depends
#     on its gate, and fails unless that gate succeeded.
for pair in $AGGREGATES; do
  job=${pair%%:*}
  gate=${pair#*:}
  block=$(job_block "$job")
  if [ -z "$block" ]; then
    fail "$job is not a job in ci.yml; the required check summarising $gate is missing"
    continue
  fi
  condition=$(printf '%s\n' "$block" | sed -n 's/^    if:[[:space:]]*//p')
  if [ "$condition" != "always() && github.event_name == 'pull_request'" ]; then
    fail "$job must run whenever $gate ran or was skipped (if: always() && github.event_name == 'pull_request'), found: ${condition:-no condition}"
  fi
  if ! printf '%s\n' "$block" | grep -qE "^    needs: \[$gate\]\$"; then
    fail "$job must depend on exactly $gate (needs: [$gate])"
  fi
  if ! printf '%s\n' "$block" | grep -qF "needs.$gate.result"; then
    fail "$job must fail unless needs.$gate.result is success"
  fi
  if printf '%s\n' "$block" | grep -qE '^[[:space:]]+continue-on-error:[[:space:]]*true'; then
    fail "$job must remain a blocking gate; continue-on-error turns a failure green"
  fi
done

# 3. No condition anywhere may test the target branch. This also catches a new
#    gate added with the old target-dependent rule (GL-106).
if grep -nE 'github\.base_ref|pull_request\.base\.ref' "$WORKFLOWS"/*.yml > /dev/null 2>&1; then
  fail "a workflow tests the target branch; assurance runs on every pull request and timed performance checks run locally"
fi

# 4. Guard the local-only decision, including renamed jobs that invoke one of
#    the timed wrappers. `cargo bench --no-run` remains permitted.
for job in $LOCAL_ONLY; do
  if grep -qE "^  $job:" "$WORKFLOWS"/*.yml 2>/dev/null; then
    fail "$job belongs in the local performance workflow, not remote CI"
  fi
done
if grep -E 'scripts/check_(perf|load)_thresholds\.sh' "$WORKFLOWS"/*.yml > /dev/null 2>&1; then
  fail "timed performance wrappers must run locally"
fi
if grep -E 'cargo bench([[:space:]]|$)' "$WORKFLOWS"/*.yml 2>/dev/null | grep -v -- '--no-run' > /dev/null; then
  fail "CI may compile benchmarks with --no-run, but timed execution is local"
fi

# 5. The recorded baseline names one physical host. A CI job that sets the
#    label either activates a host-specific contract on a machine that is not
#    that host, or sets a value that can never match, so the comparison
#    silently reports `baseline-skipped` behind a green run (GL-114).
if grep -E 'TOLLGATE_PERF_HOST' "$WORKFLOWS"/*.yml > /dev/null 2>&1; then
  fail "TOLLGATE_PERF_HOST is a local label for the host the baseline names; CI must not set it"
fi

if [ "$status" -eq 0 ]; then
  echo "ci rules: OK (formal/mutation block every pull request; timed performance is local)"
fi
exit "$status"
