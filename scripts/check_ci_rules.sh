#!/usr/bin/env sh
# Fails if a blocking gate's `rules` depend on where a change is routed.
#
# The `assurance` stage was once restricted to merge requests targeting the
# default branch (GL-106). That is not a skipped job with a visible state: the
# jobs are never created, so the pipeline page shows a complete, green pipeline
# with the whole column missing — and when the target branch merged, GitLab
# retargeted the request to `main` and left that assurance-free pipeline
# standing as its "pipelines must succeed" evidence. A change could reach the
# default branch having never been mutation-tested, and nothing anywhere said
# so.
#
# Whether a gate runs is a property of the change, not of its routing. This
# checks that mechanically, because the failure it guards against is invisible
# by construction: there is no red job to notice.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
CONFIG="$ROOT/.gitlab-ci.yml"

# Blocking gates. Each must run on every merge request, whatever it targets.
BLOCKING_GATES="formal mutation"

# Timed measurements run locally. Compilation and allocation assertions still
# belong in CI; a remote performance job must not silently restore the policy
# that made unchanged release builds fail different ratios (GL-113).
LOCAL_ONLY="perf-ratios perf-thresholds load-thresholds"

status=0

fail() {
  printf 'ci rules: %s\n' "$1" >&2
  status=1
}

# 1. The shared anchor must not grow a target-branch condition. Everything
#    below rests on `.merge-request` meaning "every merge request".
if awk '/^\.merge-request:$/{inside=1; next} /^[^ ]/{inside=0} inside' "$CONFIG" |
  grep -q 'CI_MERGE_REQUEST_TARGET_BRANCH_NAME'; then
  fail ".merge-request must not test CI_MERGE_REQUEST_TARGET_BRANCH_NAME; every job that extends it would silently stop running for a merge request targeting a feature branch"
fi

# 2. Each blocking gate extends it, and is in the stage it claims to be.
for job in $BLOCKING_GATES; do
  block=$(awk -v job="$job:" '$0 == job {inside=1; next} /^[^ #]/{inside=0} inside' "$CONFIG")
  if [ -z "$block" ]; then
    fail "$job is not a job in .gitlab-ci.yml; this list and the pipeline have drifted"
    continue
  fi
  if ! printf '%s\n' "$block" | grep -q '^  stage: assurance$'; then
    fail "$job is no longer in the assurance stage; move it or update BLOCKING_GATES with the reason"
  fi
  if ! printf '%s\n' "$block" | grep -E '^  extends: ' | grep -q '\.merge-request\b'; then
    fail "$job must extend .merge-request so it runs for every merge request, not only one targeting the default branch (GL-106)"
  fi
  if ! printf '%s\n' "$block" | grep -q '^  allow_failure: false$'; then
    fail "$job must explicitly remain a blocking gate (allow_failure: false)"
  fi
done

# 3. No job may restore a restricting performance anchor. This also catches
#    a new gate added with the old target-dependent rule (GL-106).
restricted=$(awk '
  /^[a-z][a-z0-9-]*:$/ { job = substr($0, 1, length($0) - 1) }
  /^  extends: / && /\.main-merge-request/ {
    if (job != "") print job
  }
' "$CONFIG")

for job in $restricted; do
  fail "$job uses a retired target-dependent anchor; assurance uses .merge-request and timed performance checks run locally"
done

# 4. Guard the local-only decision, including renamed jobs that invoke one
#    of the timed wrappers. `cargo bench --no-run` remains permitted.
for job in $LOCAL_ONLY; do
  if grep -q "^$job:" "$CONFIG"; then
    fail "$job belongs in the local performance workflow, not remote CI"
  fi
done
if grep -E '^[[:space:]]*-[[:space:]]*\./scripts/check_(perf|load)_thresholds\.sh' "$CONFIG" > /dev/null; then
  fail "timed performance wrappers must run locally"
fi
if grep -E '^[[:space:]]*-[[:space:]]*cargo bench([[:space:]]|$)' "$CONFIG" | grep -v -- '--no-run' > /dev/null; then
  fail "CI may compile benchmarks with --no-run, but timed execution is local"
fi

# 5. The recorded baseline names one physical host. A CI job that sets the
#    label either activates a host-specific contract on a machine that is not
#    that host, or — as the retired `perf-thresholds` job did — sets a value
#    that can never match, so the comparison silently reports
#    `baseline-skipped` and twelve days of drift accumulate behind a green
#    pipeline (GL-114).
if grep -E '^[[:space:]]*TOLLGATE_PERF_HOST:' "$CONFIG" > /dev/null; then
  fail "TOLLGATE_PERF_HOST is a local label for the host the baseline names; CI must not set it"
fi

if [ "$status" -eq 0 ]; then
  echo "ci rules: OK (formal/mutation block every merge request; timed performance is local)"
fi
exit "$status"
