#!/usr/bin/env sh
# Fails if a blocking gate's `rules` depend on where a change is routed.
#
# The `assurance` stage was once restricted to merge requests targeting the
# default branch (#106). That is not a skipped job with a visible state: the
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
BLOCKING_GATES="formal mutation perf-ratios"

# The only jobs allowed to carry `.main-merge-request`. Both are pinned to the
# single controlled host, and neither blocks a merge: `perf-thresholds` is
# manual (and uses the `-manual` anchor), and `load-thresholds` is explicitly
# non-gating evidence. Queueing every stacked merge request on one machine
# would buy no gate.
HOST_PINNED="load-thresholds"

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
    fail "$job must extend .merge-request so it runs for every merge request, not only one targeting the default branch (#106)"
  fi
done

# 3. Nothing else may carry the restricting anchor. This is what catches a
#    *new* gate added later with the wrong rule, rather than only the three
#    that exist today.
restricted=$(awk '
  /^[a-z][a-z0-9-]*:$/ { job = substr($0, 1, length($0) - 1) }
  /^  extends: / && /\.main-merge-request/ && !/\.main-merge-request-manual/ {
    if (job != "") print job
  }
' "$CONFIG")

for job in $restricted; do
  case " $HOST_PINNED " in
    *" $job "*) ;;
    *) fail "$job extends .main-merge-request, so it does not run for a merge request targeting a feature branch. Blocking gates use .merge-request; only a job pinned to the controlled host may restrict itself, and it must be listed in HOST_PINNED with the reason (#106)" ;;
  esac
done

if [ "$status" -eq 0 ]; then
  echo "ci rules: OK (blocking gates run on every merge request)"
fi
exit "$status"
