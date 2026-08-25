#!/bin/sh
# Reference release-preparation script. Copy to `scripts/prepare-release.sh`.
#
# This is the half of release-plz that `tag-release` does not replace.
# `tag-release` tags whatever version the manifest already declares; something
# has to advance that version, and until this script existed nothing did. A
# repository would accumulate commits on the default branch while the release
# job reported "already exists; nothing to do" on every push — silently, because
# a no-op is a success. Tollgate sat six commits past v0.5.0 that way, including
# a breaking change, with a green pipeline on every push.
#
# The model, matching what release-plz did here:
#
#   * one long-lived release branch, recreated from the default branch on every
#     run, so the open release merge request always reflects the current tip
#     rather than whatever the first run happened to see;
#   * the version derived from the conventional-commit subjects since the last
#     tag, not from a human's judgement at merge time;
#   * the CHANGELOG section written from those same subjects, so the notes
#     `tag-release` publishes and the commits that earned them cannot diverge.
#
# Bump rule. Under `0.x` Cargo's compatibility boundary is the minor field, so
# that is the breaking axis. This is the rule release-plz applied to these
# workspaces, and it is derived from released history rather than chosen:
# tollgate's 0.2.13 -> 0.2.14 was a feat (patch), 0.2.14 -> 0.3.0 was breaking
# (minor), 0.3.0 -> 0.3.1 was neither (patch).
#
#              | breaking | feat  | anything else
#     0.x      | minor    | patch | patch
#     >= 1.0   | major    | minor | patch
#
# Runs automatically after every merge to the default branch. It is idempotent:
# with no releasable commits it exits 0, including when the merge being examined
# is the release merge itself. A schedule remains a recovery lane, and re-running
# only force-updates the release branch to match the default branch again.
#
# Depends on git, curl, jq and a POSIX awk — the same set `tag-release.sh` needs,
# deliberately. No gawk extensions, no python: the release path must not acquire
# an interpreter the release image does not already carry.
#
# Set PREPARE_RELEASE_DRY_RUN=1 to print the computed version and notes without
# touching git or the API. Run that against the real repository before trusting
# a schedule to it.
set -eu

RELEASE_BRANCH="${RELEASE_BRANCH:-chore/release}"
DRY_RUN="${PREPARE_RELEASE_DRY_RUN:-0}"

if [ "$DRY_RUN" != "1" ]; then
  : "${CI_SERVER_HOST:?}"
  : "${CI_PROJECT_PATH:?}"
  : "${CI_PROJECT_ID:?}"
  : "${CI_API_V4_URL:?}"
  : "${CI_DEFAULT_BRANCH:?}"
  : "${RELEASE_TOKEN:?}"
fi

server_host="${CI_SERVER_HOST:-gitlab.com}"
project_path="${CI_PROJECT_PATH:-}"

# --- what is released now -------------------------------------------------
current=$(awk '
  /^\[/ { sect = $0 }
  (sect == "[workspace.package]" || sect == "[package]") && /^version *= *"/ {
    v = $0; sub(/^version *= *"/, "", v); sub(/".*/, "", v); print v; exit
  }
' Cargo.toml)
[ -n "$current" ] || { echo "prepare-release: no version in Cargo.toml" >&2; exit 1; }

last_tag=$(git tag -l 'v*' --sort=-v:refname | head -n 1 || true)

# A release merge advances the manifest before `tag-release` creates its tag.
# If preparation blindly keeps using the previous tag as its range, the release
# merge immediately proposes another bump from the same commits. Anchor at the
# untagged release commit instead. This also handles a later merge arriving
# before the tag job finishes: only commits after the pending release contribute
# to the following version.
release_commit=""
if ! git rev-parse -q --verify "refs/tags/v$current" >/dev/null 2>&1; then
  release_commit=$(git log --no-merges --format='%H %s' HEAD | awk -v wanted="chore: release v$current" '
    {
      hash = $1
      sub(/^[^ ]+ /, "")
      if ($0 == wanted) { print hash; exit }
    }
  ')
fi

if [ -n "$release_commit" ]; then
  range="$release_commit..HEAD"
  range_start="pending release v$current"
  comparison_tag="v$current"
elif [ -n "$last_tag" ]; then
  range="$last_tag..HEAD"
  range_start="$last_tag"
  comparison_tag="$last_tag"
else
  range="HEAD"
  range_start="the first commit"
  comparison_tag=""
fi

# The release commit itself is not a reason to release again.
subjects=$(git log --no-merges --format='%s' "$range" | grep -v '^chore: release v' || true)
if [ -z "$subjects" ]; then
  echo "prepare-release: no releasable commits since $range_start; nothing to do."
  exit 0
fi

# --- how big a bump -------------------------------------------------------
breaking=0
if git log --no-merges --format='%s%n%b' "$range" \
   | grep -qE '^[a-z]+(\([^)]*\))?!:|^BREAKING[ -]CHANGE'; then breaking=1; fi
feat=0
if printf '%s\n' "$subjects" | grep -qE '^feat(\([^)]*\))?!?:'; then feat=1; fi

# The manifest is not always the truth about what was last released. If a tag
# is higher than the manifest version, the manifest is stale and bumping from it
# would propose a version that is already tagged — which `tag-release` would
# then refuse forever. a sibling workspace arrived from GitHub exactly like this:
# manifest 0.1.0, tags through v0.1.2. Bump from whichever is higher.
tag_version=${last_tag#v}
if [ -n "$tag_version" ]; then
  base=$(printf '%s\n%s\n' "$current" "$tag_version" | sort -V | tail -n 1)
else
  base="$current"
fi
if [ "$base" != "$current" ]; then
  echo "prepare-release: manifest is $current but $last_tag is released; bumping from $base"
fi

major=${base%%.*}
rest=${base#*.}
minor=${rest%%.*}
patch=${rest#*.}
patch=${patch%%-*}

if [ "$major" = "0" ]; then
  if [ "$breaking" = "1" ]; then minor=$((minor + 1)); patch=0
  else patch=$((patch + 1)); fi
else
  if [ "$breaking" = "1" ]; then major=$((major + 1)); minor=0; patch=0
  elif [ "$feat" = "1" ]; then minor=$((minor + 1)); patch=0
  else patch=$((patch + 1)); fi
fi
next="$major.$minor.$patch"

if git rev-parse -q --verify "refs/tags/v$next" >/dev/null 2>&1; then
  echo "prepare-release: v$next is already tagged; refusing to propose it" >&2
  exit 1
fi

# --- the CHANGELOG section ------------------------------------------------
# feat -> Added, fix -> Fixed, everything else -> Other, matching the grouping
# already present in the released sections of these changelogs.
entries=$(printf '%s\n' "$subjects" | awk '
  {
    line = $0

    type = line
    sub(/[(!:].*/, "", type)

    scope = ""
    if (line ~ /^[a-z]+\([^)]*\)/) {
      scope = line
      sub(/^[a-z]+\(/, "", scope)
      sub(/\).*/, "", scope)
    }

    bang = (line ~ /^[a-z]+(\([^)]*\))?!:/)

    desc = line
    sub(/^[a-z]+(\([^)]*\))?!?:[ \t]*/, "", desc)

    prefix = "- "
    if (scope != "") prefix = prefix "*(" scope ")* "
    if (bang) prefix = prefix "[**breaking**] "

    if (type == "feat") added = added prefix desc "\n"
    else if (type == "fix") fixed = fixed prefix desc "\n"
    else other = other prefix desc "\n"
  }
  END {
    if (added != "") printf "### Added\n\n%s\n", added
    if (fixed != "") printf "### Fixed\n\n%s\n", fixed
    if (other != "") printf "### Other\n\n%s\n", other
  }
')

if [ -n "$comparison_tag" ] && [ -n "$project_path" ]; then
  heading="## [$next](https://$server_host/$project_path/compare/$comparison_tag...v$next) - $(date -u +%Y-%m-%d)"
else
  heading="## [$next] - $(date -u +%Y-%m-%d)"
fi

if [ "$DRY_RUN" = "1" ]; then
  echo "prepare-release: $current -> $next (breaking=$breaking feat=$feat) since $range_start"
  echo "---"
  echo "$heading"
  echo
  printf '%s' "$entries"
  exit 0
fi

# --- rewrite the manifests ------------------------------------------------
# The workspace version and every internal `=<current>` pin move together; a pin
# left behind fails resolution rather than silently building something stale.
awk -v cur="$current" -v nxt="$next" '
  /^\[/ { sect = $0 }
  !done && (sect == "[workspace.package]" || sect == "[package]") && $0 ~ ("^version *= *\"" cur "\"") {
    sub("\"" cur "\"", "\"" nxt "\""); done = 1
  }
  { print }
  END { if (!done) exit 3 }
' Cargo.toml > Cargo.toml.next || { echo "prepare-release: could not bump Cargo.toml" >&2; exit 1; }
mv Cargo.toml.next Cargo.toml

for f in $(git ls-files '*Cargo.toml'); do
  sed "s/version = \"=$current\"/version = \"=$next\"/g" "$f" > "$f.next"
  if cmp -s "$f" "$f.next"; then rm -f "$f.next"; else mv "$f.next" "$f"; echo "  pin bumped: $f"; fi
done

cargo update --workspace

# --- rewrite the CHANGELOG ------------------------------------------------
# Splice the section in by line number rather than passing it to awk: a POSIX
# awk rejects a newline inside a -v assignment ("newline in string"), so the
# multi-line section never survives the handoff. gawk tolerates it, which is
# exactly how this would have reached CI looking fine.
#
# Most of these changelogs carry an `## [Unreleased]` marker and the section goes
# directly beneath it. Some do not — release-plz never wrote one — so fall back
# to inserting above the first released version heading, and finally to appending
# after the preamble in a changelog with no versions at all.
unreleased_line=$(awk '/^## \[Unreleased\]/ { print NR; exit }' CHANGELOG.md)
first_version_line=$(awk '/^#+[ \t]+\[?v?[0-9]/ { print NR; exit }' CHANGELOG.md)

# Emit the section with exactly one blank line after it, whatever trailing
# whitespace the generated entries happen to carry, so spacing does not depend
# on which groups were non-empty.
emit_section() {
  printf '%s\n\n' "$heading"
  printf '%s' "$entries" | awk '
    { line[NR] = $0 }
    END {
      last = NR
      while (last > 0 && line[last] ~ /^[ \t]*$/) last--
      for (i = 1; i <= last; i++) print line[i]
    }
  '
  echo
}

{
  if [ -n "$unreleased_line" ]; then
    head -n "$unreleased_line" CHANGELOG.md
    echo
    emit_section
    tail -n +$((unreleased_line + 1)) CHANGELOG.md | awk 'NR == 1 && /^[ \t]*$/ { next } { print }'
  elif [ -n "$first_version_line" ]; then
    head -n $((first_version_line - 1)) CHANGELOG.md
    emit_section
    tail -n +"$first_version_line" CHANGELOG.md
  else
    cat CHANGELOG.md
    echo
    emit_section
  fi
} > CHANGELOG.md.next
mv CHANGELOG.md.next CHANGELOG.md

# --- one long-lived release branch ---------------------------------------
git config user.email "release-bot@noreply.$CI_SERVER_HOST"
git config user.name "release-bot"
git remote set-url origin "https://oauth2:${RELEASE_TOKEN}@${CI_SERVER_HOST}/${CI_PROJECT_PATH}.git"

git checkout -B "$RELEASE_BRANCH"

# Stage only the files this script edits. `git add -A` sweeps in whatever else
# the job left in the working tree — CARGO_HOME is inside CI_PROJECT_DIR in
# these projects, so the first run committed 330 files of restored registry
# cache alongside a three-line version bump.
git add CHANGELOG.md Cargo.lock
git ls-files -z '*Cargo.toml' | xargs -0 git add --

git commit -q -m "chore: release v$next"
git push -q --force origin "refs/heads/$RELEASE_BRANCH"

# --- create the merge request, or refresh the open one --------------------
api="$CI_API_V4_URL/projects/$CI_PROJECT_ID/merge_requests"
existing=$(curl --fail-with-body -sS -H "PRIVATE-TOKEN: $RELEASE_TOKEN" \
  "$api?state=opened&source_branch=$RELEASE_BRANCH" | jq -r '.[0].iid // empty')

body="Prepared by \`prepare-release\` from the conventional commits since $range_start.

Merging this bumps the workspace to **$next**; \`tag-release\` then cuts \`v$next\` and the GitLab release from the CHANGELOG section below.

$entries"

if [ -n "$existing" ]; then
  jq -n --arg t "chore: release v$next" --arg d "$body" '{title:$t, description:$d}' \
  | curl --fail-with-body -sS -X PUT -H "PRIVATE-TOKEN: $RELEASE_TOKEN" \
      -H "Content-Type: application/json" --data @- "$api/$existing" >/dev/null
  echo "prepare-release: refreshed !$existing to v$next"
else
  jq -n --arg s "$RELEASE_BRANCH" --arg tb "$CI_DEFAULT_BRANCH" \
        --arg t "chore: release v$next" --arg d "$body" \
    '{source_branch:$s, target_branch:$tb, title:$t, description:$d,
      squash:true, remove_source_branch:false}' \
  | curl --fail-with-body -sS -X POST -H "PRIVATE-TOKEN: $RELEASE_TOKEN" \
      -H "Content-Type: application/json" --data @- "$api" \
  | jq -r '"prepare-release: opened !\(.iid)"'
fi
