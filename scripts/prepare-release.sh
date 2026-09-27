#!/bin/sh
# Release preparation: the half of release-plz that `tag-release` does not replace.
# `tag-release` tags whatever version the manifest already declares; something
# has to advance that version, and until this script existed nothing did. A
# repository would accumulate commits on the default branch while the release
# job reported "already exists; nothing to do" on every push — silently, because
# a no-op is a success. Tollgate sat six commits past v0.5.0 that way, including
# a breaking change, with a green pipeline on every push.
#
# The model, matching what release-plz did here:
#
#   * one long-lived release branch, updated from the default branch whenever
#     the generated proposal changes, so the open release pull request always
#     reflects the current tip rather than whatever the first run happened to
#     see;
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
# is the release merge itself. A schedule remains a recovery lane; re-running
# leaves an identical proposal commit untouched and only force-updates the
# release branch when the generated release files have actually changed.
#
# Depends on git, gh, jq, base64 and a POSIX awk, deliberately. No gawk extensions, no python: the release path must not acquire
# an interpreter the release image does not already carry.
#
# Set PREPARE_RELEASE_DRY_RUN=1 to print the computed version and notes without
# touching git or the API. Run that against the real repository before trusting
# a schedule to it.
set -eu

RELEASE_BRANCH="${RELEASE_BRANCH:-chore/release}"
DRY_RUN="${PREPARE_RELEASE_DRY_RUN:-0}"

if [ "$DRY_RUN" != "1" ]; then
  # GH_TOKEN is the release App's installation token: a pull request opened
  # with the workflow's own GITHUB_TOKEN would never trigger the required
  # checks, so it could never merge.
  : "${GH_TOKEN:?GH_TOKEN must be the release App installation token}"
  : "${GITHUB_REPOSITORY:?}"
  : "${DEFAULT_BRANCH:?}"
fi

server_url="${GITHUB_SERVER_URL:-https://github.com}"
server_host="${server_url#https://}"
project_path="${GITHUB_REPOSITORY:-}"

[ -f CHANGELOG.md ] || {
  echo "prepare-release: CHANGELOG.md is required; add a root changelog before enabling releases" >&2
  exit 1
}

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
      # A squash merge appends the pull request number: " (#N)".
      sub(/ \(#[0-9]+\)$/, "")
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
# then refuse forever. A repository imported with its tags arrives exactly like
# this: manifest 0.1.0, tags through v0.1.2. Bump from whichever is higher.
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
# The workspace version and every matching internal path requirement move
# together; a stale compatible requirement can still resolve locally and hide
# the defect until packaging or a downstream git consumer evaluates it.
awk -v cur="$current" -v nxt="$next" '
  /^\[/ { sect = $0 }
  !done && (sect == "[workspace.package]" || sect == "[package]") && $0 ~ ("^version *= *\"" cur "\"") {
    sub("\"" cur "\"", "\"" nxt "\""); done = 1
  }
  { print }
  END { if (!done) exit 3 }
' Cargo.toml > Cargo.toml.next || { echo "prepare-release: could not bump Cargo.toml" >&2; exit 1; }
mv Cargo.toml.next Cargo.toml

git ls-files '*Cargo.toml' | while IFS= read -r f; do
  awk -v cur="$current" -v nxt="$next" '
    /path[ \t]*=/ && /version[ \t]*=/ {
      exact = "version[ \t]*=[ \t]*\"=" cur "\""
      compatible = "version[ \t]*=[ \t]*\"" cur "\""
      if ($0 ~ exact) sub(exact, "version = \"=" nxt "\"")
      else if ($0 ~ compatible) sub(compatible, "version = \"" nxt "\"")
    }
    { print }
  ' "$f" > "$f.next"
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
# A recovery schedule commonly sees the same default-branch tip as the
# post-merge run. Do not manufacture a new commit timestamp and force-push an
# identical proposal: besides being needless churn, that write can turn a
# transient push failure into a red recovery run. Fetch into
# FETCH_HEAD so this comparison does not depend on a local tracking ref.
git remote set-url origin "https://x-access-token:${GH_TOKEN}@${server_host}/${GITHUB_REPOSITORY}.git"

release_branch_current=0
if git fetch -q --no-tags origin "refs/heads/$RELEASE_BRANCH" 2>/dev/null; then
  if git diff --quiet FETCH_HEAD -- \
      CHANGELOG.md Cargo.lock ':(glob)**/Cargo.toml'; then
    release_branch_current=1
  fi
fi

if [ "$release_branch_current" = "1" ]; then
  echo "prepare-release: $RELEASE_BRANCH already matches v$next; leaving its commit unchanged"
else
  # The default branch requires signed commits, and a commit pushed with git
  # by the App is unsigned, so its pull request could never merge. A commit
  # the App creates through the API is signed by GitHub instead. Point the
  # release branch at the default branch's tip, then commit the release
  # files onto it in one API call that fails if the branch moved meanwhile.
  base=$(git rev-parse HEAD)
  if gh api "repos/$GITHUB_REPOSITORY/git/ref/heads/$RELEASE_BRANCH" >/dev/null 2>&1; then
    gh api -X PATCH "repos/$GITHUB_REPOSITORY/git/refs/heads/$RELEASE_BRANCH" \
      -f sha="$base" -F force=true >/dev/null
  else
    gh api -X POST "repos/$GITHUB_REPOSITORY/git/refs" \
      -f ref="refs/heads/$RELEASE_BRANCH" -f sha="$base" >/dev/null
  fi

  # Only the files this script edits, and only those it changed. `git add -A`
  # once swept in whatever else the job left in the working tree — a cargo
  # home inside the checkout committed 330 files of restored registry cache
  # alongside a three-line version bump.
  additions='[]'
  for path in $( { echo CHANGELOG.md; echo Cargo.lock; git ls-files '*Cargo.toml'; } \
      | xargs git diff --name-only HEAD -- ); do
    additions=$(printf '%s' "$additions" | jq --arg path "$path" \
      --arg contents "$(base64 -w0 < "$path")" '. + [{path: $path, contents: $contents}]')
  done

  jq -n --arg repo "$GITHUB_REPOSITORY" --arg branch "$RELEASE_BRANCH" \
        --arg headline "chore: release v$next" --arg base "$base" \
        --argjson additions "$additions" '{
    query: "mutation($input: CreateCommitOnBranchInput!) { createCommitOnBranch(input: $input) { commit { oid } } }",
    variables: { input: {
      branch: { repositoryNameWithOwner: $repo, branchName: $branch },
      message: { headline: $headline },
      expectedHeadOid: $base,
      fileChanges: { additions: $additions }
    } }
  }' | gh api graphql --input - --jq '"prepare-release: committed \(.data.createCommitOnBranch.commit.oid) to '"$RELEASE_BRANCH"'"'
fi

# --- create the pull request, or refresh the open one ---------------------
existing=$(gh pr list --repo "$GITHUB_REPOSITORY" --state open --head "$RELEASE_BRANCH" \
  --json number --jq '.[0].number // empty')

body="Prepared by \`prepare-release\` from the conventional commits since $range_start.

Merging this bumps the workspace to **$next**; \`tag-release\` then cuts \`v$next\`, the GitHub release from the CHANGELOG section below, and publishes the crates.

$entries"

if [ -n "$existing" ]; then
  printf '%s' "$body" | gh pr edit "$existing" --repo "$GITHUB_REPOSITORY" \
    --title "chore: release v$next" --body-file - >/dev/null
  echo "prepare-release: refreshed #$existing to v$next"
else
  printf '%s' "$body" | gh pr create --repo "$GITHUB_REPOSITORY" \
    --base "$DEFAULT_BRANCH" --head "$RELEASE_BRANCH" \
    --title "chore: release v$next" --body-file -
fi
