#!/usr/bin/env sh
# Cut the tag and the GitLab release for the version currently in Cargo.toml.
#
# The version and the changelog section are bumped in an ordinary merge request;
# this runs on the default branch after that lands.
#
# Idempotent by design: if the version is already tagged this exits 0 having done
# nothing, so it is safe to run on every merge and safe to retry a failed run.
# That matters here specifically: release-plz left `main` at 0.5.0 with v0.4.0 as
# the newest tag and no release merge request open, so the first run of this job
# completes that release rather than starting a new one.
#
# Nothing is packaged, which is the whole point. release-plz's git-only mode runs
# `cargo package` per crate — 9.5 minutes for this workspace, the slowest job in
# the org — to produce a tag this script produces in seconds without needing any
# dependency to resolve on a registry.
set -eu

DRY_RUN="${TAG_RELEASE_DRY_RUN:-0}"

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

if [ "$DRY_RUN" != "1" ]; then
  : "${RELEASE_TOKEN:?RELEASE_TOKEN must be set (Maintainer, api + write_repository)}"
  : "${CI_SERVER_HOST:?}" "${CI_PROJECT_PATH:?}" "${CI_PROJECT_ID:?}" "${CI_API_V4_URL:?}"
fi

version=$(sed -n 's/^version *= *"\(.*\)"/\1/p' Cargo.toml | head -1)
[ -n "$version" ] || { echo "tag-release: no [workspace.package] version in Cargo.toml" >&2; exit 1; }
tag="v$version"

if git rev-parse -q --verify "refs/tags/$tag" >/dev/null 2>&1 \
   || git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null 2>&1; then
  echo "tag-release: $tag already exists; nothing to do."
  exit 0
fi

# The release commit is the squashed second parent of the merge commit under
# the group-enforced merge method. Tag *it*, not the merge and not
# CI_COMMIT_SHA (#142). Its tree is exactly the tree the CHANGELOG section was
# written from and the release merge request's pipeline tested. The merge can
# also carry commits that reached the default branch after preparation — v0.29.0
# did: its tag contained a fix that its section omitted and 0.29.1's section
# listed — and `prepare-release` already attributes those to the next release.
# Tagging the release commit makes the tag's contents and its notes the same
# thing by construction. The merge is still required on the first-parent
# history, so only a release that actually landed is tagged.
release_commit=$(git log --no-merges --format='%H %s' HEAD | awk -v wanted="chore: release $tag" '
  {
    hash = $1
    sub(/^[^ ]+ /, "")
    if ($0 == wanted) { print hash; exit }
  }
')
[ -n "$release_commit" ] || {
  echo "tag-release: no 'chore: release $tag' commit found; refusing to tag HEAD" >&2
  exit 1
}

release_ref=$(git log --first-parent --merges --format='%H %P' HEAD | awk -v release="$release_commit" '
  $3 == release { print $1; exit }
')
[ -n "$release_ref" ] || {
  echo "tag-release: no first-parent merge introduces $release_commit; refusing to tag HEAD" >&2
  exit 1
}

# Headings in this repo look like `## [0.5.0](https://...)`; the pattern also
# accepts `## v0.5.0` and `## 0.5.0` so it survives a changelog style change
# rather than silently producing empty release notes.
notes=$(awk -v ver="$version" '
  BEGIN { inside = 0 }
  !inside && $0 ~ ("^#+[ \t]+\\[?v?" ver "([^0-9.]|$)") { inside = 1; next }
  inside && $0 ~ "^#+[ \t]+\\[?v?[0-9]" { exit }
  inside { print }
' CHANGELOG.md)

if [ -z "$(printf '%s' "$notes" | tr -d '[:space:]')" ]; then
  echo "tag-release: no CHANGELOG.md section found for $version" >&2
  exit 1
fi

# One variable for the dry run, the tag and the release, so the tested output
# and what is published cannot name different commits.
tag_target=$release_commit

if [ "$DRY_RUN" = "1" ]; then
  echo "tag-release: would release $tag at $tag_target (landed by $release_ref)"
  exit 0
fi

git config user.email "release-bot@noreply.$CI_SERVER_HOST"
git config user.name "release-bot"
git remote set-url origin "https://oauth2:${RELEASE_TOKEN}@${CI_SERVER_HOST}/${CI_PROJECT_PATH}.git"
git tag -a "$tag" -m "$tag" "$tag_target"
git push origin "refs/tags/$tag"

jq -n --arg tag "$tag" --arg ref "$tag_target" --arg desc "$notes" \
  '{tag_name:$tag, ref:$ref, description:$desc}' \
| curl --fail-with-body -sS -X POST \
    -H "PRIVATE-TOKEN: $RELEASE_TOKEN" -H "Content-Type: application/json" \
    --data @- "$CI_API_V4_URL/projects/$CI_PROJECT_ID/releases" >/dev/null

echo "tag-release: released $tag"
