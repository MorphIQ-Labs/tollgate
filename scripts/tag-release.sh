#!/usr/bin/env sh
# Cut the tag and the GitHub release for the version currently in Cargo.toml.
#
# The version and the changelog section are bumped in an ordinary pull request
# opened by `prepare-release`; this runs on the default branch after that lands.
#
# Idempotent by design: if the version is already tagged this exits 0 having done
# nothing, so it is safe to run on every merge and safe to retry a failed run.
#
# Nothing is packaged here, which is deliberate: the tag and the release are
# cut in seconds, and publishing to crates.io is a separate, equally idempotent
# step (`publish-crates.sh`) that can be retried on its own.
set -eu

DRY_RUN="${TAG_RELEASE_DRY_RUN:-0}"

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

if [ "$DRY_RUN" != "1" ]; then
  : "${GH_TOKEN:?GH_TOKEN must allow contents write}"
  : "${GITHUB_REPOSITORY:?}"
  : "${RELEASE_BOT_NAME:?}" "${RELEASE_BOT_EMAIL:?}"
fi

version=$(sed -n 's/^version *= *"\(.*\)"/\1/p' Cargo.toml | head -1)
[ -n "$version" ] || { echo "tag-release: no [workspace.package] version in Cargo.toml" >&2; exit 1; }
tag="v$version"

if git rev-parse -q --verify "refs/tags/$tag" >/dev/null 2>&1 \
   || git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null 2>&1; then
  echo "tag-release: $tag already exists; nothing to do."
  exit 0
fi

# Tag the release commit itself, never HEAD (GL-142). With squash-only merging
# and branch protection requiring the branch to be up to date, the release pull
# request lands as exactly one commit on the first-parent history, whose tree is
# the tree its CHANGELOG section was written from and its checks tested. Later
# commits on the default branch belong to the next release, and
# `prepare-release` already attributes them there. A squash merge appends the
# pull request number to the subject; the unsuffixed form is accepted too.
release_commit=$(git log --first-parent --format='%H %s' HEAD | awk -v wanted="chore: release $tag" '
  {
    hash = $1
    sub(/^[^ ]+ /, "")
    sub(/ \(#[0-9]+\)$/, "")
    if ($0 == wanted) { print hash; exit }
  }
')
[ -n "$release_commit" ] || {
  echo "tag-release: no 'chore: release $tag' commit on the first-parent history; refusing to tag HEAD" >&2
  exit 1
}

# Headings in this repo look like `## [0.5.0](https://...)`; the pattern also
# accepts `## v0.5.0` and `## 0.5.0` so it survives a changelog style change
# rather than silently producing empty release notes.
notes=$(git show "$release_commit:CHANGELOG.md" | awk -v ver="$version" '
  BEGIN { inside = 0 }
  !inside && $0 ~ ("^#+[ \t]+\\[?v?" ver "([^0-9.]|$)") { inside = 1; next }
  inside && $0 ~ "^#+[ \t]+\\[?v?[0-9]" { exit }
  inside { print }
')

if [ -z "$(printf '%s' "$notes" | tr -d '[:space:]')" ]; then
  echo "tag-release: no CHANGELOG.md section found for $version at $release_commit" >&2
  exit 1
fi

if [ "$DRY_RUN" = "1" ]; then
  echo "tag-release: would release $tag at $release_commit"
  exit 0
fi

git config user.name "$RELEASE_BOT_NAME"
git config user.email "$RELEASE_BOT_EMAIL"
git tag -a "$tag" -m "$tag" "$release_commit"
git push origin "refs/tags/$tag"

printf '%s\n' "$notes" | gh release create "$tag" --repo "$GITHUB_REPOSITORY" \
  --verify-tag --title "$tag" --notes-file -
echo "tag-release: released $tag at $release_commit"

# The release workflow publishes only a tag cut in the same run, from that
# tag's own commit, so it needs to know which tag that was.
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "released=$tag" >> "$GITHUB_OUTPUT"
fi
