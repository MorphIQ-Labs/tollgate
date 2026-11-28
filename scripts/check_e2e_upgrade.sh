#!/usr/bin/env sh
# Runs the mixed-version end-to-end scenarios against earlier releases built
# from their tags. Local only, and not part of the pull-request gate.
#
# TOLLGATE_PG_URL as for ./scripts/check_e2e.sh. The tags must be present
# (git fetch --tags). Builds are kept under the target directory.
#
# A release's libraries are built exactly as tagged. Where its example service
# cannot be deployed against a control plane, a patch in
# crates/tollgate-e2e/compat/ adds that wiring and may touch nothing else.
set -eu

# The releases crates/tollgate-e2e/tests/upgrade.rs names.
RELEASES="v0.33.0 v0.32.2"

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

target=$(cargo metadata --locked --no-deps --format-version 1 |
  sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
previous="$target/e2e-previous"

for tag in $RELEASES; do
  commit=$(git rev-parse --verify --quiet "refs/tags/$tag^{commit}") || {
    echo "e2e-upgrade: tag $tag is missing (git fetch --tags)" >&2
    exit 2
  }
  patch="$ROOT/crates/tollgate-e2e/compat/$tag.patch"
  wanted=$commit
  if [ -f "$patch" ]; then
    if grep '^+++ ' "$patch" | grep -qv '^+++ b/examples/pricing-api/'; then
      echo "e2e-upgrade: $patch changes more than the example service" >&2
      exit 1
    fi
    wanted="$commit $(git hash-object "$patch")"
  fi
  source="$previous/source/$tag"
  # Extracted again only when the tag or its patch changed, so an unchanged
  # release is not rebuilt from scratch.
  if [ "$(cat "$source.stamp" 2>/dev/null || true)" != "$wanted" ]; then
    rm -rf "$source" "$source.stamp"
    mkdir -p "$source"
    git archive "$commit" | tar -x -C "$source"
    if [ -f "$patch" ]; then
      patch -p1 -s -d "$source" <"$patch"
    fi
    printf '%s\n' "$wanted" >"$source.stamp"
  fi
  # From inside the source, so that release's pinned toolchain builds it.
  (cd "$source" &&
    CARGO_TARGET_DIR="$previous/target/$tag" \
      cargo build --locked -p tollgate-server -p pricing-api --bins)
  mkdir -p "$previous/bin/$tag"
  cp "$previous/target/$tag/debug/tollgate-server" \
    "$previous/target/$tag/debug/pricing-api" "$previous/bin/$tag/"
done

TOLLGATE_E2E_PREVIOUS_DIR="$previous/bin" \
  exec "$ROOT/scripts/check_e2e.sh" --test upgrade -- --ignored "$@"
