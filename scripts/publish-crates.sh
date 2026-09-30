#!/usr/bin/env sh
# Publish the workspace's publishable crates to crates.io at the version the
# manifest declares, in dependency order.
#
# Idempotent: a crate whose version is already on crates.io is skipped, so a
# release that failed half-way is completed by running this again. `cargo
# publish` waits for each version to reach the index before returning, so a
# dependent never publishes against a dependency the index cannot resolve yet.
#
# The order is stated, not inferred, and checked against the manifests: a crate
# made publishable without a place in this list, or listed without being
# publishable, fails the run before anything is published.
#
# Credentials come from the environment: CARGO_REGISTRY_TOKEN, either a scoped
# API token or the short-lived token crates.io Trusted Publishing exchanges for
# the workflow's OIDC identity.
set -eu

DRY_RUN="${PUBLISH_DRY_RUN:-0}"

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

ORDER="tollgate-core tollgate-auth tollgate-store tollgate-admission tollgate-store-postgres tollgate-client tollgate-server tollgate-axum"

publishable=$(cargo metadata --locked --format-version 1 --no-deps \
  | jq -r '.packages[] | select(.publish == null) | .name' | sort | tr '\n' ' ')
listed=$(printf '%s' "$ORDER" | tr ' ' '\n' | sort | tr '\n' ' ')
if [ "$publishable" != "$listed" ]; then
  printf 'publish-crates: ORDER and the publishable manifests disagree\n  publishable: %s\n  listed:      %s\n' \
    "$publishable" "$listed" >&2
  exit 1
fi

version=$(sed -n 's/^version *= *"\(.*\)"/\1/p' Cargo.toml | head -1)
[ -n "$version" ] || { echo "publish-crates: no [workspace.package] version in Cargo.toml" >&2; exit 1; }

on_crates_io() {
  status=$(curl -sS -o /dev/null -w '%{http_code}' \
    -A "tollgate-release (https://github.com/MorphIQ-Labs/tollgate)" \
    "https://crates.io/api/v1/crates/$1${2:+/$2}")
  case "$status" in
    200) return 0 ;;
    404) return 1 ;;
    *) echo "publish-crates: crates.io answered $status for $1 $version" >&2; exit 1 ;;
  esac
}

# Trusted Publishing cannot create a new crate name. Validate the one-time
# credential before publishing any part of this release. It is scoped only to
# tollgate-axum; existing crates retain the workflow's Trusted Publishing token.
axum_bootstrap=0
if [ "$DRY_RUN" != "1" ] && ! on_crates_io tollgate-axum; then
  : "${TOLLGATE_AXUM_BOOTSTRAP_TOKEN:?First tollgate-axum publication needs its scoped bootstrap token}"
  axum_bootstrap=1
fi

for crate in $ORDER; do
  if on_crates_io "$crate" "$version"; then
    echo "publish-crates: $crate $version is already on crates.io; skipping"
  elif [ "$DRY_RUN" = "1" ]; then
    echo "publish-crates: would publish $crate $version"
  else
    if [ "$crate" = "tollgate-axum" ] && [ "$axum_bootstrap" = "1" ]; then
      CARGO_REGISTRY_TOKEN="$TOLLGATE_AXUM_BOOTSTRAP_TOKEN" cargo publish --locked -p "$crate"
    else
      : "${CARGO_REGISTRY_TOKEN:?CARGO_REGISTRY_TOKEN must be set}"
      cargo publish --locked -p "$crate"
    fi
  fi
done
