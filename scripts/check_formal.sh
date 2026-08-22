#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PROOF_DIR="$ROOT/formal/lean"

if ! command -v lake >/dev/null 2>&1; then
  echo "lake is required; install elan with $(cat "$PROOF_DIR/lean-toolchain")" >&2
  exit 127
fi

if find "$PROOF_DIR" -path '*/.lake' -prune -o -name '*.lean' -exec \
  grep -nE '(^|[^[:alnum:]_])(sorry|admit)([^[:alnum:]_]|$)' {} +; then
  echo "formal gate: unchecked proof placeholder found" >&2
  exit 1
fi

cd "$PROOF_DIR"
lake build
