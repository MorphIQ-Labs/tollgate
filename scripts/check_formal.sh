#!/usr/bin/env sh
# Fails if any Lean proof carries an unchecked placeholder, then builds the
# library so every remaining theorem is machine-checked.
#
# INVARIANTS.md cites these proofs as evidence for #15 and #16, and the
# standard is to never claim more assurance than the checked artifacts
# establish — so a gate that can pass over a `sorry` silently downgrades that
# evidence rather than failing.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PROOF_DIR="$ROOT/formal/lean"

# One scan, used against the real tree and against the self-test fixtures, so
# the thing proven to bite is the thing that runs.
#
# `grep -r` rather than `find -exec grep +`: grep returns 1 when it matches
# nothing, and POSIX says `find -exec … +` returns non-zero if *any* invocation
# did. With one batch those two inversions cancelled and the gate worked by
# coincidence. Past `ARG_MAX` find makes several calls, so a batch containing a
# `sorry` (grep 0) alongside one that does not (grep 1) left find non-zero —
# the `if` read false and the gate walked straight past the placeholder into
# `lake build`, which accepts `sorry` with a warning and exits 0 (#60).
# Testing grep's own status removes the composition entirely.
scan_placeholders() {
  grep -rnE '(^|[^[:alnum:]_])(sorry|admit)([^[:alnum:]_]|$)' \
    --include='*.lean' --exclude-dir='.lake' "$1"
}

# The gate proves itself before it is trusted, because a gate nobody has
# watched fail is a gate nobody knows works — and this one passed for the wrong
# reason for its whole life. Two tiny greps; it costs milliseconds and it is
# the only thing standing between a `sorry` and a green pipeline.
#
# What it covers: that the scan detects a placeholder and does not invent one.
# What it cannot cover: the multi-batch inversion itself. This fixture is two
# files, so `find -exec … +` would make one call here and its two inversions
# would cancel — the same coincidence that made the old spelling work. Sizing
# the fixture past ARG_MAX would take thousands of files on every run, and
# ARG_MAX differs by platform. The composition bug is instead removed by
# construction: there is no `find` and no exit-status composition left to
# invert. Reproduced once, deliberately, at 9,001 files and three batches.
self_test() {
  fixture=$(mktemp -d)
  # `trap` fires on the `set -e` exits below as well as on a clean return.
  trap 'rm -rf "$fixture"' EXIT
  mkdir -p "$fixture/clean" "$fixture/planted"
  printf 'theorem fine : True := trivial\n' > "$fixture/clean/Fine.lean"
  printf 'theorem fine : True := trivial\n' > "$fixture/planted/Fine.lean"
  printf 'theorem planted : True := by sorry\n' > "$fixture/planted/Planted.lean"

  if scan_placeholders "$fixture/clean" > /dev/null 2>&1; then
    echo "formal gate: self-test failed — reported a placeholder in a clean tree" >&2
    exit 2
  fi
  if ! scan_placeholders "$fixture/planted" > /dev/null 2>&1; then
    echo "formal gate: self-test failed — did not detect a planted placeholder" >&2
    exit 2
  fi
  rm -rf "$fixture"
  trap - EXIT
}

self_test

# Zero proofs is not a clean tree, it is a broken checkout or a moved path.
# The old spelling reported "unchecked proof placeholder found" here, sending
# a reader hunting for a `sorry` that does not exist.
proofs=$(find "$PROOF_DIR" -path '*/.lake' -prune -o -name '*.lean' -print 2>/dev/null | wc -l)
if [ "$proofs" -eq 0 ]; then
  echo "formal gate: no .lean proofs found under $PROOF_DIR" >&2
  exit 1
fi

# Before the toolchain check: a placeholder is a failure whether or not Lean
# is installed, and this ordering is what lets the gate be exercised without
# it.
if scan_placeholders "$PROOF_DIR"; then
  echo "formal gate: unchecked proof placeholder found" >&2
  exit 1
fi

if ! command -v lake > /dev/null 2>&1; then
  echo "lake is required; install elan with $(cat "$PROOF_DIR/lean-toolchain")" >&2
  exit 127
fi

cd "$PROOF_DIR"
lake build
