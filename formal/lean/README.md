# Snapshot Cache Proofs

This Lean package models the generation watermark independently from the
evictable request-visible snapshot entry. It proves that unknown transitions
and eviction preserve the watermark, revocation advances it to the maximum
observed generation, and a positive replay at or below a revoked generation
cannot resurrect the principal after eviction.

Run from the repository root:

```sh
./scripts/check_formal.sh
```

The model covers exact generation ordering and atomic state transitions. Rust
property, cache, manager, HTTP, and backend-parity tests establish the
proof-to-code argument; the proof does not model scheduling or data-structure
internals.
