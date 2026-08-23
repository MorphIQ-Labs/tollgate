# Tollgate Formal Models

This Lean package contains exact models for two snapshot contracts:

- `SnapshotCache` models the generation watermark independently from the
  evictable request-visible entry. It proves that unknown transitions and
  eviction preserve the watermark, revocation advances it to the maximum
  observed generation, and a positive replay at or below a revoked generation
  cannot resurrect the principal after eviction.
- `SnapshotLimits` proves that checking the largest registered operation
  weight at the configured batch cap bounds every registered operation at
  every permitted item count. Consequently, a worst-case quote at or below
  the burst keeps all in-limit quotes at or below the burst.

Run from the repository root:

```sh
./scripts/check_formal.sh
```

The models use exact natural-number arithmetic and atomic transitions. Rust
property, cache, manager, HTTP, and backend-parity tests establish the
proof-to-code argument. The proofs do not model scheduling, data-structure
internals, the maximum-weight scan, or finite-width overflow; those remain
separate Rust obligations.
