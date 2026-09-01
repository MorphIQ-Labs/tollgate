# Tollgate Formal Models

This Lean package contains exact models for seven critical contracts:

- `LeaseShards` models the aggregate of cache-isolated lease counters and
  exact-total debit/refund receipts. It proves conservation through reserve, cancel,
  and commit, proves committed spend stays within the grant, and proves that
  sufficient aggregate capacity always has an exact debit (the Rust algorithm
  and property tests establish that the implementation finds it).
- `SnapshotCache` models the generation watermark independently from the
  evictable request-visible entry. The watermark records *why* it exists — a
  generation the source published a revocation at, or the newest positive this
  instance installed — and only the first may refuse an equal generation. It
  proves that unknown transitions and eviction preserve the watermark, that an
  absence never turns an observation into a revocation, that revocation
  advances the watermark to the maximum observed generation, and that a
  positive replay at or below a revoked generation cannot resurrect the
  principal after eviction. It also proves the converse the distinction exists
  for: a principal whose row went absent is admitted again at the generation it
  already had, while a duplicate publish of a visible snapshot stays a no-op.
- `ConcurrencyGauge` models principal-then-account acquisition, rollback when
  the account is full, exact ceiling preservation, and single release. Rust's
  ownership transition from `Admitted` to `CommittedAdmission`, and from there
  into `ChargeGuard`, separately enforces that execution start transfers the
  funding reservation without releasing its concurrency permit. The model
  proves that transfer retains occupancy and that only finishing either the
  pending or execution owner releases it; Rust's private fields, behavior
  witness, and compile-fail witness connect that model to the API.
- `Conservation` models the per-account ledger equation
  `deposited + overage = balance + activeGrants + settledUsage + loss` and
  proves each transition preserves it: deposit, acquire, release, reclaim, a
  straggler on a settled lease, and the overage ingest issue #1 adds. It also
  proves the negative that earns the funding column — billing overage without
  funding it *always* breaks the equation, by exactly the overage — and that an
  accepted debit never carries the counter past its cap.
- `OveragePublication` models the observer-visible split between pending and
  committed overage and the publication marker around the reservation phase
  CAS. It proves an in-flight commit is never reported as refundable, stable
  refundable/committed-saturation answers agree with committed occupancy, and
  commit, cancellation, and a lost commit claim preserve occupancy bounds. It
  intentionally does not infer central account exhaustion from local cap
  saturation.
- `RatePublication` models accepted account-policy publication. It proves that
  a rejected snapshot cannot mutate account state, newer accepted generations
  select their complete account-wide policy, same/older generations retain the
  current policy, and every principal request reads the one current rate
  authority rather than retaining a principal-local copy.
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
internals, the sharded CAS/fixed-receipt algorithm, the maximum-weight scan,
finite-width overflow, SQL, or the derivation of settled usage from recorded
usage and active lease usage; those remain separate Rust obligations. In
particular a green `formal` job says the equation closes under each modeled
transition — not that either backend performs them atomically, nor that the
client applies the same rules the admission map does.
