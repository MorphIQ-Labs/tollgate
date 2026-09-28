# Tollgate Formal Models

This Lean package contains exact models for critical contracts:

- `LeaseTiming` proves exact integer-pair encoding and ordering, equivalence
  of the reclaim cutoff to expiry plus full grace, safe underflow/overflow
  behavior, and conservative bounds for migrated legacy timestamps. Rust
  property tests and PostgreSQL migration tests separately witness the finite
  timestamp domain, driver representation and database compatibility fence.

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
  ownership transition from `Pending` through `ReadyToStart` into `Committed`
  separately enforces that execution start transfers the funding reservation
  without releasing its concurrency permit. The model
  proves that transfer retains occupancy and that only finishing either the
  pending or execution owner releases it; Rust's private fields, behavior
  witness, and compile-fail witness connect that model to the API.
- `LeaseFencing` models lease capabilities: a store of leases keyed by ID with
  a per-account fence counter starting at one. It proves that fences are
  positive, unique per account and increasing; that acquiring a lease leaves
  every existing lease unchanged, so the sequence is an audit order and not a
  validity epoch; that release and ingest refuse a mismatched `(lease, fence)`
  pair or `(lease, account, fence)` triple; that each operation changes only
  the lease it names; that a settled lease is never active again; and that
  billed plus returned units never exceed the grant. Expiry reclaim is
  modeled as forfeiture. Operations are atomic by assumption; the store
  suites' capability tests witness the Rust and SQL.
- `IdempotentIngest` models batched usage ingest keyed by request ID, with
  acceptance left as an arbitrary decision over the ledger planned so far. It
  proves that every input is classified exactly once; that a duplicate is
  recognized from its ID before its payload or the decision is consulted;
  that the ledger only appends accepted events, so no settled event is
  replaced and no request ID is billed twice across any replay; that the
  billed total grows by exactly the accepted units; that a rejected event
  does not claim its ID; and that a failed batch changes nothing. Batch
  atomicity is an assumption; the mirrored store suites witness it.
- `ChargeLifecycle` models a request's charge lifecycle on one instance: a
  list of requests sharing a lease and a usage queue of fixed capacity, moving
  through reserve, admit, cancel, commit, emit and deliver. It proves that the
  slots in use never exceed capacity and a full queue sheds with no charge;
  that nothing is charged before commit and cancellation refunds exactly the
  admitted debit, so the lease is conserved; and that a committed request
  holds the slot bound at reservation, so its event is emitted without a
  capacity check, once, with the charge fixed at commit. Queue lanes, the
  drain deadline, sharded counters and process loss are outside the model.
- [`Conservation`](Tollgate/Conservation.lean) models the per-account ledger equation
  `deposited + overage = balance + activeGrants + settledUsage + loss + expired`,
  where `balance = allowanceBalance + topupBalance`, and
  proves each transition preserves it: deposit, acquire, release, reclaim, a
  straggler on a settled lease, overage ingest, and budget rollover. It also
  proves that billing overage without funding it, or discarding an allowance
  without recording its expiry, breaks the equation by exactly the omitted
  units. An accepted overage debit never carries the counter past its cap.
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
- `CommitFallback` models single-transition commit funding and its overage
  fallback, separating terminal accounting outcomes from request ownership.
- `ExecutionCapacity` models conservation across shared and reserved execution
  pools and excludes best-effort work from the assured reserve.
- `AccountLifecycle` models at most one account-manager owner, retirement before
  replacement, and inert duplicate joins.
- `PeriodRoller` models bounded independent maintenance: one pending call,
  a fixed cutoff per pass, and terminal stopping.
- `ServerMaintenance` models independent reclaim/rollover outcomes and terminal
  owner stop or task exit. It proves that the other operation cannot conceal a
  failure and that later successes cannot restore readiness after stop/exit.
  Watch-channel closure, task cancellation, HTTP delivery and finite counters
  remain separate implementation obligations.
- `ControlPlane` models disjoint role evidence and serialized administrative
  receipt composition, whole-value replacement predecessors, and credential
  retirement identity preservation, idempotency and terminality. It assumes
  credential authenticity and atomic policy
  selection; it does not prove cryptography or database serialization.
- `CredentialProjection` models exact integer-time expiry intersection and
  complete table replacement. It proves that fetch delay consumes freshness,
  failure preserves original deadlines, and empty success withdraws all keys.
  The bounded fixed-catalogue drain has no omissions or duplicates; mixed
  revisions preserve the predecessor. Cached evidence retains its original
  bound. Ordered cursor refinement, coherent source reads, atomic
  publication, verified input and accurate clocks are assumptions. Its legacy
  expiry model proves that migration's earliest compatible timestamp cannot
  extend source authority, including the zero bucket and domain minimum;
  a reset session inherits that bound. Rust tests
  separately exercise timestamp overflow, transport and session behavior.

Run from the repository root:

```sh
./scripts/check_formal.sh
```

The models use exact natural-number or integer arithmetic and atomic transitions. Rust
property, cache, manager, HTTP, and backend-parity tests establish the
proof-to-code argument. The proofs do not model scheduling, data-structure
internals, the sharded CAS/fixed-receipt algorithm, the maximum-weight scan,
finite-width overflow, SQL, or the derivation of settled usage from recorded
usage and active lease usage; those remain separate Rust obligations. In
particular a green `formal` job says the equation closes under each modeled
transition — not that either backend performs them atomically, nor that the
client applies the same rules the admission map does.
