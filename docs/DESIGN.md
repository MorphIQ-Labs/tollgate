# tollgate design

Status: proof of concept, complete through the goals below. This document is
the architecture, the decisions made against the originating design thread,
and the findings the PoC produced. The testable contract lives in
`INVARIANTS.md`; this file explains *why*.

## Problem

A latency-sensitive API service (an options-pricing API is the motivating
consumer, with a microsecond-scale in-process request budget) must enforce
account policy, rate limits, quota, and billing **without any synchronous I/O
on the request path**. One database round-trip per request would be two to
three orders of magnitude over budget.

**Measurement context.** Absolute timings in this document are host-dependent
Criterion results. Unless a passage names another host, they were measured on
the Apple M1 Pro development laptop that later became the controlled
performance host. The same-run ratios are the portable claims;
[`PERFORMANCE.md`](PERFORMANCE.md) records the procedure and its limits.

## Architecture

Two planes. The request path is entirely local; the control plane owns every
slow operation and the source of truth.

```
request path (tollgate-core + tollgate-admission, zero I/O; policy time passed in, governor reads its own monotonic clock)
  per-connection exact-header proof → verified credential fingerprint (Principal)
    → snapshot map lookup (arc-swap immutable map or moka)
    → status / staleness / permission bits        (AccountSnapshot::admit)
    → batch cap + cost quote                      (CostTable, direct-indexed)
    → weighted rate token                         (governor; optional local shards)
    → lease debit → Reservation                   (LocalLease, optional local CAS shards)
    → commit-at-execution-start | release
    → usage event via pre-reserved permit         (bounded channel)

control plane (tollgate-client tasks + tollgate-server + tollgate-store backend)
  lease acquire/release/reclaim (lease-scoped capability, TTL-bounded, adaptive grants)
  snapshot compilation & distribution (generation-monotonic)
  usage ingest (idempotent, batched) → billing ledger
  expiry reclaim sweeps
```

The store is three narrow async traits — `LeaseAllocator`, `SnapshotSource`,
`UsageSink` (plus `AdminStore`) — implemented by `MemoryStore` (reference,
executable spec), `PostgresStore`, and `HttpStore` (the client-side transport
to `tollgate-server`). The same `LeaseManager`/`UsageWriter` instance runtime
runs unchanged over a direct backend or over HTTP: both topologies are
exercised by the same end-to-end drain test (`tollgate-client/tests/
no_double_spend.rs` and `tollgate-server/tests/loopback.rs`).

HTTP acquire and consolidation share `wire::LeaseTtl`: it preserves the full
positive `SignedDuration` value, with legacy whole seconds or an exact duration
string and a zero legacy sentinel. Positivity and unambiguous decoding are
checked before allocation; the backend still owns TTL policy clamping and the
server still supplies `now`. See the operator TTL compatibility section in
`CONTROL_PLANE_SECURITY.md` for the server-first rollout and Rust DTO migration.

`tollgate-server`'s library remains generic over those store traits. Concrete
backend selection belongs to its binary: memory support is always compiled,
while Postgres support is the default-enabled `postgres` Cargo feature. An
embedder using the generic service with its own store can disable default
features without compiling `tollgate-store-postgres` or `sqlx`.

`SnapshotSource` resolves each pull explicitly as `Present(snapshot)`,
`Revoked(generation)`, or `Unknown`. HTTP preserves the distinction: 200
carries a snapshot, 410 carries a generation-ordered revocation, and 404 means
the source has never known the principal. A pull or process restart therefore
cannot discard a tombstone generation.

A present resolution carries `PublishableSnapshot`, a typed proof created by
the owning validation in `tollgate-core`, rather than an unchecked
`Arc<AccountSnapshot>`. Admin writes require the same proof. Wire and database
records remain raw snapshots because they are trust boundaries; the server,
HTTP client, and Postgres reader validate them before converting them into the
proof. This makes backend-specific publication without validation
unrepresentable while preserving raw `AccountSnapshot` construction for the
request path's defensive backstop.

The explicit three-state resolution was an intentional Rust trait break and
an additive problem-body field, plus a 404-to-410 status change for revoked
principals. For a rolling HTTP deployment, update clients before servers: new
clients safely understand both the old 404 and new 410 behavior, while old
clients classify 410 as a retryable store failure and may retain a positive
until its own validity deadline.

### Portable identifier wire contract (GL-24, 2026-08-24)

The original v1 transport had two representations for the same opaque value:
all five 128-bit id newtypes displayed as zero-padded hexadecimal, while URL
paths bypassed `Display` and JSON serialized the inner `u128` as a decimal
number. The default Rust JSON number type rejects values above u64, and a
conventional JavaScript consumer cannot preserve even valid JSON integers
above 2^53. The path mismatch was also a refactor trap. Axum 0.8 actually
rejects an unparseable `Path<u128>` as 400 rather than the 404 first reported,
so the current client did surface that particular mismatch as a store error;
the broader contract and portability defects remained.

V1 now gives `AccountId`, `KeyId`, `LeaseId`, `RequestId`, and `Principal` one
spelling everywhere: exactly 32 lowercase hexadecimal characters, no `0x`.
The owning types implement `Display`, strict `FromStr`, and textual Serde from
the same rule; server paths extract those types rather than raw integers.
Malformed bodies and paths remain RFC-7807 responses. `HttpStore` accepts a
negative snapshot resolution only from a structured `404` carrying
`unknown-principal`; a route-level 404 or any other code is an operational
failure and cannot poison the negative cache.

This deliberately updates `/v1` in place because Tollgate's HTTP API has not
been published yet. The Rust Serde and HTTP contracts are breaking for the
same reason, so clients and servers must be deployed from the same revision;
numeric and textual peers are not wire-compatible. `HttpStore` treats an old
server's unstructured route 404 as an error rather than confirmed absence.

No database migration accompanies the wire break. PostgreSQL snapshots own a
storage-specific id DTO: values in the legacy u64 range stay numeric, while
larger values use the canonical string that the former JSON codec could not
represent. Existing rows remain readable, and a rollback binary can still read
new rows whose identifiers remain in the legacy range. Larger textual values
deliberately require the new reader. This exception is owned at the database
trust boundary rather than hidden inside the canonical id parser.

## Charging semantics

Charging follows a fixed-charge-once-per-request contract
(`commit_at_execution_start`): admission debits the lease immediately but the
charge is only *pending*; execution start commits the full quote for success,
failure, or timeout alike; anything ending the request before execution —
validation failure, cancellation, shed, drop — releases for **zero** charge.
Commit and cancel race on a single compare-exchange; exactly one wins.

## Two ledgers, one truth

Leases **bound** spend (admission control); usage events **are** the billing
record. The [ledger contract](../INVARIANTS.md), implemented by
`Conservation::holds` for both backends, states the exact per-account equation:

```
deposited + overage_recorded
    == balance + active lease grants + settled usage + settlement loss + expired
```

Usage on an active lease lives inside its grant; it stands alone only after
the lease settles. The backend suites assert this equation after ledger
transitions.

The left side is *funding*, the right side is where funded units sit — which
is why elastic enforcement (GL-1) adds its term on the left. Overage is spend no
deposit paid for and no lease debited, and it is billed like any other usage,
so it also lands in settled usage. Recording only that half would make the
equation fail by exactly the overage and report corruption on a correctly
working ledger; recording both halves in one transaction closes it by
construction. `expired` accounts for funded allowance units a closed budget
period removed (GL-97); they remain part of the ledger even though they are no
longer spendable or billable. `Tollgate.Conservation` proves the modeled
transitions preserve the equation, and that omitting overage funding or expiry
accounting breaks it by exactly the omitted units.

## Findings (what the PoC changed or proved)

1. **Rotation must not strand units.** Naïve low-water rotation stranded ~33%
   of an account in superseded leases until TTL. Fix: the manager parks a
   superseded lease and releases it only at *quiescence* — when it holds the
   only outer `Arc` and no locality alias remains, no reservation exists and
   none can be created, so the remaining count is final and the release is
   race-free.
2. **Settlement must tolerate release-before-flush.** A quiesced lease can be
   released while its usage events still sit in the writer's queue. A release
   credits only the claimed `unspent`; the gap `granted − used − unspent` is
   *provisional* loss, and a late event that fits inside it converts to
   billed usage. Expired-lease reclaim credits the whole remainder, so its
   stragglers never fit and stay rejected — no double-count either way.
3. **arc-swap beats moka for the snapshot map, ~5×** (13.9 ns vs 69.5 ns hit
   on the development laptop), confirming the thread's caution about moka's TinyLFU
   bookkeeping. Both stay behind `SnapshotMap`; the perf gate carries both,
   and both hash principals the same way (GL-9) so the ratio keeps measuring
   the data structures rather than their hashers. **That ~5× was measured on a
   cache one-eighth full, where moka enables neither its frequency sketch nor
   eviction** — see "The map choice was measured with moka's bookkeeping
   switched off" below. Filling the cache costs moka more than arc-swap, but
   the gap does not widen: it measured ×6.40 under-filled against ×5.04 at
   capacity on the controlled host.
4. **Same-account cross-core contention needs an explicit local topology.**
   The former one-counter layout took 2–4 µs with eight threads against one
   account versus roughly 100 ns uncontended. `LocalSharding` now partitions
   the mutable admission state and cache-isolates request-owned handles; the
   default remains one shard. The perf gate compares the eight-thread and
   uncontended paths from the same run and requires a ratio at or below ×3.
5. **Sequential and contended end-to-end overhead meet separate gates**:
   across five development-host production-profile loopback repetitions, the
   paired admitted-vs-baseline p50 ratio ranged ×0.997–×1.101 for one
   persistent connection (gate max ×1.15) and ×1.015–×1.104 for 10 persistent
   connections contending on one account (gate max ×1.20). Those measurements
   predate GL-2, which is why they showed most of the sequential delta as per-request
   HMAC-SHA256 credential verification, not quota machinery. Pricing-api now
   authenticates once per connection and reuses the verified fingerprint only
   while the complete authorization header remains exactly equal; paired
   post-change measurements are recorded with GL-2's threshold decision.
6. **The quota edge needs adaptive grants**: `GrantPolicy` caps a grant at
   `balance / shrink_divisor` (floored at `min_grant`), so N instances can't
   strand a small balance behind one oversized lease; the tail drains to the
   last unit.

## Instance-local admission sharding (GL-3, 2026-08-24)

Sharding is explicit and opt-in. `LocalSharding::SINGLE` is the compatibility
default; an embedder that has measured real same-account saturation supplies
the same value to `ArcSwapSnapshotMap::with_sharding` (or the Moka equivalent)
and `SlotRegistry::with_sharding`. `LocalSharding::available_parallelism` is a
control-plane convenience, not an automatic policy. The pricing example also
accepts `TOLLGATE_LOCAL_SHARDS`; an invalid or zero value is a startup error,
never a silent fallback. A deployment should normally start with its worker
thread count and validate memory and latency on its controlled host.
`SnapshotManager::spawn` rejects a map/slot mismatch before starting either
background work or admission publication.

The root cause was broader than the lease counter named in the issue. The
lease CAS, governor state, admission tally, and several shared Arc strong-count
words all bounced between the same-account workers; splitting only the lease
improved the provisional benchmark from 2.64 µs to 2.29 µs. The false-sharing
assumption also said 64-byte lines on aarch64, while the supported Apple
Silicon host reports 128. Existing safeguards missed the pattern because the
uncontended and distinct-account gates cannot create it, and the contended
benchmark had only a loose provisional absolute ceiling rather than the
same-run ratio the issue required. The prevention is structural locality plus
128-byte layout witnesses and a gating contended/uncontended ratio. A
same-pattern search across the workspace found one other 64-byte assumption,
the request-written `UsageWriter` counters; it is corrected and layout-tested
in this change. No other `repr(align(64))` site remains.

The locality is a sticky process-local number assigned once per participating
OS thread. It is deliberately not a CPU id: Tokio tasks can move and operating
systems migrate threads. Reducing that value modulo each effective shard count
is enough to keep a worker's routine writes on stable cache lines. Keeping them
on lines *no peer writes* is a further condition, and GL-124 records that it is
**reported rather than enforced** — a deployment responsibility, because the
library does not choose how many threads serve requests. All mutable hot-path
components use it:

- A lease grant and its low-water threshold are quotient/remainder partitioned
  exactly across 128-byte-aligned counters. Debit tries the local counter, then
  steals a whole debit from siblings. Only genuine fragmentation takes pieces
  from several counters. Its fixed-size receipt records the exact total and one
  refund shard; cancel and a failed gather may rebalance counters while
  restoring the aggregate exactly. No debit path allocates. Aggregate remaining
  is exact at quiescence, which is when release uses it; a *live* read walks
  shards one at a time while others move, so it is a bounded estimate in both
  directions and is clamped to the grant rather than asserted against it.
- A sharded `LeaseSlot` publishes an independently reference-counted outer
  lease view per locality. Those views share the accounting counters, but Arc
  ownership traffic stays local. Publishing N views is N swaps, so mutators
  hold a publication lock: readers still straddle one publication as they
  always did, but the slot cannot *end* one holding two different leases.
  Rotation removes all published views and releases only after the parked
  outer handle and the shared inner view count both prove quiescence, so an
  in-flight sibling cannot be missed.
- The account rate and burst are partitioned, never copied, across independent
  governor buckets. The already-validated `PublishableSnapshot` carries its
  maximum quote into the map; the effective shard count is capped so every
  bucket can admit that largest legitimate request. The account-wide cap is
  the tightest ceiling supplied by accepted principals. An older account-policy
  generation can still tighten it, and it never widens. A rejected principal
  replay cannot affect that ceiling. A local denial tries each sibling and calls a
  request unpriceable only when no bucket can hold its weight. If a bucket can
  hold it but none admits now, the weighted refusal is `RateLimited`. Both rate
  refusal variants classify as `Retry::Transient` and expose no retry instant
  or delay; governor's internal denial timing does not cross the admission API.
  Independent buckets can conservatively throttle when
  their residual tokens are fragmented, but their summed rate and burst never
  exceed the configured instance-local account budget. Raw snapshot installs
  retain one bucket because they carry no publication proof.
- Admission state, immutable snapshots, lease handles, rate state, and outcome
  counters are cache-isolated on 128-byte Apple Silicon lines (harmlessly
  over-aligned on 64-byte x86-64). Sharded maps keep one state/snapshot Arc per
  locality so reference-count writes do not recreate the bottleneck after the
  accounting atomics are split. A stored entry stays a shard array until the
  engine's already-resolved locality indexes it, so a cache that clones on its
  write path cannot fix the choice to whichever thread installed the entry.
  Counter snapshots and lease-manager operations aggregate on the control
  plane.

Low-water notification is shard-local because computing an aggregate on every
debit would put an O(shards) scan back on the request path. One local crossing
wakes the manager early. If the aggregate is not low, the manager clears every
doorbell and rechecks the sum; a debit racing the clear is therefore included
in the recheck or observes a clear flag and wakes again. The interval remains
the cold-start and expiry backstop.

The exact natural-number conservation argument lives in
`formal/lean/Tollgate/LeaseShards.lean`. Rust property tests vary the shard
count and finite-width request sequence; concurrent, rotation, shutdown, and
fault-path tests establish the proof-to-code bridge. No wire DTO, database
schema, or existing constructor changed semantics. The public changes are
additive, though opt-in sharding deliberately spends more per-account memory.

The gated benchmark ids name the shipped default, and the `_sharded` ones
price the option: `admission/full_check` and `admission/full_check_contended_8`
keep measuring the single-counter layout they were calibrated against, while
`admission/full_check_sharded` and `admission/full_check_contended_8_sharded`
measure the eight-shard one. Pointing the existing ids at the opt-in topology
would have left the layout nearly every deployment runs with no threshold at
all, and would have redefined two manifest entries in place.

Measured on the development host at load average 17–26, which is why these
are backstops rather than calibration: 103–110 ns default uncontended,
116–124 ns sharded uncontended (the price of the locality read and shard
index), 2.74–2.80 µs default contended, and 446–618 ns sharded contended. The
portable same-run ratios are what the manifest gates: sharded-contended over
default-contended (measured 0.17, bound 0.5) is what sharding buys, and
sharded-contended over sharded-uncontended (measured 4.09, bound 8.0) is the
sharded layout's own contention budget — wider than an idle host would need,
because on a machine this loaded the foreground measurement competes with far
more than its seven background threads.

### Review round: what sharding broke that single-counter code could not (GL-3)

Four defects shared one shape — a claim that held for one counter, one view,
or one snapshot, restated unchanged over N of them.

- **An aggregate read asserted a bound it no longer had.** `remaining()` summed
  shards with `checked_add(..).expect(..)` and a `debug_assert!` against the
  grant, three lines below a doc admitting the walk is not one atomic instant.
  A failed fragmented reservation returns its whole aggregate to *one* shard
  (the fixed-size receipt), so a reader that has already counted a shard the
  refund lands behind counts those units twice. That panics: the assertion in
  any debug or test build, the `expect` in release once grants approach
  `u64::MAX` — which the change's own boundary test treats as legitimate. The
  callers are the request path's `LeaseExhausted` reason, the refill task, and
  the example's readiness handler. Fixed by making the estimate honest:
  saturating sum, clamped to the grant, documented as an estimate in both
  directions and exact only at quiescence.
- **A per-snapshot bound sized an account-wide bucket.** Shard count came from
  the installing snapshot's maximum quote, but `AccountLimiter` is shared by
  every principal of the account and `update` returned early at an
  already-installed generation. A key whose cost table quoted more than one
  shard's burst — while still fitting the account's whole burst, so
  publication accepted it — was denied `UnpriceableUnderLimits` permanently,
  for a request that admitted before sharding existed. The ceiling is now
  account-wide, tightened by every install regardless of generation ordering
  and never widened; the alternative, per-principal evidence, would need its
  own eviction story for churned credentials, and holding the floor costs only
  a coarser split.
- **A publication documented as atomic was N swaps.** `LeaseSlot::take` said it
  "atomically" removed every view; `install`, `replace`, and `clear` were
  equally multi-step. Two mutators — an embedder clearing on revocation while
  the refill plane rotates — interleave into a slot that is half empty and half
  fresh, so some localities keep spending after a revocation the single-view
  slot made indivisible. Only latent in shipped code, which has one writer per
  slot, but `LeaseSlot` is public API. Mutators now serialize.
- **The moka map's sharding was inert.** Its entry resolved locality inside
  `Clone`, and moka clones on its *write* path: the entry was already reduced
  to the installing thread's shard before any request saw it, so every worker
  shared one state and the `get_at` locality argument was ignored. Discovered
  by the test written for the smaller complaint that `get_at` was not
  overridden. The stored entry now stays a shard array — behind one `Arc`, so
  the clone stays a single refcount bump — and `get_at` indexes it. Resolving
  from an owned entry keeps the single-shard path free of the extra
  clone/drop pair that would otherwise land on every lookup.

The common prevention is the same in each case: a test that exercises N of the
thing. The perf gate had the same shape of gap — the change had pointed its two
gated full-pipeline ids at the opt-in topology, leaving the default layout with
no threshold — and is fixed the same way, by gating both.

## Lease-scoped fencing contract (GL-35, 2026-08-24)

The word *fencing* originally overstated the store contract. Invariant GL-4 and
the first `FencingToken` API documentation were written before either store
backend and described an account-wide epoch: any token older than the newest
issued token would be stale. The stores subsequently implemented a different,
deliberate model. Rotation acquires a replacement before the prior lease has
quiesced, so several leases for one account can be active at once. Each keeps
its own capability until settlement; immediately invalidating the older lease
would break in-flight reservations and the quiescence-gated release protocol.
There is no lease-renewal operation.

The enforced contract is therefore exact and lease-scoped. Tokens are drawn
from a strictly increasing per-account sequence, which supplies allocation and
audit order without acting as an account validity epoch. Release matches the
stored `(lease_id, fencing_token)` pair. Usage ingest matches the stored
`(lease_id, account_id, fencing_token)` triple before applying the independent
lease-state and conservation-capacity checks. A later grant never invalidates
an older active grant; settlement or reclaim makes the lease unusable, and a
matching capability cannot revive it. The capability is evidence that the
operation names the stored grant, not proof that its token is globally newest.

The misleading contract survived because `fenced_out_holder_rejected`
reclaimed the first lease before allocating its replacement. Its rejection
proved only that a wrong token is refused and that a reclaimed lease stays
settled; it never exercised two simultaneous active leases. The backend suites
now name those obligations separately and mirror witnesses for concurrent
active grants, wrong-token and wrong-account usage, settlement, reclaim, and
capacity. `Fenced` remains the stable error/code for a token mismatch. When a
client receives it for a token copied from its grant, clearing the current slot
is a conservative fail-closed response to local/store divergence, not evidence
that a newer account holder superseded the lease.

## Failure behavior

Fail closed, always locally: unknown principal (negative-cached), suspended/
closed account, stale snapshot, missing permission, exhausted/expired/absent
lease, cost overflow, accounting backpressure — all deny with zero units and
zero I/O. Recovery is the background planes' job.

`EnforcementMode::Elastic` moves exactly one item off that list, per account:
a lease that cannot fund the quote. The other conditions stay absolute under
every mode, because a lease refusal is the only one of them that says
something about *funding* rather than about validity. An elastic admission is
not a fall-through to a slower path — there is still no I/O, and the elastic path adds no clock read —
it is a debit against a different local counter, and the resulting units are
recorded as overage rather than forgiven. The counter distinguishes committed
spend from pending reservations because the retry contract depends on that
boundary. Reservation phase and aggregate committed occupancy are separate
atomic words, so `AccountOverage::publish_commit` installs a publication
marker before it invokes the phase CAS and removes it only after committed
occupancy advances. `OverageCommitInProgress` is the observer-visible answer
inside that interval; it is `AfterInFlight` and maps to 503 because retrying
after the publication settles obtains the stable answer without promising
that answer will admit. `OverageCapTemporarilyExhausted`
then means refundable pending occupancy is what prevents this request from
fitting; cancellation or drop can recover, so it is transient and maps to 503.
`OverageCapExhausted` means this request still would not fit in the local
overage allowance after every pending reservation refunded. That is a stable
statement about this instance's overage counter, not evidence about the
account's central balance. Admission reaches it only after the installed lease
was unavailable, expired, or exhausted, and an ordinary background grant can
fund the unchanged request from existing balance. It is therefore transient
and maps to 503; claiming payment is required would cross the request/control
plane boundary without evidence.

The earlier counter stored only total occupancy. That was sufficient to prove
the cap bound, but not to tell why a request hit it. Tests covered committed
exhaustion and cancellation refunds separately, so none asked for the retry
classification while the first reservation was still pending. The account
counter now retains committed occupancy beside the total; reservation commit
advances it only after winning the same commit/cancel CAS, and refusal compares
the requested units against committed occupancy to decide whether refunding all
pending work would help.

That first two-counter correction still left one invalid intermediate state:
the reservation CAS could publish `COMMITTED` and the thread could stop before
the aggregate counter advanced. The concurrency witness originally checked
only after both racers joined, so it could not observe the interval. Reordering
the writes merely reverses the defect — committed occupancy would then include
units cancellation could still win — and spinning on a descheduled request
would violate the bounded hot path. The publication guard is therefore owned
by `AccountOverage`, and its method owns the caller's phase-CAS closure: the
required order is encoded in the API rather than repeated as caller
convention. A refusing observer uses a zero-delta RMW rather than a load, so it
is ordered before, within, or after the marker's modification interval instead
of accepting a stale zero that skips an already-linearized publication start.
The marker is a third lock-free atomic; it adds no allocation, I/O, clock read,
or blocking lock to admission. `formal/lean/Tollgate/OveragePublication.lean`
proves that an active publication is never classified as refundable, that
stable refundable/committed-saturation answers agree with committed occupancy, and that
commit, cancellation, and a lost commit claim preserve occupancy bounds.

The deny vocabulary distinguishes *transient* from *terminal*, because the two
call for opposite responses (GL-40). `RateLimited` means the bucket is
momentarily empty and refills; `UnpriceableUnderLimits` means the quote
exceeds the account's whole burst, so no wait can help — a schedule whose
batch cap admits a request its burst cannot hold. The engine decides this in
full width against the configured `rate_burst_units` *before* narrowing the
weight into governor's u32 bucket, so narrowing cannot disguise the terminal
case as the transient one, and the bucket-construction clamps in
`build_limiter` never decide a verdict. Since `tollgate-admission` carries no
logging dependency by design, the deny reason is the only channel this
condition has.

### Snapshot limit validation and rollout

Publication computes one worst-case quote from the largest registered
per-item weight:

```
max(fixed_request + max_weight * max_items_per_request, minimum_charge)
```

The fixed-width operations are checked. Before that scan, both weighted-rate
scalars are checked in full width against governor's non-zero `u32` domain;
zero or a wider value refuses publication rather than being repaired by the
request-path defensive narrowing (GL-66). The carried pair is checked even when
the new weighted-rate flag is disabled because a pre-GL-91 reader still enforces
it during rollback. Overflow or a result greater than the carried
`rate_burst_units` refuses publication; equality is valid. A table with no
registered operation is valid only after the rate-domain check because no
request can obtain a quote from it, but the configuration still has to be safe
for every reader that accepts it.
Selecting the largest weight is an O(n) control-plane scan, while request
quoting remains direct-indexed O(1) with no new request-path work. The exact
Lean model proves that the largest weight at the cap bounds every registered
operation at every permitted item count; a Rust property test separately
checks the maximum scan and `u64` behavior against a `u128` oracle.

The policy is strict refusal, not an implicit burst override: configuration
values are operator contracts, and silently raising the burst would change
the plan's rate semantics. The admin API returns 422 with stable problem code
`invalid-snapshot-limits`. `UnpriceableUnderLimits` remains in the admission
engine as defense in depth for a raw snapshot embedded directly by a caller.

This changes the public Rust store API (`AdminStore::publish_snapshot` and
`SnapshotResolution::Present`) but not the wire DTO or database schema. For a
rolling deployment, first audit stored and generated plans with the new
validator and publish corrected snapshots at a higher generation, then
upgrade every server/admin writer and client before relying on the invariant.
Mixed versions are safe only in the fail-closed sense: an older writer can
still persist invalid data, while a new Postgres reader or HTTP client refuses
it. An already cached older snapshot remains usable only through its existing
`valid_until`; if no corrected generation arrives, continuous readiness falls
and admission denies. Recovery is to correct the plan and publish a higher
generation. No SQL migration or automatic data rewrite is performed.

Two deliberate consequences:

- **Cold start and degradation**: an instance requires its snapshot set and
  healthy background tasks. Strict funding also requires a usable lease;
  Elastic may serve from overage headroom before the first grant arrives.
  Readiness (`/readyz`) continuously reflects snapshot
  freshness and task health, the refill task, current lease capacity/usability,
  and the accounting writer — it is not a one-way startup latch.
- **Backend outage**: instances drain their leases, then deny; the usage
  writer retries with backoff while the bounded queue sheds new work
  upstream with zero charge. Memory stays bounded; billing events are never
  silently dropped. For an elastic account the "then deny" becomes "then
  extend credit, then deny": the outage window is bounded by `overage_cap` per
  instance rather than by the lease, which is the trade the mode exists to
  make. Note this is the case that multiplies fastest across a fleet, since an
  outage reaches every instance at once.

## Observability

Every failure mode in this design is deliberately silent and deliberately
recoverable: the system denies, the background plane repairs, nothing throws.
The difference between "working as designed" and "broken for twenty minutes"
is therefore entirely a matter of reading the right signal at the right
threshold, and those thresholds cannot be inferred from the metric names by
anyone who does not already know the two-plane architecture. This section is
that knowledge. Why the request path and the accounting plane report the way
they do is in the dated entries below (GL-37, GL-38).

### The three surfaces

| Surface | Where it comes from | Read it with |
|---|---|---|
| Admission counters | `AdmissionEngine::counters()` — per outcome, keyed by `DenyReason` | `AdmissionCounters::snapshot()` |
| Accounting health | `UsageRecorder::health()` / `UsageWriter::health()` | `WriterHealth`, including the `WriterStats` a shutdown would return |
| Refill counters | `LeaseManager::counters()` — acquires, and refusals keyed by `AllocateError` | `LeaseCounters::snapshot()` |
| Snapshot counters | `SnapshotManager::counters()` — refresh outcomes, generation refusals, unresolved principals | `SnapshotCounters::snapshot()` |
| Control-plane events | `tracing` events from the client and server crates | any subscriber; `RUST_LOG` selects |

Each background plane exposes its counters as a shared handle, because the
manager itself is normally moved into whatever owns shutdown while a service
keeps the counters in its request state. They outlive their task, so a reader
keeps working after a plane dies.

The counters and health reads are library API. `examples/pricing-api` publishes
them as JSON on `/metrics`, which is **one way to expose them, not a product
surface** — an embedder chooses the format. Field names below are that
example's, and match the library field names one for one.

Both counter sets are **per instance**, like the rate limiter: they say what
this process admitted, refused and billed, and a fleet view is the scrape's
job to aggregate. Neither survives a restart.

### How events are chosen

The libraries emit `tracing` events and never install a subscriber — that is
the embedding binary's decision. Two rules shape them. **Level follows
consequence, not cause**: a lease acquire refused during ordinary rotation is
`debug` because the instance keeps serving, while the same refusal against an
empty slot is `warn` because every request is now denied. And **an outage is a
duration, not an event** — the writer reports entering one once, stays quiet at
`debug` while it persists, and reports recovery with how long it lasted and how
many attempts it took, because `WriterStats` shows a recovered outage as a
perfectly clean run.

`tollgate-core` and `tollgate-admission` are deliberately excluded: they gain
no logging dependency, exactly as they take no I/O, which is why a
request-path condition must be expressed in the deny vocabulary instead (see
`UnpriceableUnderLimits` above) and why the request path reports itself with
counters rather than events. The gate binaries (`check_benchmark_thresholds`,
`load_gate`) keep `println!`: their stdout is a report consumed by CI, not
diagnostics.

Events carry identifiers, never secrets: accounts, principals and leases are
opaque newtypes, and a connection string is reported with its userinfo
stripped before it can reach a log line.

### Signals worth a response

Each of these looks benign in isolation, which is why each is written down.
"Sustained" means across several scrape intervals — every one of these can
tick once under normal operation.

**`denials.lease_exhausted` rising while the account still has balance.**
Not enforcement — a refill-latency artifact. The instance spent its lease and
the background manager has not yet acquired the next one, so requests are
denied against an account that is funded (issue GL-10). Normal is zero outside
brief rotation windows. Sustained means the refill loop cannot keep up with
demand or cannot reach the allocator, and `refill.refusals` says which:
`insufficient_balance` is a genuinely empty account, `storage` is a backend
this instance cannot reach, and `refill.acquire_timeouts` is an allocator too
slow to answer — which is not a refusal at all, since it may well have granted
the lease and failed to say so. **Do not** route this to the customer as a
quota problem unless the breakdown says `insufficient_balance`; otherwise they
have paid for capacity they are being refused.

**`admitted_overage` rising, on an elastic account.** Not an error — it is the
mode working — but it is the leading indicator of an invoice, the way
`accounting.rejected` is the leading indicator of billing loss. It says this
instance is admitting work the account has not paid for. Read it against
`total_overage_spent` / `total_overage_cap`: the ratio is how much runway is
left before a cap refusal starts. **Both are per instance.** Fleet exposure is
the cap times the number of instances, so a cap that looks conservative on one box is
not, and a control-plane outage reaches every instance at once. Restore lease
capacity first; add funding only when the allocator specifically reports
`insufficient_balance`. Raising the cap only buys time.

**`denials.overage_cap_temporarily_exhausted` above zero.** Pending overage
reservations currently occupy the headroom this request needs. Cancellation or
drop can return that credit without a funding or policy change, so the refusal
is transient and maps to 503. Sustained activity means work is staying pending
long enough to saturate the cap; inspect queue and execution-start latency
before treating it as an account balance problem.

**`denials.overage_commit_in_progress` above zero.** A request reached a full
cap while another request was publishing its commit decision into aggregate
occupancy. The refusal maps to 503/`AfterInFlight`: retry after that decision
settles, then act on the stable refundable or committed-saturation answer. A small
count under contention is expected; sustained activity means committers are
being delayed inside the short execution-start transition, so inspect CPU
starvation and execution-start scheduling before changing account funding.

**`denials.overage_cap_exhausted` above zero.** This instance has spent enough
non-refundable credit that the request would not fit even if every pending
reservation cancelled. It is distinct from refundable saturation so operators
can see that cancellation will not recover local overage headroom, but it is
still a transient 503: a background lease refill can fund the unchanged
request from existing central balance. Inspect the refill breakdown; fund the
account only when it reports `insufficient_balance`, otherwise repair refill
health or revisit the per-instance cap.

**`denials.unknown_principal` spiking.** Two very different causes. Either
credentials rotated and callers are presenting keys this instance has never
been told about, or snapshot distribution has failed and the instance has
forgotten principals it used to know. `snapshots.unresolved` is the
disambiguator: nonzero means distribution, zero means credentials. Only the
first is an outage.

**`denials.snapshot_expired` above zero.** The control plane has stopped
delivering and the installed snapshot has aged past `valid_until`. This is
fail-closed working exactly as designed *and* an outage — the two are not
alternatives. Normal is zero; anything else means the snapshot manager cannot
reach its source. `/readyz` should already be false.

**`denials.accounting_backpressure` / `accounting.shed` rising.** The usage
sink is behind and admission is shedding to protect the queue (INVARIANTS GL-8).
Revenue-neutral by design — a shed request is charged zero — but it is the
leading indicator of everything worse. Watch `accounting.queue_depth`
approaching `accounting.queue_capacity`; the shed begins when they meet.

**`accounting.rejected` above zero.** The sink *refused* usage: unknown lease,
lease-capability mismatch, or no remaining lease capacity. This is bounded billing
loss that has already happened, and reconciliation is supposed to catch it.
Normal is zero. A steady trickle usually means events are arriving after their
lease settled — the drain deadline is sized too close to
`expiry_safety_margin + reclaim_grace`.

**`accounting.lost` above zero — and why it will not warn you first.** These
are committed charges that never reached the ledger. Any nonzero value is
money. But it **stays zero for the entire life of a process that cannot reach
its sink**: the steady-state path retries forever, so loss is only ever
declared during the final flush at shutdown. Alerting on `lost` alone would
mean discovering a twenty-minute outage at the moment the process exits. Alert
on `accounting.ingest_age_seconds` instead — a sink that has not answered in
minutes is the actual signal — and treat `lost` as the confirmation, not the
warning. The same applies to the `usage-writer shutdown failed` event, which
carries `unaccounted`: a lower bound on charges a dying writer was holding.

**`snapshots.refresh_attempts` flat.** Check task health before inferring an
outage: an empty catalogue or the negative-cache schedule can also explain
no fetches. An exited snapshot task leaves its retained readiness value false.
Pair attempts with `refresh_failures`, `refresh_timeouts` and
`refused_updates` to distinguish unavailable, slow and generation-incompatible
sources. The [snapshot operations guide](SNAPSHOT_OPERATIONS.md) defines the
counters, structured refusal fields and recovery procedure.

**`refill.abandoned` above zero.** Leases a shutdown budget could not return
to the allocator. Their units are stranded until TTL reclaim (INVARIANTS GL-9) —
bounded, not lost, but capacity is unavailable meanwhile. Like
`accounting.lost` this can only move at shutdown, so it is a post-mortem
number: a rising count across restarts means `shutdown_release_deadline` is
too tight for the allocator's real latency.

**`/readyz` false while every task is alive.** Not a crash: either the lease
is unusable (expired, or exhausted) or the snapshot set is stale. This is
INVARIANTS GL-10 keeping fail-closed correctness from masquerading as
availability. `snapshots.unresolved` reports the last resolution pass; Fixed
requires every tracked principal resolved, whereas All can serve with a
partially unresolved set. Use the runtime's current funding and resolution
report alongside that gauge. Zero unresolved alone does not establish readiness.

**Reclaim sweep failure** (`consecutive_failures` rising).
INVARIANTS GL-9's server half: a store
can answer `ping` and still fail `reclaim_expired`. While it fails, units from
crashed holders stay stranded rather than returning to their accounts.
Server readiness withdraws on the first failed reclaim or rollover pass, and
three consecutive failures of either operation escalate its event to `error`.
Recovery resets only that operation's streak. See the maintenance policy in
`CONTROL_PLANE_SECURITY.md` for task-exit and shutdown behavior.

**Any `error` from the control plane**, always. None of them is routine: a
snapshot or writer task that died, an allocator answering a release with an
acquire-only refusal, and — most serious — `the store rejected this release as
an accounting error`, which means local counts disagree with the ledger and
readiness has already dropped.

### Suspension, revocation, and closure: which mechanism

Three operator actions sound interchangeable and are not. Each has a different
scope, a different blast radius, and a different answer to "can I undo it".

| Action | Call | Scope | Blast radius | Latency to effect | Reversible |
|---|---|---|---|---|---|
| Suspend an account | `POST /v1/admin/accounts/{id}/status` `{"status":"Suspended"}` | the account | no new leases at once; every request denied `account_suspended` once instances refresh | leases: immediate at commit. Admission: one push in-process, else ≤ one `refresh_interval` | yes |
| Close an account | same, `{"status":"Closed"}` | the account | as above, denied `account_closed` | as above | **no — terminal** |
| Revoke one credential | `DELETE /v1/admin/snapshots/{principal}` | one principal | that credential denies `unknown_principal`; its siblings keep serving | same bound; carries the GL-15 generation watermark | republish at a higher generation |

The status change answers with what it did — `republished`, and `unreadable`
for rows that changed durably but could not be decoded to push. `republished: 0`
is the one worth reading: the account had no live snapshot to change, so it has
no credentials, or they are all revoked, or the call was a repeat. Any of those
is better learned at the call than from a later denial.

Suspension is per *account* and closure is the same action made terminal;
revocation is per *credential*. Rotating one leaked API key should not suspend
the customer, which is why revocation stayed a separate mechanism rather than
being folded into the status change (GL-51).

**An outstanding lease is not reclaimed by a status change.** Its units were
debited when it was granted, so spend after suspension is bounded by units the
instance already holds, and the lease settles at release or TTL reclaim (GL-9).
For an *elastic* account that bound is looser by the overage cap: until the
suspending snapshot installs, an instance may extend up to `overage_cap`
unfunded units on top of what it holds. The units are still recorded and still
billed, so the exposure is visible rather than lost — but an operator sizing a
cap should read it as "what a suspension cannot stop for one refresh interval",
not only as "what a funded account may overdraw".
Reclaiming early was considered and rejected: it would credit a lease back out
from under an instance that may still commit against it, which is the GL-12
window the whole reservation protocol exists to protect. An operator who needs
a harder stop than one refresh interval has to cut the tenant's traffic off
upstream; the ledger cannot offer one without breaking that window.

Worth knowing before choosing a `refresh_interval`: a status change on an
account with more than `PUSH_CHANNEL_CAPACITY` (256) live principals overruns
the push channel, so every subscriber lags and resyncs its whole tracked set.
That is correct and bounded by `max_concurrent_fetches`, and it is logged, but
it is the reason a very wide account propagates by resync rather than by push.

### Elastic enforcement has no account-level operator action (GL-1)

`EnforcementMode` rides on the snapshot and is set by publishing one, like
`ResolvedLimits`, `PermissionBits` and the cost table. There is deliberately no
`set_enforcement_mode` mirroring `set_account_status`, and the reason is worth
recording because GL-51 argued the opposite for status and the two cases look
alike.

What made status different was **two records for one fact**: the ledger's
`status` column and the published `AccountStatus`, with two propagation paths
and nothing checking them against each other, so "deactivate" returned success
while the request path kept admitting. The mode has one record. No store
operation consults it — leases are granted the same way for either mode — so
adding a ledger column would *create* the second record that unification
existed to remove.

The remaining worry is divergence across an account's principals, and the
shared counter already bounds it. Every principal of an account debits one
`AccountOverage`, so N credentials with N caps expose the account to the
**largest** cap, never their sum — which is the difference from the
per-principal rate limiter that would have multiplied an account's allowance
(review finding GL-4). `divergent_caps_bound_an_account_by_the_largest_not_the_sum`
pins it. What divergence does cost is that *lowering* a cap does not bind until
every principal of the account is republished; that is true of every
per-principal policy value already, and the control plane owns it.

`PublishableSnapshot::restamped` therefore stays infallible and stays unable to
change the mode. The mode participates in publication validation — a cap that
cannot fund one worst-case request is refused, for the same reason a batch cap
above the burst is — so re-stamping it would need a fallible signature and
would carry a proof forward over a value that proof depends on.

### Reconciliation: checking the two ledgers agree

The [per-account equation above](#two-ledgers-one-truth) includes both overage
funding and expired allowances.

Both backends implement it as `conservation(account)`, returning a
`Conservation` whose `holds()` performs the comparison in checked arithmetic
on both sides — an overflow answers "violated" rather than wrapping to a total
that might coincidentally match.
That is the supported way to check a live system — every backend suite asserts
it exactly, so it is the same check the tests run:

```rust
// PostgresStore: async, and None when the account does not exist.
let c = store.conservation(account).await?.expect("account exists");
assert!(c.holds(), "{c:?}");

// MemoryStore: the same shape, synchronous.
let c = store.conservation(account).expect("account exists");
```

Against a Postgres deployment directly, the sweep below reports every account
whose ledgers disagree. **It returns no rows on a healthy system** — that is
the passing result, and it is what to alert on being non-empty. `state = 0` is
an active lease; usage on active leases sits inside their grants, which is why
`settled_usage` subtracts it back out.
The funding sum is promoted to PostgreSQL `numeric`, as the lease sums already
are, so adding individually valid `BIGINT` columns cannot overflow the sweep.

```sql
WITH parts AS (
    SELECT a.account_id,
           a.deposited,
           a.overage_recorded,
           a.balance,
           COALESCE(SUM(l.granted) FILTER (WHERE l.state = 0), 0) AS active_grants,
           a.usage_recorded
             - COALESCE(SUM(l.used) FILTER (WHERE l.state = 0), 0) AS settled_usage,
           a.settlement_loss,
           a.expired
    FROM tollgate_accounts a
    LEFT JOIN tollgate_leases l ON l.account_id = a.account_id
    GROUP BY a.account_id
)
SELECT encode(account_id, 'hex') AS account,
       deposited, overage_recorded, balance, active_grants, settled_usage,
       settlement_loss, expired,
       deposited::numeric + overage_recorded
         - (balance + active_grants + settled_usage + settlement_loss + expired) AS drift
FROM parts
WHERE deposited::numeric + overage_recorded
      <> balance + active_grants + settled_usage + settlement_loss + expired;
```

`account_id` is the 128-bit id stored big-endian, so a single account is
`WHERE a.account_id = '\x00000000000000000000000000000001'` for account 1.

A companion sweep, in the same idiom, for the *other* pair of records that
must agree — an account's ledger status and the status its live snapshots
carry (GL-51). Since `set_account_status` writes both in one transaction this is
now unnecessary by construction, which is exactly why it is worth publishing:
it is the cheap auditor for hand-edited rows and for anything that ever writes
`tollgate_snapshots` outside this code.

```sql
SELECT encode(a.account_id, 'hex') AS account, a.status AS ledger,
       encode(s.principal, 'hex')  AS principal,
       s.snapshot ->> 'status'     AS published,
       s.generation
  FROM tollgate_accounts a
  JOIN tollgate_snapshots s ON s.account_id = a.account_id
 WHERE s.deleted = FALSE
   AND s.snapshot ->> 'status' IS DISTINCT FROM a.status;

-- Companion: snapshots whose account cannot be derived from their JSON. These
-- are exactly the rows `StoredSnapshot` already fails to decode, so a nonzero
-- count is corruption -- and it is also the set a status change cannot reach.
SELECT count(*) FROM tollgate_snapshots WHERE account_id IS NULL;
```

The ledger stores the status as text in the same spelling `AccountStatus`
serializes, rather than a `SMALLINT` code, so this check is an equality an
operator can read with no codec in between.

**A nonzero `settlement_loss` is not by itself a fault.** A lease can be
released while its usage events are still queued; the gap
`granted − used − unspent` is *provisional* loss, and a late event that fits
inside it converts back into billed usage (Finding 2). Expect it to appear
transiently and trend to zero. What is a fault is loss that persists with an
idle writer, which means those events never arrived.

**If the equation does not hold, that is a defect, not drift.** Steady-state
drift is zero by design; the ledger reads are checked (INVARIANTS GL-11), so a
negative column surfaces as an explicit store error rather than being clamped
past the corruption this check exists to detect. A failing `holds()` on
otherwise clean reads therefore means a genuine accounting bug. Capture the
account row and its lease rows *before* restarting anything — a restart
reclaims expired leases and rewrites the state that would explain what
happened.

## Consistency stance

Bounded consistency by choice: snapshot staleness is bounded by
`valid_until` + push latency; revocation propagates via generation bumps and
expiry. Revocation tombstones retain the removed generation in memory and
Postgres. Request-visible negatives are bounded and evictable, but their
generation watermarks survive ordinary visible eviction, so cache expiry cannot
resurrect stale authorization. Bounded history reclamation removes visible state
and fences outstanding reads; reopening requires a new authoritative source
read. Source tombstones remain durable. Negative validity expiry
schedules a targeted pull with retry backoff; it does not wait for a longer
full-refresh interval and works over HTTP's explicitly closed push stream. A
crashed instance strands quota at most until lease TTL. Immediate
global revocation and durable-before-response accounting would require
synchronous coordination per request — a separate strict-accounting mode
could accept that cost, and nothing in the trait surface precludes it, but it
is out of PoC scope.

## Second review round (2026-08-20)

An external review surfaced eleven findings; all are fixed and regression-
tested. The load-bearing ones and their resolutions:

1. **Expiry race** (critical): reservations could commit after their lease
   was reclaimed and re-granted. Now a three-part protocol — local
   *usability window* (`expires_at - safety margin`: debits AND commits
   stop, absorbing allocator/holder clock skew), commit-time recheck
   (post-window commits release for zero and must not execute), and
   store-side *reclaim grace* (`expires_at + grace`, with releases and late
   usage honored through it). Invariant GL-12.
2. **Writer shutdown livelock** (critical): edge-triggered shutdown could be
   consumed inside the retry loop; shutdown is now level-checked at every
   loop boundary, with a bounded, loss-reporting final flush. Dropped
   handles abort their tasks.
3. **Commit/emission gap**: the queue permit is the usage slot bound at
   admission; `Committed`'s drop emits under panic/abort. Invariant GL-13.
4. **Per-principal limiters** multiplied account allowances; limiters now
   live in a per-map registry keyed by AccountId. Rate limits are enforced
   per engine instance (documented), leases aggregate spend globally.
5. **Snapshot distribution**: SnapshotManager (initial load gates readiness,
   push + lag recovery, periodic refresh doubling as bounded-window
   revocation); memory backend now generation-monotonic like Postgres.
6. **Request ids**: random 128-bit (UUID) — process-local counters collide
   across instances and misclassify legitimate usage as duplicates.
7. **Account creation** is AlreadyExists-surfacing in both backends
   (invariant GL-14); resets are a deliberate separate workflow (unbuilt).
8. **Postgres ingest** is one transaction per batch: sorted ANY() row locks,
   one dedup lookup, in-memory classification, UNNEST bulk insert, and
   set-wise grouped aggregate updates.
9. **Bulk snapshot install**: `install_many` — 512 principals load in ~54 µs
   vs ~820 µs as a per-entry loop on the arc-swap map.
10. **Deadlines/Drop**: HttpStore carries connect/request timeouts;
    background handles abort on drop instead of leaking spinning tasks.
11. **Server**: the service stays backend-generic; the binary selects memory
    or default-featured Postgres at build time, with versioned sqlx migrations.
    `/readyz` pings the store, and non-loopback binds warn.

## Follow-up review round (2026-08-20)

The follow-up review found nine edge cases left after the first remediation;
all now have regression coverage:

1. **Usability-window rollover returns quota.** A lease removed when its local
   usability window closes is parked under the same quiescence rule as a
   low-water rotation, then released during allocator grace instead of being
   forgotten until TTL reclaim.
2. **Limit updates reach every sibling principal.** Each account owns a stable
   limiter indirection. A newer account generation atomically swaps its inner
   governor bucket, so existing principal states immediately see the new
   limits; stale snapshots cannot roll it back.
3. **Revocation is a versioned state.** Positive and negative map entries share
   generation ordering. Memory and Postgres retain deleted snapshot rows as
   tombstones; migration `0002_snapshot_tombstones.sql` adds the durable
   marker.
4. **Timing configuration is validated.** Stores, lease/snapshot managers, and
   the server reject unsafe signs, zero scheduler intervals, and inconsistent
   lease thresholds before starting. A nonpositive requested TTL is refused
   before any balance debit.
5. **Readiness is continuous.** Snapshot resolution deadlines are tracked even
   while a source fetch hangs; task/channel closure, an exhausted or unusable
   lease, or a stopped accounting writer all lower `/readyz`.
6. **Final accounting respects the batch contract.** Shutdown drains no more
   than `max_batch` per sink call, retries each chunk a bounded number of
   times, and reports rejected/lost totals to the embedding application.
   The drain waits for outstanding permits, not just buffered events (GL-32):
   closing the receiver refuses new reservations while permits reserved
   earlier can still deliver, and a real receive distinguishes "a reserved
   slot is still out there" from "done" — `try_recv` cannot. The wait is
   bounded by `shutdown_drain_deadline`; permits unresolved at the deadline
   are reported in `WriterStats::unresolved`, their charges bounded
   thereafter by TTL reclaim. The report survives the writer's own death
   (GL-41): charges are counted from the moment they enter the queue until
   they are given an outcome, in a counter held outside the task, so a
   panicked or aborted writer returns `WriterShutdownError` with a lower
   bound rather than a zeroed report. `WriterStats` is intentionally not
   `Default` — that derive is what made `unwrap_or_default()` on a dead
   task's `JoinError` spell "nothing was lost".
7. **Snapshot sweeps are bounded and cancellable.** Fetches use configured
   concurrency, shutdown aborts outstanding calls, results are classified in
   linear time, and mixed positive/negative results land in one arc-swap RCU
   update. The same property now holds for the other two background planes
   (GL-34), by a different mechanism: a sweep owns spawned tasks and can abort
   them, whereas an `ingest`/`acquire`/`release` future borrows its
   arguments, so those are bounded with `timeout` instead — per call, and per
   shutdown in total. `HttpStore` had carried these bounds at the transport
   since the beginning; the client now holds them for whichever backend is
   plugged in, including a direct `MemoryStore` or `PostgresStore`.
8. **Postgres lock order is deterministic.** Multi-account ingest aggregates
   use ordered maps and explicitly pre-lock accounts in byte order before
   their set-wise update; reclaim pre-locks its accounts the same way. Neither
   path
   relies on an `UPDATE ... FROM` executor preserving input-array order, so
   the random `HashMap` account-row deadlock cycle cannot return.
9. **Postgres generation conversion is explicit.** Values above the signed
   `BIGINT` range return an error instead of silently aliasing to `i64::MAX`.
10. **Postgres stored values cannot go, or read as, negative** (GL-15, GL-45).
    Unit columns and fence counters carry schema CHECK constraints, the
    settlement-loss subtraction refuses (and rolls back) a batch that would
    underflow it — the SQL form of the memory backend's straggler assertion —
    and every read of a unit column or fence surfaces a negative value as a
    storage error naming it. Clamping negatives to zero would let
    `Conservation::holds()` pass over exactly the corruption class the
    equation exists to detect, and aliasing a corrupt fence to 0 would
    misreport corruption as a caller's capability mismatch. Accepted usage
    events reuse the validated stored fence rather than reconverting the
    event's token, making the insert-side aliasing unrepresentable.

## Negative-cache remediation (GL-14, 2026-08-22)

The ArcSwap map previously retained every negative indefinitely and treated
the TTL as request-invisible metadata. The root fix separates two concerns:
the request-visible negative set is capped, expires on control writes, and
evicts earliest deadlines first; the separately retained generation watermark
keeps authorization monotonic. The later GL-67 retention boundary bounds that
history through fenced authoritative reconstruction, independently of TTLs. `SnapshotManager` owns each negative's deadline and
per-principal retry schedule, so expiry triggers a targeted pull. The same
generation transition functions have property tests and a Lean model proving
that revocation followed by eviction still rejects an at-or-below-generation
positive replay.

## Continuous accounting health (GL-38, 2026-08-23)

`WriterStats` existed exactly once — a local on the writer task's stack,
materialised only when the task returned. So the numbers that decide whether
billing is intact were legible only after a graceful shutdown, which is the one
ending where loss is least likely; a process that crashed, was killed, or
simply kept serving said nothing.

The fix was not to add a second tally but to move the existing one out of the
task, following the `unaccounted` counter that already lived outside it for
exactly this reason. `shutdown` now returns a snapshot of those same counters,
so "the running totals match the final report" is true by construction rather
than by test. The counters written from the request path — `unaccounted` and
the new shed count — take a cache line each, because folding them in beside
the task's own counters would have introduced false sharing that the previous
standalone `Arc<AtomicU64>` did not have.

**What an operator should actually watch, which is not what it looks like.**
`lost` is the number that matters for billing integrity, and it stays zero for
a healthy-but-cut-off process's entire life: the steady-state path retries a
failing sink forever, so nothing is declared lost until the final flush gives
up. Told to "watch `lost`", an operator would be watching a counter that can
only move at shutdown. The runtime signals are the age of the last successful
ingest, a queue depth approaching its capacity, and `rejected` — events the
sink has already refused, which is bounded billing loss reconciliation is meant
to catch. This is the substance GL-39 has to convey.

Readiness is deliberately untouched. A sink outage does not stop an instance
admitting — leases still fund requests — so flipping `/readyz` would withdraw
healthy capacity during a billing-backend outage. INVARIANTS GL-10's bar is that
the accounting task is *alive*, which `recorder.is_closed()` already answers.

## Request-path counters (GL-37, 2026-08-23)

The control plane reports itself with structured events (GL-36). The request
path cannot: INVARIANTS GL-5 bars I/O there, the hot-path budget bars blocking
locks and policy clock reads, and a logging call is all three — which is why `tollgate-admission` still carries no
logging dependency. The affordable channel is a counter, and `DenyReason`
being a closed enum is what makes it cheap: `DenyReason::COUNT` slots in a fixed array
indexed by an exhaustive `DenyReason::index`, so there is no map, no string
key, and no way for a new variant to reach a slot it shares with another.

Three things fell out of building it that were not obvious from the issue:

- **One recording point, not ten.** Several reasons never appear
  literally in `admit` — `AccountSnapshot::admit` and `Reservation::reserve`
  raise them and `?` carries them out. Instrumenting the exits would have
  produced counters that were quietly incomplete for exactly the reasons an
  operator most wants (staleness, permissions, lease expiry). Recording the
  single outcome of `admit` cannot miss one.
- **Padding helps the array, but the contended case required sharding.** Each
  original counter took its own line so cores counting *different* reasons did
  not false-share. That did nothing for `full_check_contended_8`, where every
  thread admits and writes one slot — true sharing. Issue GL-3 subsequently
  moved opt-in counters to sticky-locality shards and corrected the supported
  line assumption to 128 bytes; the instance snapshot aggregates them.
- **No deny path was benchmarked at all.** Both hot-path benches set limits
  high enough that only the admit path runs, so "the counters are free" had no
  witness on the path the counters exist for. `admission/full_check_denied`
  fills that gap at 19.7 ns — the cheapest refusal, where a per-outcome tally
  is the largest possible share of the work.

`units_admitted` counts what admission *quoted*, never what was billed: a
reservation cancelled before execution charges zero, and usage events remain
the billing record (see "Two ledgers, one truth"). Refusals decided before the
engine is consulted — accounting backpressure under INVARIANTS GL-8, and a
credential that fails verification — are recorded by the embedder against the
same tally, so no reason exports a permanent zero that would read as "this
never happens". The example exposes the tallies as JSON on `/metrics`
deliberately: naming the metrics is this change; choosing an exposition format
and documenting it for operators belongs to GL-38 and GL-39.

## Set-wise PostgreSQL usage ingest (GL-5, 2026-08-23)

Review round 8 aggregated accepted events in memory but left both aggregate
writes as per-key executor loops, so a 256-event batch could still hold its
row locks across hundreds of database round trips. The semantic store suites
could not expose query-count scaling, and there was no PostgreSQL ingest
benchmark spanning many distinct leases and accounts. The benchmark added
with this remediation makes that missing dimension reproducible without
encoding SQL source text into a test.

The first merge-request mutation run also exposed an unwitnessed branch in
the mixed-batch contract: a missing lease must contribute one rejection. The
mirrored scenario now includes that case. An earlier local parallel run had
reported the mutant caught for the wrong reason because independent nextest
processes could truncate the same PostgreSQL test database underneath each
other. The mutation wrapper therefore permits only one cargo-mutants worker
whenever PostgreSQL is enabled; nextest test groups alone serialize only
inside one process and cannot enforce that cross-process ownership rule.

Usage ingest preserves its partial-acceptance contract in application code:
each input is classified once against the locked lease rows and the existing
request-id set, then the accepted subset is committed atomically. Encoded IDs
and timestamps are prepared once per event before the transaction. Checked
`BIGINT` units are computed once after duplicate and capability classification.
An out-of-domain new event contributes one rejection; a stored invalid value
remains a store error. Accepted units are reused by every write, preserving the
existing rule that a replay is a
duplicate regardless of its other fields. Duplicate and rejected inputs never
enter the insert or either aggregate delta.

Accepted events still land through one `UNNEST` insert. Per-lease usage and
per-account usage/loss movements now land through one
`UPDATE ... FROM UNNEST(...)` statement each instead of one statement per
distinct record. The grouped deltas use checked arithmetic, and inserted or
updated row counts must equal the verified input cardinalities before commit.
Lease update deltas are stored as `Option<NonZeroI64>` during classification,
so rejected, duplicate, and zero-unit events cannot make a no-op lease row
enter the set-wise write.

Set-wise update execution does not promise row-lock order. The transaction
therefore pre-locks leases in `(account_id, lease_id)` order — shared with any
concurrent ingest, which is what a lease-lock cycle would need, and no longer
shared with reclaim, which since GL-65 selects in expiry order and cannot be a
party to such a cycle because `SKIP LOCKED` means it never waits for a lease —
and explicitly locks every affected account with `ORDER BY account_id FOR
UPDATE` before the account update. The account IDs come from a `BTreeMap`, and
the locked rows are also where stored usage overflow and settlement-loss
underflow are validated before mutation. This preserves the global
lease-then-account order and the deterministic multi-account ordering
established by the earlier concurrency remediation.

For `E` events, `D_l` distinct leases, and `D_a` distinct accounts, preparation
is `O(E)`, ordered lease indexing/classification is `O(E log D_l)`, and ordered
account aggregation is `O(E log D_a)`. SQL bound-parameter and touched-row
volume is `O(E + D_l + D_a)`. The number of database round trips is constant
in `D_l` and `D_a`; the old aggregate phase issued `D_l + D_a` sequential
updates while holding the transaction locks. No schema, migration,
configuration, wire, production-dependency, public Rust API, or
request-hot-path change is involved.

The committed non-gating Criterion scenario measures 256 accepted events
spanning 256 leases and 256 accounts against the same local PostgreSQL host;
its fixture is isolated in a generated temporary schema rather than truncating
the URL's existing tables. Absolute timings are host-dependent, so evidence is
collected by running this scenario on both revisions against the same database
without competing workloads. Two uncontended same-host runs on 2026-08-23,
performed in reversed revision order, produced central estimates of 240.10 ms
and 208.31 ms for untouched `origin/main`, versus 6.7127 ms and 7.1645 ms for
the set-wise implementation: 35.8× and 29.1× faster in the paired runs.

## Bounded PostgreSQL expiry reclaim (GL-6, 2026-08-23)

Expiry reclaim is bounded at the transaction boundary, not at the size of a
legitimate outage backlog. `LeaseAllocator::reclaim_expired_batch` takes a
nonzero limit and returns `ReclaimBatch`: its private saturation evidence is
derived from the returned row count, so the server consumes verified evidence
rather than guessing whether to continue. The original `reclaim_expired(now)`
API remains as a full-drain convenience over bounded batches; if a later batch
fails, its error names the already-committed partial progress. Existing callers
and the `/v1/leases/reclaim` JSON array therefore retain their contract. Store
trait implementors must add the bounded primitive; that is the sole source
migration.

The server reads one clock value per scheduled cycle and drains against that
fixed cutoff until a batch is not saturated. The backlog is therefore finite
even while new leases expire, and a yield between batches keeps the in-memory
backend from monopolising an executor thread. A failure after earlier batches
reports their lease, unit, and batch totals; committed recovery work is never
silently presented as a wholly failed sweep.

The PostgreSQL transaction locks at most 256 leases through the partial expiry
index, ordered by that index's own `(expires_at_floor_us,
expires_at_submicro_ns)` so the `LIMIT` stops the walk (GL-65), with `SKIP
LOCKED` cooperation between sweepers. It validates every credit before mutation, aggregates account
credits in a `BTreeMap`, explicitly locks the affected account rows in byte
order, then updates all selected leases and all affected accounts with two
`UPDATE ... FROM UNNEST(...)` statements. Account pre-locking matters because a
set-wise update does not itself promise row-lock order. Row counts are checked
before commit, and all exits retain the explicit commit/rollback completion
contract below.

For a batch bound `B`, each transaction now performs a constant number of SQL
round trips and holds `O(B)` rows, while an `N`-lease backlog drains through
`O(ceil(N/B))` short transactions. The old shape held all `N` lease and account
locks across `2N` sequential update round trips. No schema, configuration, wire,
dependency, or request-hot-path change is involved.

## PostgreSQL transaction completion (2026-08-23)

The Postgres wrong-token release scenario exposed a scheduler-sensitive
failure: a rejected `release` returned, then the immediately following reclaim
sweep sometimes found no expired lease. The ledger was intact. The first
operation had dropped an open `sqlx::Transaction`; SQLx queues rollback on drop
for a later async connection operation, while reclaim deliberately uses
`SKIP LOCKED`. Under the failing interleaving, reclaim skipped the lease row
whose failed release had already returned but whose queued rollback had not
yet released the lock.

The original backend relied on drop rollback on every `?` and domain refusal.
Local runs normally completed the queued rollback quickly enough, and the
failure-path tests generally checked ledger state through a later pooled query
rather than checking that the returning operation had released its locks. The
wrong-token scenario's immediate sweep was the one indirect witness, and it
passed until a slower CI schedule produced the narrow interleaving.
Transaction completion is now owned in one pair of helpers:
acquire, release, reclaim, and ingest route their entire transactional bodies
through them, await commit on success, and await rollback before exposing any
failure. `wrong_token_release_leaves_lease_reclaimable` is the regression
witness because its immediate `release`-then-reclaim sequence exercises the
contract without a sleep or retry that could conceal it.

## Fast principal hashing (GL-9, 2026-08-23)

Every request-path lookup hashed a 16-byte `Principal` through SipHash-1-3,
because the snapshot maps used the default `RandomState`. That resistance
defends against an attacker steering keys into one bucket — and `Principal` is
a fingerprint of an *already-verified* credential, derived under a secret the
caller does not hold, so the keys cannot be steered.

The stronger form of the argument is what settled it. An embedder who does key
admission by a raw client-supplied token has not merely weakened their hashing:
that value selects which account's snapshot, lease and rate limiter a request
lands on, so a caller who can choose it can aim at another tenant. SipHash
never defended against that. There is no configuration in which it was buying
the protection its cost implied, which is why the fix is a hasher swap plus
documentation on `Principal` rather than a per-deployment switch.

Both maps use `foldhash::fast::RandomState` through one alias,
`PrincipalHasher`. `foldhash` was already compiled into `tollgate-admission`'s
graph (`governor` → `hashbrown` → `foldhash`), so taking it directly adds no
package to the build — the same argument `atomic-waker` carries for GL-10. Moka
changed alongside arc-swap deliberately: the gap between the two lookup
benchmarks is the evidence for which map to deploy, and it would have quietly
become "one of them hashes differently" had only one side moved.

Measured paired in one session, reverting the alias in place rather than
comparing against a stored baseline (the method GL-49 records):

| benchmark | SipHash | foldhash |
|---|---|---|
| `admission/snapshot_lookup_arc_swap` | 23.6 / 23.6 / 23.7 ns | 13.9 / 13.8 ns |
| `admission/snapshot_lookup_moka` | 74.5 / 74.6 / 74.6 ns | 69.5 / 69.5 ns |
| `admission/full_check` | 115.1 / 115.7 ns | 99.6 / 99.5 ns |
| `admission/full_check_denied` | 20.0 / 19.8 / 19.9 ns | 14.7 / 14.7 ns |

`cost_table/quote`, `snapshot/admit` and both `lease/reserve_*` benchmarks were
identical on both sides, which is what makes the rest credible: an untouched
control moving would have meant a disturbed host rather than a real change.
`full_check`'s 15.9 ns saving exceeds the 9.8 ns measured on the isolated
lookup, and the remainder is not accounted for by the hash alone — recorded
rather than explained away. `full_check_contended_8` improved too, but its own
baseline spread was 20% across runs, so no figure from it is quoted.

The choice of hasher is argued, not assumed. A raw identity fold over the
`u128` — the cheaper alternative — fails on sequential principals: hashbrown
takes its control byte from the top seven bits of the hash, and consecutive
integers leave those constant, collapsing 4,096 keys onto one control byte.
`principal_hashing_stays_spread_for_sequential_and_random_keys` checks the low
bits and the top bits separately, on both key shapes an embedder produces.

The first version of that witness constructed `RandomState` directly and
required every bucket count to stay below a statistical threshold. That made
the test depend on the process-generated shared and per-hasher seeds even
though `foldhash::fast` is designed for hash tables and explicitly does not
promise statistical-quality output. A release pipeline found 87 sequential
keys in one low-bit bucket where the test allowed 64; the identity-fold defect
the test exists to reject would put all 4,096 sequential keys in one top-bit
control value. Local and prior CI runs missed the defect because each process
sampled a different seed.

The witness now exercises a fixed matrix of shared and per-hasher seeds through
`SeedableRandomState`, while a compile-time assignment keeps the production
alias tied to `fast::RandomState`. The map therefore retains randomized seeds;
only the evidence is deterministic. A same-pattern search of the Rust tests
found no other randomized hasher distribution bounds.

## Credential verification belongs in the library (GL-2, 2026-08-24)

The pricing-api recomputed HMAC-SHA256 on every request even though the load
gate deliberately uses persistent connections. Paired profiling showed the
credential check, not quota admission, dominated the sequential overhead, and
measurement put numbers on it: ~800 ns to verify a credential against ~107 ns
for an entire admission. **The step in front of tollgate cost seven times
everything tollgate does.**

That is why this landed in `crates/tollgate-auth` rather than in the example
where it was first written. A library that tunes 107 ns to the nanosecond while
leaving 800 ns to each embedder is tuning the wrong end, and the load gate
cannot honestly attribute overhead to tollgate while example-service HMAC
dominates the delta.

The crate is split where the problem splits. Credential *schemes* differ per
deployment, so `CredentialVerifier` is a trait and the embedder may bring
PASETO, JWT, or a certificate fingerprint; `HmacRegistry` is the scheme in the
box. What is *not* deployment-specific is the caching, and that is the part
worth centralising: `SessionCredential` verifies at most once per session and
compares thereafter. The embedder supplies only the two transport-bound facts
the library refuses to guess — what a session is, and how to get credential
bytes out of the wire format.

Measured on one machine:

| | ns |
|---|---:|
| verify, uncached (the old per-request cost) | 793.2 |
| verify, cache miss (verify + install) | 1031.2 |
| verify, cache hit | **16.0** |

A miss is ~238 ns dearer than the uncached path, because the entry has to be
installed. **A session serving exactly one request is therefore slower than
before**; the extra is repaid by the first hit, so the cache is ahead from the
second request on a session onward. The load gate runs 5 000 requests over 10
persistent connections — 500 per connection — so the configuration this project
measures sits at the asymptote, not the break-even. An embedder serving
one-shot connections should know the trade runs the other way for them.

**Why HMAC survived the move.** Plain SHA-256 is 181 ns against HMAC's 741 ns —
four times cheaper, better than the halving originally guessed. But caching
moved that saving from once-per-request to once-per-session, making it ~5.6 ns
per request at a hundred requests per session, against a 16 ns cached path it
cannot touch. HMAC keeps the secret and the digest table separately
insufficient; the cheaper digest buys a rounding error and costs that. Caching
did not make the swap more attractive — it removed most of the reason for it.

**The cache proves identity, never authorization** (INVARIANTS.md GL-23). A hit
skips the credential check and nothing else: admission still runs against the
current snapshot every request, so revocation stays bounded by snapshot refresh
exactly as it is with no cache. Requiring `ConnectInfo<PricingConnection>` in
the handler makes incorrect server wiring fail visibly instead of silently
reverting to per-request verification, and `credential/verify_cached` is gated
so a regression that bypasses the cache shows up as a number rather than as
nothing at all.

## Memory backend sweep cost and growth (GL-23, 2026-08-23)

`MemoryStore` never deletes, and both of its scans walked the whole lease
table: the reclaim sweep filtering for active-and-due leases, and
`conservation` summing one account's active grants. The server runs the sweep
every five seconds by default, so its cost climbed for the life of the process
while the live population stayed flat.

Postgres never had this. Its equivalents push the filter into SQL
(`WHERE account_id = $1 AND state = 0`, plus GL-6's partial expiry index), so the
*reference* implementation — the one whose job is to define the semantics the
real backend reproduces — was the only one whose cost model was wrong.

Active leases are now indexed by expiry in `crate::leases`, and the sweep walks
that index in order, stopping at the first lease not yet due. The interesting
decision was where the index lives. It is derived state maintained at three
transition points spread across `acquire`, `release` and the sweep, and the
drift it invites is silent and severe: a lease left active in the table but
missing from the index is never reclaimed *and* stops being counted by
`conservation`, so the ledger checker goes blind to the leak it caused. So the
table and the index live behind one type, with `state` and `credited` private
to it and a single `settle` transition — no call site can retire a lease and
forget the index, because none of them can retire a lease at all. That also
makes state and credit move together, which two settlement sites previously
maintained by hand.

`conservation` still recomputes its sums from the lease records. Caching
per-account aggregates would be faster and is the wrong trade: this function
exists to catch ledger bugs, and one that reads a total maintained by the same
writers that might be wrong cannot catch them. The index narrows which records
it reads; it is never the source of the numbers.

Reclaim order becomes expiry-ordered rather than `HashMap`-arbitrary. Nothing
depended on the old order — `expired_backlog_is_reclaimed_in_bounded_batches`
sorts before comparing — and determinism is an improvement.

Growth itself is unchanged and now visible: `MemoryStore::stored_records`
reports usage events, lease records, and how many of those are active, and the
sweep logs them once per drain at `debug`. Putting the active count beside the
total is the point — the sweep's cost tracks the one that stays flat while the
others climb. Usage-event retention remains deferred; it needs a dedup-window
decision, not a deletion.

## The reconciliation query's account filter (GL-12, 2026-08-23)

`conservation` sums one account's active leases, and nothing indexed
`account_id`. The issue that raised this predicted a sequential scan of a table
that grows with every lease rotation and is never pruned. Measured, that is not
the mechanism, and the difference matters for what the fix is worth.

`tollgate_leases_expiry` is already partial on `state = 0`, so PostgreSQL scans
*that* — the live set, fleet-wide — and discards the rows other accounts own.
The cost therefore never grew with lifetime rows. It grew with the number of
live leases across the whole fleet: a per-account reconciliation query paying
for every other account's live set. Bounded, but the wrong bound.

Measured on 220,000 lease rows, 20,000 of them live across 2,000 accounts,
asking for one account's ten:

| | plan | estimated cost | actual | heap blocks |
|---|---|---|---|---|
| before | `tollgate_leases_expiry` + `Filter: account_id` | 3616 | 1.934 ms | 286 |
| after | `tollgate_leases_account_active` | 42.9 | 0.098 ms | 10 |

"Rows Removed by Filter: 19990" is the whole story, and migration 0005 removes
it: `(account_id) WHERE state = 0`, partial for the same reason the expiry
index is, so it stays proportional to live leases rather than to the table.

Two notes on the migration itself. It is `CONCURRENTLY`, unlike 0003 and 0004 —
those add constraints to `tollgate_accounts`, one row per account, where a
brief ACCESS EXCLUSIVE scan costs nothing; this one touches the unbounded
table, where a plain `CREATE INDEX` would hold a SHARE lock and block lease
acquisition for its duration, which in this system means the request path
failing closed. And it deliberately omits `IF NOT EXISTS`: a concurrent build
that fails leaves an INVALID index, and `IF NOT EXISTS` would let the retry
skip it silently while the query kept discarding rows. Failing loudly is the
only version an operator can act on; the migration carries the recovery step.

The regression witness is a plan assertion rather than a timing, and what it
pins is that the account predicate is an `Index Cond` rather than a `Filter`.
"No sequential scan" would have been the wrong property twice over: there was
no sequential scan before the fix in the settled-heavy shape, and there *is*
one without the index in the live-heavy shape. Only "answered by an index, not
by discarding rows" is true in both.

## Dependency advisory gate (GL-26, 2026-08-23)

Every merge-request and default-branch pipeline audits the committed
`Cargo.lock` with pinned `cargo-audit` 0.22.2. Vulnerabilities fail by default,
and `--deny warnings` promotes yanked, unmaintained and unsound dependency
findings to failures too. The version lives in `.cargo/audit-version`; the CI
job installs exactly that locked release without the unrelated binary-scanning
feature, and `scripts/check_advisories.sh` refuses to run under a different
one. `cargo-deny` was not selected because this issue establishes an advisory
contract, while license, source and duplicate-version policy each need their
own deliberate baseline rather than defaults smuggled in with the security
gate.

The initial strict scan found one advisory: RUSTSEC-2023-0071 against
`rsa 0.9.10`, with no fixed release. The workspace enables only SQLx's
PostgreSQL support, but Cargo records the published `sqlx-macros-core` package's
optional MySQL graph in the lockfile, and that graph contains `sqlx-mysql` and
`rsa`. An all-target, all-feature inverse `cargo tree` over normal, build and
development edges finds no workspace path to `rsa`, so it is not compiled into
any Tollgate target.

That exception is executable rather than permanent. Before auditing, the
wrapper fails if `rsa` becomes reachable under the broad workspace graph. The
ignored audit then blocks every other finding. Finally, an unignored audit
against the same freshly fetched advisory database must fail; if it passes,
the exception is stale and the gate requires its removal. This gives the sole
escape hatch both an immediate safety boundary and an automatic exit.

The change is CI-only: it alters no public API, wire contract, migration,
runtime behavior, allocation, or request-path performance.

## GitLab Cargo cache policy (GL-30, 2026-08-24)

The GitLab migration replaced Woodpecker's within-run shared `target/` with a
static `cargo-registry` archive inherited by every job. That made unrelated
jobs restore Cargo state, and GitLab's default `pull-push` policy let parallel
jobs replace the same archive with whichever partial view finished last. It
stayed green because a cache miss is recoverable and the configuration is
syntactically valid; the failure was wasted work, visible in runner traces
rather than test results.

The shared build cache is now content-addressed by both `Cargo.lock` and
`rust-toolchain.toml`. It contains the narrow Cargo registry and Git database
paths from the canonical pipeline plus `target/`, accepting one archive upload
per merge-request pipeline to recover cross-job build sharing. `clippy` is the
sole `pull-push` producer in the `check` stage. The Rust build jobs in later
stages inherit one pull-only cache mapping; formatting, repository checks,
dependency audit, formal verification, and the Rust 1.89 MSRV job do not
extract target artifacts they cannot use. Formal verification and release-plz
retain their own disjoint keys and lifecycles.

CI uses `.ci-cargo` rather than `.cargo` for generated Cargo state because the
latter contains tracked audit and mutation configuration. CI caches are not
assumed to be distributed or durable, so every job must remain correct when
the cache is absent. A shared mapping makes pull-only the default, with
the one writer expressed as the sole explicit policy override.

## Mutation baseline for the pre-gate crates (GL-43, 2026-08-24)

`INVARIANTS.md` names a test behind every invariant, which is the right rule
but does not establish that the named test would *fail* if the invariant broke.
The diff-scoped gate has answered that for everything written since it landed;
these three crates were written before it.

The expectation going in was the client's first-run rate — 66 of 88 viable, a
25% survival — which would have meant roughly 80 survivors across 321 mutants.
The measurement was 12. Reservation commit/cancel, the lease usability window,
settlement arithmetic and the conservation equation were all already pinned.
What survived was almost entirely *accessors and admin operations*: code whose
callers were tests rather than the paths the suites were built around.

Ten were gaps and got tests. Two were equivalent and are excluded with their
arguments. Three findings were worth more than the tests themselves:

- **The two backend suites had drifted.** `inactive_account_refuses_leases`
  called the inherent `set_active` on the memory side and `AdminStore::set_active`
  on the PostgreSQL side, so the memory backend's trait implementation could be
  replaced by `Ok(())` — suspending an account and continuing to serve it —
  with everything green. Neither suite exercised `deposit` or the TTL clamp at
  all. Those scenarios now exist on both sides, which is what a mirror is for.
- **`Arc<T>`'s `SnapshotMap` delegation had no witness for its bulk writes.**
  `install_many` and `apply_many` could both be no-ops. That impl is `?Sized`,
  so `SnapshotManager`'s own `Arc<dyn SnapshotMap>` dispatches through it: a
  silent no-op means a refresh pass that reports success and installs nothing,
  every principal ageing out to `SnapshotExpired` with no error anywhere.
- **One survivor was dead code, not a missing test.** Correcting the memory
  suite to call the trait method left the inherent `MemoryStore::set_active`
  with no callers anywhere in the workspace. Writing a test for it would have
  turned the gate green without making anything safer, so it was deleted.

Issue GL-27 completed that cleanup's contract evidence: the unknown-account
answer was stated explicitly and mirrored in both suites. An audit of the
remaining inherent/admin pairs found no other divergent body: `deposit`,
`publish_snapshot`, and `remove_snapshot` delegate, while the panicking
`create_account` convenience remains deliberately separated from the fallible
`try_create_account` used by the trait.

`set_active` itself no longer exists — GL-51 replaced it with
`set_account_status`, which carries that same unknown-account contract as
`SetStatusError::UnknownAccount`. The drift lesson above is why every scenario
added there drives `AdminStore` rather than an inherent helper.

## Dynamic principal discovery (GL-48, 2026-08-24)

`SnapshotManagerConfig::principals` was a `Vec` fixed at construction, so
onboarding a customer meant a redeploy and, until then, an `unknown_principal`
denial indistinguishable from a bad key.

The topology decides the design, and the chosen one is **stateless**: any
instance may serve any customer. That rules out demand-driven discovery — an
instance learning from traffic converges to the full set anyway, and charges a
legitimate customer's first request for the privilege — and makes enumeration
the mechanism. `TrackedPrincipals::All` re-enumerates each refresh;
`Fixed` keeps the old behaviour exactly, so upgrading changes nothing until an
embedder opts in.

Three things the exploration turned up, each of which changed the design:

**The push was already arriving and being thrown away.** `subscribe()`
broadcasts every publish, and the manager discarded any push for a principal
outside its configured list. Under `All` that filter is simply wrong, and
removing it is what makes in-process discovery immediate rather than
refresh-bound.

**Push alone is not enough.** It carries deltas from the moment of
subscribing, so a cold instance still needs the set that already exists —
hence `SnapshotSource::principals`, defaulted to `Ok(None)` so the seven test
doubles and any embedder's own adapter keep compiling. `None` (cannot
enumerate) and `Err` (enumeration failed) are deliberately distinct: collapsing
them would let a broken source look like a limited one, and an instance would
serve a stale set forever believing it was configured that way.

**Readiness had to change, and the argument is the same one GL-10 already
makes.** Requiring every tracked principal to resolve is right for a
hand-configured slice and a fault for a whole customer base: it would hold an
instance serving 15,999 of 16,000 principals out of rotation for the one its
source cannot answer for — fail-closed correctness masquerading as
*un*availability. Under `All`, unready means *no* tracked principal is
resolved. Both readings come from the same pass that sets the `unresolved`
gauge.

Removal and revocation are not the same event and the code keeps them apart. A
revoked principal is still enumerated — the tombstone *is* the record of the
revocation — so it stays tracked and resolves negatively, keeping the
generation watermark that stops a replayed older snapshot resurrecting it. Only
a principal that disappears from the catalogue is untracked, dropping its
resolution and watermark together.

Enumeration failure gets its own counter rather than sharing `refresh_failures`,
because its consequence is different: failed fetches make known principals go
stale, which `unresolved` already shows, while a failed enumeration freezes the
tracked set — everything already known keeps working perfectly, and nothing new
ever appears.

The O(N) refresh was measured before deciding whether to batch fetches, and the
estimate that motivated the question was wrong. A full sweep in-process:

| principals | `Fixed` | discovering |
|---|---|---|
| 512 | 2.83 ms | 2.84 ms |
| 4,096 | 6.51 ms | 6.75 ms |
| 16,384 | 19.5 ms | 17.5 ms |

Enumeration adds nothing measurable, and 16,384 principals sweep in ~19 ms
against a 30 s interval. The planning estimate of ~5 s assumed network latency
per fetch; in-process there is none. **No batch-fetch method was added** —
adding one to every implementor on a hunch is what the measurement existed to
prevent. The HTTP regime is arithmetic rather than measurement: N round trips
bounded by `max_concurrent_fetches` (16) is ~100 ms on loopback and a few
seconds at 5 ms RTT, still inside the interval, and that is where batching
would first earn its keep.

## Churned catalogues (GL-52, 2026-08-24)

`TrackedPrincipals::All` (GL-48) tracks everything the source enumerates, and
enumeration returns every principal *ever published*, because revocation
tombstones must be retained (INVARIANTS.md GL-15). Steady-state control-plane
load was therefore proportional to lifetime principals rather than live ones.

The issue that raised this proposed skipping tombstones on the grounds that
their content never changes. **That premise is wrong**: a revoked principal is
reinstated by publishing at a higher generation, and skipping it would strand
the reinstatement until restart on the HTTP topology, which has no push. What
the issue missed is cheaper and real — negatives were fetched *twice per
cycle*, once by the full sweep and again on the negative TTL.

Two changes, and the safety argument comes first because it is what makes them
acceptable. **Revocation propagation is untouched.** A live principal is always
swept, and withdrawing one is a Present → Negative transition, so it still
lands within `refresh_interval`. What slows is the opposite direction — a
reinstatement, Negative → Present — which is an operator restoring an account
rather than withdrawing one. That is the safe half to trade.

1. The sweep covers what is *installed*: everything tracked whose resolution is
   not negative, plus principals not yet resolved (the initial load, and
   whatever discovery has just added). Negatives already have
   `due_for_refetch`, so sweeping them as well was duplicated work.

   One path deliberately keeps the unfiltered set: broadcast lag. A dropped
   push is most likely a reinstatement, Negative → Present, so filtering by
   local resolution there would skip exactly the principals the recovery
   exists to repair — assuming the answer in the one state that says local
   resolutions cannot be trusted. `all_tracked` and `due_for_sweep` are
   separate methods for that reason, and a unit test holds them apart.
2. Negatives take one of two TTLs, keyed on **what the source answered**.
   An absent row (`Unknown`) keeps the short TTL; a published tombstone
   (`Revoked`) takes `revoked_ttl`, because coming back means a reinstatement.

   The first cut keyed this on the merged generation instead — `Some(_)` was
   read as "once served, now withdrawn". That was wrong, and review caught it.
   A principal the instance has served resolves `Unknown` whenever the source's
   row is merely *absent*: a store rebuilding after restart, a lagging replica,
   a failover. Those inherited the hour-long reinstatement TTL, and since the
   sweep no longer covers negatives and `HttpStore::subscribe` is a closed
   channel, nothing would have repaired them — the instance denies a live
   customer for an hour while readiness still reports healthy, because a
   negative counts as resolved. A tombstone is a statement the source
   published; an absence is not, and `NegativeKind` now makes the two
   impossible to conflate at the three call sites that build a negative.

Measured with a churned fixture: a catalogue of N entries with a tenth of them
live. The all-live fixture cannot show this at all, which is why the benchmark
could not see the problem before. One sweep, criterion medians, all three
columns from one run on one laptop — an untuned host, so read the ratios and
not the milliseconds.

| principals | all-live | churned, before | churned, after |
|---|---|---|---|
| 512 | 2.77 ms | 2.88 ms | 2.38 ms |
| 4,096 | 6.04 ms | 7.20 ms | 3.00 ms |
| 16,384 | 18.8 ms | 23.2 ms | 5.40 ms |

The "before" column is the finding: a churned catalogue cost *more* than an
all-live one of the same size — 24% more at 16,384 — while serving a tenth as
many principals, because a negative resolution touches two deadline indexes
where a positive touches one, on top of being fetched twice.

The "after" column is one sweep, and a sweep is now the live tenth: 4.3x at
16,384. Precisely: the *fetch count* tracks live principals rather than
catalogue size, which is the property the issue asked for. `due_for_sweep`
itself still walks the whole tracked set with a hash lookup per entry and
allocates its `Vec` each pass, so per-sweep CPU stays O(catalogue) — negligible
beside a fetch, and the reason the win is in fetches rather than in the 512-row
case, but not the structural claim the sentence would make unqualified. Note what it is not — tombstones did not become
free. They moved from the sweep, every `refresh_interval`, to `revoked_ttl`, so
that term drops by the ratio between the two rather than to zero — ~120x at
the cadence `pricing-api` configures (30 s and 1 h). There is no `Default` for
`SnapshotManagerConfig`; both are the deployment's to choose. A catalogue that grows without bound still costs
something without bound; what changed is the constant, and that reinstatement
rather than revocation is what pays for it.

Two more things review found, both in the seam this change opened.

The targeted refetch is now **capped per wakeup** at `max_concurrent_fetches`.
Before GL-52 every sweep re-armed each negative's deadline, so the refetch index
rarely fired; now a catalogue resolved in one initial load shares a deadline
and comes due together. The caller awaits that batch inline, so an uncapped
population would hold the select loop — and with it the `tick` arm, which is
what carries revocation within `refresh_interval`. Chunking loses nothing:
whatever is still due stays due, and the index is ordered by deadline, so the
earliest lead and the tail cannot starve. The cap reuses
`max_concurrent_fetches` rather than adding a knob, on the grounds that a
targeted refetch should never queue deeper than a sweep already would. Note
this hazard is *older* than GL-52 and was worse before it: the old sweep pushed
the whole catalogue through the same inline await every `refresh_interval`,
not once per `revoked_ttl`.

A defect the tests uncovered here, older than this change and outside it, was
tracked separately and fixed in GL-53: an `Unknown` resolution inherited the
generation of the positive it replaced, so a source returning at the *same*
generation could never restore the principal. See the GL-53 entry below.

One methodological note, since it bit twice here. The benchmark's stopping
condition counted `principals` fetches, which silently stopped meaning "one
sweep" the moment the sweep stopped covering negatives; each case now waits for
the number of fetches its own sweep performs. And two behavioural tests written
for this change passed for the wrong reason — a `ManualClock` never advances,
so no TTL ever elapses, and `MemoryStore` delivers a republish as a *push*, so
neither TTL had to be right. The pure TTL selection is pinned by a unit test
that discriminates both directions; the sweep filter by a counting source with
no push.

## Unifying account suspension (GL-51, 2026-08-24)

"Suspend this customer" had two implementations. `tollgate_accounts.active`
gated `LeaseAllocator::acquire`; the `AccountStatus` inside a published
snapshot gated admission. Nothing kept them equal and nothing reported the
disagreement, so an operator could call deactivate, get a 204, and watch the
account keep being served until its lease drained.

Calling that a bug undersells the missing half. There was no way to suspend an
account *at all* such that requests stopped: doing it by hand meant
republishing every snapshot of the account, and **nothing in the system could
enumerate them.** `tollgate_snapshots` is keyed by principal, and the account
lived only inside the JSONB. Most of this change is building that seam.

**The ledger became three-valued.** Renaming the operation while leaving
`active` a bool would have half-unified it: `Closed` stays unrepresentable in
the ledger, so "Closed is terminal" could only be enforced by inspecting
snapshots, in two hand-mirrored backends — the enforcement rung this repository
already knows drifts, having caught these two `set_active` bodies diverging
once before. As a status column, terminality is one comparison inside the
store, under the row lock that performs the transition.

**`set_active(bool)` was renamed, not reinterpreted.** The identical call now
also republishes every snapshot of the account, so every existing runbook and
script would have silently acquired a much larger blast radius. That is the
config-contract rule's paradigm case, and renaming is its prescribed remedy:
`set_account_status(AccountStatus)` makes every caller confront the new
semantics, and a stale `{"active": false}` body now fails loudly instead of
half-working.

**The account column is derived, not written.** `StoredId` serializes an id
that fits `u64` as a JSON number and anything larger as a 32-hex string, so
`snapshot->>'account_id' = $1` matches one spelling and silently misses the
other — this issue's own defect, one layer down. The column is
`GENERATED ALWAYS AS (...) STORED`, which leaves exactly one writer for the
(`snapshot`, `account_id`) pair: PostgreSQL, evaluating a function of the row.
A column the Rust write path filled would have been a second writer of the same
fact, which is the thing being removed. The number branch never casts through
`bigint` — ids in `[2^63, 2^64)` are legal and overflow it — so the value is
split into two sub-2^32 halves; `the_account_column_is_derived_for_both_stored_id_spellings`
covers both branches through behaviour rather than by reading the column.

**The republish is `jsonb_set` in SQL, not read-modify-write in Rust.** Any one
of three reasons decides it. RMW reintroduces the original bug: a concurrent
`publish_snapshot` between the read and the write makes `generation + 1` no
longer greater than stored, and the monotonic guard then *silently drops the
status change* for that principal. RMW loses fields, because `StoredSnapshot`
has no `flatten` and would discard what a newer binary wrote. And RMW fails
whole on one bad row, so one corrupt credential could block suspending an
account. (The generation was stored twice at the time — a column and a JSONB
field — and both were bumped in that same statement. GL-54 removed the JSONB copy,
so the statement now patches only `status`.)

**`publish_snapshot` now refuses a contradicting status.** Without it the
unification closes the operator action but not the pattern: a publish could
recreate the disagreement one principal at a time. A snapshot for an account
the ledger does not hold still publishes unchanged, so this adds no
account-existence requirement.

Two consequences worth stating rather than discovering. The store now advances
generations that a control plane also assigns, so a publisher using a local
counter can find its next publish dropped by the monotonic guard; the bump is
the minimum `+1`, and reinstatement goes through the same call, so an operator
never depends on the control plane to undo a suspension. And `Closed` is
reachable in production for the first time, so `DenyReason::account_closed` and
`account_suspended` stop being counters that could only ever read zero.

Alternatives rejected: keeping the bool and deriving `status` at
snapshot-compile time, which pushes reconciliation into every embedder; and
detecting the divergence without unifying it, which satisfies the "not silent"
half of the issue while leaving two operator actions with two latencies.

## One stored copy of a snapshot's generation (GL-54, 2026-08-24)

The sibling search GL-51 ran found one other instance of its own defect shape, in
the same table: `tollgate_snapshots.generation` was a `BIGINT` column *and* a
field inside the `snapshot` JSONB, with no CHECK and no derivation keeping them
equal.

**No code path could actually produce a disagreement** — every writer wrote both
from one Rust value, and both `generation` references in GL-51's republish read
the same pre-update row. This is defect-class removal, not an outage fixed. What
made it worth doing anyway is that the two copies were read by *disjoint*
consumers: the column by the `ON CONFLICT ... WHERE` monotonicity guard and by a
tombstone's watermark, the JSONB by every live `Present` resolution. So one row
could answer two different generations depending on which branch a reader
reached, and only an external writer had to slip for that to become visible.

**The column won, and the issue's own proposal lost.** GL-54 proposed deriving the
column from the JSONB with `GENERATED ALWAYS AS`, mirroring what GL-51 did for
`account_id`. Deleting the JSONB copy instead is strictly cheaper: it needs no
table rewrite, leaves the monotonicity guard untouched, and — the point worth
claiming out loud — **sidesteps the verification question that blocked the
issue** entirely, namely whether `EXCLUDED` exposes a computed generated column
to an `ON CONFLICT ... WHERE` predicate. That question is now moot rather than
answered.

This leaves `tollgate_snapshots` carrying two opposite conventions for two
adjacent facts: `account_id` is derived *from* the JSON, `generation` is absent
*from* it. Both are "one writer per fact"; they differ because the generation
needs a BIGINT comparison the JSON cannot provide, and because deleting
`account_id` from the JSON would NULL its generated column and silently make an
account-wide status change republish nothing. `StoredSnapshotRef`'s doc comment
states both rules together, since that is where someone would go to break one.

Migration 0007 came with it, and is the part that was nearly missed.
`tollgate_snapshots.generation` was the only BIGINT counter in the schema
without a non-negative CHECK — 0003 and 0004 gave one to every other, including
the identically-shaped `next_fence`. The gap was invisible while the live read
path never touched the column; moving the column across the trust boundary is
what makes a negative value able to fail a live read, so the write-side guard
had to move with it. The same migration adds
`CHECK (jsonb_typeof(snapshot) = 'object')`: a scalar there is admitted by
`IS DISTINCT FROM` and then raises inside `jsonb_set`, so one malformed row
could block suspending an entire account.

Old rows keep a vestigial `generation` key. Nothing strips it — that would be a
full table rewrite for cosmetics — so the table is heterogeneous on purpose, and
`a_vestigial_jsonb_generation_is_ignored_in_favour_of_the_column` pins that the
reader ignores it.

**One-way.** A pre-GL-54 binary cannot decode a row written after it, so rolling
the binary back degrades availability — every live read for an affected
principal returns a store error. It does not degrade authorization safety: the
tombstone path reads the column, so revocation keeps working, and a failed
decode denies rather than admits. Roll forward. A staged variant (keep writing
the field, stop reading it) was rejected: it makes the field write-only, which
removes the one assertion that can enforce the property and leaves the
duplication under another name.

`MemoryStore` had the identical shape and got the same treatment by a different
mechanism. `SnapshotRecord` was `{ generation, snapshot: Option<_> }`, holding
the generation twice whenever a snapshot was present. The field could not just
be deleted — revoking sets the snapshot aside, and its generation is then the
only surviving watermark — so the struct became an enum whose two states each
carry the generation in exactly one place.

## An absence is not a revocation (GL-53, 2026-08-24)

A principal whose source row went briefly *absent* was recorded as a negative
carrying **the generation of the positive it replaced**, and the positive gate
then refused anything at or below that number. A source coming back with the
same generation — which is what a transient absence produces, nothing having
changed — could never restore it. Only a higher-generation republish revived
it, and nothing in the control plane guarantees one.

**The severity was worse than "denied until a republish".** The sweep's gate
discarded the returning snapshot with a `continue` placed *before* the
principal was marked completed, so it was reported failed and sent to
`back_off` — which moves only `next_refetch`, never `deadline`. The instance
retried forever, was refused every time, and **readiness never dropped**. A
permanently denied customer on an instance reporting itself healthy, with the
retry loop hiding the symptom rather than surfacing it.

**Root cause: the pipeline knew the distinction at both ends and discarded it
in the middle.** `SnapshotResolution` separates `Revoked { generation }` from
`Unknown`; HTTP preserves it losslessly (a 410 must carry a generation, a 404
never can and never could); the store's own record separates them. It was lost
at exactly one place — a bare `Option<Generation>` on the client's negative
resolution and on the map's watermark, where `None` meant "no generation known"
rather than "the source made no claim".

That is GL-52's own lesson, one field over. GL-52 fixed TTL selection with the same
sentence — keyed on what the source answered, never on what the instance
remembers — and added `NegativeKind` to carry it. GL-53 is that principle applied
to admission: `NegativeKind` already existed at both negative sites and was
being thrown away after picking a TTL.

**The rule.** A watermark now records *why* it exists. Refuse a strictly older
generation always. Refuse an equal one only when it is dead (a published
revocation) or already installed (a visible entry). Otherwise admit it — a
re-observation, not a resurrection. The visible-entry clause is what keeps a
duplicate publish of a live snapshot an idempotent no-op instead of a fresh
copy-on-write install of the whole map.

**One rule, not two that agree.** The `<=` comparison existed twice, in two
crates: `generation_model::accept_positive`, and the manager's own gate, which
short-circuits *before* the map is ever called. Fixing the map alone would have
changed nothing, because the client never emitted the update. Rather than keep
two copies in step, `generation_model` is now public and the manager calls it.
Two copies that must agree is how this survived in the first place.

**The proof was proving the bug.** `SnapshotCache.lean` had a single
`watermark : Option Nat`, so composing its own definitions —
`installUnknown (installPositive s g)` then
`positive_at_or_below_watermark_is_rejected` at `incoming = current = g` —
derived that the principal could never return. That was a machine-checked
theorem of the defect. `watermark` is now `Option Watermark`, matching the
Rust, and what moved is this:

- `unknown_preserves_watermark` and `eviction_preserves_watermark` are
  **unchanged**, and that is the point rather than an oversight: preserving the
  watermark was never the defect. Reading it as a tombstone was. What they
  preserve now carries its own provenance.
- `unknown_never_creates_a_revocation` is **new**, and is the property whose
  absence was the bug.
- `positive_at_or_below_revocation_is_rejected` is the old
  `positive_at_or_below_watermark_is_rejected`, **restated over revocations
  specifically** — it is the theorem that used to justify the defect, and
  narrowing its subject is the fix.
- `older_revocation_cannot_revoke_a_newer_positive` is **new**: see below.
- `revoked_then_evicted_rejects_replay` **narrowed** to a revocation pre-state,
  because it is false at equality for a positive one. Its coverage is restored
  by `revoked_then_evicted_rejects_replay_from_any_watermark`, which quantifies
  over every pre-state and concludes the principal stays dead — so GL-15's
  headline is unweakened in fact and not merely in prose.

A first cut of the model split the watermark into *two* fields, and review
caught that this broke the correspondence the file exists for: with separate
records a delayed *older* revocation passed the revocation check while a newer
positive sat in the other field, so the model admitted a transition
`accept_revoked` refuses — silently losing GL-15's second half, that a delayed
older tombstone cannot revoke a newer positive. One field fixed it, and that
half is now its own theorem.

A second review round caught the paragraph above describing the *two-field*
model after the model had already been rewritten to one — theorems reported as
split or narrowed that were byte-identical to `main`, and the one that had
genuinely narrowed reported as untouched. Worth recording because of where it
happened: in the passage a reviewer reads to decide whether the proof
obligation was met.

Note what the proof does **not** cover, since a green `formal` job is easy to
over-read: it models `generation_model.rs` only. The manager's `Resolutions`
have no Lean counterpart, and that is the layer which gates first.

**A test's safety claim rested on the defect.** `exercises_map` installed an
unversioned negative and asserted a delayed generation-1 push "cannot resurrect
the revoked principal" — but nothing had revoked it; the assertion passed on the
positive's own generation being treated as a tombstone. It now asserts ordering
against the absence and revocation against an actual revocation, which is what
it was always meant to say.

The refusal path is throttled, and that took two attempts. A refused answer is
left out of the completed set on purpose, because that set is what re-arms
`next_refetch`. A first cut moved refusals *into* it, reasoning that a refusal
is not a failed fetch — true, and beside the point: without the re-arm a
negative's deadline stays in the past, the control wakeup re-fires at zero
delay, and the client refetches at source latency. Measured: 410 fetches in
600 ms against a replica serving a stale generation, where the backoff gives
three. GL-53's severity came from the refusal being *permanent*, not from the
throttle. What that cut was right about is that the refusal was invisible, so
it is now logged rather than absorbed, and
`a_refused_answer_is_retried_with_backoff_not_at_source_latency` pins the rate —
which nothing did before, which is why it could be removed silently.

Two smaller consequences, both found in review rather than while writing the
change. At GL-53 the manager passed its *own* resolution as the "visible" input to
the shared rule. Asking the map through an ordinary lookup would make every
sweep record a synthetic read against every tracked principal, which on a
`moka` cache feeds its frequency sketch and biases eviction toward whatever the
sweep touched. Passing `false` accepts an equal generation, so an unchanged
catalogue yields an update per principal per sweep — and on the copy-on-write
map, a clone of the whole map that previously did not happen at all. The cost
of that choice was that a map which evicted a *present* entry behind the
manager's back was not repaired by a same-generation refetch. GL-67 closes this
sibling gap with `contains_cached`: Moka's membership probe does not record a
frequency hit, and the manager combines it with its owned resolution before
classifying a duplicate. History reclamation additionally removes the resolution
before the fresh read. And `remove` quietly changed meaning: a watermark left by a
positive no longer refuses its own generation, so re-fetching an evicted
generation repairs the entry. That is the point rather than a side effect, and
it is now stated on the trait method and pinned by a test, since the existing
`remove` assertion reached that line with a *revocation* watermark and so only
ever covered the other half.

The suite had also never exercised equality on the accept side at all: every
recovery test stepped strictly *over* the watermark, which is why the whole
class stayed invisible. The pair that now separates the two rules —
`a_live_principal_that_goes_absent_recovers_on_the_unknown_ttl` returning at its
own generation, and `a_revoked_principal_stays_tracked_and_cannot_be_resurrected`
refusing at its own — differ only at that generation.

## Staged admission interface (GL-96, 2026-08-28)

This section is the target contract for GL-91, GL-92, GL-93, GL-94, and GL-99, not a
claim that the target has already shipped. Those issues used to leave the
public `AdmissionEngine::admit` and `Reservation` changes to be designed in
their individual merge requests. That would make the consumer adapt to a
sequence of temporary interfaces, and it would let the temporary shape of the
first implementation constrain the final one. The signatures below are the
one reviewed destination. An implementing issue that needs to diverge changes
this section first; it does not silently publish a different seam.

**Two stages mean one lookup, not two partial admissions.** Authentication
produces a `Principal`; `begin` resolves that principal once, checks account
status, snapshot freshness, and the route-level permission, and returns an
owned context:

```rust
impl<M: SnapshotMap> AdmissionEngine<M> {
    pub fn new(map: M) -> Self;
    pub fn map(&self) -> &M;
    pub fn counters(&self) -> &AdmissionCounters;

    pub fn begin(
        &self,
        principal: Principal,
        required: PermissionBits,
        now: Timestamp,
    ) -> Result<RequestContext, DenyReason>;
}

pub struct RequestContext {
    // One Arc<AccountAdmissionState> and the Locality selected by begin.
}

impl RequestContext {
    pub fn snapshot(&self) -> &AccountSnapshot;
    pub fn limits(&self) -> &ResolvedLimits;
    pub fn generation(&self) -> Generation;
    pub fn policy_revision(&self) -> PolicyRevision;
    pub fn estimate_remaining(&self) -> Option<CostUnits>;

    pub fn admit<O: OpIndex, S: UsageSlot>(
        self,
        workload: &[(O, u64)],
        slot: S,
        now: Timestamp,
    ) -> Result<Pending<S>, DenyReason>;
}
```

Taking the route permission in `begin` is a deliberate correction to the
short `begin(principal, now)` sketch in GL-96: GL-91 requires route authorization
before the body is allocated. `RequestContext` owns the one
`Arc<AccountAdmissionState>` returned by `SnapshotMap::get_at`, rather than
cloning its `Arc<AccountSnapshot>` into a parallel object. The state already
owns the immutable snapshot, account limiter, lease slot, and principal-local
runtime evidence. The context has no engine borrow or request lifetime; it is
`Send + Sync + 'static` and can cross the body-read `await` as a plain
associated type in an embedding service's policy port. `admit` consumes it, making one
lookup authorize at most one compiled request and moving the same state into
`Pending` without another refcount operation.

`begin` resolves `Locality::current()` exactly once and stores the result.
Tokio may resume the task on another worker after the body read, but stage two
continues against the lease and limiter view chosen at stage one. Locality is
an affinity hint, never authorization evidence: selecting another valid shard
would preserve conservation, but would discard the cache locality the sharded
layout exists to buy.

The principal snapshot is generation-pinned in substance, not merely by
pointer name: status, permissions, request shaping, pricing, funding mode,
class, and revision come from the one immutable snapshot the lookup returned.
Account-wide rate and concurrency policy cannot be pinned per principal,
however. Doing so left old and new principals spending independently
refillable governor buckets and let divergent concurrency values apply
different bounds to one shared gauge.

`AccountLimiter` therefore publishes one `AccountPolicyState`. The request
path loads that state exactly once and uses it through both optional rate
checks and account-concurrency acquisition, so those decisions come from one
accepted policy generation. All principals beginning after a publication
read the same authority. Stable account and principal gauges remain shared
across publications, because resetting live occupancy would make later limit
activation overlook work already in flight. `RateState` carries the exact
`AccountRatePolicy` its buckets implement, so an enabled policy cannot lack
its bucket and a disabled policy cannot accidentally execute one.

Stage two rechecks `now >= valid_until` before using the context. It does not
recheck status or look in the map: a suspension published after `begin` takes
effect on the next request, while the already-begun request remains governed
by its pinned generation for at most the bounded body-read interval. The rest
of the stage-two order is fixed:

1. OR-fold the direct-indexed per-class work permissions and perform one
   `contains_all` check;
2. checked-sum the item counts and enforce `max_items_per_request`;
3. quote the workload with checked arithmetic;
4. take one request-count token if that bucket is configured;
5. take the cost-weighted token if that bucket is configured, retaining the
   existing whole-burst precheck;
6. acquire the narrowed principal concurrency gauge and then the account
   gauge; and
7. reserve funding from the lease or, under `Elastic`, the overage counter.

A later refusal releases any concurrency gauge already acquired. Rate tokens
are not refunded: the request arrived and was priced, and refunding a token on
funding or capacity failure would amplify an overload retry loop. No check
above funding changes with `EnforcementMode`; unknown, stale, unauthorized,
malformed, unpriceable, throttled, or backpressured work still fails closed
under both modes.

**The accounting slot is evidence, not a callback convention.** The generic
trait lives in `tollgate-core`, below both admission and the concrete client
writer, and represents capacity already reserved from a bounded usage sink:

```rust
pub trait UsageSlot: Send + 'static {
    fn record(self, event: UsageEvent);
}
```

`tollgate_client::UsagePermit` implements `UsageSlot`. Its implementation is
infallible because `UsageRecorder::try_reserve` acquired the channel slot
before admission; dropping it before commit releases that slot without an
event. A custom implementation inherits the same contract: it is verified
capacity for exactly one event, not permission to perform fallible I/O from
`Committed::drop`.

The supported request order is authenticate, `begin`, reserve the usage slot,
read/decode under `ctx.limits()`, then call `ctx.admit`. Reserving after
`begin` means an unknown or unauthorized credential never occupies the usage
queue, while reserving before body allocation still sheds backpressure before
expensive input work and remains "before admission" for INVARIANTS GL-8: the
stage that can create pending funding has not run. A body-read failure drops
the context and slot and creates no funding reservation.

**A workload is a borrowed list of compiled class aggregates.** The public
form is `&[(O, u64)]` with `O: OpIndex`: a caller may use a stack array or its
own fixed-capacity buffer, and Tollgate owns no `Vec`, const-generic capacity,
box, hash table, string, or product identifier. A const-generic
`[(usize, u64); N]` was rejected because it monomorphizes the provider seam for
each `N`, forces an abstract port to name a product-specific capacity, and
still makes dense callers zero-pad the unused tail.

`CostTable::quote_workload` is the one owning formula:

```text
variable = Σ weight[class] × count
total    = max(fixed_request + variable, minimum_charge)
```

Every multiplication and sum is checked. The fixed term and minimum apply
once to the whole request. Unknown classes produce `UnpricedOperation`; an
empty or all-zero workload produces `EmptyWorkload`; overflow produces
`CostOverflow`. Repeated classes are summed by the same fold, never quoted as
separate requests, so they cannot apply the fixed term twice. The compiled
workload contract requires one aggregate per distinct class: the normal path
is O(distinct classes), while a repeated entry is tolerated for correctness
and costs one additional bounded entry. The consumer's structural request
envelope bounds the caller-owned entry buffer; Tollgate's item cap bounds the
checked sum of its nonzero counts. `CostTable::quote(op, items)` becomes the
one-element call into this formula, eliminating the second arithmetic path.

Work permission remains compiled and direct-indexed. `CostTable` gains a
`#[serde(default)]` permission-bits array parallel to its weights. A missing
entry means `PermissionBits::NONE`; canonical serialization trims trailing
`NONE` entries and skips an all-`NONE` array, so a table decoded from legacy
JSON compares equal to the equivalent newly built table. The builder's
`.class(op, weight, required)` sets both values and the existing `.weight`
convenience sets `NONE`. GL-92 owns the golden/property/mutation witnesses,
including `legacy_cost_table_round_trips_canonically`, and generalizes
`SnapshotLimits.lean` from one weight to the checked sum. The maximum
publication quote remains fixed plus the largest registered weight times the
item cap, because the workload's total item count cannot exceed that cap.

**Pending funding, execution capacity, and committed work are three distinct
type states.** Capacity acquisition consumes the exact `Pending` whose
generation-pinned class it evaluates. A detached permit cannot be swapped
between requests, so a best-effort request cannot commit using a permit an
assured request obtained from the reserve:

```rust
pub struct Pending<S: UsageSlot> {
    // Snapshot state, funding reservation, concurrency guard, usage slot,
    // quote, and an owned-or-shared execution-control state.
}

impl<S: UsageSlot> Pending<S> {
    pub fn quote(&self) -> CostQuote;
    pub fn snapshot(&self) -> &AccountSnapshot;
    pub fn limits(&self) -> &ResolvedLimits;
    pub fn policy_revision(&self) -> PolicyRevision;
    pub fn estimate_remaining(&self) -> Option<CostUnits>;

    pub fn acquire_capacity<G: CapacityGate>(
        self,
        gate: &G,
    ) -> Result<ReadyToStart<S, G::Permit>, (DenyReason, Released)>;

    pub fn cancel(self) -> Released;
}

pub struct ReadyToStart<S: UsageSlot, P: CapacityPermit> {
    // The Pending state and the permit obtained for that same state.
}

pub trait CapacityPermit: private::Sealed + Send + 'static {}

impl<S: UsageSlot, P: CapacityPermit> ReadyToStart<S, P> {
    pub fn split(self) -> (Self, CancelHandle);

    #[must_use = "the kernel may run only while holding the returned Committed guard"]
    pub fn commit(
        self,
        request_id: RequestId,
        now: Timestamp,
    ) -> Result<Committed<S, P>, (CommitError, Released)>;

    pub fn cancel(self) -> Released;
}

pub struct CancelHandle {
    // One Arc<SharedExecutionControl> created by ReadyToStart::split.
}

impl CancelHandle {
    pub fn cancel(&self) -> CancelOutcome;
    pub fn is_cancelled(&self) -> bool;
}

pub struct Released {
    // Typed evidence that the request resolved with zero charge.
}

pub struct Committed<S: UsageSlot, P: CapacityPermit> {
    // UsageEvent, usage slot, concurrency guard, capacity permit, and the
    // shared control when split was selected.
}

impl<S: UsageSlot, P: CapacityPermit> Committed<S, P> {
    pub fn units(&self) -> CostUnits;
    pub fn request_id(&self) -> RequestId;
    pub fn policy_revision(&self) -> PolicyRevision;
    pub fn estimate_remaining(&self) -> Option<CostUnits>;
    pub fn is_cancelled(&self) -> bool;
}
```

`Pending` and `ReadyToStart` are `Send + 'static`, not `Clone`, and allocate
nothing. Dropping either before commit releases pending funding for zero
charge. Consuming `ReadyToStart` makes a second commit by the same owner and a
same-owner commit after cancel unrepresentable. `Released` is a terminal proof
rather than a container that invites the caller to reassemble a reservation;
it preserves the explicit `(CommitError, Released)` contract required by GL-96.
Only `Committed` proves that the kernel may run.

The engine's own three — `new`, `map` and `counters` — were published from
the start and declared here only in prose, which is how GL-126 found them: the
check it added reads code, not English, because "new" and "map" are ordinary
words and a scan that accepted prose would have called them declared.

**`estimate_remaining` answers "how much is left", and it is on all three
response-producing stages because that question has to be answerable on a
denial too.** `RequestContext` is the stage a denied request still holds,
`Pending` is what a cancelled one holds, and `Committed` is what a served one
holds; a caller that could only ask after committing could not put the number
on the responses that most need it.

The name is the contract. It is what the ledger last reported minus what this
instance has admitted since, so it is wrong by the fleet's spend elsewhere and
by this instance's own cancellations, and it reads low against the ledger far
more often than high — the safe direction for a number a customer acts on.
`AccountAdmissionState::estimate_remaining` states the two error terms and
their bound; that doc comment is the definition and is deliberately not
duplicated here.

Two rules follow, and a consumer that breaks either has misread the seam.
**It is never an authorization input** — admission denies from the lease and
the ledger (INVARIANTS GL-1), never from this, because a stale estimate that
could deny would turn a refresh delay into an outage. And **"left this period"
is not "spendable right now"**: the two diverge observably, which is what the
`GrantPolicy` tail-grant finding below records — `estimate_remaining` reported
58 units while every 51-unit request met `LeaseExhausted { remaining: 49 }`
until the lease's TTL.

It was published by GL-97 without this section being amended first, and GL-126 is
that correction. `scripts/check_seam_contract.sh` now fails a public seam
method this section does not name, so the rule above has a backstop rather
than only a reviewer.

`ReadyToStart::split` is optional and is the one allocation permitted by GL-93.
It moves the reservation's phase, charge source, and cancellation flag into
one shared object, returns the worker-owned `ReadyToStart`, and gives the async
waiter a `CancelHandle`. An inline executor does not split and stays
allocation-free. A consumer that needs a timeout/worker race splits before it
crosses the executor boundary; a consumer that first moves an unsplit value to
the worker has deliberately chosen no external cancel race. GL-90 reports the
split allocation separately from the allocation-free admission path.

Cancellation and commit still race on the reservation's single phase
compare-exchange. A winning cancellation refunds funding immediately. The
usage slot, concurrency guard, and capacity permit remain owned by the
worker-side value and are released exactly once when that value observes the
cancellation and drops; the API does not claim the `CancelHandle` can move
opaque RAII values out of another thread. The consumer executor must discard
canceled queued jobs and quiesce them during shutdown. GL-93's concurrency tests
cover that handoff, and GL-99 covers capacity release. Once commit wins, a late
cancel reports `AlreadyCommitted { units }`, the full charge stands, and the
worker may poll `is_cancelled` while deciding whether to stop computation.

`Committed::drop` records the prebuilt event through the reserved `UsageSlot`
and then releases account concurrency and execution capacity. The consumer
scopes it to the computational kernel, never response serialization or an
unrelated async wait. Tollgate does not run arbitrary kernels, so the consumer
executor owns `catch_unwind`; in particular, Rayon's detached `spawn` needs a
consumer panic boundary if a panic is to become a service result rather than
reach Rayon's default abort handler. Under the production profile's
`panic=abort`, unwinding does not exist and INVARIANTS GL-13 retains its stated
process-loss boundary.

**The transition counters cost one atomic each, and the baselines say so
(GL-93).** GL-20 asked for every phase of a request's life to be counted. Counting
a phase means one relaxed atomic increment on the path that reaches it, and on
a ~113 ns admission that is visible. Measured on the controlled host, against
`main` at the same revision rather than against the older recorded numbers:

| id | main | with the counters | delta |
|---|---|---|---|
| `admission/begin` | 15.07 ns | 17.16 ns | +2.1 ns |
| `admission/full_check` | 113.48 ns | 119.54 ns | +6.1 ns |

Both deltas are one atomic increment and nothing else. `admission/begin`
constructs a context and drops it, which is now an *abandonment* and therefore
a counted outcome — the benchmark measures begin-and-abandon, and the extra
~2 ns is `contexts_abandoned`. `admission/full_check` runs begin, admit, and
cancel, so it pays `canceled_before_start` once through the execution-lifetime
guard's `Drop`. The recorded baselines are updated to those measurements; the
absolute `threshold_ns` bounds are untouched and both remain far inside them.

The cost is irreducible rather than an implementation choice: the increments
are already `Relaxed`, the two per-request counters shard alongside `admitted`,
and the bounded rare outcomes stay inline so they add no contention class. What
is *not* paid is a second `Arc`: `RequestContext` holds `Option<Arc<_>>` so
`admit` can move the pinned state out rather than clone it, which is what a
`Drop` on that type would otherwise have forced. `Option<Arc<_>>` is
niche-optimized to the `Arc`'s own size, so the disarm is one null write and no
refcount traffic on the hottest staged path.

The execution-lifetime concurrency guard is where the terminal tally lives,
because it is the one value every admitted request holds exactly once and it
already owns the state the counters hang off. That makes "every admitted
request reaches exactly one terminal counter" true by construction — including
for a pending state that is simply abandoned, which no call-site convention
would have caught — at the cost of two words on a per-request guard. The
guard's size contract was updated deliberately rather than relaxed.

**The shared cancel state as built (GL-93).** `ReadyToStart::split` returns the
same worker-owned value plus a `CancelHandle` over one `Arc<SharedCharge>`. The
shared object holds the reservation and a cancellation *flag*, and the two
answer different questions. The reservation's compare-exchange decides whether
the request charged — it is the only authority, and a late cancel reports
`AlreadyCommitted { units }` against a charge that stands in full. The flag
records only that someone *asked*, set before the phase is attempted so a
worker that wins the race still observes it; `Committed::is_cancelled` exposes
it so a long kernel can stop computing work whose caller has gone. Ordering the
two the other way would let a committed worker read `false` for a cancellation
that had already returned `AlreadyCommitted` to its caller.

The worker's half is a distinct `WorkerShare` guard rather than a bare `Arc`,
and its `Drop` releases the reservation eagerly. An owned reservation is
released by `Reservation::drop`, but a shared one is co-owned by the handle, so
waiting for the reservation's own drop would hold funding until the
*asynchronous* side also let go — an unbounded interval after the worker
abandoned the request, during which the account's own retries see capacity
nothing is using. The guard sits inside the `Funding` enum rather than on it,
because `commit` and `split` both destructure the staged types and a `Drop` on
the enum would make that impossible without `unsafe`.

`split` costs exactly one allocation, held to that count by
`check_allocations.sh` under a `tollgate_opt_in` attribution rather than merely
exempted from the allocation-free rule — an exemption that cannot fail would
witness nothing. The unsplit scopes stay at zero and are the comparison a
consumer gets when it does not need a timeout/worker race: 38.7 ns for
`lease/reserve_commit` against 70.8 ns for `reservation/commit_split`.

**The panic boundary is the consumer's, and the reason is structural.**
Tollgate never runs the kernel, so it cannot wrap it. What Tollgate owns is
making its own drop safe to run while unwinding: `Committed` builds its usage
event at commit rather than at drop, so `Drop` takes no lock that could be
poisoned, allocates nothing, and cannot fail — and the shared cancel path is a
compare-exchange precisely so it remains usable from a thread that is already
panicking, which a `Mutex` would not be. What the consumer owns is the
`catch_unwind` (or equivalent) around the kernel. This is not hypothetical for
the target topology: a panic in a Rayon `spawn` closure propagates at the join
and can abort a pool thread, so without that boundary the guard is *leaked*
rather than dropped on the worker, and a leaked guard emits nothing. Under the
`production` profile's `panic=abort` none of this exists and INVARIANTS GL-13
keeps its stated process-loss boundary. `a_panicking_kernel_under_catch_unwind_still_bills`
is the witness for Tollgate's half.

**Commit-time Elastic fallback is one transition, not a second reservation.**
The terminal phase records the funding source explicitly:

```text
PENDING_LEASE ─┬→ COMMITTED_LEASE
               ├→ COMMITTED_OVERAGE
               └→ RELEASED

PENDING_OVERAGE ─→ COMMITTED_OVERAGE | RELEASED
```

When a leased reservation's window lapses at commit, `Strict` changes
`PENDING_LEASE` to `RELEASED`, refunds the lease receipt, and returns
`FundingExpiredAtStart`; the kernel must not run. `Elastic` first attempts a
tentative debit against the overage cap without changing the phase, then
compare-exchanges `PENDING_LEASE` to `COMMITTED_OVERAGE`. If commit wins, it
refunds the original lease receipt, retains the overage debit, emits an
`Overage` event, and records the commit-time overage qualifier. If cancellation
won, it refunds the tentative overage debit; cancellation already refunded the
lease. If the overage debit itself fails, the request is released and returns
`OverageCapTemporarilyExhausted` when refundable reservations are the reason it
cannot fit, or `OverageCapExhausted` when committed local occupancy is the
reason. Both remain transient because a later lease grant can fund the
unchanged request; neither is mislabeled as `FundingExpiredAtStart`.

**Implementation correction (GL-93): the debit has three refusals, not two.**
The paragraph above enumerates the two stable-occupancy reasons, but
`AccountOverage::try_debit` also returns `OverageCommitInProgress` when a
sibling publication overlaps the refusal, and that reason is `AfterInFlight`
rather than `Transient`. The fallback therefore propagates whichever of the
three the counter produced, verbatim. Folding the third into either of the
others would tell a caller to retry immediately against units that are already
irrevocable — the precise lie `DenyReason::retry()` exists to prevent — so the
commit path classifies nothing itself and the counter stays the one authority.
`CommitError::LeaseExpired` was retired in the same change: it is payload-free
and cannot carry `OverageCapExhausted { spent, overage_cap }`, so core now
returns `CommitError::Denied(DenyReason)` directly and the admission layer no
longer translates one funding refusal into another.

**The tentative debit is owned by a guard, not by a comment.** Between the
debit and the claim the units sit in `spent`, and the first implementation left
no value responsible for returning them: a panic unwinding through that window
— or any early return a later edit adds — would leave `Reservation::drop`
refunding the *lease* while those units stayed stranded for the life of the
process, silently shrinking the account's cap with the suite green.
`AccountOverage::debit_tentatively` now returns a `TentativeOverage` guard that
credits on drop, and publication consumes it. That makes "debited but never
resolved" unrepresentable rather than merely untested, and it is also what lets
the publication marker's occupancy assertion hold: the debit is in `spent`
before the marker goes up, exactly as for a natively admitted overage.

The two program orderings are structural rather than commented. The debit
precedes the claim because `TentativeOverage::publish_commit` consumes the
guard — there is no way to claim without already holding a debit. The lease
credit follows the claim because it lives only in the `Ok(_)` arm of the
claim's result, outside the closure; hoisting it above would double-refund
alongside a canceller that won the same phase.

A first design that released the leased reservation and then created a second
overage reservation was rejected: a canceller could win the first phase and
report zero while the worker committed the second. The tentative-debit/single-
CAS rule preserves one authority. It also preserves the ledger equation:
successful fallback returns the lease units, retains one overage funding term,
and settles one usage event.

**Why the billing statement reads the phase.** `Reservation::usage_event`
selects `UsageSource` from the terminal phase, not from the funding receipt,
and that is load-bearing. A fallback happens *because* the lease's window
lapsed, so the allocator reclaims that lease shortly afterwards — and
`MemoryStore::ingest` rejects a `Leased` event naming a reclaimed lease,
because the reclaim already credited its full remainder and the units would
otherwise double-count. Billing a fallback against its receipt would therefore
drop the charge for work that ran, silently, on the exact path elastic mode
exists to serve. `Reservation::is_overage` was renamed `admitted_as_overage`
to stop the two questions sharing one name: it answers what *admission* found,
which is what the `admitted_overage` counter qualifier needs, while the billing
statement is the event's own source.

GL-93 delivered the transition model as `formal/lean/Tollgate/CommitFallback.lean`
— five phases, the CAS as a partial function, the double-charge exclusion, the
refuted `PENDING_OVERAGE -> COMMITTED_LEASE` edge, and the phase-driven billing
source — alongside its Rust property, concurrency, store-parity, and allocation
witnesses and the invariant text in GL-1, GL-2, GL-3, and GL-12.

**Account-wide limits keep stable occupancy and pinned policy separate.** The
request-count bucket is a second optional direct governor state beside the
optional cost-weighted state. Both are account-scoped and may use the configured
local sharding; a request-count token has weight one. Either bucket may be
absent, and a snapshot carrying both must pass both.

The account-rate subset is a domain value, `AccountRatePolicy`, rather than
three independently copied fields. `RateState` owns that value beside the
buckets it built, and `AccountPolicyState` publishes that rate state together
with the canonical account concurrency ceiling. The request path loads this
authority once; it never chooses account-wide behavior from a principal's
possibly older snapshot. `formal/lean/Tollgate/RatePublication.lean` proves
that rejected publications leave the authority unchanged, accepted generation
ordering selects the canonical policy, principals read one current authority,
and changing either rate dimension preserves the other's mutable authority.

This publication is downstream of authorization acceptance. Both map
implementations consult and update the principal watermark before resolving
the account registry. The copy-on-write bulk path first simulates the batch's
accepted generation transitions, removes accepted positives overwritten later
in the same atomic batch, and only then resolves the surviving positives under
one registry lock. Rejected or never-visible data therefore cannot become an
account policy authority.

Concurrency occupancy lives on the stable account limiter and principal gauge,
outside replaceable policy state. While a ceiling has never been enabled, each
gauge direct-indexes cache-isolated counters with the request's existing
`Locality`; this tracks every request without undoing the opt-in sharding
topology. First activation changes the gauge to `draining` before publishing
the bounded policy. Draining seals the shards to new permits; the exact old
permits release to their owning shards, and the empty shard set is the
evidence that permits an atomic promotion to one central CAS-bounded counter.

Draining is not a closed window. Failing every acquisition closed for the
duration of the drain made publishing a ceiling an account-wide outage bounded
only by the longest request already running — and, because the drain waits on
permits held for the entire execution lifetime, one slow handler denied every
principal of the account with the ordinary saturation reason. Draining now
admits: an acquisition whose own policy carries no ceiling is never refused by
another policy's activation, and one carrying the activated ceiling takes a
central permit bounded by that ceiling less the shard residue. The residue is
live work that occupies the ceiling being published, and shards only shrink
while draining, so a scan that races a release is conservative in the safe
direction and total occupancy never passes the ceiling. The cost is one
bounded shard scan per ceiling-carrying acquisition, confined to the handoff;
the alternative was an unbounded denial window. Once central, disabling
the ceiling changes only whether the bound is checked; occupancy remains in
that counter for any later re-enable.

The principal gauge is resolved from a
`Principal`-keyed registry of `Weak<PrincipalGauge>` values, with the same
amortized sweeping rule as `AccountLimiters`. An old context keeps the gauge
strongly reachable across map replacement or moka eviction; a reinstall
therefore cannot manufacture a fresh zero while work is still in flight.
Admission acquires the principal gauge first and the account gauge second,
undoing the principal acquisition if the account is full. A published
principal ceiling may narrow an account ceiling but never widen it. GL-91 owns
the exactly-once RAII tests and the concurrency model: no bounded acquisition
increments a full gauge, every acquisition releases once, first activation
promotes only once old shards drain while still admitting within the ceiling
it is activating, and the principal check cannot bypass the account check. Both gauges record occupancy even when their ceiling is absent,
so enable and disable/re-enable transitions apply to existing work instead of
a fresh zero. Both ceilings are per instance; the embedding product, not
Tollgate, decides how it interprets that multiplication.

`ResolvedLimits` stops using public struct literals. Its constructor requires
`max_items_per_request`; builders set optional weighted rate, request-count
rate, account concurrency, and principal concurrency. Rate pairs are
both-present or both-absent, and a principal ceiling is at most a present
account ceiling. The Rust model may expose `Option<RateLimit>`, but the wire
retains the existing `rate_units_per_second` and `rate_burst_units` scalars and
adds an explicit `weighted_rate_enabled` flag defaulting to `true`. Directly
changing the old scalars to `Option<u64>` was rejected: an old reader would
fail to decode `null` or a missing required field rather than safely ignore
the option. The additive flag preserves every legacy value's semantics; an
old reader of a disabled weighted bucket conservatively continues enforcing
the carried scalar pair. New request-rate and concurrency fields use
`serde(default)` and nullable/defaulted storage, so absence means unlimited.

**A denial owns its retry classification.** Seven variants join the existing
fixed vocabulary across the staged-admission work:

| Variant | Phase | Retry | Canonical HTTP mapping | Lands with |
| --- | --- | --- | --- | --- |
| `RequestRateLimited` | stage two | `Transient` | 429 `request-rate-limited` | GL-91 |
| `ConcurrencyLimited` | stage two | `Transient` | 429 `concurrency-limited` | GL-91 |
| `OverageCapTemporarilyExhausted` | quota | `Transient` | 503 `overage-cap-temporarily-exhausted` | GL-91 |
| `OverageCommitInProgress` | quota publication | `AfterInFlight` | 503 `overage-commit-in-progress` | GL-91 |
| `EmptyWorkload` | stage two | `Never` | 422 `empty-workload` | `quote_workload` |
| `FundingExpiredAtStart` | commit | `Transient` | 503 `funding-expired-at-start` | `ReadyToStart::commit` |
| `CapacityUnavailable` | capacity gate | `Transient` | 503 `capacity-unavailable` | the capacity gate |

Each row lands in the change that adds its producer, never ahead of it. The
enum is exhaustive for embedders and `index` assigns dense counter slots in
declaration order, so a reason declared early renumbers every later slot and
obliges every embedder to write an arm for a refusal nothing can emit. The
last three rows are therefore design, not present API: `DenyReason` carries
nineteen variants until `quote_workload`, `ReadyToStart::commit`, and the
capacity gate exist to produce them.

`DenyReason::index`, `NAMES`, and `COUNT` remain exhaustive forcing
functions. `DenyReason::retry()` returns
`Retry::{Transient, AfterInFlight, Never}` so GL-40's retry
distinction is not copied into each
embedder. `RateLimited`, `RequestRateLimited`, `ConcurrencyLimited`,
`SnapshotExpired`, `LeaseUnavailable`, `LeaseExpired`, `LeaseExhausted`,
`AccountingBackpressure`, `FundingExpiredAtStart`, and
`CapacityUnavailable` are transient. So are
`OverageCapTemporarilyExhausted`, when all pending refunds would make this
request fit, and `OverageCapExhausted`, when committed local occupancy prevents
that. The latter still cannot prove central account exhaustion; a background
lease refill can recover it without a deposit or policy change.
`OverageCommitInProgress` asks the caller to wait for an already-running state
publication, after which the same request receives one of those stable local
occupancy reasons.
Unknown, suspended, closed, unauthorized, malformed, oversized, unpriced,
unpriceable-under-limits, and overflowing work is not honestly repaired by an
immediate retry. Tollgate supplies no retry-after instant: deriving one would
add a clock contract the API does not have, and the current governor hint is
not preserved across the whole staged pipeline. Because `SnapshotExpired` is
repairable only by the background publication plane, pricing-api changes its
existing `policy-stale` mapping from 403 to 503 when it adopts this classifier;
keeping a transient operational outage behind a permanent authorization
status would make the type-level answer and the canonical embedder disagree.

`ReadyToStart::commit` reports cancellation as `CommitError::Cancelled` and a
funding refusal as `CommitError::Denied(DenyReason)`, where the reason is
`FundingExpiredAtStart`, `OverageCapTemporarilyExhausted`, or
`OverageCapExhausted`. Consuming the owner makes
same-owner double commit unrepresentable; the lower-level reservation's
`AlreadyCommitted` remains a programming-error result only during the legacy
API's deprecation window.

**Counters describe phases instead of forcing later refusals into the old
identity.** `AdmissionCounters` moves behind an `Arc` owned by the snapshot
map, which already owns the sharding configuration. `SnapshotMap` requires a
`counters() -> &Arc<AdmissionCounters>` implementation; a default no-op is
forbidden because it would make stage-two reasons export a believable zero,
the defect GL-37 and INVARIANTS GL-20 exist to prevent. The two in-tree maps pass
that Arc into each `AccountAdmissionState` at control-plane installation
frequency. `AdmissionEngine::counters()` delegates to the map, and a context
can tally stage two without borrowing the engine or cloning another Arc.

The existing `denials` array contains only refusals before `Pending` exists,
and `admitted` retains its current meaning: pending funding was created and
`units_admitted` is the quote, not a bill. Later transitions have their own
bounded counters: context abandoned before stage two, capacity shed,
canceled-before-start, commit refused by reason, execution started, and
commit-time overage. They do not increment the pre-admission `denied()` total
a second time. A cancellation that wins Tollgate's CAS is a Tollgate outcome
and is not silently assigned to the consumer. GL-93 owns the precise GL-20
rewording and transition-counter witnesses; GL-99 owns the capacity and
per-class execution-start breakdown. This avoids the contradictory identity
in which one request was both `admitted` and a member of the same flat denial
sum while still preserving each phase's operational meaning.

**Policy identity is opaque and cold.** GL-94 adds
`PolicyRevision([u8; 32])`: `Copy`, `Eq`, `Hash`, and `Default`, with all zeroes
meaning the valid "unstated" revision. Its textual and Serde representation is
exactly 64 lowercase hexadecimal characters under the same strict rule GL-24
uses for identifiers. The field has `serde(default)` in snapshots and usage
events and is copied from the pinned context through `Pending` into the one
committed event; Tollgate never interprets or hashes it.

`AccountSnapshot` becomes `#[repr(C, align(128))]` and non-exhaustively
constructible through a builder that requires `AccountStatus`; omission can
never grant `Active`. Public getters preserve cheap reads while
preventing another added field from breaking external struct literals. The
declaration puts status, enforcement mode, validity, permissions, and capacity
class before the stage-two/cold fields, with `PolicyRevision` after the fields
admission reads. GL-94 owns `offset_of!` witnesses that the stage-one fields
remain within the first 64 bytes and the existing 128-byte alignment remains;
the design does not freeze a total size before that implementation proves it
is a required contract. `ResolvedLimits` follows the same constructor/
non-exhaustive rule. Snapshot generation remains ordering evidence and policy
revision remains application identity; neither substitutes for the other.

The builder originally treated status like a genuinely optional field and
defaulted it to `Active`. Existing behavior tests always supplied a status (or
intentionally built an active fixture), so they proved status enforcement after
construction without proving incomplete construction failed closed. Status is
now a required builder argument, and a function-signature witness makes that
construction contract executable.

`UsageEvent` and stored snapshot JSON default a missing revision to zero.
PostgreSQL usage storage gains an additive fixed-width revision column with a
zero default and a length check; GL-94 owns that forward migration and the
memory/PostgreSQL/HTTP/retry/reconciliation preservation tests. Snapshot JSON
needs no column migration for the revision itself.

**The revision as built (GL-94), and the two things measurement changed.**
`PolicyRevision([u8; 32])` landed as designed — `Copy`, `Eq`, `Hash`,
`Default`, 64 lowercase hex, `serde(default)` on snapshots and usage events,
copied from the pinned context into the one committed event. Two details the
design left open resolved against measurement rather than assumption.

*The strict rule is shared, not duplicated.* `parse_id` hard-coded the width 32
in its length guard, its format string, and its error message, so a second
256-bit parser beside it would have been two rules obliged to agree — the
duplication GL-53 warns about. One `validate_hex_digits` now carries the
charset-and-width rule for both, each type decodes what the digits mean, and
`ParseIdError` carries the width it was applying so a 64-digit value is never
refused with a message naming 32. The widths stay enforced separately, and each
rejects the other's canonical form.

*`repr(C)` was worth more than the field cost.* The design asked for
`#[repr(C, align(128))]` and `offset_of!` witnesses. Measuring first showed why
the witnesses needed `repr(C)` to mean anything, and that the layout they would
have pinned was poor: the compiler had placed `status` at 200, `permissions` at
192 and `valid_until` at 160, so admitting a request read four values spread
across the *second* cache line. Declaring the stage-one fields first puts them
at 0, 8, 16 and 32 — one line — and the revision at 208, cold. The struct is
still 256 bytes, so the size claim in `budget`'s documentation is now checked
rather than asserted in prose, and the new field cost nothing spatially.

*`UsageEvent` is sealed.* Its documentation had said "produced only from a
committed reservation" while the type remained a plain struct literal any crate
could fill in — a convention, not a boundary, and one that would have let a
caller assemble an event for work that never committed. It is now
`#[non_exhaustive]` with a constructor. The ~15 downstream literals were going
to change for the field regardless; sealing made that the last time they change
for a field addition, which GL-99 and GL-95 would otherwise each repeat.

*The wire constant rose, deliberately.* `MAX_USAGE_EVENT_BYTES` went from 268
to 353: the revision is fixed-width, so it costs 85 bytes on every event whether
stated or unstated. That is the price of carrying it in canonical spelling, and
`the_widest_usage_event_still_fits_its_declared_size` is what turned it into a
build failure instead of a production one — a maximal batch silently refused as
too large. A full batch is now ~1.38 MiB against the 2 MiB body limit, still
checked rather than assumed.

**The revision's storage and rollout (GL-94, second half).** Snapshots need no
column: they are one JSONB document, and the revision rides inside it beside
`enforcement_mode` and `budget`, which arrived the same way. What they do need
is the storage-local DTOs — `StoredSnapshotRef` writes and `StoredSnapshot`
reads, and `into_snapshot` rebuilds through the builder — so a field omitted
from any of the three is dropped on every read without a word. Migration `0011`
adds one `BYTEA` column to `tollgate_usage_events`, composing the two existing
templates: `0008`'s additive non-rewriting `DEFAULT` and `0009`'s
`octet_length(x) = 32` length CHECK. The zero default is not a placeholder — it
is the domain's own "no revision stated", which is exactly what is true of a row
written before the feature, so the backfill states a fact rather than inventing
one. No index: the column is carried, never queried, and an index would cost
every ingest a write for a lookup no code performs.

**Neither backend reads a usage event back, and the tests say so rather than
implying otherwise.** The memory store keeps whole events in the map it uses for
idempotency; PostgreSQL keeps columns and its only SELECT on the table is the
dedup probe. "The revision survives storage" therefore cannot be witnessed
through a round trip the API does not offer. It is witnessed at the storage
boundary instead — `MemoryStore::settled_event` reads the retained value, and
the PostgreSQL mirror reads the column with raw SQL and asserts the exact 32
bytes. Both check what can honestly be checked.

**Deployment order, and the one direction that loses data.** Additive schema
first, then every preserving reader and serving instance, then publication of
non-default revisions. An *old writer* against the new schema omits the column
and gets the zero default, which is correct: it has no revision to state. A
*new writer* against an old schema fails loudly on an unknown column rather than
silently dropping the value. The server is a reader in this ordering — it
decodes into an `AccountSnapshot` and reserializes, so a version that did not
carry the field would strip it in exactly that round trip, which is why
`admin_preserves_the_policy_revision_over_http` pins it at the HTTP boundary
rather than only in the store. Rolling a writer back *after* activating
non-default revisions is the one step that loses them, and it is the constraint
this rollout carries.

**Reconciliation is a negative obligation.** The conservation equation is
account-level and reads no usage-event rows; the revision is not a units-bearing
term and appears on neither side. So the requirement is that adding the column
leaves `Conservation::holds()` true — asserted in both backends rather than
assumed, which is what distinguishes it from `0008` and `0010`, each of which
*had* to add a term because it moved units.

**Execution capacity is optional startup composition, not an engine branch.**
GL-99 adds an account policy value and three runtime configurations:

```rust
#[derive(Default)]
pub enum CapacityClass {
    #[default]
    Assured,
    BestEffort,
}

pub enum ExecutionCapacityMode {
    Disabled,
    Uniform { total: NonZeroU32 },
    Reserved {
        total: NonZeroU32,
        assured_reserve: NonZeroU32,
    },
}

pub trait CapacityGate: private::Sealed + Send + Sync + 'static {
    type Permit: CapacityPermit;

    fn acquire(
        &self,
        evidence: CapacityEvidence,
    ) -> Result<Self::Permit, DenyReason>;
}

pub struct NoGate; // zero-sized; returns the private-constructor no-capacity permit
```

`CapacityEvidence` carries the pinned class and generation and has no public
constructor. It never leaves `Pending`; `Pending::acquire_capacity` passes it
to the selected gate while consuming that same pending state. External input
cannot claim `Assured`, and a permit cannot migrate to another request before
commit. The gate and permit traits are sealed around Tollgate's three built-in
implementations; application compute permits are not `CapacityPermit`s and
cannot stand in for Tollgate's class decision.

`AdmissionEngine` remains generic only over its map. The service selects a
gate implementation once at startup and monomorphizes its request stack over
that type. `Disabled` selects the zero-sized `NoGate`, creates no capacity
state, and its inlined acquisition performs no class branch or atomic
operation; GL-90 proves that property in the generated path rather than assuming
an optimizer result. A single runtime enum dispatched inside every request
cannot honestly promise branch-free disabled operation, which is why mode
selection is a startup composition boundary. `Uniform` uses one pool and
ignores class operationally. `Reserved` is the only implementation in which
class changes an outcome: shared capacity is `total - assured_reserve`,
best-effort work may use only shared capacity, and assured work tries shared
before its reserve so it can use the whole instance when best-effort traffic
is absent. Zero enabled capacity, a reserve above total, or a legitimate unit
request that no eligible pool can hold is rejected before startup. Changing
mode requires a restart in the MVP.

Enabled acquisition is synchronous, fail-fast, allocation-free, lock-free,
clock-free, and happens after account funding but before commit. Refusal
consumes the pending state into `Released`: funding is refunded, the usage slot
is dropped without an event, rate tokens remain consumed, and the gate records
`CapacityUnavailable` without requiring an embedder counter call. The permit
lives in `ReadyToStart` and then `Committed`, covering the computational kernel
only. GL-99 owns the two-pool conservation/class-isolation model, functional
mode tests, shutdown accounting, and bounded-cardinality gate counters. GL-90
owns disabled/uniform/reserved, cross-account contention, and mixed-saturation
measurement; the four uncontended witnesses and the contended pair have landed
with the numbers below, and the two `load/` scenarios remain reserved.

**The gate as built (GL-99), and what measurement decided.** The design above
fixed the shape; four details the implementation resolved are worth recording.

*The pools are sharded, and that was not optional.* This gate is global to the
instance — unlike a lease or a rate bucket, every request of every account
touches it — so a single atomic would put every core on one cache line at
exactly the moment the feature exists to handle. The evidence is already in
this workspace: the same-account admission path measures ~103 ns uncontended
and 2.74–2.80 us under eight-way contention, which locality sharding brings
back to ~446–618 ns. Each pool is therefore partitioned across
`#[repr(align(128))]` shards using the same exact `partition` rule a sharded
lease uses, so shard sums equal the pool and conservation is structural rather
than checked. A request takes one unit from its sticky local shard and walks
siblings before refusing, because a partition is not a reservation: capacity
idle on another shard is still this instance's, and refusing while it sits
there would be the sharding losing capacity the unsharded design would have
found.

*Shard count is capped at the pool's units.* Splitting eight units across ten
localities leaves shards holding zero, and every acquisition landing on one
would fall through to a full sibling scan — the fast path never fast. The cap
is the fix, and `shards_are_capped_at_the_pools_units` pins it.

*Both classes try shared first, and that ordering is the utilization argument.*
Assured work reaches its reserve only once shared is full, so an instance with
no best-effort traffic uses its whole capacity rather than being partitioned
against itself. Taking the reserve first would spend the guarantee on traffic
that did not need it. The isolation half is one condition — only assured work
falls through — and it is what makes a best-effort flood unable to reach the
reserve under any interleaving.

*One gate type serves `Uniform` and `Reserved`.* Uniform is the reserved shape
with no reserve pool, which keeps one acquisition path rather than two that
must agree about conservation. The mode still selects the *type* at startup —
`Disabled` is `NoGate`, a different type entirely — so the disabled path has no
gate to branch in. That is why mode is a composition boundary and not a runtime
enum: a branch matched inside every request could not honestly promise that a
product which disabled the feature pays nothing for it.

The GL-93 test double is gone. `RefusingGate` existed because `NoGate` is
infallible and the traits are sealed, so nothing could reach the shed path; its
doc said GL-99 would replace it. A `Uniform` gate of one unit with that unit held
now refuses for the real reason through the real code, and
`DenyReason::CapacityUnavailable` replaces the stand-in reason.

**What the gate measured (GL-99).** All numbers are same-run means on
mistral-apple-m1-pro under the Criterion release profile, taken on an idle
host; the manifest carries the portable ratios and `testing/perf_baseline.json`
the absolutes.

| Measurement | Result |
|---|---|
| `capacity/disabled` against the same run's `admission/full_check` | 123.61 ns against 122.47 ns (×1.01) |
| `capacity/uniform` | 134.68 ns (×1.09 of disabled) |
| `capacity/reserved_shared` | 133.75 ns (×0.99 of uniform) |
| `capacity/reserved_fallback` | 132.63 ns (×0.99 of reserved_shared) |
| `capacity/full_check_contended_8_distinct_accounts_uniform` / `_reserved` | 1.453 µs / 1.457 µs (×2.13, ×2.14 of the ungated 680.69 ns) |
| the same contended pair, sharded vs **unsharded**, in a separate paired run | ×2.11 against ×2.83 over the same denominator |

*The disabled proof is three-part, and all three parts hold.* The allocation
scope `capacity/disabled` records zero. The same-run ratio against
`admission/full_check` is ×1.01 against a 1.05 bound. And the assembly
inspection — `cargo rustc --release -p tollgate-admission --lib -- --emit asm`
over an `#[inline(never)]` probe calling `CapacityGate::acquire` — shows
`NoGate`'s acquisition compiling to **three** instructions on aarch64
(`mov`, `str`, `ret`): no atomic, no branch, no load of the gate. The enabled
probe compiles to 88, containing the two `casal` pool acquisitions, the
`ldadd` refcount bump, and the `cbz` on `may_use_assured_reserve`. A product
that selects `Disabled` is charged nothing, in the generated code and not
merely in the design.

*The sharding is worth what it was argued to be worth.* Measured as a pair in
one run, an unsharded instance-global pool costs ×2.83 over the ungated
workload under eight-way different-account contention, where the eight-shard
pool costs ×2.11 (2.035 µs and 1.515 µs against 713.82 ns). Only the
sharded pair is gated, because an unsharded global counter is not a topology
to deploy — the unsharded number is recorded here as the evidence, not as a
threshold.

*The fallback is free, and that is a property of the miss rather than luck.*
`reserved_fallback` and `reserved_shared` are indistinguishable because an
exhausted shard refuses inside `checked_sub`, before any compare-exchange: a
miss costs a load, never a write, so it cannot contend with the successes it
is losing to.

*One regression, found by this measurement and fixed where it was introduced.*
GL-99's per-class counter arrays were first filed beside the totals they break
down. `AdmissionCounters` pads every counter onto its own cache line but
carried no `repr(C)`, so the field *arrangement* was the compiler's choice —
and adding two fields silently re-drew it, costing `admission/full_check`
3.5% (122.30 ns to 126.55 ns) while executing no new code on that path.
`repr(C)` plus appending the arrays after every counter that predates them
restored parity (interleaved A/B medians of three pairs: 122.97 ns base,
122.99 ns fixed). Hoisting the two per-request counters to the front was tried
and measured 1.5% *worse*, so the order is recorded as calibrated rather than
derived, and `later_counters_are_appended_after_the_ones_they_break_down`
pins the append rule that `repr(C)` makes meaningful. The padding was buying
false-sharing isolation while the arrangement stayed a lottery; only one of
those two was ever stated.

**What the load witnesses measured (GL-99), and what they cost to build.** The
example served exactly one account, which is why these two scenarios could not
be written earlier: the class is account-owned, so a publish carrying a class
the ledger disagrees with is refused, and mixed traffic therefore needs two
accounts — and with them two lease managers, two slots, and two republishers.
`build_app_with_capacity` generalises that, readiness becomes a claim about
every tenant against *its own* published mode, and shutdown reports every
manager rather than the first.

Two sizing discoveries are worth recording, because both look obviously wrong
in hindsight and were obviously right in advance.

*A pool sized to the connection count is never contended.* The permit covers
the computational kernel only. At one contract per request the kernel is ~100 ns
of an ~86 µs round trip, so ten connections produce on the order of 0.01
concurrent permit holders: a pool of five refused **1 request in 5,000**. The
scenario now prices 512 contracts and configures two units, which is what
actually contends. A deployment sizes its pool to the hardware its kernel runs
on; a scenario sizes it to be contended, or it measures nothing.

*The guarantee is comparative, and asserting more than it says fails on correct
behaviour.* The first draft required zero assured sheds. A real run shed 1.08%
of assured requests and the gate called it a violation — but GL-30 forbids
best-effort work consuming the assured *reserve*, not assured work ever being
refused, and five assured connections contending for two reachable units shed
each other. What the reserve promises is that best-effort saturation does not
come out of assured capacity, so the witness is a shed *advantage*, with a
class-blind `Uniform` pool of the same size as the control that says the
advantage belongs to the class rather than to the workload:

| mode, same bound | assured shed | best-effort shed | advantage |
|---|---|---|---|
| `Disabled` | 0% | 0% | — |
| `Uniform` (control) | 4.04% | 4.72% | ×1.17 |
| `Reserved` | 1.72% | 29.28% | ×17.0 |

Assured shedding *falls* — 4.04% to 1.72% — while best-effort absorbs the
saturation, and assured p50 is ×0.93 of the ungated run because it is competing
with less work. Without the uniform control none of that would be evidence: a
scenario whose assured connections simply asked for less would look identical.
The gate holds the reserved advantage to ×4 and the control to ×2, so a reserve
that stopped working could not pass, and it requires best-effort shedding above
a floor, because a reserve nobody contended is indistinguishable from no
reserve.

The sequential loopback ratio remains what GL-49 recorded it to be: four runs of
one unchanged tree measured ×1.177, ×0.912, ×1.206 and ×1.043 while these
scenarios were being calibrated. That spread is the reason the automatic lane
reports evidence rather than gating, and it is unrelated to what the new
scenarios assert — all four of their conditions are comparative or structural,
and none of them reads a clock against a threshold.

**Operating a capacity gate.** The reserve is not preemptive: an assured
request that arrives while shared is full takes a reserve unit, but a reserve
unit sitting idle is never handed to best-effort work waiting on shared. That
is deliberate — lending it out would mean either revoking a permit mid-flight
or queueing, and a queue is what "fail-fast, no queue" exists to avoid. The
cost is that a reserve sized for a peak is idle between peaks; the benefit is
that the guarantee holds at the instant it is needed rather than after a
drain.

Capacity is **per instance**, so a fleet's assured capacity is the reserve
multiplied by the number of healthy instances, and it shrinks with the fleet.
Size the reserve against the assured arrival rate one instance must absorb
while a deploy or a failure has removed part of the fleet, not against the
steady state. `CapacityOccupancy` reports pool sizes and free units for
exactly this: it carries no account labels, because a metric labelled by
account is a cardinality incident waiting for a busy tenant.

Rollout is schema, then every serving instance, then classification, and the
last step is the one that cannot be rolled back through: an instance binary
predating GL-99 ignores the class and silently restores assured treatment to
best-effort accounts. That is a capacity decision quietly reverting rather
than data loss, and it is why classification comes last. Enabling a gate is a
restart, deliberately — a live resize would need its own contract for permits
already outstanding, and reinterpreting existing counters in place is the
silent-semantics change the guidelines forbid.

`CapacityClass` is an account-owned fact, not an independently writable
snapshot decoration. GL-99 adds a non-null canonical account column with the
safe `Assured` default and an account-wide administrative mutation that
republishes every live credential snapshot at the next generation; a snapshot
whose class contradicts its account is refused. Revoked principals remain
revoked. The field in snapshot JSON uses `serde(default)`, and MemoryStore,
PostgresStore, and HttpStore preserve the same default and republish semantics.

**The pre-release API changes as one coherent ownership boundary.** GL-91 has
not shipped and has no external consumers, so the workspace does not carry a
deprecation bridge for the unsafe direct-reservation lifecycle. `Pending`
exposes no raw `Reservation`: cancellation consumes it, and execution start
consumes it into `Committed`, which owns that proof for the kernel's lifetime.
Its success type is the named, must-use `Committed` itself; charged units are
borrowed through `units()`. Returning `(Committed, CostUnits)` was rejected
because `?` unwraps the `Result` and leaves an ordinary tuple expression,
hiding the guard's must-use contract from `unused_must_use`. All workspace callers move
atomically. This is an intentional pre-release Rust API break with no
mixed-version state or data migration. Keeping the borrowed reservation API,
a dummy usage slot, or a public no-capacity permit was rejected because each
would leave an escape hatch through the lifecycle invariant.

All workspace callers migrate in the same change, with one equivalence
witness. The canonical pricing-api order becomes authenticate,
`begin`, reserve usage capacity, read/decode under the pinned limit, admit the
one-element workload, acquire `NoGate` or configured capacity, commit at
kernel start, run while holding `Committed`, drop it immediately after the
kernel, then serialize. The existing homogeneous benchmark id continues to
measure that shipped one-element path rather than being silently pointed at a
different topology. GL-95 owns the later full runtime/example cleanup.

The `policy-stale` 403-to-503 correction is an intentional RFC-7807 status
change with a stable problem code, released with that canonical migration.
Clients matching the problem code remain compatible; clients matching only
the old status must accept 503 before the new example/service revision is
deployed.

Every new field decodes an absent value to current behavior: request-count
rate and concurrency are unlimited, weighted rate remains enabled,
`PolicyRevision` is zero, `CapacityClass` is `Assured`, and execution capacity
is `Disabled`. Rollout is additive schema first, then every preserving reader
and serving instance, then publication of non-default values. The server is a
reader in this ordering: an old server accepts unknown JSON fields, decodes
them into an older `AccountSnapshot` or `UsageEvent`, and reserializes without
them, silently stripping policy revision, limits, or class. An old serving
instance similarly ignores new request-rate, concurrency, and class fields;
funding enforcement is unchanged, but those new limits fail open. The explicit
weighted-rate enable flag is the exception: an old reader conservatively
continues enforcing the legacy scalar pair. Rolling back a server or instance
after activating non-default fields is therefore an explicit operational
constraint. GL-91, GL-94, and GL-99 own their storage/HTTP mixed-version witnesses;
that wire rollout is independent of the pre-release Rust ownership break
above.

An embedding service can mirror `RequestContext` as its policy context, the
borrowed class/count list as its neutral workload, `Pending<UsagePermit>` plus
the optional `CancelHandle` as its pending state, and `Committed<UsagePermit, P>`
as the proof its own compute may run. Application compute permits stay beside
that proof rather than implementing Tollgate's capacity trait. Tollgate offers
no per-key rate narrowing and no precise retry hint; embedders map
`DenyReason::retry()`.

The GL-96 merge changed no invariant text because none of this behavior was
executable then. GL-91 now owns staged pinning, limit/gauge enforcement, builders, and its gauge
model; GL-92 owns heterogeneous arithmetic, class permissions, and the generalized
snapshot-limit proof; GL-93 owns the shared state, single-transition fallback,
usage emission, panic boundary documentation, transition counters, and GL-20;
GL-94 owns revision representation, storage, and layout; GL-99 owns capacity
conservation, isolation, and disabled semantics. Tests verify those owners
as their code lands; this design note does not claim their assurance early.

## Staged admission rollout (GL-91, 2026-08-28)

GL-91 begins with a preserving-reader release before activating either new
limiter. `ResolvedLimits` is now constructor-built: `new(max_items)` starts
with optional weighted rate, request rate, and concurrency disabled;
`with_weighted_rate`, `with_request_rate`, and the single validated
`with_concurrency(account, principal)` builder add dimensions. Request-rate
values are non-zero `u32`s. A principal concurrency ceiling is representable
only with an account ceiling and cannot exceed it. `AccountSnapshot` is
non-exhaustive and constructed through its builder. Its administrative status
is required and has no fail-open default; later genuinely optional fields do
not require another workspace-wide literal break.

**The wire shape is additive and lossless.** The legacy
`rate_units_per_second` and `rate_burst_units` numbers remain non-null scalars.
`weighted_rate_enabled` defaults to true and is omitted in that default form;
when false, the raw pair is still retained byte-for-value through decode and
re-encode. `request_rate_per_second` and `request_burst` are an all-or-nothing
pair. `max_concurrent_requests` and
`principal_max_concurrent_requests` default absent, with the latter rejected
unless it narrows the former. A pre-field document therefore decodes to the
old weighted-only policy and serializes to its canonical old shape. The
memory and PostgreSQL backend suites name both that absent-field behavior and
the new-field round trip.

**The governor-domain correction is pre-release, not a migration.** GL-91's staged
limit contract had not been released, so there was no installed state to
preserve from the pre-GL-91 domain. Adding a forward migration for hypothetical
rows would permanently duplicate admission semantics in the schema and create
a rollback protocol for a rollout that cannot occur.

The first release instead has one contract: both weighted-rate scalars must fit
governor's non-zero `u32` domain. `PublishableSnapshot` enforces that before a
store or server accepts a snapshot; PostgreSQL, HTTP, and refresh decode rebuild
the same proof and fail closed on raw out-of-contract data. The finite-precision
property test and the boundary tests cover zero, `u32::MAX`, and the first value
above it. If persisted catalogues exist before this domain is changed again,
that later change will require a forward migration and an upgrade witness; no
such compatibility mechanism is carried speculatively now.

**The reader release preserves; this guard release enforces.** The legacy
admission engine now takes a configured request-count token before the
optional cost-weighted token, then records principal and account occupancy —
enforcing either configured ceiling — before funding. `Pending` makes every
field private and exposes no raw reservation. Cancellation consumes the proof;
commit consumes it into `Committed`, which retains the funding state machine,
the RAII concurrency guard, and the usage slot as one execution-lifetime proof,
and guarantees emission when it drops. This surface
therefore measures true in-flight work instead of releasing the ceiling at the
return or commit boundary. A false `weighted_rate_enabled` now disables the
weighted governor path; the carried scalar pair remains rollback data. Omitting request
rate performs no governor operation for that dimension. An absent concurrency
ceiling skips the bound but still records occupancy so future activation is
exact. Rate tokens remain consumed on a later concurrency or funding
refusal, while any concurrency acquisition is released.

The guard retains one `Arc<AccountAdmissionState>`, constructed only after the
principal and account occupancy transitions both succeed. That exact state is
the release proof; neither decision is reconstructed from mutable current
policy during `Drop`. `Pending` no longer duplicates the snapshot `Arc`; its
accessor borrows it through the guard, keeping the ownership proof compact.
The always-tracked design adds two locality-indexed atomic occupancy
transitions to the unbounded path but no request-path allocation, scan, or
extra shared-reference clone. The one-time activation handoff owns its bounded
shard scans: one when control publishes the ceiling, one per ceiling-carrying
acquisition while the handoff is open, and one when a formerly nonempty shard
releases its last old permit. The performance gate measures the steady-state transition cost
and verifies that tracking does not defeat the sharded contention topology.

Making only that guard field private was not sufficient. Rust first allowed a
caller to move the public `reservation` field out of an admitted temporary;
private fields and borrowed access stopped that move, but the borrowed
`Reservation` could still be committed into a separately owned execution
guard, and dropping the admitted value then released concurrency while
execution continued. The complete boundary now uses ownership transitions:
`RequestContext::admit` produces `Pending`, `acquire_capacity` consumes it
into `ReadyToStart`, and `ReadyToStart::commit` consumes and owns the
committed proof as `Committed`. It returns the must-use guard directly, so an
accidental `ready.commit(...)?;` is a compiler diagnostic rather than an early
release. Removing the raw reservation accessor also closes
direct-commit siblings. The behavior witness holds account and principal
ceilings of one until the execution guard drops; compile-fail doctests reject
both raw reservation access and discarded execution-start evidence.

Those doctests are pinned so they cannot pass vacuously. A `compile_fail`
block only asserts that something failed to compile, so one naming an API that
never existed refuses for an unresolved name and witnesses nothing — which is
how the raw-reservation witness spent its first life calling a
`reservation()` method the type never had. It now reads the private
field and pins `E0616`, so making the field public compiles and renaming it
reports `E0609`; either way the witness fails. Each `compile_fail` block is
also paired with a compiling companion that exercises the supported path, so a
refusal can never be a refusal of an API that stopped existing.

The account object keeps its occupancy gauge stable across publications and
holds the current `Arc<AccountPolicyState>` as the one account-wide
publication point. Installed principal states retain only the stable account
object, not a replaceable rate `Arc`; every new request loads the same current
authority. Principal gauges come from a weak principal-keyed registry and stay
strongly reachable through installed or in-flight state, so removal and
reinstall cannot manufacture a fresh zero. The generic amortized weak-registry
sweep covers both account and principal objects under the same batch lock.

The rejected design pinned a returned `Arc<RateState>` beside each submitted
snapshot. Binding the snapshot to that rate removed a panic but split one
account into independently refillable old and new buckets; applying
concurrency from each snapshot likewise let an unbounded or wider sibling
bypass the shared gauge's intended ceiling. A single request-loaded account
authority fixes the defect class for weighted rate, request-count rate, and
account concurrency together. It also keeps shard-layout tightening on one
publication point: already-admitted work has consumed its token, while every
subsequent request sees the replacement rather than continuing to refill an
old partition.

Deployment order is every preserving client and server first, then every
serving instance with the enforcement implementation, then publication of
non-default limits. An older server accepts unknown JSON, decodes into its
older snapshot type, returns 204, and writes the stripped document; it is
therefore a reader in this order, not merely a control-plane proxy. An older
instance fails open only on request rate and concurrency, while funding and
the carried weighted bucket remain enforced. Rolling a server or instance
back after activation is prohibited until the non-default fields are removed
from every published snapshot.

The complete staged denial vocabulary and `DenyReason::retry()` classifier
landed with the preserving types so later guard MRs do not repeatedly break
every exhaustive consumer. This phase begins producing
`RequestRateLimited` and `ConcurrencyLimited`; variants owned by the later
class-permission, shared-cancel, revision, and enabled-capacity phases remain
reserved for their named owners.

**The staged lifecycle is the canonical embedding path.**
`AdmissionEngine::begin` performs the only map lookup and route authorization,
then returns an owned `RequestContext` holding the exact installed state and
locality across body decoding. `RequestContext::admit` rechecks expiry, accepts
the caller-owned class/count slice, and moves that same state through request
rate, weighted rate, concurrency, and funding into `Pending<UsageSlot>`.
`UsageSlot` lives in core and `UsagePermit` implements it, so accounting
capacity is reserved after a successful `begin` but before body consumption.

The type flow already includes disabled-capacity composition:
`Pending::acquire_capacity(&NoGate)` produces `ReadyToStart`, whose consuming
`commit` is the only constructor for `Committed`. `Committed::drop` records the
pre-reserved usage event before releasing concurrency and the zero-sized
capacity permit. GL-99 extends the sealed gate with uniform/reserved pools and
pinned class evidence without changing this consumer lifecycle. GL-93 adds the
optional shared cancel race and commit-time elastic fallback to the same
states. GL-92 adds class permissions to the borrowed workload fold, and GL-94 adds
revision accessors; none requires another lookup or pricing-api lifecycle
rewrite.

`pricing-api` implements the ordering inside its body-consuming extractor,
not merely as comments inside a handler: credential verification, `begin`, and
usage-slot reservation run before Axum's JSON extractor reads the body. The
handler receives the decoded request together with its owned context/slot,
admits a stack one-entry workload, selects `NoGate`, commits, holds
`Committed` only across the pricing kernel, then drops it before serialization.
Its stable `policy-stale` problem code now carries 503 rather than 403, matching
`DenyReason::retry()`'s transient classification.

Counters are map-owned and installed into each request state, so stage two can
tally after the engine borrow is gone and two engines sharing a map cannot
export contradictory counter identities.

**GL-91 shipped a compatibility surface this workspace had already decided not to
carry, and GL-102 removed it.** The staged rollout kept
`AdmissionEngine::admit`/`AdmissionRequest`/`Admitted` and the client
`ChargeGuard` behind `#[deprecated(since = "0.9.0")]`, and added
`CommittedAdmission` as a bridge so the old surface could keep working. That
contradicted the pre-release reasoning three sections above: the crates are
unpublished, distributed by git tag, and `examples/pricing-api` — the only
embedder — moved to the staged path in the same change. The deprecation
protected no caller and cost a bridge type, `#![allow(deprecated)]` in five
files, and roughly a hundred test call sites exercising a path nothing shipped
on. `Committed` subsumes `ChargeGuard` outright: `UsagePermit` implements
`UsageSlot`, so the pre-reserved queue permit binds at admission rather than at
commit — one stage earlier, and not omittable.

Removing it also removed a gate defect it had produced. GL-91 repointed the
`admission/full_check` benchmark at the staged path while adding
`admission/admit_staged_1`, leaving two benchmark bodies that were identical
modulo a binding name, and then gated the ratio between them at 1.10 as a
compatibility contract. A ratio between one measurement and a duplicate of
itself cannot fail for the reason it exists — a real regression moves numerator
and denominator together — and its only remaining variance is Criterion's
paired noise on the deliberately untagged `perf-ratios` runner, which measured
0.88, 1.09, and 1.15 on identical code before failing a release merge request
that contained no Rust at all. The 96.2 ns / 96.2 ns equivalence recorded above
is the same duplication seen from the other side. The properties the row stood
in for are enforced where they can bite: `admit_consults_the_map_exactly_once`
counts lookups through a wrapping map, and `check_allocations.sh` counts
allocations. The benchmark, its manifest row, and the ratio are gone;
`admission/begin` and `admission/full_check` keep the staged path measured at
both stages.

Consolidating `CostTable::quote` into the one-entry `quote_workload` fold is an
intentional homogeneous-path cost: the controlled mean moved from 1.64 ns to
2.30 ns (+0.66 ns) while remaining independent of table size (the 4,096-class
same-run ratio is ×0.98). At the complete admission boundary the staged path
measures 96.2 ns and the canonical `full_check` 96.2 ns, so the arithmetic
consolidation does not create a material embedding regression. The checked-in
controlled-host baseline was refreshed from the full post-change run; no
absolute threshold or portable ratio was weakened.

## Heterogeneous workload permissions (GL-92, 2026-09-02)

Most of what GL-92 asked for was already there. `quote_workload` already took a
borrowed slice, folded with checked arithmetic, was allocation-free and O(its
entries), and `CostTable::quote` was already its one-element call, so the "one
owning formula" requirement was satisfied before this issue opened.
`compile_workload` already checked the summed item count against
`max_items_per_request`. The issue text reads as though none of that existed;
reading the code first is what kept this change to the half that was actually
missing.

**Work permission is a property of the workload, so it is checked where the
workload is known.** GL-96 split the two: `begin` checks the route permission
before a body is allocated, and the per-class work permission cannot be checked
there at all, because which classes a request touches is a property of its
decoded body. `CostTable` now carries a permission-bits array parallel to its
weights; the quote's own fold ORs the bits of every class it visits and returns
them, and stage two tests that union against the pinned snapshot. The fold was
already walking those classes, so the check consults neither the workload nor
the map a second time — `admit_consults_the_map_exactly_once` still holds, and
`admission/full_check` stays inside its absolute bound. This originally also
cited the `admit_staged_1 / full_check` ratio, measured here at 0.99 against
its 1.10 bound; GL-102 removed that row because both of its sides had become the
same benchmark body, so the reading was noise rather than evidence. The
lookup-counting test is what carries the claim.

**A zero count is not work, so it cannot carry a requirement.** The fold already
skipped zero-count entries for cost; it skips them for permission too. The
alternative would let a caller be denied for a class it asked for nothing of,
which is a denial no client could act on.

**The canonical form is what makes the field invisible to a control plane that
has not been redeployed.** Trailing `NONE` entries are trimmed at build and an
all-`NONE` array is skipped in serialization, so a table decoded from JSON
written before this field existed compares equal to the same table built today
and re-serializes to the same bytes. Without that, a stored snapshot would start
comparing unequal to the table an instance builds, and the mismatch would look
like a policy change rather than a schema addition. `.class(op, weight,
required)` sets both arrays and `.weight` delegates to it with `NONE`, so there
is one growth path rather than two that can disagree about length.

**Repeated classes are summed, and that choice is now tested rather than
implied.** The issue allowed rejecting a duplicate instead. Summing was already
what the fold did; it is the better rule because it makes grouping an
optimisation rather than a correctness obligation — a caller that groups its own
workload and one that does not are charged identically. The witness asserts the
equivalence directly (`[(A,2),(A,3)]` quotes exactly as `[(A,5)]`) rather than
re-asserting arithmetic, and pins that the fixed term still arrives once.

`SnapshotLimits.lean` generalizes from one weight to the checked sum:
`variableCost_le_max_weight_items` bounds the summed variable term by the
largest registered weight times the total item count, so
`workload_bounded_by_worst_case` shows the published worst case — fixed plus
that weight times the cap — still bounds every in-limit heterogeneous quote.
`quote_is_single_class_workload` pins that the homogeneous quote is the
single-class case of the same model, mirroring the Rust. Publication therefore
keeps validating one number after quoting became a sum.

The three benchmark ids GL-90 reserved for this issue are now measured rows, and
the ratio `quote_workload_8 / quote_workload_1` is the portable claim: 4.01
against a 6.00 bound, sub-linear because the fixed term and the call dominate
eight classes. That ratio, not the absolutes, is what rejects a scan reaching
the request path.

This is a compatible change: the permission array defaults empty, an empty array
requires nothing, and every existing table keeps quoting exactly as before.
`CostTable::quote_workload` gains a third tuple element, which is a Rust API
break for a pre-release crate with one in-tree caller.

## Periodic budgets (GL-97, 2026-09-03)

A tollgate balance was a manual deposit that never expired. The product promise
it could not express is the ordinary one — "your included units reset on the
1st" — so a plan with a monthly allowance had no owner in the ledger, and every
embedder would have had to build the reset itself, on top of a balance with no
way to distinguish an allowance from a credit.

**Leases drain, then expire.** An account can be holding leases when its period
closes, and there were three ways to handle it. Revoking active leases at the
boundary opens an admission gap and throws away the fencing discipline that
makes a lease a capability. Having the request path check the period would put
a wall-clock read into admission, which the whole two-plane split exists to
prevent. What ships instead is the third: an active lease keeps serving to its
own TTL, and the boundary is applied when it settles. That reuses the
settlement path both backends already have — the only change is *where* the
unspent units land — and it bounds the overrun at one lease TTL of last
period's allowance, which can never exceed what the account was already granted.
Usage is untouched by the decision, so a straggling event bills against the
period its lease was granted in, which is what a billing record has to do.

**Manual top-ups persist, so the balance is two buckets.** This is the single
largest complexity driver in the change and it was worth paying for. A credit
bought or granted out of band should not evaporate because a calendar month
ended. But a single balance can only do one of two wrong things at a boundary:
expire the credits along with the allowance, or resurrect allowance units that
were already spent — there is no arithmetic on one counter that separates
"unspent allowance" from "unspent top-up" after the fact. So the balance splits
into an allowance bucket and a top-up bucket, spent allowance-first (the units
with an expiry date go first), and the lease records the split it drew.

That last part is not optional. Without the recorded split, a lease funded
entirely from credits and released after a boundary would return its units to
the allowance bucket and have them expired at the next one — silently deleting
units that never had an expiry date. Settlement charges the lease's usage in
the same order the account spends, so the top-up half survives a partly spent
lease; crediting the allowance half back first would close the equation just as
well while quietly moving durable credits into the bucket that expires next
month.

PostgreSQL stores this as `balance` plus `allowance_balance` rather than as two
independent columns. `balance` stays exactly what it was — everything the
account can spend, and the only number `acquire` compares a request against —
so no existing statement or reader changes, and the top-up portion is the
difference. The cross-column CHECK (`allowance_balance <= balance`) is what
keeps that difference non-negative. It also collided with the suite's
corruption fixtures, which plant a negative column to prove the read path
surfaces it: those now suspend every CHECK that mentions the column, matched by
definition text rather than by a name convention, so a check added later is
handled without anyone remembering to list it.

**`expired` is a sink term, the mirror of `overage_recorded`.** Units a closed
period took away are neither spendable nor billable and had nowhere to rest.
Expiring an allowance without recording it would leave the equation open by
exactly the expired units, and reconciliation would report corruption on a
correctly working ledger — the same failure `overage_recorded` was added to
prevent in GL-1, on the other side of the equation.
`Tollgate.Conservation.unrecorded_expiry_always_breaks_conservation` proves
that it *always* breaks, not merely that it might.

**The rollover is a bounded batch, not a per-account call.** The first shape
tried was `roll_period(account, now)`, which reads well and is untriggerable:
`AdminStore` has no account enumeration, so the server had no way to find the
accounts that were due. Worse, every scheduled account comes due at the *same
instant* — that is what a calendar boundary means — so the first pass after
midnight on the 1st has the whole scheduled population to cross. An unbounded
statement there would hold locks across the entire account table.
`roll_due_periods(now, limit)` is what ships, with the same drain-until-partial
contract `reclaim_expired_batch` already has, and it shares the server's
existing reclaim tick rather than owning a timer: it needs a frozen cutoff, a
bounded drain, and a failure that is reported rather than swallowed, and the
sweep already provides all three. The two passes are independent, because a
stuck rollover stranding quota would be strictly worse than a late allowance.

Idempotency is the store's, not the caller's. The pass runs on every replica,
so two of them race the boundary; the crossing is done under a row lock guarded
on the stored period — a mutex in `MemoryStore`, `FOR UPDATE SKIP LOCKED` in
`PostgresStore` — so one rolls and the other finds the account already current.
A caller that read the period first and then rolled would produce two deposits
under exactly that race, and nothing in the arithmetic would notice:
`a_second_rollover_at_one_boundary_would_double_the_deposit` states that
consequence in the model rather than leaving it as an argument.

Setting a schedule deposits nothing. Were it also a funding operation, an
operator correcting a mistyped allowance would fund the account twice, and
there would be no way to describe next month's budget without paying it today.
The first allowance arrives at the first pass after the schedule exists, which
is one tick.

**The instance-visible balance is a projection, and naming it one is the
design.** A product that returns balance and period end on every response
cannot get them from an instance: it holds a lease slice, not the account. The snapshot now carries a `BudgetView`, and the runtime subtracts
what the instance has admitted since that publication.

What it reports is deliberately *balance plus every active lease's unspent
remainder*, not the balance column. Units out on lease are still the account's.
A figure that excluded them would tell a customer their quota had halved the
moment an instance took a lease and then watch it rise again when the lease
settled — a number that moves with the fleet's lease topology rather than with
what the customer spent. Conservation is what makes this cheap: `balance +
active grants` equals what the account was funded with minus what it consumed,
so the view comes from the account row with no join over the leases at every
publication.

**The store stamps it; a publisher cannot.** Permissions, limits and a cost
table are compiled policy — a publisher decides them. A balance is not: it has
one authority and it moves constantly, so a publisher's copy would be wrong
before it landed. `AccountSnapshot`'s builder therefore has no setter, and
`PublishableSnapshot::with_budget` is the only writer. That alone is not
enough, because a snapshot crosses a wire as `Arc<AccountSnapshot>` with its
own serde derive, and nothing there stops a publisher putting a balance in the
JSON — so `with_budget` takes an `Option` and every publish calls it
unconditionally, clearing the field for an account the ledger does not hold.
Overwriting always is what makes the store the only writer rather than usually
the only writer.

The estimate's baseline is captured per snapshot install rather than by
resetting a counter. `units_admitted` is account-wide, monotonic, shared across
principals and generations, and exported — nothing may rewind it. Recording its
value when a snapshot is installed gets the same subtraction and has a property
resetting would not: the baseline and `balance_at_publish` are then true of
exactly the same instant, which is the only thing that makes their difference
mean anything. A republish moves both together.

Both of the estimate's error terms point the same way. Cancelled admissions are
counted as spent, and other instances' spend is missed, so it reads low against
the ledger far more often than high — under-reporting remaining quota is the
safe direction for a number a customer acts on. Its bound is stated where it
belongs, in the method's own docs: the refresh interval times the fleet's spend
rate. An operator who needs it tighter refreshes more often.

It is never an authorization input, and `an_exhausted_estimate_does_not_deny`
is the witness. Quota comes from the lease and the ledger; if a published zero
could refuse, a snapshot refresh delay would become an outage.

Spatially free: `Option<BudgetView>` lands in the tail padding
`#[repr(align(128))]` already reserved, so `AccountSnapshot` stays 256 bytes
and the request path touches no line it did not already touch. The estimate
itself is opt-in per call — one relaxed counter read and a saturating
subtraction, on no admission path — so the allocation gate and the request
budget are unchanged.

`Period` and `Rollover` are single-variant enums rather than a "monthly" flag
and a "carry over" bool. Month arithmetic differs per period in ways a duration
cannot express, and `jiff` owns it, so 31 January, a leap day, and the year
wrap are the library's problem. Carry-over is a variant when something asks for
it, not a boolean that would leave "how much carries over" unrepresentable.
`Period::ALL` is what a backend sweeps, and `every_period_is_swept` matches
exhaustively over the enum so a new variant is a compile error rather than a
schedule that silently never rolls.

## Dynamic multi-account runtime (GL-95, 2026-09-08)

A snapshot discovered after startup previously bound an empty lease slot with
no task assigned to fund it. `InstanceRuntime` in tollgate-client now owns the
snapshot manager, its ArcSwap map, stable slot registry, bounded usage writer,
and one supervisor for dynamic account managers. A cloneable `RuntimeHandle`
exposes staged `begin`, the recorder, readiness, and diagnostic reports. The
unique owner provides bounded shutdown; dropping it aborts its task tree.
`AccountLeaseConfig::for_account` explicitly constructs every lease config field.

Membership is observed inside the publication boundary after generation
acceptance. The runtime's map has no external writer. An atomic registry
replacement updates a principal's account; a watch notification and deduplicated
dirty-account set coalesce updates without storing an unbounded event history.
Expiry uses an ordered deadline index. Managers remain while any principal has
a fresh active snapshot. Last-member loss arms linger; reacquisition cancels
it. Reconciliation matches eligibility against lifecycle phase directly;
repeated inactive publications cannot restart an already running linger timer.
Retirement owns the old manager until join, including release work, before
backoff can start another. Task identities reject delayed old health notices.
An integrity fault is terminal, including when the task exits before its
health notification is handled. LeaseManager owns a persistent integrity-fault
flag separately from health, which also becomes false on a clean stop. Cleanup
consumes that evidence before clearing the task's counters, so faults raised
during idle retirement or final shutdown are terminal too. One release-error
classification is shared by refill and shutdown: storage failures remain
unconfirmed, invalid releases retain a fault, and neither counts as settled.
The catch-all shutdown-refusal assumption dates to `bb0c5bd`; running-plane
fault tests missed the final pass. A shutdown refusal table now tests that
boundary, and the shared classification makes the meaning independent of phase. Current
slot capabilities survive task death; lost parked grants and uncertain acquire
outcomes are reported as TTL-bounded crash exposure. Diagnostic counter history
survives retirement and restart. Crash exposure includes the opening slot
capability inherited by a replacement manager; subtracting all releases from
only its own acquisitions would hide a parked grant after a second crash.
A two-crash integration scenario pins this inventory calculation.

Readiness computes account eligibility and funding from published snapshots and
slots at the supplied timestamp. `All` uses some-account funding, `Fixed` uses
all eligible accounts, and diagnostic counts expose partial availability. The
usage queue is the admission barrier for shutdown. Discovery stops and refills
pause while the writer drains outstanding permits and committed guards. Lease
release then runs concurrently under the remaining total budget. User requests
and background failures publish one immutable deadline, sampled inside the
first notification update so concurrent callers cannot disagree with the
supervisor. All three
component shutdown futures retain ownership while awaiting joins, so cancelling
one aborts its task instead of detaching it. A hung normal ingest is interrupted
by shutdown and retried within the final drain budget. The same-pattern scan
also found permanent ingest refusals leaving the unaccounted gauge elevated;
that path now accounts for the explicitly lost batch before clearing it.
The blocked-release shutdown witness also exposed an expiry-rollover defect:
the manager held its inspection Arc across the release pass, making its own
grant look busy and postponing the refund until after acquisition. Dropping
that temporary before release restores the intended order; request-held Arcs
still prevent premature release. The temporary-reader pattern dates to
`b285fdd`; eventual-refund tests missed the ordering gap, and one hung-release
fixture accidentally relied on it to construct parked grants. That fixture
now retains explicit request readers during rotation. The sibling refill-pass
fixture already waits for every rotation and now asserts its six-grant setup.
The new blocked-release witness pins refund-before-acquisition directly.

Pricing-api installs this runtime and starts its HTTP drain and runtime deadline
together. Its shutdown future takes ownership through a server-task guard before
first poll, so cancelling or discarding it aborts HTTP as well as background
work. The demo's snapshot republishers use an owning JoinSet too: dropping
plain JoinHandles detached them on cancellation. The cancellation witness
advances time and verifies that publication has stopped. Builders keep their
signatures. Its metrics intentionally change:
`total_lease_remaining`, `total_overage_spent`, `total_overage_cap`, and
`earliest_lease_usable_until` replace primary-tenant fields. Refill counters sum
all accounts and retain completed-manager history; per-account detail stays out
of metric labels. Funding sums use checked u128 arithmetic: at most usize::MAX
u64 contributions fit on supported targets (usize::BITS <= 64), with the bound
checked in `AccountLifecycle.catalogue_total_fits`. Cumulative u64 task counters
report overflow explicitly rather than wrapping. Consumers must update these example metric names when adopting
the tag. `SlotRegistry::slot` remains available as a low-level API; runtime
membership is private, so retaining it introduces no external runtime writer.
No database migration or store wire change is needed. PeriodRoller (GL-107) and
application provisioning endpoints are outside this change.

The GL-74 sibling search found two JSON rejection sites and a missing-ConnectInfo
rejection. One local `ApiJson` adapter produces zero-charge problem+json responses
and fixed-code extractor counters for malformed, unsupported, and oversized
bodies; missing connection state is a structured server configuration error.
Authentication and `begin` already precede body decoding from GL-91 and retain
their regression witness. These rejections also abandon their staged contexts.

The lifecycle's abstract ownership and transition safety are checked in
`formal/lean/Tollgate/AccountLifecycle.lean`. Paused-time integration and generated
catalogue traces test actual task counts, release conservation, reactivation,
crash restart, and shutdown. These are implementation evidence, not a formal
refinement proof. The request path still delegates directly to staged admission;
registry locks, membership scans, timers, and reporting are entirely off-path.

## Consolidating lease refill (GL-109, 2026-09-08)

An instance could refuse work its account could fund, indefinitely, and say so
honestly while doing it: `estimate_remaining` reported 58 units while every
51-unit request met `LeaseExhausted { remaining: 49 }` until the lease's TTL.
Reproduced with the reporter's numbers against `MemoryStore` — allowance 160,
`target_grant` 100, `low_water` 50, `shrink_divisor` 1 — grants ran 100 → 60 →
49 and stopped there.

Two mechanisms met. `GrantPolicy` shrinks the grant as the balance falls, so
the tail grant is smaller than `target_grant`; `LeaseManager` then caps
`low_water` at `granted - 1` so a fresh tail grant does not rotate without
serving any work. Between them the tail lease is installed *above* its own
mark. Nothing can cross it, and the debit that proved the grant too small
returned `LeaseExhausted` without touching the doorbell.

The silence was not an oversight but a decision, and a test held it in place:
`a_refused_debit_never_signals`, reasoning that "a refused debit changes no
counter, so it must not claim a crossing". That premise is true. It hid the
defect because the doorbell had been modelled as *a crossing happened* rather
than *the plane should act*, and a refusal is the second without being the
first. Mutation testing protects such a test rather than questioning it, and
the name stated the mechanism instead of the behaviour, so nothing pointed at
the gap. `RefillVerdict` makes both facts representable at once — the shard
crossing flags stay clear on a refusal, and the verdict still reports it — so
the replacement witness gives up neither.

Signalling alone would have made things worse, which is what forced the design.
Holding 49 with 9 in the ledger, an ordinary rotation acquires
`min(target, 9) = 9` and parks the 49; the next refusal swaps back. It
oscillates. The response has to fold the held units back in as it re-grants,
and that fold cannot be composed by the holder from `release` and `acquire`:

- Under the **default** `shrink_divisor` of 2, releasing 49 into a balance of 9
  re-grants `min(100, 58/2, 58) = 29`. The consolidation *shrinks* the holder,
  irreversibly.
- Between the two calls another instance can take the units just returned.

Both are properties of the split, not of the holder, so `LeaseAllocator`
gained `consolidate`: one transaction, release semantics then acquire
semantics, with the policy's shrink cap applied as a floor against what was
returned. It is served by `POST /v1/leases/consolidate` so the via-server
topology keeps parity with the direct-store one — a server without the route
would strand the tail of every allowance for instances behind it while
`MemoryStore` embedders saw no such thing.

Writing it exposed a second failure of the same shape. `MemoryStore` holds a
mutex where `PostgresStore` holds a transaction, so it has nothing to roll
back: the first implementation applied the release, then let the grant refuse,
settling a lease it could not replace — the exact failure the operation exists
to prevent, arriving by another door. `a_refused_consolidation_leaves_the
_original_lease_spendable` caught it. The backend's allocator is now a fallible
plan and an infallible apply for each half, so all-or-nothing is structural in
both backends rather than incidental in one.

The period boundary needed its own answer. A consolidation is a settlement, so
the allowance half of a lease funded by a closed period expires rather than
returning (GL-97). Sizing the replacement against `unspent` would re-lease an
allowance the account no longer has, so `spendable_credit` decides what the
credit actually restores and the grant is sized against that.

Review found that Postgres still passed nominal `unspent` as its floor.
The original boundary witness used a divisor of one, which masked the wrong
floor: both paths selected the whole restored balance. The mirrored budget
reduction witness uses a divisor of two and an old 250-unit allowance lease
against a new 100-unit allowance. Both backends now grant 50 units. Postgres
returns the restored credit from the settlement's account update, under its
existing row lock, and the acquire consumes that evidence. The sweep sibling
already distinguishes spendable and expired credit in `AccountCredit`.

Client-side, `RefillVerdict::Refused` outranks `Draining`. A crossing is
anticipatory and is answered by acquiring alongside a lease that still serves,
which is what keeps "a funded account is not refused between ticks" true; a
refusal is that failure already realised, with nothing left to preserve.
Consolidation needs an exact aggregate, so it empties the slot first — a deny
window, kept to the take-and-check by testing quiescence rather than waiting
for it. That costs nothing in the state it exists to fix, where no successful
request is holding the lease. The failure classification is deliberately not
the release pass's: `InsufficientBalance` is an integrity fault for a release,
which never draws a grant, and an ordinary near-exhaustion answer here. A
consolidation whose outcome the store did not report is parked rather than
reinstated, because serving again from a lease the ledger may already have
credited back is worse than the refusal being repaired.

The public trait contract now states this ambiguity too. A `Storage` error
can follow a committed transaction whose response was lost; it cannot promise
rollback or instruct a holder to resume spending. The allowance floor is
likewise documented in terms of surviving credit, including its ability to
exceed a smaller replacement request when preserving that credit.

Two runtime accounting omissions were found at the same boundary. Successful
consolidation counted only the replacement acquisition, so a later task death
misreported the settled predecessor as a lost grant. `record_consolidated`
now owns both counter updates. Cancelling consolidation at shutdown cleared
the pending-acquire flag even though the replacement capability could already
exist. It now retains the flag, as the ordinary acquire cancellation already
did. Tests cover consolidation followed by task death and a committed exchange
whose reply is lost during shutdown; the latter reports uncertainty and proves
that TTL reclaim eventually returns the stranded replacement. The earlier
tests stopped at slot contents and missed these runtime-report consequences.
The sibling scan also found that timeout uncertainty was copied only on task
death and `Storage` outcomes were never copied. `LeaseStats.uncertain_acquires`
now records both ambiguous outcomes in the shared acquisition counter owner.
Runtime and account reports include live counts and retain them once on join;
an interrupted call still contributes separately through its pending marker.
The timeout/Storage integration table verifies visibility before and after
clean shutdown. This adds a Rust diagnostics field without changing a wire DTO
or database schema; the branch's existing breaking API rollout covers struct
literal consumers too.

`consolidated` and `consolidations_deferred` are counted apart from `acquired`.
A rising `consolidated` rate is the signal that `target_grant` is undersized
against the largest quote the service prices — which is the operator's actual
fix, consolidation being the safety net rather than the intended steady state.

What remains bounded by design: under a `shrink_divisor` above one, a quote
larger than `balance / shrink_divisor` is unfundable by any single lease. That
is the policy's fairness trade across instances — one holder may not lock a
small balance behind an oversized lease — and `EnforcementMode::Elastic` is the
answer to it, not a larger grant.

## Control-plane identity, TLS and audit (GL-98, 2026-09-09)

The HTTP control plane now has a deployment boundary of its own. Instance
credentials fund leases, read snapshots and submit usage; operator credentials
mutate accounts and publications. The roles are deliberately disjoint. Route
middleware authenticates before decoding, and handlers require private evidence
extractors so omitting a middleware layer still fails closed. The sibling route
search included consolidate and capacity-class, added after GL-98's initial route
list: both are covered by the role matrix, with no unprotected mutation sibling
remaining. Customer admission and its hot-path authentication stay unchanged.

rustls owns the connection and the verified peer chain. Forwarded headers are
not evidence. New HTTP requests recheck certificate trust, expiry and exact leaf
mapping, including on keep-alive connections. Optional client authentication lets
mTLS, bearer callers and unauthenticated probes share the encrypted listener.
Google service-account bearer tokens use the existing `CredentialVerifier` seam,
fixed issuer/algorithm/audience and stable subject mappings. Signing-key fetch is
off the handler path, response/time bounded, and cached only through issuer
freshness (at most one hour). Failed refresh cannot renew expired authority.
On a Google Cloud workload, the attached service account can supply tokens
through the metadata provider, so no per-replica secret is required.

Verification, authorization and TLS configuration are one ArcSwap generation.
The file loader stages referenced material, validates everything, and only marks
its digest installed after successful publication. Length-delimited digest input
prevents file-boundary collisions. Empty role maps permit deliberate withdrawal
of all authority. TLS mode itself cannot change on reload. The client likewise
holds a complete transport generation so a call cannot combine old mTLS with a
new bearer provider. Review exposed an accept-loop race: loading TLS before an
awaited TCP accept let the next handshake use a superseded certificate. Selection
now occurs at connection acceptance. Handshakes have bounded task count and time,
and owned tasks abort when their listener/server/reloader is dropped.

Audit could not be made accurate by wrapping administrative calls with before
and after reads: concurrent operators would log each other's changes. The
contract therefore moved into `AdminStore`: six HTTP-facing mutations return
`AdminReceipt<T>`, with the previous result in `outcome`. Memory captures under
its mutation mutex; PostgreSQL captures under row locks/atomic RETURNING. First
publication uses exclusive insertion or locks the winning concurrent row before
comparing generations, so publication/revocation receipts name the actual
predecessor. No schema change or durable audit outbox is introduced. The HTTP
operator guard attaches actor, action, target, time and operation ID and emits
started/confirmed/failed/cancelled-unknown events. Snapshot receipts identify the
immutable generation instead of copying the complete policy graph. An ambiguous
failure does not claim rollback. Deployments own retained audit delivery; process
or collector failure can lose a post-commit log event.

Compatibility is an intentional break for unpublished crates: mandatory server
security configuration, a `security` field on `ServerState`, fallible `HttpStore`
constructors, and receipt-returning backend methods. Wire DTOs, existing domain
error codes, and PostgreSQL schemas are unchanged; authentication 401/403 codes
are additive. The [operator runbook](CONTROL_PLANE_SECURITY.md) supplies the
staged endpoint/client rollout and rotation procedure. A remote plaintext
fallback would defeat the contract, including behind a platform front end that
terminates TLS: the supported topology has clients connecting to a direct TLS
endpoint.

Dependencies are shared and locked. rustls/tokio-rustls and reqwest's rustls
feature provide maintained TLS instead of an ad-hoc protocol. jsonwebtoken uses
its AWS-LC backend for audited RS256 primitives; choosing the RustCrypto backend
would make the repository's currently unreachable `rsa` advisory exception
reachable. TLS explicitly selects ring, avoiding ambiguous process-global
provider selection when both backends are present. These packages use
Apache-2.0/MIT or ISC licenses; bundled AWS-LC components also declare BSD-3-Clause
and MIT-0. AWS-LC introduces native C/CMake build work, but
no OpenSSL runtime requirement. `rcgen` and `tempfile` are test-only and generate
short-lived certificates without committing private keys. New crypto, parsing,
allocation, locks and network calls remain in the background HTTP plane. Role
and certificate maps are expected O(1); trying configured bearer schemes is
O(schemes), two in the binary. Handshake memory scales with the configured cap,
not incoming connection attempts. Audit adds constant field capture to existing
mutation locks; publication may require one extra row-lock lookup. No request
admission data structure or cost changes.

Assurance separates an exact role/receipt model from implementation evidence.
`formal/lean/Tollgate/ControlPlane.lean` proves role separation, evidence agreement,
and deposit receipt conservation/composition under stated assumptions; it does
not claim to verify cryptographic libraries or Rust refinement. Mirrored memory
and PostgreSQL concurrency scenarios validate actual predecessor receipts. The
complete lease/snapshot/admission/usage conservation scenario runs over loopback
bearer, TLS bearer and mTLS. Rotation, invalid staging, key freshness, deadlines,
role/body ordering and audit tests are named in invariants 32–33. Existing
allocation, mutation, formal, advisory and performance CI gates apply unchanged.

The first mutation run exposed missing independent boundary witnesses: metadata
identity fetches had been exercised through a custom-provider timeout and the
server's JWT verifier, leaving the actual metadata cache, provenance and stream
limits untested. A private endpoint/clock seam now runs that same implementation
against a local HTTP fixture; production still fixes the Google metadata URL.
Tests cover exact cache expiry, failure retry, full-size valid and oversized
responses, interrupted reads and invalid provenance. The sibling input audit
also added isolated URL-component/TLS-mode cases, credential-file length and
framing boundaries, and binary rejection before a PostgreSQL connection is
opened. Those tests distinguish guards that combined-invalid fixtures and the
listener's later refusal had previously masked. Public behavior is unchanged.

The completed mutation pass also exposed the equivalent gap on Google's
signing-key transport, plus bearer-scheme agreement and same-CA certificate
mapping. The fetch implementation now takes a crate-private endpoint parameter;
the production loader fixes Google's URL, while a local HTTP fixture exercises
status, cache policy, interrupted bodies and the exact 1 MiB boundary. Key
metadata and bearer framing have independent-condition tests, so a later
signature rejection cannot hide a missing earlier guard. TLS fixtures issue two
different client leaves under one CA: the unmapped leaf must receive an HTTP
authorization refusal after a successful handshake. Reloader ownership is
witnessed by release of its task-owned clock on drop. These close the remaining
observability gaps without weakening or excluding mutations.

## Credential projection over the control plane (GL-108, 2026-09-09)

The via-server topology could lease, resolve snapshots and bill but could not
obtain customer digests to authenticate. The read-only `KeySource` now exposes
revisioned `KeyPage`s from MemoryStore, PostgresStore and HttpStore, with
instance-only `GET /v1/keys` over GL-98's transport. `KeyDirectory` retains
issuance/revocation authority; HttpStore cannot implement those capabilities by
accident. Customer credentials belong to that durable directory, separately
from the server's bearer HmacRegistry loaded from its file manifest.

We chose the dedicated key feed over adding digests to snapshots. Snapshot
publication and key issuance would otherwise be two writers of credential
liveness without a concurrency protocol. Neither the customer secret nor the
issuer's HMAC secret belongs in the feed. The verifier already has the latter;
access to digests alone does not grant verification or issuance authority.

Inspection found no production caller of `active_keys`: GL-104 implemented the
durable half, while pricing-api installed static demo strings. Turning that
read into a recurring feed exposed unbounded reads, a collect-and-sort in
memory, and an O(n) principal-uniqueness scan. The manager therefore uses bounded
store pages, MemoryStore has ordered unrevoked IDs and a retained principal
hash index, and PostgreSQL gains a partial key-ID index in forward migration
0013. The existing account-ID index matches the revocation predicate but does
not supply key ordering; it stays for account-scoped reads and old plans.
Expired unrevoked rows remain candidates: do not claim O(page size) regardless
of expiry distribution. The unbounded operator `active_keys` API is retained.

Paging without a coherence protocol can omit a key inserted behind the cursor
or install a key revoked after an earlier page. Every page now carries the
revision read in the same snapshot as its records. Memory owns both under its
mutex; PostgreSQL uses a short read-only repeatable-read transaction per page.
A statement trigger advances the revision for old writers too, transactionally
including rollback and overflow. A mixed revision restarts from page one under
the same deadline and page-call budget; no SQL transaction spans HTTP calls.
An installed revision never moves backward. This is a coherent committed read,
not a promise that no mutation can commit between the final read and publication.

The server chooses activity time per page, never accepts a client cutoff, and
the manager narrows the accumulated set with its own clock and the latest
source time. A single `KeyManager` owns the publisher and exposes only
`KeyVerifier`, preventing a second registry writer. It builds a whole table
before atomic replacement. Failed, incomplete, over-budget or expired drains
preserve the last table and deadline; authoritative empty success removes all.
Proof expiry is `min(key.not_after, pass_start + max_age)`, so an outage cannot
renew stale credentials. Cached sessions retain their originally issued proof
until that finite deadline, with current snapshot authorization every request.
Task liveness and the same table deadline drive readiness. Shutdown cancels
pending reads and task teardown withdraws new verification. Budgets rely on
cooperative tasks, not preemption of arbitrary synchronous custom code.

The HTTP boundary requires 200 without Content-Range, required nullable fields,
ordered cursors, unique IDs/principals, fingerprint agreement and canonical
lowercase hex digests. Wire limits are derived from maximum page/record sizes
and pinned against widest serialization. The sibling search found that
HmacRegistry and MintedKey derived Debug through Zeroizing, whose Debug exposes
the wrapped bytes. Zeroization had been mistaken for diagnostic redaction,
and no existing test exercised that output. Their custom diagnostics now
reveal only counts/identifiers, with a focused test against issuer/customer
secret and digest disclosure.
Snapshot and principal reads also accepted partial 2xx responses; they now
require complete responses. No other credential feed exists in the module.
The pricing example imports its public demo tokens through the auth library,
persists their digests, and runs the manager alongside InstanceRuntime; its
builders become asynchronous so bootstrap has explicit completion ordering.

This adds normal dependencies already present in the workspace: auth, arc-swap,
zeroize, and feature-gated serde_json. No external package is added. Warm and
cold managed-verifier paths are measured separately; cached authentication has
finite expiry but no new I/O, lock, allocation or clock read. Cache renewal's
proof allocation is measured separately from allocation-free warm hits and
expiry refusals. Projection construction and store-query scaling stay off-path.

Migration 0013 is additive, with an index-build write pause to consider on
large existing tables. Old binaries remain usable with the trigger installed;
rollback retains schema objects and revision history. The Rust `KeyDirectory`
supertrait and server `Backend` bound are intentional breaks in unpublished
crates. Existing wire DTOs and error codes retain their meanings; the route
and its codes are additive. Deploy migration/server before enabling clients.
The [operator guide](CREDENTIAL_PROJECTION.md) specifies exact wire, timing,
rotation, readiness and recovery behavior.

Invariants 27 and 34 name the owning types and witnesses. The Lean model proves
complete fixed-catalogue drains, no duplicates or omissions, whole replacement,
failure preservation, mixed-revision rejection and finite expiry intersections.
It assumes ordered cursor refinement, coherent reads, authenticated transport,
atomic publication and accurate clocks. Rust backend, HTTP, paused-time,
property and mutation tests are separate implementation evidence, not a proof
of cryptography, PostgreSQL or Tokio.

## Credential activity from committed usage (GL-105, 2026-09-09)

We define activity as `last_committed_at`: accepted, attributable usage, rather
than every authentication. The existing pinned AccountSnapshot had an optional
key ID, but production publishers did not populate it. Copying that ID beside
the policy revision when commitment constructs UsageEvent lets the existing
writer, transport and ledger transaction carry activity. An observed session
cache would need per-session coarsening, a separate bounded queue, retries and
its own loss contract; those mechanisms are unnecessary for committed semantics.
The field's narrower name and explicit unknown states retain the diagnostic gap.

The annotation was not verified against the key directory. Supported store
publication now validates key/principal/account binding, with a structured error
before replacement or push. This exposed the inherent MemoryStore publisher's
bypass of AdminStore status/capacity checks; one shared operation now owns all
three checks and budget stamping. Custom publishers and instances remain trusted
at their boundaries. Ingest independently filters key/account mismatches without
refusing billing, but cannot prove the authentication that preceded an event.

Activity lives outside the credential table because migration 0013 tracks every
credential mutation. Traffic-driven UPDATEs there would restart paged key drains.
The usage row retains its optional source key ID, without a credential FK, while
the activity aggregate references retained credentials. Only the canonical newly
accepted set updates maxima. A repeated request ID is classified before payload
validation, so accepting attribution from a replay would permit rewriting history.
Unattributed counts therefore cover accepted events only and are computed before
grouping; affected SQL rows cannot count events or distinguish monotonic no-ops.
The query-plan check against 100,000 credentials exposed a second scaling trap:
an ordinary bulk operator join chose a hash scan of the whole directory. Both
attribution and inspection now use correlated primary-key lookups with an
explicit one-row bound. Uniqueness makes that bound complete; it prevents the
planner from turning bounded input into catalogue-wide work. The retained plan
evidence shows indexed probes for both operations, including 4,096 operator IDs.

Mixed-version replies use an optional unattributed count. A missing field is
unsupported reporting, not zero. The writer validates the complete acknowledgement
before clearing evidence, and its cumulative outcome counters saturate visibly.
The HTTP sibling search also found acquire/consolidate accepting partial success;
they now require complete 200 responses like other typed replies. Release retains
its documented no-content success contract. Invalid attribution does not reject
a bill; a database failure still rolls back the shared transaction and retries.

Worst-case tests found two adjacent timestamp defects. The usage-size fixture
omitted fractional seconds and expanded negative years; the corrected maximum,
including the key ID, is 410 bytes and still fits the 2 MiB batch ceiling. Jiff's
microsecond constructor also omits the fractional final second from its integer
range check. The store's seconds/nanoseconds decoder preserves that valid range,
including negative times, and PostgreSQL's other timestamp decoders share it.
Boundary tests reproduce the former failure. Debug inspection additionally found
cached session bytes and KeyRecord digests exposed by derived diagnostics; their
owning Debug implementations now redact those fields, with behavioral witnesses.
The sibling snapshot ceiling fixture now includes full-width per-operation
permissions and the widest timestamp, and validates that its maximum quote fits
its burst. Its 100,000-class catalogue still fits the existing 4 MiB ceiling.
The startup audit found another distinction the additive-DDL argument missed:
SQLx rejects applied migration versions absent from the binary's embedded
catalogue. Existing connections can use the new schema, while an unmodified
v0.17 restart refuses 0014. A catalogue-level integration witness now checks that
refusal and preservation of history. Rollout documentation requires preparing a
migration-aware rollback build; it does not weaken unknown-schema validation or
rewrite an applied migration to make an old binary start.

The new benchmarks construct and emit the usage event. The existing reserve/commit
and admission/cancel benchmarks stop before that work and cannot justify its cost.
Same-run attributed/unattributed ratios, a baseline comparison, allocation counts
and database ingestion measurements supply separate implementation evidence.
The production-writer audit covered AccountSnapshot, KeyRecord and transport
DTOs: key ID was the missing annotation now populated by the example. Other
snapshot fields have their builder/compiler/store writers; optional expiry remains
an intentional issuance choice. The credential-table write search found issuance,
retirement and the explicit test/reset truncation, with no unrelated production
UPDATE. No additional missing-writer sibling was identified in that scope.
Formal max/replay/report laws do not prove Rust/SQL execution or actual authentication.
See [Credential activity](CREDENTIAL_ACTIVITY.md) for the current API and rollout.

## Bounded snapshot generation history (GL-67)

Generation entries and their eviction index share a configured, nonzero
principal capacity, including pending reconstructions. Ordinary visible eviction
still preserves history. Reclaiming history instead reserves authoritative reads,
evicts the oldest retained incarnations outside the requested batch, and removes
their visible entries under the same writer lock. Request lookups never touch
the history table or its eviction index.

Revalidation reads start after reservation and must be linearizable against the
source's publications and durable tombstones. A cached response or eventually
consistent replica cannot authorize reconstruction. A private fence identifies
the map, principal, and retained incarnation. Publication validates every fence
before resolving account policy. Incarnations never repeat: exhaustion is an
explicit failure. Within a retained incarnation, the existing positive/revoked
generation rule still rejects replays. Unknown responses cannot open a forgotten
principal to positive pushes, since an absence supplies no generation floor.

Before the first reclamation, an unseen principal can fill a vacant history
slot directly. At capacity, after reclamation, or during reconstruction, a push
reports that a fresh authoritative read is required. A refused push invalidates
an already running reconstruction; retrying must start a new read. Source failure
leaves a miss denying locally. The client removes evicted resolution/deadline
entries before I/O and limits publication batches to retention capacity. Fixed
principal sets must fit their map; discovery can churn through the budget and
reports unresolved principals. The budget must cover the simultaneously served
principal set and refresh working set.

The bound covers generation history and its index, not caller-owned responses,
source catalogues, or irreversible account spend state. The client separately
bounds fetch concurrency. Eviction preserves lease slots: recreating them would
reset an account's elastic spend cap. No request-path operation is added.

The original defect retained replay evidence correctly but never bounded that
evidence. Tests covered visible-negative bounds and replay safety separately,
missing generation-history growth under churn. TTL deletion cannot repair it:
snapshots have no maximum lifetime, and generations are per principal. The new
proof obligations are bounded retention, nonreusable read identities, rejection
of reclaimed-incarnation responses, and replay safety across revalidation.
Source linearizability is an assumption, not a cache-model theorem.

The sibling search covered both map backends, scalar and batch publication,
`Arc<T>`/test-map forwarding, and the client's duplicate resolution and deadline
indexes. Moka now shares the same generation/fence/batch acceptance boundary,
and client resolution indexes are discarded on reclamation instead of retaining
a second unbounded watermark copy. A control-plane membership probe also repairs
the existing equal-generation refresh gap after visible eviction without feeding
Moka's frequency sketch. The swept account admission-state registry already has
amortized ownership-based reclamation; lease/account spend state is deliberately
preserved. Source enumeration and tracked catalogue membership remain a separate,
explicitly documented resource, not claimed to fit the history budget.

Publication methods now return `Result`, map constructors expose separate visible
and history capacities, and `InstanceRuntimeConfig` requires a nonzero history
budget. These are Rust API changes requiring caller recompilation. No snapshot
wire DTO, SQL schema, generation arithmetic, request lookup, cost quote, or
performance threshold changes. Operational recovery and rollout requirements are
in `docs/SNAPSHOT_OPERATIONS.md`.

## Deferred seams (deliberate, not forgotten)

- Customer capability tokens (pasetors) and general JWT/client-certificate
  credential schemes — `tollgate-auth`'s `CredentialVerifier` remains their seam.
  The control plane now ships Google service-account ID tokens and TLS client
  certificates; it does not implement customer login.
- Cedar/Biscuit-class policy engines — permissions fit in a 64-bit bitset
  until proven otherwise, per the thread.
- Cross-process snapshot push (Postgres LISTEN/NOTIFY, server SSE/long-poll);
  today: in-process broadcast + pull-with-refresh, so cross-process
  positive-to-revoked freshness is bounded by the SnapshotManager refresh
  interval; negative-to-positive recovery is bounded by whichever negative TTL
  applies — `unknown_ttl` for a principal the source has never known,
  `revoked_ttl` for one it once served (GL-52).
- Cross-process snapshot *push* (the pull half landed with GL-48). `HttpStore`'s
  `subscribe` is still a closed channel, so an HTTP-transport instance learns
  of a new principal at the next refresh rather than on publish. That bounds
  onboarding by `refresh_interval`, which is fine for human-paced sign-up and
  too slow for programmatic provisioning.
- Catalogue *size* under `TrackedPrincipals::All`. Revocation tombstones are
  retained by design (INVARIANTS.md GL-15), so enumeration returns every
  principal ever published and the tracked set only grows — the same
  never-forgets property GL-23 recorded for `MemoryStore`, now at the snapshot
  layer. Its recurring *cost* is bounded (GL-52): tombstones ride their own long
  TTL instead of a fetch per sweep. What is still unbounded is memory and
  enumeration payload, and untracking exists and is tested but has no
  production caller, because no backend deletes a principal outright.
  The runtime bounds tasks and leases by eligible/lingering/retiring accounts,
  but retains stable slots and account diagnostic history for process lifetime:
  removing and rebuilding a slot would silently reset its irreversible elastic
  spend cap. This change does not bound total catalogue or slot memory.
  The GL-108 credential feed has bounded revisioned pages and a bounded drain;
  its installed table still scales with the active credential catalogue.
  `GET /v1/snapshots` enumeration remains unpaged.
- Operator key issuance/revocation over HTTP. Returning a minted customer secret
  requires its own durable-before-disclosure protocol and retry semantics;
  issuance and retirement remain direct-store operations.
- Fleet-aggregated rate limiting (limits are per instance by design today).
- Per-principal request-rate narrowing. GL-91 adds a stable principal concurrency
  gauge, but a bucket per credential has a different state, eviction, and
  multiplication contract; policy compilers reject that restriction rather
  than silently approximating it with the account bucket.
- An explicit account *reset* workflow (create refuses to overwrite).
- Redis backend (third pluggability proof), rdkafka usage stream, usage-event
  dedup-window retention policy.
- Settled-lease retention, in both backends: those records are kept because a
  straggling usage event still has to be matched to its lease capability and
  checked against the remaining settlement capacity. Released-lease usage
  that fits the provisional loss is billed; reclaimed-lease or excess usage is
  rejected. Discarding those records therefore needs the same dedup-window
  decision as the usage-event entry above — and if they are ever pruned,
  archival rather than deletion is probably right, since they are
  reconciliation history. Their *cost* is
  bounded in both: `MemoryStore`'s sweep and `conservation` walk an active
  index rather than the table (GL-23), and `tollgate_leases`' two partial indexes
  cover only live rows (GL-12). Their footprint is not.
- Per-core sub-leases for single-account multi-core saturation.
- Controlled-host calibration of the Criterion and local load-gate absolute
  thresholds (all absolute thresholds are provisional laptop numbers).

## Mutation gates

`scripts/check_mutations.sh` runs cargo-mutants in three scopes.
`--diff [base-ref]` mutates only what a branch changed — that is the CI gate,
required on every merge request to `main`, and it is a gate precisely because
it *can* pass. `--package <crate>` mutates one crate's whole surface, which is
how code written before the gate existed gets measured at all. The bare form
sweeps the workspace and requires `TOLLGATE_PG_URL`.

Every scope accepts `MUTANTS_SHARD=k/n` (0-based `k`), which passes
`--shard k/n` to cargo-mutants: the shards partition one deterministic mutant
list, so running every `k` tests exactly what one unsharded run would. CI runs
the diff gate as eight `assurance / mutation shard k/8` jobs, each with its own
PostgreSQL service because the backend suite truncates its database, and one
required aggregate, `assurance / mutation`. The aggregate runs under
`always()` and fails unless every shard succeeded: GitHub reports a job
skipped behind a failed dependency as passing, so an aggregate without
`always()` would go green exactly when a shard failed. Sharding exists because
a diff touching both backends is serial against PostgreSQL, and at one worker
a few hundred mutants outran the job timeout (GL-67; #44's diff produced
about 600).

`test_workspace` stays on in every scope: a `tollgate-core` mutant is allowed
to die to a `tollgate-client` test, because what matters is whether *anything*
notices, not whether the owning crate does.

Per-crate scores, from clean `--package` runs (GL-43, GL-50):

| crate | mutants | caught | unviable | missed |
|---|---|---|---|---|
| `tollgate-core` | 84 | 60 | 24 | 0 |
| `tollgate-admission` | 109 | 89 | 20 | 0 |
| `tollgate-alloc-count` | 19 | 16 | 3 | 0 |
| `tollgate-store` | 125 | 102 | 23 | 0 |
| `tollgate-store-postgres` | 100 | 78 | 22 | 0 |

`tollgate-client`'s two background planes were the first thing measured (GL-34).
`tollgate-server` and `examples/pricing-api` are covered by the diff gate as
they change, but have no recorded whole-crate baseline.

The PostgreSQL baseline corrected the original inventory diagnosis (GL-50).
Under the pinned cargo-mutants 26.0.0, the v0.3.0 crate initially produced
2,287 mutants, but zero were SQL-string replacements. Instead, 2,188 came from
the Cartesian replacement combinations for `lock_lease`'s anonymous
seven-field tuple. Naming that row `LockedLeaseRow` reduced the inventory to
101 without changing SQL or behavior; excluding one proven-equivalent TTL
boundary mutant left the 100-mutant surface recorded above. The serialized
live-database sweep took 20 minutes and had no survivors or timeouts. An
exploratory v0.2.14 run found one real survivor, an unconditional success from
`StoreHealth::ping`, now killed by a closed-pool failure witness before the
clean current-version sweep.

PostgreSQL tests share one database, so `MUTANTS_JOBS` is forced to 1 and no
worker may run beside another. A PostgreSQL-touching diff also refuses to run
unless `TOLLGATE_PG_URL` is set.

That serialization is what a diff run pays for the backend suite, and GL-67
showed the bill: 159 mutants at one worker ran past the job's 90-minute limit,
and a gate that cannot finish reports nothing at all — not a pass, not a
failure, no artifacts. A diff that touches no file under
`crates/tollgate-store-postgres/` cannot change backend behavior, so a diff run
now drops `TOLLGATE_PG_URL` for itself and uses every worker; the backend suite
skips rather than racing four ways over one database. The trade is that a
PostgreSQL test can no longer kill a mutant in another crate. That direction is
safe — such a mutant is reported MISSED, which fails the gate loudly, and never
becomes a silent pass — and it repairs a second, quieter defect: with the
backend suite in every mutant's run the workspace suite took longer than
cargo-mutants' own 60-second floor, so every uncaught mutant was reported
TIMEOUT rather than MISSED, and the report named a hang where the truth was a
gap in the tests. An operator who sets `MUTANTS_JOBS` explicitly keeps it.

### Equivalent mutants

Five exact mutations and two patterns are excluded in `.cargo/mutants.toml`,
each with its argument written beside it. An exclusion is for a mutant that
*no input can distinguish* — not one that is merely awkward to reach. Both
backends' TTL clamps differ between `>` and `>=` only where both arms yield
the same duration; `SnapshotMap::local_sharding` and `PricingConnection`'s
`connect_info` spell the same default two ways; `Debug`/`Display` bodies and a
binary's `main` carry no semantics the harness can run. A survivor that cannot
be argued equivalent is a gap, and gets a test.

The fifth is the first exclusion resting on a machine-checked argument rather
than prose. `ConcurrencyGauge::release_shard` guards its call to
`promote_if_drained` with `decrement() == 0 && phase == DRAINING`, but the
transition's precondition lives *inside* `promote_if_drained` — `phase ==
DRAINING && shards.is_empty()`, over a compare-exchange. Widening the call
site's `&&` to `||` therefore adds exactly two call cases, `decrement() != 0
&& phase == DRAINING` and `decrement() == 0 && phase != DRAINING`, and both
fail that precondition. `Tollgate.ConcurrencyGauge`'s
`promote_outside_its_precondition_is_a_no_op` proves the call is the identity
off its precondition, so the added calls cannot change state and no test can
separate the spellings. What the guard actually buys is cost — it keeps the
ordinary release path off an O(shards) scan — and cost is what the performance
gates measure. The exclusion is written to that one mutant: the sibling
`==` → `!=` on the same line, and the `&&` inside `promote_if_drained` itself,
both stay in scope and are caught, because both delete a real call.

### A randomized witness is not a gate

The gate can flip a mutant between *caught* and *missed* with no code change
between the two runs, and GL-91 hit exactly that: `AccountLimiter::update`'s
`policy != config.policy` was caught on one pipeline and missed on the next.
Nothing about it changed. Its only witness was
`resolved_account_authority_carries_the_generation_winners_policy`, a
proptest drawing twelve random inputs, and the discriminating combination is
merely *likely* to be generated — measured at eleven kills in twelve local
runs, which is a coin the gate flips every pipeline.

A property test proves a claim over a range and is worth keeping for that. It
cannot be the witness for one branch, because "the gate passed" then means
"the seed cooperated". Every branch the mutation gate protects needs a
deterministic unit witness;
`reinstalling_identical_limits_does_not_republish_the_policy` is that one, and
it kills the mutant fifteen times in fifteen. When a survivor appears in a
region a proptest covers, suspect a missing unit witness before suspecting a
regression.

### Running one

```sh
./scripts/check_mutations.sh --package tollgate-core
TOLLGATE_PG_URL=... ./scripts/check_mutations.sh --package tollgate-store-postgres
TOLLGATE_PG_URL=... ./scripts/check_mutations.sh --diff origin/main
```

Do not edit the tree while a sweep runs. cargo-mutants rebuilds per mutant, so
a source change mid-run can make an unrelated mutant read as *caught*: a build
or test failure caused by the edit is indistinguishable from the mutant being
detected. The first `tollgate-core` baseline undercounted its survivors exactly
this way, and the same wrong-reason hazard is why the wrapper permits only one
worker when PostgreSQL is enabled.

## Zero-allocation embedding gates (GL-90, 2026-08-28)

**Allocation is attributed by a scope, not inferred from a process total.**
`tollgate-alloc-count` wraps `System` in test binaries and counts `alloc`,
`alloc_zeroed`, `realloc`, and `dealloc` through const-initialised thread-local
cells. `AllocScope::measure` snapshots those counters around one synchronous
operation; `AllocScope::assert_zero` rejects any allocation or reallocation.
Nested scopes compose and a guard restores the active depth during unwinding.
This chose a small audited counter over `dhat` because the contract needs to
exclude caller and executor work by construction and needs no process-wide
profiling machinery.

**Unsafe code has one test-only home.** Implementing `GlobalAlloc` is itself
unsafe. Each allocator method forwards the pointer, layout, and size unchanged
to `System` inside an explicit unsafe block and documents that contract. The
allocator crate permits that boundary; every production crate and binary
forbids authored unsafe code. It is a dev-dependency, and the allocation gate
also checks the normal dependency trees for `pricing-api` and
`tollgate-server`, so the counter cannot enter either release artifact.

**The zero is a warmed steady-state zero, with every exclusion named.**
ArcSwap may allocate a process-lifetime debt node when no reusable node exists;
Moka's crossbeam epoch state initialises on a thread's first use; Tokio's
bounded MPSC acquires storage in blocks. The harness warms the exact measuring
thread and queue topology before attribution. Those cold and queue-boundary
costs are dependency behavior, not permission to subtract an observed total:
caller request buffers, request-id construction, executor job creation, and
Tollgate work each have their own report line. The Tollgate line covers cached
authentication, usage-slot reservation, admission and its pending debit,
commit, guard drop, and queue recording. The writer task does not run during
that synchronous scope.

**No business clock is hidden behind that wording.** The snapshot and lease
decisions use the caller's `Timestamp`; neither core nor admission reads a
wall clock to decide policy. Moka calls its internal monotonic clock during a
lookup and governor's `DefaultClock` does the same during a token check. Those
facts predate GL-90 and are part of the measured mechanism cost. Saying simply
"no clock read" would be false; saying the monotonic tick is admission truth
would be worse.

**Lookup cardinality is observed at the trait boundary.** A forwarding
`CountingMap<M>` implements the complete `SnapshotMap` contract and counts
`get` and `get_at`. Successful, pre-admission-denied, strictly unfunded, and
elastic outcomes each require exactly one `get_at` and zero `get` calls. This
is a behavior witness, not a source-text or statement-order assertion; the
staged path fixed by GL-96 must repoint the same witness across `begin` and
stage-two admission.

**Mutation evidence distinguishes generated mutants from deliberate
anti-patterns.** cargo-mutants can mutate the new counter implementation and
the normal `--diff` gate must kill those mutations. It cannot generate an
inserted map lookup, heap allocation, mutex, or linear table scan. Those four
edits are applied and reverted by review, with the failing allocation,
lookup, contention, or table-size witness recorded in the MR. No second
mutation runner pretends otherwise.

The GL-90 red-team runs made those sensitivities concrete. Replacing the direct
index with a linear scan moved `quote_4096_classes/quote` from ×0.88 to
×833.41 (maximum ×1.50). Serializing ArcSwap lookup behind one global mutex
moved different-account contention to ×20.16 over default uncontended and
×16.01 over sharded uncontended (both maxima ×8.00). MR A separately
showed one inserted allocation and a second map lookup failing their exact
structural witnesses. Every mutation was reverted before validation. The
final MR B `--diff origin/main` sweep tested 103 generated mutants: 95 caught,
8 unviable, 0 missed.

`scripts/check_allocations.sh` owns the five test targets, writes
`reports/allocation_report.json`, and is a required CI job distinct from
Criterion. This is a compatible test and documentation change: no public Rust
API, wire shape, invariant bypass, migration, runtime threshold, or release
artifact changes.

## Performance gates

`scripts/check_perf_thresholds.sh` — criterion microbenches vs
`testing/perf_thresholds.json` (freshness-marked, stale output rejected).
`scripts/check_load_thresholds.sh` — the
example API under the `production` profile (fat LTO), admitted vs no-admission
baseline over persistent loopback HTTP, selecting optional threshold/report
paths. `testing/load_thresholds.json` is the controlled-host manifest and
retains absolute p50/p99 ceilings and sequential/concurrent throughput floors.
`testing/load_thresholds_ci.json` carries the same workload and ratio ceilings
but sets latency and throughput absolutes to `null`; that explicitly disables
host-dependent verdicts rather than hiding a huge ceiling inside a nominal
gate. Latency ceilings and throughput floors are each an all-or-neither pair.

`testing/perf_baseline.json` is generated, whole, by
`./scripts/check_perf_thresholds.sh --record`, from the per-row median of at
least three distinct readable full runs on the host it names. Each complete,
readable full run with recording provenance deposits its measurements under
`target/perf-samples`; recording takes the median over samples with the same
revision, host ID, architecture, CPU, OS, compiler and profile, and refuses
with fewer than three, because
one run says where a benchmark landed once and a baseline has to say where it
usually lands. It carries the host, CPU architecture, operating system, compiler,
profile, source revision, timestamp, and every gated benchmark mean. The
default regression allowance is 5%; a row may carry a larger value only with
measured evidence, and a re-record carries those widened bounds forward rather
than resetting them. The file records how many runs its medians came from.
Recording refuses a row missing from any sample, a run that measured only some
rows, a
run the gate would not draw a conclusion from, provenance the tool was not
given, a revision that is not a committed sha, and a host that does not match
the file it would replace. The staged file is validated and only then promoted,
so a failed record leaves the previous baseline in place. The destination
supplies the host check and carried bounds independently of `--baseline`,
which selects the comparison input. Exclusive staging serializes recorders
from destination validation through atomic promotion.

The benchmark freshness marker's mtime identifies each run. Samples are
published atomically and exclusively; retries and copied files count once,
and divergent evidence for one run is refused. Legacy deposits without run
identity and environment are warned about and skipped. Ordinary gating does
not require a recordable sample: sample failures are diagnostic, while
`--record` requires `--samples`, provenance and a successful complete deposit.
Ratios-only runs never contribute calibration samples.

Provenance is per file, so a partial re-measure is not representable: the
`recorded_at` and `git_revision` a baseline declares apply to every row it
lists. Re-measuring five rows and carrying the rest forward therefore makes
the file claim a recording fourteen rows did not get, which is how
`admission/full_check_sharded` and `admission/full_check_contended_8` came to
be gated against a pre-GL-91 denominator while the metadata said otherwise. A
baseline update re-measures every row — which is why there is no longer a
supported way to update one by hand (GL-114).

The source revision must contain the final mechanism every row exercises, not
merely the commit that introduced a benchmark. A hot-path change that adds work
to an activated witness invalidates that row even when the benchmark function
itself is unchanged, so the change that adds the work re-records the file and
validates it with a fresh run before calling itself complete. A favorable
single run does not preserve a baseline whose provenance predates the
implementation.

Run shape is part of provenance. `scripts/check_perf_thresholds.sh` measures
each row inside a full-suite run, so a row recorded from an isolated
single-benchmark run records a value the gate cannot reproduce.
`admission/full_check_lease_exhausted_strict` is the worked example: 58-61 ns
measured alone, 68.0-68.5 ns across six consecutive full-suite runs, and 61.1
ns in a full gate invocation after an idle gap. Its recorded 56.442 came from
isolated witness runs and no gate run could meet it. The row is recorded at
the full-suite median with the default 5% allowance, which puts its ceiling
above the whole observed envelope so the gate cannot red-light a clean tree;
the row's absolute target and threshold remain the second signal. A change to
that mechanism is expected to re-measure rather than trust the envelope.

`admission/full_check_contended_8_sharded` carries 15% because eight-thread
contention disperses about 10% across quiet runs on this host, with occasional
lower outliers; its two ratio gates remain the portable check.

GL-91 exposed that failure mode directly. The request-rate witness was recorded
at `092fb74`; `97c9ea6` then made disabled concurrency track both principal and
account occupancy so a later ceiling cannot overlook live work. The benchmark
source did not change, but its production path gained two required atomic
transitions and the old 101.1 ns result stopped describing the implementation.
The final mechanism was therefore measured through five independent exact
witness runs for every affected baseline that no longer passed: the ordinary
full check, strict and elastic lease exhaustion, and request rate. Their
medians were recorded rather than inferred from the earlier samples or from a
noisy whole-suite run. The same-pattern sharded and contended rows remained
inside their existing baselines; the configured-concurrency witness was also
remeasured independently and remained below its existing baseline.
The two isolated lookup rows use 10% because unchanged production code moved
6.1% and 7.5% across quiet sessions while `full_check` returned to 0.7% on an
exact replay. Setting `TOLLGATE_PERF_HOST` to the recorded host id activates this
comparison. An unset or different host, an UNTRUSTED run, or `--ratios-only`
records `baseline-skipped` rather than applying a foreign number. A *full* run
that skipped it that way now exits `UNENFORCED` (4) instead of PASS, because
under local-only measurement that report is the acceptance evidence and it
checked nothing host-specific (GL-114). A `--ratios-only` run is unaffected: it
says in its own mode that it reached no absolute verdict.

A row whose own confidence interval was too wide to read yields `inconclusive`
against the recorded baseline rather than `regressed`. GL-112 gave the ratio path
that rule and GL-114 found the baseline path had never received it: a validating
run failed `admission/request_rate_token` at 173.1 ns against 126-132 ns in
every neighbouring run, on a row the same report had already flagged unstable.
Instability withholds a failure and never manufactures one — a row inside its
bound still passes however wide its interval — and an enforced run in which
every comparable row came back inconclusive does not read as a pass.

The unreadable-run breadth signal counts only rows that are *normally* steady.
A row whose baseline carries a widened `max_regression` was widened because
somebody measured it dispersing between quiet runs, so its wide confidence
interval describes the benchmark rather than the host. Counting it inverted the
signal as the manifest grew a contention family: across ten full GL-114 runs, 39
of 61 unstable flags landed on those thirteen rows and
`admission/full_check_contended_8_sharded` was flagged in all ten, so half the
runs in one session abstained on a quiet machine. Those rows are still flagged
individually and still make their ratios inconclusive; they no longer vote on
whether the host was disturbed.

Every report also carries `drift`: the median and quartiles of the per-row
ratios against the baseline, across the whole run. One row over its bound with
the run median at ×1.00 is a regression; nine rows over their bounds with the
run median at ×1.03 is a machine. That distinction used to require reading
every row of the artifact by hand.

The load test retains the original sequential scenario and also synchronizes
the configured number of persistent clients after warmup, all authenticated
to the same account. Warmup and measured request counts remain totals per
scenario and are distributed exactly across clients, so adding concurrency
does not silently multiply the workload. Before the September 10 local-only decision described below, default-target
merge requests ran the CI manifest automatically and retained its JSON report. The load
binary's explicit `--evidence` mode records a threshold miss as `passed: false`
without failing the CI job; invalid configuration, build, execution, and report
failures remain required failures. That CI arrangement also had a manual Criterion lane tagged for the controlled
host and a required `perf-ratios` job after formal and mutation assurance. Load
evidence followed it. This ordering is historical; timed jobs were removed on
September 10. Benchmark compilation remains required everywhere.

On a controlled host, the sequential scenario gates its p50 overhead ratio,
absolute ceilings, and admitted-throughput floor. The concurrent same-account
scenario gates a separate p50 ratio and throughput floor at the configured
connection count; changing that count requires recalibration because it changes
the workload. Five quiet-host repetitions at
10 connections produced paired ratios of ×1.015–×1.104, so the provisional
×1.20 ceiling leaves about 8.7% headroom above the worst observed run. The
pricing-api fixture exposes one account, so distinct-account end-to-end
contention is explicitly outside this gate's scope; only the direct admission
microbenchmark currently covers distinct accounts. Absolute latency should
never gate shared CI; either ratio is portable only for its like-for-like
connection count.

Waiting for mutation and formal jobs removes contamination from this pipeline,
but it cannot reserve a shared runner. A release-only merge request later
measured a sequential baseline p50 of 14.9 µs and admitted p50 of 17.6 µs
(×1.182 against ×1.15) without any production-code change, reproducing GL-49's
false-failure pattern. The automatic load lane therefore reports its verdict
as non-gating evidence without leaving every affected pipeline in a warning
state; a calibrated, controlled-host run owns the threshold decision. Failures
to produce that evidence still fail the job.

### 2026-09-10 — The baseline that was never enforced (GL-114)

A full local Criterion run of the v0.18.0 candidate failed nine recorded
baseline comparisons while all 58 absolute bounds passed. The candidate range
was two commits and touched almost none of the affected paths, so the question
was never "what did this release break".

It was the first run that ever enforced the baseline. `should_enforce_baseline`
requires `TOLLGATE_PERF_HOST` to equal the baseline's host id. Nothing set it
locally, and the retired `perf-thresholds` CI job set `perf-i9-10920x` against
a baseline naming `mistral-apple-m1-pro` — a value that could never match. Both
earlier local reports in `reports/` record `skip_reason: "host-unset"` and
`passed: true`, with every regression row `baseline-skipped`. Twelve days of
drift accumulated behind green verdicts, and the failure was invisible by
construction: the gate reported PASS for checking nothing.

The drift itself was recorded in the repository the whole time, in the manifest
comments of the rows it invalidated:

- `admission/full_check_sharded` was recorded at 122.336 ns on 2026-09-01.
  GL-111 (`bfd6c31`, 2026-09-05) then measured that same benchmark at 140.48 ns
  before its own optimisation and 137.18 ns after, wrote both numbers into the
  row's `_comment`, and did not touch the baseline. The convention said rows
  are recorded by the change that lands their benchmark, and GL-111 landed no
  benchmark — so a change that measured a row 12% above its baseline had no
  place to put that fact except a comment.
- `capacity/disabled` was recorded by GL-99 at 123.61 ns "against that
  benchmark's same-run `admission/full_check` of 122.47 ns". `admission/full_check`
  was carrying 119.54 ns from GL-93 at the time. GL-99 knew the same-run value of a
  row it was not recording, and left it.
- `cost_table/quote` is the plainest of them, and it indicts GL-91 with its own
  work. The laptop-numbers table above already reads "1.7 ns before the staged
  workload fold; 2.30 ns after GL-91". `42f4aca` recorded the row at 1.696 ns
  from `3fcac12`; `5a22126`, a *later commit of the same issue*, then turned
  `quote` into a delegation to the new `quote_workload` — a fold with an
  emptiness check and checked accumulators where a direct index had been — and
  did not re-record. Measured: 1.70 ns at `3fcac12`, 2.25 ns at `7ae62a5` one
  commit later. This is exactly the rule stated above — the source revision
  must contain the final mechanism the row exercises — broken by the issue
  whose lesson that rule was written from.

The rows themselves were sound when recorded. Re-running the full suite at
`3fcac12`, the revision the baseline names, reproduces every 2026-09-01 row on
a quiet host across two independent runs: `cost_table/quote` 1.708 and 1.691 ns
against 1.696, `admission/request_rate_token` 107.624 and 107.612 against
108.588, `admission/full_check_sharded` 121.706 and 121.897 against 122.336,
`credential/verify_cached` 15.695 and 15.995 against 16.111. Run medians ×0.987
and ×0.992.

So what the convention hid was not bookkeeping drift. It was real cost added to
the request path between 2026-09-01 and v0.18.0 — `cost_table/quote` from 1.70
to 2.24 ns, `admission/request_rate_token` from 107.6 to 128–148 ns — by
changes that each measured their own new rows and left the rows they had made
slower alone. A stale baseline is not merely an inaccurate number; it is the
absence of the signal that would have made someone ask whether the cost was
worth paying, at the point where it was still cheap to answer.

The same convention also left rows too *loose*.
`admission/full_check_lease_exhausted_strict` carries 68.253 ns, which measures
58.2 and 60.3 ns in these quiet runs at the revision that recorded it. A bound
15% above the code cannot bite, and it passed the failing run untouched.

Nine rows failing is also not nine regressions. The run's median ratio across
all forty-five baselined rows was ×1.030, quartiles ×1.005–×1.044, on a host
carrying a load average of 6.89 with unrelated work running. Against a 5%
bound that leaves about two points of headroom, so the rows that crossed were
largely the ones with the least of it — and a different quiet-host run on the
same code put `capacity/uniform` and `lease/overage_commit_fallback` over
instead, while four of the nine stayed inside. Reports now carry that median
and its quartiles as `drift`, because reconstructing it required reading every
row of the artifact by hand.

The three defects are at three layers, and each hid the next: a convention that
made partial calibration the normal case, an enforcement condition that no
environment ever satisfied, and a run-quality detector that an isolated
checkout can never feed. The fixes match: `--record` writes the file whole or
refuses; a full run that skipped the baseline exits `UNENFORCED` rather than
PASS; `check_ci_rules.sh` rejects any CI job that sets `TOLLGATE_PERF_HOST`;
and the guardrail test that asserted `entries.len() < manifest.benchmarks.len()`
— which *required* the baseline to stay incomplete and would have failed the
moment anyone finished it — now asserts the manifest and the baseline cover
each other exactly.

### Trusting the measurement, not just reading it

A threshold comparison answers "is this number too big", never "is this number
real". Three times in one day a contaminated run was reported as a confident
PASS or FAIL: a branch touching neither lease benchmark saw both breach their
threshold because every `tollgate-core` benchmark had inflated ~2.6×; a
criterion baseline captured on a busy host made a later change read as 16%
faster than it was; and a load-gate ratio of ×1.241 came from a *baseline*
denominator 15% below its own three-run band. Each was caught by a person
noticing the affected benchmarks were unrelated to the change (GL-49).

So the perf gate has a third verdict. Alongside PASS and FAIL it can answer
**UNTRUSTED**, with its own exit code, and it declines to draw a conclusion
rather than substituting a different one.

The discriminator is breadth: **a change moves one or two benchmarks; a
disturbed host moves most of them.** Each benchmark is compared against its own
value on this host's previous run, and the run is untrusted when more than one
benchmark, and at least a third of them, have shifted past a tolerance
(defaults 40% and ⅓, both settable in the manifest). Comparing against history
rather than an absolute band keeps the check free of the host-dependence the
manifest's own thresholds concede to. Measured run-to-run drift on a quiet host
is 0.04–3.5%, so the tolerance has two orders of magnitude of headroom before
it can cry wolf.

An untrusted run deliberately does **not** record history: a contaminated
measurement must never become the yardstick the next one is judged against. If
the host itself changed, deleting `reports/perf_gate_history.json` re-baselines.

Criterion's own confidence interval — already in the file the gate has always
read, and previously discarded — is recorded per benchmark, so a measurement
that was unstable *while it ran* is marked even when its mean looks plausible.
Both reports also record the wall-clock time, core count and load average, so a
borderline result can be diagnosed later rather than only re-run.

**A branch whose two arms are both cheap is not a branch (GL-111).**
`Locality::index` reduced a locality onto a shard count by testing
`shards.is_power_of_two()` and masking or dividing accordingly. The test looks
free and the fast arm looks like one `and`; what aarch64 actually emitted was
the mask *and* the 64-bit division, computed unconditionally and selected
between with a `csel`. LLVM speculates both arms of a cheap branch, so every
sharded lookup in the workspace — local leases, rate shards, observability
shards, and GL-99's capacity pools — paid for a division it discarded.

The shard count is fixed for the process lifetime, so the choice belongs at
construction: `LocalSharding` now carries the reduction it implies, as a mask
or a sentinel saying there is none. The emitted fast path is a compare, a
branch, and an `and`; the division survives only in the arm that needs it, and
only when it is taken. Three interleaved pairs put `admission/full_check_sharded`
at 140.48 ns before and 137.18 ns after — −2.3%, non-overlapping — and the
contended sharded path moved the same way with a spread too wide to quantify
from two pairs.

The sentinel is `usize::MAX` rather than an `Option`, because this value is
copied on every sharded lookup: the option costs sixteen bytes and pushes the
struct past what fits in registers, which the assembly showed as two extra
loads through a pointer. `shards - 1` reaches `usize::MAX` only at `2^64`
shards, so the encoding cannot collide with a real mask.

**Marking a measurement unreadable and then reading it is not a verdict
(GL-112).** That per-benchmark instability flag was reported and nowhere else
consulted: `evaluate_ratio` divided the same means and issued PASS or FAIL from
them. Two pipelines on one shared runner then measured the *same unchanged*
`reservation/commit_cancel_race_contended_2` against its control at ×3.40 and
×13.11, and `lease/overage_commit_fallback` against `lease/reserve_commit` at
×1.50 and ×2.14 — the second of each failing a GL-99 merge request whose diff
could not touch either mechanism. In the failing run the gate had already
printed `UNSTABLE spread` for one side of both.

So an unstable side now yields **inconclusive** rather than a verdict, in
either direction: an unreadable measurement that lands under the ceiling is no
more readable than one that lands over it, and calling that a pass is the same
mistake pointing the way nobody notices. This is the per-measurement form of
the UNTRUSTED rule above, and it exists because that rule needs history on the
same host to fire — which shared CI, with a fresh workspace every job, never
has. A confidence interval needs no history.

Inconclusive does not fail the gate, but a run in which *every* ratio is
inconclusive does: a gate that measured nothing must be red rather than quietly
green, which is the same reason `check_ci_rules.sh` exists. The report records
which side was unreadable, so the artifact explains an inconclusive without the
console log beside it. Widening the two bounds instead was considered and
rejected — a ×13.11 observation means any bound that admits the noise admits a
fourfold regression with it.

**That was necessary and not sufficient, which the retry proved.** Re-running
the same job failed on a different ratio — `cost_table/quote_workload_8` over
`quote_workload_1` at ×11.71 against a 6.00 bound — and *neither* side was
marked unstable. The numerator had gone from 17.7 ns to 44.2 ns with a tight
interval: contamination as a level shift, which a confidence interval cannot
see and only history can, and CI has none. But six of its neighbours were
unreadable in that same run, and that is visible without history.

So UNTRUSTED gained a history-free sibling. The discriminator is the breadth
argument already used for shifts, applied to instability: **a change makes one
or two benchmarks noisy; a disturbed machine makes many.** Three observed runs
of one unchanged tree calibrate it — the clean pipeline marked 0 of 39
benchmarks unreadable, while the two that produced false ratio failures marked
8 of 33 and 6 of 33. The fraction is 10%, below those 18–24% and above the
handful a quiet controlled host marks on its contention benchmarks, with the
same "more than one" guard the shift rule carries so a single flake never
condemns a run. It is checked before the shift rule, because a run that cannot
read itself is not made readable by having history to compare against.

This is the verdict shared CI can actually reach. The shift rule needs a
previous run on the same host, and every CI job starts from a clean workspace —
so before this, `assess_trust` answered `NoHistory` there no matter how
contaminated the run was, and the two limits stated above ("CI has no
measurement history") described a gate with no trust check at all rather than
one with a weaker one.

**And an unreadable run means different things in the two places it can
happen.** On a controlled host it is a failure with an action attached: the
machine is there, so re-run it idle. Shared CI has no idle host to re-run on,
so exiting non-zero there reports nothing about the change and blocks a merge
request for the state of a runner — which is the defect itself, and renaming
FAIL to UNREADABLE would not have removed it. Both observed contaminated runs
were contaminated; retrying was not a strategy either. So `--ratios-only`
abstains and records the readable ratios as evidence, exactly the trade the
loopback load lane already makes, while the controlled-host mode keeps failing.

The abstention is narrow. It covers *measurement* verdicts only: a benchmark
the run did not produce, or produced older than its freshness marker, still
fails in both modes, because a missing measurement is a configuration defect
that a quiet machine would not have fixed. That distinction is what keeps
"unreadable" from becoming a way for the gate to stop noticing that it is not
running.

Two limits worth stating. **CI has no measurement history**, because each job
starts clean. Criterion therefore gets no history-based trust verdict there,
and the recorded baseline is skipped because the shared host id does not match.
The required Criterion job still fails closed on missing/stale output and all
same-run ratios. The load job compares admitted and baseline runs inside one
job but retains ratio misses as evidence under GL-49. Both measurement jobs wait
for mutation and formal assurance, then run serially: the first required load
run scheduled all three together on one runner manager, reached load average
23.02 on 24 logical CPUs, and moved the concurrent ratio from the quiet-host
range to ×1.333. The dependency removes that self-inflicted CPU and memory
contention. Reports still record machine context because neither job can
classify unrelated host contamination from a single shared-host run.
Same-run does not make thread scheduling portable: the first required GL-90 run
measured the sharded/default same-account ratio at ×0.88 versus ×0.16–×0.24 on
the controlled laptop, and sharded different-account contention at ×5.55 over
uncontended versus ×1.42–×1.60. Their cross-topology bounds are therefore ×1.25
and ×8.00. The latter remains a real lock witness: the deliberate global mutex
measured ×16.01 and still fails it.
And **CPU saturation is not what breaks these measurements** — with all cores
busy for five minutes, every benchmark stayed within 0.5% of its quiet value.
The original 2.6× inflation came from memory pressure and swapping, which is the
condition to watch for and the reason the load average alone is context rather
than a verdict.

The sustained `full_check_contended*` fixtures quote one unit while still
traversing the weighted governor check. Governor cannot replenish faster than
its one-nanosecond token quantum; the normal 114-unit fixture exhausted even
the maximum bucket under eight workers on a fast shared runner and turned a
contention witness into a delayed `RateLimited` panic. One unit keeps the
mechanism present without making the benchmark its own workload limiter. The
uncontended and refusal fixtures retain the original 114-unit quote and their
recorded baseline.

Current laptop numbers (Apple Silicon, 2026-08). The `full_check*` rows were
re-measured at load average 17–26, so they are ranges and backstops, not
calibration:

| Measurement | Result |
|---|---|
| `cost_table/quote` | 1.7 ns before the staged workload fold; 2.30 ns after GL-91 |
| `cost_table/quote_4096_classes` | 1.45 ns before GL-91; 2.26 ns after GL-91 (×0.98 of the same-run two-class quote) |
| `snapshot/admit` | 1.2 ns |
| `lease/reserve_commit` | 43.2 ns |
| `lease/reserve_commit_contended_8` | 3.58 µs (×89.9 of the same-run uncontended path) |
| `reservation/cancel_after_commit` / `commit_after_cancel` | 42.4 ns / 46.8 ns |
| `reservation/commit_cancel_race_contended_2` / bare-CAS control (4,096 races) | 165.6 µs / 64.1 µs (×2.58) |
| `admission/snapshot_lookup` (arc-swap / moka) | 14.6 ns / 70.8 ns |
| `admission/full_check` (default single counter) | 107.86 ns median of five final-state-machine means (106.03–111.11 ns) |
| `admission/full_check_contended_8` (default, one account) | 2.74–2.80 µs |
| `admission/full_check_sharded` (8 local shards) | 116–124 ns |
| `admission/full_check_contended_8_sharded` (8 local shards, one account) | 446–618 ns (×0.17 of default contended) |
| `admission/full_check_contended_8_distinct_accounts` (default / sharded) | 609 ns / 224 ns; map-owned counters remain the cross-account residual |
| `admission/full_check_denied` (unknown principal) | 15.6 ns |
| `admission/full_check_lease_exhausted_strict` | 61.40 ns recorded median of three runs (60.33–62.48 ns) after GL-128's exhaustion check; default 5% baseline allowance |
| `admission/full_check_balance_exhausted_strict` (confirmed exhaustion) | 61.91 ns recorded median of three runs (60.26–62.11 ns); ×0.99–×1.01 of the same-run lease-exhausted refusal |
| `admission/full_check_lease_exhausted_elastic` | 117.92 ns median of five final-state-machine means (116.95–121.43 ns) |
| `admission/request_rate_token` (weighted bucket disabled) | 106.51 ns median of five final-state-machine means (105.97–107.19 ns) |
| `admission/begin` | not yet measured on the current mechanism |
| `admission/concurrency_acquire` (8 threads, one account) | 3.25 µs |
| `capacity/disabled` / same-run `admission/full_check` | 123.61 ns / 122.47 ns (×1.01) |
| `capacity/uniform` / `reserved_shared` / `reserved_fallback` | 134.68 ns / 133.75 ns / 132.63 ns |
| `capacity/full_check_contended_8_distinct_accounts_uniform` / `_reserved` | 1.453 µs / 1.457 µs (×2.13, ×2.14 of the ungated 680.69 ns); sharded ×2.11 against unsharded ×2.83 in a separate paired run |
| sequential loopback p50 baseline → admitted (five-run medians) | 28.8 µs → 30.2 µs; paired ratios ×0.997–×1.101 |
| 10-connection same-account loopback p50 baseline → admitted (five-run medians) | 81.9 µs → 85.9 µs; paired ratios ×1.015–×1.104 |
| GL-90 replay sequential p50 / p99 / admitted throughput | 29.4 µs / 62.4 µs / 31.8k req/s |
| GL-90 replay 10-connection p50 / p99 / admitted throughput | 82.1 µs / 163.2 µs / 113.4k req/s |
| GL-91 staged replay sequential / 10-connection p50 | 28.6 µs → 28.2 µs (×0.987) / 80.9 µs → 82.4 µs (×1.019) |

### Reserved witness IDs

The manifest reserves names before the staged API exists; it does not carry
placeholder measurements. The implementing issue must add the benchmark,
manifest entry, script production, portable ratio where applicable, and a
controlled-host baseline together:

| Owner | Reserved witness IDs |
|---|---|
| GL-91 | three witnesses stand: `admission/begin`, `admission/request_rate_token`, `admission/concurrency_acquire`. The fourth, `admission/admit_staged_1`, was removed by GL-102: the same change had repointed `admission/full_check` at the staged path, so it measured that path twice |
| GL-92 | `cost_table/quote_workload_1`, `cost_table/quote_workload_2`, `cost_table/quote_workload_8` |
| GL-93 | `reservation/commit_split` and `reservation/commit_split_race_contended_2` (the issue requires the commit/cancel paths benchmarked uncontended *and* contended, so the reserved name gained a contended sibling ratioed against the existing `reservation/commit_cancel_race_control_2`), plus the split allocation report lines `reservation/commit_split` and `reservation/split_cancel` under the `tollgate_opt_in` attribution |
| GL-99 | all eight have landed. The four uncontended `capacity/*` witnesses and the uniform/reserved contention pair carry controlled-host baselines and portable ratios; `load/concurrent_distinct_accounts` and `load/mixed_saturation` landed with the multi-tenant example they required |

GL-99's disabled-gate proof is three-part, and all three parts now hold: the
allocation scope `capacity/disabled` records zero; the same-run
`capacity/disabled` : `admission/full_check` ratio measured ×1.01 against its
1.05 bound; and the assembly inspection shows `NoGate`'s acquisition compiling
to three instructions with no atomic and no branch. The reserved name was
`full_check_nogate`; it shipped as `capacity/disabled`, in the group that
carries the other five, so the whole feature's cost is read in one place.
Uniform, reserved-shared, reserved-fallback, and mixed-saturation results stay
separate so a product that selects `Disabled` is never charged for a feature it
did not enable.

## 2026-09-09 — Direct-store period maintenance (GL-107)

A stored budget schedule was actionable only when `tollgate-server` drove
`AdminStore::roll_due_periods`. Direct-store embeddings had the same ledger
and schedule semantics but had to supply their own trigger. `PeriodRoller` in
`tollgate-client` now owns that off-path lifecycle. It is a standalone manager:
`InstanceRuntime` continues to accept only the three data-plane store traits,
while the application explicitly supplies administrative authority to its
period-maintenance owner. HTTP-backed instances continue to use server-side
maintenance. No wire or schema change is required; the Rust API is additive.

One task calls the existing bounded store operation immediately at startup and
then periodically. A pass chooses one cutoff, drains saturated batches with a
yield between them, and ends on a partial batch, failure, stop, or deadline.
Both individual calls and the whole pass have deadlines. The next pass waits
`poll_interval` after completion, so a slow store cannot create catch-up bursts.
Defaults are a five-second interval, 256 accounts per batch (the store's
existing production limit), a five-second call timeout, a thirty-second pass
budget, and a five-second shutdown budget. A deadline can leave a legitimate
large backlog incomplete; that is reported and retried, not silently capped.
The driver retains O(batch limit) temporary data and O(1) progress state. It
adds no per-account tasks, admission allocations, locks, or clock reads. Backend
selection and scan costs are unchanged; the bounded driver does not turn the
memory backend's repeated account scans into an indexed operation.

The store remains the authority for calendars, locking, idempotency, allowance
expiry, and top-ups. Driver health means a recent pass reached a partial batch,
not proof that every account is current: `SKIP LOCKED` can leave work with another
replica. Readiness and metrics can retain a cloneable `PeriodRollerMonitor` while
the application moves the unique owner into shutdown. The monitor owns the
liveness interpretation, deriving failure from channel closure even if the
last publication was healthy. Cancellation of either an unpolled or polled
shutdown future still drops the owner and aborts its task.

Progress is evidence, not a second ledger. A store error, timeout, or cancelled
call can conceal committed work. Such calls increment `uncertain_calls`, while
returned batches contribute confirmed accounts and unit totals. Retrying is
safe because the backend owns the period marker; a retry does not reconstruct
missing accounting observations. Reports retain uncertainty across successful
recovery and shutdown. Unit totals are checked u128 values because one batch
can legitimately exceed u64; all cumulative counters report overflow and stop
scheduling rather than wrap. The shutdown deadline is an independent bound on
the task join. Like the other Tokio managers, these bounds require store futures
to yield rather than synchronously blocking the executor.

The sibling search found the server's existing private rollover drain and its
maintenance/readiness issues GL-71 and GL-88. Server maintenance is outside this
standalone API change and remains tracked there; no second server timer or
server-to-client production dependency is introduced. A stale `roll_period`
rustdoc link in the schedule contract was corrected to the actual batch API.
The direct-store example is a compiled module doctest, so it cannot silently
fall behind the public constructor or shutdown signatures.

`Tollgate.PeriodRoller` models single-call ownership, a fixed cutoff during a
pass, terminal stopping, and the absence of a healthy result after an incomplete
pass. It does not prove Rust refinement, timer preemption, or backend liveness.
The existing conservation proofs remain the arithmetic evidence. Paused-time
integration tests and generated clock/failure traces separately test the actual
manager, including missed and backward-moving business time, concurrent rollers,
committed calls with lost replies, task death, and cancellation.


## Timed performance validation moves local (2026-09-10)

Release !177 changed only version declarations and release notes, yet job
16410805160 failed `capacity/reserved_shared / capacity/uniform` at 1.22377
against 1.15. A fresh run on the same commit, job 16417175792, passed that
comparison at 1.02782 and instead failed `capacity/disabled /
admission/full_check` at 1.14145 against 1.05. Both operands of each failing
ratio were classified individually stable. The second pair was measured about
two minutes apart: a narrow within-benchmark interval cannot establish that
the host conditions stayed equivalent between benchmarks. All six new GL-105
attribution ratios passed both runs. This establishes inconsistent comparisons,
not a proven cause in the admission implementation.

Fresh local builds of the release commit on M1 Pro measured reserved/uniform
at 0.99841 and disabled/admission at 0.89964. Those filtered diagnostics helped
localize the incident but did not replace a full local acceptance run.

The runner audit also found that two separately tagged runner registrations
reported the same manager system ID. An attempted simultaneous diagnostic run
was stopped and its results discarded once that was discovered. Separate tags
do not establish independent hardware or exclude unrelated work on the same
host. The runner investigation is GL-113; no runner settings were changed for
this release repair.

Timed Criterion and loopback load validation now run locally. The remote
`perf-ratios`, `perf-thresholds`, and `load-thresholds` jobs and their unused
routing anchors are removed. Benchmark compilation, deterministic allocation
assertions, formal and mutation assurance remain required. The CI-rules checker
owns this scheduling policy and rejects the retired timed jobs; it also checks
that formal and mutation cannot become optional or target-dependent. No benchmark
workload, threshold, baseline, CLI mode or production code changes with this
scheduling decision.

[`PERFORMANCE.md`](PERFORMANCE.md) defines the local evidence and review
contract. A green CI pipeline no longer carries a timing verdict, and there is
no claim of a machine-verified local-report attestation. Reviewers need the
local reports, provenance and explicit limitations for performance-sensitive
changes and releases. The older remote-measurement scheduling narrative above
records the progression that this policy supersedes.


## 2026-09-11 — Calibration CLI evidence boundaries (GL-114 review)

The median recorder introduced in `2a9c5d1` tested arithmetic and host/bound
preservation through helper calls, but its CLI wiring did not enforce those
contracts. The wrapper always passed `--samples`, turning optional collection
into a recording requirement for dirty and host-unset runs. Promotion received
`--baseline` instead of the destination being replaced. An invocation timestamp
counted checker retries as measurements, and storing only the revision lost the
compiler and host context. Argument parsing accepted a missing samples directory
that the recording branch then treated as an internal invariant.

Process-level tests now exercise actual argument parsing, environment variables,
Criterion fixtures, sample files and destination replacement. They establish
that ordinary verdicts survive nonrecordable provenance, retries cannot satisfy
the three-run minimum, changed environments cannot be blended or relabeled, and
the destination's host and widened bounds remain authoritative. No timing
measurements run in these tests.

The sibling search in `tollgate-perf-gate` also found partial current runs could
promote earlier complete samples, direct sample writes and shared staging had
no exclusive writer, and invalid sample means could reach median arithmetic.
The recorder now requires the current complete deposit; staging owns publication;
finite positive means are checked before calculation, including overflow-safe
even-population medians. The generated comment now describes repeated medians
instead of a single run. Remaining `expect` calls handle validated finite
ordering or serialization of internal report structures, not missing CLI input.

The checked-in single-run baseline still needs regeneration and fresh validation
on the controlled host. Measurements were explicitly paused while the host was
on battery; no old measurement is relabeled as new calibration evidence.

The full MR mutation gate subsequently found coverage the narrower recorder
diff had not exercised: legacy sample-count decoding and the CLI's assembly of
the normally-steady population. The helper tests supplied already-filtered
counts, so they could not detect incorrect filtering in the process wiring.
Synthetic CLI cases now exercise both noisy contention rows and noisy steady
rows, including a missing measurement; a serialization witness preserves the
legacy one-sample interpretation. Small-population drift tests cover sorting
and quartile selection. The drift index's redundant clamp was removed: for a
nonempty population, the three fixed fractions are all strictly below one, so
the clamp could never change a result. These checks use synthetic data and do
not resume the deferred performance investigation.

## Usage-domain classification and schema guards (GL-64)

The set-wise ingest path used the same fallible conversion for a caller's
event units as for store operations that must fail as a whole. In both the
leased and overage branches, `to_i64(...)?` therefore rolled back valid
neighboring events. The mixed-batch fixture contained `u64::MAX`, but only on
an already-recorded request ID, so duplicate detection bypassed the conversion.
Fresh oversized events and corrected same-ID replacements now exercise the
classification boundary directly. Each backend retains its existing numeric
domain; memory's full `u64` range is already an explicit boundary witness.
The shared credential-activity rollback scenario also names that backend limit
explicitly, so it exercises aggregate overflow rather than per-event rejection.

The sibling audit found the reverse mixed-event order could panic in memory:
an accepted maximum overage followed by leased usage reached `expect("usage
overflow")`. Both orders now return an atomic refusal. PostgreSQL also called
all ingest failures database failures, masking application-level aggregate and
monotonic counter overflow as retryable. Those arithmetic outcomes now retain
`IngestError::Refused` through the generic transaction finisher. Connection,
constraint, and stored-corruption errors retain their retryable classification.

Earlier nonnegative migrations enumerated account and lease columns and missed
the billing-event table; the schema test repeated that omission. The audit now
covers usage units/fences and the later allowance/expired unit columns. Fence
readers in acquire, release and ingest share one positive-domain decoder, and
all three persisted fence locations reject zero as well as negatives.

0015 installs write guards without a historical scan; 0016 validates after
that transaction releases its exclusive locks. A failed validation retains
both corrupt evidence and the installed guards. Populated-schema upgrade and
repair tests exercise the actual migrator, including SQLx's refusal to start
with an older catalogue. [Usage accounting](USAGE_ACCOUNTING.md) specifies
rollout and recovery. Accepted ledger transitions are unchanged; existing exact
conservation proofs remain applicable, with integer limits and rollback checked
separately by implementation tests and mutation testing.

## Opaque backend errors stop at the service boundary (GL-70)

HTTP error conversion copied `Display` text from `StoreError` into a public
problem title. Authentication added later restricted callers, but did not make
database credentials or row values safe to disclose to them. Tests checked
status/code mappings and ordinary domain refusals without injecting sensitive
backend payloads. The new conversion matrix first reproduced a fixture password
in the public title, then covered every storage wrapper, permanent ingest
refusal, debug formatting and accidental logging of the same payloads.

The sibling audit found `AllocateError::Storage` had its own formatting path,
and `IngestError::Refused` also held arbitrary text. Readiness, reclaim and
budget-rollover warnings formatted backend errors directly. PostgreSQL startup's
userinfo-only redactor left query parameters, keyword connection strings and
malformed inputs visible; replacing a complete URL inside driver text could
not protect fragments. These paths now report fixed public/operational fields.
No backend string is parsed, copied or hashed to decide what may be disclosed.
Credential-page errors already used a safe title; they now share HTTP incident
reporting. The scope is server-owned backend diagnostics; typed TLS/configuration
diagnostics and backend implementation logging retain their separate contracts.

The public `ApiError` and `Problem` Rust shapes stay intact. Rendering adds an
optional JSON error ID and a private response marker; router middleware logs
that marker with the matched route template. It never inspects request bodies,
query parameters or arbitrary backend text. A generated ID identifies the same
response and warning; it is independent of caller-supplied correlation headers.
Entropy failure is explicit and cannot alter status or retry classification.
Existing clients can ignore the extension and keep their domain mappings.

Error IDs use the existing entropy dependency. The HTTP middleware retains an
existing route reference; only a diagnostic failure allocates/formats the fixed
size ID. Work does not scale with backend-message size, and no admission or
credential-verification path changes. Ledger equations, SQL schemas and accepted
state transitions are unchanged. Assurance consists of adversarial conversion,
real-router, binary-startup and captured-background-event tests plus the mutation
gate; existing formal models continue to concern authority and accounting, not
confidentiality of these response/log implementations.

## Server maintenance owns its readiness evidence (GL-71, GL-88)

The maintenance loop originally owned a detached join handle whose only use was
abort at server teardown. Store ping and maintenance success were tested
separately, so the suite could prove that a failure was logged while also
accepting a healthy readiness probe during that failure. Adding bounded drains
and rollover preserved that gap: rollover's result was not returned, and a
pending rollover delayed even publication of a failed reclaim outcome. The new
controlled-call test reproduces the old startup response as 200 before either
operation completed; the corrected service returns 503.

The worker is now the single publisher of two independently checked outcomes;
publication requires exclusive mutable access to that owner.
An outcome is published before the next operation can suspend. The observer
owns interpretation of watch-channel closure, so a last healthy value cannot
conceal task death. The owner separately holds a terminal atomic stop bit and
sets it before abort; late worker publications cannot restore readiness. `serve`
observes the task join and turns unexpected return, cancellation or unwinding
panic into a static operational error while closing its listener. Expected
shutdown cancellation permits the existing HTTP drain. Panic-abort builds still
terminate immediately, and a cancelled store call does not prove rollback.

Axum moves its supplied shutdown signal into a spawned watcher. Passing the
caller's future directly therefore let it survive cancellation of `serve`,
along with resources it retained. A new cancellation test reproduced that leak.
`serve` now owns the caller future and passes Axum only a one-shot receiver;
every server exit drops the sender, releasing the watcher and signalling
existing connections to shut down. The caller's future is dropped with its owner.

The first two failures of each operation warn; the third and subsequent failures
are errors. This three-attempt boundary is an alerting choice, not a proof about
any lease's TTL. Readiness withdraws at the first failure. Recovery names and
clears only that operation's checked count. Count exhaustion is explicit and
stops the task. Existing partial-progress records remain available and arbitrary
backend or panic text is never included in the new diagnostics.

The sibling audit covered both maintenance drains, owner cancellation, graceful
shutdown, the independent security reloader, and TLS handshake tasks. The
reloader had the same unobserved-exit pattern; an owned exit guard now reports
unexpected termination and a terminal owner flag suppresses alerts for deliberate
cancellation. Its last valid policy and signing-key expiry retain their own
contracts. TLS handshakes already use a joined `JoinSet` and report failures.
No remaining unobserved task-exit site was found in the server crate.

Sweep tests previously used a 150 ms sleep as evidence that scheduled work had
finished (GL-88). They now await call notifications: entering the next reclaim call
proves that the previous complete cycle, including rollover and outcome logging,
finished. Controlled backend calls pin failure/recovery ordering across awaits.
Timeouts are failure guards only. The existing process-global tracing dispatcher
remains necessary to avoid callsite-interest races between tests. Every teardown
observes its server result rather than swallowing a task failure.

Public Rust signatures, wire schemas, store transactions and the reclaim
interval's scheduling meaning are unchanged. `/readyz` is intentionally stricter;
the bare in-process `router` has no maintenance owner and returns 503, while
`serve` supplies the owned evidence. Authentication and API handler behavior are
unchanged. Health describes the latest completed outcomes and task liveness; it
does not add a deadline to a pending backend call or certify that no rows were
skipped under locks held by another replica. The work uses O(1) health state,
adds no per-batch allocation, and changes no admission or credential verification
path. State publication occurs once per operation outcome, not once per row.

`Tollgate.ServerMaintenance` proves independent failure/recovery and terminal
stop/exit predicates over atomic observations. An exhaustive short-trace Rust
oracle and counter-boundary tests complement the proof. Controlled HTTP tests,
owner-drop/channel-closure tests, panic tests and mutation checks witness the
implementation. They do not prove executor fairness, backend completion or
network delivery; those remain explicit assumptions and operational limits.


## Shutdown liquidity and unanswered HTTP grants (GL-115)

The full-stack loopback test inherited an immediate-balance assertion from
`e954255` (the initial HTTP topology): after shutdown, balance must equal the
original deposit minus committed usage. It checked billing first and conservation
only after that assertion, and did not inspect uncertainty. Bounded cancellation
of acquire calls (GL-78), and later consolidation (GL-109), admit a different valid
outcome: the server committed a grant but the manager never received its
capability. All known leases can be released while the unanswered grant remains
active, with its acquisition reported as uncertain. Existing client tests covered
that contract directly; the shared HTTP fixture retained the stronger assumption.

During unrelated mutation testing the fixture observed 27 liquid units where it
expected 53. The original unlogged schedule cannot be reconstructed, and a fresh
300-case stress run did not repeat it. A controlled allocator-result handoff over
real HTTP, TLS and mTLS reproduces those exact numbers without changing production
code: deposit 104, receive a 52-unit lease, bill 51, then let the server issue 26
units and cancel delivery of that result to the manager. At shutdown the ledger
holds 27 in balance, 26 in an active grant and 51 in settled usage, with no loss.
The runtime reports one uncertain acquisition. The same schedule works for both
an ordinary refill and a consolidation. Delivering the result instead permits
all 53 remaining units to be returned at shutdown.

The fixture now checks the complete shutdown report, retained runtime/account
uncertainty, exact recorded and settled usage, known-grant inventory, and
conservation before any reclamation. Only reported unanswered acquisitions may
remain active. After an explicit expiry-plus-grace input, their reclaimed count
cannot exceed uncertainty, all their units return, and balance equals the original
deposit minus the unchanged bill, with no active grants or loss. The controlled
cases also establish that expiry without grace returns nothing and that backend
reclamation does not erase historical runtime uncertainty. The test controls the
handoff of an HTTP result at the allocator seam; it does not model TCP internals
or prove that cancelling an arbitrary backend operation rolls back a transaction.

The sibling audit covered the shared full-stack fixture's loopback bearer, TLS
bearer and mTLS variants and the server suite's other balance/conservation
assertions. The shared fixture contained the only immediate-refund assumption
after runtime cancellation; the explicit release/consolidation tests already
observe completed calls. The client-side unanswered-consolidation and ambiguous
outcome tests already retain uncertainty and check TTL recovery. No production,
wire, database, dependency, admission or performance-threshold change is needed.
The existing conservation and lifecycle models remain applicable; these transport
and scheduling tests add implementation evidence, not a new mathematical proof.

## Elastic readiness is not evidence of a first grant (GL-116)

The elastic pricing-api test introduced in `5a8db89` waited for `/readyz`,
then required `admitted > admitted_overage`. That inequality asserted that at
least one request used a lease, although Elastic readiness explicitly permits
an instance with only overage headroom. The assumption survived because a
fast in-process allocator normally installed a grant before the HTTP loop.
The GL-71 mutation run exposed the latent fixture defect: the observed totals
were equal while the preceding admission, overage and cap checks passed.
That unrelated server mutation did not exercise pricing-api's funding path.
The original run did not record the grant schedule, so its exact interleaving
cannot be reconstructed from that failure alone.

The funded-start test now observes enough installed lease units for its first
51-unit request before sending work. A fresh grant has an unarmed refill
signal until spending crosses its low-water mark or a request is refused, so
observing this first grant does not depend on racing an ongoing refill. The
wait checks the actual funding gauge; yielding allows the background manager
to progress, and a timeout only bounds test failure. Readiness retains its
existing mode-dependent contract.

A second HTTP fixture makes the counterexample deterministic by keeping the
balance at zero until after forty requests. No first grant is possible before
that explicit deposit: readiness still succeeds, exactly 21 requests consume
1,071 of the 1,074 credit units, and the remaining 19 requests refuse with
zero charge. `admitted == admitted_overage == 21` is correct. After depositing
200 units and observing the first installed grant, the unchanged request
succeeds against the lease. Totals become 22 admissions, 21 on credit, 1,122
billed units and 1,071 overage units; neither the cap nor prior overage resets.
This controls funding availability through the existing store boundary,
without a production test hook. It is a counterexample to the fixture's
readiness premise, not a replay of an allocator RPC suspended with an already
positive balance or proof of the unlogged historical schedule.

Both cases pin admission counts and units, the overage qualifier, cap
refusals, execution counts, exact settled usage and overage, zero loss and
expiry, and ledger conservation after shutdown. The original mixed-funding
inequality remains in the case that establishes its premise. Existing exact
accounting and cap models remain unchanged; these HTTP tests are separate
implementation evidence, not new mathematical proofs.

The sibling audit covered every mode builder, funding gauge, readiness wait
and funded/overage assertion in pricing-api's test suite and helper. The old
elastic case was the sole mixed-funding inequality. The ordinary metrics
case uses Strict with a large funded grant, so its zero-overage premise is
already established; disabled-mode metrics deliberately retain null funding
gauges. The elastic test's obsolete 402 comment and this document's
unqualified cold-start lease requirement were corrected in the same change.
No production Rust, public API, wire shape, dependency, schema, threshold or
request-path operation changes.

## Lease TTL transport preserves its configured value (GL-76)

`HttpStore::acquire` converted `ttl.as_secs().max(0)` to `u32`, substituting
`u32::MAX` on overflow. The conversion was already present at the project rename
(`6b82662`) and was copied into consolidation in `bf96da1`. The configuration
validator deliberately accepts positive SignedDuration values, while HTTP's
whole-second DTO narrowed that domain silently. A 500 ms TTL became zero,
1.5 s became one second, and a wide duration saturated. Existing HTTP tests
used whole-second production-shaped values, so they did not exercise the
transport's loss of information. Independent acquire and consolidation tests
now reproduce the refusal on unchanged code with a one-nanosecond TTL.

One shared encoding owns both operations. A positive integral duration through
`u32::MAX` retains the original JSON shape. Other positive durations carry
`ttl_seconds: 0` plus the exact Jiff `ttl` string. The sentinel is part of the
compatibility contract: an older server ignores the additive string but
rejects zero before it can create or replace a grant. New servers reject
conflicting declarations rather than choose a precedence. Missing, malformed
or nonpositive values remain structured failures. The client rejects its own
nonpositive input before building a request. No global whole-second limit is
added to lease-manager configuration; such a limit would remove legitimate
values from direct backends to accommodate the lossy transport.

The public `HttpStore` trait signatures and legacy JSON requests are unchanged.
The two Rust wire DTOs now carry `ttl: LeaseTtl` instead of `ttl_seconds: u32`,
so their source compatibility break is deliberate and documented. All servers
must understand the new form before clients enable precise durations; rolling
back requires clients to stop issuing that form first. Backend policy ceilings,
accounting transitions, database schemas and admission code are unchanged.
The extra duration formatting occurs per control-plane lease operation, with
bounded representation size, and does not enter the request path.

The audit found both lossy conversions and replaced both with the shared type.
The remaining conversions in the HTTP/wire module were checked for another TTL
narrowing; none remains. The audit also exposed a separate durable precision
defect in Postgres: the direct allocator returns an expiry at 100 s + 1 ns but
stores 100 s in its microsecond column, allowing a sweep at 130 s with 30 s
grace to reclaim one nanosecond before the advertised boundary. A direct-store
witness reproduces this on unchanged PostgreSQL source. GL-117 enumerates expiry,
grace, release and reclaim conversions and requires a storage/rolling-upgrade
design for active grants. That work is separate under the repository's
explicit design/rollout exception; this transport change does not claim to fix
or prove the finer durable boundary.

Boundary examples, a property test across positive SignedDuration's seconds
and nanoseconds domain, real HTTP against MemoryStore and PostgresStore,
legacy-server tests and invalid-input ledger checks establish implementation
evidence. The tests distinguish transport fidelity from the allocator's policy
clamp and from persisted timestamp precision. The PostgreSQL CI job executes
the HTTP parity test with a required database, and the mutation profile places
it in the same serialized group as the mirrored backend suite. Existing formal
accounting and timing models are unchanged; no new mathematical proof of
serialization or durable nanosecond timing is claimed.
## Exact durable lease timing (GL-117)

The GL-76 transport audit exposed a lease allocated at 100 s with a 1 ns TTL:
Postgres returned 100 s + 1 ns, stored 100 s, then reclaimed at 130 s with
30 s grace. The expiry truncation was already present at `6b82662`; the
grace-window change in `df0a6a5` copied the same microsecond narrowing into
release and reclaim. Existing boundary tests used integral seconds and did
not compare the durable instant with the returned grant. The GL-117 audit also
reproduced MemoryStore reclaiming at Timestamp::MAX when expiry plus grace
was beyond that instant: saturating a failed addition shortened the window.

`GrantPolicy::reclaim_cutoff` now owns the checked subtraction for both
backends. Underflow means no expiry is due. The expiry calculation still
applies the same maximum-TTL policy and refuses overflow before any ledger
transition. PostgreSQL stores exact expiry as floor microseconds and a
nonnegative submicrosecond remainder; a lexicographic SQL comparison has the
same ordering as the represented instant. This avoids a decimal dependency
or string encoding, retains the existing SQL round-trip and transaction
shape, and keeps all work on the control plane. MemoryStore's indexed sweep
receives the same computed cutoff instead of adding grace per candidate.

Migration 0017 deliberately fences older lease SQL by renaming the expiry
column. An additive column alone would leave an already-running old sweeper
free to reclaim a precise new grant using only microseconds. The transactional
rename waits for old table locks and makes later old statements fail; old
startup also refuses the unknown catalogue version. This needs a coordinated
backend maintenance window, not a transparent mixed-version rollout.

Legacy rows cannot yield the discarded fraction. Migration retains the
latest expiry consistent with truncation toward zero and records
`expiry_is_upper_bound = true`. Settlement may be delayed by at most 999 ns,
or 1,998 ns for the zero bucket straddling the epoch, but never accelerated.
Accounting fields and capabilities are preserved. Newly allocated grants
are exact; normal settlement drains the active legacy population while its
marker stays on history. Invalid historical timestamps abort the transaction
without changing schema or accounting. `docs/LEASE_TIMING.md` specifies the
maintenance, visibility and recovery contract; there is no silent downgrade
that drops the new precision.

The sibling audit covered grant expiry, both allocation paths, locked release
rows, grace construction, reclaim threshold arithmetic and MemoryStore's
indexed sweep. No lease microsecond truncation or saturated deadline remains.
Other conversions were classified: usage/activity explicitly have a shared
microsecond reporting contract; period boundaries are exact integral instants;
revocation timestamps are informational once their presence retires the key.
Credential validity is different: `insert_key`, `active_keys`,
`active_keys_page` and `credential_from_row` narrow `not_after`. A public-API
audit reproduced a 100 s + 1 ns expiry projecting as 100 s (and refusing one
nanosecond early), and -1 ns projecting as zero (extending the returned bound).
GL-118 enumerates that separate security migration and projection/session rollout.
Lease migration's upper bound must not be reused for authorization authority.
This deferral uses the explicit separate-design/rollout exception, and the
reproducer is retained with GL-117's evidence. The reclaim ordering concern in
GL-65 is fixed separately; this timing correction did not address it.

Lean's `LeaseTiming` proves exact encoding/order, cutoff equivalence and the
legacy upper bounds. It does not certify SQL, driver behavior or finite Jiff
arithmetic. Property tests use an independent i128 nanosecond oracle; mirrored
backend scenarios prove the implementation witnesses through acquire,
consolidation, release and idempotent reclaim. Migration tests exercise
existing rows, retained old connections, refused old startup, restart
durability, invalid history and both schema-domain bounds. The existing
Conservation model still supplies the exact accounting transition argument;
ledger tests separately check the actual transactions. Mutation runs stay in
CI. No hot-path, threshold or baseline changes are part of this correction.
## Credential expiry keeps its source precision (GL-118)

The broader GL-117 timestamp audit found that `insert_key`, the unbounded
directory read and both paged-read SQL shapes still converted credential
expiry to microseconds. `c56af1e` introduced that storage choice with the
durable directory; `67d195d` reused it for the projection feed. A credential
expiring at 100 s + 1 ns was projected as 100 s and disappeared one nanosecond
early. A pre-epoch expiry at -1 ns was projected as zero, extending its returned
authority bound. The old endpoint test compared only microseconds, so it
specifically could not detect the fraction it discarded. Shared backend
tests now compare exact instants and activity sets against integer-nanosecond
oracles at every boundary, including the two timestamp endpoints.

The defect is owned by the durable source. HmacRegistry, KeyManager and
SessionCredential already preserve and enforce the timestamp they receive;
re-verifying on the request path would retain the wrong source bound while
adding work. `StoredInstant` therefore generalizes GL-117's exact integer pair
for both lease and credential expiry. The source compares and decodes that
pair without narrowing either the record or the supplied read time. Both SQL
page shapes select the same evidence, including malformed nullable pairs for
explicit refusal rather than silent omission. Database constraints enforce
the pair, complete timestamp domain and uncertainty-marker consistency.

Lease settlement used the latest possible legacy expiry to avoid early reuse
of funds. Credential authority needs the opposite bound: migration 0018 keeps
the earliest instant consistent with the old truncated value, intersected
with the timestamp domain. This shortens legacy authority by at most 999 ns,
or 1,998 ns for the zero bucket, and never extends it. Finite legacy records
carry `not_after_is_lower_bound`; indefinite records remain exact nulls and
new insertions explicitly declare exact evidence. Existing identities,
digests, retirement and billing history are retained. The existing revision
trigger advances once with the backfill, and an exhausted revision or invalid
historical timestamp aborts the entire schema/data transaction.

The column rename fences old expiry queries on retained connections, and old
startup rejects the unknown migration. Neither fence can withdraw proof that
another process has already issued. The rollout witness deliberately warms a
session against the old zero expiry, installs the corrected -999 ns source
bound, and shows that refresh alone retains the old cached proof. Clearing
the session makes it inherit the corrected bound and refuse. The coordinated
upgrade therefore stops issuers/readers and ingress, drains work, clears old
projections and every session proof, migrates, then starts fresh consumers
before resuming ingress. No new request-time generation check is introduced.
`docs/CREDENTIAL_PROJECTION.md` records the maintenance, uncertainty visibility,
rotation and recovery contract. There is no lossy automatic schema downgrade.

The sibling audit covered all remaining timestamp conversions in the backend,
the registry, key manager, session cache, wire DTOs and query-plan fixture.
Expiry is exact throughout; revocation uses presence rather than its timestamp
for retirement, so its microsecond value is informational. Usage/activity have
an explicit shared microsecond reporting contract, and period boundaries are
integral instants. The query-plan fixture was updated to the current schema;
its historical timings are not presented as new measurements. The memory
backend already met the exact expiry contract and remains the reference.
No additional lossy credential-authority conversion remains in this scope.

The Lean projection model proves conservative legacy bounds and inherited
authority after session reset, while GL-117's integer-pair proof supplies exact
ordering. These do not prove SQL isolation, crypto, finite Jiff arithmetic or
the operator's fleet-wide reset. Shared backend scenarios, migration failures,
old connection/startup refusals, restart reads, malformed evidence and real
HTTP-to-HMAC-to-session tests establish separate implementation evidence.
The PostgreSQL CI job runs the new HTTP scenario with a required database;
its mutation profile serializes it with the backend suite. The production
change stays in storage: no public API or wire shape, request-path code,
threshold or baseline change, and no new dependency.

## Snapshot refusals and retained task health (GL-77)

The original snapshot manager (`23b38f7`) handed a bare watch sender to its
future. Closing that sender left its last boolean untouched; the ready API
required every embedder to combine the value with channel liveness. Tests
covered an embedder doing so, and normal distribution tests discarded their
receiver at shutdown. They never read the retained value after the task died.
The new public-API regression test fails on that exact observation.

The sibling audit found that the lease manager's trailing `signal(false)`
covered normal return only. A panic or cancelled future skipped it as well.
Both managers now construct a single `TaskHealth` owner before spawning and
move it into the future. Its destructor publishes false before closing the
watch; callers borrow the sender, and no second publisher survives task exit.
Tests cover normal shutdown, owner drop, a discarded shutdown future, cancelled
in-flight lease shutdown, panic and abort before the task's first poll.
Cancellation is cooperative: the guarantee begins when the executor destroys
the future, not when another task requests abort. Whole-process abort leaves
no in-process health observer. No accounting or generation transition changes.

Generation gates introduced in `b285fdd` discarded rejected pushes silently.
`2027616` centralized the acceptance rule and added warnings to fetched
refusals, but left the push arms silent. The same false acceptance result also
represented an unchanged visible positive, so healthy periodic refreshes warned
as though a source were stale. Tests pinned admission and retry timing, not the
operator evidence or its absence on a healthy refresh.

The manager's decision methods now obtain the shared generation verdict and
its refusal evidence together for both ingress paths. A genuine refusal emits
one warning with origin, principal, offered kind/generation and the retained
watermark, and increments one aggregate `refused_updates` counter. A visible
positive at the same generation remains a quiet no-op. The acceptance rule,
publication, resolution deadlines and retry/backoff remain unchanged. Event
capture tests run identical adversarial sequences through pushes and pulls,
including stale positives, stale revocations and a positive at its tombstone's
generation. They also verify that accepted transitions and duplicate positives
stay quiet, with a subsequent accepted push proving the duplicate was processed.
The existing absence-recovery and refusal-backoff witnesses still apply.

The release-refusal defect from the first part of GL-77 was already corrected
with the shared classification in GL-95. The event witness now covers both
fenced and invalid releases: invalid counts produce error-level evidence and
an abandoned release rather than clean settlement. No lease classification is
changed here.

All boolean health publishers in the client were checked. Snapshot and lease
were the two affected managers. The runtime already owns a terminal liveness
flag; credential publication owns withdrawal in Drop; usage and rollover
expose typed health that includes task closure. The metrics audit found that
the pricing example omitted the existing `refresh_timeouts` field, so it now
exports that alongside the new refusal count. The operator guide explains
Fixed/All resolution semantics separately from task liveness; zero unresolved
is not proof that a task still runs.

`SnapshotStats` and the example metrics struct gain public fields. Exhaustive
Rust struct construction/destructuring needs an update; JSON fields are
additive. There is no database migration, snapshot DTO or configuration change,
no new dependency and no request-path work. The existing generation/account
Lean models remain unchanged; task ownership and Tokio watch destruction are
separate Rust implementation witnesses, not a claimed Lean refinement.

## 2026-09-13 — Executable invariant citation checks (GL-75)

The original reservation contract cited `cancel_charges_zero`, while the test
was named `cancel_charges_zero_and_refunds`. The background deadline change
`bb0c5bd` added `zero_drain_deadline_is_rejected` to the contract without such a
function; `invalid_writer_config_is_rejected` already covered that case. Both
behaviors had tests, so executing the suite could not detect their incorrect
citations. The document-level promise had no resolver enforcing it.

The current-tree audit found seven more absent names. Six arrived with staged
admission's invariant edits in `5a22126`: the two shared-counter witnesses, the
stage-outcome witness, the owned-context and expiry witnesses, and the
per-principal limit-reinstall witness. Those names never existed in the source
history. `42f4aca` subsequently established one shared account authority and its
actual `limit_change_is_one_account_authority_for_every_principal` witness, but
invariant 26 still described independently retained rate buckets. The Moka
allocation gate changed to its dependency's amortized housekeeping contract in
`489715a`, leaving the old zero-allocation test name in invariant 24. One
allocation citation also used a nonexistent `tests::` module qualifier.

The citation repair includes those same-pattern siblings. Existing phase
counters cover the stage-outcome claim; four focused admission tests now
witness counters shared through both map implementations and owned contexts,
principal policy surviving republication and engine destruction, and exclusive
pinned expiry after publication, and current account-rate authority after
stage one. The context test establishes `Send + Sync +
'static` and transfers the context to a real worker thread. Invariant 26 now
distinguishes pinned principal evidence from the account authority loaded once
at admission, and cites the shared-limit test. Invariant 24 names and describes
the already-enforced Moka average allocation budget. None of these changes
modifies admission production code, a workload, threshold or baseline.

`tollgate-repo-check` now checks every candidate inline reference in
`INVARIANTS.md`, not only names after a *Tests* label. It parses Rust syntax and
property-test token streams, indexes Lean declarations with their scopes, and
checks named proof files. Five explicit non-declaration classifications cover
standard-library/lint, SQL and static diagnostic names; unused classifications
fail. Module and field references resolve as their own declarations. Opaque
macros, examples, comments and strings cannot become Rust declarations through
a text match. Synthetic repository tests exercise acceptance and rejection,
including stale names, bad qualification, missing evidence and CLI errors.

The original tree produces a nonzero reference verdict independently of test
success. The corrected tree passes the same checker. Repository hygiene runs
it on every merge request, so renaming a cited declaration without updating the
contract now fails CI. This proves reference integrity, not test coverage,
semantic enforcement, or Rust-to-Lean refinement. Historical removed names in
this design log remain intentionally outside that current-state check. The
admission and checker modules were audited for the same citation pattern; all
unresolved candidate references in the invariant document are addressed here.

## 2026-09-13 — CLI control requests are process boundaries (GL-72)

The example entry point introduced in `0799fd9` never read command-line
arguments. Asking for help therefore entered the same startup path as launching
the service: configuration validation, runtime construction and a listener.
Library/router tests could not observe that process-level defect. The server's
information flags were repaired by the security entry-point work in `6721c0c`,
and the benchmark checker acquired argument modes and information flags during
its calibration changes. Fixing those instances left the example entry point
outside the contract.

The pricing executable now decides `Startup` synchronously, before constructing
Tokio or reading application configuration. Help and version print and exit;
only ordinary no-argument startup or a bare `--` reaches the existing service
body. Unsupported arguments return status 2. Existing valid startup and
environment-variable meanings are preserved. Deployments that supplied ignored
arguments must remove them. No library API, HTTP response, database or
request-path behavior changes.

The full Cargo target audit covered all five binaries. It found a second
instance of the same boundary error in the server and both performance tools:
`std::env::args()` decoded every argument before the parsers could examine
information flags. A native non-UTF-8 argument therefore panicked even when
followed by `--help`. Entry points now collect native arguments; controls are
selected before UTF-8 conversion. The gate tools still require UTF-8 operational
arguments, but report a recoverable error instead of panicking. The service
binaries reject all unsupported arguments without echoing their contents.
The server's usage diagnostic now goes directly to stderr before log setup,
so `RUST_LOG=off` cannot conceal that refusal. The repository checker's combined
help/version handling now uses the same first-flag rule as the other binaries.

The server and load-tool information paths also moved ahead of Tokio runtime
construction. Their annotated asynchronous entry points otherwise read runtime
configuration before reaching the argument parser; an invalid
`TOKIO_WORKER_THREADS` could still make help fail. The process tests now poison
that configuration and require information commands to succeed. Only a parsed
run command constructs the runtime. The serving and measurement bodies retain
their existing behavior and error propagation.

The small control scans remain local to their executable parsers. Their
operational arguments differ, and no request-path crate gains a CLI dependency
or public API just to share this short scan. Process tests enforce the common
contract: malformed configuration, occupied listener addresses, invalid and
native arguments, flag order, argument termination, exact version output and
no output-file creation. Gate cases stop before a workload can run. The initial
pricing help reproduction failed on unchanged production by reaching invalid
sharding configuration; the final witness also poisons runtime configuration.
Both boundaries now sit after the entry point's decision. Invariant 38
names the witnesses. Existing formal/accounting models are unchanged; this is
CLI implementation evidence, not a new formal refinement. No performance
measurement, threshold or baseline change is involved.

## 2026-09-13 — Load failures are values under abort (GL-73)

The original load client (`0799fd9`) used `expect` and `assert` for socket and
HTTP failures while the inherited deployment profile already selected
`panic=abort`. The concurrent driver (`9fec1f9`) caught `JoinError`, and its
startup-failure test accepted any error. A refused connection therefore passed
that test by unwinding a worker, although the deployed tool would terminate the
process. The strengthened witness fails on that revision because it requires
the returned connection error rather than a task-panic wrapper.

The current-tree audit found two further consequences of the same failure
boundary. Every scenario error returned from the CLI without writing a report;
merely replacing `expect` could not satisfy the operator contract. The GL-99
reader (`a4fcedb`) also treated every HTTP 503 as capacity shedding, including
quota expiry and accounting backpressure. The reader now requires the explicit
capacity problem code. Unexpected statuses, malformed or duplicate lengths,
unsupported transfer framing, truncated replies and buffer overflow are errors;
response text is never interpolated into their diagnostics. Success bodies are
still consumed without JSON decoding; only refusal bodies require their code.

Private client and report modules own the behavior and are reused directly by
an abort-profile assurance fixture. Each client returns errors carrying its
connection index and warmup/measurement phase. The driver joins every client and
reports all failures rather than allowing partial samples to become a result.
Its sole coordinator owns a Drop release, so cancellation also unblocks clients
waiting at the measurement rendezvous. Poisoned state fails closed and wakes
waiters; it is not cleared or promoted into a run decision. The abort fixture
exercises ordinary I/O/protocol errors; unit tests separately cover poisoning
under an unwinding test profile. No claim is made to recover arbitrary panics
under abort or to formally refine Tokio scheduling.

The sibling audit covered the entire load binary: client setup and exchange,
readiness, rendezvous ownership, every scenario/configuration early return,
server shutdown and report serialization/publication. The implicit Tokio entry
point also panicked on invalid worker configuration or a failed runtime build;
the CLI now validates the same positive worker-count setting and builds the
runtime fallibly after command selection. Readiness used blocking
I/O inside its async polling loop and absorbed partial reads; it now retries
complete unsuccessful replies within one five-second async deadline. Client
connect/read/write operations have ten-second inactivity bounds. If execution
and shutdown both fail, both diagnostics survive. The remaining `expect`s in
the load binary construct statically nonzero constants; fixture assertions are
intentional test failures, not operational recovery paths.

The response buffer is allocated once per connection before warmup and is
64 KiB. This accommodates the example's maximum 1,024 JSON float prices, their
separators and metadata, plus the separate 16 KiB header ceiling. Header search
resumes at the previous fragment boundary instead of rescanning the entire
prefix. No request-path crate, workload, threshold, baseline or profile changes.
Tokio's existing `io-util` feature supplies cancellable readiness I/O; no new
package or version is introduced. These driver changes have no measured latency
claim in this change: no Criterion, load calibration or SQL-plan timing ran.

Configuration and execution failures now publish a distinct failed report
containing stage, message and run context. They remain exit 1 in evidence mode.
Successful measurement report fields are unchanged; report consumers must check
for the new error shape before looking for timings. The common publisher creates
an exclusive temporary file alongside the destination, syncs and reads it back,
validates JSON, then renames atomically. Failure preserves the previous file and
reports unavailability explicitly; concurrent writers have last-successful-rename
semantics, not a merged history. This is observational evidence, not billing
storage. There are no public Rust, wire, database or service configuration changes.

CI runs the production-profile probe against fixed HTTP failure peers, reusing
the exact client and report modules. It starts no pricing service and emits no
performance measurements. Ordinary tests cover report replacement, invalid CLI
configuration in both modes, framing boundaries, retained error context,
cancellation and readiness deadlines. The probe's readable failure reports are
retained as CI artifacts. Mutation assurance remains in CI; the existing pricing
workload tests and all timed acceptance remain separate from these local failure
fixtures. Invariant 39 and the performance operator guide state this contract.

## 2026-09-13 — Gate configuration keys are closed schemas (GL-81)

The load-ratio gate (`17c1353`) constructed missing-field tests by deleting
literal indented JSON lines. The test silently depended on whitespace, key
position and commas rather than the parser contract. GL-99 (`a4fcedb`) copied the
pattern for two more nullable ceilings. The audit found six affected field
checks in the one load-manifest test and no other source-text replacements in
the two gate modules. Compactly serializing the same JSON data reproduces the
failure without changing a threshold: the string deletion does nothing, and the
unchanged valid object fails the test's expectation that it should be invalid.

The tests now remove keys from parsed JSON objects and require the named
missing-field error. They cover every operational load setting in both checked-in
manifests after compact and pretty serialization, which also changes the
original key order. Complete inputs must still validate; nullable bounds remain
explicit nulls in the portable manifest. Workload and threshold data is unchanged.
The nearby comment incorrectly claimed assured work was never shed despite the
existing comparative-advantage contract; it now describes that existing contract.

TrustPolicy (`66a777d`) deliberately allowed omitted fields to use defaults, but
serde also ignored unrecognized fields. The defaults test proved omission was
supported and never distinguished it from a typo. An operator's tighter setting
could therefore disappear while the old default remained active. The same audit
found that a misspelled top-level trust block or ratios list was ignored too.
Rejecting only unknown keys inside TrustPolicy would leave those bypasses intact.
Baseline sample-count and per-row regression defaults had the same shape; the
regression allowance arrived with the baseline in `fbe2613`.

Every gate configuration object now declares its recognized keys. Explicit
zero-sized IgnoredAny fields accept existing manifest/row comments and reserved-ID
metadata without retaining or allocating it. Baseline root comments retain their
existing string representation. Unknown keys elsewhere fail decoding, including
when a correctly spelled setting is also present. There is no arbitrary underscore
extension namespace. Known numeric settings, absent or partial trust blocks, legacy
one-sample baselines and the default 5% allowance keep their existing meanings.
The implementation uses the existing serde facilities and adds no dependency.

The scope is operator configuration. Criterion estimates are an intentional
projection of an external producer's richer statistics, while history and sample
records are generated measurement evidence; they do not carry optional operator
policy settings and are not newly made closed schemas. No unhandled defaulted
configuration object remains in either gate. The checked-in files all parse
unchanged, including their documented annotations. Custom files with previously
ignored annotations must remove them or move them into supported comments.

Unit tests pin unknown-key refusal, explicit metadata, configured values and
legacy defaults. CLI fixtures require manifest errors before measurement reads
or output changes, require invalid baselines to fail comparison and survive a
recording attempt, and require load parse failures in both verdict modes. All
Criterion values in those process tests are synthetic files. The exact load
fixture also has an independently invalid connection count, so even the old
permissive parser cannot start a workload during a regression test. Invariant 16
names the witnesses; this is configuration enforcement, not a new numerical or
Rust-to-Lean refinement. No request-path code, workload, profile, threshold,
baseline, dependency or performance measurement changes are involved. Mutation
assurance remains a CI responsibility.

## 2026-09-13 — Published ledger equations follow the accounting contract (GL-80)

Elastic enforcement (`5a8db89`, GL-1) added overage funding to the implementation,
invariants and design equations, but left the README, memory-backend overview
and operational SQL on the deposit-only equation. Periodic budgets (`0b245bf`,
GL-97) added the expiry sink to the implementation, invariant and Lean model, but
left the earlier summaries unchanged. Backend tests and exact-model proofs
checked their own ledger operations; they did not execute the published SQL or
validate prose. Their success therefore did not establish that an operator's
copied equation was correct.

The audit found seven stale representations: the README, `AGENTS.md`,
the memory-backend overview, two design equations, the reconciliation
SQL and the formal-model README. The invariant, `Conservation::holds`, Lean
model and periodic-budget migration already agree. The earlier elastic
migration describes its schema at that migration; it remains unchanged.

Current summaries now include overage funding and expired allowances and link
to the owning contract or model. The memory overview links directly to
`Conservation::holds` instead of carrying another equation; reconciliation links
to the design's existing equation instead of duplicating it. The SQL projects
both missing columns and includes them in its drift and refusal expressions.
Its funding addition uses `numeric`, matching PostgreSQL's widened lease sums,
so a valid sum above `BIGINT`'s maximum does not abort an operational sweep.

Validation executes the actual SQL example on PostgreSQL 16 using temporary
tables with the projected schema's column types and an independent integer
oracle. Ten ledgers cover idle and active strict accounts, overage, expiry,
their combination with settlement loss, both signs of corruption, omitted
funding and expiry records, and funding above `BIGINT`'s maximum. Active usage
is subtracted and settled leases are excluded. The old query falsely reports
four healthy fixtures; the corrected query returns no rows for all six healthy
ledgers or an empty database and reports the exact drift for all four corrupt
ones. The fixture driver and output are retained with the review evidence.

This is a documentation and operational-example correction. Ledger behavior,
schema, invariants and proof artifacts are unchanged. Reducing duplicate
equations and validating the published query addresses the review gap; it does
not mechanically prove prose stays current. Future ledger reviews must compare
all published views with the owning contract, including nonzero overage and
expiry, rather than infer documentation correctness from backend tests.

## 2026-09-13 — Lease publication returns ownership; test helpers require opt-in (GL-84)

`LeaseSlot` exposed two pairs of mutations with the same publication mechanism:
`install`/`replace` and `clear`/`take`. The first spelling in each pair dropped
the old handle internally. The return-value tests covered the second spelling,
so they could pass while an embedder's routine rotation stranded unspent quota
until reclamation. The existing must-use lint could not diagnose a call returning
unit. GL-109 (`bf96da1`) later used `install` in four production consolidation
paths, extending the problem beyond the original test/bootstrap callers.

The API now exposes only `replace` and `take`, with must-use diagnostics and
compile-fail witnesses for ignored results and the removed convenience methods.
The slot still publishes through the same swap/serialized-shard implementation.
The caller owns quiesced release, and deliberate abandonment remains possible
through an explicit drop; the API does not claim to force an external allocator
call. The manager centralizes all five publication paths in `publish_and_park`,
which retains any displaced grant. A confirmed consolidation settlement alone
allows disposing of its predecessor without release.

The regression witness injects another grant while the allocator call is pending.
Before the fix, successful consolidation silently discarded it; rollback and
integrity-refusal restoration had the same defect. All three now retain it for
release. Slot tests cover ownership in single and sharded layouts and verify
that a replace/take race leaves each grant returned or published exactly once.
The existing locality-view witness retains the requirement to wait for readers.

The PostgreSQL audit found two test operations on the unrestricted store handle:
`truncate_all`, whose reset comment had become attached to the private push
method, and `explain_active_lease_sum`, which refreshes statistics. Both move
unchanged into the non-default `test_support` module. Compile-fail examples prove
the ordinary handle has neither method, including when the feature is enabled.
Integration tests opt in with Cargo dev-only self dependencies. The server's weak
feature forwarding does not activate its optional backend; normal dependency
trees stay free of test support, and a server without PostgreSQL stays free of
SQLx. These add two local dev graph edges, no package or version dependency.

The scope search found no other silent lease mutator or misplaced destructive
helper in these modules. Store balance, usage and conservation reads are useful
operational APIs and remain public. Rustdoc validation also exposed an admission
counter link to the removed `AdmissionEngine::admit`; it and the matching state
comment now name `RequestContext::admit`. Synthetic fixture and benchmark callers
explicitly dispose of replacement handles; their workloads do not change.
The public Rust API break and external test-harness migration are documented in
`docs/LEASE_OWNERSHIP.md`. There is no schema, wire, threshold or baseline change,
and no performance measurement is claimed. Mutation testing remains in CI.

## 2026-09-13 — Rate refusals expose a retry class, without a deadline (GL-69)

The sharding change (`c9330f5`) introduced a contradiction in the same commit:
the invariant and design overview promised the soonest retry among shards,
while the implementation retained the first refillable governor denial in a
locality-first scan. Admission then discarded that denial's timing and returned
the payload-free `RateLimited` variant. The promise never described an
observable API. Existing tests exercised admission, throttling, shard capacity,
and retry classification; none could witness a timestamp the API did not carry.

The current contract now names what consumers receive: `RateLimited` and
`RequestRateLimited` both classify as `Retry::Transient`, with no retry instant,
delay, or earliest-admission guarantee. The denial variants and classifier own
that boundary. The existing `every_reason_has_the_expected_retry_class`,
`rate_limiter_weights_by_cost`, and `request_rate_limiter_counts_requests_not_cost`
witnesses are cited explicitly. The bucket comment now also accounts for a
preferred shard that cannot hold the request: the first refillable denial can
come from a sibling, so it is not necessarily the preferred shard's denial.
The internal choice remains an implementation detail, with no timing-selection
contract added and no source-text assertion test.

The sibling search found both false deadline promises, the imprecise bucket
comment, and generation-order wording in the same shard-ceiling descriptions.
Those descriptions now require an accepted principal publication before its
quote can constrain the shared account split. This preserves the distinction
between an older account-policy generation on an accepted principal and a
rejected principal replay. GL-91 / !115 (`42f4aca`) already moved map generation
acceptance before policy resolution and made admission use the installed burst
authority; this change does not reimplement either fix. Their existing map and
account-authority witnesses remain part of invariant 5.

No other earliest-retry promise was found in the workspace's rate-limit API or
documentation. The later retry-class design text already states that no
retry-after instant is supplied. The repair updates the invariant, both design
descriptions and API/implementation comments together; it does not change Rust
behavior, public signatures, wire representation, schema, or hot-path work.
Proof artifacts and performance manifests are unchanged. Tests support the
documented observable contract; they do not mechanically prove prose accuracy
or a future timing policy. Mutation testing remains in CI.

Validation exposed two existing gaps in the core crate's checks. The GL-94 change
(`20a6081`) added `a_snapshot_without_a_revision_key_decodes_as_unstated` without
the `serde` guard its neighboring wire tests have. Workspace tests, even with
default features disabled, unified serde through dependent crates and concealed
the standalone test-build failure. The test now requires that feature, and the
existing no-default-features CI job first tests core on its own; the all-feature
workspace job still executes the wire witness. The search of core's serialization
tests found no other missing feature guard.

Public Rustdoc also failed on four links to private helpers in `cost_table` and
`lease` and three redundant links in `reservation`. Those references are corrected
without changing code. The Clippy job now also builds public core/admission docs
with warnings denied, so these documentation failures cannot pass its check.
These are test-selection and validation changes; the runtime feature defaults,
serialization behavior, and dependency graph remain unchanged.

## 2026-09-14 — The map choice was measured with moka's bookkeeping switched off (GL-68)

Two claims in this repository were wrong in the same place, and the second is
why the first went unnoticed for so long.

`tollgate-admission`'s crate documentation said "Nothing in this crate performs
I/O, takes a lock on the request path, or reads a clock". `MokaSnapshotMap`
does. `moka::sync::Cache::get` records a read op; when the read log reaches
`READ_LOG_FLUSH_POINT` (64) or a 300 ms monotonic deadline passes, the
housekeeper drains it behind a `try_lock` on a `parking_lot::Mutex`, updating
the frequency sketch and evicting at capacity. The `try_lock` never blocks, so
the *shape* of the hot-path budget survives — AGENTS.md had already been
corrected to "no **blocking** locks", naming moka and governor as measured
mechanism costs. The crate docs, `README.md`, `counters.rs` and this document
had not, and two of them cited INVARIANTS GL-5 as forbidding locks outright when
GL-5 is a rule about I/O. All of them now say what is true.

The benchmark is the more interesting half. `admission/snapshot_lookup_moka`
built a cache with `max_capacity` 4,096 and installed 512 principals, then read
one hot key — and `maps.rs` cited the resulting gap as "the evidence for which
map a deployment should pick". It is worse than the "eviction never runs"
objection that opened GL-68: moka enables its frequency sketch only once
`weighted_size >= max_capacity / 2` (moka 0.12.16), and 512 is an eighth of
4,096, so `FrequencySketch::increment` returned immediately on an empty table.
The row cited as measuring *TinyLFU bookkeeping* was measuring a moka with
TinyLFU switched off.

`admission/snapshot_lookup_{arc_swap,moka}_at_capacity` hold exactly
`max_capacity` principals and rotate over a fixed permutation of them, both
maps walking the same order so the ratio is about the maps and nothing else.
Filling the cache costs moka more than it costs arc-swap, which has no capacity
to be at. Two diagnostic runs, neither calibration:

| Row | Under-filled (512 of 4,096) | At capacity (4,096 of 4,096) |
|---|---|---|
| **quiet Apple M1 Pro** | | |
| `admission/snapshot_lookup_arc_swap` | 13.81 ns | 21.20 ns |
| `admission/snapshot_lookup_moka` | 88.34 ns | 106.88 ns |
| ratio | ×6.40 | ×5.04 |
| **loaded Linux x86, load average ~80** | | |
| `admission/snapshot_lookup_arc_swap` | 30.1 ns | 31.9 ns |
| `admission/snapshot_lookup_moka` | 138.7 ns | 408.0 ns |
| ratio | ×4.6 | ×12.8 |

The *direction* is the finding and both runs agree on it: filling the cache
costs moka about 19 ns on the quiet host, against 7 ns for arc-swap over the
same working-set growth. The under-filled row did flatter moka.

**The ratio is not the finding, and an earlier revision of this section claimed
it was.** It said the gap widened roughly threefold at capacity, on the strength
of the Linux row alone. On the quiet host the gap *narrows* — ×6.40 to ×5.04 —
because a machine at load average 80 inflates the slower path disproportionately
and the ×12.8 is that inflation, not the map. Reporting a ratio at all from a
host that cannot calibrate was the error; the absolutes were labelled
diagnostic, the conclusion drawn from them was not.

Both rows remain diagnostic. Neither host's numbers set a bound, and the
controlled-host recording (GL-119) is what will.

The benchmark lands here; its *bounds* do not. `testing/perf_baseline.json`
names `mistral-apple-m1-pro`, and a manifest row without a baseline row fails
`checked_in_baseline_covers_every_benchmark_the_gate_runs` — deliberately, as
that test's own history records. So the two ids are reserved in the manifest
and the threshold, ratio and baseline rows land together with the controlled-
host recalibration that measures them. Both ratio directions belong in that
step, because `RatioBound` has no `min_ratio` and "arc-swap beats moka" is a
directional claim only an inverse row can express.

What this pair still does not measure is eviction paid inline on a request
thread. `get` never inserts, so nothing in a read-only timed loop pushes the
cache over capacity; what the new rows add over the old ones is the enabled
sketch, the full access-order deques, and a working set past L1. Measuring
inline eviction needs sustained write pressure during the sample, which makes
the row something other than a lookup and not comparable to this family. That
is tracked separately rather than implied here.

## 2026-09-14 — The sweep's ORDER BY defeated its own index (GL-65)

`reclaim_expired_batch` selected expired leases with a predicate that matches
`tollgate_leases_expiry` exactly — and then discarded that match by ordering the
result `(account_id, lease_id)`, which no index answers. The `LIMIT` could
therefore never stop an index walk. Against 1,200 expired leases the planner
chose:

```
Limit  (cost=96.00..99.20 rows=256)
  ->  LockRows  (cost=96.00..111.00 rows=1200)
        ->  Sort  (cost=96.00..99.00 rows=1200)
              Sort Key: account_id, lease_id
              ->  Seq Scan on tollgate_leases  (cost=0.00..42.00 rows=1200)
```

Every expired row read and sorted to return one page of 256. A drain is
`ceil(N/256)` such pages, so the read work is quadratic in the backlog it exists
to clear: a 100,000-lease outage backlog costs ~2×10⁷ row reads rather than
~10⁵. INVARIANTS GL-9's bound held for locks and for writes, never for reads —
and GL-6's purpose, that an outage must not become a second incident, is exactly
what degraded.

Ordering by the index's own columns is the whole fix:

```
Limit  (cost=0.28..24.71 rows=256)
  ->  LockRows  (cost=0.28..114.81 rows=1200)
        ->  Index Scan using tollgate_leases_expiry on tollgate_leases
              Index Cond: (ROW(expires_at_floor_us, expires_at_submicro_ns) <= ROW(...))
```

No `Sort`, and the `Limit`'s estimate is a fraction of the full scan's: the walk
stops. No migration, no new index — the index was always right, the `ORDER BY`
was not.

### The order was documented as load-bearing, and the documentation was wrong

Three comments and two passages here said reclaim's `(account_id, lease_id)`
order was what ingest matched to avoid deadlock. Changing it is nonetheless
safe, and it is worth writing down why, because the reason is not the one that
was recorded. A deadlock cycle needs two waiters. Reclaim takes its leases with
`SKIP LOCKED`, so it abandons a contended row rather than queueing behind it and
can never be a waiting party on a lease. What actually prevents cycles is the
lease-then-account *phase* order that every writing transaction shares, plus
ingest-vs-ingest agreeing on one lease order — which they still do. Reclaim's
selection order was free all along.

### Two further findings

`roll_due_periods` had the identical shape: `ORDER BY account_id` over a
predicate served by `tollgate_accounts_due_rollover (budget_period,
period_start_us)`. It is milder — one row per account, not per lease rotation —
and because the due set is normally small, the planner prefers a sequential scan
until the table is large, so the sort is not always the plan. But the ordering
was also wrong on its own terms: a bounded page served the lowest account ids
rather than the most overdue boundaries. `budget_period = $1` is an equality, so
ordering by `period_start_us` is index-native within that prefix and crosses the
oldest boundary first.

And the two backends had been settling *different leases*. `MemoryStore` walks
its expiry index oldest-first and stops at the first lease not yet due (GL-23);
PostgreSQL paged by account and lease id, which for random UUIDv4 lease ids is
unrelated to expiry — so an undrained batch could settle the newest-expiring
leases and starve the oldest. Nothing caught it:
`expired_backlog_is_reclaimed_in_bounded_batches` uses three leases that are all
due, all share one expiry, and are sorted before comparing, and its memory
mirror is byte-identical. The new mirrored witness gives every lease a distinct
expiry, leaves one not due, and makes the owning account ids run opposite to
expiry order, so a backend paging by account returns `[2, 3]` where the
reference returns `[4, 3]`.

Migration 0005's comment — "The sweep path was already fine —
`tollgate_leases_expiry` covers it" — is the assumption this corrects. It covers
the `WHERE`; the `ORDER BY` took it back.

That comment cannot be fixed in place. sqlx checksums each migration, so editing
one byte of an applied file stops every deployment that has run it from starting
— `migrate: migration 5 was previously applied but has been modified`, verified
against a database with 0005 applied. Failing closed there is deliberate, and
two tests enforce it. So a comment in an applied migration is frozen at whatever
was believed when it was written, and some of them are now wrong.

`crates/tollgate-store-postgres/migrations/README.md` is where that is said and
where such corrections go, 0005's first. GitLab renders it under the directory
listing, which is where someone who has just read the stale sentence ends up.

## 2026-09-14 — The lease handle is moved into the reservation, not re-cloned (GL-79)

`reserve_from_lease` loaded a lease and handed it to the reservation by
reference, which cloned it and stored the clone; the caller's handle then died
at the end of the function. Three atomic read-modify-writes where one is enough:

```rust
let lease = state.lease.load_at(locality)?;              // load_full → RMW
Reservation::reserve_at_locality(&lease, ...)            // Arc::clone → RMW
// the local drops                                       // drop      → RMW
```

The stored handle is genuinely required — `commit_after_lapse`, `usage_event`
and `refund` all read it after the phase compare-exchange — so only the extra
clone/drop pair was avoidable. `reserve_at_locality` now takes the `Arc` by
value and moves it into `ChargeSource::Lease`. The success path costs one RMW;
the deny path is unchanged at two, the by-value handle simply dropping inside
the callee rather than at the caller.

Where those operations land is the point. Under the shipped
`LocalSharding::SINGLE` default there is exactly one `ArcSwapOption<LocalLease>`
per account, so every thread serving that account contends the same refcount
word. This is the same reasoning `request_entry_from` and `request_entry_at`
record one layer down in the snapshot map, and the `reserve` /
`reserve_at_locality` pair already *was* that owned-versus-borrowed split — the
owned half just did not own.

The public `reserve(&Arc<LocalLease>, ..)` keeps its signature and pays the
clone at its own call site, which is refcount-neutral: the clone moved rather
than multiplied. That matters because its ~40 callers across benches and
property tests hold borrows they reuse, and two of them build many reservations
from a single borrow — a by-value public signature would have made this change
cost more than it saved.

### What the measurement does and does not say

Diagnostic only; this host is not the one `testing/perf_baseline.json` names, so
no absolute or recorded-baseline verdict is claimed and nothing is recalibrated.

Five interleaved before/after pairs of `admission/full_check`, built from
separate checkouts into separate target directories and alternated rather than
batched, measured 216.09 / 185.03 / 185.20 / 188.52 / 196.43 ns before against
175.54 / 176.99 / 180.16 / 183.49 / 188.08 ns after. Every pair is
non-overlapping with the later run faster; the first pair's `before` is a
warm-up outlier, and the settled difference is about 3.5%, near 6.5 ns.

`admission/full_check_contended_8` is **inconclusive on this host and is
reported as such.** Two interleaved series disagreed in direction — the first
showed the change faster in all three pairs, the second showed it slower in two
of three — under a load average near 40. Eight threads contending one account on
a machine that busy measures the scheduler, not the refcount word. Retrying
until one series agreed would have been choosing a result rather than measuring
one.

The recorded baseline of 126.26 ns for `admission/full_check` is therefore
conservative by whatever this saves, so a later regression of that size would go
unnoticed until someone re-records. GL-119 already exists to regenerate the whole
baseline on the controlled host and will absorb it.

## 2026-09-14 — A trait default is inherited by silence, so the decision moves to one file (GL-83)

Forty-four store test-doubles across twelve files each re-implemented the store
traits method by method. The visible cost was bulk — `readiness.rs` spent 149 of
234 lines on `unreachable!()` stubs, `FlakyReclaimStore` 219 lines to intercept
three methods. The cost that mattered was invisible: a trait that grows a
*defaulted* method breaks none of them, so a wrapper meant to be a faithful
proxy stops being one with no compile error.

`crates/tollgate-store/tests/support/delegating.rs` makes that decision once,
and is shared across four crates by `#[path]` the way
`tests/support/credential_activity.rs` already was.

### The issue's example was the one case where delegating is wrong

GL-83 names `FlakyReclaimStore` inheriting `LeaseAllocator::reclaim_expired` as
the bug. It is not. `MemoryStore` does not override `reclaim_expired` either,
and the default body is written over `self.reclaim_expired_batch` — which
`FlakyReclaimStore` *does* override. The inherited default composes with the
injected flakiness exactly as intended. A delegator that "faithfully" forwarded
that method to the inner store would rebind `self`, bypass the injected
failure, and silently empty two tests — and `crates/tollgate-server/src/lib.rs`'s
`/reclaim` route calls it, so the bypass would have been live.

The real instance was in the same struct, one method over. Its `SnapshotSource`
impl forwarded `snapshot` and `subscribe` and omitted `principals`, inheriting
the `Ok(None)` sentinel while the wrapped `MemoryStore` had a real catalogue.
A server built over it answered 501 `enumeration-unsupported` on `/principals`
where the same server over the bare store answered 200. No sweep test calls that
route, so nothing failed.

So the rule is not "forward everything", and it is not "inherit everything":

> A default body defined in terms of other `Self` methods must be **inherited**,
> or the wrapper bypasses its own overrides. A default body that is a constant
> or a sentinel must be **forwarded**, or the wrapper lies about the inner
> store's capability. Decide by reading the body, not the name.

Inheriting without copying the body needed one production change:
`reclaim_expired`'s drain loop is now the free function
`drain_reclaim_expired`, `#[doc(hidden)] pub` after the
`Reservation::reserve_at_locality` precedent, because Rust has no `super` for a
trait default. The trait default calls it, so there is exactly one copy.

The same reading applied to `tollgate_admission::SnapshotMap`, whose 15
defaulted methods `ReservationSizes` inherits 7 of: every one re-dispatches
through `Self` methods that double does write out, so inheriting is correct
there too. It stays hand-written; GL-120 records the boilerplate.

### What is actually enforced

`#![deny(clippy::missing_trait_methods)]` scopes to the shared module, so a
newly defaulted method on any store trait fails `-D warnings` there and nowhere
else, naming the method. Verified by adding a probe method to `StoreHealth` and
confirming both impls refused to compile.

This is rung 3 of the ladder, not rung 1, and the difference is worth stating.
A defaulted trait method is inheritable by construction; no attribute, type, or
coherence rule makes "an impl inherited a default" a hard error, and no wrapper
can be forced to consider a method that does not exist yet. The lint is also
clippy, not rustc, so `cargo test` still passes on a stale delegator — only the
`clippy` CI job bites. What changed is that there is one place to fail instead
of forty-four to overlook.

The rung-1 answer exists and is deliberately not taken here: delete the
defaults. `reclaim_expired` becomes an extension trait with a blanket impl that
cannot be overridden; `principals` becomes its own trait, so "cannot enumerate"
is *not implementing it* rather than a sentinel a wrapper can inherit. Both
touch memory, PostgreSQL, HTTP, the snapshot manager and the server wiring, and
the first permanently forecloses a server-side drain endpoint. That is its own
change, not this one.

### What the conversion cost

Not what the issue implies. Line counts moved from 11,690 to about 11,400
across the affected files, against 965 lines of shared module — and three files
got *longer*, because a double implementing a one-method trait with no inner
store gains nothing from delegation but the shared lint. The doubles that
carried real boilerplate were the six that wrapped a store; `readiness.rs`
alone accounts for 147 of the reduction. The argument for converting the rest
is uniformity and the guard, and it should not be presented as a line count.

One property worth recording: `DelegatingStore<S>` is generic in `S` with each
trait impl separately bounded, so it implements exactly the traits its inner
store implements. That is what let `HeldGrantAllocator` wrap `HttpStore`, which
implements four of the seven and has no `AdminStore` or `StoreHealth` anywhere —
a case an `Arc<dyn Backend>` field would have rejected outright.

## 2026-09-14 — The delegating-double mechanism does not transfer to `SnapshotMap` (GL-120)

GL-83 replaced the workspace's store test-doubles with one delegating double and
armed it with `#![deny(clippy::missing_trait_methods)]`, so a newly defaulted
trait method fails the build in the one file where the forward-or-inherit
decision is made. GL-120 proposed the same for `tollgate_admission::SnapshotMap`,
whose two hand-written delegators carry the same boilerplate.

Measured, the mechanism does not transfer, and the reason is worth recording so
the next trait is assessed rather than assumed.

The lint forces the delegator to write out **every** method. That is affordable
only when the defaults a wrapper must *inherit* can be inherited by calling
something rather than by copying it. `LeaseAllocator` had exactly one such
default — `reclaim_expired`, 25 lines — so extracting `drain_reclaim_expired`
as a free function left one copy of the body and satisfied the lint.

`SnapshotMap` has **ten**, totalling **94 lines**: `contains_cached`, `get_at`,
`install_publishable`, `remove_many`, `install_many`, `apply_many`,
`apply_many_at`, `apply_publishable_many`, `apply_publishable_many_at`,
`apply_refreshed_many_at`. A lint-guarded delegator would hold a second copy of
all of them — reintroducing exactly the divergence the change exists to remove.
Extracting ten `#[doc(hidden)] pub` free functions would avoid the copies, but
that is ten additions to a production crate's surface for a test-support
benefit.

The line count agrees. The two delegators are 68 and 119 lines; a 21-method
delegator is roughly 250, or 344 lint-guarded. There are two call sites.

And there is nothing to fix: `ReservationSizes` (14 of 21) and `CountingMap`
(20 of 21) each omit only Self-dispatching defaults, which compose correctly
through their own overrides, and `NullMap` wraps nothing so has nothing to
diverge from. The delegation the request path actually runs through —
`impl<T: SnapshotMap + ?Sized> SnapshotMap for Arc<T>` — is already witnessed by
`the_arc_delegation_forwards_every_bulk_write`, which asserts a sharded map's
`local_sharding` survives dynamic dispatch.

So the rule moves to where the contract lives instead: `SnapshotMap`'s own
documentation classifies all fifteen defaults into the two lists, with the
reason each belongs there. That is the owning component stating its contract
(rung 2) rather than a convention hardened by a gate (rung 3), which is the
better rung — it is just not the mechanism GL-83 used, and GL-120 assumed it would
be.

**The general lesson:** "same boilerplate, different trait" does not imply the
same remedy. What decides it is how many of the trait's defaults are written
over `Self`, and whether their bodies can be shared rather than copied.

## 2026-09-14 — A determinism lint, and the two outputs that depended on a hash seed (GL-100)

`G14.determinism` reported `not checked`: the sweep found no denied-API lint
and no replay gate, and `not checked` blocks a level exactly as a failure does.
It is the same shape as the release job that reported `already exists; nothing
to do` while `main` sat six commits past `v0.5.0` — a state that reads as fine
and asserts nothing.

`clippy.toml` now denies thirteen methods, each with a reason, enforced by the
`clippy` job that already exists.

### What the list says, and what it deliberately does not

The denial that matters here is the **business** clock, because that is what
this architecture forbids: the request path "reads no wall/business clock for
policy decisions", and callers pass `jiff` timestamps explicitly. So
`Timestamp::now`, `Zoned::now` and `SystemTime::now` are denied, and the single
production read is `tollgate_store::Clock`.

`Instant::now` is **not** denied, and that is the interesting half. AGENTS.md
already makes monotonic reads an explicit, measured allowance — "Moka
maintenance and governor's bucket arithmetic read their own monotonic clocks;
those are measured mechanism costs, never sources of snapshot or lease truth."
The control plane's timeouts, deadlines and backoff are that same mechanism.
Denying it would have meant roughly thirty allows restating the design, and
would have blurred the distinction the rules rest on: **elapsed time is a
mechanism, business time is truth.**

`getrandom::fill` and `Uuid::new_v4` are not denied either. They are randomness
the codebase must reach for — unguessable lease identifiers, HMAC secrets. A
determinism gate is aimed at a result depending on the environment *by
accident*, not at unpredictability on purpose.

A constraint worth recording: `disallowed_methods` warns on a path it cannot
resolve, and `-D warnings` makes that fatal. The list can therefore only name
APIs reachable in this dependency graph — the seeded PRNGs that would otherwise
belong (`rand::random`, `rand::thread_rng`) cannot be denied pre-emptively,
because `rand` is not a dependency.

### The lint found two real defects, both in the reference backend

Both are `MemoryStore` outputs that depended on `HashMap` iteration order, and
both diverged from the backend `MemoryStore` is supposed to be the reference
*for*.

`principals` returned `snapshots.keys()` directly to its caller while
`PostgresStore` answered `ORDER BY principal`. The store suite's assertion
sorted before comparing, which is precisely how it stayed invisible — the same
mechanism as GL-85's mirrored-test drift, found by a different gate.

`roll_due_periods` is the one that matters. It iterated `accounts` and broke at
the batch limit, so with more accounts due than one batch takes, **two runs over
identical state rolled different accounts** — and a different set again from
`DUE_PERIODS_SQL`, which has always been `ORDER BY period_start_us LIMIT $3`.
The caller drains saturated batches, so every due account rolled eventually and
no ledger invariant broke; what was unbounded in principle was the wait for any
*particular* account. The existing bounded-rollover test asserts the set
eventually rolled, not the per-batch selection, so it stayed green throughout.

The new witness fails on all three runs against the old code. That check
matters: a test that merely passes after a fix is not evidence the fix was
needed.

### The shape of the remaining work

Fifty-one sites carry an `#[allow]` with a reason specific to the call. Most
are order-independent reductions — `count`, `any`, `max`, a checked sum, a pure
total `retain` — or collections sorted before the order escapes. Writing them
out is not ceremony: each annotation records that someone checked whether the
order reaches an output, which is the question the gate exists to force and the
one nobody was asking before.

## 2026-09-14 — Backend parity drifts inside mirrored tests, where a name diff cannot see it (GL-85)

The two backend suites are mirrored by name — 106 of `MemoryStore`'s scenarios
have PostgreSQL counterparts — and that mirroring is what AGENTS.md's rule
relies on. The drift is *inside* the mirrored tests.

`MemoryStore` carries inherent helpers that shadow its `AdminStore` methods:
`create_account`, `try_create_account`, `deposit`, `publish_snapshot`,
`remove_snapshot`. `PostgresStore` has none — it can only be driven through the
trait. So a mirrored test can call `store.publish_snapshot(..)` on one side and
`AdminStore::publish_snapshot(..)` on the other, and be two different contracts
under one name. Nothing says so: not the compiler, not a passing suite, and not
a test-name diff.

The suite's own comment already recorded the stake, from the first time this
drifted (GL-43, `set_account_status`):

> Every scenario drives `AdminStore` rather than an inherent helper: this pair
> of suites has already caught one divergence where the memory trait body could
> have been `Ok(())` with everything green.

### The audit found three; there were ten

GL-85 named `snapshot_publish_fetch_and_push`,
`enumerating_principals_includes_revoked_ones`, and
`creating_a_suspended_account_denies_from_birth`. Enumerating the divergence
mechanically found ten, including `deposit` — an operation the audit did not
mention at all — in `a_top_up_survives_rollover_but_the_allowance_does_not` and
`consolidating_across_a_boundary_regrants_only_what_the_credit_restores`.

That gap is the argument for the check rather than the fix. An audit reading two
four-thousand-line files finds the instances it happens to look at; the pattern
needs something mechanical.

### Two of the issue's claims did not survive re-checking

- **"Memory's `AdminStore::publish_snapshot` has no direct test at all."** True
  when written; false now. `a_publish_racing_a_suspension_never_leaves_the_records_disagreeing`
  was added for the lock-window defect and says in its own doc comment that it
  closes this gap. Roughly forty sites now drive the trait.
- **"INVARIANTS.md GL-11 lists `straggler_exceeding_recorded_loss_fails_ingest`
  without the `(Postgres suite)` qualifier it gives the neighbouring entries."**
  The neighbours are not individually qualified either — the parenthetical is a
  *group* qualifier after the last of seven names, and all seven are
  PostgreSQL-only. The real defect was the ambiguity of that grouping, so the
  fix is "all seven of …" and "(Postgres suite only; … so it has no counterpart
  to any of them)" rather than a per-name qualifier.

### Why the check is not a name diff

The issue suggests "a mechanical parity check that diffs test-name sets … with
an allowlist for justified exceptions". A name diff would have reported these
suites healthy: every one of the ten divergences sat inside a test both suites
already had, and the 19 genuinely backend-specific PostgreSQL tests would have
needed an allowlist that is pure maintenance cost.

`scripts/check_backend_parity.sh` reports the divergence that actually recurs:
for each mirrored test, the memory side driving an inherent helper where the
PostgreSQL side drives the trait. It parses both suites with `syn` and walks
expressions, so a call nested in a loop or closure is seen, and
`try_create_account` maps onto `AdminStore::create_account` — the name-mismatch
that let `creating_a_suspended_account_denies_from_birth` evade an
identifier-keyed check.

It was verified against `main` before the fix, where it reports all ten and
exits 1. A gate that cannot fail is not a gate.

**What it does not do, stated because the check's name overclaims otherwise:**
it does not prove the two mirrored bodies assert the same things. The
`Timestamp::MAX` half missing from the PostgreSQL TTL mirror — this change's
other fix — is exactly the kind of divergence it cannot see, and finding that
still takes reading. It reports one mechanical, historically recurring shape,
and its success line says so.

### One asymmetry left deliberately

`store_with_balance`, the memory fixture helper, became `async` so it could
drive `AdminStore::create_account` like its PostgreSQL counterpart. That cost
`.await` at 92 call sites. It is fixture rather than a mirrored scenario, so the
alternative was to exempt it — but exempting the one construct every test in the
suite runs through would have left the check's first report being a site it is
configured to ignore.

## 2026-09-15 — The baseline priced a profile nobody deploys, and rustc repartitioned it (GL-114)

Nine rows of the v0.18.0 candidate failed their recorded baseline with nothing
touching their paths, and `lease/reserve_commit_contended_8` returned 3,601.96,
3,468.27, 3,178.67 and 4,350.66 ns from identical source. The issue asked the
right question — distinguish a code regression from calibration history,
workload shape, compiler and code layout, and host conditions — and the answer
was the one nobody had instrumented.

`testing/perf_baseline.json` was recorded under Criterion's default release
profile, which is `codegen-units = 16`. rustc partitions a crate's functions
across those units and decides inlining per unit, so growing a crate anywhere
repartitions it and changes what gets inlined into functions that were not
edited. The recorded contract was therefore hostage to unrelated code motion in
the same crate.

`admission/snapshot_lookup_moka` is the clean demonstration, bisected to
`b1e0aae`, a 3,266-line commit titled `fix(docs)` that rewrote `maps.rs` and
added `history.rs`:

| Revision | `codegen-units = 16` | `codegen-units = 1` |
| --- | ---: | ---: |
| `1a3a260` (parent) | 66.92 ns | 55.84 ns |
| `f6aaa28` | 88.19 ns | 56.40 ns |

`get_at`, the moka cache builder, `StoredEntry`, `MapEntry` and
`PrincipalHasher` are byte-identical across that pair, the cache holds 512
entries in both, and moka's frequency sketch is disabled in both. Three
hypotheses were tested and refused before the profile was: boxing
`Mutex<GenerationHistory>` to undo the struct's growth changed nothing
(88.41 ns), draining moka's publication-time maintenance backlog before the
timed loop changed nothing (88.10 ns), and `entry_count()` confirmed 512 rather
than a sketch-enabling occupancy. At one codegen unit the two revisions agree
within 1%. There was no regression to fix.

One unit is also what ships: the `production` profile compiles `codegen-units =
1` with fat LTO, so the gate had been pricing this row 57% above the deployed
cost and would have accepted a real 50% regression as an improvement.

`[profile.bench] codegen-units = 1` is therefore part of the measurement
contract, and the profile string is part of a baseline's provenance so a
recording cannot silently compare across profiles. The cost is slower benchmark
builds.

### What this does not explain

`lease/reserve_commit` and `lease/reserve_cancel` moved for a real reason and
survive the profile change: 39.14 → 51.04 ns and 44.71 → 53.29 ns at one
codegen unit, bisected to GL-79's `6c9b82d`. That is the refcount operation moving
ahead of the reservation compare-exchange rather than following it, and the new
number is the faithful one — `LeaseSlot::load_at` returns an owned `Arc` from
`ArcSwapOption::load_full`, so the engine has always paid one increment before
the CAS, and the benchmark previously modelled one after it. The earlier
baseline measured a shape the request path never executed. Recalibrated here
rather than repaired, as the GL-79 entry above anticipated.

`admission/snapshot_lookup_arc_swap` is not a regression at all: it reads
13.96 ns in isolation at `f6aaa28`, against its 14.00 ns baseline. Its 16.34 ns
in the full run is run-ordering within the suite, which is why a row is judged
from a full run and not a filtered one.

## 2026-09-15 — Credential issuance shares one account-first transaction (GL-121)

The account-scoped `insert_key_within` introduced an account `FOR UPDATE` lock
before the credential write. The retained `insert_key` still inserted first,
then acquired the account's foreign-key `KEY SHARE` lock. For competing new keys,
the bounded transaction could own the account while the unbounded transaction
owned the uncommitted unique-index entry. Neither could finish; PostgreSQL
aborted one as a storage failure instead of returning `AlreadyExists`. The same
cycle applied to the unique principal index.

Both entry points now call `insert_credential`, which owns the transaction,
account lock, optional bound validation, insert and commit. There is one
credential insertion body. The account lookup remains a primary-key probe;
the unbounded API adds a transaction and account-lock round trip, and concurrent
issuance for one account serializes. It does not scan or enforce a live-key
bound. This is control-plane work; request admission is unchanged. Public
signatures and the schema are unchanged, and no migration is required. The
lock-order guarantee applies once all issuing processes use this implementation;
an older process that inserts before the account lock can still form the cycle.

The previous concurrency scenario exercised only bounded issuers, leaving the
retained sibling uncovered. The shared `mixed_issuers_report_duplicates`
scenario now drives both public methods on both backends. PostgreSQL's
`mixed_issuers_waiting_on_an_account_report_duplicates` holds the account and
observes the actual blocking graph before releasing two queued API calls. It
covers both uniqueness constraints and distinguishes the former implementation
without depending on a lucky scheduling overlap. Restoring the old
`insert_key` body makes this test fail with PostgreSQL's `deadlock detected`
storage error; the shared account-first implementation passes. These are
implementation witnesses, not a formal proof of PostgreSQL's lock manager.

The production credential-write audit found exactly these two insertion paths
and `revoke_key`; revocation takes no account lock and cannot complete this
cycle. Migration updates and fixture writes are separate from runtime issuance.
MemoryStore already uses one mutex for both methods, so its implementation
requires no change; it runs the same new outcome scenario.


## 2026-09-15 — Account administration validates intent and audits the committed transition

The GL-121 HTTP additions exposed three gaps in the earlier receipt convention.
Serde accepts a missing `Option` field as `None` without an explicit default,
so `{}` could clear a budget. The PostgreSQL budget self-join returned a
statement-snapshot predecessor after waiting for another update, so successive
100 → 200 → 300 updates could both report 100. Credential handlers supplied
`Absent` → `Absent` receipts themselves and targeted only the account, losing
both the key identity and the distinction between retirement and a no-op.

A required field deserializer now distinguishes omitted budget from explicit
null, preserving zero allowances as schedules. The budget setter locks and
decodes the account's predecessor, then updates the three schedule columns in
one transaction. Decode, write or commit failure returns no confirmed receipt.
Memory already captured that predecessor under its mutex. The PostgreSQL test
queues two real budget API calls behind a held row and observes the lock graph
before releasing them; the shared backend scenario separately verifies that
all returned receipts join into one history and that clearing is idempotent.

`KeyDirectory::insert_key_within_audited` and `revoke_key_audited` make the
store return lifecycle evidence. The existing methods retain their signatures
and use the same mutation implementation, discarding only the receipt. Issuance
retains account-first locking; revocation locks only its credential before
updating it and its revision, never acquiring an account lock afterward.
`AdminState::Credential` records account, key and retirement flag. This is
retirement evidence, not a claim that an expired credential can authenticate.
HTTP audit resources include account and key for attempts, confirmations and
store errors; confirmed states are forwarded without reconstruction. Concurrent
revocation tests require one transition and matching no-op receipts for every
other success, including an expired credential.

The listing used raw `Query`, unlike the instance projection's `ApiQuery`, and
passed oversized limits to the store, which classified them as storage failures.
Both endpoints now share a query DTO and limit validator. Tests require RFC-7807
content types and stable codes for malformed cursors, unsupported parameters,
zero and oversized limits, no store reads or backend diagnostics for invalid
input, and successful reads at both valid boundaries.

The sibling audit covered every request DTO's optional fields, both credential
listing handlers, all administrative receipt construction, and PostgreSQL
self-join predecessor reads. `SetBudgetRequest` was the only optional mutation
field whose omission cleared state; credential expiry omission intentionally
means no expiry. Both fabricated credential receipts were removed. No other
self-join receipt read or raw query extractor remains in the server module.
Existing tests exercised explicit-null budget updates and sequential receipts,
and the audit matrix predated these routes. New wire, HTTP, concurrent backend
and tracing witnesses cover those omissions. Lean models verify exact replacement
and retirement laws under a serialization assumption; they do not prove SQL
execution, logging delivery, or cryptographic verification.

Existing valid HTTP bodies and responses remain compatible. Invalid input now
fails as documented. Existing directory callers retain their return types;
out-of-tree `KeyDirectory` implementations must add the two required audited
methods, and exhaustive `AdminState` matches must handle `Credential`. The
schema needs no migration; deploy the updated server/store together. Old servers
retain the validation and audit defects until upgraded. Revocation and budget
updates add a row-lock read and transaction round trips, using primary-key probes
with constant-size receipts; this is control-plane work and adds no request-path
I/O, locks, allocations or clock reads. Audit collection remains the existing
tracing delivery contract, without a transactional outbox claim.


Validation included five deliberate regressions: accepting an omitted budget,
restoring the budget self-join, using raw Query, omitting the page ceiling, and
fabricating the issuance receipt. Each was caught by its focused witness; the
self-join probe returned 100 instead of the committed predecessor 200. The
fixed queued-budget witness passed. Full workspace tests passed against an
isolated PostgreSQL container; the final credential-owner decoder additionally
has a focused corruption witness. Workspace Clippy, the full Lean gate, invariant
references, backend parity and CI-policy checks passed. Fault injection is
implementation evidence; the existing CI mutation gate remains independently
required for this MR.

## 2026-09-22 — Two sharded benchmark rows measured a shard lottery (GL-123)

`admission/full_check_contended_8_sharded` and its `distinct_accounts` twin
dispersed far past every other contended row in the suite — worst-sample
overshoots of +149.8% and +103.0% against their medians, where the other seven
contended rows sat between 1.2% and 2.7% — and the first came back
`inconclusive` in !220's validating run with a `ci_width` of 0.294 against a
0.10 policy. Nine contended rows run eight threads on the same machine in the
same runs, so eight-thread scheduling was not the discriminator. The two
dispersive rows were exactly the two that shard.

The cause is the assignment *timing* of `Locality`, not contention. A locality
is taken on a thread's first access, not at spawn, from one process-global
counter, and `Locality::index` reduces it onto the shard count. The foreground
thread took its number in an earlier benchmark, at an unrelated position in
that counter; the seven contenders take seven consecutive numbers from wherever
the counter has reached by the time their fixture is built, which depends on
how many threads earlier rows in the same Criterion process already consumed.
Seven consecutive numbers cover seven of eight residues, so the one residue
they miss is the foreground thread's in exactly one offset out of eight.

An instrumented sweep on a development host (Intel i9-10920X, 24 logical CPUs,
Fedora 44) rebuilt the fixture in a fresh process with the counter's offset as
the only variable, and the two clusters are disjoint with nothing between them:
176.0 ns with no collision against 846–881 ns with one, recurring with period
8. The `distinct_accounts` fixture steps ×1.38 rather than ×4.9 for the same
reason its unsharded baseline is cheaper — its eight threads share less
per-shard state than eight threads on one account do. That is the shape of an
outcome that is hit or missed, and it is why the recorded medians sat between
two values neither row ever produced.

So the fixture now aligns the counter before spawning its contenders, putting
them on the shards the foreground thread does not occupy, and asserts the
mapping it realized before anything is timed — the discipline `sweep` already
applies to the at-capacity maps, where a fixture that silently stopped being
the thing the row names must fail loudly rather than be measured. A readiness
barrier comes with the assertion, and makes the contenders provably running
rather than probably running because Criterion warms up first. On the
development host both rows then measured 175.6 ns and 174.7 ns with confidence
intervals of 0.10% and 0.07% — two orders of magnitude inside the policy that
had been refusing them a verdict.

The sibling search found one more instance of the pattern and no others. The
`capacity/full_check_contended_8_distinct_accounts_{uniform,reserved}` rows run
a single-counter map against an eight-way sharded *pool*, and
`ExecutionCapacityGate::try_acquire` starts its walk at `locality.index`, so
they carried the same unpinned mapping even though their map does not shard.
They are pinned against the pool's layout, which is why the fixture takes the
sharding to spread across as a parameter rather than reading it from the
engine. `crates/tollgate-core/benches/core_hot_path.rs` spawns threads but
configures no sharding, so no locality varies a measurement there; no other
benchmark in the workspace constructs a non-`SINGLE` layout.

Existing safeguards missed this because the gate's dispersion allowance is the
only thing that ever looked at it, and it was widened rather than explained:
`dispersion_prone` excludes rows carrying a widened `max_regression` from the
host-trust breadth signal precisely because `full_check_contended_8_sharded`
was flagged in all ten GL-114 runs. A row that disperses for a reason nobody
diagnosed is indistinguishable from one that disperses because the machine is
busy, and the manifest ended up encoding the first as if it were the second.
Pinning the mapping is what lets those allowances come back down, and returns
both rows to the breadth signal they were excluded from.

Whether a *deployment* gets a clean mapping is a different question, and GL-124
owns it: the exposure is real rather than benchmark-only. A Tokio multi-thread
runtime with eight workers and eight shards collided in five runs of five once
a single non-worker thread read a locality between two workers' first requests,
and `SnapshotManager::apply` calls `map.get`, which reads one — so "only
request-serving threads consume this counter" is already false in the shipped
library. Nothing in `INVARIANTS.md` states that participating threads occupy
distinct shards, and nothing observes or reports occupancy, so a collided
instance degrades toward the unsharded cost while looking exactly like the
contention sharding was enabled to remove. That needs its own assignment and
observability design, which is why it is split rather than absorbed here.

## 2026-09-22 — A scoped tracing subscriber cannot decide a process-global cache

`credential_audits_name_the_key_and_actual_lifecycle_transition` failed the
`test` job on a merge request that changed only benchmarks and documentation.
It reproduced on `main` at the same rate — three runs in fifteen against the
branch's two — so it was pre-existing and order-dependent rather than anything
the branch did.

Instrumenting the assertion caught a failing run with the diagnosis in it. The
test captured six events: both `revoke_key` pairs, and neither `issue_key`
event — the same callsite, emitted *earlier* in the same
`with_subscriber` scope. Events from the start of a scope were dropped and
later ones kept, with the boundary moving run to run.

`tracing` keeps each callsite's `Interest` in a process-global slot and
computes it the first time any thread reaches that callsite, against that
thread's current dispatcher. A thread-local or future-scoped subscriber
therefore does not make the decision local: a test reaching a callsite while no
dispatcher is installed caches `Interest::never()` for the whole process, which
disables that callsite for every other thread. Another test installing its own
scoped dispatcher rebuilds the cache, and whichever emissions fall before that
rebuild are lost. It needs genuine parallelism to show: the test passes eight
runs in eight alone, twelve in twelve beside the only other capturing test in
its binary, and five in five forced sequential.

The lesson was already paid for and already written down. `tests/sweep.rs` and
`tollgate-client/tests/events.rs` install a global `Router` for exactly this
reason, and `sweep.rs` documents it at length — "one test reaching
`reclaim_sweep`'s `info!` with no dispatcher installed caches
`Interest::never()` for the whole process". Five capture sites across four
other binaries never got it, because nothing carried the requirement from the
binaries that knew to the binaries that did not. That is the drift a convention
upheld at each call site produces, and the enforcement ladder's answer is to
move it into the component that owns capturing.

So `common::EventCapture` now owns both halves. `during` scopes a subscriber
around a future and `on_this_thread` around a thread, and each arms the
process-global dispatcher first; there is no longer a way to subscribe without
arming, because the subscriber is no longer constructed at the call site. The
global is a bare `Registry`, whose only job is to keep interest out of `never`
— per-test isolation stays with the scoped subscribers, which `tracing`
consults ahead of it. `security.rs`'s private duplicate of the capture layer is
gone with it, so the crate has one capture mechanism rather than three.

The quiet direction is the one that justifies owning this rather than retrying
the job. `dropping_an_unpolled_reloader_is_an_expected_stop` asserts that *no*
event was emitted, and `every_backend_error_conversion_keeps_opaque_details_out_of_responses_and_debug`
asserts that no diagnostic leaked. A callsite cached as `never` makes both pass
for the wrong reason, with no failing run to investigate — a red pipeline is
the benign symptom of this defect, not its worst case.

Validation: the failing witness went from three failures in fifteen runs to
zero in forty full `tollgate-server` runs, and the whole workspace suite passes
at all features.

## 2026-09-22 — Sharding's separation is a deployment property, so it is reported (GL-124)

GL-123 established that a benchmark could measure a shard lottery instead of the
sharded path, and asked the separable question: does a deployment get a clean
thread-to-shard mapping? It does not necessarily. A Tokio runtime with eight
workers and eight shards collided in five runs of five once a single non-worker
thread read an affinity between two workers' first requests, and the collision
cost ×4.9 on the same-account fixture.

Two things were wrong beyond the mapping itself.

The module doc asserted the property. `sharding.rs` said that assigning each
participating thread a number "gives worker-thread workloads the property that
matters here: their routine writes land on **different** cache lines."
`docs/DESIGN.md` said the true, weaker thing in the same breath — "**stable**
cache lines". Distinctness does not survive the reduction by shard count, and
the module claimed it anyway.

And the library spent the budget it was asking embedders to protect.
`SnapshotManager` observed its own publications through `SnapshotMap::get`,
which resolves `Locality::current()` and therefore *assigns* on a thread that
holds no affinity yet. Six sibling reads in the runtime, the slot registry and
the lease manager did the same through `LeaseSlot::load`. None of them wanted a
shard: they ask whether a grant is present, until when, and what a published
generation says — facts every view of one slot and every shard of one entry
answer identically. The exposure is narrow but real, and it is the shape a
control plane takes whenever it does not share the request-serving worker pool:
its own runtime, the blocking pool, or a `block_on` during startup. A control
plane sharing a worker was never the problem, which is exactly why the first
version of the witness passed against the defect — a current-thread test drives
the manager on the thread that already holds an affinity, so nothing was spent
either way. Making it fail required a runtime whose worker had touched nothing.

The fix follows the enforcement ladder rather than pretending to top it.

What the library controls, the library owns. `LeaseSlot::load_observed` and a
`Locality::OBSERVER` constant give control-plane reads an affinity that is not
drawn from the counter, and the seven call sites use them. The observer aliases
shard zero under every layout, deliberately: any shard answers these questions,
so there is nothing to choose between them, and a constant cannot drift the way
"whichever number this thread happens to hold" does.

What the library does not control, it reports. It cannot know how many threads
an embedder runs, and — because affinities are never recycled — it cannot know
which of the ones it issued are still held. `LocalSharding::occupancy` returns
the shard count, the affinities issued, and `crowded_shards`, which is exact
arithmetic rather than a sample: the counter hands out `0..n`, so shard `i`
carries every `j < n` with `j % shards == i`, each shard carries `n / shards` or
one more, and the shards left above one collapse to
`shards.min(n.saturating_sub(shards))`. The test states that against the tally
it replaces, for every shard count up to sixteen and every load up to three
times it, rather than against a table of examples.

Reporting affinities rather than live threads is the honest reading and also
the useful one. An affinity a departed thread took still displaces every
affinity issued after it, so counting it is not over-reporting — it is the
condition an operator needs to act on. `RuntimeReport` carries the block so an
embedder reads it from the health surface it already polls, and
`docs/LOCAL_SHARDING.md` is the operator guidance that nothing under `docs/`
carried before: nothing there mentioned sharding at all, and the only
operator-facing text in the repository was two lines of `README.md`.

Deliberately not done: recycling affinities on thread exit, lowest-free
allocation, and any eager-claim API. They would make crowding structurally
impossible while live threads fit the layout, and they are a change to a core
primitive with their own rollout story — every test that pins a shard index,
and GL-123's `align_next_locality_after`, which reverse-engineers the counter and
panics if it cannot align. The dominant realistic cause is a shard count sized
below the worker pool, which no assignment scheme fixes and which the report
now names.

The initial counter witnesses confused one claim per thread with one claim per
read: allowing fewer than 1,000 claims over 1,000 reads accepted the old
first-use behavior, and the occupancy witness preclaimed its thread's affinity.
Their loose bounds also did not isolate the process-global counter; parallel
tests could invalidate exact report comparisons or consume the runtime test's
eight spare affinities while it awaited readiness.

The slot, occupancy, observer-constant and runtime-layout witnesses now rerun
only themselves in fresh test processes. Exact counter checks cover first and
repeated observer reads, and explicit request reads demonstrate that the
counter still advances. The snapshot-publication sibling already records the
map's requested affinity directly and needs no process isolation. Targeted
mutation checks replaced `load_observed` with `load` and inserted
`Locality::current()` in `occupancy`: each witness failed on the first read,
with one assigned affinity where zero was required. These are test-only
changes; the production affinity mechanism and its performance are unchanged.

## 2026-09-23 — Exhaustion is an allocator fact, not an empty lease (GL-128)

The issue proposed forwarding `InsufficientBalance` to admission as a permanent
refusal. Inspection and mirrored backend witnesses disproved the premise: an
account can have zero allocatable balance while another instance holds every
unit, and both release and expiry can restore those units without a deposit.
Existing tests bounded grants and pinned local retry classifications but never
claimed that the allocator's empty balance meant consumption. This is a missing
cross-plane distinction, not a regression in the original retry mapping.

The allocator now distinguishes zero *remaining funding* from zero allocatable
balance. It uses the same checked account-ledger projection as budget publication,
under the memory mutex or PostgreSQL account-row lock. Recorded usage, loss and
expired allowance consume funding; units still held in leases remain potential
funding. Unflushed usage cannot establish exhaustion. PostgreSQL performs one
additional primary-key read only on a zero-balance refusal, while holding the
row lock; it does not scan leases. Memory uses constant-time account arithmetic.

Consolidation needs care because its error rolls back settlement. A settlement
that introduces loss or expires old allowance can make the transaction's view
zero while the committed predecessor still has outstanding funding. Both
backends therefore certify only committed evidence: memory reads the unchanged
predecessor; PostgreSQL requires that the tentative settlement preserved total
funding, otherwise it retains the weaker refusal. This preserves the existing
all-or-nothing contract and avoids turning an uncommitted claim into authority.

The per-account slot owns the published deadline and an identity epoch. An
allocator attempt captures the epoch before I/O. A successful new fencing token
or accepted snapshot changing funding replaces the epoch and clears the
marker under one control-plane mutex; a delayed failure with the previous
identity cannot restore it. Epochs are Arc identities, not wrapping counters.
The high-water fencing token distinguishes new grants from restoration of the
same capability after a rolled-back consolidation. Taking or restoring that
lease leaves evidence intact. A scheduled deadline is floored to seconds for a
single AtomicI64; truncation may discard evidence early but never extends it.
The request path reads it only after stable local funding refusal.

The sibling audit covered ordinary acquire, consolidation, timeout, shutdown,
release failure classification, both snapshot-map publication paths, HTTP error
mapping, the pricing example, and dense denial/refill counters. Both map backends
construct accepted account state through the shared constructor. Release errors
cannot publish exhaustion; unknown or malformed HTTP responses cannot either.
Refundable elastic occupancy remains transient even with central exhaustion:
otherwise cancelling pending work would contradict `Never`. Successful elastic
admissions and lease-funded requests continue unchanged.

The Rust denial/allocator enums and Problem/ApiError literals require consumer
updates; HTTP clients should precede servers. There is no schema change. Backend
and transport tests establish the concrete semantics; the Lean model proves
nonnegative-ledger and epoch/expiry laws assuming serialization, not SQL locks
or machine memory ordering. See the embedding guide for propagation limits.

Validation: the all-feature workspace suite passed with a required real
PostgreSQL backend. Format, workspace Clippy, deterministic allocation,
invariant-reference, backend-parity, CI-policy, advisory and formal gates passed.
Scoped cargo-mutants runs caught all 26 viable mutations (seven more were
unviable), covering funding publication/deadlines and the changed allocator
classification. The full CI mutation gate remains independently required.
Adversarial timestamps caught Jiff's truncation of negative fractional seconds;
the implementation floors both operands, with signed-boundary witnesses.

Timed performance validation was explicitly deferred by the maintainer to
GL-129.
That issue names the affected admission refusal, successful/contended paths,
whole-baseline recalibration, fresh enforced validation and production load
measurement on the baseline's controlled host. No latency pass is claimed here,
and no baseline or threshold was changed.

Review of !234 found that the initial implementation (`a490ff9`) also kept a
snapshot generation in the account-shared funding observation. Generations are
principal-scoped: a principal advancing from 2 to 3 could not invalidate evidence
after another principal had published generation 7. The original witnesses
advanced one principal or shared equal generations, so they missed this case.
The slot now consumes the map's accepted publication and compares only budget
and enforcement mode. It needs no second generation registry. The regression
witness first failed on the original implementation and covers both map backends,
one and eight shards, independent budget and mode changes, rejected replays,
unchanged funding, and late allocator responses. The existing epoch proof still
applies; there is no request-path, API, or schema change in this correction.

The sibling search found no other exhaustion generation gate. The shared
account limiter's generation-winner policy is a separate, explicit contract
(invariant GL-5), not funding evidence. Both map publication paths reach the
corrected shared constructor. Consolidation already classifies
`BalanceExhausted` as rolled back in the client and preserves the old lease in
both backends; the allocator trait's refusal list now documents that guarantee.

Review validation passed all 1,374 workspace tests with required PostgreSQL and
caught both generated mutations of `observe_funding`. The unchanged epoch model
also passed the formal gate.

## 2026-09-23 — Remaining funding below a quote is an allocator fact too (GL-130)

GL-128 made zero remaining funding authoritative. It left an account with *some*
funding, less than one quote, answering `LeaseExhausted` / `Transient` forever:
1 unit left and a 252-unit batch retried indefinitely near the end of every
period. The issue proposed that the allocator refuse with the remainder when it
"cannot supply the units a refused quote needs". Tracing the example showed the
allocator never refuses in that state. Consolidation returns the 1-unit tail,
and the grant floor (GL-109) re-grants exactly it. A refusal-only design would need
the refused quote passed to the allocator: a new `needed` argument on
`acquire`/`consolidate`, the wire DTOs, and an atomic max on the lease's refusal
path. That was rejected in favour of evidence on *every* allocator answer.

Every grant now carries `BalanceShortfall { remaining, period_end }` read from
the ledger the grant commits into, and a refusal with nothing allocatable
carries the same fact. The allocator never learns a quote; admission compares.
Soundness is the GL-128 argument extended. Ledger remaining (`funded - consumed`)
counts outstanding lease units and unreported usage as still available, so it
is an upper bound on true remaining funding. Only new funding or a period
rollover can raise truth above it, and both invalidate the evidence. A quote
above it cannot be funded; a quote within it proves nothing and keeps the lease
refusal's advice. PostgreSQL reads the grant's evidence from `RETURNING` on the
debit it already performs, so a grant costs no extra round-trip. A refusal
keeps the one additional primary-key read under the row lock.

The rolled-back consolidation rule from GL-128 now applies to both backends
alike. PostgreSQL's in-transaction view already includes the settlement it
rolls back, so it attests only when that settlement preserved funding.
MemoryStore plans before applying and could have attested from untouched
state, but it adopts the same guard: the reference backend and the SQL backend
must answer the same scenario identically (`check_backend_parity.sh` checks
names, not bodies). The case is narrow: a lossy settlement that is refused.

Publication follows the GL-128 epoch model with two additions. A grant installs
its lease and publishes its evidence in one control-plane critical section
(`FundingAttempt::granted`), so the fence-advancing invalidation cannot erase
the grant's own reading. It publishes only when no funding change was accepted
while the call was out, because a top-up the allocator had not yet seen would
otherwise hide behind a smaller remaining. And the slot now holds a *pair*,
deadline and remaining, in two words. The first draft used the deadline as its
own sequence word: re-read it after `remaining`, and accept when unchanged.
Writing the Lean model found the ABA case that draft misses. Two publications
sharing a deadline can bracket a third whose `remaining` the reader then pairs
with the wrong deadline, which a shortened budget schedule could make unsound.
The slot now carries a real seqlock sequence. The reader loads the deadline
alone first, so a refusal with no live evidence still costs one atomic load.
Live evidence costs four more loads, only on a funding refusal. The concurrency
witness uses repeating deadlines, so it fails on the draft: a mutation removing
the sequence check failed three of three runs.

Wire compatibility is better than GL-128's. An attested shortfall keeps the
`insufficient-balance` code and adds a `balance_shortfall` extension, so an old
client sees exactly the refusal it always did. Grant responses flatten the
grant beside an optional `funding` object, which old clients ignore and old
servers omit. No rollout order is required. New clients discard evidence that
cannot be valid (a zero remaining under the shortfall code, or grant evidence
below the grant's own units) rather than let it refuse a fundable quote.

The sibling search covered every `BalanceExhausted` site: the allocator enum,
dense slots and failure maps, both backends, HTTP mapping in both directions,
the pricing example, the lease manager's acquire and consolidate paths, and the
admission classification. Each gained its shortfall counterpart; none was
deferred. Two test updates were behavioural, not mechanical. Drain loops in both
backend suites now end on `BalanceInsufficient` carrying the full deposit, and
`exhausted_quota_returns_429_and_never_overspends` accepts the 402 it now
honestly returns once usage settles.

One pre-existing liveness gap was observed and is not changed here. Under a
`shrink_divisor` above one, an account whose total remaining is below twice a
quote can never assemble a lease that funds it: 60 units re-grant as 30 + 30,
and no single grant exceeds 30. Evidence correctly keeps such a quote transient,
because it is within remaining funding. The gap belongs to the grant policy
and is tracked in
GL-131.

## 2026-09-23 — Pricing the confirmed-exhaustion refusal (GL-129)

GL-128 made a failed local funding step consult the account's exhaustion
deadline before choosing its refusal: one acquire load of an `AtomicI64` in the
`LeaseSlot` and a floored second comparison. Successful admission never reaches
it. GL-128 shipped with structural and allocation evidence only, so this change
measures it and re-records the baseline.

**The existing refusal row covered only half of the branch.**
`full_check_lease_exhausted_strict` pays the load and finds no evidence, so the
reclassification to `BalanceExhausted` and its separate denial counter had no
benchmark. `full_check_balance_exhausted_strict` runs the same fixture with
evidence recorded, asserts in setup that it reaches `BalanceExhausted` (a
fixture that fell through would measure the old row under a new name), and is
held to ×1.2 of the lease-exhausted row in the same run. The two measured
×0.99–×1.01: finding evidence costs no more than not finding it, so the price is
the load, not the branch.

**The load is a real, accepted cost.** The first full run put the strict
lease-exhausted refusal at 62.48 ns against its 58.57 ns baseline (×1.067, over
the 5% allowance) on a trusted run whose median drift was ×1.000. An interleaved
A/B of `22c67a6` (before GL-128) and `208a797`, alternating twice, measured
54.76/55.32 ns → 57.52/57.77 ns, while `full_check_denied`, which returns before
funding, stayed at 15.7–16.0 ns. A filtered A/B is not comparable to the
recorded baseline, but the difference is: roughly 2.5–2.8 ns, about 5%, on a
refusal path. It was accepted as GL-128's designed cost rather than optimized.
Whether `exhausted_until` shares a cache line with the lease pointer was not
investigated.

**The manifest had claimed an allowance the gate never enforced.** Its comment
on the strict row cited a row-specific 7% regression allowance, but no recorded
baseline has carried one; the row has always been judged at the default 5%. The
three recording runs spread 60.33–62.48 ns (3.6%), inside the default, so the
comment now states what is enforced instead of widening the row.

The baseline was recorded whole from the median of three trusted full runs at
`208a797` on `mistral-apple-m1-pro`, whose OS had moved from macOS 26.6.2 to
27.0 since the previous recording. The single-run excursion that failed the
first validation run — `commit_usage_{fallback,overage}_unattributed_split` at
×1.13 while their `key_split` twins, the same code, held at ×1.00 — was not
reproduced, and GL-128 does not touch the commit path.

This calibration was recorded against GL-128's code and merged after GL-130 had
landed. Rebasing it ported the new witness's setup to GL-130's
`FundingAttempt::shortfall` and left the recorded file unchanged. That file
therefore does not measure GL-130's live-evidence read or GL-131's refused-quote
record. Re-recording on the controlled host, with a
`BalanceInsufficient` witness, is tracked in
GL-135.

## 2026-09-23 — Consolidation grows to a refused quote (GL-131)

GL-109 recorded a deliberate limit: under a `shrink_divisor` above one, a quote
larger than `balance / shrink_divisor` is unfundable by any single lease,
"and `EnforcementMode::Elastic` is the answer to it, not a larger grant". That
trade was tolerable while the caller's advice was vague. GL-130 made the advice
precise, and it exposed the cost. A 60-unit account under the default policy
grants 30. Every refusal-driven consolidation folds 30 held and 30 in the
ledger back into a 30-unit lease. A 51-unit quote within the account's
evidenced funding is told, correctly, to retry, and every retry fails until
the period ends. Elastic is an enforcement choice with billing consequences;
it is not a remedy for a strict account that can pay.

The shrink cap guards against one holder hoarding a small balance *ahead of
demand*, stranding it from instances that would have spent it. A quote the
holder has already refused is not ahead of demand: it is demand, and the
request that proved it spends it immediately. So the lease now records the
largest quote it refused for want of units, and consolidation carries it to
the allocator. `GrantPolicy::consolidation_grant` grows the replacement to that
quote when the restored balance can fund it, and never otherwise, because no
grant would serve an unfundable quote. Growth is bounded by one refused quote.
A plain acquire, and everything the grown lease does not take, keep the
ordinary policy: `growth_leaves_the_rest_for_another_instance` witnesses a
second instance still acquiring from the remainder. Sizing is one method both
backends call, which also removed the floor arithmetic they had each inlined.

Two alternatives were rejected. Relaxing the cap for an account's sole holder
needs no new input, but leaves two instances sharing an account stuck: the
case the divisor exists for. Exempting consolidation from the cap entirely
would hand the whole balance to any refused holder, reintroducing the
stranding on every refusal. Demand-sized growth is the smallest grant that
makes the refused quote reachable.

Recording demand costs one `fetch_max` on the lease-exhaustion refusal path,
raised before the doorbell's `AcqRel` swap that publishes it; successful debits
are untouched. An expiry refusal records nothing, because it rotates rather
than consolidates. The quote is read at quiescence, the same condition that
makes `remaining` exact. On the wire, `needed` is optional and omitted at
zero, so either side can be older: the result is the earlier sizing, never a
failure. The trait gains an eighth parameter, allowed with a stated reason
rather than bundled, because the exchange's inputs are the contract. The
PostgreSQL grant helper bundles its consolidation-only inputs instead,
because they are private and a plain acquire passes one named constant.

Three existing tests pinned the old size of a refusal-driven replacement
incidentally: two orchestration tests of an unanswered consolidation (50 units)
and the loopback shutdown-accounting matrix's consolidate case (26 units).
Their refused quotes, 60 and 51, now size the replacement. Their subject, that
an unanswered grant is reported and reclaimed, is unchanged, so the expected
balances follow the grown grant; the acquire case still expects 26.

Adding the witnesses also surfaced GL-130 timing siblings. The elastic example
and two spend-down tests (`no_double_spend`, the loopback customer-key test)
accepted only lease refusals. Once the allocator has attested what is left,
`balance-insufficient` or `balance-exhausted` is the honest, zero-charge answer.
They were fixed on !236's branch after it had merged, so they land with this
change rather than with GL-130.

## 2026-09-23 — Reclaim forfeits what a holder never released (GL-136)

A hard-killed instance loses its committed-but-unflushed usage queue: there is
no write-ahead log. At TTL the sweep credited `granted - used` back, where
`used` counted only ingested usage, so the executed-but-unflushed units became
spendable again. Under `Strict` that is over-spend; nothing recorded it; and a
straggler for the lease was rejected, because the credit had already
accounted for the whole remainder. GL-9 read "crash leak is bounded by TTL", a
statement about *unspent* units stranded until TTL. It never covered *spent*
units returned at TTL. A consumer's no-overage guarantee must hold across
crashes, and graceful drain already covered the rest.

The issue proposed crediting `granted - max(used, reported_committed)`, from
a committed-units mark the instance reports periodically. That is not sound.
A report is a lower bound on what was committed, because the lease accepts
commits until `usable_until`, so work committed after the last report would
still be credited back. The only report that proves units unspent is one taken
after spending stops, and that is a release. During the outage the issue's own
acceptance scenario requires, no report reaches the store at all.

So the sweep now settles an unreleased lease exactly as a release claiming
nothing unspent would. It credits nothing and records the remainder as
provisional settlement loss. The rest already existed and was already
witnessed. A settled lease accepts usage up to `granted - used - credited`,
and such usage moves units from loss to billed usage. With `credited = 0`, a
holder that outlives an outage is billed on flush instead of dropped, which
the old credit got wrong for live holders too. The conservation equation is
unchanged, because loss is one of its terms, and there is no schema change. The
cost is bounded and visible: a crash forfeits the unspent remainder of what
the instance held. The sweep warns with `forfeited_units`, and
`ReclaimedLease.reclaimed` was renamed `forfeited` because its meaning changed;
config and wire values are contracts.

Two consequences needed decisions:

- **A closed-period lease's remainder.** It used to be split at the sweep
  between `expired` and the balance. It is now forfeited as loss, not booked
  as expiry. The sweep credits nothing, so it cannot resurrect the closed
  allowance, and stragglers still bill against the lease's own period. Release
  keeps the period split, because a release does credit.
- **The elastic commit-time fallback.** Its bill is overage-sourced, and the
  old rationale was that a leased bill would be rejected as a reclaimed-lease
  straggler. That is no longer true. The rationale that survives is stronger:
  the fallback's receipt returned to its lease, so the lease never funded the
  work. A leased bill would claim units the settlement already accounted for,
  rejected against a release's credit or billed against a forfeit the fallback
  did not cause. The witness was reworked around a release and renamed.

Rejected alternatives: a durable local commit log (the issue's option 2) bounds
loss by an fsync interval, but only when the same host and disk return, and it
adds request-path-adjacent I/O. Loss reporting alone (option 3) still
over-spends. A configurable `Credit`/`Forfeit` policy would keep a default
that over-spends under `Strict`, contrary to failing closed for unknown
state.

Deferred, not needed for soundness: a holder alive after its lease was swept
(an outage longer than the grace) could still prove its exact unspent count at
quiescence, and a "late release" could move that amount back from loss to the
balance. Without it, that holder's unspent remainder is forfeited like a
crash's. The design is open, because it needs a new settled state in both
backends and a client path, so it waits for demand.

The sibling search covered every reader of the renamed field (server sweep
progress, the drain error, loopback accounting), every invariant and doc
sentence that said reclaim returns units (GL-9, the shutdown and
bounded-operation paragraphs, the period-boundary paragraph, GL-12's fallback
rationale, the embedding guide, the control-plane operator guide, the
commit-fallback proof comment), and every test asserting post-reclaim balance
restoration or straggler rejection. The last group covered both backend
suites, the migration test, runtime orchestration, the loopback shutdown
matrix and the server sweep suite, and each was updated to the forfeit with its
subject unchanged. The crash witnesses run an instance on its own Tokio
runtime and drop it, which cancels every task without async cleanup. Before
this change they fail by construction, because the sweep returned the whole
grant.

## 2026-09-23 — Outcome tallies shard under every layout (GL-132)

`AdmissionCounters` is one instance per snapshot map, and every account's
request state holds an `Arc` to it. On the default single-locality layout its
per-request tallies — `admitted`, `units_admitted`, the denial slots,
`execution_started`, `canceled_before_start` — were inline `Padded` atomics, so
every admission of every account did relaxed read-modify-writes on the same few
lines. Only an opt-in sharded lease layout partitioned them.

**The shared tallies were most of the cross-account cost.** Eight threads on
eight distinct accounts recorded 680.48 ns per admission on the default layout
against 120.38 ns sharded (GL-129 baseline). A throwaway build that partitioned
only the tallies, eight ways, with leases and account state left on the
default layout, measured 772/749 ns → 181/252 ns in an interleaved filtered A/B
while another session's eight-process job loaded the host; same-account
contention (2.99/2.94 µs → 2.92/2.95 µs) did not move, which is the check that
the change hit the tallies and not the lease. The shipped change measured
972/1,029 ns → 269/253 ns on that row, 1.618/1.635 µs → 1.211/1.287 µs on the
uniform capacity row, and no consistent single-thread cost on `full_check`
(123.4/127.1 ns → 126.1/121.5 ns), under the same load. Those are diagnostics,
not calibration; the controlled-host record belongs with the baseline.

**Why the tallies can shard when leases stay opt-in.** Lease sharding is paid
per account: every account's admission state, lease counters and governor
buckets are replicated or partitioned per shard, so retained memory grows from
about 4.0 KB per account unsharded to 14.6 KB at eight shards and 26.0 KB at
sixteen (2,000 accounts, one admission each, counted by `tollgate-alloc-count`),
and the account's rate burst is partitioned across buckets. A sharded lease
does not strand headroom — a debit steals a whole debit from siblings before
fragmenting, which `sharded_lease_spends_to_exact_exhaustion_without_stranding`
witnesses. The tallies are one set per instance, not per account, and a
monitoring total is a sum however it is split, so they carry none of those
costs; the snapshot and `units_admitted` read paths, both control-plane, sum
the shards. (An earlier revision of this section gave stranded headroom as the
reason; it was wrong.) The layout is the larger of the lease sharding
and the host's parallelism, rounded to a power of two, so the per-request
reduction is always a mask and never a division. `execution_started_by_class`
moved into the shards with `execution_started`, because every executed request
bumps it; sheds, abandoned contexts, commit refusals and the overage
qualifiers stay inline, as bounded exceptions whose paths already serialize or
run far below the admission rate. There is no inline variant left, which also
removes the single-versus-sharded branch from every record call.

**The contended fixtures now pin against the tally layout.** They pinned only
against the lease layout, which is one shard on the default rows. With the
tallies at sixteen shards on the controlled host, the foreground thread would
have shared a contender's tally shard seven times in sixteen — GL-123's lottery
on a different structure. `spawn_contenders_with` aligns against
`AdmissionCounters::local_sharding`, which always refines the layout a fixture
spreads across, and asserts that no two fixture threads share a tally shard.

**It exposed the capacity gate's own contended cost.** The ratio of
`capacity/full_check_contended_8_distinct_accounts_{uniform,reserved}` to
`admission/full_check_contended_8_distinct_accounts` was calibrated at ×2.1
against a denominator inflated by the shared tallies. The pool itself was
already sharded, one compare-exchange and one add on the request's own shard,
so it is not the same defect. On a quiet controlled host the distinct-account
row measured 153.46 ns (from 680.48 ns) and the capacity rows 927.97 ns and
950.77 ns, ratios of ×6.05 and ×6.20 against the old ×4.0 bound. The
recording series then showed how much of that ratio is the denominator: across
five trusted full runs the distinct-account row read 120–192 ns while the
capacity rows held 927–1,320 ns, and the ratio spanned ×6.05–×9.80. A first
bound of ×8, taken from the diagnostic run alone, failed the validating run at
×9.80 with every row inside its baseline. A gate that serialized the instance
would approach same-account contention, about 3 µs or ×20, so the bound is ×12:
the measured spread plus 22%, still well short of serialization. The distinct-account row's absolute threshold tightens from
3,000 ns to 600 ns, so a returning shared line fails on any host.

The same run showed every `commit_usage` row 2–7% above its baseline, which
would have been a cost on the commit path's execution-start tally. An
interleaved filtered A/B of `main` against this change on a quiet host put five
of them level (113.17/114.92, 111.52/112.21, 115.61/115.22, 116.23/115.16,
157.82/158.02 ns) alongside `full_check` (108.60/108.41 ns): host drift, not
code.

`AdmissionCounters::new` is no longer `const`: the layout depends on the host.
No workspace caller used it in a const context.

**The same-account contention rows are bimodal on the controlled host, and
their allowances now say so.** Across eleven full runs spanning GL-129 and GL-132,
`lease/reserve_commit_contended_8`, `admission/full_check_contended_8` and
`admission/concurrency_acquire` each sat in one of two modes per run — about
2.4–2.9 µs or about 3.2–3.5 µs — and moved together. The lease row executes
no tally code, so the mode is the host's (most plausibly how eight busy
threads land on the M1 Pro's performance and efficiency cores), not GL-132's.
GL-132's first recording series happened to land in the fast mode and its
validating run in the slow one, failing both admission rows by 17% with every
other row inside its baseline. Their carried `max_regression` values were
raised deliberately before re-recording, to the measured mode spread:
×1.20 for the two admission rows (0.25) and ×1.24 from its median, ×1.39 at the
extremes, for the lease row (0.30, from 0.20).

`admission/full_check_contended_8_distinct_accounts` shows the same host modes
at a smaller scale: 113 ns with a 0.1% confidence interval in some runs,
120–223 ns with 9–16% intervals in others, across ten full runs. The recording
landed at 113 ns, and the validating run read 214 ns and passed only because
the gate treats an unstable reading as inconclusive. Its carried allowance is
therefore 1.2, the measured ×1.97 envelope plus margin. That makes the
baseline comparison weak on this row by design: what GL-132 must never regress,
a returning shared tally line at about 680 ns, is caught by the row's 600 ns
absolute threshold and its ×8 ratio against `admission/full_check`, neither of
which depends on the mode a run lands in.

## 2026-09-24 — The pinned contended rows are not quiet on the controlled host (GL-125)

GL-125 expected GL-123's pinning to make four contended rows quiet enough that
their widened allowances could come down to their dispersion. Two full runs on
a development host suggested 2–3% between-run movement. Eleven trusted full
runs on `mistral-apple-m1-pro` across GL-129 and GL-132 said otherwise:

| Row | Range | Recorded | Max over recorded | Allowance |
|---|---|---|---|---|
| `admission/full_check_contended_8_sharded` | 118–145 ns | 119 ns | ×1.22 | 0.15 → 0.25 |
| `admission/full_check_contended_8_distinct_accounts_sharded` | 118–159 ns | 118 ns | ×1.35 | 0.20 → 0.40 |
| `capacity/full_check_contended_8_distinct_accounts_uniform` | 905–1,267 ns | 1,118 ns | ×1.13 | 0.15 (kept) |
| `capacity/full_check_contended_8_distinct_accounts_reserved` | 799–1,320 ns | 1,110 ns | ×1.19 | 0.15 → 0.25 |

These are the same host modes the same-account contention rows show (GL-132):
the pinning removed the thread-to-shard lottery, which was a code-shaped
problem, but not the machine's run-to-run placement of eight busy threads.
Lowering the allowances would have made the gate flakier, so they were raised
to the observed envelope instead, and the discrimination moved to where it is
host-independent: the two admission rows' absolute thresholds drop from
2,500 ns and 1,000 ns to 400 ns each, below the 426–862 ns collision mode GL-123
removed and the ~680 ns shared tally line GL-132 removed.

## 2026-09-24 — A per-account contention signal (GL-134)

Whether an account is hot — its admissions overlapping across cores on one
instance often enough that its per-account lines bounce — depends on a
deployment's traffic shape, which the library cannot see. Sharding decisions
were therefore being made blind. This adds the cheapest honest signal.

**What is counted.** Every lease debit already runs a compare-exchange loop on
its shard's `remaining` (`try_whole`, and `take_up_to` for fragments). A lost
exchange is proof that another core wrote the line between this debit's load
and its exchange. The loop counts losses in a register and, only if there were
any, adds them once to a `contended` counter in the same `LeaseShard`, in
padding the 128-byte shard already had: no memory, no new line, and one extra
write, on a line the thread is already contending for, per contended debit. An
uncontended debit pays one untaken branch.

**Why it is a lower bound, and why that is acceptable.** Most per-account
traffic under contention is not a retry loop: the governor bucket lives in a
dependency, and reference counts and settlement are `fetch_add`s that pay for a
bouncing line without failing. A debit whose exchange lands between two rivals'
also records nothing. The alternative designs considered all cost more than the
signal is worth: returning a retry count up through `Reservation`'s public API
put telemetry into request-path signatures, and a per-locality counter array
(the GL-132 tally pattern) would have added about 2 KB per account on the default
layout — half again today's footprint — to avoid one write on an already
contended line.

**How it survives rotation.** Leases rotate; accounts do not. `LeaseSlot`
folds the outgoing lease's count into a per-account total on the one
`publish` path every `replace` and `take` goes through. The lease remembers
what it has handed over (`take_unreported_contention`, raised with
`fetch_max`), so a refused consolidation that reinstalls the old grant cannot
count the same races twice. A read takes the retired total before the current
lease's pending count, so a read racing a rotation can transiently undercount
but never double-counts.

**What an operator sees.** `RuntimeReport::contention` carries the instance
total and at most eight `(AccountId, count)` pairs, ranked by a total order so
the registry's hash order never reaches the output, and walked outside the
registry lock. The pricing example exports it under `/metrics`.

**What it said about the load gate.** A throwaway probe read the report after
each load-gate scenario on the controlled host: sequential, ten connections on
one account (about 93,000 admissions a second) and ten on distinct accounts
all recorded zero, while the eight-thread stress tests record nonzero counts. So the
same-account scenario is not hot by this measure, even though GL-133 measured
its end-to-end admission overhead at a few hundred nanoseconds: that cost is on
the lines this signal cannot see, and the lower bound is visible in practice,
not only in principle.

While here, `LocalLease::remaining`'s documentation had been separated from
its function by GL-131, which inserted `largest_refused_quote` between them; the
paragraph now documents `remaining` again.

*Tests:* `an_uncontended_debit_records_no_contention`,
`contended_debits_are_recorded_without_disturbing_the_balance`,
`unreported_contention_is_handed_out_exactly_once`,
`account_contention_survives_rotation_without_double_counting`,
`contention_report_ranks_by_count_then_account`, and the `/metrics` wiring in
`metrics_report_refill_and_snapshot_health`.

### Decision: lease sharding stays opt-in, decided per deployment from the signal

GL-134 asked whether hot single-account contention should shard without an
embedder opting in. Three options were costed on the controlled host.

| Option | What it costs | Verdict |
|---|---|---|
| Keep opt-in; document the cost and give operators the signal | Docs and the counter above | **Chosen** |
| Shard by default | Every account pays: retained memory 4.0 KB → 14.6 KB (8 shards) or 26.0 KB (16), the rate burst partitioned across buckets (a bursty tenant can see refusals it would not see unsharded), and an existing configuration value changes meaning, which needs a rename or startup warning and a rollout plan. Latency is not the objection: +5 ns uncontended. | Rejected |
| Adaptive: promote an account to a sharded slot when it contends | A new concurrency design — migrating a live lease, its rate buckets and its state between layouts while requests are in flight — with invariants and a conservation proof before any code | Deferred |

Sharding by default charges every account for a condition only some
deployments have. Whether a deployment has it depends on tenant concentration,
instance count and routing, and on request size, none of which the library can
see. Before this change that question could only be answered by inference; the
contention report now answers it per account, so the deployment decides.

Adaptive sharding is deferred rather than filed, because nothing measured so
far needs it: the load gate's heaviest same-account traffic, about 93,000
admissions a second on one instance, recorded no lost race. **Reopen it when**
a deployment's contention report shows a sustained, climbing count for an
account *and* opting that deployment into sharding measurably improves its
admission latency. That pair is the evidence an adaptive design would have to
be built against.

**The signal's limit is part of the decision.** It is a lower bound: plain
read-modify-write traffic on the rate bucket, reference counts and settlement
pays for a contended line without ever failing an exchange, which is why the
same-account load gate records zero while GL-133 measured its admission overhead
at a few hundred nanoseconds. A zero means "not shown to be hot", not "free".
If that gap ever decides a sharding question, the next instrument is a sampled
timing of admission itself rather than a retry count; GL-138, which found `admit`
costing more across unrelated accounts in service than in isolation, is the
nearest open measurement of that kind.

## 2026-09-24 — The usage queue is partitioned into lanes (GL-137)

GL-133 found the usage queue the largest contended stage the admitted path adds:
reserving a slot and sending the event cost ~0.27 µs sequentially and
~0.85 µs at ten connections, and unlike every per-account structure it is
shared by every request of every account on the instance. A contended
Criterion row (`usage_queue`, new here) put the cause on the machine: eight
threads reserving and releasing cost 848–858 ns each against 26.5 ns alone,
×32.

**Where it went.** One `tokio::mpsc` channel meant every reservation cloned
the one sender (its count), took the one semaphore, and every send pushed to
the one tail, bumped the one `unaccounted` counter and notified the one
receiver waker — which, because the writer parked on the channel, scheduled
the writer once per event even though it only *flushes* at a full batch or
its interval.

**What replaced it.**

- **Lanes.** The queue is a set of bounded channels, one per request
  locality, sized to split `queue_capacity` exactly: the host's parallelism
  as a power of two, but never so many that a lane falls below 64 slots
  (`MIN_LANE_CAPACITY`), so a small queue stays the single channel it was. A
  request reserves in its own lane and walks the others before it sheds, so
  the shed point is the whole queue, not a lane (INVARIANTS.md GL-8) — the
  capacity pool's "a partition is not a reservation" rule. Keeping
  `tokio::mpsc` per lane keeps its owned permits, its drop-releases-the-slot
  and its weak-sender count of outstanding permits, which the drain reports.
- **No reference count per request.** Every reservation used to clone
  `Arc<WriterCounters>` into the permit, one more shared line. A permit now
  holds only its lane's `Arc`, padded to its own line; what lanes share sits
  behind it and is never cloned per request.
- **No wake per event.** The writer never parks on a receiver, so a send
  finds no waker to wake. It drains every lane on its flush tick — when a
  partial batch was due anyway, so delivery timing is unchanged — and earlier
  when a lane reaches its ring point (a batch, or half a small lane), which
  rings a `Notify` once per fill: the flag is set by the send that crosses it
  and cleared by the writer before it reads the lane. A lane whose last handle
  drops rings too, so dropping the recorder still stops the writer at once.
- **The drain.** `final_flush` sets a draining flag, closes every lane, and
  collects until every lane reports disconnected. Tokio's `try_recv` reports
  a closed lane disconnected exactly when it is empty and every permit has
  resolved — the signal the single channel's `recv` gave. While draining,
  every permit that resolves rings the doorbell after releasing its slot, so
  the drain waits rather than polls; the flag is read on every permit release
  and written once, a shared read and not a write.
- **Entry counting.** `unaccounted` is each lane's entries less what the
  writer settled. An entry is counted before its send and settled only after
  its receive, so a dying writer never reports holding less than it held.

**Ordering.** Events within a lane keep their order. Events that overflow
into another lane are delivered in that lane's order: a single-connection test
that recorded two events into a queue of eight one-slot lanes saw them swapped,
which is what set the 64-slot floor. Across threads, order was already a race,
and the sink is idempotent by request id.

**Measured.** Interleaved against `main` on the controlled host:
`reserve_release_contended_8` 858/848 ns → 31.4/33.4 ns; the uncontended rows
unchanged (`reserve_release` 26.7/26.5 → 29.2/27.9 ns, `reserve_record`
125.9/105.2 → 106.8/105.2 ns). `reserve_record_contended_8` did not move
(217/231 → 232/241 ns): its background threads only reserve and release,
because eight threads *recording* in a tight loop now outrun the one writer,
fill the queue and shed — a service sheds at that rate too, far above any
request rate — and eight bench threads plus two runtime workers oversubscribe
the ten-core host. The service is the acceptance: GL-133's stage
instrumentation, rebuilt on this change, put the in-handler reservation at
10 connections on distinct accounts at 276/284 → 167/148 ns and the send at
559/557 → 281/301 ns, about 0.39 µs less per request, with sequential stages
unchanged. What remains of the contended send is the producer/consumer exchange
with the writer on the lane's own lines.

`usage_queue/reserve_release_contended_8 / reserve_release` is gated at ×3:
×1.1–×1.2 with lanes, ×32 with one channel, and portable across hosts.

**Four rows' allowances were sized to their envelopes with this change.** The
recording series tripped a different untouched row in three of four runs, each
at the edge of an allowance its own history had already exceeded:

| Row | Observed on the controlled host | Recorded | Allowance |
|---|---|---|---|
| `admission/full_check_lease_exhausted_strict` | 58.8–63.4 ns over 16 runs (GL-129–GL-137), tripping 5% in three series | 60.7 ns | 0.05 → 0.10 |
| `admission/snapshot_lookup_moka_at_capacity` | 65.4–81.75 ns | 72.2 ns | 0.10 → 0.20 |
| `reservation/commit_cancel_race_contended_2` | 163.9–189.9 µs | 169.1 µs | 0.10 → 0.20 |
| `usage_queue/reserve_record_contended_8` | 195.7–334.5 ns in its first series | 200.6 ns | 0.05 → 0.80 |

The first three are the host modes GL-125 and GL-132 recorded, reaching rows those
changes did not revisit. The last is this change's own row: on an
oversubscribed host it is too noisy to gate per-row and cannot distinguish one
channel from lanes, so its 800 ns threshold is the guard and
`reserve_release_contended_8` with its ×3 ratio is the lanes' witness. No row
the lanes change was widened to pass: the contended reservation measured
29.7–30.7 ns across the series, and its ratio ×1.06.


## 2026-09-24 — Cross-account `admit` cost is line migration, not sharing (GL-138)

GL-133 measured `admit` in the example service at 217 ns sequentially and
556–603 ns at ten connections on *distinct* accounts, while Criterion's
distinct-account row measured the whole admission cycle at 113–223 ns. After
GL-132 and GL-137 no admission line is shared across accounts, so something else
was growing.

**Where the time went.** Throwaway `Instant` marks between `admit`'s steps,
accumulated in per-locality padded slots so the instrument did not create a
shared line of its own, gave mean ns per step in the service (production
profile, controlled host):

| Step | Sequential | 10 conn, distinct accounts |
|---|---:|---:|
| quote | 25–26 | 24 |
| limiter load | 34–39 | 39–45 |
| rate tokens (governor bucket) | 30–35 | 115–129 |
| concurrency acquire | 29–32 | 124–132 |
| lease reserve | 39–41 | 147–161 |
| tally | 25–26 | 47–48 |

The reads did not grow; the three steps that read-modify-write the account's
own state grew 3.8–4.2×.

**Why.** Tokio's work-stealing scheduler moves a connection's task between
worker threads, so an account's lines are written from whichever core ran its
previous request, and each read-modify-write first pulls the line across. A
throwaway Criterion variant isolates it: eight threads on eight distinct
accounts cost 117 ns when each account stays on one thread and 1.04 µs when
each thread cycles through them. The service sits between the two because
only some requests change core. `full_check_contended_8_distinct_accounts`
pins accounts to threads, which is why it never showed this.

**What it means.** An admission and its settle write about six per-account
lines — the governor bucket, the principal and account gauges, the lease
shard, and the account state's and lease view's reference counts — each padded
to its own 128-byte line. That padding is right when several threads write one
account at once; under migration it multiplies the lines that move. Reducing
the lines written per admission without giving up that protection, and adding
a rotating-account witness to the gate, is GL-140. The deployment side needs no
library change: routing an account's connections to one core, or a
thread-per-core runtime, avoids the migration entirely.

No code changed for GL-138; the instrumentation and the variant were throwaway.

## 2026-09-24 — The per-account layout stays; the migration is gated and documented (GL-140)

GL-138 traced the in-service growth of `admit` to per-account lines migrating
between cores. GL-140 asked whether writing fewer of them was worth it, measured
each candidate in isolation first.

**The witness.** `admission/full_check_contended_8_distinct_accounts_rotating`
runs the distinct-account fixture with every thread cycling through all eight
accounts: 1.11–1.18 µs against the pinned row's 114–118 ns.

**Ablations** (throwaway builds, alternating, controlled host), removing one
set of writes at a time:

| Variant | Rotating (ns) | Pinned (ns) |
|---|---|---|
| unmodified | 966 / 1,010 | 118 / 116 |
| no principal gauge | 974 / 947 | 110 / 157 |
| no account gauge | 912 / 931 | 110 / 108 |
| no weighted governor token | 880 / 1,138 | 112 / 109 |
| none of the three | 706 / 686 | 96 / 98 |

Removing all three — which would remove the concurrency and rate limits
themselves — saves about 290 ns of about 1 µs. The feasible change,
co-locating the principal and account gauge counters, is worth at most the
larger single-gauge figure, 60–90 ns in this worst case and about half that in
the service, and those gauges carry GL-91's proof-backed activation handoff. The
remaining ~700 ns is the account state's and lease view's reference counts, the
lease debit and credit, and the request's own reservation: the request owns its
state across the body read, and its reservation must pin the lease it debited,
so those writes are the semantics, not overhead.

**Decision.** The layout stays. The lever that removes the whole cost is where
tasks run — one core per connection, which returns the rotating cost to the
pinned one — and that is the embedder's, documented in `docs/EMBEDDING.md`
("Keeping an account on one core"). What the library owes is that the pattern
not grow unseen: the rotating row is gated, with a ratio against its pinned
twin at ×12 (measured ×8.4–×9.5), so a change that adds several per-account
lines to admission fails the gate on any host.

### Allowances sized from the day's envelopes, in one pass

The GL-140 series failed its validating run on `usage_queue/reserve_record`
(×1.071 over 5%), a row it does not touch — the fifth series that day in which
each run tripped a *different* untouched row at the edge of its allowance. One
row at a time was whack-a-mole, so every row was sized in one pass from the
thirteen trusted full runs on comparable code (the GL-134, GL-137 and GL-140 series;
the usage-queue rows from GL-137 on). A row whose worst observation came within
80% of its allowance got that observation's ratio over its recorded mean, plus
three points, rounded up to the next 0.05. A reading above 1.5× the row's
median was treated as a one-off excursion and listed rather than absorbed. No
allowance was lowered and no recorded mean changed.

| Row | Runs | Range | Recorded | Allowance |
|---|---|---|---|---|
| `cost_table/quote` | 13 | 1.7 ns–1.9 ns | 1.7 ns | 0.05 → 0.15 |
| `cost_table/quote_4096_classes` | 13 | 1.4 ns–1.6 ns | 1.5 ns | 0.05 → 0.10 |
| `cost_table/quote_workload_2` | 13 | 3.2 ns–3.4 ns | 3.2 ns | 0.05 → 0.10 |
| `reservation/cancel_after_commit` | 13 | 42.0 ns–45.8 ns | 42.1 ns | 0.05 → 0.15 |
| `reservation/commit_split_race_contended_2` | 13 | 278.2 µs–307.9 µs | 282.0 µs | 0.10 → 0.15 |
| `reservation/commit_cancel_race_control_2` | 13 | 50.3 µs–69.9 µs | 58.4 µs | 0.20 → 0.25 |
| `admission/snapshot_lookup_moka_at_capacity` | 13 | 66.5 ns–85.6 ns | 70.2 ns | 0.20 → 0.25 |
| `admission/begin` | 13 | 17.0 ns–17.9 ns | 17.1 ns | 0.05 → 0.10 |
| `admission/full_check_balance_exhausted_strict` | 13 | 60.9 ns–64.7 ns | 61.2 ns | 0.05 → 0.10 |
| `admission/full_check_balance_insufficient_strict` | 13 | 60.9 ns–63.8 ns | 61.0 ns | 0.05 → 0.10 |
| `admission/full_check_lease_exhausted_elastic` | 13 | 116.9 ns–133.3 ns | 117.2 ns | 0.05 → 0.20 |
| `admission/commit_usage_overage_key_owned` | 13 | 113.8 ns–120.2 ns | 114.3 ns | 0.05 → 0.10 |
| `admission/commit_usage_leased_unattributed_owned` | 13 | 110.8 ns–115.8 ns | 111.2 ns | 0.05 → 0.10 |
| `admission/commit_usage_overage_unattributed_owned` | 13 | 113.7 ns–121.8 ns | 114.0 ns | 0.05 → 0.10 |
| `admission/commit_usage_fallback_key_split` | 13 | 155.6 ns–165.5 ns | 156.9 ns | 0.05 → 0.10 |
| `usage_queue/reserve_record` | 9 | 92.5 ns–103.4 ns (excursion 247.0 ns excluded) | 96.6 ns | 0.05 → 0.15 |
| `usage_queue/reserve_release_contended_8` | 9 | 29.5 ns–30.8 ns | 29.5 ns | 0.05 → 0.10 |

The discriminating gates for the changes this series landed are unaffected:
the lanes' `reserve_release_contended_8 / reserve_release` ratio (×3) and
200 ns threshold, and the rotating row's ×12 ratio against its pinned twin.

## 2026-09-24 — The contention signal covers every per-account retry loop (GL-139)

GL-134 counted lost compare-exchanges only in the lease debit loop. The admission
path has two more per-account retry loops, reached by fewer accounts:

- **The overage debit** (`AccountOverage::try_debit`), for elastic accounts
  with no lease able to fund a quote. Losses are counted in a register and
  added once per contended debit to a counter beside `spent` — the line the
  thread is already contending for.
- **The concurrency gauges** (principal and account `ConcurrencyCounter`),
  which every admission increments whether or not a limit is set (GL-91). The
  gauges live in the admission state, not beside the lease, so
  `try_increment` adds its losses to a register the caller owns and
  `acquire_concurrency` records the total on the account's `LeaseSlot` — one
  write, and only when there were any.

`LeaseSlot::contended_debits` became `contended_exchanges`, because gauge
acquisitions are not debits; `RuntimeReport::contention` and the example's
`/metrics` follow. `LocalLease::contended_debits` keeps its name: it is still
exactly the lease's debits. The signal remains a lower bound — the rate bucket,
reference counts and settlement use read-modify-writes that never fail — but it
now covers every per-account exchange that can observe a lost race.

*Tests:* `an_uncontended_overage_debit_records_no_contention`,
`contended_overage_debits_are_recorded_without_disturbing_spend`,
`concurrency_gauge_contention_reaches_the_account_total`,
`an_uncontended_admission_records_no_contention`.

### The one-pass allowance calibration, repeated after GL-139

The rule from GL-140, re-applied against GL-139's re-recorded baseline with its
three runs added: seventeen trusted full runs on comparable code (the GL-134,
GL-137, GL-140 and GL-139 series; the usage-queue rows from GL-137 on, the rotating
row from GL-140 on). Ten rows widen; none narrow; no recorded mean changes.

| Row | Runs | Range | Recorded | Allowance |
|---|---|---|---|---|
| `cost_table/quote_4096_classes` | 17 | 1.4 ns–1.6 ns | 1.4 ns | 0.10 → 0.15 |
| `cost_table/quote_workload_1` | 17 | 2.0 ns–2.1 ns | 2.0 ns | 0.05 → 0.10 |
| `snapshot/admit` | 17 | 1.1 ns–1.1 ns | 1.1 ns | 0.05 → 0.10 |
| `reservation/commit_split` | 17 | 67.2 ns–69.9 ns | 67.2 ns | 0.05 → 0.10 |
| `admission/snapshot_lookup_moka_at_capacity` | 17 | 63.4 ns–85.6 ns | 69.0 ns | 0.25 → 0.30 |
| `capacity/reserved_shared` | 17 | 128.1 ns–133.8 ns | 128.4 ns | 0.05 → 0.10 |
| `capacity/full_check_contended_8_distinct_accounts_uniform` | 17 | 908.7 ns–1.2 µs | 937.8 ns | 0.15 → 0.30 |
| `capacity/full_check_contended_8_distinct_accounts_reserved` | 17 | 834.9 ns–1.1 µs | 899.6 ns | 0.25 → 0.30 |
| `admission/commit_usage_leased_unattributed_split` | 17 | 145.4 ns–151.5 ns | 145.7 ns | 0.05 → 0.10 |
| `usage_queue/reserve_record` | 13 | 92.5 ns–142.1 ns (excursion 247.0 ns excluded) | 93.6 ns | 0.15 → 0.55 |

`usage_queue/reserve_record` is now bimodal: 92.5–106 ns in nearly every run,
then 142 ns once and 247 ns once (the latter above 1.5× its median, so listed
rather than absorbed). One thread handing events to the writer task depends on
where the OS places the writer's thread, so as a per-row gate it is weak. The
lanes' witness is unaffected: `reserve_release_contended_8 / reserve_release`
at ×3 and its 200 ns threshold have held at ×1.0–×1.1 and 29.5–30.8 ns in every
run.

## 2026-09-25 — Allowances are recorded from run history, not typed (GL-141)

On 2026-09-24, 18 of 32 trusted full runs on the controlled host failed on rows
their change did not touch, and allowances were re-sized by hand three times
(GL-125, GL-140, GL-139), each time from the same evidence: a row's own run-to-run
spread exceeding an allowance recorded without looking at it. Host drift was
not the cause — normalising each failing row by its run's median drift would
have rescued one of the eighteen runs; most failed at drift ≈ ×1.000 — so the
fix is per row.

**The history.** Every run that deposits a recording sample now also keeps it
in `target/perf-history`, the newest 40 runs, published with the samples'
staged, exclusive write and pruned by run id. `--fresh-samples` clears only the
series being recorded, so the window spans series and revisions.

**The derivation.** Grouping by revision and dividing each run by its own
revision's median is what lets history from different code be pooled: GL-132
moved a row by ×4 between revisions, and a raw envelope across them would have
put that move into the row's noise. Excursions above ×1.5 are listed and set
aside — the bimodal `usage_queue/reserve_record` read ×2.4 and ×1.5 once each
against 92–106 ns everywhere else — and a row needs six believed runs across
two revisions before its spread is believed. The allowance never narrows a
carried one, because narrowing claims the gate is tighter than evidence has
shown; that remains a deliberate edit. The rule is the one applied by hand in
GL-140 and GL-139, now applied by the tool at every recording.

**Wide rows.** A row above 0.30 is a weak per-row gate. Every full run now says
which rows those are, in its output and its report, so a reviewer reads the
same-run ratio or absolute threshold that actually guards them.


## 2026-09-25 — The stock server issues from its manifest, and binds policy by key (GL-143)

GL-121 left `tollgate-server` answering `501 issuance-unsupported`, so a
deployment that administers accounts had to write its own `main` around
`serve`. The manifest now carries
an optional `issuer` entry, file-backed like bearers. A second gap on the same
path shipped in the same change: once a key is issued, its snapshot could be
published only by principal, and every admin response withholds the principal.
Operator tooling would have needed the issuer secret to re-derive it, which is
exactly the authority the issuer entry keeps inside the server.

**The HMAC key is the file's text, not its decoded bytes.** Decoding 64 hex
characters to 32 bytes is the tidier definition, and both carry 256 bits. But
verifiers configured from the same value key HMAC with its text, and a decoded
definition would have made one stored secret produce two different keys, with
every issued credential failing with 401 and no error anywhere. That failure is
silent, so the definition follows the verifiers. With that
choice, uppercase must be refused rather than tolerated: under text keying it
is a different key that looks like the same secret.

**An issuer change is deferred, not refused.** The first design refused a
reload whose issuer differed, following the TLS-mode refusal. Review caught
the cost. The issuer file feeds the reload fingerprint, so a stray edit or a
half-finished rotation would fail *every* reload, freezing certificate and
bearer rotation behind a warning every five seconds until someone restarted.
Nobody edits the TLS mode by accident, but a secret file is edited during every
rotation. So the loader installs the rest of the generation, keeps the live
issuer, and reports the pending change once per distinct staged value, plus
`issuer_change_pending` for tests and operators. A malformed issuer still fails
the load, like any other malformed file.

**Binding is a store operation, not a lookup.** A principal lookup by
`(account, key)` followed by the existing publish would have been
check-then-act: a revocation between the two leaves a positive snapshot for a
retired credential. Instead, `KeyDirectory` gains `publish_key_snapshot` and
`remove_key_snapshot`. Resolution, the retirement check and publication share
one guard (MemoryStore) or one transaction (PostgreSQL, credential row
`FOR SHARE` against revocation's `FOR UPDATE`). The existing publication body
was factored into `publish_in_tx` rather than copied, so both entry points
share one set of ledger checks. Revocation still leaves the snapshot in place,
which is safe because the key leaves the projection. The documented procedure
is revoke-then-withdraw. Folding withdrawal into revocation is a possible
follow-up.

**The disclosed secret was not the digested one.** Issuance disclosed the
secret as 64 hex characters but digested the 32 bytes they encode. The
reference embedder strips `Bearer ` and verifies what remains, so no
server-issued credential could ever verify through it. Existing tests hid this,
because each handed the verifier the raw `MintedKey::secret` rather than the
disclosed text, and the first draft of this change's binary test decoded the
hex to make it pass. The contract is first violated in `mint`, so it is fixed
there. The credential is minted in its textual form, and the digest covers that
text. Disclosure, presentation and verification now handle one byte string
with no encoding step between them, and `CredentialIssuer` states that
contract for embedder issuers. The server refuses a non-text secret before
storing it. Credentials issued by earlier releases verify only when decoded,
so they must be re-issued.

**Lock order was audited, not assumed.** The key path locks credential, then
account, then snapshot. A cycle needs a path that holds an account lock and
waits on a credential row lock. Searching every function that touches
`tollgate_credential_keys` found none that does. Revocation takes no account
lock, issuance only inserts new rows, and ingest reads credentials with a plain
`SELECT` and writes activity rows whose foreign key takes `FOR KEY SHARE`,
which `FOR SHARE` does not block. The audit is recorded on `lock_account_key`,
and a status-change race test witnesses it.

## 2026-09-28 — A scoped role for self-service provisioning (#39)

A deployment with self-service signup gave its internet-facing account service
an `operator` credential, because no other role could create accounts or issue
keys. Compromising that service (RCE, SSRF, a bad image) then meant unlimited
deposits, closing any account, granting `Assured`, any budget and
principal-level snapshots, on every account. The only mitigation was reading
the audit log afterwards. The account service needs ten calls with narrow
arguments. It never deposits, suspends, closes, grants `Assured` or touches
`/snapshots/{principal}`.

**A fixed role, not permission lists.** `provisioner` is a third disjoint
`Role` with a hard-coded scope. Per-identity action lists would be more
flexible, but they are a configuration language whose every combination needs
a test, and the one known consumer needs exactly one shape. If a second shape
appears, it can be designed then.

**Routes split by who may call them.** The admin router became two routers:
`[operator]` for deposit and principal snapshots, `[operator, provisioner]`
for the rest. A provisioner therefore cannot reach a funding route even through
a handler bug. `Authorization.roles` is a role set rather than one role, and
the route-table check (`tollgate-repo-check`) derives and renders the set, so
`docs/HTTP_API.md` cannot drift from it. Argument limits have to live in the
handlers, because the router cannot see a body.

**The store owns the shape of a provisioner's account.**
`AdminStore::create_provisioned_account` takes only an id and writes zero
balance, `Suspended`, `BestEffort` and `origin = Provisioner`. The HTTP layer
still refuses a non-zero balance or a non-`Suspended` status with `403`, rather
than silently ignoring them. But a handler bug cannot produce a funded,
active or assured account, because the call that creates one has no parameter
to carry it.

**Provenance, not an actor log.** The account-scope and operator-hold rules
need two facts: which kind of authority created the account, and which kind
set its status. These are stored as `origin` and `status_set_by`
(`AdminAuthority`), not as identity names. A name is an audit concern that the
HTTP boundary already logs; the *kind* is what a rule is decided on, and the
store has no business knowing deployment identities.

**Why a pre-read for scope, but a transaction for the hold.** `origin` is
written once and never updated. A handler may therefore read it in
`AdminIdentity::check_account` and then run a separate write, with no window
in which the fact could change. `status_set_by` is mutable, and the hold
exists precisely to win a race: an operator suspending while the customer
retries signup. So the check is inside the store, in `activate_provisioned`,
under the row lock or mutex that serializes every status write. Whichever
lands second sees the other's author. Every `set_account_status` records the
operator as author, repeats included, so an operator re-suspending an account
already suspended by creation establishes a hold.

**Existing accounts are operators'.** Migration 0020 defaults both columns to
`Operator`. That is the only safe backfill: every existing account was created
with an operator credential, and the opposite default would give a newly
deployed provisioner every account in the database. The cost falls on
migration. A signup service moved from an operator credential to a provisioner
cannot administer the accounts it created before the move; they stay with
operator tooling or are re-created. Tollgate cannot tell, after the fact,
which operator-created accounts a signup service made, so it does not guess.

**The budget ceiling is per identity and required.** A periodic allowance
funds admission, so an unbounded one is a deposit renamed. The limit belongs
to the credential (`max_budget_allowance` on the manifest entry), because it
is a statement about how far that deployment trusts that service. It is
required, and refused on other roles, so a forgotten ceiling fails
configuration instead of meaning "unlimited". `ProvisionerLimits` is carried
inside the identity's grant, which makes a provisioner without a ceiling
unrepresentable.

**Elastic is refused; the rest of the snapshot is a known gap.** A key
snapshot carries policy the store validates only against the ledger (status,
class). `Elastic` enforcement extends unfunded overage credit, so a provisioner
may publish only `Strict`. The cost table, limits and permissions remain
caller-supplied: a compromised provisioner can still make its own accounts
cheap or unthrottled, though not funded. Closing that needs operator-approved
policy templates. That is a design of its own, tracked in #43, and
`INVARIANTS.md` 41 states the gap rather than overclaiming.

**Refusals are audited where they happen.** A role mismatch is refused in
`authorize` and logged there with the method and route template, since no
handler runs. An argument or scope refusal is logged by
`AdminIdentity::refuse` before any store call, as one `refused` event with no
`started`, because nothing was attempted. The operator hold is found inside
the store transaction, so it arrives as a `failed` event with code
`operator-hold`. All audit events now carry the role. Before this change a
role-mismatch `403` left no trace.

**Compatibility.** The library change is breaking and ships as a minor bump
under 0.x. `AdminStore` gains two required methods, `KeyDirectory` gains
`publish_key_snapshot_next`, `AdminState` two fields,
`SetStatusError` two variants, `AccountView` two fields, and
`ControlIdentity::new` refuses `Role::Provisioner`. Manifests, wire requests
and HTTP behaviour for `instance` and `operator` are unchanged.
`AccountResponse` gains `origin` and `status_set_by`, which default to
`Operator` when absent, so a new client reads an old server. The new codes,
`403 account-not-provisioned` and `403 operator-hold`, are additive. Rollout:
schema, then every server instance, then provisioner credentials. A server that
predates the role rejects a manifest naming it, which is the safe failure.


### Provisioner generations cannot consume operator transition headroom

Review of #44 found that the new provisioner route reused the operator's
key-snapshot write verbatim. Publishing a valid Strict snapshot at `u64::MAX`
(memory) or `i64::MAX` (PostgreSQL) exhausted the counter in one request.
Suspension, closure and capacity-class changes all republish at generation + 1,
so they rolled back on overflow. The existing hold tests covered status authors
but published no snapshot, and publication tests used small generations. The
Lean hold model likewise assumed the status transition could complete.

`KeyDirectory::publish_key_snapshot_next` now owns successor allocation for
provisioners: 1 for an absent principal, otherwise the locked live or revoked
watermark plus one. It disregards the submitted generation, uses checked
arithmetic, and publishes the same value in storage, receipts and pushes.
Memory holds its existing mutex; PostgreSQL uses the existing snapshot write
lock and, after a concurrent first-insert conflict, allocates against the
winner. The credential/account/snapshot lock order stays unchanged. Allocation
adds a constant amount of control-plane work to the existing indexed write;
there is no request-path change or new dependency.

The sibling audit covered both publication routes and all status/class
restamping paths. Principal publication is operator-only, and operator key
publication retains its existing generation/no-op contract. Withdrawal keeps
the current generation and cannot jump it. All provisioner key publications
select the new method. No other provisioner operation accepts a generation.

Mirrored tests now cover extreme supplied values, tombstones, concurrent first
and subsequent writes, actual finite-width exhaustion, validation and all
three operator transitions. An HTTP regression tests both domain maxima.
The bounded successor model in `ControlPlane.lean` proves exact increment,
monotonicity and overflow refusal; it does not prove SQL locking or HTTP
dispatch, which the integration tests witness.

This adds one required `KeyDirectory` method to the already-breaking library
change. Wire DTOs and operator behavior are unchanged. Provisioner generations
are store-assigned and retries are new publications; callers must serialize
policy updates whose order matters. No further schema migration is needed.
Deploy every server with this fix before enabling provisioner credentials;
rolling back to a build with the vulnerable provisioner route reopens the gap.
The fix does not reset existing operator-selected high watermarks. A deployment
that exercised the unreleased vulnerable role must retire an exhausted
credential, withdraw its snapshot, and issue a fresh key/principal before
publishing again. Never lower a stored generation to recover it: instances
retain the higher watermark.

## Account key listing preserves owner absence (GH-41)

The credential directory previously returned only a vector or `StoreError`.
Both stores filtered keys by account, so a missing account looked exactly like
an existing account with no credentials. Tests covered ordering, paging,
expiry and revocation but never the missing-owner boundary; the HTTP handler
therefore returned `200` with an empty page for a mistyped account ID.

`KeyDirectory::account_keys` now returns the existing `KeyError` vocabulary,
including `UnknownAccount`. Memory tests account existence while holding the
same lock used to assemble the page. PostgreSQL anchors one SELECT on the
account and left-joins a bounded lateral credential page: zero rows means no
account, while a NULL key means an existing account's empty page. Both facts
come from one statement snapshot, including when creation overlaps the read.
A creation committed after that snapshot becomes visible on the next read;
a completed creation cannot be hidden by a stale HTTP preflight. Separate
cursor query shapes preserve the account/key index range and page bound.

The sibling caller is credential revocation. It intentionally retains its
credential-scoped `unknown-credential` response for an absent owner. Other
callers propagate the typed refusal; database corruption remains `Storage`.
Shared backend scenarios and an HTTP regression cover absence, creation,
empty pages with and without cursors, and exhaustion of an existing key page.

Callers relying on `200 []` for a missing account must handle
`404 unknown-account`. Custom Rust directory implementations must change the
return error type from `StoreError` to `KeyError`. Deploy updated clients before
the server where that behavior matters. There is no schema migration,
authentication change, extra round trip, or request-path work.

## Body-limit advice belongs to the route (GH-42)

The shared JSON rejection converter appended the usage-batch event cap to
all `413 batch-too-large` responses, including snapshot publication and account
creation. Its existing regression exercised only ingest, where that advice was
correct, so the misuse on other routes went unnoticed.

The converter now supplies a route-neutral title. The ingest handler accepts
the extractor result and adds its event cap only to a body-size rejection,
before any store call. Snapshot and default-limit route tests pin the neutral
message; ingest tests retain the cap and keep malformed JSON distinct.
Status, code, byte limits, event limits, authentication and schemas are unchanged.
Only human-readable error advice changes; there is no migration or request-path
cost.

## Deposit overflow is a permanent refusal (GH-40)

Deposit arithmetic already refused overflow atomically, but memory wrapped its
checked-add failures in `Storage` and PostgreSQL treated both input conversion
and SQL arithmetic failures as storage outages. The server therefore returned
`503 storage`, whose retry advice can never repair a full lifetime deposited
counter. The existing mirrored test asserted only that the deposit failed and
moved neither column; it did not distinguish domain refusal from outage.
The HTTP diagnostics fixture also used overflow as a stand-in for an outage;
it now injects a storage failure explicitly, preserving its 503 privacy checks.

`AllocateError::BalanceOverflow` now owns that distinction in both backends.
Memory checks both sums before applying either. PostgreSQL rejects an amount
outside nonnegative `BIGINT`, and maps only numeric-value-out-of-range SQLSTATE
22003 from the deposit UPDATE to the permanent refusal. Other database errors
retain `Storage`; the single-statement atomicity and locking are unchanged.
The sibling lifetime-total overflow uses the same classification as top-up
balance overflow. Lease settlement and other operations' arithmetic failures
are outside the deposit contract and retain their existing classifications.

The wire code `422 balance-overflow` is additive and the request/receipt shapes
are unchanged. Rust exhaustive matches need the new variant; existing metric
indices stay fixed and the new label is appended. Upgrade clients that classify
problem codes before the server so they recognize the permanent refusal.
There is no schema migration or request-path work. Boundary tests cover exact
fit, both overflowing counters, repeat refusals and PostgreSQL's input domain;
the HTTP regression pins status and code as well as unchanged accounting.

## Mutation diff runs and PostgreSQL requirements

The diff gate intentionally disables PostgreSQL when no backend files change,
to let independent mutation workers run without sharing a database. After the
credential expiry integration test began enforcing `TOLLGATE_REQUIRE_PG`, the
gate still removed only the URL, leaving a contradictory required-but-unavailable
backend. Server-only changes therefore failed the unmutated baseline. The gate
now clears both variables together only in that intentional optional-backend
branch; backend changes and full sweeps retain the PostgreSQL requirement.
