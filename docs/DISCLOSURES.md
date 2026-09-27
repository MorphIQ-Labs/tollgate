# Technique disclosures

Prophetizo LLC, which publishes Tollgate as MorphIQ Labs, does not seek patent
protection for the techniques below. This document describes them so that
each is public, and citable as prior art, from the date this file is first
published. The source, `INVARIANTS.md`, and the Lean model under
`formal/lean/Tollgate` are the complete disclosure; this index names each
technique and points to where it is implemented, specified, and checked.

The primitives involved are established: token buckets, quota leasing,
fencing tokens, TTL leases, striped counters, two-phase reservation, and
idempotent event ingestion. What is disclosed here is how Tollgate composes
them. The `MQL-INV` identifiers are stable references used by MorphIQ Labs'
invention index.

Invariant numbers refer to [`INVARIANTS.md`](../INVARIANTS.md). Lean files are
under [`formal/lean/Tollgate`](../formal/lean/Tollgate); they check the named
properties of a model, not the Rust implementation.

## MQL-INV-030 — Execution-start commit of locally leased reservations

Admission debits a lease held locally by the instance, but the charge is only
pending. Execution start commits the full quote whatever the outcome (success,
failure, or timeout); anything that ends the request before execution returns
the units at zero charge. Commit and cancel race on a single compare-exchange
of one phase word, so exactly one wins, and `split` lets another task cancel
through a `CancelHandle` without a lock. A lease is usable only until
`expires_at − safety_margin`, and the allocator reclaims it only after
`expires_at + grace`, so committed work always has margin plus grace to be
billed.

- Code: `crates/tollgate-core/src/reservation.rs` (`Reservation`,
  `commit_at_execution_start`, `cancel`, `split`, `CancelHandle`);
  `crates/tollgate-core/src/lease.rs` (`usable_until`);
  `crates/tollgate-store/src/traits.rs` (`GrantPolicy::reclaim_grace`)
- Invariants: 2, 3, 12
- Lean: `CommitFallback.lean`, `LeaseTiming.lean`

## MQL-INV-031 — Commit-time overage fallback with guarded publication

Under `EnforcementMode::Elastic`, a leased reservation whose lease window
lapsed between admission and execution start settles against a per-account
overage counter in one phase transition (`PENDING_LEASE → COMMITTED_OVERAGE`),
never a release followed by a second reservation. A tentative overage debit is
taken before the claim and the lease receipt is refunded only after a winning
claim; the guard's `Drop` returns an unresolved tentative debit. A publication
marker makes the interval between the phase word and the occupancy word
observable, which yields three distinct refusals: `OverageCapExhausted`,
`OverageCapTemporarilyExhausted`, and `OverageCommitInProgress`. The overage
cap is one unsharded per-instance counter.

- Code: `reservation.rs` (`commit_after_lapse`, `CommitFunding::OverageFallback`);
  `lease.rs` (`AccountOverage::debit_tentatively`, `TentativeOverage`,
  `OverageCommitPublication`)
- Invariants: 1, 2, 3, 12
- Lean: `CommitFallback.lean`, `OveragePublication.lean`

## MQL-INV-032 — Funding-conservation ledger with overage and expiry terms

Every account ledger satisfies, with checked arithmetic,
`deposited + overage_recorded == balance + active lease grants + settled usage + settlement loss + expired`.
Leases bound spend and usage events are the billing record; overage is a
funding term recorded in the same transaction as its usage, and expired
allowance and settlement loss are sinks. One equation therefore spans
admission-side leased capacity and billing-side settled usage. The memory and
PostgreSQL backends are held to it by mirrored tests, and a reconciliation
query checks it on a live store.

- Code: `crates/tollgate-store/src/traits.rs` (`Conservation`, `Conservation::holds`);
  `crates/tollgate-store/src/memory.rs`; `crates/tollgate-store-postgres/src/lib.rs`
- Invariants: 1, 7, 9, 11, 28
- Lean: `Conservation.lean`

## MQL-INV-033 — Forfeit-on-reclaim lease settlement with straggler conversion

A lease whose holder never released it is settled by a bounded expiry sweep
after `expires_at + grace`. The sweep credits nothing back: the whole
unreported remainder becomes provisional settlement loss. Usage for that lease
that arrives later, from a holder that outlived an outage, converts loss into
billed usage and can never exceed it. Only an explicit, fenced release returns
units. The sweep settles the oldest due leases first in bounded batches.

- Code: `reclaim_expired_batch` in `crates/tollgate-store/src/memory.rs` and
  `crates/tollgate-store-postgres/src/lib.rs`; `traits.rs` (`ReclaimedLease`,
  `ReclaimBatch`)
- Invariants: 4, 9, 12
- Lean: `Conservation.lean` (forfeit and straggler lemmas)

## MQL-INV-034 — Atomic tail-lease consolidation with demand-proven growth

A holder's small remaining grant and the ledger remainder are exchanged in one
allocator transaction: the unspent units are returned and a replacement is
granted against the restored balance. The grant policy's shrink cap becomes a
floor equal to the credit actually restored. The local lease records the
largest quote it refused for lack of units, the refill plane forwards it, and
the replacement grows to that quote only when the restored balance can fund
it. At a budget-period boundary the allowance share of the returned units
expires instead of being restored.

- Code: `traits.rs` (`GrantPolicy::consolidation_grant`, `LeaseAllocator::consolidate`);
  `lease.rs` (`largest_refused_quote`); `crates/tollgate-client/src/lease_manager.rs`;
  HTTP `POST /v1/leases/consolidate`
- Invariant: 1
- Lean: `Conservation.lean` (consolidation lemmas)

## MQL-INV-035 — Ledger-attested exhaustion evidence for local refusals

Only the allocator's locked ledger may certify that an account's funding is
exhausted. Every grant and refusal carries a `BalanceShortfall
{ remaining, period_end }`, an upper bound on true remaining funding. The
instance publishes it into the account's lease slot bound to an identity
epoch, so a later grant or funding change invalidates late responses.
Admission consults it only after local lease funding and any elastic fallback
fail, with one atomic deadline read and a sequence-guarded value, and then
refuses with a non-retryable `BalanceExhausted` or `BalanceInsufficient`. An
empty local lease on its own never produces those refusals.

- Code: `crates/tollgate-core/src/budget.rs` (`BalanceShortfall`);
  `crates/tollgate-admission/src/state.rs` (`LeaseSlot`, `FundingAttempt`)
- Invariant: 1
- Lean: `BalanceExhaustion.lean`

## MQL-INV-036 — Exact-partition sharded lease counters with refund receipts

An opt-in instance-local layout splits one grant, and its low-water mark,
exactly by quotient and remainder across cache-line-aligned counters selected
by a sticky per-thread locality. A debit tries its own shard, then takes a
whole debit from a sibling, and fragments only when necessary; fragmentation
returns a fixed-size receipt carrying the exact total and one refund shard,
with no allocation. Sharding never creates capacity and never strands a
positive aggregate at exhaustion. The per-account overage cap stays one
unsharded counter, so the cap is not multiplied by the shard count.

- Code: `lease.rs` (`LeaseShard`, `LeaseDebit`); `crates/tollgate-core/src/sharding.rs`
  (`LocalSharding`); [`LOCAL_SHARDING.md`](LOCAL_SHARDING.md)
- Invariants: 1, 40
- Lean: `LeaseShards.lean`

## MQL-INV-037 — Generation-ordered revocation tombstones

The snapshot source answers `Present`, `Revoked(generation)`, or `Unknown`
(HTTP 200, 410, 404). A per-principal watermark keeps the highest generation
and why it exists: a generation the source revoked refuses that same
generation back, while an observed positive refuses only strictly older ones,
so an absence never becomes a revocation. Tombstones are durable and survive
eviction of the request-visible entry. Reclaiming bounded local history
invalidates outstanding reads through a fence, and reopening requires a fresh
authoritative read. A staged request keeps the snapshot generation it began
with.

- Code: `crates/tollgate-admission/src/history.rs` (`GenerationHistory`,
  `accept_revoked`); `crates/tollgate-admission/src/maps.rs`
- Invariants: 15, 17, 26
- Lean: `SnapshotCache.lean`, `SnapshotHistory.lean`

## MQL-INV-038 — Admission-bound billing slot for committed charges

A usage-queue permit is reserved at admission, before the charge exists, and
travels with the request through `ReadyToStart` to `Committed`. The `Drop` of
`Committed` records the billing event before releasing capacity, so normal
return, early return, panic unwind, and task abort all emit it. When no permit
is available the request is refused at zero charge. The queue is partitioned
into lanes, and shutdown drains under a total deadline, counting undelivered
events and unresolved permits in counters that outlive the writer task.

- Code: `crates/tollgate-admission/src/engine.rs` (`ReadyToStart`, `Committed`
  and its `Drop`); `crates/tollgate-client/src/usage_writer.rs` (`UsagePermit`,
  `WriterStats`)
- Invariants: 7, 8, 13

## MQL-INV-039 — Two-bucket periodic budgets settled at lease release

An account balance is split into the current period's allowance and manual
top-ups, spending allowance first. The store crosses a period boundary exactly
once under a guarded row lock: it deposits one allowance with no backlog,
expires the old remainder, and advances the period marker. Active leases keep
serving to their TTL, so the boundary causes no admission gap and the request
path reads no clock for policy. The boundary takes effect at lease release:
the unspent allowance share of an older-period lease expires and the top-up
share returns. Instances receive a store-stamped `BudgetView` that never
decides admission.

- Code: `budget.rs` (`BudgetView`); `roll_due_periods` in `traits.rs`,
  `memory.rs`, and `crates/tollgate-store-postgres/src/lib.rs`;
  `crates/tollgate-client/src/period_roller.rs` (`PeriodRoller`)
- Invariant: 28
- Lean: `PeriodRoller.lean`, `Conservation.lean`
