# tollgate invariants

A change that violates one of these is a defect even when every test passes.
Each invariant names the test(s) that enforce it; a new invariant is not "done"
until it has one.

1. **Bounded spend.** Total committed usage across all instances never exceeds
   the units allocated to the account, under any interleaving of concurrent
   clients. Enforced by fenced leases: units are spent only from a lease, and a
   lease's units were atomically debited from the account at allocation.
   *Tests:* `no_double_spend_across_instances` (memory and Postgres variants),
   `tollgate-core` reservation proptests.

2. **Zero charge before execution.** A reservation that never reaches
   `commit_at_execution_start` charges zero units, and its units return to the
   local lease. Dropping a pending reservation is a release, not a leak.
   *Tests:* `reservation::tests::{drop_releases_pending, cancel_charges_zero}`.

3. **Atomic commit-vs-cancel.** Commit and cancel race on a single atomic
   transition; exactly one wins. A cancelled reservation can never later
   commit; a committed reservation reports its full charge to a late
   canceller. *Tests:* `reservation::tests::commit_cancel_race_one_winner`.

4. **Fencing is absolute.** A lease holder with a stale fencing token is
   rejected by the allocator, by renewal, and by the usage sink — regardless
   of timing. Fencing tokens are strictly monotonic per account.
   *Tests:* `fenced_out_holder_rejected` (store suites).

5. **Fail closed, zero I/O.** Unknown principal, suspended/closed account,
   expired snapshot, missing permission, exhausted or expired lease: all deny
   locally. The request path performs no database, file, lock-file, or network
   access — not even on a miss. *Tests:* `tollgate-core` deny-path unit tests;
   admission-crate miss tests assert no store calls from the request path.

6. **Foreground isolation.** Lease refill and snapshot replacement never block
   an in-flight request. *Tests:* client refill tests; `lease/refill_offpath`
   bench assertion.

7. **Idempotent accounting.** Replaying a usage batch (same request IDs) never
   double-bills. *Tests:* `usage_replay_is_idempotent` (store suites).

8. **Accounting backpressure sheds.** When the usage queue is full, new work is
   refused with zero units charged. Usage events are never silently dropped
   and enqueue never blocks unboundedly. Shutdown flushes in configured-size
   batches; any event still undeliverable after the bounded final retries is
   counted in `WriterStats::lost`. *Tests:* client writer overflow tests,
   `shutdown_flushes_in_configured_batch_sizes`,
   `shutdown_during_outage_terminates_and_reports_loss`.

9. **Crash leak is bounded by TTL.** A crashed lease holder strands its unspent
   units only until the lease TTL expires, after which the allocator reclaims
   them. *Tests:* `expired_lease_units_reclaimed` (store suites).

10. **Ready means currently admissible.** An instance reports ready only while
    every tracked principal has a fresh positive or negative resolution, its
    lease remains inside the local usability window, and its snapshot and
    refill/accounting tasks are alive. Readiness falls again on exhaustion,
    expiry, or task exit;
    fail-closed correctness must not masquerade as availability. *Tests:*
    `initial_load_gates_readiness_and_installs`,
    `readiness_falls_when_snapshot_expires_during_outage`,
    `readiness_falls_if_refresh_hangs_across_snapshot_expiry`,
    `readiness_falls_when_background_planes_stop`.

11. **Checked arithmetic only.** Cost and lease arithmetic never wraps; any
    overflow is an explicit error that denies (fail closed), never a wrap to a
    small charge. *Tests:* `tollgate-core` proptests.

12. **No commit outside the usability window.** A lease is locally usable
    until `expires_at - safety margin`; both debits and commits stop there,
    and the allocator reclaims only after `expires_at + grace` (accepting
    releases and late usage through the window). Work committed inside the
    window therefore always has margin + grace to be flushed and billed;
    work cannot commit against capacity the allocator may have re-granted.
    *Tests:* `reservation::tests::{commit_after_window_closes_releases_for_zero,
    safety_margin_closes_window_before_expiry}`,
    `reclaim_waits_for_grace_and_release_works_within_it` (both store suites).

13. **A committed charge is always emitted.** Committing through
    `ChargeGuard` binds the billing event to the pre-reserved queue permit;
    normal completion, early return, panic unwind, and task abort all
    enqueue it. A spent lease with no billing event requires losing the
    whole process. *Tests:* `panic_after_commit_still_bills`.

14. **Account creation is never destructive.** Recreating an existing
    account is a surfaced `AlreadyExists` in every backend — never an
    overwrite, never a silent no-op. *Tests:*
    `recreate_account_is_refused_and_nondestructive` (both store suites).

15. **Authorization generations never move backward.** Positive snapshots and
    revocation tombstones retain the highest generation observed. A delayed
    positive at or below a tombstone cannot resurrect a principal, and a
    delayed older tombstone cannot revoke a newer positive snapshot. The
    source stores tombstones durably so the rule survives instance restarts;
    evicting a request-visible entry does not evict its generation watermark.
    *Tests:* both snapshot-map contract tests,
    `negative_eviction_preserves_generation_monotonicity`,
    `revoked_generation_rejects_replay_after_visible_entry_is_evicted`,
    `revocation_reaches_instances_via_refresh`,
    `snapshot_publish_fetch_and_push`, and
    `snapshot_publish_fetch_and_generation_monotonicity`. *Proof:*
    `formal/lean/Tollgate/SnapshotCache.lean`.

16. **Unsafe timing configuration never starts.** Nonpositive lease TTLs,
    polling/refresh/reclaim intervals, negative safety margins or reclaim
    grace, and internally inconsistent lease thresholds are rejected before
    allocation or task startup. *Tests:*
    `nonpositive_lease_ttl_is_rejected_without_debiting` (both stores),
    `invalid_lease_manager_durations_are_rejected`,
    `invalid_snapshot_manager_intervals_are_rejected`,
    `invalid_grant_policy_is_rejected`, and
    `zero_reclaim_interval_is_rejected`.

17. **Negative caching is bounded and self-healing.** The ArcSwap map retains
    at most its configured count of request-visible negatives, removes
    expired negatives during control writes, and evicts the earliest expiry
    first. Expiry schedules a targeted source pull even when push is
    unavailable and the full refresh interval is much longer; source errors
    retry with backoff. Pruning visible negatives never weakens invariant #15.
    *Tests:* `arc_swap_negative_cache_is_bounded_and_evicts_oldest_deadline_first`,
    `arc_swap_control_write_drops_expired_negatives`,
    `many_unknowns_leave_only_the_configured_number_visible`, and
    `http_negative_ttl_refetches_without_push`, and
    `negative_ttl_retry_is_backed_off_and_recovers_without_push`.

Ledger roles (context for 1 and 7): leases **bound** spend; usage events **are**
the billing record; reconciliation compares the two and steady-state drift is
zero.
