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
   of timing. Fencing tokens are strictly monotonic per account. A stored
   fence outside the token domain is surfaced as a storage error, never
   aliased to fence 0 — aliasing would misattribute corruption to the caller
   and break the monotonic audit trail. A holder that learns it is fenced
   while releasing clears its own slot, so it stops serving immediately
   rather than at its next acquire.
   *Tests:* `fenced_out_holder_rejected` (store suites),
   `fenced_release_clears_the_slot`;
   `acquire_surfaces_negative_stored_fence` and
   `release_and_ingest_surface_negative_stored_fence` (Postgres suite; the
   memory backend stores tokens as `u64`, so the corrupt state is
   unrepresentable there).

5. **Fail closed, zero I/O.** Unknown principal, suspended/closed account,
   expired snapshot, missing permission, exhausted or expired lease: all deny
   locally. The request path performs no database, file, lock-file, or network
   access — not even on a miss. A deny also says which kind it is: a request
   that can *never* be admitted under the account's current schedule — its
   quote exceeds the whole burst — is `UnpriceableUnderLimits`, never
   `RateLimited`, so a caller is never told to retry something that cannot
   succeed and an operator is never shown throttling for a misconfiguration.
   *Tests:* `tollgate-core` deny-path unit tests; admission-crate miss tests
   assert no store calls from the request path;
   `batch_cap_above_burst_is_unpriceable_not_throttled`,
   `zero_burst_denies_every_priced_request`,
   `quote_beyond_the_bucket_domain_is_unpriceable`, and
   `rate_limiter_weights_by_cost` (the converse: genuine throttling stays
   `RateLimited`).

6. **Foreground isolation.** Lease refill and snapshot replacement never block
   an in-flight request, and the request path never waits on the plane that
   refills it. A draining lease *tells* the refill task rather than being
   discovered by it — the debit that crosses low water raises a signal whose
   implementation is contractually non-blocking — so refill latency is no
   longer bounded below by the poll interval, and a funded account is not
   refused between ticks. Cold start and usability-window rollover keep the
   interval as their backstop, having no debit to announce them.
   *Tests:* `refill_begins_on_the_crossing_debit_not_the_next_tick`,
   `a_burst_across_a_rotation_never_denies_a_funded_account`,
   `refill_installs_lease_on_cold_start`,
   `usability_window_rollover_returns_unspent_capacity`,
   `the_crossing_debit_raises_the_signal`,
   `a_lease_signals_at_most_once_however_long_it_drains`, and the
   `RefillRequests` handoff tests.

7. **Idempotent accounting.** Replaying a usage batch (same request IDs) never
   double-bills. *Tests:* `usage_replay_is_idempotent` (store suites).

8. **Accounting backpressure sheds.** When the usage queue is full, new work is
   refused with zero units charged. Usage events are never silently dropped
   and enqueue never blocks unboundedly. Shutdown closes the queue (new
   reservations refuse from that instant), then drains with real receives
   until every outstanding permit resolves by sending or dropping, bounded by
   the configured `shutdown_drain_deadline` — which governs the ingest calls
   the drain makes as well as its receives, so the bound is the drain's total
   wall clock. It flushes in configured-size batches, counts any event still
   undeliverable after the bounded final retries in `WriterStats::lost`
   (a timed-out ingest is undelivered, never a silent success), and reports
   permits still outstanding at the deadline in `WriterStats::unresolved` —
   a deadline expiry is never a clean flush. The reporting holds across the writer's own death: every
   charge that enters the queue is counted until it is given a billing
   outcome, in a counter outside the task, so a writer that panics or is
   aborted returns `WriterShutdownError` carrying a lower bound on the
   charges it was holding. A zeroed `WriterStats` is never returned for a
   task that did not report one — and, since the type is deliberately not
   `Default`, cannot be conjured from a failure. Those numbers are readable at
   any time, not only from a graceful shutdown — which is precisely the case
   where loss is least likely. They live in counters outside the task, and
   `shutdown` returns a *snapshot of those same counters* rather than a
   parallel tally, so the running totals and the final report cannot disagree.
   Queue depth against its capacity makes backpressure visible before it
   sheds; sheds are counted at `try_reserve`, the only place a refusal can
   happen, so no embedder can forget to; and the time the sink last answered
   separates a quiet writer from an unreachable one — a distinction `lost`
   cannot make while the process runs, since the steady-state path retries
   forever and declares loss only at the final flush. *Tests:* client writer
   overflow tests,
   `shutdown_flushes_in_configured_batch_sizes`,
   `shutdown_during_outage_terminates_and_reports_loss`,
   `reserve_fails_once_shutdown_begins`,
   `shutdown_waits_for_outstanding_permit`,
   `late_permit_drop_completes_drain`,
   `drain_deadline_expiry_reports_unresolved`,
   `panicked_writer_reports_unaccounted_charges`,
   `panic_after_partial_flush_counts_only_unflushed`,
   `running_totals_are_readable_and_match_the_final_report`,
   `queue_depth_rises_before_the_shed_and_sheds_are_counted`,
   `a_failing_sink_does_not_advance_the_last_ingest_time`,
   `rejected_events_are_visible_while_running`, and
   `metrics_report_accounting_health_while_running`.

9. **Crash leak is bounded by TTL.** A crashed lease holder strands its unspent
   units only until the lease TTL expires, after which the allocator reclaims
   them. Each reclaim transaction is bounded; the server fixes one expiry
   cutoff and drains saturated batches immediately, so bounding lock scope
   never caps the legitimate backlog that returns. *Tests:*
   `expired_lease_units_reclaimed` and
   `expired_backlog_is_reclaimed_in_bounded_batches` (store suites), plus
   `one_scheduled_sweep_drains_every_saturated_batch` (server suite).

10. **Ready means currently admissible.** An instance reports ready only while
    every tracked principal has a fresh positive or negative resolution, its
    lease remains inside the local usability window, and its snapshot and
    refill/accounting tasks are alive. Readiness falls again on exhaustion,
    expiry, or task exit;
    fail-closed correctness must not masquerade as availability. Readiness is
    a single bit, so it says *that* an instance is unready and never *how
    much* is unresolved; the count of principals without a valid resolution is
    exported alongside it and is derived from the same pass that decides the
    bit, so the two cannot disagree — a separate predicate would be free to
    drift, leaving readiness false with nothing to explain it. *Tests:*
    `initial_load_gates_readiness_and_installs`,
    `readiness_falls_when_snapshot_expires_during_outage`,
    `readiness_falls_if_refresh_hangs_across_snapshot_expiry`,
    `readiness_falls_when_background_planes_stop`, and
    `snapshot_counters_track_failures_and_the_unresolved_gauge`.

11. **Checked arithmetic only.** Cost and lease arithmetic never wraps; any
    overflow is an explicit error that denies (fail closed), never a wrap to a
    small charge. Stored ledger values follow the same rule in both
    directions: a negative unit column is surfaced as an explicit store
    error, never clamped to zero — clamping would let the conservation
    equation pass over the corruption it exists to detect. The Postgres
    schema additionally CHECK-constrains unit columns non-negative.
    *Tests:* `tollgate-core` proptests;
    `negative_account_column_fails_conservation_read`,
    `negative_account_column_fails_balance_and_usage_reads`,
    `negative_lease_sum_fails_conservation_read`,
    `acquire_surfaces_negative_stored_balance`,
    `reclaim_refuses_negative_credit`,
    `straggler_exceeding_recorded_loss_fails_ingest`, and
    `checked_ledger_columns_reject_negative_writes` (Postgres suite; the
    memory backend makes negative state unrepresentable via `u64`).

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
    enqueue it. The guarantee extends through shutdown: the writer's drain
    waits for the permit, so the event is ingested or explicitly counted in
    `WriterStats::unresolved` — never silently dropped. The safe lifecycle
    order is: stop admitting, quiesce request tasks holding permits or
    guards, shut the usage writer down, then release leases. A spent lease
    with no billing event requires losing the whole process. *Tests:*
    `panic_after_commit_still_bills`, `shutdown_waits_for_committed_guard`.

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

16. **Unsafe configuration never becomes authoritative.** Nonpositive lease
    TTLs, polling/refresh/reclaim intervals, negative safety margins or
    reclaim grace, and internally inconsistent lease thresholds are rejected
    before allocation or task startup. A snapshot is publishable only when
    checked arithmetic proves that its worst registered operation at
    `max_items_per_request` — including fixed and minimum charges — does not
    exceed `rate_burst_units`; overflow and above-burst results are refused at
    every publication or decode boundary. *Tests:*
    `nonpositive_lease_ttl_is_rejected_without_debiting` (both stores),
    `invalid_lease_manager_durations_are_rejected`,
    `invalid_snapshot_manager_intervals_are_rejected`,
    `invalid_grant_policy_is_rejected`, `zero_reclaim_interval_is_rejected`,
    `zero_drain_deadline_is_rejected`, `invalid_writer_config_is_rejected`,
    `invalid_lease_manager_timeouts_are_rejected`, and
    `invalid_pool_config_is_rejected_before_connecting`, plus
    `publication_uses_the_largest_registered_weight`,
    `snapshot_publication_matches_u128_worst_case_oracle`,
    `admin_refuses_snapshot_whose_batch_quote_exceeds_burst`,
    `http_store_rejects_invalid_snapshot_from_legacy_server`, and
    `legacy_invalid_snapshot_is_rejected_on_read`. *Proof:*
    `formal/lean/Tollgate/SnapshotLimits.lean`. No such value is silently
    repaired: coercing a capacity or overriding a declared limit would change
    the runtime meaning of a configured contract.

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

18. **Background store calls are wall-clock bounded.** No background task may
    be parked by a backend that hangs rather than answering: every allocator
    and sink call a client task makes carries a configured timeout, and each
    graceful shutdown carries a total budget, so shutdown terminates whatever
    the backend does. Bounding by retry *count* alone is not a bound. What a
    bound could not complete is reported — a lease left unreleased is
    `LeaseManagerReport::abandoned` and settles at TTL reclaim (#9); an
    undelivered batch is `WriterStats::lost` — never silently assumed done.
    *Tests:* `hung_ingest_cannot_stall_shutdown`,
    `hung_ingest_times_out_into_the_retry_path`,
    `hung_release_times_out_and_reparks`, and
    `invalid_pool_config_is_rejected_before_connecting`.

19. **A control-plane failure is never silent.** Every fallible call a
    background plane makes either succeeds, is reported through a typed
    result the caller can act on, or emits a structured event — never
    nothing. This is what makes "fail closed, the background plane recovers"
    checkable in production rather than merely intended: retrying forever is
    correct behavior, so without a report an unreachable backend is
    indistinguishable from an idle one. Level follows consequence: a refusal
    the instance can absorb is `debug`, one that leaves it denying every
    request is `warn`, and accounting divergence or a dead task is `error`.
    Enforcement is mechanical, not conventional — the library crates deny
    `clippy::let_underscore_must_use`, so discarding a fallible call fails
    the build unless an `#[allow]` states why there is nothing to say.
    *Tests:* `refill_failure_with_an_empty_slot_warns`,
    `healthy_refill_emits_no_warning` (the converse: a healthy plane stays
    quiet, or the signal is worthless), and
    `usage_sink_outage_and_recovery_are_reported`.

20. **Every admission outcome is counted, exactly once, under its own reason.**
    The request path may not log (5), so its tallies are the only account it
    can give of itself; an instance refusing every request must be
    distinguishable from one serving none. `AdmissionEngine::admit` records the
    single outcome of each call — never the individual exits, five of which
    only ever arrive by `?` from `AccountSnapshot::admit` and
    `Reservation::reserve` — so a reason cannot be produced without being
    tallied. `DenyReason::index` is an exhaustive match, making a slot
    per reason total by construction and a shared slot unrepresentable; a new
    variant fails to compile until it has one. Denials add no units, because a
    refusal charges zero (2). `units_admitted` counts what was *quoted*, not
    what was billed — usage events remain the billing record. Refusals decided
    before the engine is reached (accounting backpressure, 8) are recorded by
    the embedder against the same tally, so no reason exports a permanent zero
    that reads as "never happens". *Tests:*
    `indices_cover_every_slot_exactly_once`,
    `labels_are_distinct_and_payload_free`, `payload_does_not_affect_the_slot`,
    `counters_attribute_every_outcome`, `each_reason_reaches_its_own_slot`,
    `denied_requests_add_no_units`, `concurrent_increments_are_not_lost`, and
    `metrics_separate_admissions_from_each_kind_of_refusal`.

Ledger roles (context for 1 and 7): leases **bound** spend; usage events **are**
the billing record; reconciliation compares the two and steady-state drift is
zero.
