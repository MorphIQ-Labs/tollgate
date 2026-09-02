# tollgate invariants

A change that violates one of these is a defect even when every test passes.
Each invariant names the test(s) that enforce it; a new invariant is not "done"
until it has one.

1. **Bounded spend.** Under `EnforcementMode::Strict`, total committed usage
   across all instances never exceeds the units allocated to the account, under
   any interleaving of concurrent clients. Enforced by centrally allocated
   leases: units are spent only from a lease, and a lease's units were
   atomically debited from the account at allocation. Opt-in local shards
   partition that one grant exactly; a debit first uses its sticky local
   counter, then siblings, and fragmented debits carry a fixed-size exact-total
   receipt. Sharding never creates capacity and never leaves a positive
   aggregate unspendable at exhaustion. A *live* aggregate read is an estimate —
   the walk is not one atomic instant and a refund may concentrate on a shard it
   has passed or not yet reached — so it is clamped to the grant, never asserted
   against it; settlement reads it only at quiescence, where it is exact.

   Under `EnforcementMode::Elastic { overage_cap }` an account may spend beyond
   its allocation, and the bound becomes a different one: **at most
   `overage_cap` unfunded units per service instance**, so a fleet of `N`
   instances can extend up to `N * overage_cap`. That multiplication is
   deliberate — the counter is a local atomic, like every other local mechanism
   here, and aggregating it would need the synchronous coordination the request
   path exists to avoid (1). Size the cap against the fleet, not one process.
   The cap is *not* sharded the way the grant is: a grant divides because its
   shards sum back to it, while N per-locality caps would either refuse an
   elastic request while headroom sat unreachable on another core, or silently
   raise the cap to `N × overage_cap` on the instance.

   What the mode does *not* relax: every unfunded unit is recorded. Overage is
   billed as ordinary usage and funded by its own ledger term, so per-account
   conservation stays exact (see *Ledger roles*) and no unit is ever spent without being
   accounted for. Elastic mode changes whether a request is admitted; it
   changes nothing about whether its units are counted.
   *Tests:* `no_double_spend_across_instances` (memory and Postgres variants),
   `tollgate-core` reservation proptests; `lease_units_are_conserved` and
   `concurrent_commit_conservation` across sharded layouts,
   `sharded_grant_and_low_water_partitions_are_exact`,
   `a_whole_sibling_is_used_before_fragmenting`,
   `sharded_lease_spends_to_exact_exhaustion_without_stranding`,
   `failed_fragmented_debit_reports_true_remaining_and_rolls_back`,
   `a_torn_aggregate_above_the_grant_reads_as_the_grant`,
   `a_fragmenting_rollback_never_panics_a_concurrent_aggregate_read`, and the
   exact aggregate model in `formal/lean/Tollgate/LeaseShards.lean`;
   `elastic_refuses_once_the_local_overage_cap_is_spent`,
   `every_principal_of_an_account_shares_one_cap`,
   `republishing_a_snapshot_does_not_reset_the_cap`,
   `concurrent_debits_never_exceed_the_cap`,
   `a_sharded_slot_does_not_multiply_the_overage_cap` and
   `a_sharded_slot_still_prefers_the_lease_that_can_fund_the_quote` (the cap
   and the sharded lease views sharing one slot), and
   `Tollgate.Conservation.an_accepted_debit_stays_within_the_cap`.

   Pending and committed occupancy are distinct retry states. Reservation
   phase and account occupancy publish through a guard-owned transition. A
   zero-delta observer RMW participates in the marker's modification order,
   so an observer overlapping it receives
   `OverageCommitInProgress`/`AfterInFlight`, never a claim that irrevocable
   units remain refundable or a promise that funding is already required. In
   a stable state, a refusal is
   `OverageCapTemporarilyExhausted`/`Transient` only when refunding all pending
   reservations would make that request fit; otherwise it is stable local
   `OverageCapExhausted`. Both states are `Transient` at the admission
   boundary: neither proves central account exhaustion, and an ordinary lease
   refill can fund the unchanged request. *Tests:*
   `pending_overage_is_transient_only_when_its_refund_would_make_room`,
   `overage_retry_class_matches_stable_occupancy`,
   `pending_overage_saturation_is_transient_until_cancel`, and
   `elastic_refuses_once_the_local_overage_cap_is_spent`,
   `committed_overage_exhaustion_remains_retryable_after_lease_refill`, plus
   `overage_commit_publication_never_looks_refundable` and
   `overage_retry_classes_map_to_distinct_http_contracts` for the canonical
   embedder. *Proof:* `formal/lean/Tollgate/OveragePublication.lean`.

2. **Zero charge before execution.** A reservation that never reaches
   `commit_at_execution_start` charges zero units, and its units return to the
   local lease. Dropping a pending reservation is a release, not a leak.
   A fragmented sharded reservation refunds its exact aggregate once; shards
   may rebalance because they are partitions of the same lease bound. *Tests:*
   `reservation::tests::{drop_releases_pending, cancel_charges_zero}`,
   `fragmented_reservation_refunds_without_stranding_capacity`, and
   `lease_units_are_conserved`.

3. **Atomic commit-vs-cancel.** Commit and cancel race on a single atomic
   transition; exactly one wins. A cancelled reservation can never later
   commit; a committed reservation reports its full charge to a late
   canceller. Overage wraps that phase transition and committed occupancy in a
   visible publication guard, so observers never treat the interval between
   the two atomic words as stable. Once both racers return its retry
   classification agrees with the winner. *Tests:*
   `reservation::tests::commit_cancel_race_one_winner`,
   `reservation::tests::overage_commit_publication_never_looks_refundable`, and
   `reservation::tests::overage_commit_cancel_race_preserves_retry_classification`.
   *Proof:* `formal/lean/Tollgate/OveragePublication.lean`.

4. **Lease capabilities are exact and lease-scoped.** Every acquired lease is
   stamped with the next fencing token in its account's strictly increasing
   sequence. The sequence is an allocation and audit order, not an
   account-wide validity epoch: issuing a newer token does not invalidate an
   older lease that is still active. Release accepts only the stored
   `(lease_id, fencing_token)` pair; usage accepts only the stored
   `(lease_id, account_id, fencing_token)` triple. Lease state and remaining
   capacity are independent checks: a matching capability cannot revive a
   reclaimed lease or exceed its accounting capacity. A stored fence outside
   the token domain is surfaced as a storage error, never aliased to fence 0 —
   aliasing would misattribute corruption to the caller and break the
   monotonic audit trail. If release rejects the token attached to a grant the
   client believed valid, the client clears its slot and fails closed because
   its local lease identity can no longer be trusted relative to the store.
   *Tests:* `newer_lease_does_not_invalidate_older_active_capability`,
   `wrong_token_release_leaves_lease_reclaimable`, and
   `usage_rejects_mismatched_lease_capability` (store suites);
   `expired_lease_units_reclaimed`, `straggler_usage_after_release_is_billed`,
   and `fenced_release_clears_the_slot`;
   `acquire_surfaces_negative_stored_fence` and
   `release_and_ingest_surface_negative_stored_fence` (Postgres suite; the
   memory backend stores tokens as `u64`, so the corrupt state is
   unrepresentable there).

5. **Fail closed, zero I/O.** Unknown principal, suspended/closed account,
   expired snapshot, missing permission, cost overflow, accounting
   backpressure: all deny locally, under every enforcement mode. A lease that
   cannot fund the quote — absent, expired, or exhausted — also denies locally
   under `Strict`; it is the one condition `Elastic` may admit past, because it
   is the one that says something about *funding* rather than about validity
   (1). Nothing else in this list is mode-dependent. The request path performs no database, file, lock-file, or network
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
   `RateLimited`); and
   `elastic_does_not_relax_any_refusal_that_is_not_about_funding`, which pins
   that an elastic account still denies every non-funding refusal *and* claims
   no credit while doing so. `AccountSnapshot::builder` requires the
   administrative status, and `builder_requires_account_status_at_construction`
   pins that incomplete construction cannot silently become `Active`.
   An opt-in sharded limiter partitions (never copies) the instance-local
   account rate and burst; publication's already-validated maximum quote
   limits the shard count so every shard can admit the largest legitimate
   request. That limit is account-wide, because the bucket is: it is the
   tightest ceiling any principal sharing the account has presented, tightened
   by every install irrespective of generation ordering and never widened, so
   a key with a heavier cost table is never wedged by a split its sibling
   sized. Raw snapshot installs retain one defensive bucket because they do
   not carry that proof. A shard that cannot hold a request is not the
   account's answer: siblings are tried, the soonest retry any of them offers
   is what `RateLimited` reports, and `UnpriceableUnderLimits` is returned only
   when no bucket can take the request now or later.
   *Additional tests:* `rate_shards_partition_one_account_burst_without_multiplying_it`,
   `maximum_quote_limits_shards_to_buckets_that_can_admit_it`,
   `publication_accepts_a_worst_case_quote_equal_to_the_burst`,
   `publishable_install_carries_maximum_quote_into_rate_sharding`,
   `a_heavier_principal_resplits_the_account_bucket_it_shares`,
   `an_unproven_principal_collapses_the_account_split`,
   `a_stale_snapshot_still_narrows_the_split_it_cannot_widen`, and
   `a_shared_split_bucket_never_wedges_a_principal_the_burst_can_hold`.
   Tightening that safe split publishes one replacement through the account's
   single `AccountPolicyState` indirection. Every subsequent request, from
   every principal, loads that one current authority; no installed principal
   retains an independently refillable old bucket. *Test:*
   `shard_tightening_cannot_leave_two_spendable_account_buckets`.
   Request-count rate is a second optional account bucket with unit weight;
   it is checked before the optional cost-weighted bucket, and tokens from
   either are never refunded after a later refusal. Each dimension binds its
   immutable enforced parameters to its mutable bucket. Publishing a change
   in one dimension, including inactive compatibility metadata, retains the
   exact bucket and consumed state of every unchanged dimension. `RateState`
   owns the exact `AccountRatePolicy` its buckets implement. Rate and account concurrency are
   selected by the same accepted account-policy generation and published in
   one `AccountPolicyState`; a request loads it once and uses it for every
   account-wide decision. Principal snapshots remain the authorities for
   status, permissions, request shaping, pricing, funding mode, and optional
   principal concurrency. A divergent same-generation or older principal can
   therefore neither bypass nor manufacture an account bucket or ceiling, and
   a genuinely newer accepted account policy reaches every principal on its
   next request. Omitting either bucket performs no governor operation for
   that dimension.

   Generation acceptance precedes runtime resolution in both snapshot maps.
   A rejected replay, and a positive overwritten before an atomic batch is
   published, cannot mutate the account policy registry. This ordering is the
   enforcement boundary for authorization monotonicity (15), not a downstream
   request-path check.
   *Additional tests:* `request_rate_limiter_counts_requests_not_cost`,
   `disabled_weighted_rate_performs_no_weighted_check`,
   `changing_disabled_weighted_fallback_does_not_refill_request_rate`,
   `enabling_request_rate_does_not_refill_weighted_rate`,
   `divergent_enabled_snapshot_cannot_outlive_a_disabled_account_bucket`,
   `divergent_disabled_snapshot_cannot_bypass_an_enabled_account_bucket`,
   `resolved_account_authority_carries_the_generation_winners_policy`,
   `moka_rejects_replays_before_resolving_account_state`,
   `arc_swap_rejects_replays_before_resolving_account_state`,
   `an_overwritten_batch_positive_never_becomes_account_authority`,
   `a_later_funding_refusal_releases_concurrency_but_keeps_rate_tokens`,
   `limit_change_is_one_account_authority_for_every_principal`, and
   `one_batch_with_two_generations_keeps_the_newer`. *Proof:*
   `formal/lean/Tollgate/RatePublication.lean`, including
   `unchanged_weighted_authority_is_preserved` and
   `unchanged_request_authority_is_preserved`.

6. **Foreground isolation.** Lease refill and snapshot replacement never block
   an in-flight request, and the request path never waits on the plane that
   refills it. A draining lease *tells* the refill task rather than being
   discovered by it — the debit that crosses low water raises a signal whose
   implementation is contractually non-blocking — so refill latency is no
   longer bounded below by the poll interval, and a funded account is not
   refused between ticks. Cold start and usability-window rollover keep the
   interval as their backstop, having no debit to announce them. In a sharded
   lease, an early local low-water crossing wakes at most once per shard; if
   the aggregate is not low yet, the manager clears those doorbells and
   rechecks the aggregate so a concurrent crossing cannot be lost. Publishing
   a lease to N locality views is N swaps, so mutators are serialized: a
   reader straddles one publication exactly as it straddled the single-view
   slot's one swap, but the slot never *ends* a publication holding two
   different leases, which is what would let a locality keep spending past a
   revocation. Rotation and shutdown wait for every locality's independently
   reference-counted lease view before releasing the exact aggregate.
   *Tests:* `refill_begins_on_the_crossing_debit_not_the_next_tick`,
   `a_burst_across_a_rotation_never_denies_a_funded_account`,
   `refill_installs_lease_on_cold_start`,
   `usability_window_rollover_returns_unspent_capacity`,
   `the_crossing_debit_raises_the_signal`,
   `a_lease_signals_at_most_once_however_long_it_drains`,
   `an_early_shard_signal_is_rearmed_until_the_aggregate_crosses`,
   `rotation_at_low_water_installs_fresh_lease`,
   `shutdown_releases_unspent_units`,
   `sharded_slot_keeps_release_parked_while_any_local_view_is_held`,
   `racing_mutators_never_leave_a_slot_holding_two_answers`, and the
   `RefillRequests` handoff tests. Configuration ownership is witnessed by
   `mismatched_local_sharding_is_rejected_before_tasks_start`.

7. **Idempotent partial accounting.** Replaying a usage batch (same request
   IDs) never double-bills. Every successful mixed batch classifies each
   input exactly once as accepted, duplicate, or rejected; only accepted
   events change either ledger, and their grouped lease/account effects stay
   in the same atomic transaction. *Tests:* `usage_replay_is_idempotent` and
   `mixed_usage_batch_preserves_partial_acceptance` (store suites).

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
    its snapshot resolutions meet the bar below, it can still fund work, and
    its snapshot and refill/accounting tasks are alive. Readiness falls again
    on exhaustion, expiry, or task exit; fail-closed correctness must not
    masquerade as availability.

    "Can still fund work" is the same question admission asks, and it is
    mode-dependent for the same reason (1). Under `Strict` it is a lease inside
    the local usability window with units left. Under `Elastic` an empty or
    absent lease is not the end of the answer: an account with overage headroom
    *is* admissible, and reporting it unready would withdraw from rotation
    exactly the instances the mode exists to keep serving — availability
    masquerading as fail-closed correctness, the same error in the other
    direction. Readiness reads the mode from the snapshot the request path
    reads, never a copy, so the two cannot disagree after a republish.

    The snapshot bar depends on how the tracked set is chosen, because the
    same rule means opposite things at the two scales (#48). For a
    `Fixed` set — hand-configured, small — ready requires **every** tracked
    principal to hold a fresh positive or negative resolution: the set was
    chosen deliberately, so any gap in it is a real one. For an `All` set —
    every principal the source knows, which is the whole customer base — that
    rule inverts into a fault, holding an instance serving 15,999 of 16,000
    principals out of rotation for the one its source cannot answer for. There
    ready requires only that **some** tracked principal is resolved, falling
    when none is; per-principal admissibility needs no help from readiness,
    because the map already denies fail-closed for anything unresolved. An
    instance tracking nobody is healthy, not broken.

    Readiness is a single bit either way, so it says *that* an instance is
    unready and never *how much* is unresolved; the count of principals
    without a valid resolution is exported alongside it and is derived from
    the same pass that decides the bit, so the two cannot disagree — a
    separate predicate would be free to drift, leaving readiness false with
    nothing to explain it. Enumeration failures are counted apart from fetch
    failures, since they freeze the tracked set rather than staling it and are
    otherwise invisible. *Tests:*
    `initial_load_gates_readiness_and_installs`,
    `readiness_falls_when_snapshot_expires_during_outage`,
    `readiness_falls_if_refresh_hangs_across_snapshot_expiry`,
    `readiness_falls_when_background_planes_stop`,
    `one_unanswerable_principal_unreadies_only_a_fixed_instance`,
    `snapshot_counters_track_failures_and_the_unresolved_gauge`,
    `readiness_closes_the_lease_window_exactly_when_debits_do`, and
    `readiness_counts_overage_headroom_for_an_elastic_account`.

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
    `ChargeGuard` consumes the complete `Admitted` proof, binds the billing
    event to the pre-reserved queue permit, and retains the resulting
    `CommittedAdmission`; normal completion, early return, panic unwind, and
    task abort all enqueue the event and release concurrency only when the
    execution guard drops. `ChargeGuard::commit` returns that guard directly,
    rather than hiding it in a tuple, and the guard type is `#[must_use]`, so
    discarding execution-start evidence is rejected under
    `unused_must_use`. The guarantee extends through shutdown: the
    writer's drain waits for the permit, so the event is ingested or explicitly
    counted in `WriterStats::unresolved` — never silently dropped. The safe lifecycle
    order is: stop admitting, quiesce request tasks holding permits or
    guards, shut the usage writer down, then release leases. A spent lease
    with no billing event requires losing the whole process. *Tests:*
    `panic_after_commit_still_bills`, `shutdown_waits_for_committed_guard`,
    `committed_charge_holds_concurrency_until_execution_guard_drops`, and
    `failed_commit_releases_concurrency_and_accounting_capacity`, plus the
    `ChargeGuard` discarded-result compile-fail doctest.

14. **Account creation is never destructive.** Recreating an existing
    account is a surfaced `AlreadyExists` in every backend — never an
    overwrite, never a silent no-op. *Tests:*
    `recreate_account_is_refused_and_nondestructive` (both store suites).

15. **Authorization generations never move backward.** Positive snapshots and
    revocation tombstones retain the highest generation observed. A delayed
    positive at or below a tombstone cannot resurrect a principal, and a
    delayed older tombstone cannot revoke a newer positive snapshot. The
    source stores tombstones durably so the rule survives instance restarts;
    evicting a request-visible entry does not evict its revocation watermark.
    The rule is about **tombstones**, and the implementation now says so: only
    a generation the source published a revocation at may refuse that same
    generation back. A generation this instance merely observed orders
    snapshots — a strictly older one is still refused — but asserts nothing
    about being dead, so the same generation arriving again is a
    re-observation. Conflating the two stranded any principal whose row went
    briefly absent, since the absence inherited the positive's generation and
    then refused it back forever; that is #17's rule — keyed on what the source
    answered, never on what the instance remembers — applied to admission
    rather than to TTL selection.
    *Tests:* both snapshot-map contract tests,
    `negative_eviction_preserves_generation_monotonicity`,
    `revoked_generation_rejects_replay_after_visible_entry_is_evicted`,
    `revocation_reaches_instances_via_refresh`,
    `snapshot_publish_fetch_and_push`,
    `snapshot_publish_fetch_and_generation_monotonicity`, and
    `a_vestigial_jsonb_generation_is_ignored_in_favour_of_the_column` (which
    pins *which* stored number the watermark is, now that a backend keeps only
    one), `a_generation_survives_an_absence_and_returns_unchanged`,
    `a_revocation_refuses_its_own_generation_back`,
    `a_visible_snapshot_refuses_its_own_generation_again`,
    `an_absence_never_makes_a_generation_dead`, and
    `a_live_principal_that_goes_absent_recovers_on_the_unknown_ttl` together
    with `a_revoked_principal_stays_tracked_and_cannot_be_resurrected` — the
    pair that separates an absence from a revocation at *equality*, which is
    the only generation where the two rules differ. *Proof:*
    `formal/lean/Tollgate/SnapshotCache.lean`.

16. **Unsafe configuration never becomes authoritative.** Nonpositive lease
    TTLs, polling/refresh/reclaim intervals, negative safety margins or
    reclaim grace, and internally inconsistent lease thresholds are rejected
    before allocation or task startup. A snapshot is publishable only when
    both carried weighted-rate scalars fit governor's non-zero `u32` domain
    in full width — including the rollback pair carried while weighted rate
    is disabled — and when
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
    `publication_rejects_weighted_values_outside_governors_domain`,
    `snapshot_publication_matches_u128_worst_case_oracle`,
    `admin_refuses_snapshot_whose_batch_quote_exceeds_burst`,
    `admin_refuses_weighted_rate_outside_governors_u32_domain`,
    `http_store_rejects_invalid_snapshot_from_legacy_server`,
    and `legacy_invalid_snapshot_is_rejected_on_read`. *Proof:*
    `formal/lean/Tollgate/SnapshotLimits.lean`. Values are never silently
    repaired: coercing a declared capacity would change its contract.

17. **Negative caching is bounded and self-healing.** The ArcSwap map retains
    at most its configured count of request-visible negatives, removes
    expired negatives during control writes, and evicts the earliest expiry
    first. Expiry schedules a targeted source pull even when push is
    unavailable and the full refresh interval is much longer; source errors
    retry with backoff. In the routine case that targeted pull is the only
    thing that reresolves a negative: the full sweep covers principals an
    instance can serve and skips negatives, which already carry a deadline of
    their own. Broadcast lag is the exception and refetches the unfiltered
    tracked set, because a dropped push is most often a reinstatement, and
    filtering by local resolution would skip exactly what recovery is for.
    Which TTL applies is keyed on **what the source answered**, never on the
    generation the instance remembers: an absent row takes `unknown_ttl`, a
    published revocation tombstone takes `revoked_ttl`. Conflating them
    strands a live principal for the reinstatement TTL whenever a source is
    merely rebuilding or failing over, while readiness still reports healthy.
    Pruning visible negatives never weakens invariant #15, and neither does
    skipping them in the sweep: a live principal is always swept, so
    withdrawing one still propagates within `refresh_interval`; what the
    longer TTL bounds is the Negative → Present direction only.
    *Tests:* `arc_swap_negative_cache_is_bounded_and_evicts_oldest_deadline_first`,
    `arc_swap_control_write_drops_expired_negatives`,
    `many_unknowns_leave_only_the_configured_number_visible`,
    `http_negative_ttl_refetches_without_push`,
    `negative_ttl_retry_is_backed_off_and_recovers_without_push`,
    `the_sweep_does_not_refetch_tombstones`,
    `revocation_still_propagates_within_the_refresh_interval`,
    `a_live_principal_that_goes_absent_recovers_on_the_unknown_ttl`,
    `a_reinstated_principal_comes_back_on_the_revoked_ttl`,
    `lag_recovery_covers_the_negatives_a_sweep_skips`, and
    `deadline_helpers_are_exact_in_the_supported_domain`.

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
    quiet, or the signal is worthless), `ping_surfaces_a_closed_pool`, and
    `usage_sink_outage_and_recovery_are_reported`.

20. **Every admission outcome is counted, exactly once, under its own reason.**
    The request path may not log (5), so its tallies are the only account it
    can give of itself; an instance refusing every request must be
    distinguishable from one serving none. The snapshot map owns one shared
    counter identity and installs its `Arc` into every request state. `begin`
    records a stage-one refusal; a successful context records exactly one
    stage-two outcome when `admit` consumes it. The deprecated one-call wrapper
    records the same combined outcome once. A context dropped before stage two
    has created neither pending funding nor an admission outcome.
    `DenyReason::index` is an exhaustive match, making a slot
    per reason total by construction and a shared slot unrepresentable; a new
    variant fails to compile until it has one. Those slots are also *stable*:
    `AdmissionCounters::denials` is exported through the public
    `CountersSnapshot` as dense positions, so a reason's number is a contract
    with whatever reads them. Reasons append at the next free slot and `COUNT`
    only grows; renumbering an existing one silently re-attributes a counter a
    consumer already reads, and stays internally consistent while doing it, so
    it is pinned by literal rather than left to the permutation check. Denials add no units, because a
    refusal charges zero (2). `units_admitted` counts what was *quoted*, not
    what was billed — usage events remain the billing record. Refusals decided
    before the engine is reached (accounting backpressure, 8) are recorded by
    the embedder against the same tally, so no reason exports a permanent zero
    that reads as "never happens". An admission that no lease funded is counted
    under `admitted` *and* under `admitted_overage`, a qualifier rather than a
    sibling, so a reader of `admitted` never has to add two numbers to get the
    total. *Tests:*
    `indices_cover_every_slot_exactly_once`,
    `shipped_slots_and_labels_never_move`,
    `labels_are_distinct_and_payload_free`, `payload_does_not_affect_the_slot`,
    `counters_attribute_every_outcome`, `each_reason_reaches_its_own_slot`,
    `denied_requests_add_no_units`, `concurrent_increments_are_not_lost`,
    `staged_outcomes_are_counted_once_at_their_deciding_stage`,
    `engines_sharing_a_map_export_one_counter_identity`,
    `moka_engine_and_installed_context_share_one_counter_identity`,
    `metrics_separate_admissions_from_each_kind_of_refusal`, and
    `an_overage_admission_is_counted_twice_over_and_a_refusal_once`.

21. **Every 128-bit identifier has one portable wire spelling.** `AccountId`,
    `KeyId`, `LeaseId`, `RequestId`, and `Principal` are exactly 32 lowercase
    hexadecimal characters without a prefix in human-readable serialization
    and URL paths. A malformed path is a structured `invalid-id`, never an
    unknown principal; only a structured `404 unknown-principal` is negative
    evidence that may enter the authorization cache. The pre-public v1
    contract is updated in place; numeric identifiers are rejected rather than
    silently reinterpreted. PostgreSQL's storage-local snapshot JSON
    deliberately retains numeric ids in the legacy u64 range and uses
    canonical text for values the old codec could not represent. The owning
    conversion keeps pre-existing rows and ordinary writes rollback-safe while
    extending storage to the full u128 domain. *Tests:*
    `every_id_uses_the_same_fixed_width_lowercase_hexadecimal_text`,
    `identifier_text_round_trips_the_full_u128_domain`,
    `high_bit_ids_are_portable_text_in_an_untyped_json_consumer`,
    `identifier_failures_are_structured_and_never_unknown`,
    `an_unstructured_route_404_is_not_a_confirmed_unknown_principal`,
    `full_stack_over_loopback_http`,
    `snapshot_json_preserves_legacy_numbers_and_encodes_high_ids_exactly`, and
    `malformed_storage_id_explains_both_accepted_representations`.

22. **An account's status has one writer and one propagation path.** The
    ledger's `tollgate_accounts.status` and the `AccountStatus` inside every
    live snapshot of that account are written by a single transactional
    operation, `AdminStore::set_account_status`, and cannot be observed
    disagreeing: either the ledger moved *and* every live snapshot of the
    account was republished carrying it at `generation + 1`, or nothing moved.
    Snapshots already at the target status are not rewritten, so a repeat
    converges and bumps no generation. Revoked principals are never
    republished — resurrecting a tombstone is what #15 forbids, and revocation
    stays a separate per-credential mechanism. A snapshot published directly
    with a status contradicting the ledger is refused, not accepted and
    reconciled later. `Closed` is terminal: an account enters it from any
    status and leaves it never, and the refusal changes nothing. Suspension
    does **not** reclaim outstanding leases — their units were debited at
    grant and #9 already bounds them — so the bound on "requests stop" is one
    `SnapshotManager` refresh interval, not zero. The operation reports what it
    changed: how many snapshots it republished, and how many changed durably
    but could not be decoded to push, so a partial result is surfaced rather
    than absorbed. *Tests:*
    `suspending_an_account_stops_leases_and_republishes_its_snapshots`,
    `suspension_republishes_only_the_suspended_accounts_snapshots`,
    `suspending_an_account_does_not_resurrect_revoked_principals`,
    `reactivating_an_account_restores_admission_and_bumps_generations`,
    `a_closed_account_cannot_be_reactivated`,
    `repeating_a_status_change_publishes_nothing_new`,
    `a_status_change_pushes_every_republished_principal`,
    `a_non_active_account_refuses_leases`,
    `creating_a_suspended_account_denies_from_birth`,
    `unknown_account_status_update_is_refused`, and
    `publishing_a_snapshot_that_contradicts_the_ledger_is_refused`, and
    `a_status_change_that_cannot_republish_moves_neither_record` (all
    mirrored across both backend suites);
    `the_account_column_is_derived_for_both_stored_id_spellings` and
    `an_unrecognized_status_column_is_a_storage_error` (PostgreSQL);
    `account_status_transitions_keep_the_two_records_equal` (property);
    `account_status_text_matches_its_serde_spelling` and
    `restamping_preserves_everything_validation_depends_on` (core);
    `account_status_endpoint_speaks_the_status_vocabulary` (server).

23. **A cached credential proves identity, never authorization, and never
    outlives its own validity.** A `SessionCredential` may reuse a `Principal`
    only within the session it verified in, only when the presented credential
    is byte-for-byte equal under a constant-time comparison to the one whose
    verification produced it, and only while that verification is still
    reusable at the caller's `now`. A missing or different credential clears
    that session's proof *before* any replacement is verified, so a failed
    verification can never leave the previous principal reusable; failed and
    already-expired verifications are never cached.

    The validity bound is what makes the verifier seam safe to open. Schemes
    the seam invites — PASETO, JWT, client certificates — carry expiry, and a
    cache that ignored it would honour a dead token for as long as a session
    stayed open, with admission unable to compensate because expiry is a
    property of the credential and not of the account snapshot.
    `Verified::reusable_until` carries it, `HmacRegistry` returns `None`
    because a server-issued key expires only by withdrawal, and withdrawal
    travels by snapshot.

    A cache hit skips the credential check and **nothing else**: every request
    still enters `AdmissionEngine::admit` and consults the current snapshot, so
    status, staleness, permissions, rate and quota are decided fresh and
    revocation stays bounded by snapshot refresh exactly as it is with no cache
    at all. That is why this lives in `tollgate-auth` rather than in each
    embedder: the ordering above is a security convention, and conventions
    upheld by caller discipline at many call sites drift.

    The credential bytes live only in that session-scoped cache — never in the
    verifier, durable state, or logs — and are wiped on drop rather than merely
    freed, so a core dump cannot recover credentials from sessions that have
    closed. `Drop` for `VerifiedCredential` is what enforces that, and it *is*
    witnessed: reading the buffer after the drop would be undefined behaviour,
    so the wipe is observed from inside the drop instead — the last instant
    those bytes are still defined to read.
    *Tests:* `an_unchanged_credential_is_verified_once_per_session`,
    `the_cache_is_isolated_per_session`,
    `a_failed_replacement_does_not_leave_the_previous_principal_usable`,
    `a_changed_credential_revalidates_as_the_new_principal`,
    `a_prefix_or_extension_of_the_cached_credential_is_not_accepted`,
    `no_credential_is_retained_in_the_registry`,
    `the_cache_works_with_an_arbitrary_verifier`,
    `a_cached_answer_does_not_outlive_the_validity_it_was_given`,
    `an_already_expired_answer_is_refused_and_not_cached`,
    `an_expired_answer_that_no_longer_verifies_denies`,
    `a_dropped_cache_entry_is_wiped_not_merely_freed`, and
    `every_forwarding_impl_reaches_the_verifier` (tollgate-auth);
    `price_route_requires_connection_context` and
    `cached_principal_still_observes_snapshot_revocation` (pricing-api, the
    wiring).

24. **Steady-state embedding admission allocates nothing it owns and performs
    exactly one snapshot lookup.** After the measuring thread has initialised
    dependency-owned thread-local state and the bounded usage queue has a
    reusable block, cached authentication through usage-slot reservation,
    `begin`, body-independent usage-slot reservation, stage-two admission,
    commit or cancellation, and usage recording performs no heap
    allocation or reallocation attributable to Tollgate. Every admission
    invokes `SnapshotMap::get_at` exactly once in `begin`; `RequestContext::admit`
    never re-enters the map or calls `get`. An implementation may still satisfy
    `get_at` through the trait's compatibility default. Body decoding and later
    execution consume the owned context and typed pending/committed evidence
    rather than retrieve policy again.

    This is an enforcement-ladder rung 3 convention because it spans four
    crates and neither Rust's type system nor any one owning component can
    express process allocation or map-call cardinality. A test-only global
    allocator scopes counts to the current thread, while a forwarding map
    decorator counts the observable lookup calls. Cold first-touch allocation
    by dependencies, Tokio queue-block acquisition, caller-owned request
    buffers, and the consumer executor's job object are not hidden: the gate
    reports them under separate attribution lines. Moka and governor's
    internal monotonic reads likewise do not become policy time; snapshot and
    lease decisions continue to use the caller's `Timestamp`.

    *Tests:* `a_box_inside_the_scope_is_counted`,
    `alloc_zeroed_and_realloc_are_both_visible`,
    `a_pure_scope_is_zero_and_outside_work_is_excluded`,
    `another_threads_allocations_are_excluded`, and
    `nested_scopes_compose_and_panics_restore_the_depth`, and
    `report_rows_are_appended_as_valid_json_lines`
    (`tollgate-alloc-count`); `core_hot_path_allocates_nothing` (core);
    `a_cached_credential_allocates_nothing` (auth);
    `admission_allocates_nothing_on_the_arc_swap_default`,
    `configured_and_disabled_guards_allocate_nothing_after_warmup`,
    `moka_reads_are_allocation_free_after_current_thread_warmup`, and
    `admit_consults_the_map_exactly_once` (admission); and
    `embedding_path_allocates_nothing_after_warmup` (client). The required
    `allocation-assertions` CI job runs them through
    `scripts/check_allocations.sh` and proves the counter crate has no normal
    reverse dependency from a release binary.

25. **Concurrency ceilings are exact, per instance, and released once.** Each
    stable account and principal gauge tracks every admitted request, including
    while its optional ceiling is absent. Before first activation, occupancy
    is direct-indexed by request locality so opt-in sharding does not recreate
    a shared cache line. Activation publishes a `draining` phase before the
    new policy. Draining seals the shards to new permits; it does not suspend
    admission. An acquisition carrying no ceiling of its own is never refused
    by another policy's activation, and one carrying the activated ceiling is
    admitted on the central counter bounded by that ceiling less the undrained
    shard residue — that residue is live work and occupies the ceiling being
    published, so total occupancy never passes it. Enabling a ceiling therefore
    narrows admission to that ceiling instead of closing the account for the
    lifetime of the longest request already running. Exact shard permits
    release to their owning counters, and only an all-zero shard set can
    atomically promote to the central CAS-bounded counter. Once central, the
    gauge never resets or returns to sharded tracking, so disabling and
    re-enabling retains occupancy; a ceiling withdrawn while the handoff is
    still draining, before any central work exists, does return the gauge to
    sharded tracking. A successful bounded compare-exchange never
    increments at or above the configured ceiling.

    The account ceiling comes from the same canonical `AccountPolicyState`
    every principal loads for rate policy. A configured principal ceiling is
    validated not to widen its account ceiling, acquired first, and undone if
    the account acquisition refuses. Both gauges are counted per service
    instance, like local rate state rather than fleet-wide leased funding.

    Occupancy survives snapshot replacement and cache eviction: account and
    principal registries retain weak references, while installed state and an
    in-flight RAII guard keep the exact gauges strongly reachable. The guard
    is constructed only after both occupancy increments and owns the exact
    state containing both gauges; it has no callable release operation. Every
    field of `RequestContext`, `Pending`, `ReadyToStart`, `Committed`,
    `Admitted`, `CommittedAdmission`, and `ChargeGuard` is private. The guard
    moves from `Pending` through `ReadyToStart` into `Committed`, and the
    deprecated `Admitted` wrapper retains the same guard: `Admitted` exposes no
    raw `Reservation`, cancellation consumes it, commit consumes it into
    `CommittedAdmission`, and `ChargeGuard` owns that committed proof
    throughout execution. The public commit result is the must-use guard
    itself, not a tuple that suppresses its diagnostic. Safe code therefore
    cannot retain committed funding while accidentally dropping the
    concurrency authority.
    The concurrency guard is the last admission field dropped and releases
    account then principal exactly once on cancellation, refusal, panic
    unwinding, or ordinary drop. The
    unbounded path performs the same two occupancy transitions against its
    locality shards without a limit comparison or scan; both paths remain
    allocation-free. Shard scans are confined to the one-time activation
    handoff: once when control publishes it, once more when a ceiling-carrying
    request computes its headroom while that handoff is open, and then only
    when a formerly nonempty shard releases its last old permit.

    *Tests:* `account_concurrency_is_held_for_the_admitted_lifetime`,
    `a_principal_ceiling_narrows_without_bypassing_the_account_ceiling`,
    `every_principal_observes_the_canonical_account_concurrency_ceiling`,
    `newer_account_concurrency_policy_reaches_existing_principals`,
    `enabling_account_concurrency_counts_already_in_flight_work`,
    `enabling_principal_concurrency_counts_already_in_flight_work`,
    `reenabled_account_concurrency_counts_work_admitted_while_disabled`,
    `a_later_funding_refusal_releases_concurrency_but_keeps_rate_tokens`,
    `an_in_flight_principal_gauge_survives_removal_and_reinstall`, and
    `concurrent_gauge_never_exceeds_its_ceiling_and_releases_exactly_once`,
    `enabling_a_concurrency_ceiling_observes_unbounded_occupancy`,
    `activation_admits_within_the_new_ceiling_while_old_work_drains`,
    `publishing_a_ceiling_arms_the_gauge_before_the_policy_is_readable`,
    `withdrawing_a_ceiling_mid_handoff_returns_the_gauge_to_sharded_tracking`,
    `a_concurrent_activation_never_denies_an_unlimited_caller`,
    `unbounded_gauge_fails_closed_at_its_representation_limit`,
    `concurrency_guard_carries_the_exact_occupied_state`,
    `committed_charge_holds_concurrency_until_execution_guard_drops`,
    `failed_commit_releases_concurrency_and_accounting_capacity`, and the
    `Admitted` raw-reservation compile-fail doctest — pinned to E0616 and
    paired with a compiling companion, so it cannot pass for an unresolved
    name or a vanished API;
    formal witnesses
    `Tollgate.ConcurrencyGauge.successful_acquire_never_exceeds_account`,
    `successful_acquire_never_exceeds_principal`,
    `principal_ceiling_cannot_bypass_account`,
    `account_refusal_undoes_principal`, `release_restores_counts`,
    `a_second_release_is_refused`,
    `execution_start_transfers_without_releasing`,
    `pending_cancel_releases_occupancy`,
    `execution_finish_releases_occupancy`, and
    `released_owner_cannot_release_twice`, plus
    `publication_preserves_occupancy`,
    `activation_with_existing_occupancy_enters_draining`,
    `draining_admits_an_unlimited_acquisition`,
    `draining_admits_below_the_activated_ceiling`,
    `draining_acquire_never_exceeds_the_ceiling`,
    `draining_refuses_a_ceiling_the_residue_already_fills`,
    `nonempty_shards_cannot_promote`,
    `drained_shards_promote_to_central`,
    `central_disable_reenable_preserves_occupancy`,
    `central_reenable_observes_existing_work`, and
    `promote_outside_its_precondition_is_a_no_op`, which establishes that
    promotion's precondition lives inside `promote_if_drained` rather than in
    its callers — so `release_shard`'s guard is a cost short-circuit, and the
    mutation excluded in `.cargo/mutants.toml` is equivalent rather than
    untested.

26. **A staged request is governed by exactly the generation with which it
    began.** `AdmissionEngine::begin` performs the one principal lookup and
    returns an owned `RequestContext` containing the exact
    `Arc<AccountAdmissionState>` and its selected locality. The installed state
    pins its immutable snapshot and both rate buckets; a later publication
    cannot change its permissions, limits, cost table, or limiter
    configuration. Stable concurrency gauges remain shared across generations
    because resetting live occupancy would violate their ceiling. Stage two
    rechecks only the pinned snapshot's expiry boundary before consuming the
    context; status or permission changes published after `begin` govern the
    next request, never splice two generations into one.

    This is enforcement-ladder rung 1 for ownership and single use: the
    context owns the state, has no engine borrow, is not cloneable, and
    `admit(self, ...)` consumes it. Map-call cardinality remains the rung 3
    witness in (24). *Tests:*
    `staged_context_is_owned_send_sync_and_generation_pinned`,
    `stage_two_rechecks_expiry_without_rechecking_status`,
    `limit_change_pins_each_principal_until_its_own_reinstall`, and
    `admit_consults_the_map_exactly_once`.

Ledger roles (context for 1 and 7): leases **bound** spend; usage events **are**
the billing record; reconciliation compares the two and steady-state drift is
zero. Per account, exactly:

```
deposited + overage_recorded
    == balance + active lease grants + settled usage + settlement loss
```

Read it as a funding statement: the left side is everything the account was
ever funded with — money in, and credit extended under `Elastic` (1) — and the
right side is where those units now sit. `overage_recorded` is a *funding*
term, not a bucket: overage usage also lands in settled usage, so without it
the equation would fail by exactly the overage and reconciliation would report
corruption on a correctly working ledger. Both sides use checked arithmetic and
an overflow answers "violated" rather than wrapping (11), because this equation
exists to detect corrupt state and must not be able to launder it.
*Tests:* `conservation_requires_an_exact_equation_without_overflow`,
`overage_funds_the_usage_it_bills`,
`overflowing_the_funding_sum_is_a_violation_not_a_wrap`,
`overage_usage_is_billed_and_funds_itself` and
`settlement_is_unaffected_by_an_account_carrying_overage` (both store suites);
`Tollgate.Conservation.overage_preserves_conservation` and
`unfunded_overage_always_breaks_conservation`.
