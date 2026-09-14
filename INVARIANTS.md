# tollgate invariants

A change that violates one of these is a defect even when every test passes.
Each invariant names the test(s) that enforce it; a new invariant is not "done"
until it has one. `scripts/check_invariant_witnesses.sh` checks backticked
snake-case references (including qualified brace groups) against Rust
declarations and Lean definitions/theorems, and verifies named proof files.
External API, lint, SQL and event names are documented explicitly in
`testing/invariant_external_symbols.json`. This checks citation integrity,
not whether a declaration enforces the surrounding claim; test execution,
mutation assurance and Lean elaboration remain separate gates. See
`docs/INVARIANT_REFERENCES.md` for the supported notation and limitations.
The checker is witnessed by `a_stale_witness_and_a_wrong_qualifier_both_fail`,
`comments_strings_and_unexpanded_generators_cannot_supply_a_witness`, and
`a_missing_or_misqualified_lean_witness_fails_even_when_mentioned_in_comments`.

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

   **A funded unit is reachable.** Sharding partitions a grant and never
   strands a positive aggregate at exhaustion; the refill plane partitions an
   account's allowance the same way, between what one instance holds and what
   is left in the ledger, and owes the same guarantee. It is not free: an
   instance holding a tail grant too small for the quote it is offered is
   holding exactly the units the next grant needs, and asking for one while
   still holding them sizes it against a balance they are missing from. So the
   two halves are exchanged in one allocator transaction — the unspent units
   returned and the replacement granted against the restored balance — with the
   grant policy's shrink cap applied as a floor against what was returned. The
   exchange preserves at least the spendable credit it restores, and
   no other instance can take the returned units in between. Neither property
   is available to a holder composing `release` and `acquire` itself, which is
   why the operation belongs to the component that owns both the policy and the
   transaction. What still bounds reachability is the policy: under a
   `shrink_divisor` above one a quote larger than `balance / shrink_divisor` is
   deliberately unfundable by any single lease, which is that policy's fairness
   trade across instances and not a stranding.

   A consolidation is a settlement, so the boundary rule applies to it in full:
   the allowance half of a lease funded by a period that has since closed
   expires rather than returning (28), and the replacement is sized against
   what the credit *restores*, never against the units nominally handed back.
   *Tests:* `consolidation_never_grants_less_than_it_folded_in`,
   `consolidation_folds_the_tail_grant_and_the_ledger_into_one_lease`,
   `a_refused_consolidation_leaves_the_original_lease_spendable`,
   `consolidating_without_the_lease_capability_moves_no_units`,
   `a_consolidation_with_an_invalid_ttl_settles_nothing`,
   `consolidating_across_a_boundary_regrants_only_what_the_credit_restores`,
   `consolidation_after_a_budget_reduction_uses_only_restored_credit_as_floor`
   (memory and Postgres variants),
   `consolidation_under_a_shrinking_policy_never_returns_less_than_it_folded`,
   and `a_consolidation_folds_the_tail_grant_over_http` for the wire. The
   reference backend holds a mutex where the SQL backend holds a transaction
   and so has nothing to roll back: it establishes all-or-nothing by planning
   both halves before applying either, which
   `a_refused_consolidation_leaves_the_original_lease_spendable` is what pins.
   Postgres returns the restored credit from the same account update that
   settles the old grant; the replacement consumes that evidence.
   `Conservation.settlement_restores_consolidation_credit`,
   `consolidation_grant_respects_restored_balance`, and
   `expired_allowance_cannot_enlarge_consolidation` prove the exact-model
   credit and floor bounds; the mirrored tests witness backend arithmetic.

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

   Elastic funding is decided twice: at admission, when no lease can fund the
   quote, and at execution start, when the funding lease's window lapsed in
   between (3, 12). Both go through the same cap and the same compare-exchange,
   so both are bounded identically. A commit-time attempt reports the overage
   counter's own refusal verbatim — `OverageCapExhausted`,
   `OverageCapTemporarilyExhausted`, or `OverageCommitInProgress` — because each
   carries a different retry classification — the same three states the
   retry-state paragraph below defines — and collapsing them would tell a
   caller to retry immediately against units that are already irrevocable.

   While a commit-time attempt is in flight the account's units are momentarily
   counted against both its lease and its overage counter. That window cannot
   be closed — the counters are separate atomics, and returning the lease
   earlier is the double-refund defect (3) — but it only ever *over*-states
   local occupancy, so every refusal it can cause is conservative and none of
   them can admit work the cap should have refused.
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
   embedder. The commit-time attempt reaches the same three states:
   `an_elastic_lapse_with_no_committed_headroom_releases_the_lease_for_zero`,
   `an_elastic_lapse_blocked_by_refundable_occupancy_is_temporarily_exhausted`,
   and
   `an_elastic_lapse_overlapping_a_sibling_publication_reports_an_in_flight_commit`.
   *Proof:* `formal/lean/Tollgate/OveragePublication.lean`.

2. **Zero charge before execution.** A reservation that never reaches
   `commit_at_execution_start` charges zero units, and its units return to the
   local lease. Dropping a pending reservation is a release, not a leak.
   A fragmented sharded reservation refunds its exact aggregate once; shards
   may rebalance because they are partitions of the same lease bound. A
   commit-time elastic fallback is a *commit*, not a release: it returns the
   lease receipt because overage now funds the same units, and the charge
   stands in full. *Tests:*
   `reservation::tests::{drop_releases_pending, cancel_charges_zero_and_refunds}`,
   `fragmented_reservation_refunds_without_stranding_capacity`,
   `a_fallback_commit_refunds_its_lease_exactly_once`, and
   `lease_units_are_conserved`.

3. **Atomic commit-vs-cancel.** Commit and cancel race on a single atomic
   transition; exactly one wins. A cancelled reservation can never later
   commit; a committed reservation reports its full charge to a late
   canceller. Overage wraps that phase transition and committed occupancy in a
   visible publication guard, so observers never treat the interval between
   the two atomic words as stable. Once both racers return its retry
   classification agrees with the winner.

   That race is reachable across threads without a lock. `ReadyToStart::split`
   moves the reservation into one shared object and hands the asynchronous side
   a `CancelHandle`; both halves resolve the *same* compare-exchange, so
   splitting changes who may ask for a cancellation and never how the race is
   decided. Cancellation being *requested* is a separate fact from whether it
   changed the funding: the request flag is set before the phase is attempted,
   so a worker that wins still observes it and may stop computing work whose
   caller has gone, while the charge stands in full. The worker's half releases
   eagerly on drop rather than waiting for the handle to let go, so abandoning
   a request refunds at the instant the worker gives up. Splitting is opt-in
   and costs exactly one allocation; an unsplit request has no handle, so
   nothing can have asked it to stop.

   The phase word carries five values, and the terminal one names the funding
   that settled the charge: `PENDING_LEASE` reaches `COMMITTED_LEASE`,
   `COMMITTED_OVERAGE`, or `RELEASED`; `PENDING_OVERAGE` reaches
   `COMMITTED_OVERAGE` or `RELEASED`. The middle edge is the commit-time
   elastic fallback, and it is **one** transition rather than a release
   followed by a second reservation — which would give a canceller one phase
   to win while a worker committed another, and let both report success. It is
   funded by a revocable overage debit taken strictly *before* the claim, so a
   winning claim can never name overage the account never recorded, and it
   returns the lease receipt strictly *after* the claim, so a losing claim
   cannot double-refund alongside the canceller that already returned it. An
   unresolved tentative debit is returned by its own guard, making "debited but
   never resolved" unrepresentable rather than merely tested. *Tests:*
   `reservation::tests::commit_cancel_race_one_winner`,
   `reservation::tests::overage_commit_publication_never_looks_refundable`,
   `reservation::tests::overage_commit_cancel_race_preserves_retry_classification`,
   `reservation::tests::fallback_commit_and_cancel_leave_exactly_one_funding_term`,
   `reservation::tests::a_fallback_that_loses_to_cancel_strands_no_overage_capacity`,
   `lease::tests::dropping_an_unresolved_tentative_debit_returns_the_credit`,
   `funding_terms_are_conserved_across_lapse_and_fallback`,
   `reservation::tests::split_commit_and_handle_cancel_have_exactly_one_winner`,
   `reservation::tests::a_shared_fallback_and_a_handle_cancel_leave_one_funding_term`,
   `reservation::tests::a_late_cancel_reports_the_full_charge_and_still_records_the_request`, and
   `engine::tests::dropping_the_worker_side_refunds_before_the_cancel_handle_does`.
   *Proofs:* `formal/lean/Tollgate/OveragePublication.lean` and
   `formal/lean/Tollgate/CommitFallback.lean`.

4. **Lease capabilities are exact and lease-scoped.** Every acquired lease is
   stamped with the next fencing token in its account's strictly increasing
   sequence, beginning at one; zero is not a persisted capability or counter.
   The sequence is an allocation and audit order, not an
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
   `acquire_surfaces_nonpositive_stored_fence` and
   `release_and_ingest_surface_nonpositive_stored_fence` (Postgres suite; the
   memory backend owns a counter seeded at one, updated only by checked
   increments); `zero_fences_are_refused_in_every_persisted_capability`
   (Postgres schema checks).

5. **Fail closed, zero I/O.** Unknown principal, suspended/closed account,
   expired snapshot, missing permission, cost overflow, accounting
   backpressure: all deny locally, under every enforcement mode. A lease that
   cannot fund the quote — absent, expired, or exhausted — also denies locally
   under `Strict`; it is the one condition `Elastic` may admit past, because it
   is the one that says something about *funding* rather than about validity
   (1). Nothing else in this list is mode-dependent. The request path performs no database, file, lock-file, or network
   access — not even on a miss.

   This is a rule about I/O, and it is worth saying what it is *not*, because
   two other documents used to cite it as forbidding locks outright. The hot
   path takes no *blocking* lock and reads no wall or business clock for a
   policy decision. A dependency's own bounded, non-blocking bookkeeping is a
   different thing and is permitted: `MokaSnapshotMap` lets moka's housekeeper
   drain its read log inline behind a `try_lock`, updating the frequency
   sketch and evicting at capacity, and `governor` reads a monotonic clock for
   bucket arithmetic. Neither can block a request and neither is a source of
   snapshot or lease truth. They are measured, not assumed: the allocation
   share by
   `moka_reads_stay_within_their_amortized_allocation_budget` (24), and the
   latency by the `admission/snapshot_lookup_moka` row, and — once its bound
   is recorded on the controlled host — by the at-capacity benchmark beside
   it, which exists because a moka cache below half its capacity does not
   enable its frequency sketch at all, so an under-filled benchmark prices
   neither the sketch nor eviction.
   No test asserts the absence of a `try_lock` on the request path; this
   invariant does not claim one does.

   A deny also says which kind it is: a request
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
   tightest ceiling accepted publications from principals sharing the account
   have presented. An accepted principal snapshot can tighten it even when its
   account-policy generation is older; a rejected replay cannot affect it. The
   ceiling never widens, so a key with a heavier cost table is never wedged by
   a split its sibling sized. Raw snapshot installs retain one defensive bucket
   because they do not carry that proof. A shard that cannot hold a request is
   not the account's answer: siblings are tried. If none admits now but at least
   one can hold the weight, the weighted refusal is `RateLimited`;
   `UnpriceableUnderLimits` applies only when no bucket can hold it.
   Both `RateLimited` and `RequestRateLimited` classify as `Retry::Transient`.
   Neither carries a retry instant or delay, and neither promises admission at
   an earliest retry time. Enforcement: payload-free denial variants and
   `DenyReason::retry` own the public contract; admission consumes the governor
   refusal without exposing its timing. *Tests:*
   `every_reason_has_the_expected_retry_class` (core deny tests),
   `rate_limiter_weights_by_cost`, and `request_rate_limiter_counts_requests_not_cost`.
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
   refused between ticks. Cold start keeps the interval as its backstop,
   having no debit to announce it. In a sharded
   lease, an early local low-water crossing wakes at most once per shard; if
   the aggregate is not low yet, the manager clears those doorbells and
   rechecks the aggregate so a concurrent crossing cannot be lost. Publishing
   The doorbell only removes that floor if nothing in the loop body puts one
   back: the release pass carries a single `store_call_timeout` across every
   parked lease, so a wedged backend cannot make refill latency grow with the
   parked count (#78).
   **A refusal is announced, and it is announced as a different thing.** A
   debit the lease cannot fund — for units or because its usability window has
   lapsed — raises the same doorbell, once per lease, and the plane is told
   *which* happened. The two are not degrees of one signal and cannot share a
   response. A crossing is anticipatory: the lease still serves, so the plane
   acquires alongside it and no request is refused meanwhile. A refusal is that
   failure already realised, and it is a statement about the grant's *size*
   rather than its depletion — a lease can sit above every threshold and still
   be too small for the quote offered, which is precisely the state adaptive
   allocation produces at the end of an allowance and low water then caps
   itself below. Acquiring alongside such a lease sizes the next grant against
   a balance its own unspent units are missing from, and installs the smaller
   answer; the plane must instead *consolidate*, folding those units back in as
   it re-grants (1's reachability clause).

   The units are exact only once the lease has quiesced, so consolidation takes
   it out of the slot first. That is a deny window, so quiescence is tested and
   never waited for: a lease with a reservation in flight goes straight back
   and the next refusal rings again. A consolidation whose outcome the store
   did not report is parked, never reinstated — the transaction may have
   committed, and serving from a lease the ledger has already credited back is
   the one outcome worse than the refusal being repaired.
   A consolidation interrupted by shutdown retains its pending-acquire marker:
   settling the predecessor cannot recover an unanswered replacement, whose
   capability remains uncertain until TTL reclaim. Successful consolidation
   records acquisition and settlement together inside `LeaseCounters`, so
   the runtime's terminal inventory excludes the settled predecessor.
   Timeouts and `Storage` acquisition errors also remain counted as uncertain
   while a manager runs and after a clean join; domain refusals do not. The
   same counter owner classifies ordinary and consolidating acquisitions.
   Witnesses: `shutdown_reports_an_unanswered_consolidation_grant` and
   `a_consolidated_predecessor_is_not_reported_as_crash_exposure`,
   `ambiguous_consolidations_remain_visible_after_a_clean_shutdown`, and
   `only_ambiguous_acquire_outcomes_increase_uncertainty`, with
   `AccountLifecycle.consolidation_preserves_parked_inventory` proving the
   exact-model inventory transition.
   The HTTP boundary also witnesses both delivered and cancelled acquisition
   results: `shutdown_accounts_for_unanswered_grants_over_http`,
   `shutdown_accounts_for_unanswered_grants_over_tls`, and
   `shutdown_accounts_for_unanswered_grants_over_mtls`. They reconcile billing
   and active grants at shutdown, preserve the reclaim grace period, and verify
   the exact unspent balance returns when the reported unanswered grant expires.
   Uncertainty counts possible grants, not units or confirmed transactions.

   Publishing a lease to N locality views is N swaps, so mutators are serialized: a
   reader straddles one publication exactly as it straddled the single-view
   slot's one swap, but the slot never *ends* a publication holding two
   different leases, which is what would let a locality keep spending past a
   revocation. Rotation and shutdown wait for every locality's independently
   reference-counted lease view before releasing the exact aggregate. Shutdown
   waits inside its own budget and *abandons* what has not quiesced by the
   deadline: releasing units a request may still spend cannot be undone, while
   abandoning them only defers their return to TTL reclaim (#9). The lifecycle
   order in #13 asks an embedder to quiesce before shutting down; this no
   longer depends on that, the predicate being the one the steady-state pass
   already applies (#62). A lease leaves this instance's books only by being
   released or by being reported.
   *Tests:* `refill_begins_on_the_crossing_debit_not_the_next_tick`,
   `shutdown_abandons_a_lease_an_in_flight_request_still_holds`,
   `fenced_release_clears_the_slot`,
   `a_refill_does_not_wait_behind_the_release_pass`,
   `a_burst_across_a_rotation_never_denies_a_funded_account`,
   `refill_installs_lease_on_cold_start`,
   `usability_window_rollover_returns_unspent_capacity`,
   `the_crossing_debit_raises_the_signal`,
   `a_lease_signals_at_most_once_however_long_it_drains`,
   `an_early_shard_signal_is_rearmed_until_the_aggregate_crosses`,
   `rotation_at_low_water_installs_fresh_lease`,
   `a_refused_debit_reports_a_refusal_and_never_a_crossing`,
   `a_refused_debit_tells_the_refill_plane_rather_than_waiting_to_be_polled`,
   `a_refusal_storm_rings_once_and_reports_once`,
   `a_refusal_outranks_a_low_water_crossing`,
   `a_sharded_refusal_reports_once_for_the_whole_grant`,
   `a_lease_with_no_doorbell_still_records_its_refusal`,
   `an_expired_lease_reports_its_refusal_rather_than_waiting_for_the_tick`,
   `a_refused_lease_consolidates_rather_than_stranding_the_tail`,
   `a_successful_consolidation_installs_the_grant_and_parks_nothing`,
   `consolidation_retains_a_grant_published_during_the_store_call`,
   `a_rolled_back_consolidation_returns_the_lease_to_the_slot`,
   `an_ambiguous_consolidation_parks_the_grant_rather_than_reinstating_it`,
   `a_settled_lease_falls_through_to_an_ordinary_acquire`,
   `an_over_claimed_fold_withdraws_readiness_and_keeps_serving`,
   `a_consolidation_defers_while_a_reservation_is_in_flight`,
   `shutdown_releases_unspent_units`,
   `sharded_slot_keeps_release_parked_while_any_local_view_is_held`,
   `racing_mutators_never_leave_a_slot_holding_two_answers`, and the
   `RefillRequests` handoff tests. Configuration ownership is witnessed by
   `mismatched_local_sharding_is_rejected_before_tasks_start`.

7. **Idempotent partial accounting.** Replaying a usage batch (same request
   IDs) never double-bills. Every successful mixed batch classifies each
   input exactly once as accepted, duplicate, or rejected; only accepted
   events change either ledger, and their grouped lease/account effects stay
   in the same atomic transaction.

   A duplicate is recognized before its payload is examined. A new event
   outside the backend's unit domain is rejected individually and does not
   claim its request ID. Memory retains its full `u64` domain; PostgreSQL's
   nonnegative `BIGINT` domain is `0..=i64::MAX`. Representable events whose
   combined deltas or resulting monotonic accounting totals overflow refuse
   the whole batch with `IngestError::Refused`; a retry cannot recover capacity.
   Stored corruption and operational failures remain retryable store errors.

   Credential activity consumes that accepted set too (35); a duplicate's
   changed key ID or timestamp cannot become fresh evidence.

   A batch that *fails* changes neither ledger. This is where the reference
   backend has to work for its living: `PostgresStore` gets it from one
   transaction and `finish_transaction`, while `MemoryStore` must reach the
   same outcome by computing every fallible value before it mutates anything
   — the rule `acquire` states and `set_account_status` splits into
   plan/apply. Applying as it walked left earlier events of a failed batch
   committed while the caller was told the batch failed, which inverts
   "partial data is surfaced, never silently absorbed": the replay counts them
   as duplicates, so a partial success is reported as a total failure and
   nothing says otherwise (#57).

   *Tests:* `usage_replay_is_idempotent`,
   `mixed_usage_batch_preserves_partial_acceptance`,
   `usage_accepts_zero_and_the_backends_unit_ceiling`,
   `overage_accounting_overflow_is_surfaced`,
   `a_failed_ingest_batch_leaves_the_ledger_untouched`, and
   `a_refused_deposit_moves_neither_column` (all mirrored across both store
   suites); `unrepresentable_usage_rejects_only_that_event_and_does_not_claim_its_id`
   (Postgres: `UsageEvent` cannot exceed MemoryStore's `u64` domain).

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
   forever and declares loss only at the final flush. The drain's budget is
   total wall clock, so its retry backoffs sleep into whatever remains and
   never past it: an overrun spends the margin `expiry_safety_margin +
   reclaim_grace` reserves (#12), and bounding by attempt count alone is not a
   bound (#18, #63). *Tests:* client writer
   overflow tests,
   `the_final_flush_backoff_cannot_overrun_the_drain_deadline`,
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

   Routine publication preserves ownership of the displaced grant. `LeaseSlot`
   exposes only `replace` and `take`, both returning a must-use handle; no
   convenience mutation silently drops it. The caller retains that handle for
   quiesced release or explicitly abandons it to TTL reclamation. `LeaseManager`
   owns retention through `publish_and_park`, including publications after a
   consolidation call while another publisher may have filled the slot.
   Enforcement: the API removes silent-discard operations; must-use diagnostics
   catch ignored outcomes; the manager retains displaced handles internally.
   A caller can still deliberately drop a handle, so this does not prove every
   external embedder releases it. *Tests:* `superseding_a_lease_hands_back_the_old_one`,
   `racing_mutators_never_leave_a_slot_holding_two_answers`,
   `consolidation_retains_a_grant_published_during_the_store_call`, and the
   compile-fail examples on `LeaseSlot`, `LeaseSlot::replace`, and `LeaseSlot::take`.

   Both backends retain exact nanosecond expiry and grace. PostgreSQL stores
   a canonical integer pair and compares it to `GrantPolicy::reclaim_cutoff`;
   MemoryStore uses the same checked cutoff. An underflow has no due expiry,
   and a deadline beyond Timestamp::MAX is never shortened to that instant.
   Migration 0017 retains an explicit conservative upper bound for legacy
   rows with lost precision and fences old lease SQL through a column rename.
   *Tests:* `nanosecond_lease_boundaries_preserve_release_reclaim_and_consolidation`,
   `a_grace_deadline_beyond_timestamp_max_never_reclaims_early`,
   `an_unrepresentable_replacement_expiry_leaves_the_original_grant_untouched`
   (both stores),
   `reclaim_cutoff_matches_an_independent_nanosecond_oracle`,
   `reclaim_cutoff_preserves_single_nanosecond_boundaries`,
   `durable_pairs_round_trip_and_preserve_order`,
   `lease_instants_preserve_epoch_edges_and_the_full_timestamp_domain`,
   `expiry_upgrade_preserves_accounting_and_fences_old_lease_queries`,
   `legacy_expiry_bounds_cover_both_sides_of_the_epoch`, and
   `invalid_legacy_expiry_rolls_back_upgrade_and_exact_rows_enforce_the_domain`.
   *Proof:* `formal/lean/Tollgate/LeaseTiming.lean`; finite representation and
   database behavior are separate test evidence. See `docs/LEASE_TIMING.md`
   for migration bounds, deployment and recovery.

   The server owns and observes its maintenance task. Each reclaim and rollover
   outcome is published independently; readiness requires both to have completed
   successfully and the publisher to remain alive. A first failure withdraws
   readiness before the next operation can suspend. Failure of one pass does
   not prevent the other from running. Unexpected task completion, cancellation
   or unwinding panic stops the listener with an operational error. Graceful
   shutdown and owner cancellation withdraw readiness before aborting the task;
   a late success cannot restore it. These are observed-outcome/liveness claims,
   not a new deadline on a pending backend call or proof that another replica
   holds no skipped rows.
   Enforcement: `maintenance::Publisher`, `Monitor`, `Task` and `serve`.
   *Tests:* `readiness_requires_both_passes_and_tracks_independent_failure_and_recovery`,
   `a_panicked_maintenance_call_stops_the_server_with_a_safe_error`,
   `graceful_shutdown_cancels_pending_maintenance_without_failure_events`,
   `cancelling_the_server_drops_its_shutdown_future`,
   `owner_drop_withdraws_readiness_before_abort_is_polled`,
   `closed_publication_cannot_preserve_a_healthy_last_value`, and
   `cancellation_without_a_stop_request_closes_maintenance_health`.
   `Tollgate.ServerMaintenance` proves independent failure, recovery, and terminal
   stop/exit properties over atomic observations. Rust tests witness the watch,
   task, HTTP and finite-counter implementation boundaries separately.

10. **Ready means currently admissible.** An instance reports ready only while
    its snapshot resolutions meet the bar below, it can still fund work, and
    its snapshot and refill/accounting tasks are alive. Readiness falls again
    on exhaustion, expiry, or task exit; fail-closed correctness must not
    masquerade as availability.

    Snapshot and lease tasks own the sole health publisher through
    `TaskHealth`. Its destructor stores false before closing the channel on
    normal return, unwind or cancellation, including cancellation before the
    first poll. Retained watch receivers cannot preserve a true value after
    the task exits. Aborting a task requests cancellation; this guarantee
    applies when its future is destroyed, not before the executor processes
    the abort. Whole-process abort has no surviving in-process observer.
    *Tests:* `snapshot_readiness_is_false_after_shutdown_or_owner_drop`,
    `a_snapshot_task_panic_withdraws_the_retained_readiness_value`,
    `lease_health_is_false_after_an_unpolled_abort_or_panic`,
    `shutdown_releases_unspent_units`, and
    `cancelling_lease_shutdown_aborts_the_owned_release_task`.

    "Can still fund work" is the same question admission asks, and it is
    mode-dependent for the same reason (1). Under `Strict` it is a lease inside
    the local usability window with units left. Under `Elastic` an empty or
    absent lease is not the end of the answer: an account with overage headroom
    *is* admissible, and reporting it unready would withdraw from rotation
    exactly the instances the mode exists to keep serving — availability
    masquerading as fail-closed correctness, the same error in the other
    direction. Readiness reads the mode from the snapshot the request path
    reads, never a copy, so the two cannot disagree after a republish.

    Elastic readiness does not establish that the first lease has arrived.
    A workload that requires lease-funded requests must observe installed
    funding separately; a ready instance with only overage headroom may bill
    every admission as overage. *HTTP tests:*
    `an_elastic_account_serves_past_its_deposit_and_bills_the_overage` and
    `elastic_readiness_serves_before_the_first_grant_and_recovers_after_funding`.

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

    `InstanceRuntime` applies the same split to funding: `All` requires some
    fresh active account with usable lease units or elastic headroom; `Fixed`
    requires every eligible account to be fundable. Freshness and funding must
    belong to the same account. The runtime evaluates resolution deadlines at
    the probe's supplied timestamp and reports unresolved and unfundable
    counts alongside readiness. With no positive snapshots, the instance is
    healthy once its tracked resolutions and background tasks meet their
    requirements. Positive snapshots that are all inactive or stale do not
    advertise available funding.
    Tests: `an_exhausted_account_withdraws_a_fixed_instance_but_not_a_discovering_one`,
    `readiness_checks_freshness_at_the_callers_time_before_a_background_wakeup`, and
    `a_fixed_instance_with_only_inactive_accounts_does_not_advertise_funding`.

    Readiness is a single bit either way, so it says *that* an instance is
    unready and never *how much* is unresolved; the count of principals
    without a valid resolution is exported alongside it and is derived from
    the same pass that decides the resolution part of readiness. Task liveness
    is separate: an exited task can be unready with a last unresolved count
    of zero. Enumeration failures are counted apart from fetch
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
    schema additionally CHECK-constrains unit columns non-negative, including
    billing-event units, and every persisted fence strictly positive.
    *Tests:* `tollgate-core` proptests;
    `negative_account_column_fails_conservation_read`,
    `negative_account_column_fails_balance_and_usage_reads`,
    `negative_lease_sum_fails_conservation_read`,
    `acquire_surfaces_negative_stored_balance`,
    `reclaim_refuses_negative_credit`,
    `straggler_exceeding_recorded_loss_fails_ingest`, and
    `checked_ledger_columns_reject_negative_writes` (Postgres suite; the
    memory backend makes negative state unrepresentable via `u64`);
    `usage_guard_upgrade_preserves_legacy_rows_and_refuses_an_old_catalogue` and
    `invalid_history_blocks_validation_but_leaves_write_guards_and_can_be_repaired`
    (Postgres migration suite).

12. **No commit outside the usability window.** A lease is locally usable
    until `expires_at - safety margin`; both debits and commits stop there,
    and the allocator reclaims only after `expires_at + grace` (accepting
    releases and late usage through the window). Work committed inside the
    window therefore always has margin + grace to be flushed and billed;
    work cannot commit against capacity the allocator may have re-granted.

    Under `Elastic` a lapsed window is not a refusal but a change of funding:
    the charge settles against overage and the lease receipt is returned. The
    bill it emits therefore names **no lease capability**, and that follows
    from the terminal phase rather than the funding receipt. This is not a
    stylistic choice — the sink rejects a leased event whose lease has been
    reclaimed, and a lapsed lease is precisely one about to be reclaimed, so
    billing a fallback against its receipt would silently drop the charge for
    work that ran. Under `Strict` the lapse still releases for zero and the
    kernel must not run.
    *Tests:* `reservation::tests::{commit_after_window_closes_releases_for_zero,
    safety_margin_closes_window_before_expiry,
    an_elastic_lapse_at_execution_start_bills_as_overage_with_no_lease_capability,
    a_strict_lapse_at_execution_start_releases_for_zero_and_yields_no_event}`,
    `engine::tests::a_strict_expiry_at_execution_start_produces_no_committed_guard`,
    `reclaim_waits_for_grace_and_release_works_within_it` and
    `a_commit_time_fallback_is_ingested_as_overage_after_its_lease_is_reclaimed`
    (both store suites).
    *Proof:* `formal/lean/Tollgate/CommitFallback.lean`.

13. **A committed charge is always emitted.** The usage slot is bound at
    admission, not at commit: `RequestContext::admit` takes the pre-reserved
    queue permit and carries it through `Pending` and `ReadyToStart` into
    `Committed`, whose `Drop` records the billing event and only then releases
    concurrency and execution capacity. Normal completion, early return, panic
    unwind, and task abort therefore all enqueue the event. Binding the slot
    one stage earlier than the charge removes the window in which a committed
    request had no place to be billed. `ReadyToStart::commit` returns the
    guard directly, rather than hiding it in a tuple, and `Committed` is
    `#[must_use]`, so discarding execution-start evidence is rejected under
    `unused_must_use`. The guarantee extends through shutdown: the
    writer's drain waits for the permit, so the event is ingested or explicitly
    counted in `WriterStats::unresolved` — never silently dropped. The safe lifecycle
    order is: stop admitting, quiesce request tasks holding permits or
    guards, shut the usage writer down, then release leases. A spent lease
    with no billing event requires losing the whole process.

    `InstanceRuntime` owns this order under one total deadline: stop snapshot
    discovery and pause refills, close the accounting queue and drain issued
    permits/guards, then release all account leases concurrently. A task still
    holding a lease prevents its release and is reported at the deadline.
    Concurrent requests and background failures share the immutable deadline
    sampled inside the first shutdown publication.
    Dropping the owner or cancelling shutdown aborts owned tasks; the three
    component shutdown futures retain their join handles until completion.
    HTTP embedders start their own bounded quiescence with the runtime's first
    shutdown request, rather than awaiting an unbounded server drain first.
    Tests: `shutdown_waits_for_committed_usage_before_returning_the_grant`,
    `shutdown_with_an_unresolved_permit_still_obeys_the_total_deadline`, and
    `a_shutdown_deadline_shorter_than_its_phases_is_rejected`,
    `cancelling_writer_shutdown_aborts_the_owned_ingest_task`,
    `cancelling_lease_shutdown_aborts_the_owned_release_task`, and
    `shutdown_interrupts_normal_ingest_before_its_long_timeout`,
    `repeated_shutdown_requests_share_the_first_deadline`,
    `http_shutdown_joins_the_listener_and_settles_usage_before_returning`, and
    `http_quiescence_is_bounded_by_the_runtime_deadline`,
    `a_runtime_stop_closes_the_queue_and_bounds_the_actual_drain`, and
    `a_held_reservation_exhausts_and_reports_the_shared_shutdown_deadline`, and
    `cancelling_http_shutdown_aborts_the_server_before_or_after_first_poll`.

    Unwinding is safe on Tollgate's side by construction: the event is built at
    commit rather than at drop, so `Drop` takes no lock that could be poisoned,
    allocates nothing, and cannot fail — and the shared cancel path is a
    compare-exchange rather than a mutex precisely so it stays usable from a
    thread that is already panicking. **The panic boundary around the kernel
    belongs to the consumer**, because Tollgate does not run the kernel and
    cannot wrap it. A panic in a Rayon `spawn` closure propagates at the join
    and can abort a pool thread, so without a consumer-installed `catch_unwind`
    the guard is leaked rather than dropped on the worker, and a leaked guard
    emits nothing. Under the `production` profile's `panic=abort` unwinding
    does not exist and the process-loss boundary above is the whole story.
    *Tests:*
    `panic_after_commit_still_bills`,
    `a_panicking_kernel_under_catch_unwind_still_bills`,
    `shutdown_waits_for_committed_guard`,
    `committed_charge_holds_concurrency_until_execution_guard_drops`, and
    `failed_commit_releases_concurrency_and_accounting_capacity`, plus the
    `Committed` discarded-result and `CancelHandle` cannot-commit compile-fail
    doctests.

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
    Reclaiming bounded local history is a separate control-plane transition:
    it removes visible state and invalidates that incarnation's outstanding
    reads. Reopening requires a new authoritative read, linearizable against
    durable source publications and tombstones. A private map/principal/read
    fence enforces this boundary before account-policy resolution. Pending
    reconstruction refuses pushes; an unknown response cannot authorize them.
    Source-read freshness remains an explicit source contract, not something
    the cache can infer from a generation number or timestamp.
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
    `formal/lean/Tollgate/SnapshotCache.lean` and
    `formal/lean/Tollgate/SnapshotHistory.lean`. Retention witnesses:
    `reclaimed_history_requires_a_fresh_authority_before_replay`,
    `a_recreated_principal_rejects_the_old_in_flight_response`,
    `refresh_fences_are_bound_to_the_map_principal_and_whole_batch`,
    `a_refused_push_invalidates_an_older_reconstruction`,
    `an_unknown_revalidation_cannot_reopen_forgotten_history`,
    `a_source_read_cannot_overwrite_a_concurrent_revocation`,
    `reclaimed_principals_refetch_authority_instead_of_replaying_a_push`,
    `an_unseen_principal_needs_authority_once_history_is_full_or_has_reclaimed`,
    `a_push_batch_may_not_exceed_the_room_history_has_left`, and
    `a_push_for_an_unresolved_principal_is_discovery_not_authority`.

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
    `invalid_writer_config_is_rejected`,
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

    Lease TTLs cross HTTP without truncation or saturation. `LeaseTtl::try_from`
    rejects nonpositive caller input before a request is built; its wire
    representation retains positive whole `u32` seconds or carries the exact
    `SignedDuration` with a zero legacy sentinel. `LeaseTtl::duration` refuses
    nonpositive or conflicting declarations before either acquire or
    consolidation invokes a backend. The allocator alone applies its policy
    ceiling. Fractional and wide positive TTLs remain valid configuration.
    *Tests:* `whole_second_ttls_keep_the_legacy_wire_contract`,
    `precise_ttls_carry_an_exact_string_and_a_refusing_legacy_sentinel`,
    `every_positive_duration_round_trips_in_both_lease_requests`,
    `nonpositive_and_ambiguous_ttls_are_rejected`,
    `malformed_or_partial_ttl_fields_are_not_repaired`,
    `duplicated_ttl_fields_are_rejected_inside_both_request_envelopes`,
    `http_acquire_preserves_positive_ttl`,
    `http_consolidation_preserves_positive_ttl`,
    `postgres_and_http_preserve_ttl_across_acquire_and_consolidation`,
    `invalid_wire_ttls_never_debit_or_settle_a_lease`,
    `legacy_servers_reject_precise_ttls_and_invalid_input_never_reaches_http`,
    and `fractional_and_wide_lease_ttls_remain_valid_configuration`.

    Performance-gate configuration rejects unknown keys before policy is used.
    This includes the benchmark manifest, its trust policy and bounds, recorded
    baseline settings and load thresholds. A misspelled optional key cannot
    activate its default or disable a comparison. Explicitly documented comment
    and reserved-ID metadata is inert; it is not an open extension namespace.
    Omitted optional trust settings and legacy baseline defaults retain their
    existing meanings. Every operational load setting remains required; `null`
    disables a nullable bound only when its key is explicitly present.
    Enforcement is inside each owning serde decoder. These are schema and CLI
    witnesses, not new numerical or performance proofs. *Tests:*
    `misspelled_gate_settings_never_become_defaults`,
    `partial_trust_settings_preserve_explicit_values_and_omitted_defaults`,
    `gate_metadata_is_explicit_and_does_not_hide_unknown_settings`,
    `baseline_setting_typos_are_rejected_without_changing_legacy_defaults`,
    `every_load_setting_is_required_independent_of_json_layout`,
    `load_settings_accept_documented_comments_and_reject_unknown_keys`,
    `unknown_manifest_keys_fail_before_measurement_reads_or_output_changes`,
    `unknown_baseline_keys_cannot_be_used_for_comparison_or_promoted_over`, and
    `unknown_load_settings_produce_parse_errors_in_both_verdict_modes`.

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
    Both maps also bound retained generation histories and their eviction
    index, including pending reconstructions. The client removes reclaimed
    resolution/deadline entries before source I/O, batches reads within that
    budget, and rejects fixed sets exceeding it before tasks start. Visible
    eviction alone is repaired by an equal-generation refresh without making
    a background probe count as a request-frequency hit. Reclamation cannot
    reset account lease slots or irreversible spend. This local-history bound
    does not bound the authoritative catalogue or account spend history.
    *Retention tests:* `generation_churn_is_bounded_in_both_maps`,
    `churn_bounds_both_history_and_its_index`,
    `history_reclamation_bounds_resolution_and_deadline_indexes`,
    `a_superseded_publication_cannot_claim_a_resolved_deadline`,
    `a_discovered_catalogue_larger_than_history_is_refreshed_in_bounded_batches`,
    `a_fixed_set_larger_than_history_is_rejected_before_tasks_start`,
    `same_generation_refresh_repairs_a_visible_cache_eviction`,
    `a_refresh_pass_reserves_at_the_retention_budget_not_per_principal`, and
    `the_visibility_probe_tracks_installation_and_eviction_in_every_map`.
    An embedder's own map inherits the unbounded compatibility defaults
    instead, which both maps here override and therefore never execute:
    `a_map_without_reclamation_never_demands_a_refresh_and_retains_everything`,
    `the_default_visibility_probe_answers_from_the_request_lookup`,
    `the_default_batch_writes_dispatch_every_update_variant`,
    `the_default_install_many_publishes_the_whole_batch`, and
    `an_unfenced_reservation_round_trip_publishes_its_reads`.
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

18. **Background store calls are wall-clock bounded, and so is every pass
    over them.** No background task may be parked by a backend that hangs
    rather than answering: every allocator, sink, and snapshot-source call a
    client task makes carries a configured timeout, and each graceful shutdown
    carries a total budget, so shutdown terminates whatever the backend does. Bounding by
    retry *count* alone is not a bound, and neither is a per-call bound on a
    pass that makes `N` calls: the lease manager's release pass carries one
    `store_call_timeout` across every parked lease, so no loop-body cost
    scales with the parked count (#78). Every long await in a background
    loop's body is raced against that task's shutdown watch, so the signal is
    acted on where it arrives rather than at the next loop top — this is what
    makes `LeaseManager::shutdown`'s documented `shutdown_release_deadline`
    bound true. A bound must also let the loop *return*: a fetch future that
    never resolves is abandoned at `fetch_timeout` rather than awaited
    forever, because a `JoinSet` that cannot empty stops the snapshot sweep
    from returning at all, and readiness then falls without ever recovering
    (#103). An abandoned call is counted apart from a refusal — `refresh_timeouts`
    beside `refresh_failures`, as `acquire_timeouts` sits beside the lease
    manager's refusals — because a timeout is not a domain answer: the backend
    may have done the work and simply not said so in time. Racing a call
    against shutdown is not a substitute for bounding it: it frees the
    shutdown path and leaves every other caller parked, which is how principal
    enumeration stayed unbounded after its cancellation was fixed (#59). What
    a bound could not complete is
    reported — a lease left unreleased is `LeaseManagerReport::abandoned` and
    settles at TTL reclaim (#9); an undelivered batch is `WriterStats::lost` —
    never silently assumed done.
    *Tests:* `hung_ingest_cannot_stall_shutdown`,
    `hung_ingest_times_out_into_the_retry_path`,
    `hung_release_times_out_and_reparks`,
    `hung_release_cannot_stall_shutdown`,
    `shutdown_during_a_hung_acquire_is_not_delayed_by_it`,
    `a_hung_enumeration_does_not_hold_shutdown_open`,
    `a_hung_enumeration_is_abandoned_so_the_loop_keeps_sweeping`,
    `a_zero_enumeration_timeout_is_rejected`,
    `a_hung_fetch_is_abandoned_so_the_sweep_keeps_running`,
    `a_zero_fetch_timeout_is_rejected`,
    `lease_manager::tests::{a_release_pass_costs_one_budget_whatever_the_parked_count,
    a_lease_that_eats_the_budget_yields_its_place,
    shutdown_during_a_release_reparks_every_lease}`, and
    `invalid_pool_config_is_rejected_before_connecting`.

    `PeriodRoller` bounds each administrative rollover call and the entire
    multi-batch pass, including yields between batches. Each pass freezes one
    business-time cutoff; the next pass starts after a configured pause, so a
    long pass cannot produce a burst of catch-up retries. Shutdown interrupts
    the pending call and also has its own total join deadline. These bounds
    assume store futures cooperate with the executor; synchronous blocking
    cannot be preempted by a Tokio deadline. Witnesses:
    `one_pass_budget_bounds_every_batch_and_preserves_its_progress`,
    `a_pass_deadline_can_interrupt_a_call_before_its_own_timeout`,
    `a_hung_call_times_out_and_the_next_pass_recovers`,
    `shutdown_interrupts_a_hung_call_and_reports_its_uncertainty`, and
    `shutdown_has_a_total_deadline_even_if_its_task_does_not_observe_the_signal`.

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

    Snapshot generation decisions own their refusal evidence: both push and
    refresh paths emit one structured warning per refused positive or
    revocation and increment `SnapshotStats::refused_updates` once. Events
    identify the origin, principal, offered kind/generation and retained
    watermark. An unchanged visible positive at its current generation is
    an idempotent no-op, not a refusal to report. Accepted absences and
    revocations are not counted. Acceptance, retained deadlines, publication
    and retry backoff retain the shared generation model's semantics (15).
    *Tests:* `snapshot_refusals_are_reported_and_counted_for_pushes_and_refreshes`,
    `unchanged_snapshots_and_accepted_updates_do_not_report_refusals`,
    `a_push_reinstates_an_absent_principal_at_its_own_generation`, and
    `a_refused_answer_is_retried_with_backoff_not_at_source_latency`.

    Release refusal severity and ownership remain consistent at shutdown:
    invalid counts are an error and abandoned release, never a clean
    settlement. The shared classification already enforces this in refill
    and shutdown. *Tests:* `refused_release_at_shutdown_is_reported` and
    `shutdown_distinguishes_settled_leases_from_unconfirmed_or_invalid_releases`.

    Server reclaim and rollover failures each carry an independent checked
    consecutive-failure count: the first two are `warn`, the third and later
    are `error`. Recovery emits one `info` with the preceding count and resets
    the streak. Three attempts is an alerting policy, not a derived TTL bound;
    readiness falls on the first failure. Counter exhaustion stops maintenance
    and is explicit, never wrapping or silently saturating. Task-exit events
    contain only a static reason, never a backend or panic payload (37).
    *Tests:* `persistent_failures_escalate_and_recovery_resets_each_operation`,
    `failure_counter_exhaustion_withdraws_health_without_wrapping`, and
    `readiness_matches_both_operation_outcomes_for_every_short_trace`.

    The separately owned security reloader reports unexpected task exit at its
    drop boundary, without exposing panic text. Its owner marks deliberate stop
    before abort, including before the task's first poll. This does not change
    the validity of the previously installed security configuration or signing
    keys. *Tests:* `a_dead_security_reloader_reports_a_safe_error` and
    `dropping_an_unpolled_reloader_is_an_expected_stop`.

20. **Every admission outcome is counted, exactly once, under its own reason.**
    The request path may not log (5), so its tallies are the only account it
    can give of itself; an instance refusing every request must be
    distinguishable from one serving none. The snapshot map owns one shared
    counter identity and installs its `Arc` into every request state.

    A request has **three** deciding stages, and each records exactly one
    outcome. `begin` records a stage-one refusal. A successful context records
    one stage-two outcome when `admit` consumes it. An admitted request then
    records one *terminal* outcome at execution start: it started executing,
    it was shed by the capacity gate, it was refused by its own funding, or it
    resolved for zero before starting. Those four partition `admitted`
    exactly — `execution_started + capacity_shed + refused_at_start +
    canceled_before_start == admitted` — and the partition is enforced by
    construction rather than by call-site discipline: the execution-lifetime
    concurrency guard is the one value every admitted request holds exactly
    once, so its `Drop` records the zero-charge outcome for any request no
    later phase claimed, including a pending state that was simply abandoned.

    A context dropped before stage two creates neither pending funding nor an
    admission outcome, and is counted as `contexts_abandoned` — its own phase,
    neither an admission nor a denial. Counting it is the point: an instance
    authenticating a flood of requests whose bodies never arrive would
    otherwise be indistinguishable from one serving nothing.

    **No later outcome joins the pre-admission denial total.** A request
    refused at execution start was already counted under `admitted`, so adding
    it to `denials` would give one request two contradictory identities;
    `denied()` therefore stays pre-admission and `refused_at_start()` reads the
    later phase. A cancellation that wins Tollgate's compare-exchange is a
    Tollgate outcome and is recorded here, not left for the consumer to infer.
    Commit-time refusals get their own dense vocabulary rather than a second
    twenty-one-slot table: only four refusals can reach commit, and
    `CommitRefusal::index` keeps `DenyReason`'s forcing function — an
    exhaustive match, stable slots pinned by literal — at the size the outcome
    space actually has.

    The two elastic qualifiers stay disjoint. `admitted_overage` counts
    admissions no lease could fund; `committed_at_overage` counts admissions a
    lease *did* fund whose window lapsed before execution start (3, 12). They
    answer different operational questions and an account can produce either
    without the other, so neither is a subset of the other and adding them is
    never correct.

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
    total. The qualifier may equal the total when every admission is unfunded;
    a strict inequality requires at least one lease-funded admission. *Tests:*
    `indices_cover_every_slot_exactly_once`,
    `shipped_slots_and_labels_never_move`,
    `labels_are_distinct_and_payload_free`, `payload_does_not_affect_the_slot`,
    `counters_attribute_every_outcome`, `each_reason_reaches_its_own_slot`,
    `denied_requests_add_no_units`, `concurrent_increments_are_not_lost`,
    `shared_counters_follow_engines_and_owned_contexts`,
    `metrics_separate_admissions_from_each_kind_of_refusal`,
    `an_elastic_account_serves_past_its_deposit_and_bills_the_overage`,
    `elastic_readiness_serves_before_the_first_grant_and_recovers_after_funding`,
    `an_overage_admission_is_counted_twice_over_and_a_refusal_once`,
    `every_admitted_request_reaches_exactly_one_terminal_counter`,
    `an_abandoned_context_is_counted_and_denies_nothing`,
    `a_consumed_context_is_never_counted_as_abandoned`,
    `a_commit_time_funding_refusal_is_counted_under_its_own_reason`,
    `a_commit_time_fallback_is_counted_under_its_own_qualifier`,
    `an_admission_time_overage_is_not_counted_as_a_commit_time_one`,
    `commit_refusal_indices_cover_every_slot_exactly_once`,
    `shipped_commit_refusal_slots_and_labels_never_move`,
    `every_commit_time_funding_refusal_reaches_a_slot`,
    `transition_counters_never_move_the_denied_total`,
    `a_sharded_layout_reports_the_same_transition_totals`, and
    `concurrency_guard_carries_the_exact_occupied_state`.

21. **Every opaque identifier has one portable wire spelling.** `AccountId`,
    `KeyId`, `LeaseId`, `RequestId`, and `Principal` are exactly 32 lowercase
    hexadecimal characters without a prefix in human-readable serialization
    and URL paths. `PolicyRevision` is the same rule at 64 characters, being
    256 bits rather than 128.

    One rule, at two widths — `validate_hex_digits` is the single function
    both go through, and `ParseIdError` carries the width it was applying so a
    caller is never told a 64-digit value should have been 32. Two parsers
    that had to agree would be the defect; the widths are nevertheless
    enforced separately, and each rejects the other's canonical form.
    Strictness earns its keep differently for the revision: nothing in
    Tollgate reads it, but a consumer compares it for equality to select its
    own metadata, so two spellings of one revision would silently look like
    two policies. A malformed path is a structured `invalid-id`, never an
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
    `revision_text_round_trips_the_full_256_bit_domain`,
    `noncanonical_revision_spellings_are_rejected`,
    `the_two_identifier_widths_reject_each_others_canonical_form`,
    `the_parse_error_names_the_width_it_expected`,
    `revision_serde_is_textual_and_strict`,
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
    `Verified::reusable_until` carries it, and `HmacRegistry` populates it
    from the credential's own `not_after` when its record carries one (#104).
    A key without one is indefinite: it expires only by withdrawal, and
    withdrawal travels by snapshot.

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

    Moka's access-log housekeeping is dependency-owned amortized allocation,
    not a strict zero-allocation promise for every individual cache read. Its
    separate allocation witness enforces the existing per-call average budget
    over its batch; Tollgate-owned admission scopes remain zero.

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
    `moka_reads_stay_within_their_amortized_allocation_budget`, and
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
    field of `RequestContext`, `Pending`, `ReadyToStart`, and `Committed` is
    private, and the guard moves from `Pending` through `ReadyToStart` into
    `Committed` without ever exposing a raw `Reservation`: cancellation
    consumes the state that holds it, and commit consumes it into the
    execution guard. The public commit result is the must-use guard itself,
    not a tuple that suppresses its diagnostic. Safe code therefore cannot
    retain committed funding while accidentally dropping the concurrency
    authority. *Witnesses:* the `Pending` private-reservation compile-fail
    doctest (`E0616`, with its companion that pins the refusal to the field
    rather than to a vanished API), and the `Committed` discarded-result
    doctest under `unused_must_use`.
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
    `Pending` raw-reservation compile-fail doctest — pinned to E0616 and
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

26. **A staged request retains its principal generation and reads one current
    account authority at admission.** `AdmissionEngine::begin` performs the one
    principal lookup and returns an owned `RequestContext` containing the exact
    `Arc<AccountAdmissionState>` and its selected locality. The installed state
    pins its immutable snapshot; a later publication cannot change its principal
    permissions, batch limit, cost table or policy revision. Account-wide rate
    and concurrency policy is shared through one authority, loaded once in
    stage two (5). Retaining an old independently spendable rate bucket would
    multiply capacity. Stable concurrency gauges remain shared across
    generations because resetting live occupancy would violate their ceiling.
    For principal validity, stage two consults the pinned snapshot: it rechecks that snapshot's expiry
    boundary and tests the workload's per-class work permissions against the
    permissions that snapshot pinned. Both read the context it is consuming,
    never the map, so neither can observe a generation the request did not
    begin with. Route permission is checked once, in `begin`; work permission
    cannot be, because which classes a request touches is a property of its
    decoded body. Status or permission changes published after `begin` govern
    the next request, never splice two generations into one.

    This is enforcement-ladder rung 1 for ownership and single use: the
    context owns the state, has no engine borrow, is not cloneable, and
    `admit(self, ...)` consumes it. Map-call cardinality remains the rung 3
    witness in (24). *Tests:*
    `a_workload_requiring_ungranted_bits_is_denied_at_stage_two`,
    `a_workload_within_granted_bits_admits`,
    `a_staged_context_keeps_principal_policy_after_republication_and_owner_drop`,
    `stage_two_checks_the_pinned_expiry_after_republication`,
    `a_staged_context_observes_account_rate_published_after_begin`,
    `limit_change_is_one_account_authority_for_every_principal`, and
    `admit_consults_the_map_exactly_once`.

Ledger roles (context for 1 and 7): leases **bound** spend; usage events **are**
the billing record; reconciliation compares the two and steady-state drift is
zero. Per account, exactly:

```
deposited + overage_recorded
    == balance + active lease grants + settled usage + settlement loss + expired
```

Read it as a funding statement: the left side is everything the account was
ever funded with — money in, and credit extended under `Elastic` (1) — and the
right side is where those units now sit. `overage_recorded` is a *funding*
term, not a bucket: overage usage also lands in settled usage, so without it
the equation would fail by exactly the overage and reconciliation would report
corruption on a correctly working ledger. `expired` is the mirror of that on
the sink side: units a closed budget period took away (28) are neither
spendable nor billable, and without a resting place the equation would fail by
exactly the units that expired. Both sides use checked arithmetic and an
overflow answers "violated" rather than wrapping (11), because this equation
exists to detect corrupt state and must not be able to launder it.
*Tests:* `conservation_requires_an_exact_equation_without_overflow`,
`overage_funds_the_usage_it_bills`,
`expiry_accounts_for_an_allowance_that_was_never_spent`,
`overflowing_the_funding_sum_is_a_violation_not_a_wrap`,
`overage_usage_is_billed_and_funds_itself` and
`settlement_is_unaffected_by_an_account_carrying_overage` (both store suites);
`Tollgate.Conservation.overage_preserves_conservation`,
`unfunded_overage_always_breaks_conservation`,
`rollover_preserves_conservation` and
`unrecorded_expiry_always_breaks_conservation`.

27. **A credential is durable before it is disclosed, and its digest table is
    a projection.** Credential lifecycle is control-plane state like every
    other: the durable record lives behind [`KeyDirectory`] and the verifier
    holds a read projection of it, installed whole. Two orderings are
    load-bearing and neither is recoverable if broken. **Durability before
    disclosure:** the record commits before the secret is returned to anyone,
    because a crash between the two leaves a credential the server has never
    heard of, and the digest cannot be recovered from the record — that being
    the point of storing digests. **Replacement, never merge:** a projection
    that merged would keep verifying a credential the directory has already
    retired, so the directory decides which credentials are live and the
    registry reflects a complete committed revision of that set. A refresh
    window can lag later commits; failed refresh preserves the prior table
    without extending its finite deadline (34).

    Last-committed activity is derived separately from canonical accepted
    usage (35), never from or into this projection.

    The secret never reaches a store and the server secret never leaves the
    verifier, so neither half alone verifies or mints anything — the property
    HMAC is paid for, preserved across the persistence boundary rather than
    only within one process.

    Revocation is durable and terminal, like a snapshot tombstone (#15): a
    retired credential is never resurrected, and issuance refuses to overwrite
    an existing `KeyId` rather than silently retiring what it replaces. A
    credential's own `not_after` is enforced by the directory, so every
    backend answers "active" the same way. `KeySource` pages bind records to
    a revision in the same backend read. The manager accepts only a complete
    coherent drain and narrows it with source and instance expiry checks. The
    projection may cross the instance-only secured control-plane link (32);
    it cannot invent liveness absent from the selected ledger revision.

    New credential expiry is preserved at nanosecond precision through every
    backend read and projection. PostgreSQL's `StoredInstant` representation
    and schema constraints own the exact pair; a missing component is a storage
    error, never an indefinite credential. Migration 0018 marks legacy finite
    rows with the earliest compatible expiry and fences old readers/issuers.
    It cannot extend source authority. Existing cached proofs must be cleared
    during the coordinated upgrade described in `docs/CREDENTIAL_PROJECTION.md`.
    *Tests:* `credential_expiry_is_exact_in_directory_and_every_page` (both
    stores), `credential_expiry_preserves_the_final_fractional_second`,
    `credential_expiry_upgrade_bounds_legacy_authority_and_fences_old_queries`,
    `invalid_credential_history_or_revision_overflow_preserves_the_old_schema`,
    `credential_expiry_constraints_and_readers_refuse_incomplete_evidence`,
    `an_indefinite_catalogue_migrates_without_inventing_expiry_or_revision`,
    `memory_expiry_reaches_http_projection_and_session_exactly`,
    `postgres_expiry_reaches_http_projection_and_session_exactly`, and
    `repairing_source_expiry_requires_resetting_preupgrade_cached_proofs`.
    *Proof:* the legacy lower-bound and reset-session theorems in
    `formal/lean/Tollgate/CredentialProjection.lean`; actual schema and session
    behavior are separate implementation witnesses.

    Per-credential withdrawal reaches the request path by the mechanism that
    already exists: a `Principal` *is* the credential's digest fingerprint, so
    revoking one is `install_revoked` for that principal, and admission
    already denies a tombstoned principal on every request regardless of what
    any verifier's cache holds.

    *Tests:* `a_minted_credential_verifies_until_it_is_revoked`,
    `a_credentials_own_expiry_travels_to_the_verifier`,
    `rotation_keeps_both_credentials_live_until_the_old_one_is_retired`,
    `issuance_refuses_a_duplicate_key_or_an_unknown_account`, and
    `minting_never_repeats_a_credential`,
    `credential_diagnostics_never_disclose_issuer_or_customer_secrets`; plus the paging, freshness and
    transport witnesses in 34.

28. **A budget period is crossed exactly once, and only its allowance
    expires.** An account may carry a [`BudgetSchedule`]: an allowance
    replenished each period, with unspent units that do not carry over. The
    balance is therefore two buckets — the current period's allowance and
    manual top-ups — spent allowance-first, so the units with an expiry date
    go first. A rollover deposits one allowance, expires what is left of the
    old one, and moves the account's period marker, all in one transaction.
    Manual top-ups are never touched by a boundary; that is what the split
    exists to guarantee, and a single balance could only expire both or
    neither.

    **Crossing is the store's job, not the caller's.** The scheduled pass runs
    on every control-plane replica, so two of them will race a boundary: the
    backend crosses it under a row lock (a mutex in `MemoryStore`,
    `FOR UPDATE SKIP LOCKED` in `PostgresStore`) guarded on the stored period,
    and the loser finds the account already current. A caller that read the
    period and then rolled would produce two deposits under exactly that race.
    One allowance is deposited however many boundaries have passed — an
    account left unrolled for two months is entitled to what its schedule
    gives it now, not to a backlog. The pass is bounded per transaction and
    its caller drains saturated batches, because every scheduled account comes
    due at the same instant.

    **Direct-store scheduling has an owner.** `PeriodRoller` runs an immediate
    startup pass and periodic bounded drains through `AdminStore`. It owns no
    account catalogue or calendar logic. A cloneable monitor retains confirmed
    progress and derives task death from channel closure, so a stale healthy
    publication cannot conceal a dead task. A partial batch means this pass
    completed, not that every account is current: PostgreSQL can skip rows
    held by another replica. Errors, timeouts, and interrupted calls may have
    committed unknown work; confirmed totals exclude it and `uncertain_calls`
    preserves that distinction through recovery and shutdown. Counter overflow
    is explicit and stops further scheduling. Dropping the owner or cancelling
    shutdown aborts the owned task. Witnesses:
    `startup_drains_saturated_batches_without_waiting_for_a_tick`,
    `a_pass_freezes_its_cutoff_even_when_the_clock_crosses_another_boundary`,
    `failure_preserves_confirmed_progress_and_waits_before_retrying`,
    `uncertain_commits_remain_visible_after_recovery_and_shutdown`,
    `a_task_that_dies_after_a_healthy_pass_is_reported_failed`,
    `dropping_the_owner_or_an_unpolled_shutdown_aborts_the_task`,
    `cancelling_a_polled_shutdown_keeps_ownership_of_the_task`,
    `generated_period_and_failure_traces_preserve_funding_and_observations`,
    and `counter_overflow_preserves_the_last_known_totals_and_reports_incompleteness`.
    `Tollgate.PeriodRoller.every_trace_is_safe`,
    `at_most_one_owned_call`, `cutoff_is_frozen_during_a_pass`, and
    `stopped_is_terminal` prove the abstract ownership and transition rules;
    paused-time tests witness actual Rust scheduling and cancellation.

    **Leases drain, then expire.** An active lease at a boundary keeps serving
    to its own TTL: there is no admission gap, and the request path still
    reads no clock for policy. The boundary is applied at settlement instead —
    release and the reclaim sweep both — where a lease funded by a period
    older than the account's credits its unspent *allowance* half to `expired`
    rather than to the balance. The top-up half returns to its bucket either
    way. Usage is unaffected, so a straggling event bills against the period
    its lease was granted in.

    **What an instance is told is a projection, never an authority.** A
    published snapshot carries a [`BudgetView`] — what the account could still
    spend at publication, its balance *plus* every active lease's unspent
    remainder, and when the period ends. The store stamps it on every publish
    and is its only writer: `AccountSnapshot`'s builder has no setter, and the
    overwrite is unconditional, so a value arriving over the wire cannot
    survive publication. Absent means the control plane said nothing, which a
    reader reports as nothing rather than as a zero balance.

    `estimate_remaining` subtracts what the instance has admitted since that
    publication, saturating at zero. It is an *estimate*, wrong by the fleet's
    spend elsewhere and by this instance's cancelled admissions, both of which
    make it read low rather than high. It never decides admission: quota comes
    from the lease and the ledger (1), so a stale figure cannot turn a refresh
    delay into an outage.

    This is rung 2 of the enforcement ladder: the store owns the crossing, the
    settlement decision, and the published view, and none is expressible by a
    caller. The `expired` term makes the outcome auditable rather than merely
    correct — see *Ledger roles*. *Tests:* `racing_passes_cross_a_boundary_exactly_once`,
    `a_top_up_survives_rollover_but_the_allowance_does_not`,
    `a_missed_period_does_not_accrue_a_backlog`,
    `a_lease_from_the_closed_period_expires_its_unspent_allowance`,
    `a_lease_funded_by_a_top_up_is_unaffected_by_a_boundary`,
    `a_split_funded_lease_charges_the_allowance_half_first`,
    `reclaim_expires_a_closed_period_lease_it_sweeps`,
    `the_rollover_pass_is_bounded_and_saturation_says_there_is_more`, and
    `the_allowance_split_cannot_exceed_what_it_is_part_of` (both store suites
    but the last, which is the PostgreSQL schema's half);
    `a_published_snapshot_carries_the_ledgers_budget`,
    `the_budget_view_counts_units_out_on_lease`,
    `the_budget_view_does_not_report_expired_units_as_spendable`,
    `the_budget_view_writes_off_settlement_loss`,
    `a_supplied_budget_never_survives_publication`,
    `an_instance_reports_no_estimate_when_the_control_plane_reported_no_budget`,
    `an_estimate_subtracts_what_this_instance_admitted_since_publication`,
    `a_republish_rebases_the_estimate`,
    `an_estimate_saturates_at_zero_rather_than_wrapping`,
    `a_committed_request_reports_the_estimate_its_response_carries`,
    `an_exhausted_estimate_does_not_deny`,
    `a_snapshot_without_a_budget_key_decodes_as_no_budget`,
    `a_partially_populated_schedule_is_reported_not_interpreted`, and
    `an_unrecognized_stored_schedule_name_is_refused`;
    `Tollgate.Conservation.rollover_preserves_conservation`,
    `a_rollover_never_touches_a_top_up`,
    `a_top_up_funded_lease_expires_nothing`, and
    `unrecorded_expiry_always_breaks_conservation`.

29. **The application's policy identity is carried, never interpreted.** A
    snapshot may name the product policy it was compiled from — a
    [`PolicyRevision`], 256 opaque bits — and every charge admitted under that
    snapshot is billed carrying the same value. Tollgate stores it, transports
    it, and hands it back; it never parses, hashes, orders, or branches on it.
    That is what keeps product vocabulary out of an enforcement substrate
    while still letting a consumer say which of *its* policies priced a
    request.

    **It is pinned with everything else.** The revision a request reports is
    the one its snapshot carried when `begin` resolved it, so a republication
    mid-request reaches the next request and not this one (26). A consumer can
    therefore resolve its own customer-visible metadata locally from the same
    value, with no I/O, and know it describes the policy that actually priced
    the work. `Committed::policy_revision` reads it from the usage event it
    will emit rather than from the snapshot a second time, which is what makes
    "what the response says" and "what the bill says" one value rather than
    two reads that agree.

    **Distinct from generation, in both directions.** [`Generation`] orders
    publications and decides staleness (15, 26); this identifies the inputs one
    publication was compiled from and carries no order at all — which is why it
    is deliberately not comparable. Two generations may share a revision (the
    same policy republished after a status change) and each generation carries
    exactly one. Neither may be derived from the other.

    **Unstated is a value.** All zeroes means "no revision stated", and an
    absent field on the wire or in storage decodes to it rather than failing.
    That is safe precisely because nothing in Tollgate reads the value: an
    unstated revision cannot change an enforcement outcome, only the identity
    reported alongside one. *Tests:*
    `a_committed_event_carries_the_pinned_revision`,
    `an_unstated_revision_reaches_the_charge_unstated`,
    `a_republished_revision_does_not_reach_an_already_pinned_request`,
    `a_snapshot_without_a_revision_key_decodes_as_unstated`,
    `a_builder_without_a_revision_states_none`,
    `an_event_without_a_revision_key_decodes_as_unstated`,
    `a_revision_survives_the_event_round_trip_exactly`,
    `the_unstated_revision_is_all_zeroes_and_round_trips`,
    `the_policy_revision_is_cold`,
    `a_snapshot_revision_round_trips_through_the_store` and
    `..._through_postgres`,
    `a_usage_event_is_ingested_with_its_revision_and_conserves`,
    `a_usage_row_carries_its_policy_revision`,
    `a_usage_row_cannot_carry_a_wrong_width_revision`,
    `a_legacy_snapshot_document_defaults_the_revision_to_unstated`,
    `admin_preserves_the_policy_revision_over_http`,
    `admin_refuses_a_noncanonical_policy_revision`,
    `full_stack_over_loopback_http`, and
    `the_response_and_the_bill_name_the_same_policy_revision`.

30. **Execution capacity is conserved, and the reserve is reachable.** An
    instance may bound how much work it *starts*, separately from what an
    account may fund. Capacity is two exactly partitioned pools — shared plus
    an assured reserve — and at every instant live permits from both sum to at
    most the configured total. Each successful acquisition returns exactly its
    unit, to the shard and pool that issued it, exactly once when its permit
    drops.

    **Best effort cannot consume the assured reserve.** Under every concurrent
    interleaving, best-effort permits are funded only from shared. A saturated
    best-effort flood therefore leaves the configured reserve reachable by
    assured work — which is the entire point of the feature, and is why the
    reserve transition takes the class's permission as a precondition rather
    than checking it. Both classes try shared first, so an instance with no
    best-effort traffic is not partitioned against itself: assured work reaches
    shared *and* reserve.

    **A capacity refusal is zero-charge.** A request that never obtains
    capacity returns all pending funding, resolves its writer permit without an
    event, and records neither settled usage nor overage. Rate tokens stay
    consumed — the request arrived and was priced, and refunding them would
    amplify an overload retry loop. It is counted as `capacity_shed`, its own
    terminal outcome, never as a second pre-admission denial (20).

    **Classification is verified and generation-pinned.** The class comes from
    the same immutable snapshot that authorized and priced the request, through
    evidence with no public constructor. It is never re-derived from caller
    input or a second lookup, and it is an account-owned fact with one writer
    (22's ownership, applied to a second field): a snapshot whose class
    contradicts its account is refused at publication.

    **The gate is request-path local.** Acquisition is synchronous and
    fail-fast: bounded in the configured shard count, no I/O, no blocking lock,
    no allocation, no clock. There is deliberately no queue — holding a request
    that cannot start would convert a capacity bound into an unbounded latency
    tail. Pools are sharded on cache-isolated lines for the reason leases are:
    this gate is global to the instance, so a single atomic would put every
    core on one line exactly during the overload it exists to handle.

    **Disabled means absent.** Under `ExecutionCapacityMode::Disabled` the
    service composes a zero-sized gate: no capacity state is allocated, no
    class branch runs, and no capacity atomic is touched. Classification cannot
    change an outcome because there is nothing for it to change. That is a
    startup composition boundary rather than a runtime enum, because a branch
    matched inside every request could not honestly promise it.
    *Tests:*
    `capacity::tests::disabled_never_refuses_and_ignores_the_class`,
    `uniform_bounds_total_and_treats_both_classes_alike`,
    `a_best_effort_flood_cannot_consume_the_assured_reserve`,
    `assured_work_reaches_shared_and_reserve_when_alone`,
    `assured_work_spends_shared_before_its_reserve`,
    `a_sharded_pool_strands_no_capacity`,
    `shards_are_capped_at_the_pools_units`,
    `a_partition_sums_to_the_whole`,
    `a_reserve_that_swallows_the_instance_is_refused`,
    `concurrent_acquisition_never_exceeds_the_total`,
    `concurrent_best_effort_load_leaves_the_reserve_reachable`,
    `engine::tests::a_capacity_shed_is_counted_without_a_second_denial`, and
    the account-ownership witnesses named in 22. The allocation-free and
    disabled-costs-nothing claims are gated rather than asserted:
    `allocations::acquiring_and_refusing_execution_capacity_allocates_nothing`
    records the `capacity/disabled`, `capacity/uniform`,
    `capacity/reserved_shared`, `capacity/reserved_fallback` and
    `capacity/shed` scopes at zero under `./scripts/check_allocations.sh`, and
    the `capacity/disabled` : `admission/full_check` same-run ratio in
    `testing/perf_thresholds.json` bounds what the gate call costs when the
    gate is `NoGate`. End to end, `load/mixed_saturation` drives assured and
    best-effort traffic at one instance and requires best-effort work to be
    shed several times more often than assured work, with a class-blind
    `Uniform` pool of the same size as the control that says the advantage
    belongs to the class; `api::two_classes_share_an_instance_and_each_start_is_attributed`
    carries the class through the whole embedding, and
    `a_capacity_refusal_is_a_retryable_503_and_not_a_rate_limit` pins what a
    caller is told.
    *Proof:* `formal/lean/Tollgate/ExecutionCapacity.lean`.


31. **An account has at most one owned refill manager, including retirement.**
    Runtime membership follows the snapshot map's accepted publication, never
    a candidate that generation ordering rejected. Catalogue removal withdraws
    admission and membership together. A fresh active principal makes its
    account eligible; removal, revocation, suspension, or freshness expiry of
    its last eligible principal arms a configurable linger. Reactivation
    cancels linger. After retirement begins, a replacement waits for the prior
    task's join and backoff. Notifications are matched to their task identity,
    so an old task cannot retire a new one. Unexpected task death is visible;
    accounting-integrity faults shut down the instance instead of being erased
    by restart. Stable slots retain irreversible overage spend for the process
    lifetime. Tasks and pending transitions scale with eligible, lingering, and
    retiring accounts; slot and diagnostic history scale with accounts ever seen.

    Enforcement: the runtime owns publication and one supervisor owns manager
    handles and indexed timers; coalesced registry notifications carry no
    historical event backlog. `formal/lean/Tollgate/AccountLifecycle.lean`
    proves ownership conservation, at most one owner, retirement before
    replacement, and inert duplicate joins in the abstract transition system.
    It does not prove Tokio scheduling or Rust refinement. Implementation
    witnesses: `arbitrary_catalogue_churn_retires_each_owner_once`,
    `sibling_principals_share_one_manager_and_refresh_does_not_restart_it`,
    `reactivation_during_linger_reuses_the_manager_and_later_reactivation_releases_then_refills`,
    `retiring_an_elastic_account_does_not_reset_its_spend_cap`,
    `a_dead_manager_is_joined_then_restarted_with_backoff`, and
    `a_refresh_outage_preserves_a_fresh_accounts_manager`,
    `catalogue_removal_withdraws_a_still_fresh_map_entry_and_can_restore_it`,
    `suspension_retires_funding_and_reinstatement_starts_it_again`, and
    `shutdown_racing_onboarding_joins_every_started_manager`,
    `membership_wakes_once_per_publication_and_expires_at_its_exact_deadline`,
    `repeated_shutdown_requests_share_the_first_deadline`,
    `shutdown_pauses_refills_while_previously_issued_permits_drain`, and
    `an_in_flight_release_cannot_start_a_refill_after_shutdown_pauses_it`, and
    `a_second_crash_reports_parked_grants_after_releasing_an_inherited_capability`,
    `repeated_inactive_publications_do_not_extend_the_initial_linger`, and
    `supervisor_exit_withdraws_readiness_before_children_receive_their_abort`, and
    `removing_the_last_principal_cancels_a_pending_manager_restart`, and
    `integrity_faults_in_idle_and_final_release_are_reported_as_terminal`, and
    `shutdown_distinguishes_settled_leases_from_unconfirmed_or_invalid_releases`.

32. **Control-plane authority is verified before decoding or mutation.** Every
    protected HTTP handler requires private instance or operator evidence;
    middleware authenticates and authorizes before body/path extraction. The
    roles are disjoint. A client certificate must pass TLS key possession and
    current CA/time validation, and its leaf fingerprint must have a role.
    Bearer schemes return verified principals with validity bounds, mapped to
    roles in the same immutable generation. Conflicting evidence, expired
    evidence, missing credentials, and wrong roles fail closed. Forwarded
    headers cannot manufacture TLS evidence. Probes carry no authority.

    Exposed server listeners require TLS; clients refuse remote plaintext,
    redirects, and credential-bearing URLs. A reload stages and validates the
    entire verifier/role/TLS generation before atomic publication. Failed
    loads preserve the previous generation and its original expiry. New
    requests recheck current authority even on existing connections; already
    authorized requests retain their pinned proof. Client transport replacement
    atomically rotates roots, identity and provider. Handshake count/time and
    total HTTP call time (including credential retrieval) are bounded off-path.

    Enforcement: private handler extractors, `ServerSecurity`, `SecureListener`,
    `SecurityLoader`, and the generation-owning `HttpRequest`. Exact-model proof:
    `formal/lean/Tollgate/ControlPlane.lean` proves role separation and agreement
    of presented evidence, assuming verification and atomic generation selection.
    It does not prove cryptography, rustls, Tokio, clock accuracy or Rust refinement.
    Implementation witnesses: `every_control_plane_route_requires_its_own_role_before_decoding`,
    `ambiguous_framing_and_forged_peer_headers_authenticate_nobody`,
    `exposed_plaintext_is_rejected_before_the_server_starts`,
    `unsafe_client_urls_and_deadlines_are_rejected_without_io`,
    `control_plane_redirects_are_not_followed`,
    `google_tokens_require_signature_issuer_audience_subject_and_live_expiry`,
    `signing_keys_never_outlive_the_issuer_cache_policy_or_one_hour`,
    `failed_key_refresh_never_extends_verified_identity_validity`,
    `security_reload_stages_validates_and_only_then_replaces`,
    `a_failed_install_is_retried_and_cannot_remove_tls`,
    `file_boundaries_are_part_of_the_rotation_fingerprint`,
    `bearer_rotation_changes_existing_connections_and_preserves_usage_for_retry`,
    `mtls_and_bearer_fund_and_settle_over_tls_and_revocation_affects_keepalive`,
    `a_hung_credential_provider_is_inside_the_http_deadline`,
    `google_metadata_cache_refreshes_at_its_exact_deadline_and_never_caches_failure`,
    `metadata_requires_success_google_provenance_and_a_complete_bounded_token`,
    `client_validation_rejects_each_unsafe_url_component_independently`,
    `static_credential_files_enforce_both_length_bounds_and_visible_framing`,
    `signing_key_refresh_honors_success_cadence_and_failure_backoff`,
    `initial_signing_key_failure_preserves_the_dependency_error`,
    `dropping_the_reloader_releases_its_owned_task_and_clock`,
    `every_signing_key_must_independently_name_an_rs256_signature_key`,
    `signing_key_transport_enforces_status_cache_policy_and_complete_body_bounds`,
    `bearer_framing_checks_each_condition_before_scheme_verification`,
    `overlapping_bearer_schemes_must_agree_on_the_verified_identity`,
    `certificate_configuration_checks_trust_key_pairs_and_handshake_bounds`,
    `the_binary_refuses_an_exposed_plaintext_listener_before_opening_the_backend`,
    `pending_tls_handshakes_are_bounded_expire_and_drop_with_the_listener`,
    `full_stack_over_tls_bearer` and `full_stack_over_mtls`.

33. **An administrative audit receipt describes its own serialized mutation.**
    Each successful HTTP-facing `AdminStore` mutation returns a typed receipt
    alongside its result, captured under the owning memory lock or PostgreSQL
    transaction. Concurrent operations must name the actual predecessor they
    replaced; a separate audit read cannot substitute for that evidence.
    Idempotent no-ops have equal before/after states. Snapshot state is identified
    by principal, immutable generation and revocation status; receipts do not
    duplicate complete policy graphs.

    The HTTP operator guard emits actor, operation ID, action, resource and time
    before a store call, then confirms with the receipt, or reports failure or
    cancellation without inventing a state transition. Storage errors and
    cancellation can conceal a commit. Delivery uses the embedder's tracing
    subscriber; this contract does not claim transactional durability across
    process death or logging failure. The binary retains audit events separately
    from normal verbosity; operators retain and monitor their log delivery.

    Enforcement: `AdminReceipt` in trait return types, receipt creation inside
    both backends, and `OperatorIdentity::run` around every admin mutation.
    `formal/lean/Tollgate/ControlPlane.lean` proves abstract deposit receipt
    conservation and predecessor composition; checked arithmetic and database
    serialization are separate implementation evidence. Mirrored backend tests:
    `admin_receipts_identify_the_state_each_operation_replaced`,
    `concurrent_deposit_receipts_form_one_exact_funding_history`, and
    `racing_publication_and_revocation_receipts_name_the_actual_predecessor`.
    HTTP witnesses: `every_admin_mutation_logs_its_actor_and_the_backend_receipt`,
    `cancelled_admin_operations_report_an_unknown_commit_without_a_receipt`, and
    `normal_log_verbosity_cannot_silence_the_binarys_audit_target`.

34. **A credential projection cannot renew stale identity evidence.** A single
    owned publisher replaces the complete validated active-key table. Duplicate
    principals or a digest naming a different principal reject the entire set;
    failed, incomplete or expired reads preserve the previous table and its
    original deadline. A successful empty set withdraws all entries. Every
    installed credential carries an exclusive evidence deadline equal to the
    earlier of its source expiry and fetch start plus configured `max_age`.
    Fetch time consumes freshness. Overflow cannot publish authority.

    `KeyVerifier` exposes verification only. It cannot mint credentials or
    install an indefinite table. `SessionCredential` checks its evidence at
    caller-supplied time (23); cached sessions can survive removal only until
    their original deadline, and snapshot authorization still runs every
    request. No request fetches credentials or reads a business clock. A live
    task and a fresh complete table are required for key-manager readiness;
    readiness reads the same published deadline as verification. A fresh empty
    catalogue is healthy. Task teardown clears the published table, while
    already-issued proofs keep their original bound. Manager drop owns abort,
    including when its consuming shutdown future is never polled.

    The HTTP read requires instance evidence (32), selects active keys at the
    server's clock, rejects caller-selected cutoffs, and disallows caching.
    `KeySource` gives instances read authority without lifecycle or
    administrative mutation capability. Both stores derive the projection from
    their own coherent revisioned active-key pages; transport reconstructs
    `KeyPage` identity, ordering, request and expiry evidence. Revision changes
    discard the candidate and restart within the same pass budget. Budget
    exhaustion never publishes a partial table or silently succeeds. Installed
    revisions never move backward. Nullable expiry and continuation fields are
    required on the wire; partial HTTP responses never become successful reads.

    Enforcement: private `CredentialSet`, immutable `KeyVerifier` projection,
    single-owner `KeyManager`, and existing session evidence checks. Exact-model
    proof: `formal/lean/Tollgate/CredentialProjection.lean` establishes expiry
    bounds and complete replacement/failure semantics, assuming coherent reads,
    atomic publication, verified digests and accurate clocks. It does not prove
    cryptography, Tokio, timestamp representation or Rust refinement.
    Implementation witnesses: `projected_evidence_is_bounded_by_both_source_expiry_and_fetch_start`,
    `complete_refresh_replaces_keys_and_cached_proofs_keep_their_original_deadline`,
    `an_outage_never_extends_projection_validity_and_recovery_replaces_it`,
    `fetch_time_consumes_freshness_and_expired_responses_cannot_replace_the_table`,
    `timestamp_overflow_cannot_renew_a_previously_valid_projection`,
    `hung_fetches_timeout_retry_and_shutdown_interrupts_the_pending_read`,
    `dropping_an_unpolled_shutdown_future_aborts_the_owned_refresh`,
    `task_death_withdraws_new_verification_and_is_visible_to_the_monitor`,
    `unusable_key_refresh_configuration_is_rejected_before_starting`,
    `a_large_legitimate_projection_is_installed_without_truncation`,
    `revision_change_restarts_from_the_beginning_instead_of_omitting_a_new_key`,
    `page_budget_and_revision_regression_preserve_the_previous_table_and_deadline`,
    `page_budget_cannot_publish_an_incomplete_first_table`,
    `the_instance_clock_narrows_the_server_set_at_publication`,
    `credential_refresh_failure_is_structured_without_exposing_the_source_body`,
    `credential_pages_order_bound_skip_retired_and_expose_every_mutation` (both stores),
    `credential_revision_covers_direct_writes_rollback_and_overflow` (PostgreSQL),
    `maximal_key_pages_fit_the_derived_envelope_and_digests_are_canonical`,
    `cached_projected_credentials_remain_allocation_free_through_expiry`,
    `only_instances_receive_active_keys_at_the_server_clock_without_caching`,
    `http_projection_refuses_partial_ambiguous_or_unsuccessful_responses`,
    the mirrored backend
    `credential_projection_preserves_lifecycle_truth_and_refuses_corrupt_identity`,
    and the three full-stack HTTP/TLS/mTLS conservation tests named in 32.

35. **Credential activity derives from canonical accepted commitments.** A
    pinned snapshot supplies its optional key ID when commitment constructs
    the usage event. Store publication validates a stated key's principal and
    account binding; custom snapshot producers remain trusted. Ingest checks
    account ownership and derives activity only from newly accepted events.
    Duplicate request IDs cannot replace their original identity or timestamp.
    Activity advances by maximum at microsecond precision and remains outside
    the credential revision, snapshots, authorization and conservation equation.

    Missing, unknown or different-account key attribution never rejects an
    otherwise valid bill. Such accepted events are reported unattributed;
    older/equal attributable timestamps remain successful no-ops. The count is
    bounded by accepted events, not the number of credential rows updated.
    Missing attribution reporting is unknown, never a fabricated zero. Complete
    report cardinality is validated before a writer releases queued evidence;
    cumulative outcome overflow saturates and is reported. Storage failures
    still roll back the complete ingest transaction and remain retryable.

    No observation is proof only of missing recorded evidence, never of non-use.
    Pre-execution denials/cancellation, unflushed, lost, rejected and unattributed
    commitments are outside this history. Activity cannot certify that revocation
    is harmless and never supplies authentication or authorization evidence.

    Enforcement: the shared MemoryStore publication operation and PostgreSQL
    publication transaction own identity checks; accepted-event classification,
    the memory plan/apply operation and conditional SQL upsert own activity.
    UsageWriter validates acknowledgements from every sink. The separate table
    has no credential-revision trigger; its regression witness checks isolation.
    `CredentialActivity.lean` proves exact-model max/idempotency laws, first-event
    replay identity, report partitioning, revision isolation and atomic failure
    preservation, assuming correct classification and transaction execution.
    It does not prove authentication, Rust/SQL refinement or lossless delivery.

    *Tests:* the shared memory/PostgreSQL scenarios
    `committed_usage_attributes_each_event_and_preserves_replay_identity`,
    `missing_unknown_and_wrong_account_attribution_preserve_billing`,
    `retired_activity_never_changes_the_credential_revision`,
    `publication_checks_the_stated_credential_binding`,
    `concurrent_activity_commits_converge_to_the_maximum`,
    `activity_uses_durable_microsecond_precision`,
    `activity_reads_preserve_every_requested_key_and_its_state`, and
    `a_failed_batch_preserves_activity_and_canonical_events`; PostgreSQL's
    `activity_failure_rolls_back_billing_and_source_metadata`,
    `a_commit_failure_after_activity_staging_preserves_the_predecessor`,
    `activity_and_source_identity_survive_restart_and_reset_together`, and
    `competing_request_ids_preserve_the_first_attribution`; the core property
    `committed_evidence_carries_the_supplied_credential`; admission's
    `emitted_usage_pins_the_key_and_cancelled_work_emits_nothing`; writer's
    `attribution_and_existing_outcome_counters_saturate_visibly`,
    `attribution_coverage_reports_transitions_without_repeating_incidents`,
    and the uncertain-retry/drain scenarios in `usage_attribution`; the HTTP
    `usage_acknowledgements_require_complete_bounded_valid_evidence` and
    `invalid_published_key_binding_has_a_structured_code`; the wire-limit,
    attributed embedding allocation and three full-stack transport witnesses.


36. **Calibration counts comparable benchmark runs, and replacement preserves
    the previous contract on refusal.** The performance recorder owns a minimum
    of three distinct, complete readable runs at one committed revision and one
    host, target, CPU, OS, compiler and profile. Reprocessing a run or copying
    its sample cannot increase its weight; divergent evidence is an error.
    Legacy samples lacking identity or environment contribute nothing. Ordinary
    gate verdicts remain independent of optional sample collection.

    Enforcement: argument validation requires a samples directory for recording;
    sample publication is atomic and exclusive; loading deduplicates run IDs and
    matches complete recording contexts. The recorder reserves exclusive staging
    before validating the actual destination, carries that destination's bounds,
    and validates the replacement before atomic promotion. A partial current run
    cannot record using earlier complete samples. Mean inputs must be finite and
    positive; median arithmetic avoids intermediate overflow.

    *Tests:* `optional_samples_preserve_gate_verdicts_without_recordable_provenance`,
    `recording_requires_samples_before_reading_input_and_help_still_works`,
    `checker_retries_cannot_satisfy_the_minimum_run_count`,
    `samples_from_different_revisions_or_environments_are_not_combined`,
    `profile_changes_and_legacy_samples_cannot_supply_missing_runs`,
    `replay_cannot_relabel_a_runs_environment_or_replace_its_measurements`,
    `concurrent_retries_publish_one_complete_sample`,
    `recording_validates_the_destination_and_preserves_its_bounds`,
    `a_foreign_or_invalid_destination_survives_recording_without_baseline_input`,
    `an_existing_staging_owner_blocks_replacement_without_touching_either_file`,
    `a_partial_run_cannot_record_using_previous_complete_samples`,
    `divergent_copies_and_invalid_sample_values_preserve_the_destination`, and
    `calibration_medians_reject_invalid_means_and_avoid_intermediate_overflow`.

37. **Backend error text is private across the HTTP and diagnostic boundaries.**
    The server converts opaque `StoreError` payloads to fixed public titles,
    retaining the existing status/code and retry classification. This covers
    allocation, account creation/status, publication and both ingest outcomes.
    Server-owned readiness, maintenance and PostgreSQL startup failure logs
    retain operation and progress context without formatting backend text or
    connection strings. Authentication does not make a backend payload public.

    The router reports HTTP problem responses with 5xx or `usage-refused` using a private
    response marker containing only a static code and a generated 128-bit ID.
    Its warning records status and the route template, never raw path values,
    queries, headers or bodies. The optional JSON `error_id` matches that warning
    and carries no authority. Entropy failure preserves the original refusal,
    omits the ID and reports `error_id_unavailable`; it cannot invent an ID or
    turn a failed operation into success. Delivery requires a tracing subscriber
    retaining `tollgate::diagnostics` warnings and is not a durable audit outbox.

    Enforcement: `ApiError` conversions, its response marker and the router's
    diagnostic middleware; fixed fields at the background/startup log sites.
    *Tests:* `every_backend_error_conversion_keeps_opaque_details_out_of_responses_and_debug`,
    `router_correlates_backend_failures_without_logging_request_or_error_payloads`,
    `diagnostic_identifiers_use_all_entropy_and_surface_entropy_failure`,
    `entropy_failure_preserves_the_error_without_inventing_an_identifier`,
    `readyz_is_503_when_the_store_cannot_answer`,
    `a_failing_sweep_reports_consecutive_failures`,
    `failed_rollover_retains_safe_progress_without_backend_text`, and
    `backend_startup_failure_never_discloses_connection_strings_or_driver_text`.

38. **CLI information requests precede application startup and validation.**
    Every binary recognizes `--help`/`-h` and `--version`/`-V`; the first such
    argument before `--` selects an information response and exits successfully.
    Those commands do not construct async runtimes, bind application listeners, start application tasks,
    read gate inputs or run measurements. After `--`, all arguments are literal
    positional inputs. The service binaries accept no positionals; a bare marker
    is equivalent to their ordinary no-argument startup. Invalid service
    arguments return status 2 with a visible diagnostic, regardless of log
    filtering. Native arguments are inspected before UTF-8 validation, so a
    non-UTF-8 argument cannot panic or conceal a subsequent information flag.

    Enforcement: each entry point owns the control decision; the pricing
    executable's `Startup` result selects whether its runtime is constructed.
    Gate parsers return their existing `Command` variants before decoding or
    validating operational arguments. This is component-owned enforcement with
    a cross-binary tested convention, not a mathematical proof of process I/O.
    *Tests:* `pricing_help_and_version_precede_configuration_and_argument_validation`,
    `pricing_information_does_not_attempt_to_bind_a_configured_listener`,
    `pricing_rejects_arguments_and_treats_everything_after_the_marker_literally`,
    `load_cli_information_does_not_open_files_or_start_measurements`,
    `native_arguments_cannot_panic_or_hide_information_flags`,
    `help_and_version_precede_configuration_and_respect_the_end_marker`,
    `server_native_arguments_cannot_panic_or_hide_help_and_version`,
    `benchmark_information_precedes_validation_and_uses_the_first_flag`,
    `benchmark_native_arguments_report_errors_after_information_flags_are_checked`,
    and `command_line_controls_precede_validation_and_terminator_is_respected`.

39. **Load execution failures remain reportable under the deployment panic policy.**
    Socket and protocol failures are returned by the owning client, never
    converted to a panic. Every client must finish warmup before measurement
    starts; failure or coordinator destruction releases waiting clients, and an
    incomplete run cannot return successful sample data. Only an HTTP 503 problem
    explicitly naming capacity unavailability is a shed outcome; other refusals
    are execution failures. Readiness and socket inactivity are bounded.

    Once command parsing supplies a report destination, configuration and
    execution errors publish a failed report with stage, message and run context
    and exit unsuccessfully in both verdict modes. Partial measurements are not
    presented as latency evidence. Reports are exclusively staged, validated and
    atomically replaced; a publication failure preserves the old destination
    and explicitly reports that the new report is unavailable. Concurrent writers
    have last-successful-publication semantics, with no merging of partial files.
    Response payloads do not enter diagnostics. Arbitrary panics or process death
    cannot be recovered under abort and are not covered by this guarantee.

    Enforcement: fallible client operations, one owned coordinator whose Drop
    releases the rendezvous, and the common atomic report publisher. These are
    component-owned contracts with implementation evidence, not an accounting
    proof or a formal refinement of Tokio and filesystem behavior.
    *Tests:* `concurrent_driver_surfaces_client_startup_failure_without_deadlock`,
    `failed_clients_publish_diagnostics_without_measurements`,
    `response_framing_accepts_fragmentation_and_only_capacity_shedding`,
    `invalid_responses_are_errors_without_response_payloads`,
    `socket_failures_are_returned_at_the_io_boundary`,
    `measurement_gate_publishes_run_and_abort_decisions`,
    `coordinator_drop_releases_waiters_and_poison_is_an_error`,
    `cancelling_the_driver_releases_ready_clients_and_refuses_late_readiness`,
    `readiness_io_cannot_outlive_the_startup_deadline`,
    `readiness_retries_incomplete_responses_until_a_complete_success`,
    `reports_replace_atomically_and_failures_keep_the_previous_file`, and
    `load_configuration_failures_write_reports_even_in_evidence_mode`, and
    `load_runtime_configuration_errors_are_reported_before_starting_tasks`.
    The CI production-profile fixture reuses the client/report implementation
    to witness ordinary process completion and readable failure reports with
    abort enabled; it performs no performance measurement.
