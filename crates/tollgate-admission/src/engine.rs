//! The admission pipeline itself.

use std::num::NonZeroU32;
use std::sync::Arc;

use jiff::Timestamp;

use tollgate_core::{
    AccountSnapshot, CostQuote, CostUnits, DenyReason, Locality, OpIndex, PermissionBits,
    QuoteError, Reservation,
};

use crate::counters::AdmissionCounters;
use crate::state::{AccountAdmissionState, MapEntry, Principal, SnapshotMap};

/// One request's admission inputs. `items` is the batch size (1 for a single
/// request); the quote is `fixed + weight(op) * items`, floored at the
/// table's minimum charge.
#[derive(Debug, Clone, Copy)]
pub struct AdmissionRequest<'a, O: OpIndex> {
    pub principal: Principal,
    pub required: PermissionBits,
    pub op: &'a O,
    pub items: u64,
}

/// A fully admitted request: the caller executes the work, calls
/// [`Reservation::commit_at_execution_start`] when execution begins, and
/// emits the usage event from the committed reservation. Dropping this
/// without committing charges zero.
#[derive(Debug)]
pub struct Admitted {
    pub snapshot: Arc<AccountSnapshot>,
    pub quote: CostQuote,
    pub reservation: Reservation,
}

/// The engine: a snapshot map plus the pipeline. Generic over the map so the
/// moka and arc-swap candidates compete under identical logic.
pub struct AdmissionEngine<M: SnapshotMap> {
    map: M,
    // By value, not behind an `Arc`: the counters sit at a known offset from
    // an engine the embedder already holds, so recording an outcome is a
    // direct index off `self` rather than a pointer chase (#37).
    counters: AdmissionCounters,
}

impl<M: SnapshotMap> AdmissionEngine<M> {
    #[must_use]
    pub fn new(map: M) -> Self {
        let sharding = map.local_sharding();
        AdmissionEngine {
            map,
            counters: AdmissionCounters::with_sharding(sharding),
        }
    }

    /// Control-plane surface: the underlying map, for installs/invalidation.
    #[must_use]
    pub fn map(&self) -> &M {
        &self.map
    }

    /// Observability surface: what this instance has admitted and refused.
    #[must_use]
    pub fn counters(&self) -> &AdmissionCounters {
        &self.counters
    }

    /// Admit or deny. No I/O, no locks, no clock reads; every deny charges
    /// zero because the reservation is the last step.
    ///
    /// The outcome is tallied here rather than at each exit: several reasons
    /// never appear literally in [`Self::admit_inner`], since
    /// `AccountSnapshot::admit` and `Reservation::reserve` produce them and
    /// `?` propagates them. Counting at the sites would therefore have been
    /// both noisier and — where it mattered — incomplete.
    pub fn admit<O: OpIndex>(
        &self,
        request: AdmissionRequest<'_, O>,
        now: Timestamp,
    ) -> Result<Admitted, DenyReason> {
        let locality = Locality::current();
        let outcome = self.admit_inner(request, now, locality);
        match &outcome {
            Ok(admitted) if admitted.reservation.is_overage() => {
                self.counters
                    .record_admit_overage_at(admitted.quote.total, locality);
            }
            Ok(admitted) => self
                .counters
                .record_admit_at(admitted.quote.total, locality),
            Err(reason) => self.counters.record_deny_at(reason, locality),
        }
        outcome
    }

    fn admit_inner<O: OpIndex>(
        &self,
        request: AdmissionRequest<'_, O>,
        now: Timestamp,
        locality: Locality,
    ) -> Result<Admitted, DenyReason> {
        // 1. Lookup. A miss or live negative entry denies; an expired
        //    negative entry also denies but signals the background plane may
        //    retry resolution (it observes the map, not this return value).
        let state = match self.map.get_at(&request.principal, locality) {
            Some(MapEntry::Present(state)) => state,
            Some(MapEntry::NegativeUntil { .. }) | None => {
                return Err(DenyReason::UnknownPrincipal);
            }
        };

        // 2. Status, staleness, permissions.
        state.snapshot.admit(now, request.required)?;

        // 3. Shape and price the work.
        let limits = &state.snapshot.limits;
        if request.items > limits.max_items_per_request() {
            return Err(DenyReason::RequestTooLarge {
                max_items: limits.max_items_per_request(),
            });
        }
        let quote = state
            .snapshot
            .cost_table
            .quote(request.op, request.items)
            .map_err(|e| match e {
                QuoteError::UnknownOperation { .. } => DenyReason::UnpricedOperation,
                QuoteError::Overflow => DenyReason::CostOverflow,
            })?;

        // 4. Weighted rate token. Cost-weighted: heavy requests draw down the
        //    bucket proportionally.
        //
        //    A weight beyond the bucket's whole burst can never pass, however
        //    long the caller waits — that is a schedule whose batch cap admits
        //    a quote its burst cannot hold, and it is reported as such rather
        //    than as throttling (#40). Deciding it here, in full width against
        //    the configured burst, is what keeps the u32 conversion below
        //    honest: the weight is known to fit the bucket before it is
        //    narrowed, so narrowing can no longer disguise an unadmittable
        //    request as an ordinary empty bucket.
        let legacy_rate = limits.legacy_weighted_rate();
        if quote.total.get() > legacy_rate.burst_units() {
            return Err(DenyReason::UnpriceableUnderLimits {
                weight: quote.total,
                burst_units: CostUnits(legacy_rate.burst_units()),
            });
        }
        let weight = u32::try_from(quote.total.get()).unwrap_or(u32::MAX);
        match NonZeroU32::new(weight) {
            // Zero-cost requests draw no token; the minimum-charge floor
            // makes this unreachable for any real table.
            None => {}
            Some(n) => match state.limiter.check_n_at(n, locality) {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(DenyReason::RateLimited),
                // Unreachable: the check above clears the quote against the
                // account's whole burst, and a split bucket's shards are
                // sized to admit the largest quote any principal sharing the
                // account can present (`shard_ceiling`), so clearing the
                // whole burst clears every shard. Kept because "the bucket
                // cannot ever hold this" must never be reported as "the
                // bucket is momentarily empty".
                Err(_) => {
                    return Err(DenyReason::UnpriceableUnderLimits {
                        weight: quote.total,
                        burst_units: CostUnits(legacy_rate.burst_units()),
                    });
                }
            },
        }

        // 5. Quota: debit the lease and open the state machine. Note the
        //    deliberate ordering — a lease-denied request has still consumed
        //    its rate token, because it did arrive and was priced.
        //
        //    Under `EnforcementMode::Elastic` a lease that cannot fund the
        //    quote is not the end of the request. Three conditions say the
        //    same thing — this instance holds no capacity for these units —
        //    and they are the only three the mode intercepts:
        //
        //    - `LeaseUnavailable`: no lease at all, the cold-start and
        //      control-plane-outage case;
        //    - `LeaseExpired`: the lease's local window lapsed before refill
        //      replaced it;
        //    - `LeaseExhausted`: the lease is live and empty.
        //
        //    Nothing above this point is intercepted, and that is the whole
        //    safety argument. An unknown principal, a suspended or closed
        //    account, a stale snapshot, a missing permission, an oversized
        //    batch, an unpriced operation, a cost overflow, an empty rate
        //    bucket — every one of those still denies with zero charge under
        //    either mode, because none of them is a statement about funding
        //    (INVARIANTS.md #1, #5).
        let reservation = match Self::reserve_from_lease(&state, quote.total, now, locality) {
            Ok(reservation) => reservation,
            Err(denied) => Self::reserve_from_overage(&state, quote.total, denied)?,
        };

        Ok(Admitted {
            snapshot: Arc::clone(&state.snapshot),
            quote,
            reservation,
        })
    }

    /// The lease half of step 5, unchanged from the strict-only pipeline.
    #[inline]
    fn reserve_from_lease(
        state: &AccountAdmissionState,
        units: CostUnits,
        now: Timestamp,
        locality: Locality,
    ) -> Result<Reservation, DenyReason> {
        let lease = state
            .lease
            .load_at(locality)
            .ok_or(DenyReason::LeaseUnavailable)?;
        Reservation::reserve_at_locality(&lease, units, now, locality)
    }

    /// The elastic half: extend unfunded credit, or return the lease's own
    /// refusal untouched.
    ///
    /// `denied` is carried through rather than re-derived, so a `Strict`
    /// account reports exactly the reason it reported before this branch
    /// existed — including `LeaseExhausted`'s `remaining`, which a second
    /// lookup could not reproduce.
    #[inline]
    fn reserve_from_overage(
        state: &AccountAdmissionState,
        units: CostUnits,
        denied: DenyReason,
    ) -> Result<Reservation, DenyReason> {
        let Some(overage_cap) = state.snapshot.enforcement_mode.overage_cap() else {
            return Err(denied);
        };
        if !matches!(
            denied,
            DenyReason::LeaseUnavailable
                | DenyReason::LeaseExpired
                | DenyReason::LeaseExhausted { .. }
        ) {
            return Err(denied);
        }
        // No refill signal is raised here, and the reason is worth recording
        // because the opposite looks necessary. `LocalLease::try_debit`
        // announces the low-water *crossing*, which for an exhausted lease
        // already happened on the debit that drained it — before any request
        // reached this branch. For an absent or expired lease there is no
        // `RefillSignal` to raise at all, and recovery is the lease manager's
        // poll, exactly as it is under `Strict`. Elastic mode therefore does
        // not suppress refill; it runs alongside a refill already in flight.
        Reservation::reserve_overage(state.lease.overage(), units, overage_cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maps::{ArcSwapSnapshotMap, MokaSnapshotMap};
    use crate::state::LeaseSlot;
    use tollgate_core::EnforcementMode;
    use tollgate_core::{
        AccountId, AccountStatus, CancelOutcome, CostTable, CostUnits, FencingToken, Generation,
        LeaseGrant, LeaseId, LocalLease, LocalSharding, PublishableSnapshot, ResolvedLimits, Retry,
    };

    #[derive(Clone, Copy)]
    enum Op {
        Price,
        Unpriced,
    }
    impl OpIndex for Op {
        fn index(&self) -> usize {
            *self as usize
        }
    }

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    fn snapshot(status: AccountStatus) -> Arc<AccountSnapshot> {
        Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(1),
                status,
                t(10_000),
                PermissionBits::bit(0),
                ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000),
                Arc::new(
                    CostTable::builder(CostUnits(50), CostUnits(50))
                        .weight(&Op::Price, CostUnits(1))
                        .build(),
                ),
            )
            .build(),
        )
    }

    fn lease(units: u64) -> Arc<LocalLease> {
        lease_until(units, t(10_000))
    }

    fn lease_until(units: u64, expires_at: Timestamp) -> Arc<LocalLease> {
        Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id: LeaseId(7),
                account_id: AccountId(1),
                fencing_token: FencingToken(1),
                units: CostUnits(units),
                expires_at,
            },
            CostUnits::ZERO,
        ))
    }

    fn engine_with(
        status: AccountStatus,
        lease_units: Option<u64>,
    ) -> AdmissionEngine<ArcSwapSnapshotMap> {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        if let Some(units) = lease_units {
            slot.install(lease(units));
        }
        engine.map().install(Principal(1), snapshot(status), slot);
        engine
    }

    /// An engine whose account is elastic with `overage_cap`, and whose slot
    /// holds `lease_units` if any. `None` is the cold-start / lost-lease
    /// state; `Some(0)` is a live but empty lease.
    fn elastic_engine(
        overage_cap: u64,
        lease_units: Option<u64>,
    ) -> AdmissionEngine<ArcSwapSnapshotMap> {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        if let Some(units) = lease_units {
            slot.install(lease(units));
        }
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(overage_cap),
        };
        engine.map().install(Principal(1), Arc::new(snapshot), slot);
        engine
    }

    /// The three lease conditions elastic mode intercepts, each of which says
    /// "this instance holds no capacity for these units" and nothing else. A
    /// quote here is 50 fixed + 1 per item.
    #[test]
    fn elastic_admits_past_every_lease_condition_a_strict_account_denies() {
        for (name, lease_units, expected_strict) in [
            ("no lease at all", None, DenyReason::LeaseUnavailable),
            (
                "a live but empty lease",
                Some(0),
                DenyReason::LeaseExhausted {
                    remaining: CostUnits::ZERO,
                },
            ),
        ] {
            let strict = engine_with(AccountStatus::Active, lease_units);
            assert_eq!(
                strict.admit(request(1), t(0)).unwrap_err(),
                expected_strict,
                "strict must still deny with {name}"
            );

            let elastic = elastic_engine(1_000, lease_units);
            let admitted = elastic
                .admit(request(1), t(0))
                .unwrap_or_else(|denied| panic!("elastic denied {name}: {denied}"));
            assert!(admitted.reservation.is_overage());
            assert_eq!(admitted.quote.total, CostUnits(51));
        }
    }

    /// The expired-lease case needs its own clock, so it is separated from the
    /// loop above rather than folded in with a synthetic timestamp.
    #[test]
    fn elastic_admits_past_an_expired_lease() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease_until(1_000, t(5)));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(1_000),
        };
        engine.map().install(Principal(1), Arc::new(snapshot), slot);

        let admitted = engine.admit(request(1), t(10)).expect("elastic admits");
        assert!(admitted.reservation.is_overage());
    }

    /// The safety argument in one test: elasticity is a statement about
    /// *funding*, so every refusal that is not about funding still denies with
    /// zero charge and no overage claimed.
    #[test]
    fn elastic_does_not_relax_any_refusal_that_is_not_about_funding() {
        let overage_spent = |engine: &AdmissionEngine<ArcSwapSnapshotMap>| {
            let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
                panic!("principal present");
            };
            state.lease.overage().spent()
        };

        // Suspended and closed accounts: step 2, before the quota step.
        for status in [AccountStatus::Suspended, AccountStatus::Closed] {
            let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
            let slot = LeaseSlot::for_account(AccountId(1));
            let mut snapshot = AccountSnapshot::clone(&snapshot(status));
            snapshot.enforcement_mode = EnforcementMode::Elastic {
                overage_cap: CostUnits(1_000),
            };
            engine.map().install(Principal(1), Arc::new(snapshot), slot);
            assert!(engine.admit(request(1), t(0)).is_err(), "{status:?}");
            assert_eq!(overage_spent(&engine), CostUnits::ZERO);
        }

        // A stale snapshot, an oversized batch, and an unpriced operation.
        let engine = elastic_engine(1_000, None);
        assert_eq!(
            engine.admit(request(1), t(20_000)).unwrap_err(),
            DenyReason::SnapshotExpired
        );
        assert_eq!(
            engine.admit(request(65), t(0)).unwrap_err(),
            DenyReason::RequestTooLarge { max_items: 64 }
        );
        assert_eq!(
            engine
                .admit(
                    AdmissionRequest {
                        op: &Op::Unpriced,
                        ..request(1)
                    },
                    t(0)
                )
                .unwrap_err(),
            DenyReason::UnpricedOperation
        );
        assert_eq!(
            overage_spent(&engine),
            CostUnits::ZERO,
            "no refusal above the quota step may claim credit"
        );

        // An unknown principal never reaches an account at all.
        assert_eq!(
            engine
                .admit(
                    AdmissionRequest {
                        principal: Principal(99),
                        ..request(1)
                    },
                    t(0)
                )
                .unwrap_err(),
            DenyReason::UnknownPrincipal
        );
    }

    /// The cap bounds the account, not the request: repeated admissions
    /// accumulate against it and the refusal reports the exhausted local
    /// overage source without claiming that central funding is exhausted.
    #[test]
    fn elastic_refuses_once_the_local_overage_cap_is_spent() {
        // Two quotes of 51 fit in 102; the third does not.
        let engine = elastic_engine(102, Some(0));
        for _ in 0..2 {
            let admitted = engine.admit(request(1), t(0)).expect("within the cap");
            admitted
                .reservation
                .commit_at_execution_start(t(0))
                .expect("commit");
        }
        let denied = engine.admit(request(1), t(0)).unwrap_err();
        assert_eq!(
            denied,
            DenyReason::OverageCapExhausted {
                spent: CostUnits(102),
                overage_cap: CostUnits(102),
            }
        );
        assert_eq!(denied.retry(), Retry::Transient);
    }

    /// Exhausting the local overage allowance does not establish that the
    /// account itself needs funding: an ordinary background grant can make
    /// the unchanged request admissible without changing the deposit or cap.
    #[test]
    fn committed_overage_exhaustion_remains_retryable_after_lease_refill() {
        for (name, initial_lease, now) in [
            ("unavailable", None, t(0)),
            ("exhausted", Some(lease(0)), t(0)),
            ("expired", Some(lease_until(1_000, t(5))), t(10)),
        ] {
            let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
            let slot = LeaseSlot::for_account(AccountId(1));
            if let Some(initial_lease) = initial_lease {
                slot.install(initial_lease);
            }
            let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
            snapshot.enforcement_mode = EnforcementMode::Elastic {
                overage_cap: CostUnits(51),
            };
            engine
                .map()
                .install(Principal(1), Arc::new(snapshot), Arc::clone(&slot));

            engine
                .admit(request(1), now)
                .unwrap_or_else(|denied| panic!("the cap must cover {name}: {denied}"))
                .reservation
                .commit_at_execution_start(now)
                .expect("commit the local overage");

            let denied = engine.admit(request(1), now).unwrap_err();
            assert!(
                matches!(denied, DenyReason::OverageCapExhausted { .. }),
                "the stable local cap reason must survive {name}"
            );
            assert_eq!(denied.retry(), Retry::Transient, "lease was {name}");

            slot.install(lease(51));
            let admitted = engine
                .admit(request(1), now)
                .unwrap_or_else(|denied| panic!("a refill must recover {name}: {denied}"));
            assert!(!admitted.reservation.is_overage());
        }
    }

    /// The two mechanisms this merge put in the same slot had never met: an
    /// account can be elastic *and* sharded, and the slot now holds one
    /// unsharded overage counter beside N per-locality lease views.
    ///
    /// The failure this pins is the plausible one, and it only shows itself
    /// across threads. Had the counter been partitioned the way the grant is —
    /// one per view, the layout every neighbour in this struct uses — each
    /// locality would have carried its own full cap, and an eight-shard
    /// instance would extend `8 x overage_cap` while every doc, metric, and
    /// INVARIANTS #1 still said `overage_cap`. A single-threaded test cannot
    /// see that: one thread has one locality, and one locality's private
    /// counter refuses at the cap exactly like a shared one. So the requests
    /// have to arrive from different threads, which is where `Locality`
    /// assigns different values.
    #[test]
    fn a_sharded_slot_does_not_multiply_the_overage_cap() {
        const THREADS: usize = 8;
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(THREADS).unwrap());
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        slot.install(lease(0));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(102),
        };
        engine.map().install(Principal(1), Arc::new(snapshot), slot);
        let engine = Arc::new(engine);

        // Two quotes of 51 fit in a cap of 102. Eight threads ask; the answer
        // is two whatever order they arrive in, because the cap comparison
        // lives inside the counter's compare-exchange.
        let admitted = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|_| {
                    let engine = Arc::clone(&engine);
                    scope.spawn(move || match engine.admit(request(1), t(0)) {
                        Ok(admitted) => {
                            assert!(admitted.reservation.is_overage());
                            admitted
                                .reservation
                                .commit_at_execution_start(t(0))
                                .expect("commit");
                            1
                        }
                        Err(denied) => {
                            match denied {
                                DenyReason::OverageCapExhausted { spent, overage_cap }
                                | DenyReason::OverageCapTemporarilyExhausted {
                                    spent,
                                    overage_cap,
                                }
                                | DenyReason::OverageCommitInProgress { spent, overage_cap } => {
                                    assert_eq!(spent, CostUnits(102));
                                    assert_eq!(overage_cap, CostUnits(102));
                                }
                                other => panic!("unexpected cap refusal: {other}"),
                            }
                            0
                        }
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("no thread panicked"))
                .sum::<usize>()
        });

        assert_eq!(
            admitted, 2,
            "the cap is per account per instance, not per locality"
        );
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!("principal present");
        };
        assert_eq!(state.lease.overage().spent(), CostUnits(102));
        assert!(matches!(
            engine.admit(request(1), t(0)).unwrap_err(),
            DenyReason::OverageCapExhausted { .. }
        ));
    }

    /// The other half of the same interaction: a sharded slot whose lease can
    /// still fund the quote must debit that lease's local view, not reach for
    /// overage. A `load_at` that missed its view would look exactly like an
    /// unavailable lease and silently start billing overage instead.
    #[test]
    fn a_sharded_slot_still_prefers_the_lease_that_can_fund_the_quote() {
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        slot.install(lease(1_000));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(1_000),
        };
        engine.map().install(Principal(1), Arc::new(snapshot), slot);

        let admitted = engine.admit(request(1), t(0)).expect("the lease funds it");
        assert!(!admitted.reservation.is_overage());
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!("principal present");
        };
        assert_eq!(state.lease.overage().spent(), CostUnits::ZERO);
    }

    /// A pending overage can temporarily fill the cap, but its denial must say
    /// that cancellation can recover: no funding or allowance change is
    /// required. Cancelling charges zero and returns the credit.
    #[test]
    fn pending_overage_saturation_is_transient_until_cancel() {
        let engine = elastic_engine(51, Some(0));
        let admitted = engine.admit(request(1), t(0)).expect("within the cap");
        let denied = engine.admit(request(1), t(0)).unwrap_err();
        assert_eq!(
            denied,
            DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits(51),
                overage_cap: CostUnits(51),
            }
        );
        assert_eq!(denied.retry(), Retry::Transient);
        assert_eq!(admitted.reservation.cancel(), CancelOutcome::ZeroCharged);
        engine
            .admit(request(1), t(0))
            .expect("the cancelled credit is available again");
    }

    /// A lease that can still fund the quote is used, and the overage counter
    /// stays untouched: elastic mode is a fallback, never a preference.
    #[test]
    fn elastic_prefers_the_lease_while_it_can_fund_the_quote() {
        let engine = elastic_engine(1_000, Some(1_000));
        let admitted = engine.admit(request(1), t(0)).expect("the lease funds it");
        assert!(!admitted.reservation.is_overage());
        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!("principal present");
        };
        assert_eq!(state.lease.overage().spent(), CostUnits::ZERO);
        assert_eq!(state.lease.load().unwrap().remaining(), CostUnits(949));
    }

    /// INVARIANTS.md #20: an overage admission is counted under `admitted`
    /// like any other, *and* under its own qualifier. Readers of `admitted`
    /// must not have to add two numbers to get the total.
    #[test]
    fn an_overage_admission_is_counted_twice_over_and_a_refusal_once() {
        let engine = elastic_engine(51, Some(0));
        // Held, not dropped: an unresolved reservation releases its credit on
        // drop, so a test that lets one fall out of scope would be measuring
        // the refund rather than the cap.
        let _held = engine.admit(request(1), t(0)).expect("within the cap");
        engine.admit(request(1), t(0)).expect_err("beyond the cap");

        let counters = engine.counters().snapshot();
        assert_eq!(counters.admitted, 1);
        assert_eq!(counters.units_admitted, 51);
        assert_eq!(counters.admitted_overage, 1);
        assert_eq!(counters.units_admitted_overage, 51);
        assert_eq!(counters.denied(), 1);
        assert_eq!(
            counters.denials[DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits::ZERO,
                overage_cap: CostUnits::ZERO,
            }
            .index()],
            1
        );

        // A lease-funded admission moves only the unqualified counters.
        let strict = engine_with(AccountStatus::Active, Some(1_000));
        strict.admit(request(1), t(0)).expect("the lease funds it");
        let counters = strict.counters().snapshot();
        assert_eq!(counters.admitted, 1);
        assert_eq!(counters.admitted_overage, 0);
        assert_eq!(counters.units_admitted_overage, 0);
    }

    /// Every principal of an account draws from one cap. Two API keys must
    /// not double an account's credit — the same rule the rate limiter follows
    /// for the same reason.
    #[test]
    fn every_principal_of_an_account_shares_one_cap() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(0));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(51),
        };
        let snapshot = Arc::new(snapshot);
        for principal in [Principal(1), Principal(2)] {
            engine
                .map()
                .install(principal, Arc::clone(&snapshot), Arc::clone(&slot));
        }

        let _held = engine
            .admit(request(1), t(0))
            .expect("the first key spends");
        assert_eq!(
            engine
                .admit(
                    AdmissionRequest {
                        principal: Principal(2),
                        ..request(1)
                    },
                    t(0)
                )
                .unwrap_err(),
            DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits(51),
                overage_cap: CostUnits(51),
            },
            "a second key must not double the account's credit"
        );
    }

    /// Divergent caps across an account's principals bound the account by the
    /// *largest* of them, never their sum. This is the property that makes the
    /// mode safe without an account-level operator action: one shared counter
    /// means N credentials cannot multiply an account's credit the way N
    /// per-principal limiters would have multiplied its rate (review finding
    /// #4). What divergence costs is that lowering a cap does not bind until
    /// every principal of the account is republished — the same of every other
    /// per-principal policy value, `ResolvedLimits` included.
    #[test]
    fn divergent_caps_bound_an_account_by_the_largest_not_the_sum() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(0));
        for (principal, cap) in [(Principal(1), 51u64), (Principal(2), 102)] {
            let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
            snapshot.enforcement_mode = EnforcementMode::Elastic {
                overage_cap: CostUnits(cap),
            };
            engine
                .map()
                .install(principal, Arc::new(snapshot), Arc::clone(&slot));
        }

        let held: Vec<_> = (0..2)
            .map(|_| {
                engine
                    .admit(
                        AdmissionRequest {
                            principal: Principal(2),
                            ..request(1)
                        },
                        t(0),
                    )
                    .expect("the larger cap admits two")
            })
            .collect();
        assert_eq!(slot.overage().spent(), CostUnits(102));

        // The sum of the two caps is 153. If the caps combined rather than
        // maximised, a third request would fit; it must not.
        assert!(
            engine
                .admit(
                    AdmissionRequest {
                        principal: Principal(2),
                        ..request(1)
                    },
                    t(0)
                )
                .is_err(),
            "caps must not add up across an account's principals"
        );
        // And the smaller cap is already over its own limit, so it refuses too.
        assert!(engine.admit(request(1), t(0)).is_err());
        drop(held);
    }

    /// Republishing a snapshot must not reset the counter: the cap bounds an
    /// account's exposure over time, and a control plane that publishes
    /// frequently would otherwise hand out unlimited credit in cap-sized
    /// slices.
    #[test]
    fn republishing_a_snapshot_does_not_reset_the_cap() {
        let engine = elastic_engine(51, Some(0));
        let _held = engine.admit(request(1), t(0)).expect("within the cap");

        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!("principal present");
        };
        let slot = Arc::clone(&state.lease);
        let mut next = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        next.generation = Generation(2);
        next.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(51),
        };
        engine.map().install(Principal(1), Arc::new(next), slot);

        assert_eq!(
            engine.admit(request(1), t(0)).unwrap_err(),
            DenyReason::OverageCapTemporarilyExhausted {
                spent: CostUnits(51),
                overage_cap: CostUnits(51),
            }
        );
    }

    /// Raising the cap takes effect on the next request with no reconciliation
    /// step, because the cap is read from the snapshot rather than stored
    /// beside the counter.
    #[test]
    fn a_republished_cap_takes_effect_immediately() {
        let engine = elastic_engine(51, Some(0));
        let _held = engine.admit(request(1), t(0)).expect("within the cap");
        assert!(engine.admit(request(1), t(0)).is_err());

        let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
            panic!("principal present");
        };
        let slot = Arc::clone(&state.lease);
        let mut next = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        next.generation = Generation(2);
        next.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(102),
        };
        engine.map().install(Principal(1), Arc::new(next), slot);

        engine
            .admit(request(1), t(0))
            .expect("the raised cap admits the next request");
    }

    fn request(items: u64) -> AdmissionRequest<'static, Op> {
        AdmissionRequest {
            principal: Principal(1),
            required: PermissionBits::bit(0),
            op: &Op::Price,
            items,
        }
    }

    #[test]
    fn full_pipeline_admits_and_commits() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let admitted = engine.admit(request(14), t(0)).unwrap();
        assert_eq!(admitted.quote.total, CostUnits(64));
        admitted
            .reservation
            .commit_at_execution_start(t(0))
            .unwrap();
    }

    #[test]
    fn unknown_principal_denies() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        assert_eq!(
            engine.admit(request(1), t(0)).unwrap_err(),
            DenyReason::UnknownPrincipal
        );
    }

    #[test]
    fn negative_cache_denies() {
        let engine = AdmissionEngine::new(MokaSnapshotMap::new(10));
        engine.map().install_unknown(Principal(1), t(100));
        assert_eq!(
            engine.admit(request(1), t(0)).unwrap_err(),
            DenyReason::UnknownPrincipal
        );
    }

    #[test]
    fn suspended_account_denies() {
        let engine = engine_with(AccountStatus::Suspended, Some(10_000));
        assert_eq!(
            engine.admit(request(1), t(0)).unwrap_err(),
            DenyReason::AccountSuspended
        );
    }

    #[test]
    fn batch_cap_denies() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        assert_eq!(
            engine.admit(request(65), t(0)).unwrap_err(),
            DenyReason::RequestTooLarge { max_items: 64 }
        );
    }

    #[test]
    fn unpriced_operation_denies() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let req = AdmissionRequest {
            principal: Principal(1),
            required: PermissionBits::bit(0),
            op: &Op::Unpriced,
            items: 1,
        };
        assert_eq!(
            engine.admit(req, t(0)).unwrap_err(),
            DenyReason::UnpricedOperation
        );
    }

    #[test]
    fn missing_lease_denies_cold_start() {
        let engine = engine_with(AccountStatus::Active, None);
        assert_eq!(
            engine.admit(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseUnavailable
        );
    }

    #[test]
    fn exhausted_lease_denies_and_charges_zero() {
        let engine = engine_with(AccountStatus::Active, Some(60));
        // First request (51 units) fits; second is denied by remaining=9.
        let admitted = engine.admit(request(1), t(0)).unwrap();
        assert_eq!(admitted.quote.total, CostUnits(51));
        assert_eq!(
            engine.admit(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseExhausted {
                remaining: CostUnits(9)
            }
        );
        // Cancelling the first returns its units; admission works again.
        assert_eq!(admitted.reservation.cancel(), CancelOutcome::ZeroCharged);
        engine.admit(request(1), t(0)).unwrap();
    }

    #[test]
    fn rate_limiter_weights_by_cost() {
        // Burst of 1000 units, negligible refill within the test: exactly ten
        // 100-unit requests fit the burst (GCRA's boundary is inclusive), and
        // the eleventh is rate limited even though the lease has plenty left.
        let mut snapshot = (*snapshot(AccountStatus::Active)).clone();
        snapshot.limits = ResolvedLimits::new(64).with_weighted_rate(1, 1_000);
        let snapshot = Arc::new(snapshot);
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));
        engine.map().install(Principal(1), snapshot, slot);

        let req = request(50); // 50 + 50 fixed = 100 units
        for _ in 0..10 {
            let admitted = engine.admit(req, t(0)).unwrap();
            assert_eq!(admitted.quote.total, CostUnits(100));
        }
        assert_eq!(
            engine.admit(req, t(0)).unwrap_err(),
            DenyReason::RateLimited,
            "an empty-but-refilling bucket is throttling, not misconfiguration"
        );
    }

    /// Builds an engine whose account carries the given limits.
    fn engine_with_limits(limits: ResolvedLimits) -> AdmissionEngine<ArcSwapSnapshotMap> {
        let mut snapshot = (*snapshot(AccountStatus::Active)).clone();
        snapshot.limits = limits;
        let snapshot = Arc::new(snapshot);
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));
        engine.map().install(Principal(1), snapshot, slot);
        engine
    }

    /// Issue #40: a batch cap that admits a quote larger than the whole burst
    /// is a misconfigured schedule. Reporting it as throttling invites a retry
    /// that can never succeed.
    #[test]
    fn batch_cap_above_burst_is_unpriceable_not_throttled() {
        let engine = engine_with_limits(ResolvedLimits::new(64).with_weighted_rate(
            1_000,
            // 64 items quote 50 + 64 = 114 units: inside the batch cap, past
            // the burst, and unadmittable however long the caller waits.
            64,
        ));

        assert_eq!(
            engine.admit(request(64), t(0)).unwrap_err(),
            DenyReason::UnpriceableUnderLimits {
                weight: CostUnits(114),
                burst_units: CostUnits(64),
            }
        );
        // Repeating never converts it into ordinary throttling.
        assert_eq!(
            engine.admit(request(64), t(0)).unwrap_err(),
            DenyReason::UnpriceableUnderLimits {
                weight: CostUnits(114),
                burst_units: CostUnits(64),
            }
        );
        // A request the burst *can* hold still admits: the deny is about this
        // request's weight, not a wedged account.
        engine.admit(request(1), t(0)).unwrap();
    }

    /// The burst bound is inclusive, matching GCRA's own inclusive boundary
    /// (see `rate_limiter_weights_by_cost`): a quote of exactly the burst is
    /// admissible, so it must not be pre-empted as unpriceable. An off-by-one
    /// here would deny requests governor would have accepted.
    #[test]
    fn weight_equal_to_burst_admits() {
        let engine = engine_with_limits(ResolvedLimits::new(64).with_weighted_rate(
            1, // 64 items quote exactly 50 + 64 = 114 units.
            114,
        ));

        let admitted = engine.admit(request(64), t(0)).unwrap();
        assert_eq!(admitted.quote.total, CostUnits(114));
        // The bucket is now empty, so the next one is ordinary throttling —
        // never the terminal reason.
        assert_eq!(
            engine.admit(request(64), t(0)).unwrap_err(),
            DenyReason::RateLimited
        );
    }

    /// A zero burst is not silently repaired into a burst of one: every
    /// priced request is refused, and says why.
    #[test]
    fn zero_burst_denies_every_priced_request() {
        let engine = engine_with_limits(ResolvedLimits::new(64).with_weighted_rate(1_000, 0));

        assert_eq!(
            engine.admit(request(1), t(0)).unwrap_err(),
            DenyReason::UnpriceableUnderLimits {
                weight: CostUnits(51),
                burst_units: CostUnits::ZERO,
            }
        );
    }

    /// A quote wider than the bucket's u32 domain is unadmittable by
    /// construction; narrowing must not disguise it as an empty bucket.
    #[test]
    fn quote_beyond_the_bucket_domain_is_unpriceable() {
        let engine = engine_with_limits(
            ResolvedLimits::new(u64::MAX).with_weighted_rate(1_000, u64::from(u32::MAX)),
        );

        // 50 fixed + items: the first quote to exceed the burst.
        let items = u64::from(u32::MAX);
        assert_eq!(
            engine.admit(request(items), t(0)).unwrap_err(),
            DenyReason::UnpriceableUnderLimits {
                weight: CostUnits(items + 50),
                burst_units: CostUnits(u64::from(u32::MAX)),
            }
        );
    }

    /// The account's rate bucket is shared by every principal, but the split
    /// is sized from a *snapshot's* largest quote. Sizing it from whichever
    /// principal installed first left a sibling with a heavier cost table
    /// unable to spend its largest quote in any shard — reported as
    /// `UnpriceableUnderLimits` for a request the account's burst can hold,
    /// and admitted before the split existed (INVARIANTS.md #5).
    #[test]
    fn a_shared_split_bucket_never_wedges_a_principal_the_burst_can_hold() {
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
        let limits = ResolvedLimits::new(64).with_weighted_rate(800, 800);

        // A light key: 64 items quote 2 + 64 = 66 units, so eight buckets of
        // a hundred each can hold one.
        let mut light = (*snapshot(AccountStatus::Active)).clone();
        light.limits = limits;
        light.cost_table = Arc::new(
            CostTable::builder(CostUnits(2), CostUnits(1))
                .weight(&Op::Price, CostUnits(1))
                .build(),
        );
        let light = PublishableSnapshot::try_new(Arc::new(light)).unwrap();

        // A heavy key of the same account at the same generation: 64 items
        // quote 2 + 640 = 642 units. Publication accepts it — it fits the
        // account's 800-unit burst — so admission must too.
        let mut heavy = (*snapshot(AccountStatus::Active)).clone();
        heavy.limits = limits;
        heavy.cost_table = Arc::new(
            CostTable::builder(CostUnits(2), CostUnits(1))
                .weight(&Op::Price, CostUnits(10))
                .build(),
        );
        let heavy = PublishableSnapshot::try_new(Arc::new(heavy)).unwrap();

        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        slot.install(lease(1_000_000));
        engine
            .map()
            .install_publishable(Principal(1), light, Arc::clone(&slot));
        engine.map().install_publishable(Principal(2), heavy, slot);

        let admitted = engine
            .admit(
                AdmissionRequest {
                    principal: Principal(2),
                    required: PermissionBits::bit(0),
                    op: &Op::Price,
                    items: 64,
                },
                t(0),
            )
            .expect("a quote within the account's burst is admissible");
        assert_eq!(admitted.quote.total, CostUnits(642));
        assert_eq!(admitted.reservation.cancel(), CancelOutcome::ZeroCharged);
    }

    /// The counters must attribute each outcome to the right slot and leave
    /// every other slot alone. The sequence deliberately mixes reasons raised
    /// inside `admit_inner` with ones that only arrive through `?` from
    /// `AccountSnapshot::admit` and `Reservation::reserve` — the latter are
    /// exactly the ones per-site instrumentation would have missed.
    #[test]
    fn counters_attribute_every_outcome() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));

        // Two admissions: 1 item quotes 51 units, 14 items quote 64.
        engine.admit(request(1), t(0)).unwrap();
        engine.admit(request(14), t(0)).unwrap();
        // Raised in the pipeline itself.
        engine.admit(request(65), t(0)).unwrap_err();
        engine.admit(request(65), t(0)).unwrap_err();
        // Propagated out of `AccountSnapshot::admit`: staleness is decided
        // against `valid_until`, never by an inline refresh.
        assert_eq!(
            engine.admit(request(1), t(20_000)).unwrap_err(),
            DenyReason::SnapshotExpired
        );
        // Propagated out of `AccountSnapshot::admit`: permissions.
        assert_eq!(
            engine
                .admit(
                    AdmissionRequest {
                        principal: Principal(1),
                        required: PermissionBits::bit(3),
                        op: &Op::Price,
                        items: 1,
                    },
                    t(0)
                )
                .unwrap_err(),
            DenyReason::MissingPermission
        );

        let snapshot = engine.counters().snapshot();
        assert_eq!(snapshot.admitted, 2);
        assert_eq!(snapshot.units_admitted, 115, "51 + 64 units quoted");
        assert_eq!(snapshot.denied(), 4);
        let denials: Vec<_> = snapshot
            .denials_by_name()
            .filter(|(_, count)| *count > 0)
            .collect();
        assert_eq!(
            denials,
            vec![
                ("snapshot_expired", 1),
                ("missing_permission", 1),
                ("request_too_large", 2),
            ],
            "each reason in its own slot, and nothing in the others"
        );
    }

    /// Every reason the engine can produce must reach its own slot. Six of
    /// these had no engine-level test before the counters needed one, so a
    /// reason could have been produced and never observed here.
    #[test]
    fn each_reason_reaches_its_own_slot() {
        // Closed and suspended accounts, and an unknown principal.
        for (status, expected) in [
            (AccountStatus::Suspended, DenyReason::AccountSuspended),
            (AccountStatus::Closed, DenyReason::AccountClosed),
        ] {
            let engine = engine_with(status, Some(10_000));
            assert_eq!(engine.admit(request(1), t(0)).unwrap_err(), expected);
            let snapshot = engine.counters().snapshot();
            assert_eq!(snapshot.denials[expected.index()], 1);
            assert_eq!(snapshot.denied(), 1, "exactly one slot moved");
        }

        // A lease past its expiry: `Reservation::reserve` refuses it, and the
        // reason propagates through `?`. The lease must lapse well before the
        // snapshot does, or the staleness check upstream would answer first
        // and this slot would never be reached.
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease_until(10_000, t(100)));
        engine
            .map()
            .install(Principal(1), snapshot(AccountStatus::Active), slot);
        assert_eq!(
            engine.admit(request(1), t(200)).unwrap_err(),
            DenyReason::LeaseExpired
        );
        assert_eq!(
            engine.counters().snapshot().denials[DenyReason::LeaseExpired.index()],
            1
        );

        // Cost overflow: a table whose weight cannot be multiplied out.
        let mut overflowing = (*snapshot(AccountStatus::Active)).clone();
        overflowing.limits = ResolvedLimits::new(u64::MAX).with_weighted_rate(u64::MAX, u64::MAX);
        overflowing.cost_table = Arc::new(
            CostTable::builder(CostUnits(50), CostUnits(50))
                .weight(&Op::Price, CostUnits(u64::MAX / 2))
                .build(),
        );
        let overflowing = Arc::new(overflowing);
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(u64::MAX));
        engine.map().install(Principal(1), overflowing, slot);
        assert_eq!(
            engine.admit(request(4), t(0)).unwrap_err(),
            DenyReason::CostOverflow
        );
        assert_eq!(
            engine.counters().snapshot().denials[DenyReason::CostOverflow.index()],
            1
        );

        // AccountingBackpressure is decided before admission by the embedder,
        // so the engine cannot raise it — recording it is the caller's job,
        // and the slot exists for exactly that (INVARIANTS.md #8).
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        engine
            .counters()
            .record_deny(&DenyReason::AccountingBackpressure);
        assert_eq!(
            engine.counters().snapshot().denials[DenyReason::AccountingBackpressure.index()],
            1
        );
    }

    /// A denial charges zero units, so it must never move `units_admitted` —
    /// the counter an operator is most likely to misread as money.
    #[test]
    fn denied_requests_add_no_units() {
        let engine = engine_with(AccountStatus::Active, Some(60));
        // Held, not dropped: an uncommitted reservation returns its units on
        // drop, so releasing it here would refill the lease and the next
        // request would be admitted instead of refused.
        let held = engine.admit(request(1), t(0)).unwrap();
        // The lease now has 9 units left: the next request is refused.
        engine.admit(request(1), t(0)).unwrap_err();

        let snapshot = engine.counters().snapshot();
        assert_eq!(snapshot.admitted, 1);
        assert_eq!(snapshot.units_admitted, 51);
        assert_eq!(
            snapshot.denials[DenyReason::LeaseExhausted {
                remaining: CostUnits(9)
            }
            .index()],
            1
        );
        drop(held);
    }

    /// The lease slot is shared: a refill installed after a cold-start deny
    /// admits without reinstalling the snapshot.
    #[test]
    fn refill_after_cold_start_recovers() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        engine.map().install(
            Principal(1),
            snapshot(AccountStatus::Active),
            Arc::clone(&slot),
        );
        assert_eq!(
            engine.admit(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseUnavailable
        );
        slot.install(lease(10_000));
        engine.admit(request(1), t(0)).unwrap();
    }
}
