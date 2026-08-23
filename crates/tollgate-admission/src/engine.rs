//! The admission pipeline itself.

use std::num::NonZeroU32;
use std::sync::Arc;

use jiff::Timestamp;

use tollgate_core::{
    AccountSnapshot, CostQuote, CostUnits, DenyReason, OpIndex, PermissionBits, QuoteError,
    Reservation,
};

use crate::state::{MapEntry, Principal, SnapshotMap};

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
}

impl<M: SnapshotMap> AdmissionEngine<M> {
    #[must_use]
    pub fn new(map: M) -> Self {
        AdmissionEngine { map }
    }

    /// Control-plane surface: the underlying map, for installs/invalidation.
    #[must_use]
    pub fn map(&self) -> &M {
        &self.map
    }

    /// Admit or deny. No I/O, no locks, no clock reads; every deny charges
    /// zero because the reservation is the last step.
    pub fn admit<O: OpIndex>(
        &self,
        request: AdmissionRequest<'_, O>,
        now: Timestamp,
    ) -> Result<Admitted, DenyReason> {
        // 1. Lookup. A miss or live negative entry denies; an expired
        //    negative entry also denies but signals the background plane may
        //    retry resolution (it observes the map, not this return value).
        let state = match self.map.get(&request.principal) {
            Some(MapEntry::Present(state)) => state,
            Some(MapEntry::NegativeUntil { .. }) | None => {
                return Err(DenyReason::UnknownPrincipal);
            }
        };

        // 2. Status, staleness, permissions.
        state.snapshot.admit(now, request.required)?;

        // 3. Shape and price the work.
        let limits = &state.snapshot.limits;
        if request.items > limits.max_items_per_request {
            return Err(DenyReason::RequestTooLarge {
                max_items: limits.max_items_per_request,
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
        if quote.total.get() > limits.rate_burst_units {
            return Err(DenyReason::UnpriceableUnderLimits {
                weight: quote.total,
                burst_units: CostUnits(limits.rate_burst_units),
            });
        }
        let weight = u32::try_from(quote.total.get()).unwrap_or(u32::MAX);
        match NonZeroU32::new(weight) {
            // Zero-cost requests draw no token; the minimum-charge floor
            // makes this unreachable for any real table.
            None => {}
            Some(n) => match state.limiter.check_n(n) {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(DenyReason::RateLimited),
                // Unreachable given the check above, which uses the same
                // configured burst the bucket was built from; kept because
                // "the bucket cannot ever hold this" must never be reported
                // as "the bucket is momentarily empty".
                Err(_) => {
                    return Err(DenyReason::UnpriceableUnderLimits {
                        weight: quote.total,
                        burst_units: CostUnits(limits.rate_burst_units),
                    });
                }
            },
        }

        // 5. Quota: debit the lease and open the state machine. Note the
        //    deliberate ordering — a lease-denied request has still consumed
        //    its rate token, because it did arrive and was priced.
        let lease = state.lease.load().ok_or(DenyReason::LeaseUnavailable)?;
        let reservation = Reservation::reserve(&lease, quote.total, now)?;

        Ok(Admitted {
            snapshot: Arc::clone(&state.snapshot),
            quote,
            reservation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maps::{ArcSwapSnapshotMap, MokaSnapshotMap};
    use crate::state::LeaseSlot;
    use tollgate_core::{
        AccountId, AccountStatus, CancelOutcome, CostTable, CostUnits, FencingToken, Generation,
        LeaseGrant, LeaseId, LocalLease, ResolvedLimits,
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
        Arc::new(AccountSnapshot {
            account_id: AccountId(1),
            key_id: None,
            generation: Generation(1),
            status,
            valid_until: t(10_000),
            permissions: PermissionBits::bit(0),
            limits: ResolvedLimits {
                max_items_per_request: 64,
                rate_units_per_second: 1_000_000,
                rate_burst_units: 1_000_000,
            },
            cost_table: Arc::new(
                CostTable::builder(CostUnits(50), CostUnits(50))
                    .weight(&Op::Price, CostUnits(1))
                    .build(),
            ),
        })
    }

    fn lease(units: u64) -> Arc<LocalLease> {
        Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id: LeaseId(7),
                account_id: AccountId(1),
                fencing_token: FencingToken(1),
                units: CostUnits(units),
                expires_at: t(10_000),
            },
            CostUnits::ZERO,
        ))
    }

    fn engine_with(
        status: AccountStatus,
        lease_units: Option<u64>,
    ) -> AdmissionEngine<ArcSwapSnapshotMap> {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::empty();
        if let Some(units) = lease_units {
            slot.install(lease(units));
        }
        engine.map().install(Principal(1), snapshot(status), slot);
        engine
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
        engine.map().install_negative(Principal(1), t(100));
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
        let snapshot = Arc::new(AccountSnapshot {
            limits: ResolvedLimits {
                max_items_per_request: 64,
                rate_units_per_second: 1,
                rate_burst_units: 1_000,
            },
            ..(*snapshot(AccountStatus::Active)).clone()
        });
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::empty();
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
        let snapshot = Arc::new(AccountSnapshot {
            limits,
            ..(*snapshot(AccountStatus::Active)).clone()
        });
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::empty();
        slot.install(lease(1_000_000));
        engine.map().install(Principal(1), snapshot, slot);
        engine
    }

    /// Issue #40: a batch cap that admits a quote larger than the whole burst
    /// is a misconfigured schedule. Reporting it as throttling invites a retry
    /// that can never succeed.
    #[test]
    fn batch_cap_above_burst_is_unpriceable_not_throttled() {
        let engine = engine_with_limits(ResolvedLimits {
            max_items_per_request: 64,
            rate_units_per_second: 1_000,
            // 64 items quote 50 + 64 = 114 units: inside the batch cap, past
            // the burst, and unadmittable however long the caller waits.
            rate_burst_units: 64,
        });

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
        let engine = engine_with_limits(ResolvedLimits {
            max_items_per_request: 64,
            rate_units_per_second: 1,
            // 64 items quote exactly 50 + 64 = 114 units.
            rate_burst_units: 114,
        });

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
        let engine = engine_with_limits(ResolvedLimits {
            max_items_per_request: 64,
            rate_units_per_second: 1_000,
            rate_burst_units: 0,
        });

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
        let engine = engine_with_limits(ResolvedLimits {
            max_items_per_request: u64::MAX,
            rate_units_per_second: 1_000,
            rate_burst_units: u64::from(u32::MAX),
        });

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

    /// The lease slot is shared: a refill installed after a cold-start deny
    /// admits without reinstalling the snapshot.
    #[test]
    fn refill_after_cold_start_recovers() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::empty();
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
