//! The admission pipeline itself.

use std::num::NonZeroU32;
use std::sync::Arc;

use jiff::Timestamp;

use tollgate_core::{
    AccountSnapshot, CostQuote, DenyReason, OpIndex, PermissionBits, QuoteError, Reservation,
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
            Some(MapEntry::NegativeUntil(_)) | None => {
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
        //    bucket proportionally. A weight beyond the bucket's burst can
        //    never pass (`check_n` reports insufficient capacity), which is
        //    fail-closed for misconfigured schedules.
        let weight = u32::try_from(quote.total.get()).unwrap_or(u32::MAX);
        match NonZeroU32::new(weight) {
            // Zero-cost requests draw no token; the minimum-charge floor
            // makes this unreachable for any real table.
            None => {}
            Some(n) => match state.limiter.check_n(n) {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => return Err(DenyReason::RateLimited),
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
            DenyReason::RateLimited
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
