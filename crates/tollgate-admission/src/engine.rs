//! The admission pipeline itself.

use std::num::NonZeroU32;
use std::sync::Arc;

use jiff::Timestamp;

use tollgate_core::{
    AccountSnapshot, CommitError, CommitFunding, CostQuote, CostUnits, DenyReason, Generation,
    Locality, OpIndex, PermissionBits, QuoteError, RequestId, Reservation, UsageEvent, UsageSlot,
};

use crate::counters::AdmissionCounters;
use crate::state::{AccountAdmissionState, ConcurrencyGuard, MapEntry, Principal, SnapshotMap};

/// Generation-pinned stage-one evidence, owned across body decoding.
#[derive(Debug)]
pub struct RequestContext {
    state: Arc<AccountAdmissionState>,
    locality: Locality,
}

impl RequestContext {
    #[must_use]
    pub fn snapshot(&self) -> &AccountSnapshot {
        &self.state.snapshot
    }

    #[must_use]
    pub fn limits(&self) -> &tollgate_core::ResolvedLimits {
        &self.state.snapshot.limits
    }

    #[must_use]
    pub fn generation(&self) -> Generation {
        self.state.snapshot.generation
    }

    /// What the account can still spend this period, as this instance best
    /// knows it — see [`AccountAdmissionState::estimate_remaining`] for what
    /// the estimate is wrong by and why it is never an authorization input.
    ///
    /// Reachable from all three response-producing stages, because a caller
    /// answering "how much is left" has to answer it on a denial as well: this
    /// stage is the only one a denied request still holds, `Pending` is what a
    /// cancelled one holds, and [`Committed`] is what a served one holds.
    #[must_use]
    pub fn estimate_remaining(&self) -> Option<CostUnits> {
        self.state.estimate_remaining()
    }

    pub fn admit<O: OpIndex, S: UsageSlot>(
        self,
        workload: &[(O, u64)],
        slot: S,
        now: Timestamp,
    ) -> Result<Pending<S>, DenyReason> {
        let locality = self.locality;
        let quote = match compile_workload(&self.state.snapshot, workload, now) {
            Ok(quote) => quote,
            Err(reason) => {
                self.state.counters.record_deny_at(&reason, locality);
                return Err(reason);
            }
        };
        admit_priced(self.state, locality, quote, now).map(|priced| Pending {
            concurrency: priced.concurrency,
            reservation: priced.reservation,
            slot,
            quote: priced.quote,
        })
    }
}

/// Funding is reserved, but execution capacity has not yet been acquired.
///
/// The reservation, the concurrency guard, and the usage slot have no public
/// accessors. Every transition out of this state consumes it, so safe code
/// cannot retain funding while releasing occupancy, and cannot reach the
/// reservation to resolve it out of band.
///
/// The reservation is unreachable because the field is private, and the error
/// code is pinned so this witness cannot pass for an unrelated reason: making
/// the field public compiles (E0616 disappears), and renaming it reports E0609
/// instead. Either way the doctest fails and the invariant is re-examined.
///
/// ```compile_fail,E0616
/// use tollgate_core::DiscardedUsageSlot;
///
/// fn detach(pending: tollgate_admission::Pending<DiscardedUsageSlot>) {
///     let _raw = pending.reservation;
/// }
/// ```
///
/// Its companion: the supported path compiles, so the refusal above can never
/// be a refusal of an API that stopped existing.
///
/// ```
/// use tollgate_core::DiscardedUsageSlot;
///
/// # fn release(
/// #     pending: tollgate_admission::Pending<DiscardedUsageSlot>,
/// # ) -> tollgate_admission::Released {
/// pending.cancel()
/// # }
/// ```
#[derive(Debug)]
pub struct Pending<S: UsageSlot> {
    concurrency: ConcurrencyGuard,
    reservation: Reservation,
    slot: S,
    quote: CostQuote,
}

impl<S: UsageSlot> Pending<S> {
    #[must_use]
    pub fn quote(&self) -> CostQuote {
        self.quote
    }

    #[must_use]
    pub fn snapshot(&self) -> &AccountSnapshot {
        &self.concurrency.state().snapshot
    }

    #[must_use]
    pub fn limits(&self) -> &tollgate_core::ResolvedLimits {
        &self.snapshot().limits
    }

    /// See [`RequestContext::estimate_remaining`]. This request's own quote is
    /// already counted: admission tallies the units it reserved.
    #[must_use]
    pub fn estimate_remaining(&self) -> Option<CostUnits> {
        self.concurrency.state().estimate_remaining()
    }

    pub fn acquire_capacity<G: CapacityGate>(
        self,
        gate: &G,
    ) -> Result<ReadyToStart<S, G::Permit>, (DenyReason, Released)> {
        let evidence = CapacityEvidence { _private: () };
        match gate.acquire(evidence) {
            Ok(permit) => Ok(ReadyToStart {
                pending: self,
                permit,
            }),
            Err(denied) => Err((denied, Released)),
        }
    }

    pub fn cancel(self) -> Released {
        self.reservation.cancel();
        Released
    }
}

mod private {
    pub trait Sealed {}
}

/// Tollgate-owned execution-capacity evidence.
pub trait CapacityPermit: private::Sealed + Send + 'static {}

/// A startup-selected execution-capacity policy.
pub trait CapacityGate: private::Sealed + Send + Sync + 'static {
    type Permit: CapacityPermit;

    fn acquire(&self, evidence: CapacityEvidence) -> Result<Self::Permit, DenyReason>;
}

/// Opaque evidence retained from the pinned request context.
#[derive(Debug, Clone, Copy)]
pub struct CapacityEvidence {
    _private: (),
}

/// Disabled execution-capacity policy. This is a zero-sized startup choice.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoGate;

/// Permit produced by [`NoGate`]; callers cannot construct one directly.
#[derive(Debug)]
pub struct NoCapacityPermit(());

impl private::Sealed for NoGate {}
impl private::Sealed for NoCapacityPermit {}
impl CapacityPermit for NoCapacityPermit {}

impl CapacityGate for NoGate {
    type Permit = NoCapacityPermit;

    #[inline]
    fn acquire(&self, _evidence: CapacityEvidence) -> Result<Self::Permit, DenyReason> {
        Ok(NoCapacityPermit(()))
    }
}

/// Funding and execution capacity are both held; committing consumes this
/// proof immediately before the computational kernel starts.
#[derive(Debug)]
pub struct ReadyToStart<S: UsageSlot, P: CapacityPermit> {
    pending: Pending<S>,
    permit: P,
}

impl<S: UsageSlot, P: CapacityPermit> ReadyToStart<S, P> {
    #[must_use = "the kernel may run only while holding the returned Committed guard"]
    pub fn commit(
        self,
        request_id: RequestId,
        now: Timestamp,
    ) -> Result<Committed<S, P>, (CommitError, Released)> {
        let Pending {
            concurrency,
            reservation,
            slot,
            quote: _,
        } = self.pending;
        // The pinned snapshot decides what a lapsed lease means, and the
        // counter comes from the same slot that funded the reservation.
        let state = concurrency.state();
        let funding =
            CommitFunding::from_mode(state.snapshot.enforcement_mode, state.lease.overage());
        let units = match reservation.commit_at_execution_start(now, funding) {
            Ok(units) => units,
            // Core already released for zero and classified the refusal —
            // expired funding, or an overage cap the fallback could not fit
            // inside. Each keeps its own retry class through to the embedder.
            Err(denied @ CommitError::Denied(_)) => return Err((denied, Released)),
            Err(CommitError::AlreadyReleased | CommitError::Cancelled) => {
                return Err((CommitError::Cancelled, Released));
            }
            Err(CommitError::AlreadyCommitted) => {
                debug_assert!(false, "an owned ready state can commit only once");
                return Err((CommitError::Cancelled, Released));
            }
        };
        let event = reservation
            .usage_event(request_id, now)
            .expect("a committed reservation produces usage evidence");
        Ok(Committed {
            event: Some(event),
            slot: Some(slot),
            units,
            request_id,
            _concurrency: concurrency,
            _capacity: self.permit,
        })
    }

    pub fn cancel(self) -> Released {
        self.pending.cancel()
    }
}

/// Typed evidence that a request resolved before execution with zero charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Released;

/// The only proof that the computational kernel may run.
///
/// The guard owns the committed charge for the whole execution: its `Drop`
/// records the billing event into the usage slot bound at admission and only
/// then releases concurrency and execution capacity. Normal completion, early
/// return, panic unwind, and task abort at an await point all run `Drop`, so a
/// committed charge cannot be spent-but-unbilled.
///
/// That guarantee is worth nothing if the guard can be dropped at the point it
/// is produced, so discarding execution-start evidence is a compile-time error
/// under the standard `unused_must_use` lint:
///
/// ```compile_fail
/// # #![deny(unused_must_use)]
/// use tollgate_core::DiscardedUsageSlot;
/// use tollgate_admission::{NoCapacityPermit, ReadyToStart, Released};
///
/// # fn discard(
/// #     ready: ReadyToStart<DiscardedUsageSlot, NoCapacityPermit>,
/// #     request_id: tollgate_core::RequestId,
/// #     now: jiff::Timestamp,
/// # ) -> Result<(), (tollgate_core::CommitError, Released)> {
/// ready.commit(request_id, now)?;
/// # Ok(())
/// # }
/// ```
///
/// Its companion: binding the guard compiles, so the refusal above is a
/// refusal to *discard* the guard rather than a refusal of a call that stopped
/// type-checking.
///
/// ```
/// # #![deny(unused_must_use)]
/// use tollgate_core::DiscardedUsageSlot;
/// use tollgate_admission::{Committed, NoCapacityPermit, ReadyToStart, Released};
///
/// # fn hold(
/// #     ready: ReadyToStart<DiscardedUsageSlot, NoCapacityPermit>,
/// #     request_id: tollgate_core::RequestId,
/// #     now: jiff::Timestamp,
/// # ) -> Result<Committed<DiscardedUsageSlot, NoCapacityPermit>, (tollgate_core::CommitError, Released)> {
/// ready.commit(request_id, now)
/// # }
/// ```
#[derive(Debug)]
#[must_use = "hold this guard for the full execution lifetime: dropping it emits the billing event and releases occupancy"]
pub struct Committed<S: UsageSlot, P: CapacityPermit> {
    event: Option<UsageEvent>,
    slot: Option<S>,
    units: CostUnits,
    request_id: RequestId,
    _concurrency: ConcurrencyGuard,
    _capacity: P,
}

impl<S: UsageSlot, P: CapacityPermit> Committed<S, P> {
    #[must_use]
    pub fn units(&self) -> CostUnits {
        self.units
    }

    #[must_use]
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// See [`RequestContext::estimate_remaining`]. This is the one a response
    /// carries: the charge for this request is already in it.
    #[must_use]
    pub fn estimate_remaining(&self) -> Option<CostUnits> {
        self._concurrency.state().estimate_remaining()
    }
}

impl<S: UsageSlot, P: CapacityPermit> Drop for Committed<S, P> {
    fn drop(&mut self) {
        if let (Some(event), Some(slot)) = (self.event.take(), self.slot.take()) {
            slot.record(event);
        }
    }
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

    /// Observability surface: what this instance has admitted and refused.
    #[must_use]
    pub fn counters(&self) -> &AdmissionCounters {
        self.map.counters()
    }

    /// Resolve and authorize one immutable generation before body decoding.
    pub fn begin(
        &self,
        principal: Principal,
        required: PermissionBits,
        now: Timestamp,
    ) -> Result<RequestContext, DenyReason> {
        let locality = Locality::current();
        let outcome = self.begin_inner(principal, required, now, locality);
        if let Err(reason) = &outcome {
            self.map.counters().record_deny_at(reason, locality);
        }
        outcome
    }

    fn begin_inner(
        &self,
        principal: Principal,
        required: PermissionBits,
        now: Timestamp,
        locality: Locality,
    ) -> Result<RequestContext, DenyReason> {
        let state = match self.map.get_at(&principal, locality) {
            Some(MapEntry::Present(state)) => state,
            Some(MapEntry::NegativeUntil { .. }) | None => {
                return Err(DenyReason::UnknownPrincipal);
            }
        };
        state.snapshot.admit(now, required)?;
        Ok(RequestContext { state, locality })
    }
}

#[derive(Debug)]
struct Priced {
    concurrency: ConcurrencyGuard,
    reservation: Reservation,
    quote: CostQuote,
}

fn compile_workload<O: OpIndex>(
    snapshot: &AccountSnapshot,
    workload: &[(O, u64)],
    now: Timestamp,
) -> Result<CostQuote, DenyReason> {
    if now >= snapshot.valid_until {
        return Err(DenyReason::SnapshotExpired);
    }
    let (quote, items, required) =
        snapshot
            .cost_table
            .quote_workload(workload)
            .map_err(|error| match error {
                QuoteError::EmptyWorkload => DenyReason::EmptyWorkload,
                QuoteError::UnknownOperation { .. } => DenyReason::UnpricedOperation,
                QuoteError::Overflow => DenyReason::CostOverflow,
            })?;
    // Work permission, as distinct from the route permission `begin` already
    // checked. It is only knowable here: which classes a request touches is a
    // property of its decoded workload, not of the route it arrived on. The
    // bits were folded by the quote's own pass, so this consults neither the
    // workload nor the map a second time.
    if !snapshot.permissions.contains_all(required) {
        return Err(DenyReason::MissingPermission);
    }
    if items > snapshot.limits.max_items_per_request() {
        return Err(DenyReason::RequestTooLarge {
            max_items: snapshot.limits.max_items_per_request(),
        });
    }
    Ok(quote)
}

fn admit_priced(
    state: Arc<AccountAdmissionState>,
    locality: Locality,
    quote: CostQuote,
    now: Timestamp,
) -> Result<Priced, DenyReason> {
    // Account-wide policy has one mutable authority shared by every principal.
    // Load it once so rate and concurrency decisions are from one generation
    // even if the control plane publishes concurrently.
    let account = state.limiter.load();
    let rate = account.rate();

    // 4. Request-count token. Every priced request costs exactly one,
    //    independently of its cost-weighted charge.
    if let Some(requests) = rate.requests() {
        match requests.check_n_at(NonZeroU32::MIN, locality) {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => {
                return deny_priced(&state, DenyReason::RequestRateLimited, locality);
            }
        }
    }

    // 5. Weighted rate token. Cost-weighted: heavy requests draw down the
    //    bucket proportionally. A disabled bucket performs no governor
    //    check and the carried legacy pair remains rollout data only.
    //
    //    A weight beyond the bucket's whole burst can never pass, however
    //    long the caller waits — that is a schedule whose batch cap admits
    //    a quote its burst cannot hold, and it is reported as such rather
    //    than as throttling (#40). Deciding it here, in full width against
    //    the configured burst, is what keeps the u32 conversion below
    //    honest: the weight is known to fit the bucket before it is
    //    narrowed, so narrowing can no longer disguise an unadmittable
    //    request as an ordinary empty bucket.
    if let Some(weighted_rate) = rate.policy().weighted_rate() {
        if quote.total.get() > weighted_rate.burst_units() {
            return deny_priced(
                &state,
                DenyReason::UnpriceableUnderLimits {
                    weight: quote.total,
                    burst_units: CostUnits(weighted_rate.burst_units()),
                },
                locality,
            );
        }
        let weight = u32::try_from(quote.total.get()).unwrap_or(u32::MAX);
        match NonZeroU32::new(weight) {
            // Zero-cost requests draw no token; the minimum-charge floor
            // makes this unreachable for any real table.
            None => {}
            Some(n) => match rate
                .weighted()
                .expect("configured weighted rate has an installed bucket")
                .check_n_at(n, locality)
            {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return deny_priced(&state, DenyReason::RateLimited, locality),
                // Unreachable: the check above clears the quote against
                // the whole burst, and split buckets are sized so every
                // legitimate maximum quote fits at least one shard.
                Err(_) => {
                    return deny_priced(
                        &state,
                        DenyReason::UnpriceableUnderLimits {
                            weight: quote.total,
                            burst_units: CostUnits(weighted_rate.burst_units()),
                        },
                        locality,
                    );
                }
            },
        }
    }

    // 6. Concurrency: principal first, then account. The RAII guard undoes
    //    either acquisition on every later refusal and remains held by
    //    `Admitted` for the caller's full in-flight interval.
    let concurrency = match AccountAdmissionState::acquire_concurrency(
        state,
        account.max_concurrent_requests(),
        locality,
    ) {
        Ok(concurrency) => concurrency,
        Err((reason, state)) => return deny_priced(&state, reason, locality),
    };

    // 7. Quota: debit the lease and open the state machine. Note the
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
    let reservation = match reserve_from_lease(concurrency.state(), quote.total, now, locality) {
        Ok(reservation) => reservation,
        Err(denied) => match reserve_from_overage(concurrency.state(), quote.total, denied) {
            Ok(reservation) => reservation,
            Err(reason) => {
                concurrency
                    .state()
                    .counters
                    .record_deny_at(&reason, locality);
                return Err(reason);
            }
        },
    };

    if reservation.admitted_as_overage() {
        concurrency
            .state()
            .counters
            .record_admit_overage_at(quote.total, locality);
    } else {
        concurrency
            .state()
            .counters
            .record_admit_at(quote.total, locality);
    }

    Ok(Priced {
        concurrency,
        reservation,
        quote,
    })
}

#[inline]
fn deny_priced(
    state: &AccountAdmissionState,
    reason: DenyReason,
    locality: Locality,
) -> Result<Priced, DenyReason> {
    state.counters.record_deny_at(&reason, locality);
    Err(reason)
}

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
        DenyReason::LeaseUnavailable | DenyReason::LeaseExpired | DenyReason::LeaseExhausted { .. }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maps::{ArcSwapSnapshotMap, MokaSnapshotMap};
    use crate::state::LeaseSlot;
    use tollgate_core::EnforcementMode;
    use tollgate_core::{
        AccountId, AccountStatus, BudgetView, CancelOutcome, CostTable, CostUnits, DiscardedUsage,
        DiscardedUsageSlot, FencingToken, Generation, LeaseGrant, LeaseId, LocalLease,
        LocalSharding, PublishableSnapshot, ResolvedLimits, Retry,
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

    /// A snapshot granting the route bit but not the work bit a class needs.
    fn snapshot_without_work_permission() -> Arc<AccountSnapshot> {
        Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(1),
                AccountStatus::Active,
                t(10_000),
                // Route permission only. The class below wants bit 5.
                PermissionBits::bit(0),
                ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000),
                Arc::new(
                    CostTable::builder(CostUnits(50), CostUnits(50))
                        .class(&Op::Price, CostUnits(1), PermissionBits::bit(5))
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
                strict.admit_one(request(1), t(0)).unwrap_err(),
                expected_strict,
                "strict must still deny with {name}"
            );

            let elastic = elastic_engine(1_000, lease_units);
            let admitted = elastic
                .admit_one(request(1), t(0))
                .unwrap_or_else(|denied| panic!("elastic denied {name}: {denied}"));
            assert!(admitted.reservation.admitted_as_overage());
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

        let admitted = engine.admit_one(request(1), t(10)).expect("elastic admits");
        assert!(admitted.reservation.admitted_as_overage());
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
            assert!(engine.admit_one(request(1), t(0)).is_err(), "{status:?}");
            assert_eq!(overage_spent(&engine), CostUnits::ZERO);
        }

        // A stale snapshot, an oversized batch, and an unpriced operation.
        let engine = elastic_engine(1_000, None);
        assert_eq!(
            engine.admit_one(request(1), t(20_000)).unwrap_err(),
            DenyReason::SnapshotExpired
        );
        assert_eq!(
            engine.admit_one(request(65), t(0)).unwrap_err(),
            DenyReason::RequestTooLarge { max_items: 64 }
        );
        assert_eq!(
            engine
                .admit_one(
                    TestRequest {
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
                .admit_one(
                    TestRequest {
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
            let admitted = engine.admit_one(request(1), t(0)).expect("within the cap");
            admitted
                .reservation
                .commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
                .expect("commit");
        }
        let denied = engine.admit_one(request(1), t(0)).unwrap_err();
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
                .admit_one(request(1), now)
                .unwrap_or_else(|denied| panic!("the cap must cover {name}: {denied}"))
                .reservation
                .commit_at_execution_start(now, CommitFunding::LeaseOnly)
                .expect("commit the local overage");

            let denied = engine.admit_one(request(1), now).unwrap_err();
            assert!(
                matches!(denied, DenyReason::OverageCapExhausted { .. }),
                "the stable local cap reason must survive {name}"
            );
            assert_eq!(denied.retry(), Retry::Transient, "lease was {name}");

            slot.install(lease(51));
            let admitted = engine
                .admit_one(request(1), now)
                .unwrap_or_else(|denied| panic!("a refill must recover {name}: {denied}"));
            assert!(!admitted.reservation.admitted_as_overage());
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
                    scope.spawn(move || match engine.admit_one(request(1), t(0)) {
                        Ok(admitted) => {
                            assert!(admitted.reservation.admitted_as_overage());
                            admitted
                                .reservation
                                .commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
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
            engine.admit_one(request(1), t(0)).unwrap_err(),
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

        let admitted = engine
            .admit_one(request(1), t(0))
            .expect("the lease funds it");
        assert!(!admitted.reservation.admitted_as_overage());
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
        let admitted = engine.admit_one(request(1), t(0)).expect("within the cap");
        let denied = engine.admit_one(request(1), t(0)).unwrap_err();
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
            .admit_one(request(1), t(0))
            .expect("the cancelled credit is available again");
    }

    /// A lease that can still fund the quote is used, and the overage counter
    /// stays untouched: elastic mode is a fallback, never a preference.
    #[test]
    fn elastic_prefers_the_lease_while_it_can_fund_the_quote() {
        let engine = elastic_engine(1_000, Some(1_000));
        let admitted = engine
            .admit_one(request(1), t(0))
            .expect("the lease funds it");
        assert!(!admitted.reservation.admitted_as_overage());
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
        let _held = engine.admit_one(request(1), t(0)).expect("within the cap");
        engine
            .admit_one(request(1), t(0))
            .expect_err("beyond the cap");

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
        strict
            .admit_one(request(1), t(0))
            .expect("the lease funds it");
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
            .admit_one(request(1), t(0))
            .expect("the first key spends");
        assert_eq!(
            engine
                .admit_one(
                    TestRequest {
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
                    .admit_one(
                        TestRequest {
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
                .admit_one(
                    TestRequest {
                        principal: Principal(2),
                        ..request(1)
                    },
                    t(0)
                )
                .is_err(),
            "caps must not add up across an account's principals"
        );
        // And the smaller cap is already over its own limit, so it refuses too.
        assert!(engine.admit_one(request(1), t(0)).is_err());
        drop(held);
    }

    /// Republishing a snapshot must not reset the counter: the cap bounds an
    /// account's exposure over time, and a control plane that publishes
    /// frequently would otherwise hand out unlimited credit in cap-sized
    /// slices.
    #[test]
    fn republishing_a_snapshot_does_not_reset_the_cap() {
        let engine = elastic_engine(51, Some(0));
        let _held = engine.admit_one(request(1), t(0)).expect("within the cap");

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
            engine.admit_one(request(1), t(0)).unwrap_err(),
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
        let _held = engine.admit_one(request(1), t(0)).expect("within the cap");
        assert!(engine.admit_one(request(1), t(0)).is_err());

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
            .admit_one(request(1), t(0))
            .expect("the raised cap admits the next request");
    }

    /// One request's inputs, for the tests whose subject is a single
    /// admission outcome rather than the staging itself. The staged tests
    /// above drive `begin` and `RequestContext::admit` directly.
    #[derive(Debug, Clone, Copy)]
    struct TestRequest<'a, O: OpIndex> {
        principal: Principal,
        required: PermissionBits,
        op: &'a O,
        items: u64,
    }

    impl<M: SnapshotMap> AdmissionEngine<M> {
        /// Stage one and stage two in one call, so a test that is about
        /// pricing, funding, or a deny reason does not restate the handoff.
        fn admit_one<O: OpIndex>(
            &self,
            request: TestRequest<'_, O>,
            now: Timestamp,
        ) -> Result<Pending<DiscardedUsageSlot>, DenyReason> {
            self.begin(request.principal, request.required, now)
                .and_then(|context| {
                    context.admit(
                        &[(request.op, request.items)],
                        DiscardedUsage::new().slot(),
                        now,
                    )
                })
        }
    }

    // --- Instance-visible balance (#97) -------------------------------------
    //
    // A quote here is 50 fixed + 1 per item, so `request(n)` costs `50 + n`.

    /// An engine whose snapshot carries `balance_at_publish`, funded by a
    /// lease large enough that the estimate is never what refuses a request.
    fn budgeted_engine(balance: u64, generation: u64) -> AdmissionEngine<ArcSwapSnapshotMap> {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        install_budget(&engine, balance, generation);
        engine
    }

    fn install_budget(engine: &AdmissionEngine<ArcSwapSnapshotMap>, balance: u64, generation: u64) {
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.generation = Generation(generation);
        snapshot.budget = Some(BudgetView {
            balance_at_publish: CostUnits(balance),
            period_end: None,
        });
        engine.map().install(Principal(1), Arc::new(snapshot), slot);
    }

    fn estimate(engine: &AdmissionEngine<ArcSwapSnapshotMap>) -> Option<CostUnits> {
        engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .expect("an active principal begins")
            .estimate_remaining()
    }

    /// `None` is not zero. A snapshot published by a control plane that
    /// predates this field would otherwise make every instance tell every
    /// caller they were out of quota — a number nobody could stand behind,
    /// reported with the same confidence as a real one.
    #[test]
    fn an_instance_reports_no_estimate_when_the_control_plane_reported_no_budget() {
        let engine = engine_with(AccountStatus::Active, Some(1_000));
        assert_eq!(estimate(&engine), None);
    }

    /// The subtraction that makes the number worth publishing: the ledger's
    /// figure ages, and the instance knows exactly how much of that ageing it
    /// caused itself.
    #[test]
    fn an_estimate_subtracts_what_this_instance_admitted_since_publication() {
        let engine = budgeted_engine(1_000, 1);
        assert_eq!(estimate(&engine), Some(CostUnits(1_000)));

        let pending = engine.admit_one(request(10), t(0)).expect("admits");
        assert_eq!(
            pending.estimate_remaining(),
            Some(CostUnits(940)),
            "this request's own 60-unit quote is already counted"
        );
        let _released = pending.cancel();

        assert_eq!(
            estimate(&engine),
            Some(CostUnits(940)),
            "a cancelled admission still reads as spent: the estimate errs low, \
             which is the safe direction for a number a customer acts on"
        );
    }

    /// A republish is a fresh statement of the ledger, so the instance's own
    /// tally has to restart with it. Carrying the old baseline forward would
    /// double-count every unit spent before the refresh.
    #[test]
    fn a_republish_rebases_the_estimate() {
        let engine = budgeted_engine(1_000, 1);
        engine.admit_one(request(10), t(0)).expect("admits");
        assert_eq!(estimate(&engine), Some(CostUnits(940)));

        install_budget(&engine, 800, 2);

        assert_eq!(
            estimate(&engine),
            Some(CostUnits(800)),
            "the new figure already accounts for what was spent before it"
        );
        engine.admit_one(request(10), t(0)).expect("admits");
        assert_eq!(estimate(&engine), Some(CostUnits(740)));
    }

    /// Spend outruns the published figure whenever the fleet is busy between
    /// refreshes. Zero is the floor; a wrapped estimate would report a
    /// customer's exhausted quota as astronomically large.
    #[test]
    fn an_estimate_saturates_at_zero_rather_than_wrapping() {
        let engine = budgeted_engine(100, 1);
        engine.admit_one(request(10), t(0)).expect("admits");
        engine.admit_one(request(10), t(0)).expect("admits");

        assert_eq!(estimate(&engine), Some(CostUnits::ZERO));
    }

    /// The stage a response is actually built from. `Committed` is what a
    /// served request holds while its kernel runs, so an embedder answering
    /// "how much is left" on a successful response reads it here — and it must
    /// give the same answer the earlier stages would, not a fresh `None`
    /// because the guard forgot to carry the state.
    #[test]
    fn a_committed_request_reports_the_estimate_its_response_carries() {
        let engine = budgeted_engine(1_000, 1);

        let committed = engine
            .admit_one(request(10), t(0))
            .expect("admits")
            .acquire_capacity(&NoGate)
            .expect("NoGate always permits")
            .commit(RequestId(1), t(0))
            .expect("a fresh reservation commits");

        assert_eq!(
            committed.estimate_remaining(),
            Some(CostUnits(940)),
            "the 60 units this request was charged are already subtracted"
        );

        // And it keeps moving as the instance serves more, rather than being
        // frozen at whatever the first response happened to see.
        engine.admit_one(request(10), t(0)).expect("admits");
        assert_eq!(committed.estimate_remaining(), Some(CostUnits(880)));
    }

    /// The estimate is a report, never an input. Admission denies from the
    /// lease and the ledger (INVARIANTS.md #1); if a stale published zero
    /// could refuse, a refresh delay would become an outage.
    #[test]
    fn an_exhausted_estimate_does_not_deny() {
        let engine = budgeted_engine(0, 1);

        let pending = engine
            .admit_one(request(10), t(0))
            .expect("the lease funds this request whatever the published balance says");

        assert_eq!(pending.estimate_remaining(), Some(CostUnits::ZERO));
        let _released = pending.cancel();
    }

    fn request(items: u64) -> TestRequest<'static, Op> {
        request_for(Principal(1), items)
    }

    fn request_for(principal: Principal, items: u64) -> TestRequest<'static, Op> {
        TestRequest {
            principal,
            required: PermissionBits::bit(0),
            op: &Op::Price,
            items,
        }
    }

    #[test]
    fn full_pipeline_admits_and_commits() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let admitted = engine.admit_one(request(14), t(0)).unwrap();
        assert_eq!(admitted.quote.total, CostUnits(64));
        admitted
            .reservation
            .commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
            .unwrap();
    }

    /// The staged path under `Strict`: a lease whose window lapsed between
    /// admission and execution start produces **no** `Committed` guard, so the
    /// kernel cannot run. `Committed` is the only proof execution may begin,
    /// and the type system is what enforces that here.
    #[test]
    fn a_strict_expiry_at_execution_start_produces_no_committed_guard() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease_until(10_000, t(100)));
        engine
            .map()
            .install(Principal(1), snapshot(AccountStatus::Active), slot);

        let ready = engine
            .admit_one(request(1), t(99))
            .expect("the request admits while the lease is usable")
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled");

        // The window lapses before the worker starts.
        let (error, _released) = ready
            .commit(RequestId(1), t(100))
            .expect_err("an expired lease must not produce an execution guard");
        assert_eq!(
            error,
            CommitError::Denied(DenyReason::FundingExpiredAtStart)
        );
    }

    /// The same lapse under `Elastic` settles against overage instead of
    /// refusing: the worker gets its guard, the charge stands in full, and the
    /// bill names no lease — the lease that funded admission was returned.
    #[test]
    fn an_elastic_expiry_at_execution_start_produces_a_guard_billed_as_overage() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease_until(10_000, t(100)));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(10_000),
        };
        let overage = Arc::clone(slot.overage());
        engine.map().install(Principal(1), Arc::new(snapshot), slot);

        let pending = engine
            .admit_one(request(1), t(99))
            .expect("the request admits while the lease is usable");
        let quote = pending.quote().total;
        let committed = pending
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .commit(RequestId(1), t(100))
            .expect("an elastic lapse settles against overage");

        assert_eq!(committed.units(), quote, "the full quote still stands");
        assert_eq!(
            overage.spent(),
            quote,
            "the charge moved to the overage counter"
        );
    }

    /// A commit-time fallback that cannot fit inside the cap reports the
    /// overage counter's own refusal, with its own retry classification —
    /// never relabelled as expired funding, which would tell the caller the
    /// lease was the problem.
    #[test]
    fn a_commit_time_fallback_beyond_the_cap_reports_the_overage_refusal() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease_until(10_000, t(100)));
        let mut snapshot = AccountSnapshot::clone(&snapshot(AccountStatus::Active));
        // A cap far below any quote this request can produce.
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(1),
        };
        engine.map().install(Principal(1), Arc::new(snapshot), slot);

        let (error, _released) = engine
            .admit_one(request(1), t(99))
            .expect("the request admits while the lease is usable")
            .acquire_capacity(&NoGate)
            .expect("capacity is disabled")
            .commit(RequestId(1), t(100))
            .expect_err("a fallback beyond the cap cannot produce a guard");
        let CommitError::Denied(reason) = error else {
            panic!("expected a classified funding denial, got {error:?}")
        };
        assert!(
            matches!(reason, DenyReason::OverageCapExhausted { .. }),
            "expected the overage counter's own refusal, got {reason:?}"
        );
        assert_eq!(reason.retry(), Retry::Transient);
    }

    #[test]
    fn unknown_principal_denies() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::UnknownPrincipal
        );
    }

    #[test]
    fn negative_cache_denies() {
        let engine = AdmissionEngine::new(MokaSnapshotMap::new(10));
        engine.map().install_unknown(Principal(1), t(100));
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::UnknownPrincipal
        );
    }

    #[test]
    fn suspended_account_denies() {
        let engine = engine_with(AccountStatus::Suspended, Some(10_000));
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::AccountSuspended
        );
    }

    #[test]
    fn batch_cap_denies() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        assert_eq!(
            engine.admit_one(request(65), t(0)).unwrap_err(),
            DenyReason::RequestTooLarge { max_items: 64 }
        );
    }

    #[test]
    fn unpriced_operation_denies() {
        let engine = engine_with(AccountStatus::Active, Some(10_000));
        let req = TestRequest {
            principal: Principal(1),
            required: PermissionBits::bit(0),
            op: &Op::Unpriced,
            items: 1,
        };
        assert_eq!(
            engine.admit_one(req, t(0)).unwrap_err(),
            DenyReason::UnpricedOperation
        );
    }

    #[test]
    fn missing_lease_denies_cold_start() {
        let engine = engine_with(AccountStatus::Active, None);
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseUnavailable
        );
    }

    #[test]
    fn exhausted_lease_denies_and_charges_zero() {
        let engine = engine_with(AccountStatus::Active, Some(60));
        // First request (51 units) fits; second is denied by remaining=9.
        let admitted = engine.admit_one(request(1), t(0)).unwrap();
        assert_eq!(admitted.quote.total, CostUnits(51));
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseExhausted {
                remaining: CostUnits(9)
            }
        );
        // Cancelling the first returns its units; admission works again.
        assert_eq!(admitted.reservation.cancel(), CancelOutcome::ZeroCharged);
        engine.admit_one(request(1), t(0)).unwrap();
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
            let admitted = engine.admit_one(req, t(0)).unwrap();
            assert_eq!(admitted.quote.total, CostUnits(100));
        }
        assert_eq!(
            engine.admit_one(req, t(0)).unwrap_err(),
            DenyReason::RateLimited,
            "an empty-but-refilling bucket is throttling, not misconfiguration"
        );
    }

    #[test]
    fn request_rate_limiter_counts_requests_not_cost() {
        let limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_request_rate(NonZeroU32::new(1).unwrap(), NonZeroU32::new(2).unwrap());
        let engine = engine_with_limits(limits);

        // Different quotes each consume one request token.
        drop(engine.admit_one(request(1), t(0)).unwrap());
        drop(engine.admit_one(request(64), t(0)).unwrap());
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RequestRateLimited
        );
    }

    #[test]
    fn disabled_weighted_rate_performs_no_weighted_check() {
        let engine = engine_with_limits(
            ResolvedLimits::new(64).with_weighted_rate_compatibility_fallback(1, 1),
        );

        // Every request quotes far beyond the fallback burst. A new reader
        // treats that pair as rollout data when the explicit flag disables
        // the bucket, while an old reader conservatively keeps enforcing it.
        for _ in 0..3 {
            drop(engine.admit_one(request(64), t(0)).unwrap());
        }
    }

    #[test]
    fn changing_disabled_weighted_fallback_does_not_refill_request_rate() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));

        let mut original = (*snapshot(AccountStatus::Active)).clone();
        original.limits = ResolvedLimits::new(64)
            .with_weighted_rate_compatibility_fallback(1, 1)
            .with_request_rate(NonZeroU32::MIN, NonZeroU32::MIN);
        engine
            .map()
            .install(Principal(1), Arc::new(original), Arc::clone(&slot));

        drop(engine.admit_one(request(1), t(0)).unwrap());
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RequestRateLimited
        );

        let mut compatibility_only = (*snapshot(AccountStatus::Active)).clone();
        compatibility_only.generation = Generation(2);
        compatibility_only.limits = ResolvedLimits::new(64)
            .with_weighted_rate_compatibility_fallback(2, 2)
            .with_request_rate(NonZeroU32::MIN, NonZeroU32::MIN);
        engine.map().install(
            Principal(1),
            Arc::new(compatibility_only),
            Arc::clone(&slot),
        );

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RequestRateLimited,
            "inactive compatibility metadata cannot refill an unchanged request bucket"
        );
    }

    #[test]
    fn enabling_request_rate_does_not_refill_weighted_rate() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));

        let mut weighted_only = (*snapshot(AccountStatus::Active)).clone();
        weighted_only.limits = ResolvedLimits::new(64).with_weighted_rate(1, 51);
        engine
            .map()
            .install(Principal(1), Arc::new(weighted_only), Arc::clone(&slot));

        drop(engine.admit_one(request(1), t(0)).unwrap());
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RateLimited
        );

        let mut request_rate_added = (*snapshot(AccountStatus::Active)).clone();
        request_rate_added.generation = Generation(2);
        request_rate_added.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1, 51)
            .with_request_rate(
                NonZeroU32::new(1_000_000).unwrap(),
                NonZeroU32::new(1_000_000).unwrap(),
            );
        engine.map().install(
            Principal(1),
            Arc::new(request_rate_added),
            Arc::clone(&slot),
        );

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RateLimited,
            "changing request-rate policy cannot refill an unchanged weighted bucket"
        );
    }

    /// Two valid principal snapshots can share an account and generation but
    /// disagree about whether weighted rate is enabled. The account bucket
    /// selected by the first publication must be the policy advertised by the
    /// second installed state too; otherwise admission dereferences a bucket
    /// the state does not contain and aborts in production.
    #[test]
    fn divergent_enabled_snapshot_cannot_outlive_a_disabled_account_bucket() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));

        let mut disabled = (*snapshot(AccountStatus::Active)).clone();
        disabled.limits = ResolvedLimits::new(64)
            .with_weighted_rate_compatibility_fallback(1_000_000, 1_000_000)
            .with_request_rate(NonZeroU32::MIN, NonZeroU32::MIN);
        engine
            .map()
            .install(Principal(1), Arc::new(disabled), Arc::clone(&slot));

        let mut enabled = (*snapshot(AccountStatus::Active)).clone();
        enabled.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_request_rate(NonZeroU32::new(10).unwrap(), NonZeroU32::new(10).unwrap());
        engine
            .map()
            .install(Principal(2), Arc::new(enabled), Arc::clone(&slot));

        let admitted = engine
            .admit_one(request_for(Principal(2), 1), t(0))
            .expect("the canonical disabled account bucket performs no check");
        let MapEntry::Present(state) = engine.map().get(&Principal(2)).unwrap() else {
            panic!("principal present");
        };
        let account = state.limiter.current();
        assert_eq!(account.rate().policy().weighted_rate(), None);
        assert_eq!(
            account
                .rate()
                .policy()
                .request_rate()
                .expect("the canonical request bucket is enabled")
                .burst_requests(),
            NonZeroU32::MIN,
        );
        drop(admitted);
    }

    /// The inverse mismatch must not let a principal whose submitted snapshot
    /// disables weighted rate bypass the enabled account bucket it joined.
    #[test]
    fn divergent_disabled_snapshot_cannot_bypass_an_enabled_account_bucket() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));

        let mut enabled = (*snapshot(AccountStatus::Active)).clone();
        enabled.limits = ResolvedLimits::new(64).with_weighted_rate(1, 51);
        engine
            .map()
            .install(Principal(1), Arc::new(enabled), Arc::clone(&slot));

        let mut disabled = (*snapshot(AccountStatus::Active)).clone();
        disabled.limits =
            ResolvedLimits::new(64).with_weighted_rate_compatibility_fallback(1_000_000, 1_000_000);
        engine
            .map()
            .install(Principal(2), Arc::new(disabled), Arc::clone(&slot));

        drop(
            engine
                .admit_one(request_for(Principal(2), 1), t(0))
                .expect("the first request consumes the whole account burst"),
        );
        assert_eq!(
            engine
                .admit_one(request_for(Principal(2), 1), t(0))
                .unwrap_err(),
            DenyReason::RateLimited
        );
    }

    /// Tightening a shared bucket's safe shard count publishes one new
    /// account authority. An already-installed principal must load that same
    /// authority on its next request instead of continuing to refill and
    /// spend an old partition in parallel.
    #[test]
    fn shard_tightening_cannot_leave_two_spendable_account_buckets() {
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        slot.install(lease(1_000_000));

        let priced = |quote: u64| {
            let snapshot = Arc::new(
                AccountSnapshot::builder(
                    AccountId(1),
                    Generation(1),
                    AccountStatus::Active,
                    t(10_000),
                    PermissionBits::bit(0),
                    ResolvedLimits::new(1).with_weighted_rate(1, 800),
                    Arc::new(
                        CostTable::builder(CostUnits(quote), CostUnits(quote))
                            .weight(&Op::Price, CostUnits::ZERO)
                            .build(),
                    ),
                )
                .build(),
            );
            PublishableSnapshot::try_new(snapshot).unwrap()
        };
        engine
            .map()
            .install_publishable(Principal(1), priced(100), Arc::clone(&slot));
        engine
            .map()
            .install_publishable(Principal(2), priced(500), Arc::clone(&slot));

        for _ in 0..8 {
            drop(
                engine
                    .admit_one(request_for(Principal(1), 1), t(0))
                    .expect("the shared 800-unit burst admits eight 100-unit requests"),
            );
        }
        assert_eq!(
            engine
                .admit_one(request_for(Principal(2), 1), t(0))
                .unwrap_err(),
            DenyReason::RateLimited,
            "the heavier principal cannot retain a second account bucket"
        );
    }

    #[test]
    fn account_concurrency_is_held_for_the_admitted_lifetime() {
        let limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(1).unwrap(), None)
            .unwrap();
        let engine = engine_with_limits(limits);

        let first = engine.admit_one(request(1), t(0)).unwrap();
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::ConcurrencyLimited
        );
        drop(first);
        drop(engine.admit_one(request(1), t(0)).unwrap());
    }

    #[test]
    fn every_principal_observes_the_canonical_account_concurrency_ceiling() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));

        let mut bounded = (*snapshot(AccountStatus::Active)).clone();
        bounded.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::MIN, None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(bounded), Arc::clone(&slot));

        let mut unbounded = (*snapshot(AccountStatus::Active)).clone();
        unbounded.limits = ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000);
        engine
            .map()
            .install(Principal(2), Arc::new(unbounded), Arc::clone(&slot));

        let mut wider = (*snapshot(AccountStatus::Active)).clone();
        wider.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(8).unwrap(), None)
            .unwrap();
        engine
            .map()
            .install(Principal(3), Arc::new(wider), Arc::clone(&slot));

        let held = engine
            .admit_one(request_for(Principal(2), 1), t(0))
            .expect("the canonical account slot is initially free");
        assert_eq!(
            engine
                .admit_one(request_for(Principal(1), 1), t(0))
                .unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "an unbounded sibling must not bypass the account's selected ceiling"
        );
        assert_eq!(
            engine
                .admit_one(request_for(Principal(3), 1), t(0))
                .unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "a larger sibling value cannot widen the selected account ceiling"
        );
        drop(held);
    }

    #[test]
    fn newer_account_concurrency_policy_reaches_existing_principals() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));

        let mut original = (*snapshot(AccountStatus::Active)).clone();
        original.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(2).unwrap(), None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(original), Arc::clone(&slot));

        let mut narrower = (*snapshot(AccountStatus::Active)).clone();
        narrower.generation = Generation(2);
        narrower.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::MIN, None)
            .unwrap();
        engine
            .map()
            .install(Principal(2), Arc::new(narrower), Arc::clone(&slot));

        let held = engine
            .admit_one(request_for(Principal(1), 1), t(0))
            .unwrap();
        assert_eq!(
            engine
                .admit_one(request_for(Principal(1), 1), t(0))
                .unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "an existing principal must load the newer account ceiling"
        );
        drop(held);
    }

    #[test]
    fn enabling_account_concurrency_counts_already_in_flight_work() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));

        let held = {
            let mut unlimited = (*snapshot(AccountStatus::Active)).clone();
            unlimited.limits = ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000);
            engine
                .map()
                .install(Principal(1), Arc::new(unlimited), Arc::clone(&slot));
            engine.admit_one(request(1), t(0)).unwrap()
        };

        let mut limited = (*snapshot(AccountStatus::Active)).clone();
        limited.generation = Generation(2);
        limited.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::MIN, None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(limited), Arc::clone(&slot));

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "enabling a ceiling must include work admitted while enforcement was disabled"
        );
        drop(held);
        drop(engine.admit_one(request(1), t(0)).unwrap());
    }

    #[test]
    fn enabling_principal_concurrency_counts_already_in_flight_work() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));

        let mut account_only = (*snapshot(AccountStatus::Active)).clone();
        account_only.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(2).unwrap(), None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(account_only), Arc::clone(&slot));
        let held = engine.admit_one(request(1), t(0)).unwrap();

        let mut principal_limited = (*snapshot(AccountStatus::Active)).clone();
        principal_limited.generation = Generation(2);
        principal_limited.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(2).unwrap(), Some(NonZeroU32::MIN))
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(principal_limited), Arc::clone(&slot));

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "a newly enabled principal ceiling must include existing work for that principal"
        );
        drop(held);
        drop(engine.admit_one(request(1), t(0)).unwrap());
    }

    #[test]
    fn reenabled_account_concurrency_counts_work_admitted_while_disabled() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));

        let mut initially_limited = (*snapshot(AccountStatus::Active)).clone();
        initially_limited.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(2).unwrap(), None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(initially_limited), Arc::clone(&slot));
        let before_disable = engine.admit_one(request(1), t(0)).unwrap();

        let mut disabled = (*snapshot(AccountStatus::Active)).clone();
        disabled.generation = Generation(2);
        disabled.limits = ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000);
        engine
            .map()
            .install(Principal(1), Arc::new(disabled), Arc::clone(&slot));
        let while_disabled = engine.admit_one(request(1), t(0)).unwrap();

        let mut reenabled = (*snapshot(AccountStatus::Active)).clone();
        reenabled.generation = Generation(3);
        reenabled.limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(NonZeroU32::new(2).unwrap(), None)
            .unwrap();
        engine
            .map()
            .install(Principal(1), Arc::new(reenabled), Arc::clone(&slot));

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "disabling enforcement must not erase occupancy seen after re-enable"
        );
        drop(before_disable);
        drop(while_disabled);
        drop(engine.admit_one(request(1), t(0)).unwrap());
    }

    #[test]
    fn a_principal_ceiling_narrows_without_bypassing_the_account_ceiling() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000_000));
        let limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_concurrency(
                NonZeroU32::new(2).unwrap(),
                Some(NonZeroU32::new(1).unwrap()),
            )
            .unwrap();
        for principal in [Principal(1), Principal(2)] {
            let mut account = (*snapshot(AccountStatus::Active)).clone();
            account.limits = limits;
            engine
                .map()
                .install(principal, Arc::new(account), Arc::clone(&slot));
        }

        let first = engine
            .admit_one(request_for(Principal(1), 1), t(0))
            .unwrap();
        assert_eq!(
            engine
                .admit_one(request_for(Principal(1), 1), t(0))
                .unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "the principal-local ceiling binds first"
        );
        let second = engine
            .admit_one(request_for(Principal(2), 1), t(0))
            .unwrap();
        assert_eq!(
            engine
                .admit_one(request_for(Principal(2), 1), t(0))
                .unwrap_err(),
            DenyReason::ConcurrencyLimited,
            "the shared account ceiling still binds"
        );
        drop(first);
        drop(second);
    }

    #[test]
    fn a_later_funding_refusal_releases_concurrency_but_keeps_rate_tokens() {
        let limits = ResolvedLimits::new(64)
            .with_weighted_rate(1_000_000, 1_000_000)
            .with_request_rate(NonZeroU32::new(1).unwrap(), NonZeroU32::new(1).unwrap())
            .with_concurrency(NonZeroU32::new(1).unwrap(), None)
            .unwrap();
        let mut account = (*snapshot(AccountStatus::Active)).clone();
        account.limits = limits;
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        engine.map().install(
            Principal(1),
            Arc::new(account),
            LeaseSlot::for_account(AccountId(1)),
        );

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseUnavailable
        );
        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::RequestRateLimited,
            "the request token is not refunded after a funding refusal"
        );
        let state = match engine.map().get(&Principal(1)).unwrap() {
            MapEntry::Present(state) => state,
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };
        assert_eq!(state.account_concurrency_in_flight(), 0);
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
            engine.admit_one(request(64), t(0)).unwrap_err(),
            DenyReason::UnpriceableUnderLimits {
                weight: CostUnits(114),
                burst_units: CostUnits(64),
            }
        );
        // Repeating never converts it into ordinary throttling.
        assert_eq!(
            engine.admit_one(request(64), t(0)).unwrap_err(),
            DenyReason::UnpriceableUnderLimits {
                weight: CostUnits(114),
                burst_units: CostUnits(64),
            }
        );
        // A request the burst *can* hold still admits: the deny is about this
        // request's weight, not a wedged account.
        engine.admit_one(request(1), t(0)).unwrap();
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

        let admitted = engine.admit_one(request(64), t(0)).unwrap();
        assert_eq!(admitted.quote.total, CostUnits(114));
        // The bucket is now empty, so the next one is ordinary throttling —
        // never the terminal reason.
        assert_eq!(
            engine.admit_one(request(64), t(0)).unwrap_err(),
            DenyReason::RateLimited
        );
    }

    /// A zero burst is not silently repaired into a burst of one: every
    /// priced request is refused, and says why.
    ///
    /// Publication rejects this schedule outright
    /// (`WeightedRateOutsideGovernorDomain`), so the configuration under test
    /// is reachable only through the unvalidated `SnapshotMap::install` seam.
    /// That seam is exactly what the runtime `UnpriceableUnderLimits` check
    /// exists to backstop: the governor quota is clamped to a nonzero burst
    /// for construction, and the full-width comparison must still refuse.
    #[test]
    fn zero_burst_denies_every_priced_request() {
        let engine = engine_with_limits(ResolvedLimits::new(64).with_weighted_rate(1_000, 0));

        assert_eq!(
            engine.admit_one(request(1), t(0)).unwrap_err(),
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
            engine.admit_one(request(items), t(0)).unwrap_err(),
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
            .admit_one(
                TestRequest {
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
        engine.admit_one(request(1), t(0)).unwrap();
        engine.admit_one(request(14), t(0)).unwrap();
        // Raised in the pipeline itself.
        engine.admit_one(request(65), t(0)).unwrap_err();
        engine.admit_one(request(65), t(0)).unwrap_err();
        // Propagated out of `AccountSnapshot::admit`: staleness is decided
        // against `valid_until`, never by an inline refresh.
        assert_eq!(
            engine.admit_one(request(1), t(20_000)).unwrap_err(),
            DenyReason::SnapshotExpired
        );
        // Propagated out of `AccountSnapshot::admit`: permissions.
        assert_eq!(
            engine
                .admit_one(
                    TestRequest {
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
            assert_eq!(engine.admit_one(request(1), t(0)).unwrap_err(), expected);
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
            engine.admit_one(request(1), t(200)).unwrap_err(),
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
            engine.admit_one(request(4), t(0)).unwrap_err(),
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
        let held = engine.admit_one(request(1), t(0)).unwrap();
        // The lease now has 9 units left: the next request is refused.
        engine.admit_one(request(1), t(0)).unwrap_err();

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
            engine.admit_one(request(1), t(0)).unwrap_err(),
            DenyReason::LeaseUnavailable
        );
        slot.install(lease(10_000));
        engine.admit_one(request(1), t(0)).unwrap();
    }

    /// Work permission is checked at stage two, and refusing costs nothing.
    ///
    /// `begin` cannot make this decision: which classes a request touches is a
    /// property of its decoded workload. The account here passes the route
    /// check and still may not run the class it asked for.
    #[test]
    fn a_workload_requiring_ungranted_bits_is_denied_at_stage_two() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000));
        engine
            .map()
            .install(Principal(1), snapshot_without_work_permission(), slot);

        // Stage one succeeds: the route permission is granted.
        let context = engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .expect("route permission is granted");

        let denied = context
            .admit(&[(Op::Price, 1)], DiscardedUsage::new().slot(), t(0))
            .expect_err("the class requires a bit the account lacks");
        assert_eq!(denied, DenyReason::MissingPermission);
    }

    /// A class the account *is* entitled to still admits.
    #[test]
    fn a_workload_within_granted_bits_admits() {
        let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
        let slot = LeaseSlot::for_account(AccountId(1));
        slot.install(lease(1_000));
        let snapshot = Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(1),
                AccountStatus::Active,
                t(10_000),
                PermissionBits::bit(0).union(PermissionBits::bit(5)),
                ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000),
                Arc::new(
                    CostTable::builder(CostUnits(50), CostUnits(50))
                        .class(&Op::Price, CostUnits(1), PermissionBits::bit(5))
                        .build(),
                ),
            )
            .build(),
        );
        engine.map().install(Principal(1), snapshot, slot);

        let context = engine
            .begin(Principal(1), PermissionBits::bit(0), t(0))
            .expect("route permission is granted");
        let pending = context
            .admit(&[(Op::Price, 1)], DiscardedUsage::new().slot(), t(0))
            .expect("the account holds the class bit");
        assert_eq!(pending.quote().total, CostUnits(51));
    }
}
