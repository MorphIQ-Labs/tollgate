use std::hint::black_box;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jiff::Timestamp;
use tollgate_admission::{
    AdmissionEngine, ArcSwapSnapshotMap, ExecutionCapacityGate, ExecutionCapacityMode, LeaseSlot,
    MapEntry, MokaSnapshotMap, NoGate, Principal, PublishableSnapshotUpdate, SnapshotMap,
    SnapshotUpdate,
};
use tollgate_alloc_count::AllocScope;
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, EnforcementMode, FencingToken,
    Generation, LeaseGrant, LeaseId, LocalLease, LocalSharding, Locality, OpIndex, PermissionBits,
    ResolvedLimits, UsageEvent, UsageSlot,
};

tollgate_alloc_count::install!();

#[derive(Clone, Copy)]
struct PriceOp;

impl OpIndex for PriceOp {
    fn index(&self) -> usize {
        0
    }
}

#[derive(Debug)]
struct AllocationSlot;

impl UsageSlot for AllocationSlot {
    fn record(self, _event: UsageEvent) {}
}

fn now() -> Timestamp {
    Timestamp::from_second(1_755_600_000).unwrap()
}

fn far_future() -> Timestamp {
    Timestamp::from_second(4_102_444_800).unwrap()
}

fn snapshot(account: AccountId, mode: EnforcementMode) -> Arc<AccountSnapshot> {
    snapshot_with_limits(
        account,
        mode,
        ResolvedLimits::new(4_096).with_weighted_rate(u64::from(u32::MAX), u64::from(u32::MAX)),
    )
}

fn snapshot_with_limits(
    account: AccountId,
    mode: EnforcementMode,
    limits: ResolvedLimits,
) -> Arc<AccountSnapshot> {
    Arc::new(
        AccountSnapshot::builder(
            account,
            Generation(1),
            AccountStatus::Active,
            far_future(),
            PermissionBits::bit(0),
            limits,
            Arc::new(
                CostTable::builder(CostUnits(50), CostUnits(50))
                    .weight(&PriceOp, CostUnits(1))
                    .build(),
            ),
        )
        .enforcement_mode(mode)
        .build(),
    )
}

fn engine_with_limits(limits: ResolvedLimits) -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
    let slot = LeaseSlot::for_account(AccountId(1));
    drop(slot.replace(lease(
        AccountId(1),
        CostUnits(u64::MAX / 2),
        LocalSharding::SINGLE,
    )));
    engine
        .map()
        .install(
            Principal(1),
            snapshot_with_limits(AccountId(1), EnforcementMode::Strict, limits),
            slot,
        )
        .unwrap();
    engine
}

fn lease(account: AccountId, units: CostUnits, sharding: LocalSharding) -> Arc<LocalLease> {
    Arc::new(LocalLease::with_sharding(
        LeaseGrant {
            lease_id: LeaseId(account.0),
            account_id: account,
            fencing_token: FencingToken(1),
            units,
            expires_at: far_future(),
        },
        CostUnits::ZERO,
        jiff::SignedDuration::ZERO,
        sharding,
    ))
}

fn install(
    map: &impl SnapshotMap,
    principal: Principal,
    account: AccountId,
    mode: EnforcementMode,
    units: CostUnits,
) {
    let sharding = map.local_sharding();
    let slot = LeaseSlot::with_sharding(account, sharding);
    drop(slot.replace(lease(account, units, sharding)));
    map.install(principal, snapshot(account, mode), slot)
        .unwrap();
}

fn engine(
    sharding: LocalSharding,
    principal: Principal,
    mode: EnforcementMode,
    units: CostUnits,
) -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::with_sharding(sharding));
    install(engine.map(), principal, AccountId(1), mode, units);
    engine
}

fn admit(
    engine: &AdmissionEngine<ArcSwapSnapshotMap>,
    principal: Principal,
) -> tollgate_admission::Pending<AllocationSlot> {
    engine
        .begin(principal, PermissionBits::bit(0), now())
        .unwrap()
        .admit(&[(PriceOp, 64)], AllocationSlot, now())
        .unwrap()
}

fn admit_and_cancel(engine: &AdmissionEngine<ArcSwapSnapshotMap>, principal: Principal) {
    black_box(
        admit(engine, principal)
            .acquire_capacity(&NoGate)
            .unwrap()
            .cancel(),
    );
}

fn assert_zero(scope: &str, operation: impl FnOnce()) {
    let ((), allocations) = AllocScope::measure(operation);
    tollgate_alloc_count::record_if_requested!(scope, "tollgate", allocations).unwrap();
    assert!(
        allocations.is_allocation_free(),
        "{scope} allocated: {allocations:?}"
    );
}

/// Record a scope whose allocations belong to a dependency's amortized
/// housekeeping, and hold it to a per-operation bound.
///
/// Attribution matters here rather than being bookkeeping. INVARIANTS.md #24
/// says steady-state admission allocates nothing *it owns*; moka's cache
/// maintenance is not Tollgate's, and moka does not promise a read never
/// allocates. Recording it under `tollgate` and asserting zero claimed a
/// guarantee the dependency does not give, and the claim failed on a loaded
/// runner rather than on a change (see the caller for the mechanism).
///
/// The bound is what keeps this from being an exemption that cannot fail: a
/// regression in which Tollgate itself allocated per lookup would report one
/// allocation per operation, which is far outside `max_per_operation`.
fn assert_amortized_bound(
    scope: &str,
    operations: usize,
    max_per_operation: f64,
    operation: impl FnOnce(),
) {
    let ((), allocations) = AllocScope::measure(operation);
    tollgate_alloc_count::record_if_requested!(scope, "dependency_amortized", allocations).unwrap();
    let budget = (operations as f64 * max_per_operation) as u64;
    assert!(
        allocations.alloc_calls <= budget,
        "{scope} allocated {} times over {operations} operations, past the amortized \
         budget of {budget}: {allocations:?}",
        allocations.alloc_calls
    );
}

#[test]
fn admission_allocates_nothing_on_the_arc_swap_default() {
    black_box(Locality::current());
    let sharded = LocalSharding::new(NonZeroUsize::new(8).unwrap());
    let admitted = engine(
        LocalSharding::SINGLE,
        Principal(1),
        EnforcementMode::Strict,
        CostUnits(u64::MAX / 2),
    );
    let admitted_sharded = engine(
        sharded,
        Principal(1),
        EnforcementMode::Strict,
        CostUnits(u64::MAX / 2),
    );
    let strict_exhausted = engine(
        LocalSharding::SINGLE,
        Principal(1),
        EnforcementMode::Strict,
        CostUnits::ZERO,
    );
    let elastic_exhausted = engine(
        LocalSharding::SINGLE,
        Principal(1),
        EnforcementMode::Elastic {
            overage_cap: CostUnits(10_000),
        },
        CostUnits::ZERO,
    );

    // Warm each dependency-owned thread-local path before attribution.
    admit_and_cancel(&admitted, Principal(1));
    admit_and_cancel(&admitted_sharded, Principal(1));
    black_box(
        admitted
            .begin(Principal(999), PermissionBits::bit(0), now())
            .unwrap_err(),
    );
    black_box(
        strict_exhausted
            .begin(Principal(1), PermissionBits::bit(0), now())
            .unwrap()
            .admit(&[(PriceOp, 64)], AllocationSlot, now())
            .unwrap_err(),
    );
    admit_and_cancel(&elastic_exhausted, Principal(1));

    assert_zero("admission/arc_swap_admitted", || {
        admit_and_cancel(&admitted, Principal(1));
    });
    assert_zero("admission/arc_swap_admitted_sharded", || {
        admit_and_cancel(&admitted_sharded, Principal(1));
    });
    assert_zero("admission/arc_swap_unknown", || {
        black_box(
            admitted
                .begin(Principal(999), PermissionBits::bit(0), now())
                .unwrap_err(),
        );
    });
    assert_zero("admission/arc_swap_lease_exhausted_strict", || {
        black_box(
            strict_exhausted
                .begin(Principal(1), PermissionBits::bit(0), now())
                .unwrap()
                .admit(&[(PriceOp, 64)], AllocationSlot, now())
                .unwrap_err(),
        );
    });
    assert_zero("admission/arc_swap_lease_exhausted_elastic", || {
        admit_and_cancel(&elastic_exhausted, Principal(1));
    });
}

/// The first leg of #99's three-part disabled proof, and the same claim for
/// every enabled mode: acquiring or refusing execution capacity allocates
/// nothing.
///
/// The permit carries an `Arc` clone of the pools, which is a refcount bump
/// rather than an allocation — this is what says so. The refusal scope
/// matters just as much: a fail-closed path that allocated would make an
/// overloaded instance allocate hardest exactly when it is shedding.
#[test]
fn acquiring_and_refusing_execution_capacity_allocates_nothing() {
    black_box(Locality::current());
    let admitted = engine(
        LocalSharding::SINGLE,
        Principal(1),
        EnforcementMode::Strict,
        CostUnits(u64::MAX / 2),
    );
    let enabled = |mode| {
        ExecutionCapacityGate::new(mode, LocalSharding::SINGLE)
            .unwrap()
            .unwrap()
    };
    let units = |units: u32| NonZeroU32::new(units).unwrap();

    let uniform = enabled(ExecutionCapacityMode::Uniform {
        total: units(4_096),
    });
    let reserved = enabled(ExecutionCapacityMode::Reserved {
        total: units(4_096),
        assured_reserve: units(64),
    });
    // Shared holds one unit and an admission keeps it, so assured work here
    // always reaches the reserve.
    let fallback = enabled(ExecutionCapacityMode::Reserved {
        total: units(2),
        assured_reserve: units(1),
    });
    let holding_shared = admit(&admitted, Principal(1))
        .acquire_capacity(&fallback)
        .unwrap();
    // One unit, held, so every further acquisition is refused.
    let full = enabled(ExecutionCapacityMode::Uniform { total: units(1) });
    let holding_only_unit = admit(&admitted, Principal(1))
        .acquire_capacity(&full)
        .unwrap();

    // Warm every path before attribution, as the neighbouring tests do.
    for gate in [&uniform, &reserved, &fallback] {
        black_box(
            admit(&admitted, Principal(1))
                .acquire_capacity(gate)
                .unwrap()
                .cancel(),
        );
    }
    black_box(
        admit(&admitted, Principal(1))
            .acquire_capacity(&full)
            .map(|_| ())
            .unwrap_err(),
    );

    assert_zero("capacity/disabled", || {
        admit_and_cancel(&admitted, Principal(1));
    });
    assert_zero("capacity/uniform", || {
        black_box(
            admit(&admitted, Principal(1))
                .acquire_capacity(&uniform)
                .unwrap()
                .cancel(),
        );
    });
    assert_zero("capacity/reserved_shared", || {
        black_box(
            admit(&admitted, Principal(1))
                .acquire_capacity(&reserved)
                .unwrap()
                .cancel(),
        );
    });
    assert_zero("capacity/reserved_fallback", || {
        black_box(
            admit(&admitted, Principal(1))
                .acquire_capacity(&fallback)
                .unwrap()
                .cancel(),
        );
    });
    assert_zero("capacity/shed", || {
        black_box(
            admit(&admitted, Principal(1))
                .acquire_capacity(&full)
                .map(|_| ())
                .unwrap_err(),
        );
    });

    drop(holding_shared);
    drop(holding_only_unit);
}

#[test]
fn configured_and_disabled_guards_allocate_nothing_after_warmup() {
    black_box(Locality::current());
    let request_rate = engine_with_limits(
        ResolvedLimits::new(4_096)
            .with_weighted_rate(u64::from(u32::MAX), u64::from(u32::MAX))
            .with_request_rate(
                NonZeroU32::new(u32::MAX).unwrap(),
                NonZeroU32::new(u32::MAX).unwrap(),
            ),
    );
    let concurrency = engine_with_limits(
        ResolvedLimits::new(4_096)
            .with_weighted_rate(u64::from(u32::MAX), u64::from(u32::MAX))
            .with_concurrency(
                NonZeroU32::new(u32::MAX).unwrap(),
                Some(NonZeroU32::new(u32::MAX).unwrap()),
            )
            .unwrap(),
    );
    let disabled = engine_with_limits(ResolvedLimits::new(4_096));

    for engine in [&request_rate, &concurrency, &disabled] {
        admit_and_cancel(engine, Principal(1));
    }

    assert_zero("admission/request_rate_token", || {
        admit_and_cancel(&request_rate, Principal(1));
    });
    assert_zero("admission/concurrency_acquire", || {
        admit_and_cancel(&concurrency, Principal(1));
    });
    assert_zero("admission/guards_disabled", || {
        admit_and_cancel(&disabled, Principal(1));
    });
    assert_zero("admission/begin", || {
        black_box(
            disabled
                .begin(Principal(1), PermissionBits::bit(0), now())
                .unwrap(),
        );
    });
}

#[test]
fn moka_reads_stay_within_their_amortized_allocation_budget() {
    let map = MokaSnapshotMap::new(4_096);
    for value in 0..512_u128 {
        install(
            &map,
            Principal(value),
            AccountId(1),
            EnforcementMode::Strict,
            CostUnits(u64::MAX / 2),
        );
    }
    let principal = Principal(97);
    let locality = Locality::current();

    // Crossbeam epoch and Moka read maintenance initialise on first use. Run
    // through one maintenance interval before measuring steady-state reads.
    for _ in 0..65 {
        black_box(map.get_at(&principal, locality).expect("installed"));
    }

    // Amortized, not zero — and the difference is moka's, not Tollgate's.
    //
    // `MokaSnapshotMap::get_at` allocates nothing. Moka's own housekeeper
    // drains its read log when either of two things is true
    // (`Housekeeper::should_apply`, moka 0.12.16):
    //
    //     ch_len >= READ_LOG_FLUSH_POINT || now >= self.run_after
    //
    // The first is a read count. **The second is a 300 ms wall clock**
    // (`LOG_SYNC_INTERVAL_MILLIS`), and that is why this cannot be made
    // deterministic by choosing a warmup or a read count: a loaded machine
    // that deschedules this loop crosses the timer, moka drains, and the
    // drain allocates. This assertion used to demand zero across 1,024 reads
    // and failed a release merge request whose diff was version numbers and a
    // changelog — the same shape of defect #102 removed from a Criterion
    // ratio, where a gate failed for a reason the change could not have
    // caused.
    //
    // A budget of one allocation per 32 reads leaves room for several timer
    // drains while still failing loudly at the regression that matters: were
    // Tollgate to allocate per lookup, this scope would report ~1,024 against
    // a budget of 32.
    assert_amortized_bound("admission/moka_reads_1024", 1_024, 1.0 / 32.0, || {
        for _ in 0..1_024 {
            black_box(map.get_at(&principal, locality).expect("installed"));
        }
    });
}

struct CountingMap<M> {
    inner: M,
    get: AtomicUsize,
    get_at: AtomicUsize,
}

impl<M> CountingMap<M> {
    fn new(inner: M) -> Self {
        Self {
            inner,
            get: AtomicUsize::new(0),
            get_at: AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.get.store(0, Ordering::Relaxed);
        self.get_at.store(0, Ordering::Relaxed);
    }

    fn assert_one_lookup(&self) {
        assert_eq!(self.get.load(Ordering::Relaxed), 0);
        assert_eq!(self.get_at.load(Ordering::Relaxed), 1);
    }
}

impl<M: SnapshotMap> SnapshotMap for CountingMap<M> {
    fn contains_cached(&self, principal: &Principal) -> bool {
        self.inner.contains_cached(principal)
    }
    fn history_stats(&self) -> Option<tollgate_admission::SnapshotHistoryStats> {
        self.inner.history_stats()
    }
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        self.get.fetch_add(1, Ordering::Relaxed);
        self.inner.get(principal)
    }

    fn get_at(&self, principal: &Principal, locality: Locality) -> Option<MapEntry> {
        self.get_at.fetch_add(1, Ordering::Relaxed);
        self.inner.get_at(principal, locality)
    }

    fn local_sharding(&self) -> LocalSharding {
        self.inner.local_sharding()
    }

    fn counters(&self) -> &Arc<tollgate_admission::AdmissionCounters> {
        self.inner.counters()
    }

    fn install(
        &self,
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
        lease: Arc<LeaseSlot>,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.install(principal, snapshot, lease)
    }

    fn install_publishable(
        &self,
        principal: Principal,
        snapshot: tollgate_core::PublishableSnapshot,
        lease: Arc<LeaseSlot>,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.install_publishable(principal, snapshot, lease)
    }

    fn install_revoked(
        &self,
        principal: Principal,
        until: Timestamp,
        generation: Generation,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.install_revoked(principal, until, generation)
    }

    fn install_unknown(
        &self,
        principal: Principal,
        until: Timestamp,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.install_unknown(principal, until)
    }

    fn remove(&self, principal: &Principal) {
        self.inner.remove(principal);
    }

    fn install_many(
        &self,
        entries: Vec<(Principal, Arc<AccountSnapshot>, Arc<LeaseSlot>)>,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.install_many(entries)
    }

    fn apply_many(
        &self,
        updates: Vec<SnapshotUpdate>,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.apply_many(updates)
    }

    fn apply_many_at(
        &self,
        updates: Vec<SnapshotUpdate>,
        now: Timestamp,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.apply_many_at(updates, now)
    }

    fn apply_publishable_many(
        &self,
        updates: Vec<PublishableSnapshotUpdate>,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.apply_publishable_many(updates)
    }

    fn apply_publishable_many_at(
        &self,
        updates: Vec<PublishableSnapshotUpdate>,
        now: Timestamp,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.apply_publishable_many_at(updates, now)
    }
    fn generation_capacity(&self) -> std::num::NonZeroUsize {
        self.inner.generation_capacity()
    }
    fn needs_refresh(&self, principal: Principal) -> bool {
        self.inner.needs_refresh(principal)
    }
    fn prepare_refreshes(
        &self,
        principals: &[Principal],
    ) -> Result<tollgate_admission::RefreshBatch, tollgate_admission::PublicationError> {
        self.inner.prepare_refreshes(principals)
    }
    fn apply_refreshed_many_at(
        &self,
        updates: Vec<tollgate_admission::Refreshed<PublishableSnapshotUpdate>>,
        now: Timestamp,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.apply_refreshed_many_at(updates, now)
    }
}

fn assert_lookup(
    engine: &AdmissionEngine<CountingMap<ArcSwapSnapshotMap>>,
    operation: impl FnOnce(),
) {
    engine.map().reset();
    operation();
    engine.map().assert_one_lookup();
}

#[test]
fn admit_consults_the_map_exactly_once() {
    let map = CountingMap::new(ArcSwapSnapshotMap::new());
    install(
        &map,
        Principal(1),
        AccountId(1),
        EnforcementMode::Strict,
        CostUnits(u64::MAX / 2),
    );
    install(
        &map,
        Principal(2),
        AccountId(2),
        EnforcementMode::Strict,
        CostUnits::ZERO,
    );
    install(
        &map,
        Principal(3),
        AccountId(3),
        EnforcementMode::Elastic {
            overage_cap: CostUnits(10_000),
        },
        CostUnits::ZERO,
    );
    let engine = AdmissionEngine::new(map);

    assert_lookup(&engine, || {
        let pending = engine
            .begin(Principal(1), PermissionBits::bit(0), now())
            .unwrap()
            .admit(&[(PriceOp, 64)], AllocationSlot, now())
            .unwrap();
        black_box(pending.cancel());
    });
    assert_lookup(&engine, || {
        black_box(
            engine
                .begin(Principal(999), PermissionBits::bit(0), now())
                .unwrap_err(),
        );
    });
    assert_lookup(&engine, || {
        black_box(
            engine
                .begin(Principal(1), PermissionBits::bit(1), now())
                .unwrap_err(),
        );
    });
    assert_lookup(&engine, || {
        black_box(
            engine
                .begin(Principal(2), PermissionBits::bit(0), now())
                .unwrap()
                .admit(&[(PriceOp, 64)], AllocationSlot, now())
                .unwrap_err(),
        );
    });
    assert_lookup(&engine, || {
        let pending = engine
            .begin(Principal(3), PermissionBits::bit(0), now())
            .unwrap()
            .admit(&[(PriceOp, 64)], AllocationSlot, now())
            .unwrap();
        black_box(pending.cancel());
    });
    assert_lookup(&engine, || {
        black_box(
            engine
                .begin(Principal(1), PermissionBits::bit(0), now())
                .unwrap(),
        );
    });
    assert_lookup(&engine, || {
        black_box(
            engine
                .begin(Principal(1), PermissionBits::bit(0), now())
                .unwrap()
                .admit(&[] as &[(PriceOp, u64)], AllocationSlot, now())
                .unwrap_err(),
        );
    });
}

#[test]
fn confirmed_balance_exhaustion_allocates_nothing() {
    let engine = engine(
        LocalSharding::SINGLE,
        Principal(1),
        EnforcementMode::Strict,
        CostUnits::ZERO,
    );
    let MapEntry::Present(state) = engine.map().get(&Principal(1)).unwrap() else {
        panic!()
    };
    state
        .lease
        .funding_attempt()
        .exhausted(tollgate_core::BalanceExhaustion { period_end: None });
    let refuse = || {
        assert_eq!(
            engine
                .begin(Principal(1), PermissionBits::bit(0), now())
                .unwrap()
                .admit(&[(PriceOp, 1)], AllocationSlot, now())
                .unwrap_err(),
            tollgate_core::DenyReason::BalanceExhausted
        );
    };
    refuse();
    assert_zero("admission/balance_exhausted", refuse);
}
