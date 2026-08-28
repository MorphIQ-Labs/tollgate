use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jiff::Timestamp;
use tollgate_admission::{
    AdmissionEngine, AdmissionRequest, ArcSwapSnapshotMap, LeaseSlot, MapEntry, MokaSnapshotMap,
    Principal, PublishableSnapshotUpdate, SnapshotMap, SnapshotUpdate,
};
use tollgate_alloc_count::AllocScope;
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, EnforcementMode, FencingToken,
    Generation, LeaseGrant, LeaseId, LocalLease, LocalSharding, Locality, OpIndex, PermissionBits,
    ResolvedLimits,
};

tollgate_alloc_count::install!();

#[derive(Clone, Copy)]
struct PriceOp;

impl OpIndex for PriceOp {
    fn index(&self) -> usize {
        0
    }
}

fn now() -> Timestamp {
    Timestamp::from_second(1_755_600_000).unwrap()
}

fn far_future() -> Timestamp {
    Timestamp::from_second(4_102_444_800).unwrap()
}

fn snapshot(account: AccountId, mode: EnforcementMode) -> Arc<AccountSnapshot> {
    Arc::new(AccountSnapshot {
        account_id: account,
        key_id: None,
        generation: Generation(1),
        status: AccountStatus::Active,
        enforcement_mode: mode,
        valid_until: far_future(),
        permissions: PermissionBits::bit(0),
        limits: ResolvedLimits {
            max_items_per_request: 4_096,
            rate_units_per_second: u64::from(u32::MAX),
            rate_burst_units: u64::from(u32::MAX),
        },
        cost_table: Arc::new(
            CostTable::builder(CostUnits(50), CostUnits(50))
                .weight(&PriceOp, CostUnits(1))
                .build(),
        ),
    })
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
    slot.install(lease(account, units, sharding));
    map.install(principal, snapshot(account, mode), slot);
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

fn request(principal: Principal, required: PermissionBits) -> AdmissionRequest<'static, PriceOp> {
    AdmissionRequest {
        principal,
        required,
        op: &PriceOp,
        items: 64,
    }
}

fn admit_and_cancel(engine: &AdmissionEngine<ArcSwapSnapshotMap>, principal: Principal) {
    let admitted = engine
        .admit(request(principal, PermissionBits::bit(0)), now())
        .unwrap();
    black_box(admitted.reservation.cancel());
}

fn assert_zero(scope: &str, operation: impl FnOnce()) {
    let ((), allocations) = AllocScope::measure(operation);
    tollgate_alloc_count::record_if_requested!(scope, "tollgate", allocations).unwrap();
    assert!(
        allocations.is_allocation_free(),
        "{scope} allocated: {allocations:?}"
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
            .admit(request(Principal(999), PermissionBits::bit(0)), now())
            .unwrap_err(),
    );
    black_box(
        strict_exhausted
            .admit(request(Principal(1), PermissionBits::bit(0)), now())
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
                .admit(request(Principal(999), PermissionBits::bit(0)), now())
                .unwrap_err(),
        );
    });
    assert_zero("admission/arc_swap_lease_exhausted_strict", || {
        black_box(
            strict_exhausted
                .admit(request(Principal(1), PermissionBits::bit(0)), now())
                .unwrap_err(),
        );
    });
    assert_zero("admission/arc_swap_lease_exhausted_elastic", || {
        admit_and_cancel(&elastic_exhausted, Principal(1));
    });
}

#[test]
fn moka_reads_are_allocation_free_after_current_thread_warmup() {
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
    // through one maintenance interval before asserting steady-state reads.
    for _ in 0..65 {
        black_box(map.get_at(&principal, locality).expect("installed"));
    }

    assert_zero("admission/moka_reads_1024", || {
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

    fn install(&self, principal: Principal, snapshot: Arc<AccountSnapshot>, lease: Arc<LeaseSlot>) {
        self.inner.install(principal, snapshot, lease);
    }

    fn install_publishable(
        &self,
        principal: Principal,
        snapshot: tollgate_core::PublishableSnapshot,
        lease: Arc<LeaseSlot>,
    ) {
        self.inner.install_publishable(principal, snapshot, lease);
    }

    fn install_revoked(&self, principal: Principal, until: Timestamp, generation: Generation) {
        self.inner.install_revoked(principal, until, generation);
    }

    fn install_unknown(&self, principal: Principal, until: Timestamp) {
        self.inner.install_unknown(principal, until);
    }

    fn remove(&self, principal: &Principal) {
        self.inner.remove(principal);
    }

    fn install_many(&self, entries: Vec<(Principal, Arc<AccountSnapshot>, Arc<LeaseSlot>)>) {
        self.inner.install_many(entries);
    }

    fn apply_many(&self, updates: Vec<SnapshotUpdate>) {
        self.inner.apply_many(updates);
    }

    fn apply_many_at(&self, updates: Vec<SnapshotUpdate>, now: Timestamp) {
        self.inner.apply_many_at(updates, now);
    }

    fn apply_publishable_many(&self, updates: Vec<PublishableSnapshotUpdate>) {
        self.inner.apply_publishable_many(updates);
    }

    fn apply_publishable_many_at(&self, updates: Vec<PublishableSnapshotUpdate>, now: Timestamp) {
        self.inner.apply_publishable_many_at(updates, now);
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
        let admitted = engine
            .admit(request(Principal(1), PermissionBits::bit(0)), now())
            .unwrap();
        black_box(admitted.reservation.cancel());
    });
    assert_lookup(&engine, || {
        black_box(
            engine
                .admit(request(Principal(999), PermissionBits::bit(0)), now())
                .unwrap_err(),
        );
    });
    assert_lookup(&engine, || {
        black_box(
            engine
                .admit(request(Principal(1), PermissionBits::bit(1)), now())
                .unwrap_err(),
        );
    });
    assert_lookup(&engine, || {
        black_box(
            engine
                .admit(request(Principal(2), PermissionBits::bit(0)), now())
                .unwrap_err(),
        );
    });
    assert_lookup(&engine, || {
        let admitted = engine
            .admit(request(Principal(3), PermissionBits::bit(0)), now())
            .unwrap();
        black_box(admitted.reservation.cancel());
    });
}
