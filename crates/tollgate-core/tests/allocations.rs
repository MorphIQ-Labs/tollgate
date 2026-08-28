use std::hint::black_box;
use std::sync::Arc;

use jiff::Timestamp;
use tollgate_alloc_count::AllocScope;
use tollgate_core::{
    AccountId, AccountOverage, AccountSnapshot, AccountStatus, CostTable, CostUnits,
    EnforcementMode, FencingToken, Generation, KeyId, LeaseGrant, LeaseId, LocalLease, Locality,
    OpIndex, PermissionBits, RequestId, Reservation, ResolvedLimits,
};

tollgate_alloc_count::install!();

#[derive(Clone, Copy)]
enum Op {
    Price,
    Greeks,
}

impl OpIndex for Op {
    fn index(&self) -> usize {
        *self as usize
    }
}

fn timestamp() -> Timestamp {
    Timestamp::from_second(1_755_600_000).unwrap()
}

fn far_future() -> Timestamp {
    Timestamp::from_second(4_102_444_800).unwrap()
}

fn cost_table() -> Arc<CostTable> {
    Arc::new(
        CostTable::builder(CostUnits(50), CostUnits(50))
            .weight(&Op::Price, CostUnits(1))
            .weight(&Op::Greeks, CostUnits(5))
            .build(),
    )
}

fn snapshot(table: Arc<CostTable>) -> AccountSnapshot {
    AccountSnapshot {
        account_id: AccountId(1),
        key_id: Some(KeyId(2)),
        generation: Generation(1),
        status: AccountStatus::Active,
        enforcement_mode: EnforcementMode::Strict,
        valid_until: far_future(),
        permissions: PermissionBits::bit(0),
        limits: ResolvedLimits {
            max_items_per_request: 1_024,
            rate_units_per_second: 100_000,
            rate_burst_units: 500_000,
        },
        cost_table: table,
    }
}

fn lease() -> Arc<LocalLease> {
    Arc::new(LocalLease::new(
        LeaseGrant {
            lease_id: LeaseId(7),
            account_id: AccountId(1),
            fencing_token: FencingToken(3),
            units: CostUnits(u64::MAX / 2),
            expires_at: far_future(),
        },
        CostUnits::ZERO,
    ))
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
fn core_hot_path_allocates_nothing() {
    let table = cost_table();
    let snapshot = snapshot(Arc::clone(&table));
    let lease = lease();
    let overage = Arc::new(AccountOverage::new(AccountId(1)));
    let now = timestamp();

    // Resolve the locality TLS outside every measured scope.
    black_box(Locality::current());

    assert_zero("core/quote", || {
        black_box(table.quote(&Op::Greeks, black_box(64)).unwrap());
    });
    assert_zero("core/snapshot_admit", || {
        snapshot
            .admit(black_box(now), PermissionBits::bit(0))
            .unwrap();
    });
    assert_zero("core/reserve_commit_usage", || {
        let reservation = Reservation::reserve(&lease, CostUnits(100), now).unwrap();
        black_box(reservation.commit_at_execution_start(now).unwrap());
        black_box(
            reservation
                .usage_event(RequestId(9), now)
                .expect("committed reservation has usage"),
        );
    });
    assert_zero("core/reserve_cancel", || {
        let reservation = Reservation::reserve(&lease, CostUnits(100), now).unwrap();
        black_box(reservation.cancel());
    });
    assert_zero("core/reserve_overage_cancel", || {
        let reservation =
            Reservation::reserve_overage(&overage, CostUnits(100), CostUnits(1_000)).unwrap();
        black_box(reservation.cancel());
    });
}
