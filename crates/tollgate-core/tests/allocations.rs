use std::hint::black_box;
use std::sync::Arc;

use jiff::Timestamp;
use tollgate_alloc_count::AllocScope;
use tollgate_core::{
    AccountId, AccountOverage, AccountSnapshot, AccountStatus, CommitFunding, CostTable, CostUnits,
    FencingToken, Generation, KeyId, LeaseGrant, LeaseId, LocalLease, Locality, OpIndex,
    PermissionBits, RequestId, Reservation, ResolvedLimits,
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
    AccountSnapshot::builder(
        AccountId(1),
        Generation(1),
        AccountStatus::Active,
        far_future(),
        PermissionBits::bit(0),
        ResolvedLimits::new(1_024).with_weighted_rate(100_000, 500_000),
        table,
    )
    .key_id(KeyId(2))
    .build()
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

/// Record a scope that is *allowed* to allocate, and pin the exact count.
///
/// `split` is the one allocation #93 adds, and it is opt-in: the unsplit
/// scopes above stay at zero and are the comparison. Recording it under its own
/// attribution keeps it out of the `tollgate` allocation-free rule while the
/// gate still holds it to exactly one `Arc` — an exemption that could not fail
/// would witness nothing.
fn assert_exactly_one_allocation(scope: &str, operation: impl FnOnce()) {
    let ((), allocations) = AllocScope::measure(operation);
    tollgate_alloc_count::record_if_requested!(scope, "tollgate_opt_in", allocations).unwrap();
    assert_eq!(
        (
            allocations.alloc_calls,
            allocations.alloc_zeroed_calls,
            allocations.realloc_calls
        ),
        (1, 0, 0),
        "{scope} must cost exactly one allocation: {allocations:?}"
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

#[test]
fn core_hot_path_allocates_nothing() {
    let table = cost_table();
    let snapshot = snapshot(Arc::clone(&table));
    let lease = lease();
    // A lease usable at `timestamp()` but not one second later, so the
    // reservation opens normally and the commit takes the fallback branch.
    let lapsed_at = Timestamp::from_second(timestamp().as_second() + 1).unwrap();
    let lapsed_lease = Arc::new(LocalLease::new(
        LeaseGrant {
            lease_id: LeaseId(8),
            account_id: AccountId(1),
            fencing_token: FencingToken(4),
            units: CostUnits(u64::MAX / 2),
            expires_at: lapsed_at,
        },
        CostUnits::ZERO,
    ));
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
        black_box(
            reservation
                .commit_at_execution_start(now, CommitFunding::LeaseOnly)
                .unwrap(),
        );
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
    // The commit-time elastic fallback changes a reservation's funding source
    // on the request path, so it must stay allocation-free like every other
    // resolution (INVARIANTS.md #24). A lease that lapsed at execution start.
    assert_zero("core/reserve_commit_overage_fallback", || {
        let reservation = Reservation::reserve(&lapsed_lease, CostUnits(100), now).unwrap();
        black_box(
            reservation
                .commit_at_execution_start(
                    lapsed_at,
                    CommitFunding::OverageFallback {
                        overage: &overage,
                        cap: CostUnits(u64::MAX / 2),
                    },
                )
                .unwrap(),
        );
        black_box(
            reservation
                .usage_event(RequestId(9), lapsed_at)
                .expect("a committed fallback has usage"),
        );
    });
    // The shared cancel state: the single allocation #93 permits, measured
    // separately from the allocation-free path above so a consumer that does
    // not need a timeout/worker race is never charged for one.
    assert_exactly_one_allocation("reservation/commit_split", || {
        let reservation = Reservation::reserve(&lease, CostUnits(100), now).unwrap();
        let (shared, handle) = reservation.split();
        black_box(
            shared
                .reservation()
                .commit_at_execution_start(now, CommitFunding::LeaseOnly)
                .unwrap(),
        );
        black_box(handle.is_cancelled());
    });
    // And cancelling through the handle stays on the same one allocation.
    assert_exactly_one_allocation("reservation/split_cancel", || {
        let reservation = Reservation::reserve(&lease, CostUnits(100), now).unwrap();
        let (_shared, handle) = reservation.split();
        black_box(handle.cancel());
    });
    assert_zero("core/reserve_overage_cancel", || {
        let reservation =
            Reservation::reserve_overage(&overage, CostUnits(100), CostUnits(1_000)).unwrap();
        black_box(reservation.cancel());
    });
}
