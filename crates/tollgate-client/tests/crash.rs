//! #136, end to end: an instance killed without a graceful shutdown loses its
//! committed-but-unflushed usage queue. Its leases are then settled by expiry
//! reclaim, and the work it executed must not become spendable again.
//!
//! A kill is modelled by running the instance on its own Tokio runtime and
//! dropping that runtime: every task is cancelled where it stands, no async
//! cleanup runs, nothing is released and nothing is flushed. The usage sink
//! is held in an outage throughout, so every committed event is still queued
//! when the instance dies.

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};

use tollgate_admission::NoGate;
use tollgate_client::{
    AccountLeaseConfig, InstanceRuntime, InstanceRuntimeConfig, ManualClock, RuntimeHandle,
    SnapshotManagerConfig, TrackedPrincipals, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, Generation,
    OpIndex, PermissionBits, Principal, RequestId, ResolvedLimits,
};
use tollgate_store::{
    AccountConfig, GrantPolicy, IngestError, LeaseAllocator, MemoryStore, StoreError,
};

#[path = "../../tollgate-store/tests/support/delegating.rs"]
mod delegating;
use delegating::DelegatingStore;

const ACCOUNT: AccountId = AccountId(1);
const PRINCIPAL: Principal = Principal(7);
const DEPOSIT: u64 = 10_000;
const GRANT: u64 = 2_000;
/// Fixed 50 + weight 1 × 1 item.
const COST_PER_REQUEST: u64 = 51;
const LEASE_TTL_SECS: i64 = 60;

#[derive(Clone, Copy)]
struct PriceOp;
impl OpIndex for PriceOp {
    fn index(&self) -> usize {
        0
    }
}

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

fn snapshot() -> Arc<AccountSnapshot> {
    Arc::new(
        AccountSnapshot::builder(
            ACCOUNT,
            Generation(1),
            AccountStatus::Active,
            t(1_000_000),
            PermissionBits::bit(0),
            ResolvedLimits::new(64).with_weighted_rate(u64::from(u32::MAX), u64::from(u32::MAX)),
            Arc::new(
                CostTable::builder(CostUnits(50), CostUnits(50))
                    .weight(&PriceOp, CostUnits(1))
                    .build(),
            ),
        )
        .build(),
    )
}

fn store() -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        ..GrantPolicy::default()
    })
    .unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(DEPOSIT),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    store
        .publish_snapshot(
            PRINCIPAL,
            tollgate_core::PublishableSnapshot::try_new(snapshot()).unwrap(),
        )
        .expect("snapshot fixture matches its account and credential");
    store
}

fn config(queue_capacity: usize) -> InstanceRuntimeConfig {
    InstanceRuntimeConfig {
        snapshot_history_capacity:
            tollgate_admission::ArcSwapSnapshotMap::DEFAULT_GENERATION_CAPACITY,
        snapshots: SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_secs(30),
            unknown_ttl: SignedDuration::from_secs(1),
            revoked_ttl: SignedDuration::from_secs(1),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 4,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(5),
        },
        leases: AccountLeaseConfig {
            target_grant: CostUnits(GRANT),
            // Low enough that the burst below never triggers a second grant.
            low_water: CostUnits(1),
            lease_ttl: SignedDuration::from_secs(LEASE_TTL_SECS),
            expiry_safety_margin: SignedDuration::ZERO,
            poll_interval: std::time::Duration::from_millis(5),
            store_call_timeout: std::time::Duration::from_secs(5),
            shutdown_release_deadline: std::time::Duration::from_secs(10),
        },
        usage: UsageWriterConfig {
            queue_capacity,
            max_batch: 16,
            flush_interval: std::time::Duration::from_millis(5),
            retry_backoff: std::time::Duration::from_millis(5),
            shutdown_drain_deadline: std::time::Duration::from_secs(60),
            ingest_timeout: std::time::Duration::from_secs(5),
        },
        sharding: tollgate_core::LocalSharding::SINGLE,
        idle_account_linger: std::time::Duration::from_secs(3_600),
        manager_restart_backoff: std::time::Duration::from_millis(10),
        shutdown_deadline: std::time::Duration::from_secs(70),
    }
}

/// Commit up to `attempts` requests, stopping when the writer's queue is full.
fn commit_burst(handle: &RuntimeHandle, attempts: u64) -> u64 {
    let mut committed = 0;
    for request in 0..attempts {
        let Ok(permit) = handle.recorder().try_reserve() else {
            break;
        };
        let pending = handle
            .begin(PRINCIPAL, PermissionBits::bit(0), t(0))
            .and_then(|context| context.admit(&[(PriceOp, 1)], permit, t(0)))
            .expect("the lease funds every request in the burst");
        let guard = pending
            .acquire_capacity(&NoGate)
            .unwrap()
            .commit(RequestId(u128::from(request) + 1), t(0))
            .unwrap();
        committed += guard.units().get();
        // Dropping the guard queues the usage event: the work executed.
        drop(guard);
    }
    committed
}

/// Run an instance against `store` with the usage sink in an outage, commit
/// `attempts` requests, then kill it. Returns the units it executed.
fn run_and_kill(store: &Arc<MemoryStore>, queue_capacity: usize, attempts: u64) -> u64 {
    // The control plane is unreachable for the instance's whole life.
    let sink = Arc::new(
        DelegatingStore::wrapping(Arc::clone(store)).on_ingest(|_, _, _| async {
            Err(IngestError::Unavailable(StoreError(
                "control plane unreachable".into(),
            )))
        }),
    );
    let instance = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let committed = instance.block_on(async {
        let (runtime, handle) = InstanceRuntime::spawn(
            store.clone(),
            store.clone(),
            sink,
            Arc::new(ManualClock::new(t(0))),
            config(queue_capacity),
        )
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !handle.readiness(t(0)).is_ready() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the instance becomes ready");
        let committed = commit_burst(&handle, attempts);
        // Let the writer try, and fail, to flush at least once.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // A kill runs no graceful shutdown: forget the runtime owner so its
        // drop cannot, and let the executor's teardown cancel every task.
        std::mem::forget(runtime);
        std::mem::forget(handle);
        committed
    });
    instance.shutdown_background();
    committed
}

fn reclaim_after_ttl(store: &MemoryStore) -> CostUnits {
    let grace = GrantPolicy::default().reclaim_grace.as_secs();
    let after = t(LEASE_TTL_SECS + grace + 1);
    let reclaimed = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(store.reclaim_expired(after))
        .unwrap();
    reclaimed.iter().fold(CostUnits::ZERO, |total, lease| {
        total.checked_add(lease.forfeited).unwrap()
    })
}

fn assert_nothing_returns(store: &MemoryStore, committed: u64) {
    assert!(committed > 0, "the killed instance executed work");
    let before = store.conservation(ACCOUNT).unwrap();
    assert_eq!(
        before.settled_usage,
        CostUnits::ZERO,
        "the outage kept every event unflushed"
    );
    assert_eq!(before.balance, CostUnits(DEPOSIT - GRANT));
    assert_eq!(before.active_lease_grants, CostUnits(GRANT));

    let forfeited = reclaim_after_ttl(store);
    let after = store.conservation(ACCOUNT).unwrap();
    assert!(after.holds(), "{after:?}");
    assert_eq!(
        after.balance, before.balance,
        "the {committed} executed units must not become spendable again"
    );
    assert_eq!(forfeited, CostUnits(GRANT), "the whole unaccounted grant");
    assert_eq!(after.settlement_loss, CostUnits(GRANT));
    assert_eq!(after.active_lease_grants, CostUnits::ZERO);
    // Before #136 this was `DEPOSIT - GRANT + GRANT`: the whole deposit
    // spendable again, although `committed` units of it had been executed.
    assert!(
        after.balance.get() + committed <= DEPOSIT,
        "spendable {} plus executed {committed} exceeds the deposit {DEPOSIT}",
        after.balance
    );
}

/// The ordinary case: a flush interval's worth of work, lost with the process.
#[test]
fn a_killed_holders_unflushed_usage_is_not_credited_back() {
    let store = store();
    let committed = run_and_kill(&store, 64, 5);
    assert_eq!(committed, 5 * COST_PER_REQUEST);
    assert_nothing_returns(&store, committed);
}

/// The worst case the issue names: a control-plane outage fills the queue to
/// capacity, and the host dies with all of it. Everything held is forfeited,
/// however large the backlog.
#[test]
fn a_kill_during_an_outage_with_a_full_backlog_forfeits_everything_held() {
    const CAPACITY: usize = 16;
    let store = store();
    let committed = run_and_kill(&store, CAPACITY, 1_000);
    // The queue holds `CAPACITY` events, and the writer's in-flight batch,
    // taken off the queue and retried in place, holds the rest.
    assert!(
        committed >= u64::try_from(CAPACITY).unwrap() * COST_PER_REQUEST,
        "at least a full queue was committed: {committed}"
    );
    assert!(
        committed < 1_000 * COST_PER_REQUEST,
        "the burst stopped because the backlog was full: {committed}"
    );
    assert_nothing_returns(&store, committed);
}
