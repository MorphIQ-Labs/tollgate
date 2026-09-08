//! INVARIANTS.md #1, end to end: two full instance stacks (admission engine +
//! lease manager + usage writer) spend one account down to zero through the
//! memory store. Total committed usage must equal the recorded billing ledger
//! exactly and never exceed the deposit; conservation must hold at the store.

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};

use tollgate_admission::NoGate;
use tollgate_client::{
    AccountLeaseConfig, InstanceRuntime, InstanceRuntimeConfig, ManualClock, RuntimeHandle,
    SnapshotManagerConfig, TrackedPrincipals, UsageWriterConfig,
};

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, DenyReason,
    Generation, OpIndex, PermissionBits, Principal, RequestId, ResolvedLimits,
};
use tollgate_store::{AccountConfig, GrantPolicy, MemoryStore};

const ACCOUNT: AccountId = AccountId(1);
const PRINCIPAL: Principal = Principal(7);
const DEPOSIT: u64 = 10_000;
/// fixed 50 + weight 1 × 1 item.
const COST_PER_REQUEST: u64 = 51;

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

struct Instance {
    engine: RuntimeHandle,
    runtime: InstanceRuntime,
    committed: u64,
}

fn spawn_instance(store: &Arc<MemoryStore>, clock: &Arc<ManualClock>) -> Instance {
    let (runtime, engine) = InstanceRuntime::spawn(
        store.clone(),
        store.clone(),
        store.clone(),
        clock.clone(),
        InstanceRuntimeConfig {
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
                target_grant: CostUnits(2_000),
                low_water: CostUnits(500),
                lease_ttl: SignedDuration::from_secs(3_600),
                expiry_safety_margin: SignedDuration::ZERO,
                poll_interval: std::time::Duration::from_millis(5),
                store_call_timeout: std::time::Duration::from_secs(5),
                shutdown_release_deadline: std::time::Duration::from_secs(10),
            },
            usage: UsageWriterConfig {
                queue_capacity: 64,
                max_batch: 16,
                flush_interval: std::time::Duration::from_millis(5),
                retry_backoff: std::time::Duration::from_millis(5),
                shutdown_drain_deadline: std::time::Duration::from_secs(60),
                ingest_timeout: std::time::Duration::from_secs(5),
            },
            sharding: tollgate_core::LocalSharding::SINGLE,
            idle_account_linger: std::time::Duration::from_millis(30),
            manager_restart_backoff: std::time::Duration::from_millis(10),
            shutdown_deadline: std::time::Duration::from_secs(70),
        },
    )
    .unwrap();
    Instance {
        engine,
        runtime,
        committed: 0,
    }
}

/// Attempt up to `burst` requests on one instance; returns how many committed.
/// Request ids come from each instance's own random generator — no shared
/// sequence (review finding #6): global idempotency must hold across
/// independently generated ids, and the duplicate count proves it.
fn hammer(instance: &mut Instance, burst: usize) -> usize {
    let mut committed = 0;
    for _ in 0..burst {
        // INVARIANTS.md #8 ordering: accounting capacity is reserved before
        // admission.
        let Ok(permit) = instance.engine.recorder().try_reserve() else {
            continue;
        };
        let admitted = instance
            .engine
            .begin(PRINCIPAL, PermissionBits::bit(0), t(0))
            .and_then(|context| context.admit(&[(PriceOp, 1)], permit, t(0)));
        match admitted {
            Ok(pending) => {
                let committed_guard = pending
                    .acquire_capacity(&NoGate)
                    .unwrap()
                    .commit(RequestId(uuid::Uuid::new_v4().as_u128()), t(0))
                    .unwrap();
                instance.committed += committed_guard.units().get();
                committed += 1;
                drop(committed_guard);
            }
            Err(
                DenyReason::LeaseUnavailable
                | DenyReason::LeaseExhausted { .. }
                | DenyReason::LeaseExpired,
            ) => {
                // Quota denied: permit drops, slot is freed, zero charged.
            }
            Err(other) => panic!("unexpected deny: {other}"),
        }
    }
    committed
}

#[tokio::test(start_paused = true)]
async fn two_instances_never_overspend_one_account() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(DEPOSIT),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    store.publish_snapshot(
        PRINCIPAL,
        tollgate_core::PublishableSnapshot::try_new(snapshot()).unwrap(),
    );
    let clock = Arc::new(ManualClock::new(t(0)));

    let mut instances = [
        spawn_instance(&store, &clock),
        spawn_instance(&store, &clock),
    ];
    for instance in &instances {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !instance.engine.readiness(t(0)).is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    // Rounds of interleaved spending; between rounds the paused runtime
    // auto-advances so managers refill and writers flush.
    let mut quiet_rounds = 0;
    for _ in 0..500 {
        let mut round_commits = 0;
        for instance in &mut instances {
            round_commits += hammer(instance, 10);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        if round_commits == 0 {
            quiet_rounds += 1;
            // Several quiet rounds prove denial is stable — the tail churn of
            // parked-lease releases and tiny re-grants produces no commits.
            if quiet_rounds >= 5 {
                break;
            }
        } else {
            quiet_rounds = 0;
        }
    }
    assert!(quiet_rounds >= 5, "account never drained to stable denial");

    let total_committed: u64 = instances.iter().map(|i| i.committed).sum();
    assert!(total_committed <= DEPOSIT, "overspend: {total_committed}");
    // 51 does not divide 10_000, and the tail fragments across two current
    // leases plus a sub-grantable balance. Everything beyond that must have
    // been spent.
    assert!(
        total_committed >= DEPOSIT - 5 * COST_PER_REQUEST,
        "underspend: {total_committed}"
    );

    // Orderly shutdown: flush usage first, then release leases.
    let [a, b] = instances;
    let (a, b) = tokio::join!(a.runtime.shutdown(), b.runtime.shutdown());
    let stats_a = a.unwrap().usage.unwrap();
    let stats_b = b.unwrap().usage.unwrap();
    assert_eq!(stats_a.lost + stats_b.lost, 0);
    assert_eq!(stats_a.rejected + stats_b.rejected, 0);
    // Independent generators must never collide into duplicates.
    assert_eq!(stats_a.duplicate + stats_b.duplicate, 0);

    // The billing ledger equals committed admission exactly (leases bound
    // spend; usage events are the record; drift is zero).
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(total_committed));
    // Everything unspent is back in the balance; nothing was lost at
    // settlement.
    assert_eq!(store.balance(ACCOUNT), CostUnits(DEPOSIT - total_committed));
    let conservation = store.conservation(ACCOUNT).unwrap();
    assert!(conservation.holds(), "conservation: {conservation:?}");
    assert_eq!(conservation.settlement_loss, CostUnits::ZERO);
}
