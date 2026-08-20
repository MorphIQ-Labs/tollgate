//! INVARIANTS.md #1, end to end: two full instance stacks (admission engine +
//! lease manager + usage writer) spend one account down to zero through the
//! memory store. Total committed usage must equal the recorded billing ledger
//! exactly and never exceed the deposit; conservation must hold at the store.

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{
    AdmissionEngine, AdmissionRequest, ArcSwapSnapshotMap, LeaseSlot, SnapshotMap,
};
use tollgate_client::{
    LeaseManager, LeaseManagerConfig, ManualClock, UsageRecorder, UsageWriter, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, DenyReason, Generation,
    OpIndex, PermissionBits, Principal, RequestId, ResolvedLimits,
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
    Arc::new(AccountSnapshot {
        account_id: ACCOUNT,
        key_id: None,
        generation: Generation(1),
        status: AccountStatus::Active,
        valid_until: t(1_000_000),
        permissions: PermissionBits::bit(0),
        limits: ResolvedLimits {
            max_items_per_request: 64,
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

struct Instance {
    engine: AdmissionEngine<ArcSwapSnapshotMap>,
    recorder: UsageRecorder,
    manager: LeaseManager,
    writer: UsageWriter,
    committed: u64,
}

fn spawn_instance(store: &Arc<MemoryStore>, clock: &Arc<ManualClock>) -> Instance {
    let slot = LeaseSlot::empty();
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
    engine
        .map()
        .install(PRINCIPAL, snapshot(), Arc::clone(&slot));

    let manager = LeaseManager::spawn(
        store.clone(),
        Arc::clone(&slot),
        Arc::clone(clock) as _,
        LeaseManagerConfig {
            account: ACCOUNT,
            target_grant: CostUnits(2_000),
            low_water: CostUnits(500),
            lease_ttl: SignedDuration::from_secs(3_600),
            expiry_safety_margin: SignedDuration::ZERO,
            poll_interval: std::time::Duration::from_millis(5),
        },
    )
    .unwrap();
    let (recorder, writer) = UsageWriter::spawn(
        store.clone(),
        Arc::clone(clock) as _,
        UsageWriterConfig {
            queue_capacity: 64,
            max_batch: 16,
            flush_interval: std::time::Duration::from_millis(5),
            retry_backoff: std::time::Duration::from_millis(5),
        },
    );
    Instance {
        engine,
        recorder,
        manager,
        writer,
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
        let Ok(permit) = instance.recorder.try_reserve() else {
            continue;
        };
        let admitted = instance.engine.admit(
            AdmissionRequest {
                principal: PRINCIPAL,
                required: PermissionBits::bit(0),
                op: &PriceOp,
                items: 1,
            },
            t(0),
        );
        match admitted {
            Ok(admitted) => {
                admitted
                    .reservation
                    .commit_at_execution_start(t(0))
                    .unwrap();
                let event = admitted
                    .reservation
                    .usage_event(RequestId(uuid::Uuid::new_v4().as_u128()), t(0))
                    .expect("committed reservation yields an event");
                permit.record(event);
                instance.committed += admitted.quote.total.get();
                committed += 1;
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
        active: true,
    });
    let clock = Arc::new(ManualClock::new(t(0)));

    let mut instances = [
        spawn_instance(&store, &clock),
        spawn_instance(&store, &clock),
    ];
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
    let stats_a = a.writer.shutdown().await;
    let stats_b = b.writer.shutdown().await;
    assert_eq!(stats_a.lost + stats_b.lost, 0);
    assert_eq!(stats_a.rejected + stats_b.rejected, 0);
    // Independent generators must never collide into duplicates.
    assert_eq!(stats_a.duplicate + stats_b.duplicate, 0);
    a.manager.shutdown().await;
    b.manager.shutdown().await;

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
