//! SnapshotManager behavior (review finding #5): initial load gates
//! readiness, pushes propagate, refresh recovers, and revocation reaches
//! running instances.

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{AdmissionEngine, AdmissionRequest, ArcSwapSnapshotMap, SnapshotMap};
use tollgate_client::{ManualClock, SlotRegistry, SnapshotManager, SnapshotManagerConfig};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, DenyReason, FencingToken,
    Generation, LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, Principal,
    ResolvedLimits,
};
use tollgate_store::{AccountConfig, GrantPolicy, MemoryStore};

const ACCOUNT: AccountId = AccountId(1);
const PRINCIPAL: Principal = Principal(7);

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

fn snapshot(generation: u64, permissions: PermissionBits) -> Arc<AccountSnapshot> {
    Arc::new(AccountSnapshot {
        account_id: ACCOUNT,
        key_id: None,
        generation: Generation(generation),
        status: AccountStatus::Active,
        valid_until: t(100_000),
        permissions,
        limits: ResolvedLimits {
            max_items_per_request: 64,
            rate_units_per_second: 1_000_000,
            rate_burst_units: 1_000_000,
        },
        cost_table: Arc::new(
            CostTable::builder(CostUnits(50), CostUnits(50))
                .weight(&PriceOp, CostUnits(1))
                .build(),
        ),
    })
}

struct Fixture {
    store: Arc<MemoryStore>,
    engine: AdmissionEngine<Arc<ArcSwapSnapshotMap>>,
    slots: Arc<SlotRegistry>,
    manager: SnapshotManager,
}

fn fixture(store: Arc<MemoryStore>) -> Fixture {
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let engine = AdmissionEngine::new(Arc::clone(&map));
    let slots = SlotRegistry::new();
    let manager = SnapshotManager::spawn(
        store.clone(),
        map,
        Arc::clone(&slots),
        Arc::new(ManualClock::new(t(0))),
        SnapshotManagerConfig {
            principals: vec![PRINCIPAL],
            refresh_interval: std::time::Duration::from_millis(20),
            negative_ttl: SignedDuration::from_secs(30),
            retry_backoff: std::time::Duration::from_millis(5),
        },
    );
    Fixture {
        store,
        engine,
        slots,
        manager,
    }
}

fn base_store() -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy::default());
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(1_000_000),
        active: true,
    });
    store
}

/// Fund the account's slot directly so admission outcomes isolate snapshot
/// behavior.
fn stock_slot(fixture: &Fixture) {
    fixture
        .slots
        .slot(ACCOUNT)
        .install(Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id: LeaseId(1),
                account_id: ACCOUNT,
                fencing_token: FencingToken(1),
                units: CostUnits(1_000_000),
                expires_at: t(100_000),
            },
            CostUnits::ZERO,
        )));
}

fn admit(fixture: &Fixture) -> Result<(), DenyReason> {
    fixture
        .engine
        .admit(
            AdmissionRequest {
                principal: PRINCIPAL,
                required: PermissionBits::bit(0),
                op: &PriceOp,
                items: 1,
            },
            t(1),
        )
        .map(|admitted| {
            admitted.reservation.cancel();
        })
}

async fn settle() {
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
}

#[tokio::test(start_paused = true)]
async fn initial_load_gates_readiness_and_installs() {
    let store = base_store();
    store.publish_snapshot(PRINCIPAL, snapshot(1, PermissionBits::bit(0)));
    let fixture = fixture(store);
    stock_slot(&fixture);

    let mut ready = fixture.manager.ready();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !*ready.borrow() {
            ready.changed().await.unwrap();
        }
    })
    .await
    .expect("manager must become ready");
    assert_eq!(admit(&fixture), Ok(()));
    fixture.manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn push_update_propagates_permission_change() {
    let store = base_store();
    store.publish_snapshot(PRINCIPAL, snapshot(1, PermissionBits::bit(0)));
    let fixture = fixture(store.clone());
    stock_slot(&fixture);
    settle().await;
    assert_eq!(admit(&fixture), Ok(()));

    // The control plane strips the permission in generation 2.
    store.publish_snapshot(PRINCIPAL, snapshot(2, PermissionBits::NONE));
    settle().await;
    assert_eq!(admit(&fixture), Err(DenyReason::MissingPermission));
    fixture.manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn revocation_reaches_instances_via_refresh() {
    let store = base_store();
    store.publish_snapshot(PRINCIPAL, snapshot(1, PermissionBits::bit(0)));
    let fixture = fixture(store.clone());
    stock_slot(&fixture);
    settle().await;
    assert_eq!(admit(&fixture), Ok(()));

    // Key revoked at the source: within a refresh interval the instance
    // denies and negative-caches.
    store.remove_snapshot(PRINCIPAL);
    settle().await;
    assert_eq!(admit(&fixture), Err(DenyReason::UnknownPrincipal));
    fixture.manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn unknown_principal_resolves_once_published() {
    // Nothing published yet: initial load negative-caches, readiness still
    // arrives (the principal is *resolved* — as unknown).
    let store = base_store();
    let fixture = fixture(store.clone());
    stock_slot(&fixture);
    settle().await;
    assert_eq!(admit(&fixture), Err(DenyReason::UnknownPrincipal));

    // Publication later is picked up by push/refresh.
    store.publish_snapshot(PRINCIPAL, snapshot(1, PermissionBits::bit(0)));
    settle().await;
    assert_eq!(admit(&fixture), Ok(()));
    fixture.manager.shutdown().await;
}
