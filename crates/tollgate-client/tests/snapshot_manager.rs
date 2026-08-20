//! SnapshotManager behavior (review finding #5): initial load gates
//! readiness, pushes propagate, refresh recovers, and revocation reaches
//! running instances.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{AdmissionEngine, AdmissionRequest, ArcSwapSnapshotMap};
use tollgate_client::{
    ManualClock, SlotRegistry, SnapshotManager, SnapshotManagerConfig, SystemClock,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, DenyReason, FencingToken,
    Generation, LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, Principal,
    ResolvedLimits,
};
use tollgate_store::{
    AccountConfig, GrantPolicy, MemoryStore, SnapshotPush, SnapshotSource, StoreError,
};

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
            max_concurrent_fetches: 4,
        },
    )
    .unwrap();
    Fixture {
        engine,
        slots,
        manager,
    }
}

fn base_store() -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
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

    // Generation 1 is older than the retained tombstone and cannot
    // resurrect the key. A genuinely newer generation can.
    store.publish_snapshot(PRINCIPAL, snapshot(1, PermissionBits::bit(0)));
    settle().await;
    assert_eq!(admit(&fixture), Err(DenyReason::UnknownPrincipal));
    store.publish_snapshot(PRINCIPAL, snapshot(2, PermissionBits::bit(0)));
    settle().await;
    assert_eq!(admit(&fixture), Ok(()));
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

struct ToggleSource {
    snapshot: Arc<AccountSnapshot>,
    fail: AtomicBool,
    push: tokio::sync::broadcast::Sender<SnapshotPush>,
}

impl ToggleSource {
    fn new(snapshot: Arc<AccountSnapshot>) -> Arc<Self> {
        let (push, _) = tokio::sync::broadcast::channel(4);
        Arc::new(Self {
            snapshot,
            fail: AtomicBool::new(false),
            push,
        })
    }
}

#[async_trait]
impl SnapshotSource for ToggleSource {
    async fn snapshot(
        &self,
        _principal: Principal,
    ) -> Result<Option<Arc<AccountSnapshot>>, StoreError> {
        if self.fail.load(Ordering::Acquire) {
            Err(StoreError("injected snapshot outage".into()))
        } else {
            Ok(Some(Arc::clone(&self.snapshot)))
        }
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
        self.push.subscribe()
    }
}

#[tokio::test(start_paused = true)]
async fn readiness_falls_when_snapshot_expires_during_outage() {
    let source = ToggleSource::new(snapshot(1, PermissionBits::bit(0)));
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let slots = SlotRegistry::new();
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = SnapshotManager::spawn(
        source.clone(),
        map,
        slots,
        Arc::clone(&clock) as _,
        SnapshotManagerConfig {
            principals: vec![PRINCIPAL],
            refresh_interval: std::time::Duration::from_millis(20),
            negative_ttl: SignedDuration::from_secs(30),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 2,
        },
    )
    .unwrap();
    let ready = manager.ready();
    settle().await;
    assert!(*ready.borrow());

    source.fail.store(true, Ordering::Release);
    clock.set(t(100_000));
    settle().await;
    assert!(!*ready.borrow());
    manager.shutdown().await;
}

struct FirstThenHangsSource {
    snapshot: Arc<AccountSnapshot>,
    calls: AtomicUsize,
    push: tokio::sync::broadcast::Sender<SnapshotPush>,
}

#[async_trait]
impl SnapshotSource for FirstThenHangsSource {
    async fn snapshot(
        &self,
        _principal: Principal,
    ) -> Result<Option<Arc<AccountSnapshot>>, StoreError> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            Ok(Some(Arc::clone(&self.snapshot)))
        } else {
            std::future::pending().await
        }
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
        self.push.subscribe()
    }
}

#[tokio::test]
async fn readiness_falls_if_refresh_hangs_across_snapshot_expiry() {
    let mut expiring = (*snapshot(1, PermissionBits::bit(0))).clone();
    expiring.valid_until = Timestamp::now()
        .checked_add(SignedDuration::from_millis(500))
        .unwrap();
    let (push, _) = tokio::sync::broadcast::channel(4);
    let source = Arc::new(FirstThenHangsSource {
        snapshot: Arc::new(expiring),
        calls: AtomicUsize::new(0),
        push,
    });
    let manager = SnapshotManager::spawn(
        source,
        Arc::new(ArcSwapSnapshotMap::new()),
        SlotRegistry::new(),
        Arc::new(SystemClock),
        SnapshotManagerConfig {
            principals: vec![PRINCIPAL],
            refresh_interval: std::time::Duration::from_millis(5),
            negative_ttl: SignedDuration::from_secs(30),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 1,
        },
    )
    .unwrap();
    let mut ready = manager.ready();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !*ready.borrow() {
            ready.changed().await.unwrap();
        }
    })
    .await
    .expect("initial snapshot must make the manager ready");

    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert!(!*ready.borrow());
    tokio::time::timeout(std::time::Duration::from_millis(100), manager.shutdown())
        .await
        .expect("shutdown must cancel the hung refresh");
}

struct HangingSource {
    push: tokio::sync::broadcast::Sender<SnapshotPush>,
}

#[async_trait]
impl SnapshotSource for HangingSource {
    async fn snapshot(
        &self,
        _principal: Principal,
    ) -> Result<Option<Arc<AccountSnapshot>>, StoreError> {
        std::future::pending().await
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
        self.push.subscribe()
    }
}

#[tokio::test]
async fn shutdown_cancels_in_flight_snapshot_fetches() {
    let (push, _) = tokio::sync::broadcast::channel(4);
    let manager = SnapshotManager::spawn(
        Arc::new(HangingSource { push }),
        Arc::new(ArcSwapSnapshotMap::new()),
        SlotRegistry::new(),
        Arc::new(ManualClock::new(t(0))),
        SnapshotManagerConfig {
            principals: (0..64).map(Principal).collect(),
            refresh_interval: std::time::Duration::from_secs(1),
            negative_ttl: SignedDuration::from_secs(30),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 4,
        },
    )
    .unwrap();
    tokio::task::yield_now().await;
    tokio::time::timeout(std::time::Duration::from_millis(100), manager.shutdown())
        .await
        .expect("shutdown must cancel outstanding source futures");
}

#[test]
fn invalid_snapshot_manager_intervals_are_rejected() {
    let config = SnapshotManagerConfig {
        principals: vec![PRINCIPAL],
        refresh_interval: std::time::Duration::ZERO,
        negative_ttl: SignedDuration::from_secs(30),
        retry_backoff: std::time::Duration::from_millis(5),
        max_concurrent_fetches: 4,
    };
    assert!(config.validate().is_err());
}
