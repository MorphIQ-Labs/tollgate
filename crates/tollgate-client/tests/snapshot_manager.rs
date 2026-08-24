//! SnapshotManager behavior (review finding #5): initial load gates
//! readiness, pushes propagate, refresh recovers, and revocation reaches
//! running instances.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{
    AdmissionEngine, AdmissionRequest, ArcSwapSnapshotMap, MapEntry, SnapshotMap,
};
use tollgate_client::{
    ManualClock, SlotRegistry, SnapshotManager, SnapshotManagerConfig, SystemClock,
    TrackedPrincipals,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, DenyReason, FencingToken,
    Generation, LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, Principal,
    PublishableSnapshot, ResolvedLimits,
};
use tollgate_store::{
    AccountConfig, GrantPolicy, MemoryStore, SnapshotPush, SnapshotResolution, SnapshotSource,
    StoreError,
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

fn publishable(snapshot: Arc<AccountSnapshot>) -> PublishableSnapshot {
    PublishableSnapshot::try_new(snapshot).expect("test snapshot limits are valid")
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
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
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
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))));
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
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))));
    let fixture = fixture(store.clone());
    stock_slot(&fixture);
    settle().await;
    assert_eq!(admit(&fixture), Ok(()));

    // The control plane strips the permission in generation 2.
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(2, PermissionBits::NONE)));
    settle().await;
    assert_eq!(admit(&fixture), Err(DenyReason::MissingPermission));
    fixture.manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn revocation_reaches_instances_via_refresh() {
    let store = base_store();
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))));
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
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))));
    settle().await;
    assert_eq!(admit(&fixture), Err(DenyReason::UnknownPrincipal));
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(2, PermissionBits::bit(0))));
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
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))));
    settle().await;
    assert_eq!(admit(&fixture), Ok(()));
    fixture.manager.shutdown().await;
}

#[derive(Clone)]
enum MutableMode {
    Unknown,
    Present(Arc<AccountSnapshot>),
    Failing,
}

struct MutableNoPushSource {
    mode: Mutex<MutableMode>,
    calls: AtomicUsize,
}

impl MutableNoPushSource {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            mode: Mutex::new(MutableMode::Unknown),
            calls: AtomicUsize::new(0),
        })
    }

    fn set(&self, mode: MutableMode) {
        *self.mode.lock().expect("source mode poisoned") = mode;
    }
}

#[async_trait]
impl SnapshotSource for MutableNoPushSource {
    async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        match self.mode.lock().expect("source mode poisoned").clone() {
            MutableMode::Unknown => Ok(SnapshotResolution::Unknown),
            MutableMode::Present(snapshot) => {
                Ok(SnapshotResolution::Present(publishable(snapshot)))
            }
            MutableMode::Failing => Err(StoreError("injected targeted-refetch outage".into())),
        }
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
        let (sender, receiver) = tokio::sync::broadcast::channel(1);
        drop(sender);
        receiver
    }
}

#[tokio::test]
async fn negative_ttl_retry_is_backed_off_and_recovers_without_push() {
    let source = MutableNoPushSource::new();
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let manager = SnapshotManager::spawn(
        source.clone(),
        map.clone(),
        SlotRegistry::new(),
        Arc::new(SystemClock),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_secs(60),
            negative_ttl: SignedDuration::from_millis(40),
            retry_backoff: std::time::Duration::from_millis(80),
            max_concurrent_fetches: 1,
        },
    )
    .unwrap();

    let mut ready = manager.ready();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !*ready.borrow() {
            ready.changed().await.unwrap();
        }
    })
    .await
    .expect("initial unknown must resolve");

    source.set(MutableMode::Failing);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while source.calls.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("negative TTL must trigger the first targeted refetch");
    let calls_after_failure = source.calls.load(Ordering::Acquire);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(
        source.calls.load(Ordering::Acquire),
        calls_after_failure,
        "a source error must not cause a zero-delay retry loop"
    );

    source.set(MutableMode::Present(snapshot(1, PermissionBits::bit(0))));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("backed-off targeted refetch must recover");

    manager.shutdown().await;
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
    async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
        if self.fail.load(Ordering::Acquire) {
            Err(StoreError("injected snapshot outage".into()))
        } else {
            Ok(SnapshotResolution::Present(publishable(Arc::clone(
                &self.snapshot,
            ))))
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
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
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

/// Issue #4: `ready` answers one bit, which is the right shape for a probe
/// and the wrong shape for diagnosis — it cannot say whether one principal is
/// unresolved or a thousand, nor whether the source has been failing all
/// morning. The counters are what distinguish those, and `unresolved` is
/// computed from the same pass that decides readiness, so the two agree by
/// construction rather than by luck.
#[tokio::test(start_paused = true)]
async fn snapshot_counters_track_failures_and_the_unresolved_gauge() {
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
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_millis(20),
            negative_ttl: SignedDuration::from_secs(30),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 2,
        },
    )
    .unwrap();
    let counters = manager.counters();
    let ready = manager.ready();
    settle().await;

    let healthy = counters.snapshot();
    assert!(healthy.refresh_attempts > 0, "the loop is fetching");
    assert_eq!(healthy.refresh_failures, 0);
    assert_eq!(
        healthy.unresolved,
        0,
        "resolved, and readiness agrees: {}",
        *ready.borrow()
    );
    assert!(*ready.borrow());

    // The source goes down and the resolution lapses. Readiness falls, and
    // the gauge says how much of the tracked set is affected — the thing a
    // boolean cannot report.
    source.fail.store(true, Ordering::Release);
    clock.set(t(100_000));
    settle().await;

    let outage = counters.snapshot();
    assert!(!*ready.borrow());
    assert_eq!(outage.unresolved, 1, "one tracked principal, unresolved");
    assert!(
        outage.refresh_failures > 0,
        "a failing source is distinguishable from an idle one"
    );
    assert!(outage.refresh_attempts > healthy.refresh_attempts);

    manager.shutdown().await;
}

struct FirstThenHangsSource {
    snapshot: Arc<AccountSnapshot>,
    calls: AtomicUsize,
    push: tokio::sync::broadcast::Sender<SnapshotPush>,
}

#[async_trait]
impl SnapshotSource for FirstThenHangsSource {
    async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            Ok(SnapshotResolution::Present(publishable(Arc::clone(
                &self.snapshot,
            ))))
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
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
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
    async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
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
            principals: TrackedPrincipals::Fixed((0..64).map(Principal).collect()),
            refresh_interval: std::time::Duration::from_secs(1),
            negative_ttl: SignedDuration::from_secs(30),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 4,
        },
    )
    .unwrap();
    tokio::task::yield_now().await;
    let report = tokio::time::timeout(std::time::Duration::from_millis(100), manager.shutdown())
        .await
        .expect("shutdown must cancel outstanding source futures");
    assert!(
        !report.task_died,
        "cancelling a hung fetch is a clean stop, not a death"
    );
}

/// A source that panics kills only its own fetch task: the manager keeps
/// sweeping and still reports a clean stop. The panic is not swallowed — it
/// surfaces as an event (issue #36) — but it must not be mistaken for the
/// manager itself dying, which is what `task_died` is for.
#[tokio::test]
async fn a_panicking_fetch_does_not_kill_the_manager() {
    struct PanickingSource {
        push: tokio::sync::broadcast::Sender<SnapshotPush>,
    }

    #[async_trait]
    impl SnapshotSource for PanickingSource {
        async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
            panic!("source exploded");
        }

        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
            self.push.subscribe()
        }
    }

    let (push, _keep) = tokio::sync::broadcast::channel(4);
    let manager = SnapshotManager::spawn(
        Arc::new(PanickingSource { push }),
        Arc::new(ArcSwapSnapshotMap::new()),
        SlotRegistry::new(),
        Arc::new(ManualClock::new(t(0))),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_millis(10),
            negative_ttl: SignedDuration::from_secs(30),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 1,
        },
    )
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let report = manager.shutdown().await;
    assert!(
        !report.task_died,
        "the sweep survives a fetch that panicked; only the fetch died"
    );
}

#[test]
fn invalid_snapshot_manager_intervals_are_rejected() {
    let config = SnapshotManagerConfig {
        principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
        refresh_interval: std::time::Duration::ZERO,
        negative_ttl: SignedDuration::from_secs(30),
        retry_backoff: std::time::Duration::from_millis(5),
        max_concurrent_fetches: 4,
    };
    assert!(config.validate().is_err());
}

// ---- dynamic principal discovery (#48) ------------------------------------

const LATER_PRINCIPAL: Principal = Principal(8);

/// A discovering fixture, seeded with nothing: the set comes from the source.
fn discovering_fixture(store: Arc<MemoryStore>) -> Fixture {
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let engine = AdmissionEngine::new(Arc::clone(&map));
    let slots = SlotRegistry::new();
    let manager = SnapshotManager::spawn(
        store.clone(),
        map,
        Arc::clone(&slots),
        Arc::new(ManualClock::new(t(0))),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::All { seed: Vec::new() },
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

fn admit_as(fixture: &Fixture, principal: Principal) -> Result<(), DenyReason> {
    fixture
        .engine
        .admit(
            AdmissionRequest {
                principal,
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

/// The point of #48: a principal published *after* the instance started is
/// served without a restart. Before this, the tracked set was fixed at
/// construction and a newly provisioned customer was denied until a redeploy.
#[tokio::test(start_paused = true)]
async fn a_principal_published_after_start_is_discovered_and_served() {
    let store = base_store();
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))));
    let fixture = discovering_fixture(store.clone());
    stock_slot(&fixture);
    settle().await;
    assert_eq!(admit_as(&fixture, PRINCIPAL), Ok(()));
    assert_eq!(
        admit_as(&fixture, LATER_PRINCIPAL),
        Err(DenyReason::UnknownPrincipal),
        "not published yet, so denied fail-closed"
    );

    store.publish_snapshot(
        LATER_PRINCIPAL,
        publishable(snapshot(1, PermissionBits::bit(0))),
    );
    settle().await;

    assert_eq!(
        admit_as(&fixture, LATER_PRINCIPAL),
        Ok(()),
        "a principal this instance was never configured with must now be served"
    );
    assert_eq!(
        admit_as(&fixture, PRINCIPAL),
        Ok(()),
        "and the original still is"
    );
}

/// Discovery is opt-in: a `Fixed` instance must not pick the new principal up,
/// so upgrading changes no existing deployment's behaviour.
#[tokio::test(start_paused = true)]
async fn a_fixed_instance_ignores_principals_it_was_not_configured_with() {
    let store = base_store();
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))));
    let fixture = fixture(store.clone());
    stock_slot(&fixture);
    settle().await;

    store.publish_snapshot(
        LATER_PRINCIPAL,
        publishable(snapshot(1, PermissionBits::bit(0))),
    );
    settle().await;

    assert_eq!(
        admit_as(&fixture, LATER_PRINCIPAL),
        Err(DenyReason::UnknownPrincipal),
        "Fixed means fixed: this instance serves its configured slice only"
    );
    assert_eq!(admit_as(&fixture, PRINCIPAL), Ok(()));
}

/// Revocation and removal-from-the-catalogue are different things, and
/// conflating them is how a revoked principal gets resurrected
/// (INVARIANTS.md #15).
///
/// `remove_snapshot` leaves a tombstone, so the principal stays *enumerated*
/// and therefore stays tracked. It is denied because its resolution is
/// negative, not because it was forgotten — and the generation watermark it
/// keeps is what makes a replayed older snapshot a no-op.
#[tokio::test(start_paused = true)]
async fn a_revoked_principal_stays_tracked_and_cannot_be_resurrected() {
    let store = base_store();
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(5, PermissionBits::bit(0))));
    let fixture = discovering_fixture(store.clone());
    stock_slot(&fixture);
    settle().await;
    assert_eq!(admit_as(&fixture, PRINCIPAL), Ok(()));

    store.remove_snapshot(PRINCIPAL);
    settle().await;
    assert_eq!(
        admit_as(&fixture, PRINCIPAL),
        Err(DenyReason::UnknownPrincipal),
        "revocation reaches a running instance"
    );

    // A delayed publish at an older generation must not bring it back.
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(4, PermissionBits::bit(0))));
    settle().await;
    assert_eq!(
        admit_as(&fixture, PRINCIPAL),
        Err(DenyReason::UnknownPrincipal),
        "the tombstone's watermark outranks the replay"
    );

    // A genuinely newer one does.
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(6, PermissionBits::bit(0))));
    settle().await;
    assert_eq!(admit_as(&fixture, PRINCIPAL), Ok(()));
}

/// A source that enumerates two principals but can only answer for one.
///
/// A *revoked* principal is not unresolvable — it resolves negatively, and a
/// negative resolution counts as resolved. The only thing that genuinely
/// leaves a principal unresolved is a fetch that errors, so that is what this
/// injects.
struct OneUnanswerableSource {
    good: Arc<AccountSnapshot>,
}

#[async_trait]
impl SnapshotSource for OneUnanswerableSource {
    async fn snapshot(&self, principal: Principal) -> Result<SnapshotResolution, StoreError> {
        if principal == PRINCIPAL {
            Ok(SnapshotResolution::Present(publishable(Arc::clone(
                &self.good,
            ))))
        } else {
            Err(StoreError("injected permanent fetch failure".into()))
        }
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
        let (sender, receiver) = tokio::sync::broadcast::channel(1);
        drop(sender);
        receiver
    }

    async fn principals(&self) -> Result<Option<Vec<Principal>>, StoreError> {
        Ok(Some(vec![PRINCIPAL, LATER_PRINCIPAL]))
    }
}

/// Readiness under `All` means "I can serve someone", not "I can serve
/// everyone": one principal the source cannot answer for must not hold an
/// otherwise healthy instance out of rotation (INVARIANTS.md #10). Under
/// `Fixed` the strict all-resolved reading is kept, and this same source
/// leaves that instance unready — the two readings are asserted against one
/// fixture so the difference is the rule, not the setup.
#[tokio::test(start_paused = true)]
async fn one_unanswerable_principal_unreadies_only_a_fixed_instance() {
    fn spawn(mode: TrackedPrincipals) -> Fixture {
        let source = Arc::new(OneUnanswerableSource {
            good: snapshot(1, PermissionBits::bit(0)),
        });
        let map = Arc::new(ArcSwapSnapshotMap::new());
        let engine = AdmissionEngine::new(Arc::clone(&map));
        let slots = SlotRegistry::new();
        let manager = SnapshotManager::spawn(
            source,
            map,
            Arc::clone(&slots),
            Arc::new(ManualClock::new(t(0))),
            SnapshotManagerConfig {
                principals: mode,
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

    let discovering = spawn(TrackedPrincipals::All { seed: Vec::new() });
    stock_slot(&discovering);
    settle().await;
    assert!(
        *discovering.manager.ready().borrow(),
        "one of two principals is serviceable, so the instance belongs in rotation"
    );
    assert_eq!(admit_as(&discovering, PRINCIPAL), Ok(()));
    assert_eq!(
        admit_as(&discovering, LATER_PRINCIPAL),
        Err(DenyReason::UnknownPrincipal),
        "and the unanswerable one is still denied, fail-closed"
    );

    let fixed = spawn(TrackedPrincipals::Fixed(vec![PRINCIPAL, LATER_PRINCIPAL]));
    stock_slot(&fixed);
    settle().await;
    assert!(
        !*fixed.manager.ready().borrow(),
        "a configured slice is small and hand-picked, so an unanswerable \
         member is a real gap and readiness still says so"
    );
}

/// A source with no catalogue at all — it does not override `principals`, so
/// it takes the trait's default.
struct NoCatalogueSource {
    snapshot: Arc<AccountSnapshot>,
}

#[async_trait]
impl SnapshotSource for NoCatalogueSource {
    async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
        Ok(SnapshotResolution::Present(publishable(Arc::clone(
            &self.snapshot,
        ))))
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
        let (sender, receiver) = tokio::sync::broadcast::channel(1);
        drop(sender);
        receiver
    }
}

/// A source that enumerates but always fails to.
struct FailingCatalogueSource {
    snapshot: Arc<AccountSnapshot>,
    failures: Arc<AtomicUsize>,
}

#[async_trait]
impl SnapshotSource for FailingCatalogueSource {
    async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
        Ok(SnapshotResolution::Present(publishable(Arc::clone(
            &self.snapshot,
        ))))
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
        let (sender, receiver) = tokio::sync::broadcast::channel(1);
        drop(sender);
        receiver
    }

    async fn principals(&self) -> Result<Option<Vec<Principal>>, StoreError> {
        self.failures.fetch_add(1, Ordering::AcqRel);
        Err(StoreError("injected catalogue outage".into()))
    }
}

fn spawn_with(source: Arc<dyn SnapshotSource>, config: SnapshotManagerConfig) -> Fixture {
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let engine = AdmissionEngine::new(Arc::clone(&map));
    let slots = SlotRegistry::new();
    let manager = SnapshotManager::spawn(
        source,
        map,
        Arc::clone(&slots),
        Arc::new(ManualClock::new(t(0))),
        config,
    )
    .unwrap();
    Fixture {
        engine,
        slots,
        manager,
    }
}

fn discovering_config(refresh_ms: u64) -> SnapshotManagerConfig {
    SnapshotManagerConfig {
        principals: TrackedPrincipals::All {
            seed: vec![PRINCIPAL],
        },
        refresh_interval: std::time::Duration::from_millis(refresh_ms),
        negative_ttl: SignedDuration::from_secs(30),
        retry_backoff: std::time::Duration::from_millis(5),
        max_concurrent_fetches: 4,
    }
}

/// "Cannot enumerate" and "the catalogue is empty" lead an instance to
/// opposite conclusions, and the trait's default must mean the first: keep
/// the configured set. Returning `Some(vec![])` instead would untrack
/// everyone and serve nobody — from a source whose only failing is that it
/// predates the seam.
#[tokio::test(start_paused = true)]
async fn a_source_that_cannot_enumerate_keeps_the_configured_set() {
    let fixture = spawn_with(
        Arc::new(NoCatalogueSource {
            snapshot: snapshot(1, PermissionBits::bit(0)),
        }),
        discovering_config(20),
    );
    stock_slot(&fixture);
    settle().await;

    assert_eq!(
        admit_as(&fixture, PRINCIPAL),
        Ok(()),
        "the seed must survive a source with no catalogue"
    );
    assert!(*fixture.manager.ready().borrow());
}

/// A failing enumeration is not a silent no-op: it keeps the current set —
/// so everything already known carries on working — and says so in its own
/// counter, because that is the only place it is visible. `unresolved` cannot
/// show it: nothing became unresolved, new principals just stopped arriving.
#[tokio::test(start_paused = true)]
async fn a_failing_enumeration_is_counted_and_keeps_the_current_set() {
    let failures = Arc::new(AtomicUsize::new(0));
    let fixture = spawn_with(
        Arc::new(FailingCatalogueSource {
            snapshot: snapshot(1, PermissionBits::bit(0)),
            failures: Arc::clone(&failures),
        }),
        discovering_config(20),
    );
    stock_slot(&fixture);
    settle().await;

    assert!(failures.load(Ordering::Acquire) > 0, "it was attempted");
    assert_eq!(
        admit_as(&fixture, PRINCIPAL),
        Ok(()),
        "the seeded principal keeps being served through the outage"
    );
    let stats = fixture.manager.counters().snapshot();
    assert!(
        stats.discovery_failures > 0,
        "a frozen tracked set must be visible somewhere: {stats:?}"
    );
    assert_eq!(
        stats.unresolved, 0,
        "and it is not visible in `unresolved`, which is why it needs its own counter"
    );
}

/// A push is discovery in its own right: the principal it names is tracked and
/// installed without waiting for the next enumeration. The refresh interval
/// here is longer than the test, so only the push can explain the result.
#[tokio::test(start_paused = true)]
async fn a_push_discovers_the_principal_it_names() {
    let store = base_store();
    store.publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))));
    let fixture = spawn_with(store.clone(), discovering_config(600_000));
    stock_slot(&fixture);
    settle().await;
    assert_eq!(
        admit_as(&fixture, LATER_PRINCIPAL),
        Err(DenyReason::UnknownPrincipal)
    );

    store.publish_snapshot(
        LATER_PRINCIPAL,
        publishable(snapshot(1, PermissionBits::bit(0))),
    );
    settle().await;

    assert_eq!(
        admit_as(&fixture, LATER_PRINCIPAL),
        Ok(()),
        "the push carried the principal; the next refresh is ten minutes away"
    );
}

/// The other half of the readiness rule: `All` falls unready when *nothing*
/// resolves. "Serving someone" is the bar, and an instance serving nobody is
/// below it however many principals it tracks.
#[tokio::test(start_paused = true)]
async fn a_discovering_instance_with_nothing_resolvable_is_unready() {
    struct AllFail;

    #[async_trait]
    impl SnapshotSource for AllFail {
        async fn snapshot(&self, _principal: Principal) -> Result<SnapshotResolution, StoreError> {
            Err(StoreError("injected total source outage".into()))
        }

        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SnapshotPush> {
            let (sender, receiver) = tokio::sync::broadcast::channel(1);
            drop(sender);
            receiver
        }

        async fn principals(&self) -> Result<Option<Vec<Principal>>, StoreError> {
            Ok(Some(vec![PRINCIPAL, LATER_PRINCIPAL]))
        }
    }

    let fixture = spawn_with(Arc::new(AllFail), discovering_config(20));
    stock_slot(&fixture);
    settle().await;

    assert!(
        !*fixture.manager.ready().borrow(),
        "two principals tracked and neither resolvable: this instance serves nobody"
    );
}
