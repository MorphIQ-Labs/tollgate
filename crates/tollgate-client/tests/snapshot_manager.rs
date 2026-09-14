//! SnapshotManager behavior (review finding #5): initial load gates
//! readiness, pushes propagate, refresh recovers, and revocation reaches
//! running instances.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{AdmissionEngine, ArcSwapSnapshotMap, MapEntry, SnapshotMap};
use tollgate_client::{
    ManualClock, SlotRegistry, SnapshotManager, SnapshotManagerConfig, SystemClock,
    TrackedPrincipals,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, DenyReason,
    DiscardedUsage, FencingToken, Generation, LeaseGrant, LeaseId, LocalLease, LocalSharding,
    OpIndex, PermissionBits, Principal, PublishableSnapshot, ResolvedLimits,
};
#[path = "../../tollgate-store/tests/support/delegating.rs"]
mod delegating;
use delegating::{DelegatingStore, RejectingStore, rejecting};

use tollgate_store::{
    AccountConfig, GrantPolicy, MemoryStore, SnapshotPush, SnapshotResolution, SnapshotSource,
    StoreError,
};

#[path = "support/snapshot_source.rs"]
mod snapshot_source;
use snapshot_source::DrivenPushSource;

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
    Arc::new(
        AccountSnapshot::builder(
            ACCOUNT,
            Generation(generation),
            AccountStatus::Active,
            t(100_000),
            permissions,
            ResolvedLimits::new(64).with_weighted_rate(1_000_000, 1_000_000),
            Arc::new(
                CostTable::builder(CostUnits(50), CostUnits(50))
                    .weight(&PriceOp, CostUnits(1))
                    .build(),
            ),
        )
        .build(),
    )
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
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 4,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    store
}

/// Fund the account's slot directly so admission outcomes isolate snapshot
/// behavior.
fn stock_slot(fixture: &Fixture) {
    drop(
        fixture
            .slots
            .slot(ACCOUNT)
            .replace(Arc::new(LocalLease::new(
                LeaseGrant {
                    lease_id: LeaseId(1),
                    account_id: ACCOUNT,
                    fencing_token: FencingToken(1),
                    units: CostUnits(1_000_000),
                    expires_at: t(100_000),
                },
                CostUnits::ZERO,
            ))),
    );
}

fn admit(fixture: &Fixture) -> Result<(), DenyReason> {
    fixture
        .engine
        .begin(PRINCIPAL, PermissionBits::bit(0), t(1))
        .and_then(|context| context.admit(&[(PriceOp, 1)], DiscardedUsage::new().slot(), t(1)))
        .map(|pending| {
            pending.cancel();
        })
}

async fn settle() {
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
}

#[tokio::test(start_paused = true)]
async fn initial_load_gates_readiness_and_installs() {
    let store = base_store();
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
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
async fn snapshot_readiness_is_false_after_shutdown_or_owner_drop() {
    for stop in 0..3 {
        let fixture = fixture(base_store());
        let mut ready = fixture.manager.ready();
        while !*ready.borrow_and_update() {
            ready.changed().await.unwrap();
        }
        match stop {
            0 => assert!(!fixture.manager.shutdown().await.task_died),
            1 => drop(fixture.manager),
            _ => drop(fixture.manager.shutdown()),
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while ready.changed().await.is_ok() {}
        })
        .await
        .expect("task must release its readiness publisher");
        assert!(!*ready.borrow(), "closed readiness must retain false");
    }
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_task_panic_withdraws_the_retained_readiness_value() {
    struct PanicClock(AtomicBool);
    impl tollgate_store::Clock for PanicClock {
        fn now(&self) -> Timestamp {
            assert!(!self.0.load(Ordering::Relaxed), "fixture clock panic");
            t(0)
        }
    }
    let clock = Arc::new(PanicClock(AtomicBool::new(false)));
    let (source, pulls) = DrivenPushSource::new(SnapshotResolution::Unknown);
    let manager = SnapshotManager::spawn(
        pulls.clone(),
        Arc::new(ArcSwapSnapshotMap::new()),
        SlotRegistry::new(),
        clock.clone(),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_secs(60),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_secs(1),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
        },
    )
    .unwrap();
    let mut ready = manager.ready();
    while !*ready.borrow_and_update() {
        ready.changed().await.unwrap();
    }
    clock.0.store(true, Ordering::Relaxed);
    source.send(PRINCIPAL, SnapshotResolution::Unknown);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while ready.changed().await.is_ok() {}
    })
    .await
    .expect("panicked task must release its readiness publisher");
    assert!(!*ready.borrow());
    assert!(manager.shutdown().await.task_died);
}

#[tokio::test(start_paused = true)]
async fn push_update_propagates_permission_change() {
    let store = base_store();
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
    let fixture = fixture(store.clone());
    stock_slot(&fixture);
    settle().await;
    assert_eq!(admit(&fixture), Ok(()));

    // The control plane strips the permission in generation 2.
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(2, PermissionBits::NONE)))
        .expect("snapshot fixture matches its account and credential");
    settle().await;
    assert_eq!(admit(&fixture), Err(DenyReason::MissingPermission));
    fixture.manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn revocation_reaches_instances_via_refresh() {
    let store = base_store();
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
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
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
    settle().await;
    assert_eq!(admit(&fixture), Err(DenyReason::UnknownPrincipal));
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(2, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
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
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
    settle().await;
    assert_eq!(admit(&fixture), Ok(()));
    fixture.manager.shutdown().await;
}

#[derive(Clone)]
enum MutableMode {
    Unknown,
    Revoked,
    Present(Arc<AccountSnapshot>),
    Failing,
}

/// A receiver that never yields a push: the sender is dropped immediately.
///
/// Nine fixtures in this file wrote this out; it is one function now.
fn no_pushes() -> tokio::sync::broadcast::Receiver<SnapshotPush> {
    let (sender, receiver) = tokio::sync::broadcast::channel(1);
    drop(sender);
    receiver
}

/// The shape most fixtures here have: no pushes and no catalogue.
///
/// Both answers are *stated*. `principals` has a default body -- the `Ok(None)`
/// sentinel -- and these doubles used to get it by omission, which is the same
/// silence that made wrappers elsewhere lie about a catalogue they did have
/// (#83). Saying it is the difference between the two.
fn bare_source(reason: &'static str) -> DelegatingStore<RejectingStore> {
    rejecting(reason)
        .on_subscribe(|_| no_pushes())
        .on_principals(|_| async { Ok(None) })
}

/// A source whose answer the test rewrites between passes.
struct MutableNoPushSource {
    mode: Mutex<MutableMode>,
    calls: AtomicUsize,
}

impl MutableNoPushSource {
    /// The handle and the source to hand [`SnapshotManager::spawn`].
    fn new() -> (Arc<Self>, Arc<DelegatingStore<RejectingStore>>) {
        let source = Arc::new(Self {
            mode: Mutex::new(MutableMode::Unknown),
            calls: AtomicUsize::new(0),
        });
        let pulls = Arc::clone(&source);
        (
            source,
            Arc::new(
                bare_source("a mutable fixture answers snapshots and nothing else").on_snapshot(
                    move |_, _principal| {
                        let source = Arc::clone(&pulls);
                        async move {
                            source.calls.fetch_add(1, Ordering::AcqRel);
                            match source.mode.lock().expect("source mode poisoned").clone() {
                                MutableMode::Unknown => Ok(SnapshotResolution::Unknown),
                                MutableMode::Revoked => Ok(SnapshotResolution::Revoked {
                                    generation: Generation(9),
                                }),
                                MutableMode::Present(snapshot) => {
                                    Ok(SnapshotResolution::Present(publishable(snapshot)))
                                }
                                MutableMode::Failing => {
                                    Err(StoreError("injected targeted-refetch outage".into()))
                                }
                            }
                        }
                    },
                ),
            ),
        )
    }

    fn set(&self, mode: MutableMode) {
        *self.mode.lock().expect("source mode poisoned") = mode;
    }
}

#[tokio::test]
async fn negative_ttl_retry_is_backed_off_and_recovers_without_push() {
    let (source, pulls) = MutableNoPushSource::new();
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let manager = SnapshotManager::spawn(
        pulls.clone(),
        map.clone(),
        SlotRegistry::new(),
        Arc::new(SystemClock),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_secs(60),
            unknown_ttl: SignedDuration::from_millis(40),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(80),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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

/// A source with live pushes whose pulls can be switched to failing.
struct ToggleSource {
    fail: AtomicBool,
    push: tokio::sync::broadcast::Sender<SnapshotPush>,
}

impl ToggleSource {
    fn new(snapshot: Arc<AccountSnapshot>) -> (Arc<Self>, Arc<DelegatingStore<RejectingStore>>) {
        let (push, _) = tokio::sync::broadcast::channel(4);
        let source = Arc::new(Self {
            fail: AtomicBool::new(false),
            push,
        });
        let (pulls, subs) = (Arc::clone(&source), Arc::clone(&source));
        (
            source,
            Arc::new(
                rejecting("a toggle fixture answers snapshots and nothing else")
                    .on_snapshot(move |_, _principal| {
                        let (source, snapshot) = (Arc::clone(&pulls), Arc::clone(&snapshot));
                        async move {
                            if source.fail.load(Ordering::Acquire) {
                                Err(StoreError("injected snapshot outage".into()))
                            } else {
                                Ok(SnapshotResolution::Present(publishable(snapshot)))
                            }
                        }
                    })
                    .on_subscribe(move |_| subs.push.subscribe())
                    .on_principals(|_| async { Ok(None) }),
            ),
        )
    }
}

#[tokio::test(start_paused = true)]
async fn readiness_falls_when_snapshot_expires_during_outage() {
    let (source, pulls) = ToggleSource::new(snapshot(1, PermissionBits::bit(0)));
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let slots = SlotRegistry::new();
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = SnapshotManager::spawn(
        pulls.clone(),
        map,
        slots,
        Arc::clone(&clock) as _,
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_millis(20),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 2,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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
    let (source, pulls) = ToggleSource::new(snapshot(1, PermissionBits::bit(0)));
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let slots = SlotRegistry::new();
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = SnapshotManager::spawn(
        pulls.clone(),
        map,
        slots,
        Arc::clone(&clock) as _,
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_millis(20),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 2,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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

/// Answers the first pull, then never answers again.
fn first_then_hangs_source(
    snapshot: Arc<AccountSnapshot>,
    push: tokio::sync::broadcast::Sender<SnapshotPush>,
) -> Arc<DelegatingStore<RejectingStore>> {
    let calls = Arc::new(AtomicUsize::new(0));
    Arc::new(
        rejecting("a first-then-hangs fixture answers snapshots and nothing else")
            .on_snapshot(move |_, _principal| {
                let (calls, snapshot) = (Arc::clone(&calls), Arc::clone(&snapshot));
                async move {
                    if calls.fetch_add(1, Ordering::AcqRel) == 0 {
                        Ok(SnapshotResolution::Present(publishable(snapshot)))
                    } else {
                        std::future::pending().await
                    }
                }
            })
            .on_subscribe(move |_| push.subscribe())
            .on_principals(|_| async { Ok(None) }),
    )
}

#[tokio::test]
async fn readiness_falls_if_refresh_hangs_across_snapshot_expiry() {
    let mut expiring = (*snapshot(1, PermissionBits::bit(0))).clone();
    expiring.valid_until = Timestamp::now()
        .checked_add(SignedDuration::from_millis(500))
        .unwrap();
    let (push, _) = tokio::sync::broadcast::channel(4);
    let source = first_then_hangs_source(Arc::new(expiring), push);
    let manager = SnapshotManager::spawn(
        source,
        Arc::new(ArcSwapSnapshotMap::new()),
        SlotRegistry::new(),
        Arc::new(SystemClock),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_millis(5),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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

/// A pull that never returns.
fn hanging_source(
    push: tokio::sync::broadcast::Sender<SnapshotPush>,
) -> Arc<DelegatingStore<RejectingStore>> {
    Arc::new(
        rejecting("a hanging fixture answers snapshots and nothing else")
            .on_snapshot(|_, _principal| async { std::future::pending().await })
            .on_subscribe(move |_| push.subscribe())
            .on_principals(|_| async { Ok(None) }),
    )
}

#[tokio::test]
async fn shutdown_cancels_in_flight_snapshot_fetches() {
    let (push, _) = tokio::sync::broadcast::channel(4);
    let manager = SnapshotManager::spawn(
        hanging_source(push),
        Arc::new(ArcSwapSnapshotMap::new()),
        SlotRegistry::new(),
        Arc::new(ManualClock::new(t(0))),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed((0..64).map(Principal).collect()),
            refresh_interval: std::time::Duration::from_secs(1),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 4,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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
    let (push, _keep) = tokio::sync::broadcast::channel(4);
    let manager = SnapshotManager::spawn(
        Arc::new(
            rejecting("a panicking fixture answers snapshots and nothing else")
                .on_snapshot(|_, _principal| async { panic!("source exploded") })
                .on_subscribe(move |_| push.subscribe())
                .on_principals(|_| async { Ok(None) }),
        ),
        Arc::new(ArcSwapSnapshotMap::new()),
        SlotRegistry::new(),
        Arc::new(ManualClock::new(t(0))),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_millis(10),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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

/// INVARIANTS.md #16: no config field is silently repaired.
#[test]
fn a_zero_fetch_timeout_is_rejected() {
    let config = SnapshotManagerConfig {
        principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
        refresh_interval: std::time::Duration::from_millis(20),
        unknown_ttl: SignedDuration::from_secs(30),
        revoked_ttl: SignedDuration::from_secs(3_600),
        retry_backoff: std::time::Duration::from_millis(5),
        max_concurrent_fetches: 4,
        fetch_timeout: std::time::Duration::ZERO,
        enumeration_timeout: std::time::Duration::from_secs(30),
    };
    assert_eq!(
        config.validate().unwrap_err().to_string(),
        "fetch_timeout must be positive"
    );
}

#[test]
fn invalid_snapshot_manager_intervals_are_rejected() {
    let config = SnapshotManagerConfig {
        principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
        refresh_interval: std::time::Duration::ZERO,
        unknown_ttl: SignedDuration::from_secs(30),
        revoked_ttl: SignedDuration::from_secs(3_600),
        retry_backoff: std::time::Duration::from_millis(5),
        max_concurrent_fetches: 4,
        fetch_timeout: std::time::Duration::from_secs(5),
        enumeration_timeout: std::time::Duration::from_secs(30),
    };
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn mismatched_local_sharding_is_rejected_before_tasks_start() {
    let sharding = LocalSharding::new(NonZeroUsize::new(8).unwrap());
    let result = SnapshotManager::spawn(
        base_store(),
        Arc::new(ArcSwapSnapshotMap::with_sharding(sharding)),
        SlotRegistry::new(),
        Arc::new(ManualClock::new(t(0))),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_millis(20),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 4,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
        },
    );
    let error = match result {
        Ok(_) => panic!("mismatched sharding must not start"),
        Err(error) => error,
    };
    assert_eq!(
        error.0,
        "snapshot map and lease slots must use the same local sharding"
    );
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
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 4,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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
        .begin(principal, PermissionBits::bit(0), t(1))
        .and_then(|context| context.admit(&[(PriceOp, 1)], DiscardedUsage::new().slot(), t(1)))
        .map(|pending| {
            pending.cancel();
        })
}

/// The point of #48: a principal published *after* the instance started is
/// served without a restart. Before this, the tracked set was fixed at
/// construction and a newly provisioned customer was denied until a redeploy.
#[tokio::test(start_paused = true)]
async fn a_principal_published_after_start_is_discovered_and_served() {
    let store = base_store();
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
    let fixture = discovering_fixture(store.clone());
    stock_slot(&fixture);
    settle().await;
    assert_eq!(admit_as(&fixture, PRINCIPAL), Ok(()));
    assert_eq!(
        admit_as(&fixture, LATER_PRINCIPAL),
        Err(DenyReason::UnknownPrincipal),
        "not published yet, so denied fail-closed"
    );

    store
        .publish_snapshot(
            LATER_PRINCIPAL,
            publishable(snapshot(1, PermissionBits::bit(0))),
        )
        .expect("snapshot fixture matches its account and credential");
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
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
    let fixture = fixture(store.clone());
    stock_slot(&fixture);
    settle().await;

    store
        .publish_snapshot(
            LATER_PRINCIPAL,
            publishable(snapshot(1, PermissionBits::bit(0))),
        )
        .expect("snapshot fixture matches its account and credential");
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
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(5, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
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
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(4, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
    settle().await;
    assert_eq!(
        admit_as(&fixture, PRINCIPAL),
        Err(DenyReason::UnknownPrincipal),
        "the tombstone's watermark outranks the replay"
    );

    // Nor at the tombstone's *own* generation. This is the case #53 must not
    // loosen: an absence lets its generation back because nothing declared it
    // dead, but a revocation declared exactly this one dead. Nothing pinned
    // equality here before — every recovery test stepped strictly over the
    // watermark — so the accept side had never been exercised at it.
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(5, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
    settle().await;
    assert_eq!(
        admit_as(&fixture, PRINCIPAL),
        Err(DenyReason::UnknownPrincipal),
        "a replay at the tombstone's own generation stays dead"
    );

    // A genuinely newer one does.
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(6, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
    settle().await;
    assert_eq!(admit_as(&fixture, PRINCIPAL), Ok(()));
}

/// A source that enumerates two principals but can only answer for one.
///
/// A *revoked* principal is not unresolvable — it resolves negatively, and a
/// negative resolution counts as resolved. The only thing that genuinely
/// leaves a principal unresolved is a fetch that errors, so that is what this
/// injects.
/// Answers for one principal and permanently refuses the other, while
/// enumerating both.
fn one_unanswerable_source(good: Arc<AccountSnapshot>) -> Arc<DelegatingStore<RejectingStore>> {
    Arc::new(
        rejecting("an unanswerable fixture answers snapshots and enumeration")
            .on_snapshot(move |_, principal| {
                let good = Arc::clone(&good);
                async move {
                    if principal == PRINCIPAL {
                        Ok(SnapshotResolution::Present(publishable(good)))
                    } else {
                        Err(StoreError("injected permanent fetch failure".into()))
                    }
                }
            })
            .on_subscribe(|_| no_pushes())
            .on_principals(|_| async { Ok(Some(vec![PRINCIPAL, LATER_PRINCIPAL])) }),
    )
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
        let source = one_unanswerable_source(snapshot(1, PermissionBits::bit(0)));
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
                unknown_ttl: SignedDuration::from_secs(30),
                revoked_ttl: SignedDuration::from_secs(3_600),
                retry_backoff: std::time::Duration::from_millis(5),
                max_concurrent_fetches: 4,
                fetch_timeout: std::time::Duration::from_secs(5),
                enumeration_timeout: std::time::Duration::from_secs(30),
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
/// Answers snapshots and cannot enumerate.
fn no_catalogue_source(snapshot: Arc<AccountSnapshot>) -> Arc<DelegatingStore<RejectingStore>> {
    Arc::new(
        bare_source("a no-catalogue fixture answers snapshots and nothing else").on_snapshot(
            move |_, _principal| {
                let snapshot = Arc::clone(&snapshot);
                async move { Ok(SnapshotResolution::Present(publishable(snapshot))) }
            },
        ),
    )
}

/// A source that enumerates but always fails to.
fn failing_catalogue_source(
    snapshot: Arc<AccountSnapshot>,
    failures: &Arc<AtomicUsize>,
) -> Arc<DelegatingStore<RejectingStore>> {
    let failures = Arc::clone(failures);
    Arc::new(
        rejecting("a failing-catalogue fixture answers snapshots and enumeration")
            .on_snapshot(move |_, _principal| {
                let snapshot = Arc::clone(&snapshot);
                async move { Ok(SnapshotResolution::Present(publishable(snapshot))) }
            })
            .on_subscribe(|_| no_pushes())
            .on_principals(move |_| {
                let failures = Arc::clone(&failures);
                async move {
                    failures.fetch_add(1, Ordering::AcqRel);
                    Err(StoreError("injected catalogue outage".into()))
                }
            }),
    )
}

/// A source that answers snapshots but never answers enumeration.
fn hanging_catalogue_source(
    snapshot: Arc<AccountSnapshot>,
) -> Arc<DelegatingStore<RejectingStore>> {
    Arc::new(
        rejecting("a hanging-catalogue fixture answers snapshots and enumeration")
            .on_snapshot(move |_, _principal| {
                let snapshot = Arc::clone(&snapshot);
                async move { Ok(SnapshotResolution::Present(publishable(snapshot))) }
            })
            .on_subscribe(|_| no_pushes())
            .on_principals(|_| async { std::future::pending().await }),
    )
}

/// A source whose enumeration hangs until released, and which answers
/// snapshots normally throughout.
fn hangs_on_enumeration_source(
    snapshot: Arc<AccountSnapshot>,
    release: &Arc<tokio::sync::Notify>,
    enumerations: &Arc<AtomicUsize>,
) -> Arc<DelegatingStore<RejectingStore>> {
    let (release, enumerations) = (Arc::clone(release), Arc::clone(enumerations));
    Arc::new(
        rejecting("an enumeration-hang fixture answers snapshots and enumeration")
            .on_snapshot(move |_, _principal| {
                let snapshot = Arc::clone(&snapshot);
                async move { Ok(SnapshotResolution::Present(publishable(snapshot))) }
            })
            .on_subscribe(|_| no_pushes())
            .on_principals(move |_| {
                let (release, enumerations) = (Arc::clone(&release), Arc::clone(&enumerations));
                async move {
                    enumerations.fetch_add(1, Ordering::AcqRel);
                    release.notified().await;
                    Ok(Some(vec![PRINCIPAL]))
                }
            }),
    )
}

/// Issue #59: an enumeration that never answers is abandoned at
/// `enumeration_timeout`, so the loop keeps sweeping.
///
/// #78 raced this call against the shutdown watch, which stopped a wedged
/// enumeration holding shutdown open — but nothing else escaped it. During
/// ordinary operation the loop stayed parked in `discover`, stopped sweeping,
/// and never recovered. That is the shape #103 fixed for fetches, on the one
/// source call it did not cover.
///
/// The counter is the evidence the loop kept running: a manager parked inside
/// one enumeration never starts a second.
#[tokio::test(start_paused = true)]
async fn a_hung_enumeration_is_abandoned_so_the_loop_keeps_sweeping() {
    let release = Arc::new(tokio::sync::Notify::new());
    let enumerations = Arc::new(AtomicUsize::new(0));
    let fixture = spawn_with(
        hangs_on_enumeration_source(snapshot(1, PermissionBits::bit(0)), &release, &enumerations),
        SnapshotManagerConfig {
            // `All` is what makes the manager enumerate at all.
            principals: TrackedPrincipals::All {
                seed: vec![PRINCIPAL],
            },
            refresh_interval: std::time::Duration::from_millis(50),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 4,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_millis(100),
        },
    );
    let counters = fixture.manager.counters();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let attempted = enumerations.load(Ordering::Acquire);
    assert!(
        attempted >= 2,
        "the loop must keep sweeping and re-enumerating, got {attempted} attempts"
    );
    let stats = counters.snapshot();
    assert!(
        stats.discovery_failures >= 2,
        "an abandoned enumeration is a discovery failure, so a frozen tracked \
         set is visible to a scrape; got {}",
        stats.discovery_failures
    );
    assert!(
        stats.refresh_attempts > 0,
        "the seeded principal must still be refreshed while enumeration is wedged"
    );

    release.notify_waiters();
    fixture.manager.shutdown().await;
}

/// INVARIANTS.md #16: no config field is silently repaired.
#[test]
fn a_zero_enumeration_timeout_is_rejected() {
    let config = SnapshotManagerConfig {
        principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
        refresh_interval: std::time::Duration::from_millis(20),
        unknown_ttl: SignedDuration::from_secs(30),
        revoked_ttl: SignedDuration::from_secs(3_600),
        retry_backoff: std::time::Duration::from_millis(5),
        max_concurrent_fetches: 4,
        fetch_timeout: std::time::Duration::from_secs(5),
        enumeration_timeout: std::time::Duration::ZERO,
    };
    assert_eq!(
        config.validate().unwrap_err().to_string(),
        "enumeration_timeout must be positive"
    );
}

/// INVARIANTS.md #18, issue #78's sibling: no snapshot-source call carries a
/// wall-clock timeout — the manager bounds them by cancellation — so every
/// loop-body await must be raced against the shutdown watch. Enumeration was
/// the one that was not, which let a source that hangs on `principals()`
/// hold shutdown open indefinitely.
#[tokio::test(start_paused = true)]
async fn a_hung_enumeration_does_not_hold_shutdown_open() {
    let fixture = spawn_with(
        hanging_catalogue_source(snapshot(1, PermissionBits::bit(0))),
        SnapshotManagerConfig {
            // `All` is what makes the manager enumerate at all.
            principals: TrackedPrincipals::All { seed: Vec::new() },
            refresh_interval: std::time::Duration::from_millis(20),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 4,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
        },
    );
    settle().await;

    let report = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.manager.shutdown(),
    )
    .await
    .expect("a hung enumeration must not hold shutdown open");
    assert!(!report.task_died, "the task returned rather than hanging");
}

/// A source that hangs on one principal until released, and answers every
/// other principal normally.
/// Blocks on one principal and answers the rest.
fn hangs_on_one_source(
    snapshot: Arc<AccountSnapshot>,
    hung: Principal,
    release: &Arc<tokio::sync::Notify>,
    answered: &Arc<AtomicUsize>,
) -> Arc<DelegatingStore<RejectingStore>> {
    let (release, answered) = (Arc::clone(release), Arc::clone(answered));
    Arc::new(
        bare_source("a one-hang fixture answers snapshots and nothing else").on_snapshot(
            move |_, principal| {
                let (snapshot, release, answered) = (
                    Arc::clone(&snapshot),
                    Arc::clone(&release),
                    Arc::clone(&answered),
                );
                async move {
                    if principal == hung {
                        release.notified().await;
                    }
                    answered.fetch_add(1, Ordering::AcqRel);
                    Ok(SnapshotResolution::Present(publishable(snapshot)))
                }
            },
        ),
    )
}

/// Issue #103: a fetch that never resolves is abandoned at `fetch_timeout`,
/// so the sweep returns and the loop keeps running. Before this, one hung
/// fetch left `refresh_all_cancellable` waiting on a `JoinSet` that could
/// never empty: no tick, push, or control wakeup was processed again for the
/// life of the process, and readiness fell without ever recovering.
///
/// The recovery half is the point. `readiness_falls_if_refresh_hangs_across_snapshot_expiry`
/// already pins that readiness *falls*; both existing hung-source tests then
/// shut down, so nothing asserted the instance could come back.
#[tokio::test(start_paused = true)]
async fn a_hung_fetch_is_abandoned_so_the_sweep_keeps_running() {
    let release = Arc::new(tokio::sync::Notify::new());
    let answered = Arc::new(AtomicUsize::new(0));
    let hung = Principal(99);
    let fixture = spawn_with(
        hangs_on_one_source(
            snapshot(1, PermissionBits::bit(0)),
            hung,
            &release,
            &answered,
        ),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL, hung]),
            refresh_interval: std::time::Duration::from_millis(50),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_millis(100),
            enumeration_timeout: std::time::Duration::from_secs(30),
        },
    );
    let counters = fixture.manager.counters();

    // The loop must keep sweeping while one principal never answers: the
    // attempt counter is the code's own stated signal that it has not stopped.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let during = counters.snapshot();
    assert!(
        during.refresh_timeouts >= 2,
        "the hung principal must be abandoned repeatedly, got {}",
        during.refresh_timeouts
    );
    assert!(
        during.refresh_attempts > during.refresh_timeouts,
        "the answering principal must keep being refreshed alongside it"
    );
    assert_eq!(
        during.refresh_failures, 0,
        "a timeout is not a refusal and must not be counted as one"
    );

    // Recovery: once the source answers, the instance comes back without a
    // restart — the half neither existing hung-source test covers.
    release.notify_waiters();
    let recovered = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut ready = fixture.manager.ready();
        while !*ready.borrow() {
            ready.changed().await.unwrap();
        }
    })
    .await;
    recovered.expect("readiness must recover once the source answers");
    assert!(
        answered.load(Ordering::Acquire) > 0,
        "the recovered fetch must have been re-attempted"
    );

    fixture.manager.shutdown().await;
}

/// Answers `Unknown` once — putting the principal into a negative resolution
/// with a live refetch deadline — and hangs on every fetch after that.
/// Answers `Unknown` once, then never answers again.
fn unknown_then_hangs_source(calls: &Arc<AtomicUsize>) -> Arc<DelegatingStore<RejectingStore>> {
    let calls = Arc::clone(calls);
    Arc::new(
        bare_source("an unknown-then-hangs fixture answers snapshots and nothing else")
            .on_snapshot(move |_, _principal| {
                let calls = Arc::clone(&calls);
                async move {
                    if calls.fetch_add(1, Ordering::AcqRel) == 0 {
                        return Ok(SnapshotResolution::Unknown);
                    }
                    std::future::pending().await
                }
            }),
    )
}

/// Issue #103 meets issue #53: an abandoned fetch must land in the failed set
/// that re-arms `next_refetch`, exactly as a refusal does.
///
/// `back_off` only moves a principal already holding a negative resolution,
/// so a hung fetch on a *never-resolved* principal cannot reach it — which is
/// why this needs its own fixture rather than an extra assertion on the
/// abandonment test. Marking the abandoned principal completed instead leaves
/// its deadline in the past, the control wakeup re-fires at zero delay, and
/// the manager refetches at source latency: the shape #53 measured at 410
/// fetches in 600 ms.
///
/// Real time and a real clock, like `readiness_falls_if_refresh_hangs_across_snapshot_expiry`:
/// the retry deadline is a business-clock timestamp, so a manual clock that
/// never advances would never make one due and the test would pass while
/// measuring nothing.
#[tokio::test]
async fn an_abandoned_fetch_is_throttled_like_a_refusal() {
    let calls = Arc::new(AtomicUsize::new(0));
    let manager = SnapshotManager::spawn(
        unknown_then_hangs_source(&calls),
        Arc::new(ArcSwapSnapshotMap::new()),
        SlotRegistry::new(),
        Arc::new(SystemClock),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            // Long, so the periodic sweep is not what drives the retries:
            // a negative principal is skipped by `due_for_sweep`, and the
            // control wakeup is the path #53 was measured on.
            refresh_interval: std::time::Duration::from_secs(10),
            unknown_ttl: SignedDuration::from_millis(50),
            revoked_ttl: SignedDuration::from_secs(3_600),
            // Well above `fetch_timeout`, so a throttled retry and an
            // unthrottled one are an order of magnitude apart rather than
            // both being paced by the abandonment itself.
            retry_backoff: std::time::Duration::from_millis(300),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_millis(20),
            enumeration_timeout: std::time::Duration::from_secs(30),
        },
    )
    .unwrap();
    let counters = manager.counters();

    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;

    let stats = counters.snapshot();
    assert!(
        stats.refresh_timeouts >= 2,
        "the hung principal must keep being retried, got {}",
        stats.refresh_timeouts
    );
    // Paced by `retry_backoff` (300ms), not by `fetch_timeout` (20ms):
    // roughly four attempts across this window rather than roughly sixty.
    assert!(
        stats.refresh_timeouts <= 12,
        "abandoned fetches are spinning rather than backing off: {} in 1.2s",
        stats.refresh_timeouts
    );

    manager.shutdown().await;
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
        unknown_ttl: SignedDuration::from_secs(30),
        revoked_ttl: SignedDuration::from_secs(3_600),
        retry_backoff: std::time::Duration::from_millis(5),
        max_concurrent_fetches: 4,
        fetch_timeout: std::time::Duration::from_secs(5),
        enumeration_timeout: std::time::Duration::from_secs(30),
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
        no_catalogue_source(snapshot(1, PermissionBits::bit(0))),
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
        failing_catalogue_source(snapshot(1, PermissionBits::bit(0)), &failures),
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
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot(1, PermissionBits::bit(0))))
        .expect("snapshot fixture matches its account and credential");
    let fixture = spawn_with(store.clone(), discovering_config(600_000));
    stock_slot(&fixture);
    settle().await;
    assert_eq!(
        admit_as(&fixture, LATER_PRINCIPAL),
        Err(DenyReason::UnknownPrincipal)
    );

    store
        .publish_snapshot(
            LATER_PRINCIPAL,
            publishable(snapshot(1, PermissionBits::bit(0))),
        )
        .expect("snapshot fixture matches its account and credential");
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
    let fixture = spawn_with(
        Arc::new(
            rejecting("an all-fail fixture answers snapshots and enumeration")
                .on_snapshot(|_, _principal| async {
                    Err(StoreError("injected total source outage".into()))
                })
                .on_subscribe(|_| no_pushes())
                .on_principals(|_| async { Ok(Some(vec![PRINCIPAL, LATER_PRINCIPAL])) }),
        ),
        discovering_config(20),
    );
    stock_slot(&fixture);
    settle().await;

    assert!(
        !*fixture.manager.ready().borrow(),
        "two principals tracked and neither resolvable: this instance serves nobody"
    );
}

// ---- churned catalogues (#52) ---------------------------------------------

/// A source that counts fetches per principal and can be flipped between
/// serving and revoking, so a test can watch what a sweep actually asks for.
/// Counts pulls per half of a two-principal catalogue, one of which the test
/// can revoke.
struct CountingSource {
    revoked: AtomicBool,
    live_calls: AtomicUsize,
    dead_calls: AtomicUsize,
}

impl CountingSource {
    /// The handle and the source to hand [`SnapshotManager::spawn`].
    fn new() -> (Arc<Self>, Arc<DelegatingStore<RejectingStore>>) {
        let source = Arc::new(CountingSource {
            revoked: AtomicBool::new(false),
            live_calls: AtomicUsize::new(0),
            dead_calls: AtomicUsize::new(0),
        });
        let snapshot = snapshot(1, PermissionBits::bit(0));
        let pulls = Arc::clone(&source);
        (
            source,
            Arc::new(
                rejecting("a counting fixture answers snapshots and enumeration")
                    .on_snapshot(move |_, principal| {
                        let (source, snapshot) = (Arc::clone(&pulls), Arc::clone(&snapshot));
                        async move {
                            if principal == LATER_PRINCIPAL {
                                // The permanently-dead half of the catalogue.
                                source.dead_calls.fetch_add(1, Ordering::AcqRel);
                                return Ok(SnapshotResolution::Revoked {
                                    generation: Generation(9),
                                });
                            }
                            source.live_calls.fetch_add(1, Ordering::AcqRel);
                            if source.revoked.load(Ordering::Acquire) {
                                Ok(SnapshotResolution::Revoked {
                                    generation: Generation(9),
                                })
                            } else {
                                Ok(SnapshotResolution::Present(publishable(snapshot)))
                            }
                        }
                    })
                    .on_subscribe(|_| no_pushes())
                    .on_principals(|_| async { Ok(Some(vec![PRINCIPAL, LATER_PRINCIPAL])) }),
            ),
        )
    }
}

fn churn_config(revoked_ttl: SignedDuration) -> SnapshotManagerConfig {
    SnapshotManagerConfig {
        principals: TrackedPrincipals::All { seed: Vec::new() },
        refresh_interval: std::time::Duration::from_millis(10),
        unknown_ttl: SignedDuration::from_millis(10),
        revoked_ttl,
        retry_backoff: std::time::Duration::from_millis(5),
        max_concurrent_fetches: 4,
        fetch_timeout: std::time::Duration::from_secs(5),
        enumeration_timeout: std::time::Duration::from_secs(30),
    }
}

/// **The one that matters.** Withdrawing a principal must still propagate
/// within `refresh_interval`, however long `revoked_ttl` is — a live principal
/// is always swept, so revocation never rides the tombstone schedule. If this
/// is wrong, #52 traded away the wrong direction.
#[tokio::test(start_paused = true)]
async fn revocation_still_propagates_within_the_refresh_interval() {
    let (source, pulls) = CountingSource::new();
    let fixture = spawn_with(
        Arc::clone(&pulls) as Arc<dyn SnapshotSource>,
        // A revoked TTL far longer than the test could ever wait.
        churn_config(SignedDuration::from_secs(86_400)),
    );
    stock_slot(&fixture);
    settle().await;
    assert_eq!(admit_as(&fixture, PRINCIPAL), Ok(()));

    source.revoked.store(true, Ordering::Release);
    settle().await;

    assert_eq!(
        admit_as(&fixture, PRINCIPAL),
        Err(DenyReason::UnknownPrincipal),
        "a live principal is swept every refresh, so withdrawing it lands there"
    );
}

/// The saving: a tombstone is fetched on its own schedule, not on every sweep
/// as well. With a revoked TTL longer than the test, the dead half of the
/// catalogue is fetched once — during the initial load — while the live half
/// keeps being refreshed.
#[tokio::test(start_paused = true)]
async fn the_sweep_does_not_refetch_tombstones() {
    let (source, pulls) = CountingSource::new();
    let fixture = spawn_with(
        Arc::clone(&pulls) as Arc<dyn SnapshotSource>,
        churn_config(SignedDuration::from_secs(86_400)),
    );
    stock_slot(&fixture);
    settle().await;

    let dead = source.dead_calls.load(Ordering::Acquire);
    let live = source.live_calls.load(Ordering::Acquire);
    assert_eq!(
        dead, 1,
        "the tombstone is resolved once and then left to its own TTL"
    );
    assert!(
        live > dead,
        "while the live principal keeps being swept: {live} live vs {dead} dead"
    );
}

/// A principal the instance has served, whose row then goes *absent*, must
/// recover on `unknown_ttl` — not sit stranded for `revoked_ttl`.
///
/// `Unknown` is not a withdrawal. A source that is restarting, failing over,
/// or serving a lagging replica reports principals it has served for years as
/// absent, and an earlier cut of #52 keyed the TTL on the locally remembered
/// generation, so exactly those principals inherited the hour-long
/// reinstatement TTL. With the sweep no longer covering negatives and
/// `subscribe` closed on the HTTP transport, nothing else would have repaired
/// them: the instance denies every request for an hour while readiness still
/// reports healthy, because a negative counts as resolved.
///
/// The refresh interval here is far longer than the wait, so passing requires
/// the *targeted* refetch to have fired on the short TTL.
#[tokio::test]
async fn a_live_principal_that_goes_absent_recovers_on_the_unknown_ttl() {
    let (source, pulls) = MutableNoPushSource::new();
    source.set(MutableMode::Present(snapshot(1, PermissionBits::bit(0))));
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let manager = SnapshotManager::spawn(
        pulls.clone(),
        map.clone(),
        SlotRegistry::new(),
        Arc::new(SystemClock),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            // Short enough to carry Present -> Unknown, long enough that it
            // cannot be what carries Unknown -> Present back.
            refresh_interval: std::time::Duration::from_millis(30),
            unknown_ttl: SignedDuration::from_millis(40),
            // An hour: if the absence took this TTL, the test would time out.
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
        },
    )
    .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the principal must be served before its row disappears");

    // The source loses the row without ever publishing a tombstone.
    source.set(MutableMode::Unknown);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("a sweep must observe the absence");

    // The source comes back with the *same* snapshot it always had. Nothing
    // changed while the row was missing, so nothing bumped the generation --
    // that is what a transient absence looks like, and it must be enough to
    // restore the principal (#53). Requiring a higher generation here would
    // mean a customer stays denied until someone happens to republish.
    source.set(MutableMode::Present(snapshot(1, PermissionBits::bit(0))));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("an absent row must recover at its own generation, not only above it");

    manager.shutdown().await;
}

/// `revoked_ttl` must actually drive a refetch, so a reinstated principal
/// comes back without a restart.
///
/// The sibling tests pin the sweep *filter* under a `ManualClock` that never
/// advances, so no TTL elapses in them and `revoked_ttl` could have been wired
/// to nothing at all. This is the end-to-end half: a real clock, a tombstone,
/// and a reinstatement that only the tombstone's own schedule can deliver.
#[tokio::test]
async fn a_reinstated_principal_comes_back_on_the_revoked_ttl() {
    let (source, pulls) = MutableNoPushSource::new();
    source.set(MutableMode::Revoked);
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let manager = SnapshotManager::spawn(
        pulls.clone(),
        map.clone(),
        SlotRegistry::new(),
        Arc::new(SystemClock),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            // Longer than the wait below: the sweep cannot be what recovers it,
            // and it would skip this principal anyway once it is negative.
            refresh_interval: std::time::Duration::from_secs(60),
            // Longer than the wait too, so passing pins `revoked_ttl`
            // specifically rather than whichever TTL happens to be shorter.
            unknown_ttl: SignedDuration::from_secs(60),
            revoked_ttl: SignedDuration::from_millis(40),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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
    .expect("the tombstone must resolve the initial load");
    assert!(
        !matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))),
        "a revoked principal is not served"
    );

    // Above the tombstone's generation: reinstatement is a *higher* publish,
    // and INVARIANTS #15 refuses anything at or below it.
    source.set(MutableMode::Present(snapshot(10, PermissionBits::bit(0))));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("revoked_ttl must schedule the refetch that finds the reinstatement");

    manager.shutdown().await;
}

/// A snapshot whose validity lapses at `valid_until`, for tests that care what
/// deadline the manager adopts rather than what generation it serves.
fn expiring_snapshot(generation: u64, valid_until: Timestamp) -> Arc<AccountSnapshot> {
    let mut snapshot = AccountSnapshot::clone(&snapshot(generation, PermissionBits::bit(0)));
    snapshot.valid_until = valid_until;
    Arc::new(snapshot)
}

/// A stale positive cannot drag readiness down.
///
/// It is tempting to think the manager's generation gate is redundant, since
/// the map refuses a rolled-back generation anyway and admission is therefore
/// safe either way. It is not redundant: the manager *also* records the
/// accepted snapshot's `valid_until` as its own deadline, and readiness is
/// computed from those deadlines. Accepting a stale snapshot adopts a stale
/// deadline, so an instance that is serving perfectly well from the newer
/// snapshot would withdraw itself from rotation.
///
/// Mutation testing is what surfaced this: `Resolutions::accepts_positive`
/// could be replaced with `true` and every other test stayed green, because
/// they all observe the *map*, which protects itself.
#[tokio::test]
async fn a_stale_positive_cannot_drop_readiness() {
    let (source, pulls) = MutableNoPushSource::new();
    source.set(MutableMode::Present(snapshot(5, PermissionBits::bit(0))));
    let clock = Arc::new(ManualClock::new(t(0)));
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let manager = SnapshotManager::spawn(
        pulls.clone(),
        map.clone(),
        SlotRegistry::new(),
        clock,
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_millis(10),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(5),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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
    .expect("the live snapshot must make the instance ready");

    // An older generation whose validity has *already* lapsed at the manual
    // clock's instant. Refusing it keeps the generation-5 deadline; accepting
    // it would adopt an expired one.
    source.set(MutableMode::Present(expiring_snapshot(4, t(0))));
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    assert!(
        *manager.ready().borrow(),
        "a refused stale snapshot must not unready an instance that is still serving"
    );
    assert!(
        matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))),
        "and the newer snapshot is still installed"
    );

    manager.shutdown().await;
}

/// #53 on the **push** path: a principal that goes absent and is then
/// reinstated by a push at its original generation must come back.
///
/// The sweep half is pinned by
/// `a_live_principal_that_goes_absent_recovers_on_the_unknown_ttl`. This is its
/// counterpart, and it exists because mutation probing showed the push site's
/// `visible` read could be replaced with `true` — reinstating #53 on the path
/// most deployments actually use — with the whole suite still green.
#[tokio::test]
async fn a_push_reinstates_an_absent_principal_at_its_own_generation() {
    let live = SnapshotResolution::Present(publishable(snapshot(5, PermissionBits::bit(0))));
    let (source, pulls) = DrivenPushSource::new(live.clone());
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let manager = SnapshotManager::spawn(
        pulls.clone(),
        map.clone(),
        SlotRegistry::new(),
        Arc::new(ManualClock::new(t(0))),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            // Long: the sweep must not be what carries either transition, or
            // this would silently retest the sweep path.
            refresh_interval: std::time::Duration::from_secs(3_600),
            unknown_ttl: SignedDuration::from_secs(3_600),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_secs(3_600),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
        },
    )
    .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the initial load must install the principal");

    // The row disappears, announced by push rather than discovered by a sweep.
    source.send(PRINCIPAL, SnapshotResolution::Unknown);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the absence must reach the map");

    // And comes back, unchanged, at the generation it always had.
    source.set_pull(live.clone());
    source.send(PRINCIPAL, live);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("a push must reinstate an absent principal at its own generation");

    assert_eq!(manager.counters().snapshot().refused_updates, 0);
    manager.shutdown().await;
}

/// A refused answer must stay *throttled*.
///
/// Refusing is correct when a source offers a generation this instance already
/// holds a revocation for — a lagging replica, say. What must not happen is
/// refusing at source latency: the refusal has to re-arm the retry deadline, or
/// the control wakeup re-fires at zero delay and the client hammers the store
/// for as long as the replica lags.
///
/// This exists because that is exactly what a fix for a *reporting* complaint
/// did: moving refusals out of the set that drives `back_off` made a throttled
/// retry unthrottled — 410 fetches in this window instead of 3. Nothing caught
/// it, because the suite pinned what the refusal *decided* and never how often
/// it was retried.
#[tokio::test]
async fn a_refused_answer_is_retried_with_backoff_not_at_source_latency() {
    let (source, pulls) = MutableNoPushSource::new();
    source.set(MutableMode::Revoked);
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let manager = SnapshotManager::spawn(
        pulls.clone(),
        map.clone(),
        SlotRegistry::new(),
        Arc::new(SystemClock),
        SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            // Only the tombstone's own schedule may drive a refetch here.
            refresh_interval: std::time::Duration::from_secs(3_600),
            unknown_ttl: SignedDuration::from_secs(3_600),
            revoked_ttl: SignedDuration::from_millis(40),
            retry_backoff: std::time::Duration::from_millis(200),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
        },
    )
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // A lagging replica answers with the very generation the tombstone holds.
    source.set(MutableMode::Present(snapshot(9, PermissionBits::bit(0))));
    let before = source.calls.load(Ordering::Acquire);
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let fetches = source.calls.load(Ordering::Acquire) - before;

    // 600ms / 200ms of backoff is a handful; the unthrottled loop was ~400.
    assert!(
        fetches <= 12,
        "a refused answer must be retried on the backoff, not spun: {fetches} fetches"
    );
    assert!(
        matches!(
            map.get(&PRINCIPAL),
            Some(MapEntry::NegativeUntil { .. }) | None
        ),
        "and the refusal still holds: the revoked principal is not resurrected"
    );

    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn reclaimed_principals_refetch_authority_instead_of_replaying_a_push() {
    for map in [
        Arc::new(ArcSwapSnapshotMap::with_capacities(
            LocalSharding::SINGLE,
            1,
            NonZeroUsize::new(1).unwrap(),
        )) as Arc<dyn SnapshotMap>,
        Arc::new(tollgate_admission::MokaSnapshotMap::with_capacities(
            8,
            LocalSharding::SINGLE,
            NonZeroUsize::new(1).unwrap(),
        )),
    ] {
        let (source, pulls) = DrivenPushSource::new(SnapshotResolution::Revoked {
            generation: Generation(9),
        });
        let slots = SlotRegistry::new();
        let original_slot = slots.slot(ACCOUNT);
        let mut config = discovering_config(3_600_000);
        config.max_concurrent_fetches = 8;
        let manager = SnapshotManager::spawn(
            pulls.clone(),
            map.clone(),
            slots.clone(),
            Arc::new(ManualClock::new(t(0))),
            config,
        )
        .unwrap();
        settle().await;
        assert!(matches!(
            map.get(&PRINCIPAL),
            Some(MapEntry::NegativeUntil { .. })
        ));

        let later = Principal(8);
        let live = SnapshotResolution::Present(publishable(snapshot(1, PermissionBits::bit(0))));
        source.set_pull(live.clone());
        source.send(later, live);
        settle().await;
        assert!(
            map.get(&PRINCIPAL).is_none(),
            "history reclamation removes its visible tombstone"
        );
        assert!(matches!(map.get(&later), Some(MapEntry::Present(_))));
        assert_eq!(manager.counters().snapshot().unresolved, 1);
        assert_eq!(manager.counters().snapshot().history_evictions, 1);

        source.set_pull(SnapshotResolution::Revoked {
            generation: Generation(9),
        });
        // This message is still valid at the caller's time, but the source has
        // revoked it. The forgotten principal's push can only request a pull.
        source.send(
            PRINCIPAL,
            SnapshotResolution::Present(publishable(snapshot(9, PermissionBits::ALL))),
        );
        settle().await;
        assert!(matches!(
            map.get(&PRINCIPAL),
            Some(MapEntry::NegativeUntil { .. })
        ));
        assert!(map.get(&later).is_none());
        assert_eq!(map.history_stats().unwrap().retained, 1);
        assert_eq!(manager.counters().snapshot().history_evictions, 2);
        assert_eq!(manager.counters().snapshot().unresolved, 1);
        assert!(
            Arc::ptr_eq(&original_slot, &slots.slot(ACCOUNT)),
            "history eviction must preserve irreversible account spend state"
        );
        manager.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_discovered_catalogue_larger_than_history_is_refreshed_in_bounded_batches() {
    let (_source, pulls) = CountingSource::new();
    let map = Arc::new(ArcSwapSnapshotMap::with_capacities(
        LocalSharding::SINGLE,
        1,
        NonZeroUsize::new(1).unwrap(),
    ));
    let mut config = churn_config(SignedDuration::from_secs(3_600));
    config.refresh_interval = std::time::Duration::from_secs(3_600);
    let manager = SnapshotManager::spawn(
        pulls,
        map.clone(),
        SlotRegistry::new(),
        Arc::new(ManualClock::new(t(0))),
        config,
    )
    .unwrap();
    settle().await;
    assert_eq!(map.history_stats().unwrap().retained, 1);
    assert_eq!(manager.counters().snapshot().history_evictions, 1);
    assert_eq!(manager.counters().snapshot().publication_failures, 0);
    assert_eq!(manager.counters().snapshot().refresh_attempts, 2);
    assert_eq!(manager.counters().snapshot().unresolved, 1);
    assert!(*manager.ready().borrow());
    manager.shutdown().await;
}

#[tokio::test]
async fn a_fixed_set_larger_than_history_is_rejected_before_tasks_start() {
    let (_source, pulls) = DrivenPushSource::new(SnapshotResolution::Unknown);
    let map = Arc::new(ArcSwapSnapshotMap::with_capacities(
        LocalSharding::SINGLE,
        1,
        NonZeroUsize::new(1).unwrap(),
    ));
    let mut config = discovering_config(1000);
    config.principals = TrackedPrincipals::Fixed(vec![Principal(1), Principal(2)]);
    let error = SnapshotManager::spawn(
        pulls,
        map.clone(),
        SlotRegistry::new(),
        Arc::new(ManualClock::new(t(0))),
        config,
    )
    .err()
    .expect("oversized fixed set must be rejected");
    assert_eq!(
        error.0,
        "fixed principals exceed snapshot generation capacity"
    );
    assert_eq!(map.history_stats().unwrap().retained, 0);
}

#[tokio::test(start_paused = true)]
async fn same_generation_refresh_repairs_a_visible_cache_eviction() {
    for map in [
        Arc::new(ArcSwapSnapshotMap::new()) as Arc<dyn SnapshotMap>,
        Arc::new(tollgate_admission::MokaSnapshotMap::new(8)),
    ] {
        let (source, pulls) = MutableNoPushSource::new();
        source.set(MutableMode::Present(snapshot(5, PermissionBits::bit(0))));
        let mut config = discovering_config(1000);
        config.principals = TrackedPrincipals::Fixed(vec![PRINCIPAL]);
        let manager = SnapshotManager::spawn(
            pulls,
            map.clone(),
            SlotRegistry::new(),
            Arc::new(ManualClock::new(t(0))),
            config,
        )
        .unwrap();
        settle().await;
        assert!(matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))));
        map.remove(&PRINCIPAL);
        assert!(!map.contains_cached(&PRINCIPAL));
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        settle().await;
        let Some(MapEntry::Present(state)) = map.get(&PRINCIPAL) else {
            panic!("equal-generation refresh must repair a visible eviction")
        };
        assert_eq!(state.snapshot.generation, Generation(5));
        assert_eq!(manager.counters().snapshot().refused_updates, 0);
        manager.shutdown().await;
    }
}

/// Records the size of every reservation a refresh pass makes.
///
/// Still hand-written after #83: `SnapshotMap` belongs to `tollgate-admission`,
/// so a delegating double for it is a second shared module in a second crate
/// for one call site. Its seven inherited defaults are all written over `Self`
/// methods this double does override, so nothing is bypassed. #120 tracks it. The pass must
/// reserve at the retention budget: reserving one principal at a time would
/// still resolve the catalogue, but it would pay a reservation, a publication
/// and a concurrency window per principal instead of per budget-sized chunk.
struct ReservationSizes {
    inner: ArcSwapSnapshotMap,
    sizes: Mutex<Vec<usize>>,
}

impl ReservationSizes {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: ArcSwapSnapshotMap::with_capacities(
                LocalSharding::SINGLE,
                capacity,
                NonZeroUsize::new(capacity).unwrap(),
            ),
            sizes: Mutex::new(Vec::new()),
        })
    }

    fn recorded(&self) -> Vec<usize> {
        self.sizes
            .lock()
            .expect("reservation sizes poisoned")
            .clone()
    }
}

impl SnapshotMap for ReservationSizes {
    fn prepare_refreshes(
        &self,
        principals: &[Principal],
    ) -> Result<tollgate_admission::RefreshBatch, tollgate_admission::PublicationError> {
        self.sizes
            .lock()
            .expect("reservation sizes poisoned")
            .push(principals.len());
        self.inner.prepare_refreshes(principals)
    }
    fn apply_refreshed_many_at(
        &self,
        updates: Vec<tollgate_admission::Refreshed<tollgate_admission::PublishableSnapshotUpdate>>,
        now: Timestamp,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.apply_refreshed_many_at(updates, now)
    }
    fn generation_capacity(&self) -> NonZeroUsize {
        self.inner.generation_capacity()
    }
    fn needs_refresh(&self, principal: Principal) -> bool {
        self.inner.needs_refresh(principal)
    }
    fn contains_cached(&self, principal: &Principal) -> bool {
        self.inner.contains_cached(principal)
    }
    fn history_stats(&self) -> Option<tollgate_admission::SnapshotHistoryStats> {
        self.inner.history_stats()
    }
    fn local_sharding(&self) -> LocalSharding {
        self.inner.local_sharding()
    }
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        self.inner.get(principal)
    }
    fn get_at(&self, principal: &Principal, locality: tollgate_core::Locality) -> Option<MapEntry> {
        self.inner.get_at(principal, locality)
    }
    fn counters(&self) -> &Arc<tollgate_admission::AdmissionCounters> {
        self.inner.counters()
    }
    fn install(
        &self,
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
        lease: Arc<tollgate_admission::LeaseSlot>,
    ) -> Result<(), tollgate_admission::PublicationError> {
        self.inner.install(principal, snapshot, lease)
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
}

#[tokio::test(start_paused = true)]
async fn a_refresh_pass_reserves_at_the_retention_budget_not_per_principal() {
    let (source, pulls) = MutableNoPushSource::new();
    source.set(MutableMode::Present(snapshot(1, PermissionBits::bit(0))));
    let map = ReservationSizes::new(3);
    let mut config = discovering_config(3_600_000);
    config.principals = TrackedPrincipals::Fixed(vec![Principal(1), Principal(2), Principal(3)]);
    let manager = SnapshotManager::spawn(
        pulls,
        map.clone(),
        SlotRegistry::new(),
        Arc::new(ManualClock::new(t(0))),
        config,
    )
    .unwrap();
    settle().await;
    assert_eq!(
        map.recorded(),
        vec![3],
        "the whole budget-sized set belongs to one reservation"
    );
    assert_eq!(map.history_stats().unwrap().retained, 3);
    manager.shutdown().await;
}

/// A push *names* a principal this instance has never resolved; it does not
/// carry authority for one. Discovery therefore fetches, and the source's
/// answer is what lands -- so a push that has been superseded, replayed, or
/// simply raced by a revocation cannot install a snapshot the source no longer
/// publishes. Only a principal this instance already holds may be advanced by
/// the push's own payload.
#[tokio::test(start_paused = true)]
async fn a_push_for_an_unresolved_principal_is_discovery_not_authority() {
    let (source, pulls) = DrivenPushSource::new(SnapshotResolution::Revoked {
        generation: Generation(9),
    });
    let fixture = spawn_with(pulls.clone(), discovering_config(600_000));
    stock_slot(&fixture);
    settle().await;
    assert_eq!(
        admit_as(&fixture, LATER_PRINCIPAL),
        Err(DenyReason::UnknownPrincipal)
    );

    source.send(
        LATER_PRINCIPAL,
        SnapshotResolution::Present(publishable(snapshot(5, PermissionBits::bit(0)))),
    );
    settle().await;

    assert_eq!(
        admit_as(&fixture, LATER_PRINCIPAL),
        Err(DenyReason::UnknownPrincipal),
        "the authoritative read decides a newly discovered principal, not the push that named it"
    );
}
