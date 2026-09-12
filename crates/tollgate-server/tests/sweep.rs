//! The reclaim sweep is INVARIANTS.md #9's server half: the only thing that
//! returns units stranded by a crashed holder.
//!
//! The store's own reclaim is covered by both backend suites; what is tested
//! here is that the *server* actually invokes it on its interval, and that a
//! sweep failing every tick says so — `/readyz` cannot see it, because a
//! store can answer `ping` and still fail `reclaim_expired` (issue #36).
//!
//! As in `loopback.rs`, the oneshot teardown signals here are outside the
//! discard rule this MR installs: the assertions below are what fail if
//! shutdown misbehaves.
#![allow(clippy::let_underscore_must_use)]

mod common;

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Once};

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};

use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits};
use tollgate_store::{
    AccountConfig, AdminStore, DEFAULT_RECLAIM_BATCH_LIMIT, GrantPolicy, LeaseAllocator,
    MemoryStore, ReclaimBatch, StoreError, StoreHealth, SystemClock,
};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt as _};
use tracing_subscriber::registry::LookupSpan;

use tollgate_server::{ServerState, serve};

const ACCOUNT: AccountId = AccountId(1);

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

/// Wraps a real store, failing `reclaim_expired` a fixed number of times and
/// counting how often it was called.
struct FlakyReclaimStore {
    inner: Arc<MemoryStore>,
    failures_left: AtomicU32,
    fail_on_call: Option<u32>,
    fail_rollover: bool,
    calls: AtomicU32,
}

#[async_trait]
impl tollgate_store::KeySource for FlakyReclaimStore {
    async fn active_keys_page(
        &self,
        now: Timestamp,
        after: Option<tollgate_core::KeyId>,
        limit: std::num::NonZeroUsize,
    ) -> Result<tollgate_store::KeyPage, StoreError> {
        tollgate_store::KeySource::active_keys_page(&*self.inner, now, after, limit).await
    }
}

#[async_trait]
impl LeaseAllocator for FlakyReclaimStore {
    async fn acquire(
        &self,
        account: AccountId,
        requested: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<tollgate_core::LeaseGrant, tollgate_store::AllocateError> {
        self.inner.acquire(account, requested, ttl, now).await
    }

    async fn release(
        &self,
        lease_id: tollgate_core::LeaseId,
        fencing_token: tollgate_core::FencingToken,
        unspent: CostUnits,
        now: Timestamp,
    ) -> Result<(), tollgate_store::AllocateError> {
        self.inner
            .release(lease_id, fencing_token, unspent, now)
            .await
    }

    async fn consolidate(
        &self,
        _lease_id: tollgate_core::LeaseId,
        _fencing_token: tollgate_core::FencingToken,
        _unspent: tollgate_core::CostUnits,
        _requested: tollgate_core::CostUnits,
        _ttl: jiff::SignedDuration,
        _now: jiff::Timestamp,
    ) -> Result<tollgate_core::LeaseGrant, tollgate_store::AllocateError> {
        unreachable!("the sweep never consolidates")
    }

    async fn reclaim_expired_batch(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel) + 1;
        if self.fail_on_call == Some(call) {
            return Err(StoreError(
                "backend failure: password=fixture-maintenance-sensitive-70".into(),
            ));
        }
        if self
            .failures_left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(StoreError(
                "backend failure: password=fixture-maintenance-sensitive-70".into(),
            ));
        }
        self.inner.reclaim_expired_batch(now, limit).await
    }
}

#[async_trait]
impl StoreHealth for FlakyReclaimStore {
    async fn ping(&self) -> Result<(), StoreError> {
        // Deliberately healthy: readiness cannot see the sweep failing.
        Ok(())
    }
}

#[async_trait]
impl tollgate_store::SnapshotSource for FlakyReclaimStore {
    async fn snapshot(
        &self,
        principal: tollgate_core::Principal,
    ) -> Result<tollgate_store::SnapshotResolution, StoreError> {
        self.inner.snapshot(principal).await
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<tollgate_store::SnapshotPush> {
        tollgate_store::SnapshotSource::subscribe(&*self.inner)
    }
}

#[async_trait]
impl tollgate_store::UsageSink for FlakyReclaimStore {
    async fn ingest(
        &self,
        events: &[tollgate_core::UsageEvent],
        now: Timestamp,
    ) -> Result<tollgate_store::IngestReport, tollgate_store::IngestError> {
        self.inner.ingest(events, now).await
    }
}

#[async_trait]
impl AdminStore for FlakyReclaimStore {
    async fn create_account(
        &self,
        config: AccountConfig,
    ) -> Result<tollgate_store::AdminReceipt<()>, tollgate_store::CreateAccountError> {
        AdminStore::create_account(&*self.inner, config).await
    }

    async fn deposit(
        &self,
        account: AccountId,
        units: CostUnits,
    ) -> Result<tollgate_store::AdminReceipt<()>, tollgate_store::AllocateError> {
        AdminStore::deposit(&*self.inner, account, units).await
    }

    async fn set_account_status(
        &self,
        account: AccountId,
        status: AccountStatus,
    ) -> Result<
        tollgate_store::AdminReceipt<tollgate_store::StatusChange>,
        tollgate_store::SetStatusError,
    > {
        AdminStore::set_account_status(&*self.inner, account, status).await
    }

    async fn set_capacity_class(
        &self,
        account: AccountId,
        class: CapacityClass,
    ) -> Result<
        tollgate_store::AdminReceipt<tollgate_store::StatusChange>,
        tollgate_store::SetStatusError,
    > {
        AdminStore::set_capacity_class(&*self.inner, account, class).await
    }

    async fn set_budget_schedule(
        &self,
        account: AccountId,
        schedule: Option<tollgate_core::BudgetSchedule>,
    ) -> Result<(), tollgate_store::BudgetError> {
        AdminStore::set_budget_schedule(&*self.inner, account, schedule).await
    }

    async fn roll_due_periods(
        &self,
        now: Timestamp,
        limit: std::num::NonZeroUsize,
    ) -> Result<tollgate_store::RolloverBatch, StoreError> {
        if self.fail_rollover {
            return Err(StoreError(
                "backend failure: password=fixture-maintenance-sensitive-70".into(),
            ));
        }
        AdminStore::roll_due_periods(&*self.inner, now, limit).await
    }

    async fn publish_snapshot(
        &self,
        principal: tollgate_core::Principal,
        snapshot: tollgate_core::PublishableSnapshot,
    ) -> Result<tollgate_store::AdminReceipt<()>, tollgate_store::PublishSnapshotError> {
        AdminStore::publish_snapshot(&*self.inner, principal, snapshot).await
    }

    async fn remove_snapshot(
        &self,
        principal: tollgate_core::Principal,
    ) -> Result<tollgate_store::AdminReceipt<()>, StoreError> {
        AdminStore::remove_snapshot(&*self.inner, principal).await
    }
}

#[derive(Debug, Clone)]
struct Captured {
    level: Level,
    fields: Vec<(String, String)>,
}

#[derive(Default, Clone)]
struct Captor(Arc<Mutex<Vec<Captured>>>);

struct FieldCollector(Vec<(String, String)>);

impl Visit for FieldCollector {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
}

thread_local! {
    /// The capturing test's buffer, if this thread is inside `capture()`.
    static SINK: RefCell<Option<Captor>> = const { RefCell::new(None) };
}

/// The one subscriber this binary ever installs, and it is installed
/// *globally* rather than per test with `tracing::subscriber::set_default`.
///
/// That is not a style choice. `tracing` caches each callsite's `Interest`
/// in a process-global slot, and computes it the first time any thread hits
/// that callsite — against *that* thread's current dispatcher
/// (`tracing_core::callsite::Rebuilder::JustOne` calls `get_default`). A
/// thread-local subscriber therefore does not make the decision thread-local:
/// one test reaching `reclaim_sweep`'s `info!` with no dispatcher installed
/// caches `Interest::never()` for the whole process, and every *other* test's
/// capture of that event silently returns nothing. That is invisible on a
/// developer box, where libtest runs one test per core, and reproducible on a
/// two-vCPU runner, where exactly two tests interleave.
///
/// A global dispatcher is what makes the failure unrepresentable: every
/// thread always has a real subscriber, so interest is never `never`, whether
/// or not the test that first reaches a callsite is capturing. Per-test
/// isolation moves to `SINK`, which is genuinely thread-local.
struct Router;

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Router {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        SINK.with(|sink| {
            let Some(captor) = sink.borrow().clone() else {
                return;
            };
            let mut collector = FieldCollector(Vec::new());
            event.record(&mut collector);
            captor.0.lock().unwrap().push(Captured {
                level: *event.metadata().level(),
                fields: collector.0,
            });
        });
    }
}

/// Install `Router` as the process-wide dispatcher. Idempotent, and called
/// from every path that can reach a callsite — see `Router` for why the
/// install must happen before the first emission rather than lazily.
fn install_subscriber() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Router))
            .expect("this binary installs the global subscriber exactly once");
    });
}

/// Capture this thread's events until the guard drops.
fn capture() -> (Captor, CaptureGuard) {
    install_subscriber();
    let captor = Captor::default();
    SINK.with(|sink| *sink.borrow_mut() = Some(captor.clone()));
    (captor, CaptureGuard)
}

/// Detaches this thread's sink, so a later test on the same libtest thread
/// starts from an empty buffer.
struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        SINK.with(|sink| *sink.borrow_mut() = None);
    }
}

fn store_with_expired_lease() -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(3_600),
        reclaim_grace: SignedDuration::ZERO,
    })
    .unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(1_000),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    store
}

/// The only way this binary starts a server, and therefore the only way it
/// can reach `reclaim_sweep`'s callsites. Installing the global subscriber
/// here is what makes the hazard described on `Router` unreachable: a test
/// that captures nothing still cannot be the first to register a callsite
/// against an absent dispatcher, because there is never an absent dispatcher.
async fn spawn_server(
    store: Arc<FlakyReclaimStore>,
    clock: Arc<dyn tollgate_store::Clock>,
    reclaim_interval: std::time::Duration,
) -> (
    tokio::task::JoinHandle<std::io::Result<()>>,
    tokio::sync::oneshot::Sender<()>,
) {
    install_subscriber();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(
        listener,
        ServerState {
            security: common::security(),
            store,
            clock,
        },
        reclaim_interval,
        async move {
            let _ = stop_rx.await;
        },
    ));
    (server, stop_tx)
}

/// Run `serve` against the given store for long enough to sweep, then stop.
async fn serve_briefly(store: Arc<FlakyReclaimStore>, clock: Arc<dyn tollgate_store::Clock>) {
    let (server, stop_tx) = spawn_server(store, clock, std::time::Duration::from_millis(10)).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let _ = stop_tx.send(());
    let _ = server.await;
}

/// The server actually runs the sweep: an expired lease's units come back
/// without anyone asking.
#[tokio::test]
async fn the_server_reclaims_expired_leases_on_its_interval() {
    let inner = store_with_expired_lease();
    let grant = inner
        .acquire(ACCOUNT, CostUnits(400), SignedDuration::from_secs(1), t(0))
        .await
        .unwrap();
    assert_eq!(inner.balance(ACCOUNT), CostUnits(600));

    let store = Arc::new(FlakyReclaimStore {
        inner: Arc::clone(&inner),
        failures_left: AtomicU32::new(0),
        fail_on_call: None,
        fail_rollover: false,
        calls: AtomicU32::new(0),
    });
    let (captor, guard) = capture();
    // A clock past the lease's expiry, so the sweep has something to reclaim.
    serve_briefly(
        Arc::clone(&store),
        Arc::new(tollgate_store::ManualClock::new(t(120))),
    )
    .await;
    drop(guard);

    assert!(store.calls.load(Ordering::Acquire) > 0, "the sweep ran");
    assert_eq!(
        inner.balance(ACCOUNT),
        CostUnits(1_000),
        "lease {} units returned to the account",
        grant.units
    );

    // The units moving is one half; an operator seeing that they moved is the
    // other. Ticks that reclaim nothing stay silent, so the count of these
    // events is the count of real reclaims — never noise, never absent.
    let reclaims: Vec<_> = captor
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|event| {
            event.level == Level::INFO && event.fields.iter().any(|(key, _)| key == "leases")
        })
        .cloned()
        .collect();
    assert_eq!(
        reclaims.len(),
        1,
        "exactly the sweep that reclaimed something reports it: {reclaims:?}"
    );
    let fields = &reclaims[0].fields;
    assert!(
        fields.contains(&("leases".to_string(), "1".to_string()))
            && fields.contains(&("units".to_string(), "400".to_string())),
        "the event must say what came back: {fields:?}"
    );
    assert!(
        !captor
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.fields.iter().any(|(key, _)| key == "after_failures")),
        "a sweep that never failed must not announce a recovery"
    );
}

/// A full transaction is evidence to continue now, not after the next
/// interval. The long interval makes a second scheduled tick impossible
/// during the test, so both calls must belong to the first sweep cycle.
#[tokio::test]
async fn one_scheduled_sweep_drains_every_saturated_batch() {
    let inner = store_with_expired_lease();
    let lease_count = DEFAULT_RECLAIM_BATCH_LIMIT.get() + 1;
    for _ in 0..lease_count {
        inner
            .acquire(ACCOUNT, CostUnits(1), SignedDuration::from_secs(1), t(0))
            .await
            .unwrap();
    }
    assert_eq!(
        inner.balance(ACCOUNT),
        CostUnits(1_000 - u64::try_from(lease_count).unwrap())
    );

    let store = Arc::new(FlakyReclaimStore {
        inner: Arc::clone(&inner),
        failures_left: AtomicU32::new(0),
        fail_on_call: None,
        fail_rollover: false,
        calls: AtomicU32::new(0),
    });
    let (server, stop_tx) = spawn_server(
        Arc::clone(&store),
        Arc::new(tollgate_store::ManualClock::new(t(120))),
        std::time::Duration::from_secs(3_600),
    )
    .await;

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while store.calls.load(Ordering::Acquire) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first sweep cycle must drain both batches");
    assert_eq!(store.calls.load(Ordering::Acquire), 2);
    assert_eq!(inner.balance(ACCOUNT), CostUnits(1_000));

    let _ = stop_tx.send(());
    let _ = server.await;
}

/// A later batch can fail after earlier transactions committed. That partial
/// progress is part of the operational result and must be carried by the
/// warning rather than disappearing behind the final error.
#[tokio::test]
async fn a_failed_later_batch_reports_already_committed_progress() {
    let inner = store_with_expired_lease();
    let lease_count = DEFAULT_RECLAIM_BATCH_LIMIT.get() + 1;
    for _ in 0..lease_count {
        inner
            .acquire(ACCOUNT, CostUnits(1), SignedDuration::from_secs(1), t(0))
            .await
            .unwrap();
    }
    let store = Arc::new(FlakyReclaimStore {
        inner,
        failures_left: AtomicU32::new(0),
        fail_on_call: Some(2),
        fail_rollover: false,
        calls: AtomicU32::new(0),
    });
    let (captor, guard) = capture();
    let (server, stop_tx) = spawn_server(
        Arc::clone(&store),
        Arc::new(tollgate_store::ManualClock::new(t(120))),
        std::time::Duration::from_secs(3_600),
    )
    .await;

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if captor.0.lock().unwrap().iter().any(|event| {
                event.level == Level::WARN
                    && event
                        .fields
                        .iter()
                        .any(|(key, _)| key == "completed_batches")
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the failed second batch must be reported");
    drop(guard);

    {
        let events = captor.0.lock().unwrap();
        let warning = events
            .iter()
            .find(|event| {
                event.level == Level::WARN
                    && event
                        .fields
                        .iter()
                        .any(|(key, _)| key == "completed_batches")
            })
            .unwrap();
        assert!(warning.fields.contains(&(
            "reclaimed_leases".to_string(),
            DEFAULT_RECLAIM_BATCH_LIMIT.get().to_string()
        )));
        assert!(warning.fields.contains(&(
            "reclaimed_units".to_string(),
            DEFAULT_RECLAIM_BATCH_LIMIT.get().to_string()
        )));
        assert!(
            warning
                .fields
                .contains(&("completed_batches".to_string(), "1".to_string()))
        );
    }

    let _ = stop_tx.send(());
    let _ = server.await;
}

/// Recovery is its own signal: an operator watching a failing sweep needs to
/// learn it came back, and how long it was down for.
#[tokio::test]
async fn a_recovering_sweep_reports_how_many_failures_it_took() {
    let store = Arc::new(FlakyReclaimStore {
        inner: store_with_expired_lease(),
        failures_left: AtomicU32::new(2),
        fail_on_call: None,
        fail_rollover: false,
        calls: AtomicU32::new(0),
    });
    let (captor, guard) = capture();
    serve_briefly(Arc::clone(&store), Arc::new(SystemClock)).await;
    drop(guard);

    let recoveries: Vec<_> = captor
        .0
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| {
            event
                .fields
                .iter()
                .find(|(key, _)| key == "after_failures")
                .map(|(_, value)| value.clone())
        })
        .collect();
    assert_eq!(
        recoveries,
        vec!["2".to_string()],
        "exactly one recovery, naming the failures it followed"
    );
}

/// A sweep that fails every tick reports each failure with a rising count —
/// the only signal there is, since `/readyz` still answers OK.
#[tokio::test]
async fn a_failing_sweep_reports_consecutive_failures() {
    let (captor, guard) = capture();

    let store = Arc::new(FlakyReclaimStore {
        inner: store_with_expired_lease(),
        failures_left: AtomicU32::new(u32::MAX),
        fail_on_call: None,
        fail_rollover: false,
        calls: AtomicU32::new(0),
    });
    serve_briefly(Arc::clone(&store), Arc::new(SystemClock)).await;
    drop(guard);

    let events = captor.0.lock().unwrap().clone();
    assert!(!format!("{events:?}").contains("fixture-maintenance-sensitive-70"));
    assert!(events.iter().any(|event| {
        event
            .fields
            .contains(&("operation".into(), "\"reclaim\"".into()))
    }));

    let counts: Vec<u64> = captor
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.level == Level::WARN)
        .filter_map(|event| {
            event
                .fields
                .iter()
                .find(|(key, _)| key == "consecutive_failures")
                .and_then(|(_, value)| value.parse().ok())
        })
        .collect();

    assert!(
        counts.len() >= 2,
        "every failed sweep must be reported: {counts:?}"
    );
    assert_eq!(counts[0], 1);
    assert_eq!(
        counts[1], 2,
        "the count must rise so a persistent failure is distinguishable from a blip"
    );
}

#[tokio::test]
async fn failed_rollover_retains_safe_progress_without_backend_text() {
    let store = Arc::new(FlakyReclaimStore {
        inner: store_with_expired_lease(),
        failures_left: AtomicU32::new(0),
        fail_on_call: None,
        fail_rollover: true,
        calls: AtomicU32::new(0),
    });
    let (captor, guard) = capture();
    let (server, stop) = spawn_server(
        store,
        Arc::new(SystemClock),
        std::time::Duration::from_secs(3600),
    )
    .await;
    let observed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let found = captor.0.lock().unwrap().iter().any(|event| {
                event
                    .fields
                    .contains(&("operation".into(), "\"budget-rollover\"".into()))
            });
            if found {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let _ = stop.send(());
    server.await.unwrap().unwrap();
    drop(guard);
    observed.expect("the failing rollover must report its outcome");
    let events = captor.0.lock().unwrap();
    let event = events
        .iter()
        .find(|event| {
            event
                .fields
                .contains(&("operation".into(), "\"budget-rollover\"".into()))
        })
        .unwrap();
    assert_eq!(event.level, Level::WARN);
    assert!(
        event
            .fields
            .contains(&("code".into(), "\"storage\"".into()))
    );
    assert!(
        event
            .fields
            .contains(&("rolled_accounts".into(), "0".into()))
    );
    assert!(
        event
            .fields
            .contains(&("completed_batches".into(), "0".into()))
    );
    assert!(!format!("{events:?}").contains("fixture-maintenance-sensitive-70"));
}

/// The rollover pass shares this tick, and it is the *only* trigger: without
/// it a budget schedule is a stored intention nothing ever acts on. The
/// account below has one and has never been rolled, so a served interval must
/// leave it holding its allowance.
#[tokio::test]
async fn the_server_rolls_due_budget_periods_on_its_interval() {
    let inner = store_with_expired_lease();
    AdminStore::set_budget_schedule(
        &*inner,
        ACCOUNT,
        Some(tollgate_core::BudgetSchedule::monthly(CostUnits(500))),
    )
    .await
    .unwrap();
    assert_eq!(inner.balance(ACCOUNT), CostUnits(1_000), "no allowance yet");

    let store = Arc::new(FlakyReclaimStore {
        inner: Arc::clone(&inner),
        failures_left: AtomicU32::new(0),
        fail_on_call: None,
        fail_rollover: false,
        calls: AtomicU32::new(0),
    });
    let (captor, guard) = capture();
    // 2026-02-14, past the boundary the epoch-stamped account still sits
    // behind.
    serve_briefly(
        Arc::clone(&store),
        Arc::new(tollgate_store::ManualClock::new(t(1_771_027_200))),
    )
    .await;
    drop(guard);

    assert_eq!(
        inner.balance(ACCOUNT),
        CostUnits(1_500),
        "the allowance was deposited beside the opening top-up"
    );

    // Reported once, and only by the tick that rolled something: a pass that
    // logged every tick would bury the one event an operator needs, and one
    // that logged none would leave "rolling normally" and "the schedule
    // stopped firing" indistinguishable.
    let rolls: Vec<_> = captor
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|event| {
            event.level == Level::INFO && event.fields.iter().any(|(key, _)| key == "accounts")
        })
        .cloned()
        .collect();
    assert_eq!(
        rolls.len(),
        1,
        "exactly the tick that crossed a boundary reports it: {rolls:?}"
    );
    assert!(
        rolls[0]
            .fields
            .contains(&("accounts".to_string(), "1".to_string())),
        "the event must say how many accounts moved: {:?}",
        rolls[0].fields
    );
}

/// An account with no schedule is not "rolled with zero accounts" — the pass
/// must stay silent, or every quiet tick would emit an event and the signal
/// above would be worthless.
#[tokio::test]
async fn a_tick_that_rolls_nothing_says_nothing() {
    let inner = store_with_expired_lease();
    let store = Arc::new(FlakyReclaimStore {
        inner: Arc::clone(&inner),
        failures_left: AtomicU32::new(0),
        fail_on_call: None,
        fail_rollover: false,
        calls: AtomicU32::new(0),
    });
    let (captor, guard) = capture();
    serve_briefly(
        Arc::clone(&store),
        Arc::new(tollgate_store::ManualClock::new(t(1_771_027_200))),
    )
    .await;
    drop(guard);

    assert_eq!(inner.balance(ACCOUNT), CostUnits(1_000));
    assert!(
        !captor
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.fields.iter().any(|(key, _)| key == "accounts")),
        "an unscheduled account must not produce a rollover event"
    );
}

#[tokio::test]
async fn cancelling_the_server_releases_its_owned_maintenance_task() {
    let store = Arc::new(FlakyReclaimStore {
        inner: store_with_expired_lease(),
        failures_left: AtomicU32::new(0),
        fail_on_call: None,
        fail_rollover: false,
        calls: AtomicU32::new(0),
    });
    let (server, _stop) = spawn_server(
        Arc::clone(&store),
        Arc::new(SystemClock),
        std::time::Duration::from_secs(3600),
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while store.calls.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while Arc::strong_count(&store) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a detached sweeper would keep holding the backend");
}
