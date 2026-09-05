//! Failures reach an operator (issue #36).
//!
//! The control plane retries forever by design, so a failing backend is not
//! an error anyone returns — without an event it is indistinguishable from an
//! idle one. These tests treat the events as behavior: they assert on
//! *structured fields* (target, level, named values), never on rendered
//! message text, which would be the source-text assertion AGENTS.md forbids.

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, Once};

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};

use tollgate_admission::LeaseSlot;
use tollgate_client::{LeaseManager, LeaseManagerConfig, ManualClock, UsageWriter};
use tollgate_core::{
    AccountId, AccountStatus, CapacityClass, CostUnits, PolicyRevision, UsageEvent, UsageSource,
};
use tollgate_store::{
    AccountConfig, AllocateError, GrantPolicy, IngestError, IngestReport, LeaseAllocator,
    MemoryStore, ReclaimBatch, StoreError, UsageSink,
};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

const ACCOUNT: AccountId = AccountId(1);

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

/// One captured event, reduced to what a test should depend on.
#[derive(Debug, Clone)]
struct Captured {
    level: Level,
    target: String,
    fields: Vec<(String, String)>,
}

impl Captured {
    fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Default, Clone)]
struct Captor(Arc<Mutex<Vec<Captured>>>);

impl Captor {
    fn events(&self) -> Vec<Captured> {
        self.0.lock().unwrap().clone()
    }

    /// Events at or above `level` whose `target` starts with `target`.
    fn at_least(&self, level: Level, target: &str) -> Vec<Captured> {
        self.events()
            .into_iter()
            .filter(|event| event.level <= level && event.target.starts_with(target))
            .collect()
    }
}

struct FieldCollector(Vec<(String, String)>);

impl Visit for FieldCollector {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_string(), value.to_string()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.push((field.name().to_string(), value.to_string()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
}

thread_local! {
    /// The capturing test's buffer, if this thread is inside `capture()`.
    static SINK: RefCell<Option<Captor>> = const { RefCell::new(None) };
}

/// The one subscriber this binary installs, and it is installed *globally*
/// rather than per test with `tracing::subscriber::set_default`.
///
/// `tracing` caches each callsite's `Interest` in a process-global slot and
/// computes it the first time any thread hits that callsite, against *that*
/// thread's dispatcher (`tracing_core::callsite::Rebuilder::JustOne` calls
/// `get_default`). A thread-local subscriber therefore does not make the
/// decision thread-local: one test reaching a logged path with no dispatcher
/// installed caches `Interest::never()` for the whole process, and every
/// other test's capture of that event silently returns nothing. Assertions
/// that an event is *absent* then pass for the wrong reason.
///
/// A global dispatcher makes that unrepresentable: every thread always has a
/// real subscriber, so interest is never `never`. Per-test isolation moves to
/// `SINK`, which is genuinely thread-local. See `tollgate-server`'s
/// `tests/sweep.rs`, where this cost a CI-only failure on a two-vCPU runner.
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
                target: event.metadata().target().to_string(),
                fields: collector.0,
            });
        });
    }
}

/// Install `Router` as the process-wide dispatcher. Idempotent, and called
/// from `capture()` and from the store fixture every test builds, so no test
/// can reach a logged path before a dispatcher exists.
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

fn store(balance: u64) -> Arc<MemoryStore> {
    install_subscriber();
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(3_600),
        reclaim_grace: SignedDuration::ZERO,
    })
    .unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(balance),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    store
}

fn manager_config() -> LeaseManagerConfig {
    LeaseManagerConfig {
        account: ACCOUNT,
        target_grant: CostUnits(1_000),
        low_water: CostUnits(250),
        lease_ttl: SignedDuration::from_secs(60),
        expiry_safety_margin: SignedDuration::ZERO,
        poll_interval: std::time::Duration::from_millis(5),
        store_call_timeout: std::time::Duration::from_secs(5),
        shutdown_release_deadline: std::time::Duration::from_secs(10),
    }
}

/// An allocator that refuses every acquire, as an unreachable backend does.
struct RefusingAllocator;

#[async_trait]
impl LeaseAllocator for RefusingAllocator {
    async fn acquire(
        &self,
        _account: AccountId,
        _requested: CostUnits,
        _ttl: SignedDuration,
        _now: Timestamp,
    ) -> Result<tollgate_core::LeaseGrant, AllocateError> {
        Err(AllocateError::Storage(StoreError("backend down".into())))
    }

    async fn release(
        &self,
        _lease_id: tollgate_core::LeaseId,
        _fencing_token: tollgate_core::FencingToken,
        _unspent: CostUnits,
        _now: Timestamp,
    ) -> Result<(), AllocateError> {
        Ok(())
    }

    async fn reclaim_expired_batch(
        &self,
        _now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError> {
        ReclaimBatch::try_new(Vec::new(), limit)
    }
}

/// The condition an operator most needs to see: this instance is denying
/// every request because it cannot get a lease. Retrying forever is correct
/// behavior, which is exactly why it must not be quiet.
#[tokio::test(start_paused = true)]
async fn refill_failure_with_an_empty_slot_warns() {
    let (captor, _guard) = capture();
    let manager = LeaseManager::spawn(
        Arc::new(RefusingAllocator) as Arc<dyn LeaseAllocator>,
        LeaseSlot::for_account(ACCOUNT),
        Arc::new(ManualClock::new(t(0))),
        manager_config(),
    )
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    manager.shutdown().await;

    let warnings = captor.at_least(Level::WARN, "tollgate_client::lease_manager");
    assert!(
        !warnings.is_empty(),
        "a refill failure that leaves the slot empty must warn"
    );
    assert!(
        warnings.iter().any(|event| {
            event
                .field("reason")
                .is_some_and(|reason| reason.contains("backend down"))
        }),
        "the event must carry why the allocator refused: {warnings:?}"
    );
}

/// A healthy refill is not an incident: the same code path must stay quiet
/// when nothing is wrong, or the signal is worthless.
#[tokio::test(start_paused = true)]
async fn healthy_refill_emits_no_warning() {
    let (captor, _guard) = capture();
    let manager = LeaseManager::spawn(
        store(10_000),
        LeaseSlot::for_account(ACCOUNT),
        Arc::new(ManualClock::new(t(0))),
        manager_config(),
    )
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    manager.shutdown().await;

    assert!(
        captor.at_least(Level::WARN, "tollgate_client").is_empty(),
        "a healthy control plane must not cry wolf: {:?}",
        captor.at_least(Level::WARN, "tollgate_client")
    );
}

/// A sink that fails a fixed number of times, then delegates.
struct FlakySink {
    inner: Arc<MemoryStore>,
    failures_left: std::sync::atomic::AtomicU32,
}

#[async_trait]
impl UsageSink for FlakySink {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        now: Timestamp,
    ) -> Result<IngestReport, IngestError> {
        if self
            .failures_left
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |n| n.checked_sub(1),
            )
            .is_ok()
        {
            return Err(StoreError("sink unavailable".into()).into());
        }
        self.inner.ingest(events, now).await
    }
}

/// An outage is a duration, not an event. `WriterStats` reports a recovered
/// outage as a perfectly clean run, so its beginning and its length have no
/// channel other than these events.
#[tokio::test(start_paused = true)]
async fn usage_sink_outage_and_recovery_are_reported() {
    let store = store(10_000);
    let lease = store
        .acquire(
            ACCOUNT,
            CostUnits(1_000),
            SignedDuration::from_secs(60),
            t(0),
        )
        .await
        .unwrap();
    let (captor, _guard) = capture();
    let sink = Arc::new(FlakySink {
        inner: store.clone(),
        failures_left: std::sync::atomic::AtomicU32::new(3),
    });
    let (recorder, writer) = UsageWriter::spawn(
        sink as Arc<dyn UsageSink>,
        Arc::new(ManualClock::new(t(0))),
        tollgate_client::UsageWriterConfig {
            queue_capacity: 16,
            max_batch: 4,
            flush_interval: std::time::Duration::from_millis(10),
            retry_backoff: std::time::Duration::from_millis(10),
            shutdown_drain_deadline: std::time::Duration::from_secs(60),
            ingest_timeout: std::time::Duration::from_secs(5),
        },
    )
    .unwrap();

    recorder.try_reserve().unwrap().record(UsageEvent::new(
        tollgate_core::RequestId(1),
        lease.account_id,
        UsageSource::Leased {
            lease_id: lease.lease_id,
            fencing_token: lease.fencing_token,
        },
        CostUnits(25),
        t(0),
        PolicyRevision::UNSTATED,
    ));
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let stats = writer.shutdown().await.unwrap();

    // The run is clean by every existing measure — which is the point.
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.lost, 0);

    // A persisting outage stays at debug: said once loudly, then quietly, so
    // a long outage is a trail rather than a flood.
    let persisting: Vec<_> = captor
        .at_least(Level::DEBUG, "tollgate_client::usage_writer")
        .into_iter()
        .filter(|event| event.level == Level::DEBUG && event.field("attempts").is_some())
        .collect();
    assert_eq!(
        persisting.len(),
        2,
        "attempts 2 and 3 must trail at debug, not repeat the warning: {persisting:?}"
    );

    let writer_events = captor.at_least(Level::INFO, "tollgate_client::usage_writer");
    assert!(
        writer_events
            .iter()
            .any(|event| event.level == Level::WARN && event.field("timed_out").is_some()),
        "entering an outage must warn once: {writer_events:?}"
    );
    let recovery = writer_events
        .iter()
        .find(|event| event.field("outage_ms").is_some())
        .expect("recovery must report how long the sink was down");
    assert_eq!(recovery.level, Level::INFO);
    assert!(
        recovery
            .field("attempts")
            .is_some_and(|attempts| attempts.parse::<u64>().unwrap_or(0) >= 3),
        "recovery must report the attempts it took: {recovery:?}"
    );
}

/// An allocator that grants normally and rejects every release capability.
struct FencedReleaseAllocator {
    inner: Arc<MemoryStore>,
}

#[async_trait]
impl LeaseAllocator for FencedReleaseAllocator {
    async fn acquire(
        &self,
        account: AccountId,
        requested: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<tollgate_core::LeaseGrant, AllocateError> {
        self.inner.acquire(account, requested, ttl, now).await
    }

    async fn release(
        &self,
        _lease_id: tollgate_core::LeaseId,
        _fencing_token: tollgate_core::FencingToken,
        _unspent: CostUnits,
        _now: Timestamp,
    ) -> Result<(), AllocateError> {
        Err(AllocateError::Fenced)
    }

    async fn reclaim_expired_batch(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError> {
        self.inner.reclaim_expired_batch(now, limit).await
    }
}

/// `LeaseManagerReport` counts a refused release as released — the store is
/// no longer holding it — so *which* refusal happened has no channel but the
/// event. A `Fenced` at shutdown means the store rejected the capability the
/// manager copied from its grant.
#[tokio::test(start_paused = true)]
async fn refused_release_at_shutdown_is_reported() {
    let (captor, _guard) = capture();
    let manager = LeaseManager::spawn(
        Arc::new(FencedReleaseAllocator {
            inner: store(10_000),
        }) as Arc<dyn LeaseAllocator>,
        LeaseSlot::for_account(ACCOUNT),
        Arc::new(ManualClock::new(t(0))),
        manager_config(),
    )
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let report = manager.shutdown().await;
    assert_eq!(report.released, 1, "the store is not holding it open");
    assert_eq!(report.abandoned, 0);

    let refusals: Vec<_> = captor
        .at_least(Level::WARN, "tollgate_client::lease_manager")
        .into_iter()
        .filter(|event| event.field("lease").is_some())
        .collect();
    assert!(
        !refusals.is_empty(),
        "a refused release at shutdown must be reported, not counted as clean"
    );
    assert!(
        refusals
            .iter()
            .any(|event| event.field("error").is_some_and(|e| e.contains("fenc"))),
        "the event must name the refusal: {refusals:?}"
    );
}
