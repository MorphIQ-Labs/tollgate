//! Failures reach an operator (issue #36).
//!
//! The control plane retries forever by design, so a failing backend is not
//! an error anyone returns — without an event it is indistinguishable from an
//! idle one. These tests treat the events as behavior: they assert on
//! *structured fields* (target, level, named values), never on rendered
//! message text, which would be the source-text assertion AGENTS.md forbids.

use std::cell::RefCell;
use std::sync::{Arc, Mutex, Once};

use jiff::{SignedDuration, Timestamp};

use tollgate_admission::LeaseSlot;
use tollgate_client::{LeaseManager, LeaseManagerConfig, ManualClock, UsageWriter};
use tollgate_core::{
    AccountId, AccountStatus, CapacityClass, CostUnits, PolicyRevision, UsageEvent, UsageSource,
};
#[path = "../../tollgate-store/tests/support/delegating.rs"]
mod delegating;
use delegating::{DelegatingStore, RejectingStore, rejecting};

use tollgate_store::{
    AccountConfig, AllocateError, GrantPolicy, IngestReport, LeaseAllocator, MemoryStore,
    ReclaimBatch, StoreError, UsageSink,
};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

#[path = "support/snapshot_source.rs"]
mod snapshot_source;
use snapshot_source::DrivenPushSource;

const PRINCIPAL: tollgate_core::Principal = tollgate_core::Principal(77);

fn positive(generation: u64) -> tollgate_store::SnapshotResolution {
    use tollgate_core::{
        AccountSnapshot, CostTable, Generation, PermissionBits, PublishableSnapshot, ResolvedLimits,
    };
    tollgate_store::SnapshotResolution::Present(
        PublishableSnapshot::try_new(Arc::new(
            AccountSnapshot::builder(
                ACCOUNT,
                Generation(generation),
                AccountStatus::Active,
                t(1_000),
                PermissionBits::NONE,
                ResolvedLimits::new(1),
                Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
            )
            .build(),
        ))
        .unwrap(),
    )
}

fn snapshots(
    source: Arc<snapshot_source::DelegatingStore<snapshot_source::RejectingStore>>,
    clock: Arc<ManualClock>,
) -> (
    tollgate_client::SnapshotManager,
    Arc<tollgate_admission::ArcSwapSnapshotMap>,
) {
    let map = Arc::new(tollgate_admission::ArcSwapSnapshotMap::new());
    let manager = tollgate_client::SnapshotManager::spawn(
        source,
        map.clone(),
        tollgate_client::SlotRegistry::new(),
        clock,
        tollgate_client::SnapshotManagerConfig {
            principals: tollgate_client::TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_secs(1),
            unknown_ttl: SignedDuration::from_secs(1),
            revoked_ttl: SignedDuration::from_secs(1),
            retry_backoff: std::time::Duration::from_secs(1),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(1),
            enumeration_timeout: std::time::Duration::from_secs(1),
        },
    )
    .unwrap();
    (manager, map)
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("observable update must arrive");
}

#[tokio::test(start_paused = true)]
async fn snapshot_refusals_are_reported_and_counted_for_pushes_and_refreshes() {
    use tollgate_admission::{MapEntry, SnapshotMap};
    use tollgate_core::Generation;
    use tollgate_store::SnapshotResolution;
    for push in [true, false] {
        let (captor, _guard) = capture();
        let (source, pulls) = DrivenPushSource::new(positive(5));
        let clock = Arc::new(ManualClock::new(t(0)));
        let (manager, map) = snapshots(pulls.clone(), clock.clone());
        let counters = manager.counters();
        wait_for(|| *manager.ready().borrow()).await;
        let offer = |resolution: SnapshotResolution| {
            source.set_pull(resolution.clone());
            if push {
                source.send(PRINCIPAL, resolution);
            } else {
                // Expire a prior tombstone's refetch deadline; policy time
                // remains independent of Tokio's paused scheduling clock.
                clock.advance(SignedDuration::from_secs(2));
            }
        };
        offer(positive(4));
        wait_for(|| counters.snapshot().refused_updates == 1).await;
        offer(SnapshotResolution::Revoked {
            generation: Generation(4),
        });
        wait_for(|| counters.snapshot().refused_updates == 2).await;
        let Some(MapEntry::Present(state)) = map.get(&PRINCIPAL) else {
            panic!("refusals must preserve the live snapshot")
        };
        assert_eq!(state.snapshot.generation, Generation(5));
        assert!(*manager.ready().borrow());

        // A legitimate withdrawal is accepted, but its own generation
        // cannot return as positive authority.
        offer(SnapshotResolution::Revoked {
            generation: Generation(5),
        });
        wait_for(|| !matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_)))).await;
        assert_eq!(counters.snapshot().refused_updates, 2);
        offer(positive(5));
        wait_for(|| counters.snapshot().refused_updates == 3).await;
        assert!(!matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(_))));
        offer(positive(6));
        wait_for(|| matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(state)) if state.snapshot.generation == Generation(6))).await;
        assert_eq!(counters.snapshot().refused_updates, 3);
        assert_eq!(counters.snapshot().refresh_failures, 0);
        manager.shutdown().await;
        let warnings = captor.at_least(Level::WARN, "tollgate_client::snapshot_manager");
        assert_eq!(
            warnings.len(),
            3,
            "exactly one event per refused update: {warnings:?}"
        );
        for (event, (kind, offered)) in
            warnings
                .iter()
                .zip([("positive", "4"), ("revoked", "4"), ("positive", "5")])
        {
            assert_eq!(event.level, Level::WARN);
            assert_eq!(
                event.field("origin"),
                Some(if push { "push" } else { "refresh" })
            );
            assert_eq!(event.field("kind"), Some(kind));
            assert_eq!(event.field("offered"), Some(offered));
            assert!(event.field("principal").is_some());
            assert!(event.field("retained").is_some());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn unchanged_snapshots_and_accepted_updates_do_not_report_refusals() {
    use tollgate_admission::{MapEntry, SnapshotMap};
    use tollgate_core::Generation;
    let (captor, _guard) = capture();
    let (source, pulls) = DrivenPushSource::new(positive(5));
    let (manager, map) = snapshots(pulls.clone(), Arc::new(ManualClock::new(t(0))));
    let counters = manager.counters();
    wait_for(|| counters.snapshot().refresh_attempts >= 2).await;
    source.send(PRINCIPAL, positive(5));
    // The next accepted push is a FIFO barrier proving the duplicate was
    // processed; no absence assertion can pass just because a task was idle.
    source.set_pull(positive(6));
    source.send(PRINCIPAL, positive(6));
    wait_for(|| matches!(map.get(&PRINCIPAL), Some(MapEntry::Present(state)) if state.snapshot.generation == Generation(6))).await;
    assert_eq!(counters.snapshot().refused_updates, 0);
    manager.shutdown().await;
    assert!(
        captor
            .at_least(Level::WARN, "tollgate_client::snapshot_manager")
            .is_empty()
    );
}

#[tokio::test(start_paused = true)]
async fn lease_health_is_false_after_an_unpolled_abort_or_panic() {
    struct PanicClock;
    impl tollgate_store::Clock for PanicClock {
        fn now(&self) -> Timestamp {
            panic!("fixture clock panic")
        }
    }
    for abort in [true, false] {
        let (_captor, _guard) = capture();
        let manager = LeaseManager::spawn(
            store(10_000),
            LeaseSlot::for_account(ACCOUNT),
            Arc::new(PanicClock),
            manager_config(),
        )
        .unwrap();
        let mut health = manager.health();
        assert!(*health.borrow());
        if abort {
            // No await before Drop: the spawned task has never been polled.
            drop(manager);
        } else {
            wait_for(|| health.has_changed().is_err()).await;
            manager.shutdown().await;
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while health.changed().await.is_ok() {}
        })
        .await
        .unwrap();
        assert!(!*health.borrow());
    }
}

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
/// Refuses every acquire with a backend error, over no store at all.
///
/// `consolidate` is left to `rejecting`, which panics naming the method --
/// the same claim the hand-written `unreachable!()` made.
fn refusing_allocator() -> Arc<DelegatingStore<RejectingStore>> {
    Arc::new(
        rejecting("these fixtures never consolidate")
            .on_acquire(|_, _account, _requested, _ttl, _now| async {
                Err(AllocateError::Storage(StoreError("backend down".into())))
            })
            .on_release(|_, _lease, _fence, _unspent, _now| async { Ok(()) })
            .on_reclaim_expired_batch(|_, _now, limit| async move {
                ReclaimBatch::try_new(Vec::new(), limit)
            }),
    )
}

/// The condition an operator most needs to see: this instance is denying
/// every request because it cannot get a lease. Retrying forever is correct
/// behavior, which is exactly why it must not be quiet.
#[tokio::test(start_paused = true)]
async fn refill_failure_with_an_empty_slot_warns() {
    let (captor, _guard) = capture();
    let manager = LeaseManager::spawn(
        refusing_allocator() as Arc<dyn LeaseAllocator>,
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
/// Fails a fixed number of ingests, then behaves like the store it wraps.
fn flaky_sink(store: Arc<MemoryStore>, failures: u32) -> Arc<DelegatingStore<MemoryStore>> {
    let failures_left = Arc::new(std::sync::atomic::AtomicU32::new(failures));
    Arc::new(
        DelegatingStore::wrapping(store).on_ingest(move |inner, events, now| {
            let failures_left = Arc::clone(&failures_left);
            async move {
                if failures_left
                    .fetch_update(
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Acquire,
                        |n| n.checked_sub(1),
                    )
                    .is_ok()
                {
                    return Err(StoreError("sink unavailable".into()).into());
                }
                inner.ingest(&events, now).await
            }
        }),
    )
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
        .unwrap()
        .grant;
    let (captor, _guard) = capture();
    let sink = flaky_sink(store.clone(), 3);
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
        None,
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

#[tokio::test(start_paused = true)]
async fn missing_credential_attribution_emits_a_structured_coverage_event() {
    let store = store(100);
    let (captor, _guard) = capture();
    let (recorder, writer) = UsageWriter::spawn(
        store,
        Arc::new(ManualClock::new(t(0))),
        tollgate_client::UsageWriterConfig {
            queue_capacity: 1,
            max_batch: 1,
            flush_interval: std::time::Duration::from_millis(1),
            retry_backoff: std::time::Duration::from_millis(1),
            ingest_timeout: std::time::Duration::from_millis(10),
            shutdown_drain_deadline: std::time::Duration::from_secs(1),
        },
    )
    .unwrap();
    recorder.try_reserve().unwrap().record(UsageEvent::new(
        tollgate_core::RequestId(105),
        ACCOUNT,
        UsageSource::Overage,
        CostUnits(1),
        t(0),
        PolicyRevision::UNSTATED,
        None,
    ));
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.unattributed, 1);
    assert!(
        captor
            .at_least(Level::WARN, "tollgate_client::usage_writer")
            .iter()
            .any(|event| event.field("unattributed") == Some("Some(1)"))
    );
}

/// Reports attribution three ways, as the `mode` byte says.
fn attribution_sink(
    mode: &Arc<std::sync::atomic::AtomicU8>,
) -> Arc<DelegatingStore<RejectingStore>> {
    let mode = Arc::clone(mode);
    Arc::new(
        rejecting("an attribution fixture answers ingest and nothing else").on_ingest(
            move |_, events, _now| {
                let mode = Arc::clone(&mode);
                async move {
                    Ok(IngestReport {
                        accepted: events.len() as u64,
                        duplicate: 0,
                        rejected: 0,
                        unattributed: match mode.load(std::sync::atomic::Ordering::Relaxed) {
                            0 => Some(0),
                            1 => Some(events.len() as u64),
                            _ => None,
                        },
                    })
                }
            },
        ),
    )
}

#[tokio::test(start_paused = true)]
async fn attribution_coverage_reports_transitions_without_repeating_incidents() {
    let (captor, _guard) = capture();
    let mode = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let sink = attribution_sink(&mode);
    let (recorder, writer) = UsageWriter::spawn(
        sink.clone(),
        Arc::new(ManualClock::new(t(0))),
        tollgate_client::UsageWriterConfig {
            queue_capacity: 1,
            max_batch: 1,
            flush_interval: std::time::Duration::from_millis(1),
            retry_backoff: std::time::Duration::from_millis(1),
            ingest_timeout: std::time::Duration::from_millis(10),
            shutdown_drain_deadline: std::time::Duration::from_secs(1),
        },
    )
    .unwrap();
    for (request, (reported, transitions)) in [
        (0, 0),
        (0, 0),
        (1, 1),
        (1, 1),
        (0, 2),
        (0, 2),
        (2, 3),
        (2, 3),
        (0, 4),
    ]
    .into_iter()
    .enumerate()
    {
        mode.store(reported, std::sync::atomic::Ordering::Relaxed);
        recorder.try_reserve().unwrap().record(UsageEvent::new(
            tollgate_core::RequestId(request as u128),
            ACCOUNT,
            UsageSource::Overage,
            CostUnits(1),
            t(0),
            PolicyRevision::UNSTATED,
            None,
        ));
        for _ in 0..100 {
            if recorder.health().stats.accepted == request as u64 + 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(recorder.health().stats.accepted, request as u64 + 1);
        // A persisting gap must not announce recovery. Checking only the
        // final event sequence would also accept recovery one batch early.
        assert_eq!(
            captor
                .at_least(Level::INFO, "tollgate_client::usage_writer")
                .iter()
                .filter(|event| event.field("coverage_complete").is_some())
                .count(),
            transitions
        );
    }
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(
        (
            stats.accepted,
            stats.unattributed,
            stats.attribution_unreported_batches
        ),
        (9, 2, 2)
    );
    let transitions: Vec<_> = captor
        .at_least(Level::INFO, "tollgate_client::usage_writer")
        .into_iter()
        .filter(|event| event.field("coverage_complete").is_some())
        .collect();
    assert_eq!(transitions.len(), 4);
    assert_eq!(transitions[0].field("unattributed"), Some("Some(1)"));
    assert_eq!(transitions[2].field("unattributed"), Some("None"));
    for (event, (level, complete)) in transitions.iter().zip([
        (Level::WARN, "false"),
        (Level::INFO, "true"),
        (Level::WARN, "false"),
        (Level::INFO, "true"),
    ]) {
        assert_eq!(event.level, level);
        assert_eq!(event.field("coverage_complete"), Some(complete));
    }
}

/// An allocator that grants normally and rejects every release capability.
/// Refuses every release, fenced or invalid, over a real store.
///
/// `consolidate` keeps its `unreachable!()`: this wrapper has a store behind
/// it, so delegation would silently give it a working exchange.
fn refusing_release(store: Arc<MemoryStore>, invalid: bool) -> Arc<DelegatingStore<MemoryStore>> {
    Arc::new(
        DelegatingStore::wrapping(store)
            .on_release(move |_, _lease, _fence, _unspent, _now| async move {
                Err(if invalid {
                    AllocateError::InvalidRelease
                } else {
                    AllocateError::Fenced
                })
            })
            .on_consolidate(|_, _, _, _, _, _, _, _| async {
                unreachable!("these fixtures never consolidate")
            }),
    )
}

/// A fenced capability is obsolete; invalid counts are an integrity fault
/// and unconfirmed release. They must differ in both the report and event.
#[tokio::test(start_paused = true)]
async fn refused_release_at_shutdown_is_reported() {
    for invalid in [false, true] {
        let (captor, _guard) = capture();
        let manager = LeaseManager::spawn(
            refusing_release(store(10_000), invalid) as Arc<dyn LeaseAllocator>,
            LeaseSlot::for_account(ACCOUNT),
            Arc::new(ManualClock::new(t(0))),
            manager_config(),
        )
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let counters = manager.counters();
        let report = manager.shutdown().await;
        assert_eq!(report.released, u64::from(!invalid));
        assert_eq!(report.abandoned, u64::from(invalid));
        assert_eq!(counters.snapshot().abandoned, u64::from(invalid));

        let refusals: Vec<_> = captor
            .at_least(Level::WARN, "tollgate_client::lease_manager")
            .into_iter()
            .filter(|event| event.field("lease").is_some())
            .collect();
        assert_eq!(refusals.len(), 1);
        assert_eq!(
            refusals[0].level,
            if invalid { Level::ERROR } else { Level::WARN }
        );
        assert!(refusals[0].field("error").is_some());
    }
}

/// Serves an empty key page, or refuses, as the `healthy` flag says.
fn refusing_keys(
    healthy: &Arc<std::sync::atomic::AtomicBool>,
) -> Arc<DelegatingStore<RejectingStore>> {
    let healthy = Arc::clone(healthy);
    Arc::new(
        rejecting("a key-source fixture answers pages and nothing else").on_active_keys_page(
            move |_, now, after, limit| {
                let healthy = Arc::clone(&healthy);
                async move {
                    if healthy.load(std::sync::atomic::Ordering::SeqCst) {
                        tollgate_store::KeyPage::try_new(1, now, after, limit, vec![], None)
                    } else {
                        Err(StoreError("fixture-sensitive-response-body".into()))
                    }
                }
            },
        ),
    )
}

#[tokio::test(start_paused = true)]
async fn credential_refresh_failure_is_structured_without_exposing_the_source_body() {
    let (captor, _guard) = capture();
    let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let source = refusing_keys(&healthy);
    let manager = tollgate_client::KeyManager::spawn(
        source.clone(),
        b"fixture-events-credential-secret-108",
        Arc::new(ManualClock::new(t(100))),
        tollgate_client::KeyManagerConfig::default(),
    )
    .unwrap();
    let mut monitor = manager.monitor();
    while monitor.report(t(100)).stats.failures == 0 {
        monitor.changed().await.unwrap();
    }
    let events = captor.at_least(Level::WARN, "tollgate_client::key_manager");
    assert!(
        events
            .iter()
            .any(|event| event.field("reason") == Some("source-read"))
    );
    assert!(
        events
            .iter()
            .flat_map(|event| &event.fields)
            .all(|(_, value)| !value.contains("fixture-sensitive-response-body"))
    );
    healthy.store(true, std::sync::atomic::Ordering::SeqCst);
    for completed in 1..=2 {
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        while monitor.report(t(100)).stats.refreshes < completed {
            monitor.changed().await.unwrap();
        }
        let recoveries = captor
            .at_least(Level::INFO, "tollgate_client::key_manager")
            .into_iter()
            .filter(|event| event.level == Level::INFO)
            .count();
        assert_eq!(recoveries, 1, "recovery is emitted once per incident");
    }
    manager.shutdown().await;
}
