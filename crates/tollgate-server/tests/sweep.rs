//! The reclaim sweep is INVARIANTS.md #9's server half: the only thing that
//! returns units stranded by a crashed holder.
//!
//! The store's own reclaim is covered by both backend suites; what is tested
//! here is that the *server* actually invokes it on its interval, and that a
//! sweep failing every tick withdraws readiness even when the store answers
//! `ping`. Tests synchronize on observed calls and outcomes, never fixed sleeps.
//!
//! As in `loopback.rs`, the oneshot teardown signals here are outside the
//! discard rule this MR installs: the assertions below are what fail if
//! shutdown misbehaves.
#![allow(
    clippy::let_underscore_must_use,
    reason = "test teardown discards results the assertions have already read"
)]

mod common;

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Once};

use jiff::{SignedDuration, Timestamp};

use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits};
use tollgate_store::{
    AccountConfig, AdminStore, DEFAULT_RECLAIM_BATCH_LIMIT, GrantPolicy, LeaseAllocator,
    MemoryStore, StoreError, SystemClock,
};
#[path = "../../tollgate-store/tests/support/delegating.rs"]
mod delegating;
use delegating::DelegatingStore;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt as _};
use tracing_subscriber::registry::LookupSpan;

use tollgate_server::{ServerState, serve};

struct Control(tokio::sync::mpsc::UnboundedSender<Call>);
struct Call {
    operation: &'static str,
    reply: tokio::sync::oneshot::Sender<Action>,
}

#[derive(Clone, Copy)]
enum Action {
    Succeed,
    Fail,
    Panic,
}

impl Control {
    async fn call(&self, operation: &'static str) -> Result<(), StoreError> {
        let (reply, action) = tokio::sync::oneshot::channel();
        self.0
            .send(Call { operation, reply })
            .expect("test owns call receiver");
        match action.await.expect("test supplies each call outcome") {
            Action::Succeed => Ok(()),
            Action::Fail => Err(StoreError("private-fixture-maintenance-71".into())),
            Action::Panic => panic!("public test panic fixture"),
        }
    }
}

async fn next_call(
    calls: &mut tokio::sync::mpsc::UnboundedReceiver<Call>,
    operation: &str,
) -> Call {
    let call = tokio::time::timeout(std::time::Duration::from_secs(2), calls.recv())
        .await
        .expect("maintenance must make its next call")
        .expect("server retains call sender");
    assert_eq!(call.operation, operation);
    call
}

fn complete(call: Call, action: Action) {
    assert!(
        call.reply.send(action).is_ok(),
        "the observed call is still owned by maintenance"
    );
}

const ACCOUNT: AccountId = AccountId(1);

/// Each observed call stays pending until the test supplies its outcome.
/// This pins ordering across awaits without making a scheduler-speed claim.
struct ControlledServer {
    state: Arc<SweepState>,
    calls: tokio::sync::mpsc::UnboundedReceiver<Call>,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    base: String,
    client: reqwest::Client,
}

impl ControlledServer {
    async fn start() -> Self {
        install_subscriber();
        let (calls_tx, calls) = tokio::sync::mpsc::unbounded_channel();
        let state = Arc::new(SweepState::default().controlled(Control(calls_tx)));
        let store = flaky(store_with_expired_lease(), &state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopping) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve(
            listener,
            ServerState {
                store: Arc::clone(&store),
                clock: Arc::new(SystemClock),
                security: common::security(),
                issuer: None,
            },
            std::time::Duration::from_millis(1),
            async {
                let _ = stopping.await;
            },
        ));
        Self {
            state,
            calls,
            server,
            stop: Some(stop),
            base,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .unwrap(),
        }
    }

    async fn call(&mut self, operation: &str) -> Call {
        next_call(&mut self.calls, operation).await
    }

    async fn probe(&self, path: &str) -> u16 {
        self.client
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    async fn shutdown(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), &mut self.server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

impl Drop for ControlledServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn readiness_requires_both_passes_and_tracks_independent_failure_and_recovery() {
    let mut server = ControlledServer::start().await;
    let reclaim = server.call("reclaim").await;
    assert_eq!(server.probe("/livez").await, 200);
    assert_eq!(server.probe("/readyz").await, 503);
    complete(reclaim, Action::Succeed);
    let rollover = server.call("budget-rollover").await;
    assert_eq!(
        server.probe("/readyz").await,
        503,
        "startup requires both passes"
    );
    complete(rollover, Action::Succeed);
    let reclaim = server.call("reclaim").await;
    assert_eq!(server.probe("/readyz").await, 200);
    server.state.ping_healthy.store(false, Ordering::Release);
    assert_eq!(
        server.probe("/readyz").await,
        503,
        "maintenance cannot replace store health"
    );
    server.state.ping_healthy.store(true, Ordering::Release);
    assert_eq!(server.probe("/readyz").await, 200);

    complete(reclaim, Action::Fail);
    let rollover = server.call("budget-rollover").await;
    assert_eq!(
        server.probe("/readyz").await,
        503,
        "report reclaim failure before rollover can suspend"
    );
    assert_eq!(server.probe("/livez").await, 200);
    complete(rollover, Action::Succeed);
    let reclaim = server.call("reclaim").await;
    assert_eq!(
        server.probe("/readyz").await,
        503,
        "rollover cannot repair reclaim health"
    );
    complete(reclaim, Action::Succeed);
    let rollover = server.call("budget-rollover").await;
    assert_eq!(server.probe("/readyz").await, 200);
    complete(rollover, Action::Fail);
    let reclaim = server.call("reclaim").await;
    assert_eq!(server.probe("/readyz").await, 503);
    complete(reclaim, Action::Succeed);
    let rollover = server.call("budget-rollover").await;
    assert_eq!(
        server.probe("/readyz").await,
        503,
        "reclaim cannot repair rollover health"
    );
    complete(rollover, Action::Succeed);
    let _pending = server.call("reclaim").await;
    assert_eq!(server.probe("/readyz").await, 200);
    server.shutdown().await;
}

#[tokio::test]
async fn persistent_failures_escalate_and_recovery_resets_each_operation() {
    for operation in ["reclaim", "budget-rollover"] {
        let (captor, _guard) = capture();
        let mut server = ControlledServer::start().await;
        let mut reclaim = server.call("reclaim").await;
        for action in [
            Action::Fail,
            Action::Fail,
            Action::Fail,
            Action::Fail,
            Action::Succeed,
            Action::Fail,
        ] {
            complete(
                reclaim,
                if operation == "reclaim" {
                    action
                } else {
                    Action::Succeed
                },
            );
            let rollover = server.call("budget-rollover").await;
            complete(
                rollover,
                if operation == "budget-rollover" {
                    action
                } else {
                    Action::Succeed
                },
            );
            reclaim = server.call("reclaim").await;
            assert_eq!(
                server.probe("/readyz").await,
                if matches!(action, Action::Succeed) {
                    200
                } else {
                    503
                }
            );
        }
        server.shutdown().await;
        let events = captor.0.lock().unwrap();
        let for_operation = |event: &&Captured| {
            event
                .fields
                .contains(&("operation".into(), format!("{operation:?}")))
        };
        let failures: Vec<_> = events
            .iter()
            .filter(for_operation)
            .filter_map(|event| {
                event
                    .fields
                    .iter()
                    .find(|(field, _)| field == "consecutive_failures")
                    .map(|(_, count)| (event.level, count.clone()))
            })
            .collect();
        assert_eq!(
            failures,
            vec![
                (Level::WARN, "1".into()),
                (Level::WARN, "2".into()),
                (Level::ERROR, "3".into()),
                (Level::ERROR, "4".into()),
                (Level::WARN, "1".into())
            ]
        );
        let recoveries: Vec<_> = events
            .iter()
            .filter(for_operation)
            .filter_map(|event| {
                event
                    .fields
                    .iter()
                    .find(|(field, _)| field == "after_failures")
                    .map(|(_, count)| (event.level, count.clone()))
            })
            .collect();
        assert_eq!(recoveries, vec![(Level::INFO, "4".into())]);
        assert!(!format!("{events:?}").contains("private-fixture-maintenance-71"));
    }
}

#[tokio::test]
async fn a_panicked_maintenance_call_stops_the_server_with_a_safe_error() {
    for operation in ["reclaim", "budget-rollover"] {
        let (captor, _guard) = capture();
        let mut server = ControlledServer::start().await;
        let mut call = server.call("reclaim").await;
        if operation == "budget-rollover" {
            complete(call, Action::Succeed);
            call = server.call(operation).await;
        }
        complete(call, Action::Panic);
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), &mut server.server)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(
            error.to_string(),
            "server maintenance task stopped unexpectedly"
        );
        assert!(server.stop.as_ref().unwrap().is_closed());
        let events = captor.0.lock().unwrap();
        assert!(events.iter().any(|event| {
            event.level == Level::ERROR
                && event
                    .fields
                    .contains(&("operation".into(), "\"maintenance\"".into()))
                && event
                    .fields
                    .contains(&("reason".into(), "\"panic\"".into()))
        }));
        assert!(!format!("{events:?}").contains("public test panic fixture"));
    }
}

#[tokio::test]
async fn graceful_shutdown_cancels_pending_maintenance_without_failure_events() {
    let (captor, _guard) = capture();
    let mut server = ControlledServer::start().await;
    let _pending = server.call("reclaim").await;
    server.shutdown().await;
    assert!(
        !captor
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.level == Level::ERROR)
    );
}

#[tokio::test]
async fn cancelling_the_server_drops_its_shutdown_future() {
    let mut server = ControlledServer::start().await;
    let _pending = server.call("reclaim").await;
    assert_eq!(server.probe("/livez").await, 200);
    server.server.abort();
    assert!((&mut server.server).await.unwrap_err().is_cancelled());
    assert!(
        server.stop.as_ref().unwrap().is_closed(),
        "the shutdown future belongs to serve, not a detached signal watcher"
    );
}

#[tokio::test]
async fn invalid_maintenance_intervals_are_rejected_without_starting_tasks() {
    for interval in [std::time::Duration::ZERO, std::time::Duration::MAX] {
        let store = store_with_expired_lease();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let error = serve(
            listener,
            ServerState {
                store: Arc::clone(&store),
                clock: Arc::new(SystemClock),
                security: common::security(),
                issuer: None,
            },
            interval,
            std::future::pending(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(Arc::strong_count(&store), 1);
    }
}

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

/// The state the sweep fixture's hooks share and the tests observe.
///
/// These were the double's own fields. Only five of the nine are ever varied,
/// so the rest come from `Default` rather than being respelled at ten sites.
struct SweepState {
    failures_left: AtomicU32,
    fail_on_call: Option<u32>,
    fail_rollover: AtomicBool,
    calls: AtomicU32,
    called: tokio::sync::Notify,
    cycles: AtomicU32,
    next_cycle: AtomicBool,
    control: Option<Control>,
    ping_healthy: AtomicBool,
}

impl Default for SweepState {
    fn default() -> Self {
        Self {
            failures_left: AtomicU32::new(0),
            fail_on_call: None,
            fail_rollover: AtomicBool::new(false),
            calls: AtomicU32::new(0),
            called: tokio::sync::Notify::new(),
            cycles: AtomicU32::new(0),
            next_cycle: AtomicBool::new(true),
            control: None,
            ping_healthy: AtomicBool::new(true),
        }
    }
}

impl SweepState {
    fn failing(self, batches: u32) -> Self {
        Self {
            failures_left: AtomicU32::new(batches),
            ..self
        }
    }

    fn failing_on_call(self, call: u32) -> Self {
        Self {
            fail_on_call: Some(call),
            ..self
        }
    }

    fn failing_rollover(self) -> Self {
        Self {
            fail_rollover: AtomicBool::new(true),
            ..self
        }
    }

    fn controlled(self, control: Control) -> Self {
        Self {
            control: Some(control),
            ..self
        }
    }
}

type SweepStore = DelegatingStore<MemoryStore>;

/// Wraps a real store, failing reclaim batches on a script and counting how
/// often the sweep ran.
///
/// `reclaim_expired` is deliberately left un-hooked. Its trait default is
/// written over `reclaim_expired_batch`, so the wrapper inherits it and the
/// drain re-enters the hook below — which is what carries an injected failure
/// out to the `/reclaim` route. A hand-written delegation would naturally
/// forward `reclaim_expired` to the inner store instead, and the failure would
/// vanish with no compile error (#83).
fn flaky(inner: Arc<MemoryStore>, state: &Arc<SweepState>) -> Arc<SweepStore> {
    let (batch, rollover, ping) = (Arc::clone(state), Arc::clone(state), Arc::clone(state));
    Arc::new(
        DelegatingStore::wrapping(inner)
            .on_reclaim_expired_batch(move |inner, now, limit| {
                let state = Arc::clone(&batch);
                async move {
                    let call = state.calls.fetch_add(1, Ordering::AcqRel) + 1;
                    if state.next_cycle.swap(false, Ordering::AcqRel) {
                        state.cycles.fetch_add(1, Ordering::AcqRel);
                        state.called.notify_one();
                    }
                    if let Some(control) = &state.control {
                        control.call("reclaim").await?;
                    }
                    if state.fail_on_call == Some(call) {
                        return Err(StoreError(
                            "backend failure: password=fixture-maintenance-sensitive-70".into(),
                        ));
                    }
                    if state
                        .failures_left
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                        .is_ok()
                    {
                        return Err(StoreError(
                            "backend failure: password=fixture-maintenance-sensitive-70".into(),
                        ));
                    }
                    inner.reclaim_expired_batch(now, limit).await
                }
            })
            .on_roll_due_periods(move |inner, now, limit| {
                let state = Arc::clone(&rollover);
                async move {
                    let result = async {
                        if let Some(control) = &state.control {
                            control.call("budget-rollover").await?;
                        }
                        if state.fail_rollover.load(Ordering::Acquire) {
                            return Err(StoreError(
                                "backend failure: password=fixture-maintenance-sensitive-70".into(),
                            ));
                        }
                        AdminStore::roll_due_periods(&*inner, now, limit).await
                    }
                    .await;
                    // A terminal rollover outcome ends the cycle. Saturated
                    // batches are additional calls inside one cycle, never
                    // evidence of a later tick.
                    if match &result {
                        Ok(batch) => !batch.is_saturated(),
                        Err(_) => true,
                    } {
                        state.next_cycle.store(true, Ordering::Release);
                    }
                    result
                }
            })
            .on_ping(move |_| {
                let state = Arc::clone(&ping);
                async move {
                    // A successful ping is deliberately independent of
                    // maintenance health.
                    if state.ping_healthy.load(Ordering::Acquire) {
                        Ok(())
                    } else {
                        Err(StoreError("private-fixture-ping-71".into()))
                    }
                }
            })
            .on_consolidate(|_, _, _, _, _, _, _| async {
                unreachable!("the sweep never consolidates")
            }),
    )
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

async fn wait_for_cycles(state: &SweepState, minimum: u32) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let called = state.called.notified();
            if state.cycles.load(Ordering::Acquire) >= minimum {
                return;
            }
            called.await;
        }
    })
    .await
    .expect("maintenance must reach the observed cycle");
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
    store: Arc<SweepStore>,
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
            issuer: None,
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

/// Observe completed cycles, then stop. Entering the next reclaim call proves
/// that the prior cycle, including its rollover and outcome logs, finished.
/// The timeout detects a broken driver; elapsed time is never success evidence.
async fn serve_briefly(
    store: Arc<SweepStore>,
    state: Arc<SweepState>,
    clock: Arc<dyn tollgate_store::Clock>,
) {
    let (server, stop_tx) = spawn_server(
        Arc::clone(&store),
        clock,
        std::time::Duration::from_millis(10),
    )
    .await;
    wait_for_cycles(&state, 5).await;
    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();
}

/// The server actually runs the sweep: an expired lease's units come back
/// without anyone asking.
#[tokio::test]
async fn the_server_reclaims_expired_leases_on_its_interval() {
    let inner = store_with_expired_lease();
    let grant = inner
        .acquire(ACCOUNT, CostUnits(400), SignedDuration::from_secs(1), t(0))
        .await
        .unwrap()
        .grant;
    assert_eq!(inner.balance(ACCOUNT), CostUnits(600));

    let state = Arc::new(SweepState::default());
    let store = flaky(Arc::clone(&inner), &state);
    let (captor, guard) = capture();
    // A clock past the lease's expiry, so the sweep has something to reclaim.
    serve_briefly(
        Arc::clone(&store),
        Arc::clone(&state),
        Arc::new(tollgate_store::ManualClock::new(t(120))),
    )
    .await;
    drop(guard);

    assert!(state.calls.load(Ordering::Acquire) > 0, "the sweep ran");
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

    let state = Arc::new(SweepState::default());
    let store = flaky(Arc::clone(&inner), &state);
    let (server, stop_tx) = spawn_server(
        Arc::clone(&store),
        Arc::new(tollgate_store::ManualClock::new(t(120))),
        std::time::Duration::from_secs(3_600),
    )
    .await;

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while state.calls.load(Ordering::Acquire) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first sweep cycle must drain both batches");
    assert_eq!(state.calls.load(Ordering::Acquire), 2);
    assert_eq!(
        state.cycles.load(Ordering::Acquire),
        1,
        "saturated batches do not count as additional cycles"
    );
    assert_eq!(inner.balance(ACCOUNT), CostUnits(1_000));

    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();
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
    let state = Arc::new(SweepState::default().failing_on_call(2));
    let store = flaky(inner, &state);
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
    server.await.unwrap().unwrap();
}

/// Recovery is its own signal: an operator watching a failing sweep needs to
/// learn it came back, and how long it was down for.
#[tokio::test]
async fn a_recovering_sweep_reports_how_many_failures_it_took() {
    let state = Arc::new(SweepState::default().failing(2));
    let store = flaky(store_with_expired_lease(), &state);
    let (captor, guard) = capture();
    serve_briefly(
        Arc::clone(&store),
        Arc::clone(&state),
        Arc::new(SystemClock),
    )
    .await;
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
/// readiness also falls and persistent failures escalate to error.
#[tokio::test]
async fn a_failing_sweep_reports_consecutive_failures() {
    let (captor, guard) = capture();

    let state = Arc::new(SweepState::default().failing(u32::MAX));
    let store = flaky(store_with_expired_lease(), &state);
    serve_briefly(
        Arc::clone(&store),
        Arc::clone(&state),
        Arc::new(SystemClock),
    )
    .await;
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
        .filter(|event| event.level == Level::WARN || event.level == Level::ERROR)
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
    let state = Arc::new(SweepState::default().failing_rollover());
    let store = flaky(store_with_expired_lease(), &state);
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

    let state = Arc::new(SweepState::default());
    let store = flaky(Arc::clone(&inner), &state);
    let (captor, guard) = capture();
    // 2026-02-14, past the boundary the epoch-stamped account still sits
    // behind.
    serve_briefly(
        Arc::clone(&store),
        Arc::clone(&state),
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
    let state = Arc::new(SweepState::default());
    let store = flaky(Arc::clone(&inner), &state);
    let (captor, guard) = capture();
    serve_briefly(
        Arc::clone(&store),
        Arc::clone(&state),
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
    let state = Arc::new(SweepState::default());
    let store = flaky(store_with_expired_lease(), &state);
    let (server, _stop) = spawn_server(
        Arc::clone(&store),
        Arc::new(SystemClock),
        std::time::Duration::from_secs(3600),
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while state.calls.load(Ordering::Acquire) == 0 {
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
