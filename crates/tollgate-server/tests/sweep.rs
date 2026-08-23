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

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};

use tollgate_core::{AccountId, CostUnits};
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, LeaseAllocator, MemoryStore, ReclaimedLease,
    StoreError, StoreHealth, SystemClock,
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
    calls: AtomicU32,
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

    async fn reclaim_expired(&self, now: Timestamp) -> Result<Vec<ReclaimedLease>, StoreError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if self
            .failures_left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(StoreError("sweep backend unavailable".into()));
        }
        self.inner.reclaim_expired(now).await
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
    ) -> Result<tollgate_store::IngestReport, StoreError> {
        self.inner.ingest(events, now).await
    }
}

#[async_trait]
impl AdminStore for FlakyReclaimStore {
    async fn create_account(
        &self,
        config: AccountConfig,
    ) -> Result<(), tollgate_store::CreateAccountError> {
        AdminStore::create_account(&*self.inner, config).await
    }

    async fn deposit(
        &self,
        account: AccountId,
        units: CostUnits,
    ) -> Result<(), tollgate_store::AllocateError> {
        AdminStore::deposit(&*self.inner, account, units).await
    }

    async fn set_active(
        &self,
        account: AccountId,
        active: bool,
    ) -> Result<(), tollgate_store::AllocateError> {
        AdminStore::set_active(&*self.inner, account, active).await
    }

    async fn publish_snapshot(
        &self,
        principal: tollgate_core::Principal,
        snapshot: Arc<tollgate_core::AccountSnapshot>,
    ) -> Result<(), StoreError> {
        AdminStore::publish_snapshot(&*self.inner, principal, snapshot).await
    }

    async fn remove_snapshot(&self, principal: tollgate_core::Principal) -> Result<(), StoreError> {
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

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Captor {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut collector = FieldCollector(Vec::new());
        event.record(&mut collector);
        self.0.lock().unwrap().push(Captured {
            level: *event.metadata().level(),
            fields: collector.0,
        });
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
        active: true,
    });
    store
}

/// Run `serve` against the given store for long enough to sweep, then stop.
async fn serve_briefly(store: Arc<FlakyReclaimStore>, clock: Arc<dyn tollgate_store::Clock>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(
        listener,
        ServerState { store, clock },
        std::time::Duration::from_millis(10),
        async move {
            let _ = stop_rx.await;
        },
    ));
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
        calls: AtomicU32::new(0),
    });
    let captor = Captor::default();
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(captor.clone()));
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

/// Recovery is its own signal: an operator watching a failing sweep needs to
/// learn it came back, and how long it was down for.
#[tokio::test]
async fn a_recovering_sweep_reports_how_many_failures_it_took() {
    let store = Arc::new(FlakyReclaimStore {
        inner: store_with_expired_lease(),
        failures_left: AtomicU32::new(2),
        calls: AtomicU32::new(0),
    });
    let captor = Captor::default();
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(captor.clone()));
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
    let captor = Captor::default();
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(captor.clone()));

    let store = Arc::new(FlakyReclaimStore {
        inner: store_with_expired_lease(),
        failures_left: AtomicU32::new(u32::MAX),
        calls: AtomicU32::new(0),
    });
    serve_briefly(Arc::clone(&store), Arc::new(SystemClock)).await;
    drop(guard);

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
