//! The memory backend reports what it is holding (#23).
//!
//! `MemoryStore` never forgets a usage event — it is the idempotency index —
//! so its footprint grows with lifetime request count. That is documented, and
//! documentation is invisible to a process already running, so each reclaim
//! drain also logs the numbers. These tests treat the gauge as behavior: they
//! assert on *structured fields*, never on rendered message text, which would
//! be the source-text assertion AGENTS.md forbids.

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, Once};

use jiff::{SignedDuration, Timestamp};

use tollgate_core::{
    AccountId, AccountStatus, CapacityClass, CostUnits, PolicyRevision, UsageSource,
};
use tollgate_store::{
    AccountConfig, GrantPolicy, LeaseAllocator, MemoryStore, StoredRecords, UsageSink,
};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

const ACCOUNT: AccountId = AccountId(1);
const TTL: SignedDuration = SignedDuration::from_secs(60);

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

#[derive(Debug, Clone)]
struct Captured {
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
    /// Events carrying the gauge, identified by the fields it reports rather
    /// than by its message.
    fn holdings(&self) -> Vec<Captured> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.field("usage_events").is_some())
            .cloned()
            .collect()
    }
}

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

fn store() -> Arc<MemoryStore> {
    install_subscriber();
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(3_600),
        reclaim_grace: SignedDuration::ZERO,
    })
    .expect("policy is valid");
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(1_000_000),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    store
}

/// A drain settles a backlog in bounded batches, and the gauge belongs to the
/// *sweep*, not to each batch: one line per drain, carrying the numbers as
/// they stand once the drain is done. Reporting per batch would multiply the
/// noise by the backlog and quote mid-sweep numbers that were never true at
/// rest.
#[tokio::test]
async fn a_drain_reports_its_holdings_once() {
    let store = store();
    for _ in 0..5 {
        store
            .acquire(ACCOUNT, CostUnits(1), TTL, t(0))
            .await
            .expect("funded");
    }

    let (captor, _guard) = capture();
    let limit = NonZeroUsize::new(2).unwrap();
    let mut batches = 0;
    loop {
        let batch = store
            .reclaim_expired_batch(t(120), limit)
            .await
            .expect("sweep");
        batches += 1;
        if !batch.is_saturated() {
            break;
        }
    }
    assert_eq!(batches, 3, "five leases drain as 2 + 2 + 1");

    let reported = captor.holdings();
    assert_eq!(
        reported.len(),
        1,
        "one report per drain, not one per batch: {reported:?}"
    );
    assert_eq!(reported[0].field("leases"), Some("5"));
    assert_eq!(
        reported[0].field("active_leases"),
        Some("0"),
        "the drain settled every lease, so the live count is what fell to zero"
    );
}

/// An idle sweep still reports. A process serving nothing is exactly when an
/// operator wants to see whether the footprint is still climbing.
#[tokio::test]
async fn a_sweep_with_nothing_to_reclaim_still_reports() {
    let store = store();
    let (captor, _guard) = capture();
    let batch = store
        .reclaim_expired_batch(t(120), NonZeroUsize::new(8).unwrap())
        .await
        .expect("sweep");
    assert!(batch.is_empty());
    assert_eq!(captor.holdings().len(), 1);
}

/// The gauge has to describe reality, or it is worse than none: it would make
/// a growing process look like a steady one. `stored_records` is the same view
/// the sweep logs, so checking it checks both.
#[tokio::test]
async fn holdings_climb_with_traffic_while_the_live_count_returns_to_zero() {
    let store = store();
    assert_eq!(
        store.stored_records(),
        StoredRecords {
            usage_events: 0,
            leases: 0,
            active_leases: 0,
        }
    );

    for request in 0..8u128 {
        let lease = store
            .acquire(ACCOUNT, CostUnits(10), TTL, t(0))
            .await
            .expect("funded");
        assert_eq!(store.stored_records().active_leases, 1);
        store
            .ingest(
                &[tollgate_core::UsageEvent::new(
                    tollgate_core::RequestId(request),
                    ACCOUNT,
                    UsageSource::Leased {
                        lease_id: lease.lease_id,
                        fencing_token: lease.fencing_token,
                    },
                    CostUnits(3),
                    t(1),
                    PolicyRevision::UNSTATED,
                )],
                t(1),
            )
            .await
            .expect("ingest");
        store
            .release(lease.lease_id, lease.fencing_token, CostUnits(7), t(2))
            .await
            .expect("active");
    }

    assert_eq!(
        store.stored_records(),
        StoredRecords {
            usage_events: 8,
            leases: 8,
            active_leases: 0,
        },
        "both totals track what the process has done; only the live count rests"
    );
}
