//! The memory backend reports what it is holding (#23).
//!
//! `MemoryStore` never forgets a usage event — it is the idempotency index —
//! so its footprint grows with lifetime request count. That is documented, and
//! documentation is invisible to a process already running, so each reclaim
//! drain also logs the numbers. These tests treat the gauge as behavior: they
//! assert on *structured fields*, never on rendered message text, which would
//! be the source-text assertion AGENTS.md forbids.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use jiff::{SignedDuration, Timestamp};

use tollgate_core::{AccountId, CostUnits};
use tollgate_store::{
    AccountConfig, GrantPolicy, LeaseAllocator, MemoryStore, StoredRecords, UsageSink,
};
use tracing::field::{Field, Visit};
use tracing::subscriber::DefaultGuard;
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

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Captor {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut collector = FieldCollector(Vec::new());
        event.record(&mut collector);
        self.0.lock().unwrap().push(Captured {
            fields: collector.0,
        });
    }
}

/// Thread-local, so tests stay independent under a parallel runner.
fn capture() -> (Captor, DefaultGuard) {
    let captor = Captor::default();
    let subscriber = tracing_subscriber::registry().with(captor.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    (captor, guard)
}

fn store() -> Arc<MemoryStore> {
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
        active: true,
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
                &[tollgate_core::UsageEvent {
                    request_id: tollgate_core::RequestId(request),
                    account_id: ACCOUNT,
                    lease_id: lease.lease_id,
                    fencing_token: lease.fencing_token,
                    units: CostUnits(3),
                    occurred_at: t(1),
                }],
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
