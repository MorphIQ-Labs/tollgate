//! Behavior tests for the lease manager and usage writer (INVARIANTS.md #5,
//! #6, #8, #9's client half).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};

use tollgate_admission::LeaseSlot;
use tollgate_client::{
    Clock, LeaseManager, LeaseManagerConfig, ManualClock, UsageWriter, UsageWriterConfig,
};
use tollgate_core::{AccountId, CostUnits, DenyReason, RequestId, UsageEvent};
use tollgate_store::{
    AccountConfig, GrantPolicy, IngestReport, LeaseAllocator, MemoryStore, StoreError, UsageSink,
};

const ACCOUNT: AccountId = AccountId(1);

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

fn store(balance: u64) -> Arc<MemoryStore> {
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(3_600),
        reclaim_grace: SignedDuration::ZERO,
    });
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(balance),
        active: true,
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
    }
}

async fn settle() {
    // Paused-clock runtimes auto-advance: this lets background tasks run
    // through several poll intervals deterministically.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

#[tokio::test(start_paused = true)]
async fn refill_installs_lease_on_cold_start() {
    let store = store(10_000);
    let slot = LeaseSlot::empty();
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(store.clone(), Arc::clone(&slot), clock, manager_config());

    settle().await;
    let lease = slot.load().expect("lease installed");
    assert_eq!(lease.grant().units, CostUnits(1_000));
    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn rotation_at_low_water_installs_fresh_lease() {
    let store = store(10_000);
    let slot = LeaseSlot::empty();
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(store.clone(), Arc::clone(&slot), clock, manager_config());
    settle().await;

    let first = slot.load().unwrap();
    // Spend down to the low-water mark; the manager should rotate.
    first.try_debit(CostUnits(800), t(0)).unwrap();
    assert!(first.needs_refill());
    settle().await;

    let second = slot.load().unwrap();
    assert_ne!(first.grant().lease_id, second.grant().lease_id);
    assert!(second.grant().fencing_token > first.grant().fencing_token);

    // While this test still holds an Arc to the superseded lease (standing in
    // for an in-flight reservation), the manager must NOT release it: its
    // remaining count is not final yet.
    assert_eq!(first.remaining(), CostUnits(200));
    settle().await;
    assert_eq!(store.balance(ACCOUNT), CostUnits(8_000)); // two 1000 grants out

    // Once the last outside Arc drops, the lease has quiesced and its 200
    // unspent units flow back to the account.
    drop(first);
    settle().await;
    assert_eq!(store.balance(ACCOUNT), CostUnits(8_200));
    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn expired_slot_fails_closed_then_recovers() {
    // 2000 units: the manager's 1000 grant plus 1000 for the drain lease
    // that starves re-acquisition.
    let store = store(2_000);
    let slot = LeaseSlot::empty();
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(
        store.clone(),
        Arc::clone(&slot),
        Arc::clone(&clock) as Arc<dyn Clock>,
        manager_config(),
    );
    settle().await;
    assert!(slot.load().is_some());

    // Drain the account so the manager cannot re-acquire, then expire the
    // held lease: the slot must clear (deny) rather than serve a dead lease.
    let drain = store
        .acquire(
            ACCOUNT,
            CostUnits(u64::MAX),
            SignedDuration::from_secs(3_600),
            t(0),
        )
        .await
        .unwrap();
    clock.set(t(120)); // past the manager lease's 60s TTL
    settle().await;
    assert!(slot.load().is_none(), "expired slot must fail closed");

    // Balance returns (the drain lease is released) — recovery is automatic.
    store
        .release(drain.lease_id, drain.fencing_token, drain.units, t(121))
        .await
        .unwrap();
    settle().await;
    assert!(
        slot.load().is_some(),
        "manager recovers once balance exists"
    );
    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_releases_unspent_units() {
    let store = store(10_000);
    let slot = LeaseSlot::empty();
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(store.clone(), Arc::clone(&slot), clock, manager_config());
    settle().await;
    slot.load()
        .unwrap()
        .try_debit(CostUnits(300), t(0))
        .unwrap();

    manager.shutdown().await;
    assert!(slot.load().is_none());
    // 10_000 - 1_000 grant + 700 released = 9_700; the 300 spent stay out
    // (settlement loss until usage lands — callers flush first in real use).
    assert_eq!(store.balance(ACCOUNT), CostUnits(9_700));
}

fn event(request: u128, units: u64, lease: &tollgate_core::LeaseGrant) -> UsageEvent {
    UsageEvent {
        request_id: RequestId(request),
        account_id: lease.account_id,
        lease_id: lease.lease_id,
        fencing_token: lease.fencing_token,
        units: CostUnits(units),
        occurred_at: t(0),
    }
}

fn writer_config(capacity: usize) -> UsageWriterConfig {
    UsageWriterConfig {
        queue_capacity: capacity,
        max_batch: 4,
        flush_interval: std::time::Duration::from_millis(10),
        retry_backoff: std::time::Duration::from_millis(10),
    }
}

#[tokio::test(start_paused = true)]
async fn writer_flushes_batches_idempotently() {
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
    let clock = Arc::new(ManualClock::new(t(0)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(64));

    // Ten events, one duplicated request id: nine accepted, one duplicate.
    for i in 0..10u128 {
        let request = if i == 9 { 0 } else { i };
        recorder
            .try_reserve()
            .unwrap()
            .record(event(request, 10, &lease));
    }
    settle().await;
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(90));

    let stats = writer.shutdown().await;
    assert_eq!(stats.accepted, 9);
    assert_eq!(stats.duplicate, 1);
    assert_eq!(stats.lost, 0);
}

#[tokio::test(start_paused = true)]
async fn full_queue_sheds_before_admission() {
    let store = store(10_000);
    let clock = Arc::new(ManualClock::new(t(0)));
    // Capacity 2 with the writer effectively idle (nothing sent yet): two
    // permits reserve the whole queue; the third sheds.
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(2));
    let p1 = recorder.try_reserve().unwrap();
    let p2 = recorder.try_reserve().unwrap();
    assert_eq!(
        recorder.try_reserve().err(),
        Some(DenyReason::AccountingBackpressure)
    );
    // Dropping a permit (deny/cancel path) frees its slot.
    drop(p1);
    let _p3 = recorder.try_reserve().unwrap();
    drop(p2);
    writer.shutdown().await;
}

/// A sink that fails its first N calls, then delegates to the store.
struct FlakySink {
    inner: Arc<MemoryStore>,
    failures_left: AtomicU32,
}

#[async_trait]
impl UsageSink for FlakySink {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        now: Timestamp,
    ) -> Result<IngestReport, StoreError> {
        if self
            .failures_left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(StoreError("injected outage".into()));
        }
        self.inner.ingest(events, now).await
    }
}

#[tokio::test(start_paused = true)]
async fn writer_retries_through_outage_without_losing_events() {
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
    let clock = Arc::new(ManualClock::new(t(0)));
    let sink = Arc::new(FlakySink {
        inner: store.clone(),
        failures_left: AtomicU32::new(3),
    });
    let (recorder, writer) = UsageWriter::spawn(sink, clock, writer_config(64));

    recorder.try_reserve().unwrap().record(event(1, 25, &lease));
    settle().await;
    settle().await;
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(25));
    let stats = writer.shutdown().await;
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.lost, 0);
}

/// Review finding #2 regression: shutdown during an *ongoing* outage must
/// still terminate via the bounded final flush, not hang in the retry loop.
#[tokio::test(start_paused = true)]
async fn shutdown_during_outage_terminates_and_reports_loss() {
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
    let clock = Arc::new(ManualClock::new(t(0)));
    // Sink that never recovers.
    let sink = Arc::new(FlakySink {
        inner: store.clone(),
        failures_left: AtomicU32::new(u32::MAX),
    });
    let (recorder, writer) = UsageWriter::spawn(sink, clock, writer_config(64));

    recorder.try_reserve().unwrap().record(event(1, 25, &lease));
    recorder.try_reserve().unwrap().record(event(2, 25, &lease));
    // Let the writer enter its retry loop against the dead sink.
    settle().await;

    // The whole point: this must complete (bounded final flush), not hang.
    let stats = tokio::time::timeout(std::time::Duration::from_secs(60), writer.shutdown())
        .await
        .expect("shutdown must terminate during an outage");
    assert_eq!(stats.lost, 2);
    assert_eq!(stats.accepted, 0);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits::ZERO);
}

/// Companion: a shutdown signalled during the outage still delivers if the
/// sink recovers before the final flush runs out of attempts.
#[tokio::test(start_paused = true)]
async fn shutdown_after_recovery_delivers_everything() {
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
    let clock = Arc::new(ManualClock::new(t(0)));
    // Fails long enough to outlast several retry backoffs, then recovers in
    // time for the final flush.
    let sink = Arc::new(FlakySink {
        inner: store.clone(),
        failures_left: AtomicU32::new(4),
    });
    let (recorder, writer) = UsageWriter::spawn(sink, clock, writer_config(64));
    recorder.try_reserve().unwrap().record(event(1, 25, &lease));
    settle().await;

    let stats = tokio::time::timeout(std::time::Duration::from_secs(60), writer.shutdown())
        .await
        .expect("shutdown must terminate");
    assert_eq!(stats.lost, 0);
    assert_eq!(stats.accepted, 1);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(25));
}

/// A lease-expiry straggler: events flushed after reclaim are rejected and
/// reported, never silently absorbed (bounded billing loss, INVARIANTS.md #9
/// documentation).
#[tokio::test(start_paused = true)]
async fn straggler_usage_after_reclaim_is_reported_rejected() {
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
    store.reclaim_expired(t(120)).await.unwrap();

    let clock = Arc::new(ManualClock::new(t(121)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8));
    recorder.try_reserve().unwrap().record(event(1, 10, &lease));
    settle().await;
    let stats = writer.shutdown().await;
    assert_eq!(stats.rejected, 1);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits::ZERO);
}
