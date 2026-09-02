//! Behavior tests for the lease manager and usage writer (INVARIANTS.md #5,
//! #6, #8, #9's client half).
// Exercises the deprecated one-shot surface on purpose: it is supported for a
// minor and must keep working.
#![allow(deprecated)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{
    AdmissionEngine, AdmissionRequest, Admitted, ArcSwapSnapshotMap, LeaseSlot, SnapshotMap,
};
use tollgate_client::{
    Clock, LeaseManager, LeaseManagerConfig, ManualClock, UsageWriter, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, DenyReason, Generation,
    LeaseGrant, LocalLease, LocalSharding, OpIndex, PermissionBits, Principal, RequestId,
    ResolvedLimits, UsageEvent, UsageSource,
};
use tollgate_store::{
    AccountConfig, GrantPolicy, IngestReport, LeaseAllocator, MemoryStore, ReclaimBatch,
    StoreError, UsageSink,
};

const ACCOUNT: AccountId = AccountId(1);
const PRINCIPAL: Principal = Principal(1);

#[derive(Clone, Copy)]
struct Operation;

impl OpIndex for Operation {
    fn index(&self) -> usize {
        0
    }
}

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

fn admitted_from_grant(grant: LeaseGrant) -> Admitted {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
    let snapshot = Arc::new(
        AccountSnapshot::builder(
            ACCOUNT,
            Generation(1),
            AccountStatus::Active,
            t(1_000),
            PermissionBits::bit(0),
            ResolvedLimits::new(64),
            Arc::new(
                CostTable::builder(CostUnits(50), CostUnits(50))
                    .weight(&Operation, CostUnits(1))
                    .build(),
            ),
        )
        .build(),
    );
    let slot = LeaseSlot::for_account(ACCOUNT);
    slot.install(Arc::new(LocalLease::new(grant, CostUnits::ZERO)));
    engine.map().install(PRINCIPAL, snapshot, slot);
    engine
        .admit(
            AdmissionRequest {
                principal: PRINCIPAL,
                required: PermissionBits::bit(0),
                op: &Operation,
                items: 1,
            },
            t(0),
        )
        .unwrap()
}

fn store(balance: u64) -> Arc<MemoryStore> {
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

async fn settle() {
    // Paused-clock runtimes auto-advance: this lets background tasks run
    // through several poll intervals deterministically.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

#[tokio::test(start_paused = true)]
async fn refill_installs_lease_on_cold_start() {
    let store = store(10_000);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager =
        LeaseManager::spawn(store.clone(), Arc::clone(&slot), clock, manager_config()).unwrap();

    settle().await;
    let lease = slot.load().expect("lease installed");
    assert_eq!(lease.grant().units, CostUnits(1_000));
    manager.shutdown().await;
}

/// Issue #10: refill used to begin only when the poll timer fired, so a burst
/// could drain a lease between ticks and deny against an account that is
/// funded. The poll interval here is a minute — far longer than the test runs
/// — so a tick cannot explain the rotation. Only the crossing debit can.
#[tokio::test(start_paused = true)]
async fn refill_begins_on_the_crossing_debit_not_the_next_tick() {
    let store = store(10_000);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(
        store.clone(),
        Arc::clone(&slot),
        clock,
        LeaseManagerConfig {
            poll_interval: std::time::Duration::from_secs(60),
            ..manager_config()
        },
    )
    .unwrap();

    // The cold-start acquire still comes from the interval's immediate first
    // tick — there is no lease to spend, so nothing could have signalled.
    settle().await;
    let first = slot.load().expect("cold start installs a lease");
    let first_id = first.grant().lease_id;

    // Cross low water: 1000 - 800 = 200, at or below the 250 mark.
    first.try_debit(CostUnits(800), t(0)).unwrap();
    assert!(first.needs_refill());
    drop(first);

    settle().await;
    let second = slot.load().expect("a replacement must be installed");
    assert_ne!(
        second.grant().lease_id,
        first_id,
        "the crossing debit must have started a refill well inside the \
         60s poll interval; on the polling-only design this is still the \
         original lease"
    );
    manager.shutdown().await;
}

/// The user-visible bug, and the witness for INVARIANTS.md #6: sustained
/// spending against a funded account must not start denying just because a
/// rotation fell due between ticks.
#[tokio::test(start_paused = true)]
async fn a_burst_across_a_rotation_never_denies_a_funded_account() {
    let store = store(1_000_000);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(
        store.clone(),
        Arc::clone(&slot),
        clock,
        LeaseManagerConfig {
            // Long enough that polling alone cannot keep this burst funded:
            // the 1000-unit grants below are spent in ~13 iterations.
            poll_interval: std::time::Duration::from_secs(60),
            ..manager_config()
        },
    )
    .unwrap();
    settle().await;

    let mut denied = 0;
    let mut spent = 0u64;
    for _ in 0..200 {
        match slot.load() {
            Some(lease) => match lease.try_debit(CostUnits(75), t(0)) {
                Ok(()) => spent += 75,
                Err(_) => denied += 1,
            },
            None => denied += 1,
        }
        // Let the refill task act on any request the debit above raised.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    assert_eq!(
        denied, 0,
        "a funded account was refused {denied} times across rotations"
    );
    assert_eq!(spent, 200 * 75);
    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn adaptive_tail_grant_does_not_rotate_while_unspent() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(50),
        status: AccountStatus::Active,
    });
    let clock = Arc::new(ManualClock::new(t(0)));
    let slot = LeaseSlot::for_account(ACCOUNT);
    let manager = LeaseManager::spawn(
        store.clone(),
        Arc::clone(&slot),
        clock,
        LeaseManagerConfig {
            target_grant: CostUnits(500),
            low_water: CostUnits(100),
            ..manager_config()
        },
    )
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    let first = slot.load().unwrap();
    assert_eq!(first.grant().units, CostUnits(25));
    let lease_id = first.grant().lease_id;
    drop(first);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(slot.load().unwrap().grant().lease_id, lease_id);
    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn rotation_at_low_water_installs_fresh_lease() {
    let store = store(10_000);
    let slot = LeaseSlot::with_sharding(ACCOUNT, LocalSharding::new(NonZeroUsize::new(8).unwrap()));
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager =
        LeaseManager::spawn(store.clone(), Arc::clone(&slot), clock, manager_config()).unwrap();
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
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(
        store.clone(),
        Arc::clone(&slot),
        Arc::clone(&clock) as Arc<dyn Clock>,
        manager_config(),
    )
    .unwrap();
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
async fn usability_window_rollover_returns_unspent_capacity() {
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(3_600),
        reclaim_grace: SignedDuration::from_secs(30),
    })
    .unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(1_000),
        status: AccountStatus::Active,
    });
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let mut config = manager_config();
    config.expiry_safety_margin = SignedDuration::from_secs(5);
    let manager = LeaseManager::spawn(
        store.clone(),
        Arc::clone(&slot),
        Arc::clone(&clock) as Arc<dyn Clock>,
        config,
    )
    .unwrap();
    settle().await;
    let first = slot.load().unwrap().grant().lease_id;

    clock.set(t(55));
    settle().await;
    let replacement = slot.load().expect("capacity should rotate during grace");
    assert_ne!(replacement.grant().lease_id, first);
    assert_eq!(replacement.remaining(), CostUnits(1_000));
    manager.shutdown().await;
}

#[test]
fn invalid_lease_manager_durations_are_rejected() {
    let mut config = manager_config();
    config.expiry_safety_margin = SignedDuration::from_secs(-1);
    assert!(config.validate().is_err());

    let mut config = manager_config();
    config.poll_interval = std::time::Duration::ZERO;
    assert!(config.validate().is_err());
}

#[tokio::test(start_paused = true)]
async fn shutdown_releases_unspent_units() {
    let store = store(10_000);
    let slot = LeaseSlot::with_sharding(ACCOUNT, LocalSharding::new(NonZeroUsize::new(8).unwrap()));
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager =
        LeaseManager::spawn(store.clone(), Arc::clone(&slot), clock, manager_config()).unwrap();
    let health = manager.health();
    settle().await;
    slot.load()
        .unwrap()
        .try_debit(CostUnits(300), t(0))
        .unwrap();

    manager.shutdown().await;
    assert!(!*health.borrow());
    assert!(health.has_changed().is_err());
    assert!(slot.load().is_none());
    // 10_000 - 1_000 grant + 700 released = 9_700; the 300 spent stay out
    // (settlement loss until usage lands — callers flush first in real use).
    assert_eq!(store.balance(ACCOUNT), CostUnits(9_700));
}

fn event(request: u128, units: u64, lease: &tollgate_core::LeaseGrant) -> UsageEvent {
    UsageEvent {
        request_id: RequestId(request),
        account_id: lease.account_id,
        source: UsageSource::Leased {
            lease_id: lease.lease_id,
            fencing_token: lease.fencing_token,
        },
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
        shutdown_drain_deadline: std::time::Duration::from_secs(60),
        ingest_timeout: std::time::Duration::from_secs(5),
    }
}

/// Issue #4: the refill task's numbers existed only as `tracing` events with
/// nothing to threshold on, and a shutdown report nobody sees while the
/// process runs. A refusal must be attributable to its reason — that is what
/// separates "the account is out of balance" from "the allocator is down".
#[tokio::test(start_paused = true)]
async fn refill_counters_attribute_refusals_to_their_reason() {
    // An account that does not exist: every acquire is refused, always the
    // same way.
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager =
        LeaseManager::spawn(store.clone(), Arc::clone(&slot), clock, manager_config()).unwrap();
    let counters = manager.counters();

    assert_eq!(counters.snapshot().refused(), 0, "nothing tried yet");
    settle().await;

    let stats = counters.snapshot();
    assert_eq!(stats.acquired, 0);
    assert_eq!(stats.acquired_units, 0);
    assert!(stats.refused() > 0, "the allocator refused every acquire");
    let refusals: Vec<_> = stats
        .refusals_by_name()
        .filter(|(_, count)| *count > 0)
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        refusals,
        vec!["unknown_account"],
        "one reason, and the others left alone: {:?}",
        stats.acquire_refused
    );
    assert_eq!(stats.acquire_timeouts, 0, "a refusal is not a timeout");

    manager.shutdown().await;
}

/// A healthy refill counts what it was granted, not just that it succeeded:
/// adaptive allocation can return less than `target_grant`, so the units are
/// the number that says whether the instance is actually being funded.
#[tokio::test(start_paused = true)]
async fn refill_counters_record_grants_and_releases() {
    let store = store(10_000);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager =
        LeaseManager::spawn(store.clone(), Arc::clone(&slot), clock, manager_config()).unwrap();
    let counters = manager.counters();
    settle().await;

    let stats = counters.snapshot();
    assert_eq!(stats.acquired, 1);
    assert_eq!(stats.acquired_units, 1_000, "the grant, not the request");
    assert_eq!(stats.refused(), 0);
    assert_eq!(stats.released, 0, "nothing returned while still serving");
    assert_eq!(stats.abandoned, 0);

    manager.shutdown().await;
    // Shutdown returns the lease, and the running counter says so too — the
    // report is no longer the only place that knows.
    assert_eq!(counters.snapshot().released, 1);
    assert_eq!(counters.snapshot().abandoned, 0);
}

/// Issue #38: the accounting numbers used to exist only as a local on the
/// writer task's stack, so a process that kept running — or died — reported
/// nothing. They are now readable at any time, and `shutdown` reports the very
/// same counters, so the running totals and the final report cannot disagree.
#[tokio::test(start_paused = true)]
async fn running_totals_are_readable_and_match_the_final_report() {
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
    let clock = Arc::new(ManualClock::new(t(100)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(64)).unwrap();

    // Nothing has happened yet: an ingest age of `None` is how a freshly
    // started writer differs from one whose sink has gone quiet.
    let health = recorder.health();
    assert_eq!(health.stats, tollgate_client::WriterStats::ZERO);
    assert_eq!(health.last_ingest_at, None);
    assert_eq!(health.ingest_age(t(100)), None);
    assert_eq!(health.queue_capacity, 64);

    for i in 0..6u128 {
        let request = if i == 5 { 0 } else { i };
        recorder
            .try_reserve()
            .unwrap()
            .record(event(request, 10, &lease));
    }
    settle().await;

    // Read while the task is still running — the whole point of the issue.
    let mid_flight = recorder.health();
    assert_eq!(mid_flight.stats.accepted, 5);
    assert_eq!(mid_flight.stats.duplicate, 1);
    assert_eq!(mid_flight.stats.lost, 0);
    assert_eq!(
        mid_flight.unaccounted, 0,
        "every event has a billing outcome once flushed"
    );
    assert_eq!(
        mid_flight.last_ingest_at,
        Some(t(100)),
        "the sink answered, at the instant the batch was ingested with"
    );
    assert_eq!(
        mid_flight.ingest_age(t(160)),
        Some(SignedDuration::from_secs(60))
    );

    let stats = writer.shutdown().await.unwrap();
    assert_eq!(
        stats, mid_flight.stats,
        "the final report is a read of the same counters, not a second tally"
    );
}

/// Backpressure must be visible *before* it sheds, which is what the queue
/// gauge is for; and the shed itself is counted where it happens, so an
/// embedder cannot forget to.
#[tokio::test(start_paused = true)]
async fn queue_depth_rises_before_the_shed_and_sheds_are_counted() {
    let store = store(10_000);
    let clock = Arc::new(ManualClock::new(t(0)));
    // Capacity 2, and the permits are held rather than sent, so the queue
    // stays full and the writer cannot drain it.
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(2)).unwrap();

    assert_eq!(recorder.health().queue_depth, 0);
    let first = recorder.try_reserve().unwrap();
    assert_eq!(recorder.health().queue_depth, 1, "a held permit is depth");
    // The writer half reports the same queue, through a weak handle that must
    // not itself keep the channel open. Asserting the depth here — not just
    // that it is nonzero — pins the occupancy arithmetic on both sides.
    assert_eq!(writer.health().queue_depth, 1);
    assert_eq!(writer.health().queue_capacity, 2);

    let second = recorder.try_reserve().unwrap();
    let full = recorder.health();
    assert_eq!(full.queue_depth, full.queue_capacity);
    assert_eq!(full.shed, 0, "full is not yet shed");
    assert_eq!(writer.health().queue_depth, 2);

    assert_eq!(
        recorder.try_reserve().err(),
        Some(DenyReason::AccountingBackpressure)
    );
    assert_eq!(recorder.health().shed, 1);
    assert_eq!(
        recorder.try_reserve().err(),
        Some(DenyReason::AccountingBackpressure)
    );
    assert_eq!(recorder.health().shed, 2);

    drop(first);
    drop(second);
    // The permits were reserved and dropped, never sent: nothing was billed,
    // and a shed is not a charge that went missing.
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats, tollgate_client::WriterStats::ZERO);
}

/// The signal that separates "quiet" from "the sink has been down for twenty
/// minutes": a failing sink must not advance the last-answered time, however
/// many attempts it makes.
#[tokio::test(start_paused = true)]
async fn a_failing_sink_does_not_advance_the_last_ingest_time() {
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
    // Never recovers on its own: the outage lasts until this test ends it.
    let sink = Arc::new(FlakySink {
        inner: store.clone(),
        failures_left: AtomicU32::new(u32::MAX),
    });
    let clock = Arc::new(ManualClock::new(t(500)));
    let (recorder, writer) =
        UsageWriter::spawn(sink.clone(), clock.clone(), writer_config(64)).unwrap();

    recorder.try_reserve().unwrap().record(event(1, 10, &lease));
    // Twenty minutes of retries, all failing.
    settle().await;
    clock.advance(SignedDuration::from_secs(1_200));
    settle().await;
    let during = recorder.health();
    assert_eq!(
        during.last_ingest_at, None,
        "a sink that has never answered leaves no timestamp to age"
    );
    assert_eq!(during.stats.accepted, 0);
    assert_eq!(
        during.unaccounted, 1,
        "the charge is still in the queue with no billing outcome"
    );

    // Recovery: the sink answers, and only then does the time advance.
    sink.failures_left.store(0, Ordering::Relaxed);
    settle().await;
    let after = recorder.health();
    assert_eq!(after.stats.accepted, 1);
    assert_eq!(after.last_ingest_at, Some(t(1_700)));
    assert_eq!(after.unaccounted, 0);

    // A recovered outage is a clean run by the time shutdown reports it —
    // which is exactly why the age above, not `lost`, is the runtime signal.
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.lost, 0);
}

/// `rejected` is bounded billing loss the sink has already refused, and the
/// issue singles it out because nothing surfaced it before shutdown. It is a
/// runtime signal now.
#[tokio::test(start_paused = true)]
async fn rejected_events_are_visible_while_running() {
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
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(64)).unwrap();

    // A capability token the allocator never attached to this lease: the
    // sink refuses the mismatch.
    let mut mismatched = event(1, 10, &lease);
    mismatched.source = UsageSource::Leased {
        lease_id: lease.lease_id,
        fencing_token: tollgate_core::FencingToken(lease.fencing_token.0 + 99),
    };
    recorder.try_reserve().unwrap().record(mismatched);
    settle().await;

    let health = recorder.health();
    assert_eq!(health.stats.rejected, 1);
    assert_eq!(health.stats.accepted, 0);
    assert_eq!(
        health.unaccounted, 0,
        "refused is still an outcome; it is not left unaccounted"
    );
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.rejected, 1);
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
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(64)).unwrap();

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

    let stats = writer.shutdown().await.unwrap();
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
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(2)).unwrap();
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
    writer.shutdown().await.unwrap();
}

/// A sink that fails its first N calls, then delegates to the store.
struct FlakySink {
    inner: Arc<MemoryStore>,
    failures_left: AtomicU32,
}

struct BatchCappedSink {
    cap: usize,
    largest: AtomicUsize,
    ingested: AtomicUsize,
}

#[async_trait]
impl UsageSink for BatchCappedSink {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, StoreError> {
        self.largest.fetch_max(events.len(), Ordering::AcqRel);
        if events.len() > self.cap {
            return Err(StoreError("batch exceeds sink limit".into()));
        }
        self.ingested.fetch_add(events.len(), Ordering::AcqRel);
        Ok(IngestReport {
            accepted: events.len() as u64,
            ..IngestReport::default()
        })
    }
}

#[tokio::test(start_paused = true)]
async fn steady_state_flushes_in_configured_batch_sizes() {
    let sink = Arc::new(BatchCappedSink {
        cap: 2,
        largest: AtomicUsize::new(0),
        ingested: AtomicUsize::new(0),
    });
    let grant = tollgate_core::LeaseGrant {
        lease_id: tollgate_core::LeaseId(1),
        account_id: ACCOUNT,
        fencing_token: tollgate_core::FencingToken(1),
        units: CostUnits(100),
        expires_at: t(100),
    };
    let (recorder, writer) = UsageWriter::spawn(
        Arc::clone(&sink) as Arc<dyn UsageSink>,
        Arc::new(ManualClock::new(t(0))),
        UsageWriterConfig {
            queue_capacity: 8,
            max_batch: 2,
            flush_interval: std::time::Duration::from_secs(60),
            retry_backoff: std::time::Duration::from_millis(1),
            shutdown_drain_deadline: std::time::Duration::from_secs(60),
            ingest_timeout: std::time::Duration::from_secs(5),
        },
    )
    .unwrap();
    // Let the writer receive one event into a partial batch before the rest
    // arrive. The next bulk receive must be limited to the one remaining
    // slot, even though more events are buffered.
    recorder.try_reserve().unwrap().record(event(0, 1, &grant));
    tokio::task::yield_now().await;
    for request in 1..4 {
        recorder
            .try_reserve()
            .unwrap()
            .record(event(request, 1, &grant));
    }

    settle().await;
    assert_eq!(sink.ingested.load(Ordering::Acquire), 4);
    assert_eq!(sink.largest.load(Ordering::Acquire), 2);

    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 4);
    assert_eq!(stats.lost, 0);
}

#[tokio::test(start_paused = true)]
async fn shutdown_flushes_in_configured_batch_sizes() {
    let sink = Arc::new(BatchCappedSink {
        cap: 2,
        largest: AtomicUsize::new(0),
        ingested: AtomicUsize::new(0),
    });
    let grant = tollgate_core::LeaseGrant {
        lease_id: tollgate_core::LeaseId(1),
        account_id: ACCOUNT,
        fencing_token: tollgate_core::FencingToken(1),
        units: CostUnits(100),
        expires_at: t(100),
    };
    let (recorder, writer) = UsageWriter::spawn(
        Arc::clone(&sink) as Arc<dyn UsageSink>,
        Arc::new(ManualClock::new(t(0))),
        UsageWriterConfig {
            queue_capacity: 8,
            max_batch: 2,
            flush_interval: std::time::Duration::from_secs(60),
            retry_backoff: std::time::Duration::from_millis(1),
            shutdown_drain_deadline: std::time::Duration::from_secs(60),
            ingest_timeout: std::time::Duration::from_secs(5),
        },
    )
    .unwrap();
    for request in 0..6 {
        recorder
            .try_reserve()
            .unwrap()
            .record(event(request, 1, &grant));
    }

    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 6);
    assert_eq!(stats.lost, 0);
    assert_eq!(sink.largest.load(Ordering::Acquire), 2);
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
    let (recorder, writer) = UsageWriter::spawn(sink, clock, writer_config(64)).unwrap();

    recorder.try_reserve().unwrap().record(event(1, 25, &lease));
    settle().await;
    settle().await;
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(25));
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.lost, 0);
}

/// Review finding #3 regression: a panic between commit and response must
/// still bill — ChargeGuard binds the event to the permit at commit time and
/// emits it during unwind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(deprecated)]
async fn panic_after_commit_still_bills() {
    use tollgate_client::ChargeGuard;

    let store = store(10_000);
    let grant = store
        .acquire(
            ACCOUNT,
            CostUnits(1_000),
            SignedDuration::from_secs(60),
            t(0),
        )
        .await
        .unwrap();
    let clock = Arc::new(ManualClock::new(t(0)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();

    let permit = recorder.try_reserve().unwrap();
    let worker = tokio::spawn(async move {
        let admitted = admitted_from_grant(grant);
        let charge = ChargeGuard::commit(admitted, permit, RequestId(9), t(0)).unwrap();
        assert_eq!(charge.units(), CostUnits(51));
        panic!("kernel exploded mid-execution");
    });
    assert!(worker.await.is_err(), "the worker must have panicked");

    // Shutdown drains and flushes whatever the unwind enqueued.
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.lost, 0);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(51));
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
    let (recorder, writer) = UsageWriter::spawn(sink, clock, writer_config(64)).unwrap();

    recorder.try_reserve().unwrap().record(event(1, 25, &lease));
    recorder.try_reserve().unwrap().record(event(2, 25, &lease));
    // Let the writer enter its retry loop against the dead sink.
    settle().await;

    // The whole point: this must complete (bounded final flush), not hang.
    let stats = tokio::time::timeout(std::time::Duration::from_secs(60), writer.shutdown())
        .await
        .expect("shutdown must terminate during an outage")
        .unwrap();
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
    let (recorder, writer) = UsageWriter::spawn(sink, clock, writer_config(64)).unwrap();
    recorder.try_reserve().unwrap().record(event(1, 25, &lease));
    settle().await;

    let stats = tokio::time::timeout(std::time::Duration::from_secs(60), writer.shutdown())
        .await
        .expect("shutdown must terminate")
        .unwrap();
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
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();
    recorder.try_reserve().unwrap().record(event(1, 10, &lease));
    settle().await;
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.rejected, 1);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits::ZERO);
}

// ---- shutdown drain (issue #32) -------------------------------------------
//
// A permit reserved before shutdown must resolve — by sending or dropping —
// before the writer returns, bounded by the drain deadline; a deadline expiry
// is reported in `unresolved`, never a clean flush.

/// Shutdown refuses new reservations from the instant it begins, while a
/// permit reserved earlier still delivers into the drain (INVARIANTS.md #8).
#[tokio::test(start_paused = true)]
async fn reserve_fails_once_shutdown_begins() {
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
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();

    let held = recorder.try_reserve().unwrap();
    let shutdown = tokio::spawn(writer.shutdown());
    settle().await; // the writer has closed the channel and is draining

    assert_eq!(
        recorder.try_reserve().err(),
        Some(DenyReason::AccountingBackpressure)
    );
    assert!(recorder.is_closed(), "readiness must observe the shutdown");

    held.record(event(1, 25, &lease));
    let stats = shutdown.await.unwrap().unwrap();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.unresolved, 0);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(25));
}

/// The #32 defect: a permit that records only after shutdown has begun must
/// be ingested, not silently dropped with zero reported loss.
#[tokio::test(start_paused = true)]
async fn shutdown_waits_for_outstanding_permit() {
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
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();

    let permit = recorder.try_reserve().unwrap();
    let late = event(1, 25, &lease);
    let sender = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        permit.record(late);
    });

    let stats = writer.shutdown().await.unwrap();
    sender.await.unwrap();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.lost, 0);
    assert_eq!(stats.unresolved, 0);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(25));
}

/// INVARIANTS.md #13 across shutdown: a committed guard dropped after
/// shutdown begins still bills.
#[tokio::test(start_paused = true)]
#[allow(deprecated)]
async fn shutdown_waits_for_committed_guard() {
    use tollgate_client::ChargeGuard;

    let store = store(10_000);
    let grant = store
        .acquire(
            ACCOUNT,
            CostUnits(1_000),
            SignedDuration::from_secs(60),
            t(0),
        )
        .await
        .unwrap();
    let clock = Arc::new(ManualClock::new(t(0)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();

    let permit = recorder.try_reserve().unwrap();
    let holder = tokio::spawn(async move {
        let admitted = admitted_from_grant(grant);
        let charge = ChargeGuard::commit(admitted, permit, RequestId(9), t(0)).unwrap();
        assert_eq!(charge.units(), CostUnits(51));
        // The guard outlives the start of shutdown; its drop must still bill.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(charge);
    });

    let stats = writer.shutdown().await.unwrap();
    holder.await.unwrap();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.unresolved, 0);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(51));
}

/// A permit dropped (zero-charge path) after shutdown begins completes the
/// drain — no event, no hang, nothing unresolved.
#[tokio::test(start_paused = true)]
async fn late_permit_drop_completes_drain() {
    let store = store(10_000);
    let clock = Arc::new(ManualClock::new(t(0)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();

    let permit = recorder.try_reserve().unwrap();
    let dropper = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(permit);
    });

    let stats = writer.shutdown().await.unwrap();
    dropper.await.unwrap();
    assert_eq!(stats, tollgate_client::WriterStats::ZERO);
}

/// Deadline expiry can neither hang nor report a clean flush: a permit that
/// never resolves is counted unresolved.
#[tokio::test(start_paused = true)]
async fn drain_deadline_expiry_reports_unresolved() {
    let store = store(10_000);
    let clock = Arc::new(ManualClock::new(t(0)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();

    // A permit that never sends and never drops.
    std::mem::forget(recorder.try_reserve().unwrap());

    let stats = tokio::time::timeout(std::time::Duration::from_secs(120), writer.shutdown())
        .await
        .expect("shutdown must return at the drain deadline, not hang")
        .unwrap();
    assert_eq!(stats.unresolved, 1);
    assert_eq!(stats.lost, 0);
    assert_eq!(stats.accepted, 0);
}

/// The drain's own accounting: duplicates and rejections discovered by the
/// *final* flush are counted as such, not folded into accepted or lost.
#[tokio::test(start_paused = true)]
async fn final_flush_counts_duplicates_and_rejections() {
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
    let mut config = writer_config(16);
    // Nothing flushes before shutdown, so every event meets the sink for the
    // first time inside the final flush.
    config.flush_interval = std::time::Duration::from_secs(3_600);
    config.max_batch = 16;
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, config).unwrap();

    recorder.try_reserve().unwrap().record(event(1, 10, &lease));
    recorder.try_reserve().unwrap().record(event(1, 10, &lease));
    let mut mismatched = event(2, 10, &lease);
    mismatched.source = UsageSource::Leased {
        lease_id: lease.lease_id,
        fencing_token: tollgate_core::FencingToken(999),
    };
    recorder.try_reserve().unwrap().record(mismatched);

    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.duplicate, 1);
    assert_eq!(stats.rejected, 1);
    assert_eq!(stats.lost, 0);
    assert_eq!(stats.unresolved, 0);
}

/// The final flush backs off *between* attempts and never sleeps a backoff
/// no attempt can follow: three attempts, two waits.
#[tokio::test(start_paused = true)]
async fn final_flush_backs_off_only_between_attempts() {
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
        failures_left: AtomicU32::new(u32::MAX),
    });
    let backoff = std::time::Duration::from_millis(100);
    let mut config = writer_config(16);
    config.flush_interval = std::time::Duration::from_secs(3_600);
    config.retry_backoff = backoff;
    let (recorder, writer) = UsageWriter::spawn(sink, clock, config).unwrap();
    recorder.try_reserve().unwrap().record(event(1, 10, &lease));

    let start = tokio::time::Instant::now();
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.lost, 1);
    assert_eq!(start.elapsed(), 2 * backoff, "three attempts, two backoffs");
}

/// Every unresolved permit is counted, not merely detected.
#[tokio::test(start_paused = true)]
async fn drain_deadline_reports_every_unresolved_permit() {
    let store = store(10_000);
    let clock = Arc::new(ManualClock::new(t(0)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();

    std::mem::forget(recorder.try_reserve().unwrap());
    std::mem::forget(recorder.try_reserve().unwrap());

    let stats = tokio::time::timeout(std::time::Duration::from_secs(120), writer.shutdown())
        .await
        .expect("shutdown must return at the drain deadline")
        .unwrap();
    assert_eq!(stats.unresolved, 2);
}

// ---- a dead writer never reports zero loss (issue #41) --------------------

/// A sink that panics once it has been called `panic_after` times.
struct PanickingSink {
    inner: Arc<MemoryStore>,
    calls_before_panic: AtomicU32,
}

#[async_trait]
impl UsageSink for PanickingSink {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        now: Timestamp,
    ) -> Result<IngestReport, StoreError> {
        if self
            .calls_before_panic
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_err()
        {
            panic!("sink exploded mid-ingest");
        }
        self.inner.ingest(events, now).await
    }
}

/// The #41 defect: a writer that dies holding committed charges must say so,
/// not return a zeroed report indistinguishable from a clean shutdown.
#[tokio::test(start_paused = true)]
async fn panicked_writer_reports_unaccounted_charges() {
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
    let sink = Arc::new(PanickingSink {
        inner: store.clone(),
        calls_before_panic: AtomicU32::new(0),
    });
    let mut config = writer_config(16);
    config.flush_interval = std::time::Duration::from_secs(3_600);
    let (recorder, writer) = UsageWriter::spawn(sink, clock, config).unwrap();

    for request in 0..3u128 {
        recorder
            .try_reserve()
            .unwrap()
            .record(event(request, 10, &lease));
    }

    let error = writer
        .shutdown()
        .await
        .expect_err("a panicked writer must not report a clean shutdown");
    assert!(error.panicked);
    assert_eq!(error.unaccounted, 3);
    assert!(
        error.to_string().contains("no billing record"),
        "operator-facing message must name the consequence: {error}"
    );
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits::ZERO);
}

/// The count is what is still unaccounted, not everything ever enqueued:
/// charges already given a billing outcome are excluded.
#[tokio::test(start_paused = true)]
async fn panic_after_partial_flush_counts_only_unflushed() {
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
    // The first batch is ingested; the second kills the task.
    let sink = Arc::new(PanickingSink {
        inner: store.clone(),
        calls_before_panic: AtomicU32::new(1),
    });
    let mut config = writer_config(16);
    config.max_batch = 2;
    config.flush_interval = std::time::Duration::from_secs(3_600);
    let (recorder, writer) = UsageWriter::spawn(sink, clock, config).unwrap();

    for request in 0..2u128 {
        recorder
            .try_reserve()
            .unwrap()
            .record(event(request, 10, &lease));
    }
    settle().await; // first full batch flushes cleanly
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(20));

    for request in 2..5u128 {
        recorder
            .try_reserve()
            .unwrap()
            .record(event(request, 10, &lease));
    }

    let error = writer.shutdown().await.expect_err("the sink panicked");
    assert_eq!(
        error.unaccounted, 3,
        "the two billed charges must not be counted again"
    );
}

// ---- wall-clock bounds on a wedged backend (issue #34) --------------------

/// A sink whose `ingest` never resolves — a backend that is hung rather than
/// erroring, which retry *counts* alone cannot bound.
struct HangingSink;

#[async_trait]
impl UsageSink for HangingSink {
    async fn ingest(
        &self,
        _events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, StoreError> {
        std::future::pending().await
    }
}

/// The #34 defect: a hung ingest parked the writer task forever, and with it
/// every later shutdown step. The drain must terminate and report.
#[tokio::test(start_paused = true)]
async fn hung_ingest_cannot_stall_shutdown() {
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
    let mut config = writer_config(16);
    config.flush_interval = std::time::Duration::from_secs(3_600);
    let (recorder, writer) =
        UsageWriter::spawn(Arc::new(HangingSink) as Arc<dyn UsageSink>, clock, config).unwrap();
    recorder.try_reserve().unwrap().record(event(1, 25, &lease));

    let stats = tokio::time::timeout(std::time::Duration::from_secs(300), writer.shutdown())
        .await
        .expect("a hung sink must not stall shutdown")
        .unwrap();

    // Undeliverable is undeliverable: counted lost, never accepted.
    assert_eq!(stats.lost, 1);
    assert_eq!(stats.accepted, 0);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits::ZERO);
}

/// Steady state: a hung ingest times out into the ordinary retry path rather
/// than parking the task, so a later shutdown still observes the signal.
#[tokio::test(start_paused = true)]
async fn hung_ingest_times_out_into_the_retry_path() {
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
    let mut config = writer_config(16);
    config.ingest_timeout = std::time::Duration::from_millis(100);
    let (recorder, writer) =
        UsageWriter::spawn(Arc::new(HangingSink) as Arc<dyn UsageSink>, clock, config).unwrap();
    recorder.try_reserve().unwrap().record(event(1, 25, &lease));

    // Several timeout+backoff rounds elapse; the task stays responsive.
    settle().await;
    let stats = tokio::time::timeout(std::time::Duration::from_secs(300), writer.shutdown())
        .await
        .expect("the retry loop must remain interruptible")
        .unwrap();
    assert_eq!(stats.accepted, 0);
    assert_eq!(stats.lost, 1);
}

/// A sink that is slow but healthy must still be delivered to: the timeout
/// bounds a hang, it does not shorten the budget of a working call.
struct SlowSink {
    inner: Arc<MemoryStore>,
    delay: std::time::Duration,
}

#[async_trait]
impl UsageSink for SlowSink {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        now: Timestamp,
    ) -> Result<IngestReport, StoreError> {
        tokio::time::sleep(self.delay).await;
        self.inner.ingest(events, now).await
    }
}

#[tokio::test(start_paused = true)]
async fn slow_but_healthy_sink_still_delivers_at_shutdown() {
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
    let sink = Arc::new(SlowSink {
        inner: store.clone(),
        delay: std::time::Duration::from_millis(50),
    });
    let mut config = writer_config(16);
    config.flush_interval = std::time::Duration::from_secs(3_600);
    config.ingest_timeout = std::time::Duration::from_secs(5);
    let (recorder, writer) = UsageWriter::spawn(sink as Arc<dyn UsageSink>, clock, config).unwrap();
    recorder.try_reserve().unwrap().record(event(1, 25, &lease));

    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 1, "a slow call inside its budget delivers");
    assert_eq!(stats.lost, 0);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(25));
}

/// An allocator that grants normally but never answers a release.
/// An allocator that never answers an acquire. Distinct from refusing: the
/// grant may well have been made, so the client cannot treat this as a
/// domain answer.
struct HangingAcquireAllocator;

#[async_trait]
impl LeaseAllocator for HangingAcquireAllocator {
    async fn acquire(
        &self,
        _account: AccountId,
        _requested: CostUnits,
        _ttl: SignedDuration,
        _now: Timestamp,
    ) -> Result<tollgate_core::LeaseGrant, tollgate_store::AllocateError> {
        std::future::pending().await
    }

    async fn release(
        &self,
        _lease_id: tollgate_core::LeaseId,
        _fencing_token: tollgate_core::FencingToken,
        _unspent: CostUnits,
        _now: Timestamp,
    ) -> Result<(), tollgate_store::AllocateError> {
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

/// INVARIANTS.md #18: an allocator that hangs rather than answering is
/// bounded by `store_call_timeout`. It is counted apart from every refusal,
/// because it is not a domain answer — treating it as one would assert the
/// lease was not granted, which this client cannot know.
#[tokio::test(start_paused = true)]
async fn a_hung_acquire_is_counted_as_a_timeout_not_a_refusal() {
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(
        Arc::new(HangingAcquireAllocator) as Arc<dyn LeaseAllocator>,
        Arc::clone(&slot),
        clock,
        LeaseManagerConfig {
            // Short enough that the bound is reached inside a settle; the
            // shared config's five seconds is a production-shaped value.
            store_call_timeout: std::time::Duration::from_millis(10),
            ..manager_config()
        },
    )
    .unwrap();
    let counters = manager.counters();

    settle().await;
    let stats = counters.snapshot();
    assert!(
        stats.acquire_timeouts > 0,
        "the refill loop must not park on a hung allocator"
    );
    assert_eq!(
        stats.refused(),
        0,
        "a timeout is not a refusal: {:?}",
        stats.acquire_refused
    );
    assert_eq!(stats.acquired, 0);
    assert!(slot.load().is_none(), "fail closed while unanswered");

    manager.shutdown().await;
}

struct HangingReleaseAllocator {
    inner: Arc<MemoryStore>,
}

#[async_trait]
impl LeaseAllocator for HangingReleaseAllocator {
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
        _lease_id: tollgate_core::LeaseId,
        _fencing_token: tollgate_core::FencingToken,
        _unspent: CostUnits,
        _now: Timestamp,
    ) -> Result<(), tollgate_store::AllocateError> {
        std::future::pending().await
    }

    async fn reclaim_expired_batch(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError> {
        self.inner.reclaim_expired_batch(now, limit).await
    }
}

/// A clean shutdown accounts for the leases it returned.
#[tokio::test(start_paused = true)]
async fn shutdown_reports_released_leases() {
    let store = store(10_000);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager =
        LeaseManager::spawn(store.clone(), Arc::clone(&slot), clock, manager_config()).unwrap();
    settle().await;
    assert!(slot.load().is_some());

    let report = manager.shutdown().await;
    assert_eq!(report.released, 1);
    assert_eq!(report.abandoned, 0);
    assert!(!report.task_died);
    assert_eq!(store.balance(ACCOUNT), CostUnits(10_000), "units came back");
}

/// A hung release cannot stall shutdown, and what it could not return is
/// reported rather than assumed settled (INVARIANTS.md #18).
#[tokio::test(start_paused = true)]
async fn hung_release_cannot_stall_shutdown() {
    let store = store(10_000);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let allocator = Arc::new(HangingReleaseAllocator {
        inner: store.clone(),
    });
    let manager = LeaseManager::spawn(
        allocator as Arc<dyn LeaseAllocator>,
        Arc::clone(&slot),
        clock,
        manager_config(),
    )
    .unwrap();
    let counters = manager.counters();
    settle().await;
    assert!(slot.load().is_some());
    assert_eq!(counters.snapshot().abandoned, 0, "nothing abandoned yet");

    let report = tokio::time::timeout(std::time::Duration::from_secs(300), manager.shutdown())
        .await
        .expect("a hung release must not stall shutdown");
    assert_eq!(report.released, 0);
    assert_eq!(report.abandoned, 1, "the unreturned lease is reported");
    assert!(!report.task_died);
    // The same number outside the report: stranded units are visible to a
    // scrape, not only to whoever awaited this shutdown.
    let stats = counters.snapshot();
    assert_eq!(stats.abandoned, 1);
    assert_eq!(stats.released, 0);
}

/// INVARIANTS.md #16: no writer field is silently repaired.
#[tokio::test]
async fn invalid_writer_config_is_rejected() {
    let zero = std::time::Duration::ZERO;
    let mut zero_capacity = writer_config(8);
    zero_capacity.queue_capacity = 0;
    let mut zero_batch = writer_config(8);
    zero_batch.max_batch = 0;
    let mut zero_flush = writer_config(8);
    zero_flush.flush_interval = zero;
    let mut zero_backoff = writer_config(8);
    zero_backoff.retry_backoff = zero;
    let mut zero_drain = writer_config(8);
    zero_drain.shutdown_drain_deadline = zero;
    let mut zero_ingest = writer_config(8);
    zero_ingest.ingest_timeout = zero;

    for (field, config) in [
        ("queue_capacity", zero_capacity),
        ("max_batch", zero_batch),
        ("flush_interval", zero_flush),
        ("retry_backoff", zero_backoff),
        ("shutdown_drain_deadline", zero_drain),
        ("ingest_timeout", zero_ingest),
    ] {
        assert!(
            config.validate().is_err(),
            "zero {field} must be rejected, not coerced"
        );
        let store = store(10_000);
        let clock = Arc::new(ManualClock::new(t(0)));
        assert!(
            UsageWriter::spawn(store, clock, config).is_err(),
            "zero {field} must not start a task"
        );
    }
}

/// INVARIANTS.md #16, manager half: the two new bounds are contracts too.
#[test]
fn invalid_lease_manager_timeouts_are_rejected() {
    let mut config = manager_config();
    config.store_call_timeout = std::time::Duration::ZERO;
    assert!(config.validate().is_err());

    let mut config = manager_config();
    config.shutdown_release_deadline = std::time::Duration::ZERO;
    assert!(config.validate().is_err());
}
