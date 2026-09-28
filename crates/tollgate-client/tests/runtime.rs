//! Behavior tests for the lease manager and usage writer (INVARIANTS.md GL-5,
//! GL-6, GL-8, GL-9's client half).
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{
    AdmissionEngine, ArcSwapSnapshotMap, LeaseSlot, NoCapacityPermit, NoGate, ReadyToStart,
    SnapshotMap,
};
use tollgate_client::{
    Clock, LeaseManager, LeaseManagerConfig, ManualClock, UsagePermit, UsageWriter,
    UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, DenyReason,
    Generation, LeaseGrant, LocalLease, LocalSharding, OpIndex, PermissionBits, PolicyRevision,
    Principal, RequestId, ResolvedLimits, UsageEvent, UsageSource,
};
#[path = "../../tollgate-store/tests/support/delegating.rs"]
mod delegating;
use delegating::{DelegatingStore, RejectingStore, rejecting};

use tollgate_store::{
    AccountConfig, GrantPolicy, IngestError, IngestReport, LeaseAllocator, MemoryStore,
    ReclaimBatch, StoreError, UsageSink,
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

fn ready_from_grant(
    grant: LeaseGrant,
    permit: UsagePermit,
) -> ReadyToStart<UsagePermit, NoCapacityPermit> {
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
    drop(slot.replace(Arc::new(LocalLease::new(grant, CostUnits::ZERO))));
    engine.map().install(PRINCIPAL, snapshot, slot).unwrap();
    engine
        .begin(PRINCIPAL, PermissionBits::bit(0), t(0))
        .and_then(|context| context.admit(&[(&Operation, 1)], permit, t(0)))
        .expect("the fixture lease funds one request")
        .acquire_capacity(&NoGate)
        .expect("capacity is disabled in this fixture")
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

/// Issue GL-10: refill used to begin only when the poll timer fired, so a burst
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

/// The user-visible bug, and the witness for INVARIANTS.md GL-6: sustained
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
        capacity_class: CapacityClass::Assured,
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
        .unwrap()
        .grant;
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
        capacity_class: CapacityClass::Assured,
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
    for ttl in [SignedDuration::ZERO, SignedDuration::from_nanos(-1)] {
        let mut config = manager_config();
        config.lease_ttl = ttl;
        config.expiry_safety_margin = SignedDuration::ZERO;
        assert!(config.validate().is_err());
    }
    let mut config = manager_config();
    config.expiry_safety_margin = SignedDuration::from_secs(-1);
    assert!(config.validate().is_err());

    let mut config = manager_config();
    config.poll_interval = std::time::Duration::ZERO;
    assert!(config.validate().is_err());
}

#[test]
fn fractional_and_wide_lease_ttls_remain_valid_configuration() {
    for ttl in [
        SignedDuration::from_nanos(1),
        SignedDuration::from_millis(500),
        SignedDuration::from_millis(1_500),
        SignedDuration::from_secs(i64::from(u32::MAX) + 1),
        SignedDuration::MAX,
    ] {
        let mut config = manager_config();
        config.lease_ttl = ttl;
        config.expiry_safety_margin = SignedDuration::ZERO;
        assert_eq!(config.validate(), Ok(()), "TTL {ttl}");
    }
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
    UsageEvent::new(
        RequestId(request),
        lease.account_id,
        UsageSource::Leased {
            lease_id: lease.lease_id,
            fencing_token: lease.fencing_token,
        },
        CostUnits(units),
        t(0),
        PolicyRevision::UNSTATED,
        None,
    )
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

/// Issue GL-4: the refill task's numbers existed only as `tracing` events with
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

/// Issue GL-38: the accounting numbers used to exist only as a local on the
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
        .unwrap()
        .grant;
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
        .unwrap()
        .grant;
    // Never recovers on its own: the outage lasts until this test ends it.
    let failures_left = Arc::new(AtomicU32::new(u32::MAX));
    let sink = flaky_sink(&store.clone(), &failures_left);
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
    failures_left.store(0, Ordering::Relaxed);
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
        .unwrap()
        .grant;
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
        .unwrap()
        .grant;
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
/// What a capped sink saw, shared with the test.
#[derive(Default)]
struct BatchCap {
    largest: AtomicUsize,
    ingested: AtomicUsize,
}

/// A sink that refuses any batch above `cap`. It has no inner store, so it
/// rejects anything it is not given rather than delegating.
fn batch_capped_sink(cap: usize, seen: &Arc<BatchCap>) -> Arc<DelegatingStore<RejectingStore>> {
    let seen = Arc::clone(seen);
    Arc::new(
        rejecting("a capped-sink fixture answers ingest and nothing else").on_ingest(
            move |_, events, _now| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.largest.fetch_max(events.len(), Ordering::AcqRel);
                    if events.len() > cap {
                        return Err(StoreError("batch exceeds sink limit".into()).into());
                    }
                    seen.ingested.fetch_add(events.len(), Ordering::AcqRel);
                    Ok(IngestReport {
                        accepted: events.len() as u64,
                        ..IngestReport::default()
                    })
                }
            },
        ),
    )
}

#[tokio::test(start_paused = true)]
async fn steady_state_flushes_in_configured_batch_sizes() {
    let seen = Arc::new(BatchCap::default());
    let sink = batch_capped_sink(2, &seen);
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
    assert_eq!(seen.ingested.load(Ordering::Acquire), 4);
    assert_eq!(seen.largest.load(Ordering::Acquire), 2);

    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 4);
    assert_eq!(stats.lost, 0);
}

#[tokio::test(start_paused = true)]
async fn shutdown_flushes_in_configured_batch_sizes() {
    let seen = Arc::new(BatchCap::default());
    let sink = batch_capped_sink(2, &seen);
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
    assert_eq!(seen.largest.load(Ordering::Acquire), 2);
}

/// Fails a fixed number of ingests, then behaves like the store it wraps.
fn flaky_sink(
    store: &Arc<MemoryStore>,
    failures_left: &Arc<AtomicU32>,
) -> Arc<DelegatingStore<MemoryStore>> {
    let failures_left = Arc::clone(failures_left);
    Arc::new(
        DelegatingStore::wrapping(Arc::clone(store)).on_ingest(move |inner, events, now| {
            let failures_left = Arc::clone(&failures_left);
            async move {
                if failures_left
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                    .is_ok()
                {
                    return Err(StoreError("injected outage".into()).into());
                }
                inner.ingest(&events, now).await
            }
        }),
    )
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
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    let failures_left = Arc::new(AtomicU32::new(3));
    let sink = flaky_sink(&store.clone(), &failures_left);
    let (recorder, writer) = UsageWriter::spawn(sink, clock, writer_config(64)).unwrap();

    recorder.try_reserve().unwrap().record(event(1, 25, &lease));
    settle().await;
    settle().await;
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(25));
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.lost, 0);
}

/// Review finding GL-3 regression: a panic between commit and response must
/// still bill — the queue permit is the usage slot bound at admission, and
/// `Committed`'s drop emits into it during unwind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_after_commit_still_bills() {
    let store = store(10_000);
    let grant = store
        .acquire(
            ACCOUNT,
            CostUnits(1_000),
            SignedDuration::from_secs(60),
            t(0),
        )
        .await
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();

    let permit = recorder.try_reserve().unwrap();
    let worker = tokio::spawn(async move {
        let charge = ready_from_grant(grant, permit)
            .commit(RequestId(9), t(0))
            .expect("a live lease commits");
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

/// The panic boundary a Rayon-style executor must own, exercised the way that
/// executor would: the kernel runs under `catch_unwind` on a plain thread, and
/// the guard is dropped there rather than leaked.
///
/// Tollgate never runs the kernel, so it cannot install this boundary itself —
/// which is exactly why the ownership is documented on `Committed`. What this
/// asserts is Tollgate's half of the contract: when the guard *is* dropped
/// during an unwind, emission still happens, because the event was built at
/// commit and `Drop` takes no lock and allocates nothing.
#[tokio::test]
async fn a_panicking_kernel_under_catch_unwind_still_bills() {
    let store = store(10_000);
    let grant = store
        .acquire(
            ACCOUNT,
            CostUnits(1_000),
            SignedDuration::from_secs(60),
            t(0),
        )
        .await
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();

    let permit = recorder.try_reserve().unwrap();
    let ready = ready_from_grant(grant, permit);

    // A consumer executor's shape: commit at execution start on a worker
    // thread, run the kernel inside a panic boundary, drop the guard there.
    let worker = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let charge = ready
                .commit(RequestId(9), t(0))
                .expect("a live lease commits");
            assert_eq!(charge.units(), CostUnits(51));
            panic!("kernel exploded mid-execution");
        }))
    });
    let outcome = worker.join().expect("the boundary contains the panic");
    assert!(outcome.is_err(), "the kernel panicked");

    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 1, "the committed charge was still emitted");
    assert_eq!(stats.lost, 0);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(51));
}

/// Review finding GL-2 regression: shutdown during an *ongoing* outage must
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
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    // Sink that never recovers.
    let failures_left = Arc::new(AtomicU32::new(u32::MAX));
    let sink = flaky_sink(&store.clone(), &failures_left);
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
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    // Fails long enough to outlast several retry backoffs, then recovers in
    // time for the final flush.
    let failures_left = Arc::new(AtomicU32::new(4));
    let sink = flaky_sink(&store.clone(), &failures_left);
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

/// A lease-expiry straggler (GL-136): a holder that outlived an outage flushes
/// after its lease was swept, and the usage is billed against the forfeit
/// rather than dropped. Usage beyond what was forfeited is still rejected and
/// reported, never silently absorbed.
#[tokio::test(start_paused = true)]
async fn a_survivor_flushes_after_reclaim_and_is_billed() {
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
    store.reclaim_expired(t(120)).await.unwrap();

    let clock = Arc::new(ManualClock::new(t(121)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();
    recorder.try_reserve().unwrap().record(event(1, 10, &lease));
    recorder
        .try_reserve()
        .unwrap()
        .record(event(2, 991, &lease));
    settle().await;
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.rejected, 1, "991 exceeds the 990 still forfeited");
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(10));
    let c = store.conservation(ACCOUNT).unwrap();
    assert_eq!(c.settlement_loss, CostUnits(990));
    assert!(c.holds(), "{c:?}");
}

// ---- shutdown drain (issue GL-32) -------------------------------------------
//
// A permit reserved before shutdown must resolve — by sending or dropping —
// before the writer returns, bounded by the drain deadline; a deadline expiry
// is reported in `unresolved`, never a clean flush.

/// Shutdown refuses new reservations from the instant it begins, while a
/// permit reserved earlier still delivers into the drain (INVARIANTS.md GL-8).
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
        .unwrap()
        .grant;
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

/// The GL-32 defect: a permit that records only after shutdown has begun must
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
        .unwrap()
        .grant;
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

/// INVARIANTS.md GL-13 across shutdown: a committed guard dropped after
/// shutdown begins still bills.
#[tokio::test(start_paused = true)]
async fn shutdown_waits_for_committed_guard() {
    let store = store(10_000);
    let grant = store
        .acquire(
            ACCOUNT,
            CostUnits(1_000),
            SignedDuration::from_secs(60),
            t(0),
        )
        .await
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    let (recorder, writer) = UsageWriter::spawn(store.clone(), clock, writer_config(8)).unwrap();

    let permit = recorder.try_reserve().unwrap();
    let holder = tokio::spawn(async move {
        let charge = ready_from_grant(grant, permit)
            .commit(RequestId(9), t(0))
            .expect("a live lease commits");
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
        .unwrap()
        .grant;
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
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    let failures_left = Arc::new(AtomicU32::new(u32::MAX));
    let sink = flaky_sink(&store.clone(), &failures_left);
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

// ---- a dead writer never reports zero loss (issue GL-41) --------------------

/// A sink that panics once it has been called `panic_after` times.
/// Panics once its budget of clean calls runs out.
fn panicking_sink(
    store: &Arc<MemoryStore>,
    calls_before_panic: u32,
) -> Arc<DelegatingStore<MemoryStore>> {
    let budget = Arc::new(AtomicU32::new(calls_before_panic));
    Arc::new(
        DelegatingStore::wrapping(Arc::clone(store)).on_ingest(move |inner, events, now| {
            let budget = Arc::clone(&budget);
            async move {
                if budget
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                    .is_err()
                {
                    panic!("sink exploded mid-ingest");
                }
                inner.ingest(&events, now).await
            }
        }),
    )
}

/// The GL-41 defect: a writer that dies holding committed charges must say so,
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
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    let sink = panicking_sink(&store.clone(), 0);
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
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    // The first batch is ingested; the second kills the task.
    let sink = panicking_sink(&store.clone(), 1);
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

// ---- wall-clock bounds on a wedged backend (issue GL-34) --------------------

/// A sink whose `ingest` never resolves — a backend that is hung rather than
/// erroring, which retry *counts* alone cannot bound.
/// An ingest that never returns.
fn hanging_sink() -> Arc<DelegatingStore<RejectingStore>> {
    Arc::new(
        rejecting("a hanging-sink fixture answers ingest and nothing else")
            .on_ingest(|_, _events, _now| async { std::future::pending().await }),
    )
}

/// The GL-34 defect: a hung ingest parked the writer task forever, and with it
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
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    let mut config = writer_config(16);
    config.flush_interval = std::time::Duration::from_secs(3_600);
    let (recorder, writer) =
        UsageWriter::spawn(hanging_sink() as Arc<dyn UsageSink>, clock, config).unwrap();
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
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    let mut config = writer_config(16);
    config.ingest_timeout = std::time::Duration::from_millis(100);
    let (recorder, writer) =
        UsageWriter::spawn(hanging_sink() as Arc<dyn UsageSink>, clock, config).unwrap();
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
/// Delays, then behaves like the store it wraps.
fn slow_sink(
    store: &Arc<MemoryStore>,
    delay: std::time::Duration,
) -> Arc<DelegatingStore<MemoryStore>> {
    Arc::new(DelegatingStore::wrapping(Arc::clone(store)).on_ingest(
        move |inner, events, now| async move {
            tokio::time::sleep(delay).await;
            inner.ingest(&events, now).await
        },
    ))
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
        .unwrap()
        .grant;
    let clock = Arc::new(ManualClock::new(t(0)));
    let sink = slow_sink(&store.clone(), std::time::Duration::from_millis(50));
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

/// An allocator that never answers an acquire. Distinct from refusing: the
/// grant may well have been made, so the client cannot treat this as a
/// domain answer.
/// An `acquire` that never returns, over no store at all.
///
/// `reclaim_expired` is not hooked and needs no inner store: the wrapper
/// inherits the trait default, which drains through the `reclaim_expired_batch`
/// hook below and so still answers with an empty batch.
fn hanging_acquire() -> Arc<DelegatingStore<RejectingStore>> {
    Arc::new(
        rejecting("this fixture never consolidates")
            .on_acquire(|_, _account, _requested, _ttl, _now| async {
                std::future::pending().await
            })
            .on_release(|_, _lease, _fence, _unspent, _now| async { Ok(()) })
            .on_reclaim_expired_batch(|_, _now, limit| async move {
                ReclaimBatch::try_new(Vec::new(), limit)
            }),
    )
}

/// INVARIANTS.md GL-18, issue GL-78: the shutdown signal is observed inside a
/// hung `acquire`, not after it. The release pass is the louder half of that
/// finding, but `acquire` is the loop's other long await and a signal
/// arriving inside it was equally invisible until the next loop top.
#[tokio::test(start_paused = true)]
async fn shutdown_during_a_hung_acquire_is_not_delayed_by_it() {
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    // A long per-call timeout with a short budget: if the signal is only
    // observed at the loop top, shutdown waits out the acquire and blows the
    // budget. The production shape has the same relationship, more slowly.
    let config = LeaseManagerConfig {
        store_call_timeout: std::time::Duration::from_secs(120),
        shutdown_release_deadline: std::time::Duration::from_secs(10),
        ..manager_config()
    };
    let manager = LeaseManager::spawn(
        hanging_acquire() as Arc<dyn LeaseAllocator>,
        Arc::clone(&slot),
        clock,
        config,
    )
    .unwrap();
    settle().await;
    assert!(slot.load().is_none(), "the acquire is still unanswered");

    let bound = config.shutdown_release_deadline + std::time::Duration::from_secs(1);
    let began = tokio::time::Instant::now();
    let report = tokio::time::timeout(bound, manager.shutdown())
        .await
        .expect("a hung acquire must not hold the shutdown signal");
    let took = began.elapsed();

    assert!(!report.task_died);
    assert_eq!(report.released, 0, "there was never a lease to return");
    assert_eq!(report.abandoned, 0);
    assert!(
        took < config.store_call_timeout,
        "shutdown took {took:?}, which is the hung acquire's timeout, not the signal"
    );
}

/// INVARIANTS.md GL-18: an allocator that hangs rather than answering is
/// bounded by `store_call_timeout`. It is counted apart from every refusal,
/// because it is not a domain answer — treating it as one would assert the
/// lease was not granted, which this client cannot know.
#[tokio::test(start_paused = true)]
async fn a_hung_acquire_is_counted_as_a_timeout_not_a_refusal() {
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(
        hanging_acquire() as Arc<dyn LeaseAllocator>,
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

/// A `release` that never returns, over a real store.
///
/// `consolidate` keeps its `unreachable!()`: delegation would quietly give
/// this fixture a working exchange it is not supposed to have.
fn hanging_release(store: &Arc<MemoryStore>) -> Arc<DelegatingStore<MemoryStore>> {
    Arc::new(
        DelegatingStore::wrapping(Arc::clone(store))
            .on_release(|_, _lease, _fence, _unspent, _now| async { std::future::pending().await })
            .on_consolidate(|_, _, _, _, _, _, _, _| async {
                unreachable!("this fixture never consolidates")
            }),
    )
}

/// INVARIANTS.md GL-6, issue GL-78: a refill must not queue behind the release
/// pass. With leases parked against a backend whose `release` hangs, the old
/// loop paid `parked.len()` sequential timeouts before it issued `acquire`,
/// which put refill latency back on a floor that grew with the parked count —
/// the very floor GL-6 exists to remove.
#[tokio::test(start_paused = true)]
async fn a_refill_does_not_wait_behind_the_release_pass() {
    let store = store(100_000);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let allocator = hanging_release(&store.clone());
    let manager = LeaseManager::spawn(
        allocator as Arc<dyn LeaseAllocator>,
        Arc::clone(&slot),
        Arc::clone(&clock) as Arc<dyn tollgate_client::Clock>,
        manager_config(),
    )
    .unwrap();
    settle().await;

    // Park a queue by rotating on expiry; a hung release keeps every
    // superseded lease on the books.
    //
    // The fixture waits on the observable state rather than on a fixed sleep,
    // so that a regression shows up in the measurement below instead of
    // breaking the setup: a slower loop must still reach a fresh lease here,
    // and then be caught by the ceiling.
    for tick in 1..=5 {
        clock.set(t(tick * 120));
        let installed = tokio::time::timeout(std::time::Duration::from_secs(600), async {
            loop {
                match slot.load() {
                    Some(lease) if lease.grant().expires_at > clock.now() => break,
                    _ => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
                }
            }
        })
        .await;
        installed.expect("the manager must install a lease after each rotation");
    }
    assert_eq!(manager.counters().snapshot().acquired, 6);
    assert_eq!(manager.counters().snapshot().released, 0);
    let before = slot.load().expect("a lease is installed");
    assert!(
        !before.needs_refill(),
        "the fixture must start from a lease that has not already crossed"
    );

    // Ring the low-water doorbell and time the refill. Waiting on the fresh
    // lease rather than on a fixed sleep is what makes the ceiling below the
    // thing under test: a slow refill is measured, not deadlocked.
    let began = tokio::time::Instant::now();
    before.try_debit(CostUnits(800), clock.now()).unwrap();
    assert!(before.needs_refill());
    tokio::time::timeout(std::time::Duration::from_secs(600), async {
        loop {
            match slot.load() {
                Some(fresh) if fresh.grant().lease_id != before.grant().lease_id => break,
                _ => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("the crossing debit must be followed by a fresh lease");
    let waited = began.elapsed();
    // One pass budget, not one per parked lease. `settle()` bounds the wait
    // from above anyway, so the assertion is about the shape: five parked
    // leases must not cost five timeouts.
    // Generous enough to absorb finishing a pass already in flight plus the
    // next one's head lease; five parked leases at one timeout each — the
    // pre-GL-78 cost — is 25s and does not fit.
    let ceiling = manager_config().store_call_timeout * 3;
    assert!(
        waited < ceiling,
        "refill waited {waited:?} behind the release pass, over the {ceiling:?} ceiling"
    );
    manager.shutdown().await;
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
/// reported rather than assumed settled (INVARIANTS.md GL-18).
#[tokio::test(start_paused = true)]
async fn hung_release_cannot_stall_shutdown() {
    let store = store(10_000);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let allocator = hanging_release(&store.clone());
    let manager = LeaseManager::spawn(
        allocator as Arc<dyn LeaseAllocator>,
        Arc::clone(&slot),
        Arc::clone(&clock) as Arc<dyn tollgate_client::Clock>,
        manager_config(),
    )
    .unwrap();
    let counters = manager.counters();
    settle().await;
    assert!(slot.load().is_some());
    assert_eq!(counters.snapshot().abandoned, 0, "nothing abandoned yet");

    // In-flight readers retain each superseded grant while the slot rotates.
    // Once they finish, every parked grant reaches the hanging backend. This
    // is the shape GL-78 bounds: the old loop paid `parked.len()` sequential
    // timeouts before it could even inspect the shutdown signal.
    let mut readers = Vec::new();
    for tick in 1..=5 {
        readers.push(
            slot.load()
                .expect("the preceding rotation installed a grant"),
        );
        clock.set(t(tick * 120));
        settle().await;
    }
    drop(readers);
    let parked = counters.snapshot();
    assert_eq!(parked.released, 0, "a hung release settles nothing");

    // The stated budget, not an arbitrary ceiling. `shutdown()` documents
    // itself as bounded by `shutdown_release_deadline`, and that is only true
    // if the signal ends the release pass where it arrives: a pass that runs
    // to its own budget first would add `store_call_timeout` on top, which
    // this bound deliberately does not allow for. The extra second is
    // scheduling slack, not budget.
    let config = manager_config();
    let bound = config.shutdown_release_deadline + std::time::Duration::from_secs(1);
    let began = tokio::time::Instant::now();
    let report = tokio::time::timeout(bound, manager.shutdown())
        .await
        .expect("a hung release must not stall shutdown past its stated budget");
    let took = began.elapsed();

    assert!(
        report.abandoned >= 2,
        "the test must exercise a multi-lease parked list, got {}",
        report.abandoned
    );
    assert_eq!(report.released, 0);
    assert!(!report.task_died);
    assert!(
        took <= bound,
        "shutdown took {took:?} against a stated {:?} budget",
        config.shutdown_release_deadline
    );
    // The same numbers outside the report: stranded units are visible to a
    // scrape, not only to whoever awaited this shutdown.
    let stats = counters.snapshot();
    assert_eq!(stats.abandoned, report.abandoned);
    assert_eq!(stats.released, 0);
}

/// INVARIANTS.md GL-16: no writer field is silently repaired.
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

/// INVARIANTS.md GL-16, manager half: the two new bounds are contracts too.
#[test]
fn invalid_lease_manager_timeouts_are_rejected() {
    let mut config = manager_config();
    config.store_call_timeout = std::time::Duration::ZERO;
    assert!(config.validate().is_err());

    let mut config = manager_config();
    config.shutdown_release_deadline = std::time::Duration::ZERO;
    assert!(config.validate().is_err());
}

/// Issue GL-62, INVARIANTS GL-1 and GL-6: shutdown must not release a lease an
/// in-flight request can still spend.
///
/// `parked` at shutdown holds, by construction, the leases the last pass
/// determined were *not* quiesced — `release_quiesced` re-parks exactly those.
/// Releasing them read `remaining()` at an instant that was not final: the
/// account is credited, the request then debits and commits, and total
/// committed usage exceeds the allocation with a usage event to prove it.
///
/// The documented lifecycle says an embedder quiesces first (GL-13), but that is
/// caller discipline — the tier the standards call drift-prone — and the
/// predicate that makes it unnecessary already existed in the same file.
///
/// Held here by keeping an `Arc<LocalLease>` across `shutdown()`, which is
/// what a request task in flight looks like from the manager's side.
#[tokio::test(start_paused = true)]
async fn shutdown_abandons_a_lease_an_in_flight_request_still_holds() {
    let store = store(10_000);
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

    // The in-flight request: a view loaded before shutdown and still held
    // across it, exactly as a handler holding a reservation would.
    let in_flight = slot.load().expect("a lease is installed");
    let balance_before = store.balance(ACCOUNT);

    let report = manager.shutdown().await;

    assert_eq!(
        report.released, 0,
        "a lease still reachable by a request must not be released"
    );
    assert_eq!(
        report.abandoned, 1,
        "it is abandoned instead, and reported — never silently dropped"
    );
    assert_eq!(
        store.balance(ACCOUNT),
        balance_before,
        "no units were credited back while a request could still spend them; \
         crediting early is what lets committed usage exceed the allocation"
    );

    // The request can still spend what it holds, which is the whole reason
    // the units were not returned.
    assert!(in_flight.try_debit(CostUnits(50), t(0)).is_ok());
}

/// Issue GL-63, INVARIANTS GL-8: the drain deadline is the drain's total wall
/// clock, backoffs included.
///
/// `final_flush_backs_off_only_between_attempts` pins the backoff *count*, but
/// runs with a 60s deadline and a 100ms backoff — so it can never observe the
/// two interacting. The exposing configuration is the opposite one, and it is
/// not exotic: the module doc tells an operator to size the drain inside
/// `expiry_safety_margin + reclaim_grace`, which makes a short deadline beside
/// a longer backoff the ordinary shape.
///
/// Overrunning here is not merely a slow shutdown. The lease manager releases
/// after the writer drains, so the overrun spends the margin that keeps a
/// straggler billable: events that do land arrive against a lease the
/// allocator has re-granted and are refused — the outcome GL-12's budget exists
/// to prevent.
#[tokio::test(start_paused = true)]
async fn the_final_flush_backoff_cannot_overrun_the_drain_deadline() {
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
    let clock = Arc::new(ManualClock::new(t(0)));
    let failures_left = Arc::new(AtomicU32::new(u32::MAX));
    let sink = flaky_sink(&store.clone(), &failures_left);
    let mut config = writer_config(16);
    config.flush_interval = std::time::Duration::from_secs(3_600);
    // The shape that exposes it: a backoff longer than the whole budget.
    config.shutdown_drain_deadline = std::time::Duration::from_secs(2);
    config.retry_backoff = std::time::Duration::from_secs(5);
    config.ingest_timeout = std::time::Duration::from_millis(50);
    let (recorder, writer) = UsageWriter::spawn(sink, clock, config).unwrap();
    recorder.try_reserve().unwrap().record(event(1, 10, &lease));

    let start = tokio::time::Instant::now();
    let stats = writer.shutdown().await.unwrap();
    let took = start.elapsed();

    assert_eq!(
        stats.lost, 1,
        "an undeliverable event is counted, not dropped"
    );
    assert!(
        took <= config.shutdown_drain_deadline + config.ingest_timeout,
        "the drain took {took:?} against a {:?} budget; a backoff that sleeps \
         past the deadline spends the margin that keeps a straggler billable",
        config.shutdown_drain_deadline
    );
}

/// The sink refuses whatever it is handed, so the event's shape is irrelevant
/// — only that it is a real event the writer will try to deliver.
fn refused_event(request: u128) -> UsageEvent {
    UsageEvent::new(
        RequestId(request),
        ACCOUNT,
        UsageSource::Overage,
        CostUnits(50),
        t(0),
        PolicyRevision::UNSTATED,
        None,
    )
}

/// A sink that refuses every batch and will keep refusing it, as an
/// over-limit body or an undecodable event does.
/// A sink that refuses every batch with the same permanent error.
fn always_refuses_sink(attempts: &Arc<AtomicUsize>) -> Arc<DelegatingStore<RejectingStore>> {
    let attempts = Arc::clone(attempts);
    Arc::new(
        rejecting("a refusing-sink fixture answers ingest and nothing else").on_ingest(
            move |_, _events, _now| {
                let attempts = Arc::clone(&attempts);
                async move {
                    attempts.fetch_add(1, Ordering::AcqRel);
                    Err(IngestError::Refused(StoreError(
                        "413 batch-too-large: request body exceeds this endpoint's limit".into(),
                    )))
                }
            },
        ),
    )
}

/// Issue GL-61: a batch the sink will never accept must not block every later
/// event behind it.
///
/// The writer retried any failed batch forever, which is right for an outage
/// and wrong for a refusal: the queue fills, later requests shed as
/// `AccountingBackpressure`, `/readyz` stays 200 because the task is alive and
/// spinning, and `lost` stays zero because loss is declared only at the final
/// flush. A permanent, deterministic error became an unbounded billing and
/// availability outage.
///
/// Two assertions, and the second is the one that matters: the batch is
/// attempted *once*, and a later event still bills.
/// Issue GL-61, INVARIANTS GL-16: a `max_batch` the ingest endpoint would refuse
/// is a startup error, not a discovery made in production.
///
/// `validate` previously checked only that it was non-zero, so an embedder
/// raising it to cut round-trips was accepted and then had every batch
/// refused forever.
#[test]
fn a_max_batch_beyond_the_ingest_limit_is_rejected() {
    let mut oversized = writer_config(8);
    oversized.max_batch = tollgate_store::MAX_INGEST_BATCH + 1;
    assert_eq!(
        oversized.validate().unwrap_err().to_string(),
        "max_batch exceeds the ingest endpoint's documented limit"
    );

    let mut at_limit = writer_config(8);
    at_limit.max_batch = tollgate_store::MAX_INGEST_BATCH;
    assert!(
        at_limit.validate().is_ok(),
        "the documented limit itself must be usable, or it is not the limit"
    );
}

#[tokio::test(start_paused = true)]
async fn a_refused_batch_is_counted_lost_rather_than_retried_forever() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let sink = always_refuses_sink(&attempts);
    let clock = Arc::new(ManualClock::new(t(0)));
    let (recorder, writer) = UsageWriter::spawn(sink, clock, writer_config(8)).unwrap();

    recorder.try_reserve().unwrap().record(refused_event(1));
    settle().await;

    assert_eq!(
        attempts.load(Ordering::Acquire),
        1,
        "a refused batch must be attempted once, not retried"
    );

    assert_eq!(
        recorder.health().unaccounted,
        0,
        "a reported loss is a completed accounting outcome"
    );

    // The queue keeps working: a later event is not stuck behind the refusal.
    recorder
        .try_reserve()
        .expect("the queue drained rather than filling with a wedged batch")
        .record(refused_event(2));
    settle().await;

    let stats = writer.shutdown().await.unwrap();
    assert!(
        stats.lost >= 2,
        "refused events are counted lost, never silently dropped; got {stats:?}"
    );
}

/// Hangs, and reports how many ingests are parked inside it.
fn observed_hung_sink(active: &Arc<AtomicUsize>) -> Arc<DelegatingStore<RejectingStore>> {
    let active = Arc::clone(active);
    Arc::new(
        rejecting("a hung-sink fixture answers ingest and nothing else").on_ingest(
            move |_, _events, _now| {
                let active = Arc::clone(&active);
                async move {
                    struct Active(Arc<AtomicUsize>);
                    impl Drop for Active {
                        fn drop(&mut self) {
                            self.0.fetch_sub(1, Ordering::AcqRel);
                        }
                    }
                    active.fetch_add(1, Ordering::AcqRel);
                    let _active = Active(Arc::clone(&active));
                    std::future::pending().await
                }
            },
        ),
    )
}

#[tokio::test(start_paused = true)]
async fn cancelling_writer_shutdown_aborts_the_owned_ingest_task() {
    let active = Arc::new(AtomicUsize::new(0));
    let mut config = writer_config(8);
    config.max_batch = 1;
    config.ingest_timeout = std::time::Duration::from_secs(120);
    config.shutdown_drain_deadline = std::time::Duration::from_secs(60);
    let (recorder, writer) = UsageWriter::spawn(
        observed_hung_sink(&active),
        Arc::new(ManualClock::new(t(0))),
        config,
    )
    .unwrap();
    recorder.try_reserve().unwrap().record(refused_event(1));
    settle().await;
    assert_eq!(active.load(Ordering::Acquire), 1);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(1), writer.shutdown())
            .await
            .is_err()
    );
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        active.load(Ordering::Acquire),
        0,
        "cancelled shutdown must not detach its writer"
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_normal_ingest_before_its_long_timeout() {
    let active = Arc::new(AtomicUsize::new(0));
    let mut config = writer_config(8);
    config.max_batch = 1;
    config.ingest_timeout = std::time::Duration::from_secs(120);
    config.shutdown_drain_deadline = std::time::Duration::from_millis(10);
    let (recorder, writer) = UsageWriter::spawn(
        observed_hung_sink(&active),
        Arc::new(ManualClock::new(t(0))),
        config,
    )
    .unwrap();
    recorder.try_reserve().unwrap().record(refused_event(1));
    settle().await;
    assert_eq!(active.load(Ordering::Acquire), 1);
    let result = tokio::time::timeout(std::time::Duration::from_millis(11), writer.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.lost, 1);
    assert_eq!(active.load(Ordering::Acquire), 0);
}

#[tokio::test(start_paused = true)]
async fn cancelling_lease_shutdown_aborts_the_owned_release_task() {
    let store = store(10_000);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let manager = LeaseManager::spawn(
        hanging_release(&store),
        slot.clone(),
        Arc::new(ManualClock::new(t(0))),
        manager_config(),
    )
    .unwrap();
    let health = manager.health();
    settle().await;
    assert!(slot.load().is_some());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(1), manager.shutdown())
            .await
            .is_err()
    );
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        health.has_changed().is_err(),
        "cancelled shutdown must not detach its manager"
    );
    assert!(!*health.borrow(), "cancelled refill task must retain false");
}

/// Issue GL-109: near the end of an allowance the allocator shrinks the grant,
/// and `install_lease` caps low water below it, so the tail lease sits *above*
/// its own mark. Nothing crosses, and before this change a refused debit said
/// nothing either — the instance answered `LeaseExhausted` until the TTL while
/// lease and ledger together could fund the request.
///
/// The reporter's numbers exactly: allowance 160, target 100, low water 50,
/// three 51-unit requests. Grants run 100 → 60 → 49, and the third request
/// meets a 49-unit lease with 9 units still in the ledger.
#[tokio::test(start_paused = true)]
async fn a_refused_lease_consolidates_rather_than_stranding_the_tail() {
    let store = store(160);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(
        store.clone(),
        Arc::clone(&slot),
        clock,
        LeaseManagerConfig {
            target_grant: CostUnits(100),
            low_water: CostUnits(50),
            // A minute is far longer than this test runs, so no tick can
            // explain a rotation. Only the refusal can.
            poll_interval: std::time::Duration::from_secs(60),
            ..manager_config()
        },
    )
    .unwrap();

    settle().await;
    for spent in 1..=2 {
        let lease = slot.load().expect("a lease is installed");
        lease
            .try_debit(CostUnits(51), t(0))
            .unwrap_or_else(|e| panic!("debit {spent} should be funded: {e:?}"));
        drop(lease);
        settle().await;
    }

    let tail = slot.load().expect("a lease is installed");
    assert_eq!(
        tail.remaining(),
        CostUnits(49),
        "the shrunken tail grant is the state the defect needs"
    );
    assert!(
        !tail.needs_refill(),
        "and it sits above its own capped low-water mark, so nothing crosses"
    );
    let refused = tail.try_debit(CostUnits(51), t(0));
    assert!(
        matches!(refused, Err(DenyReason::LeaseExhausted { .. })),
        "this one debit is still refused: {refused:?}"
    );
    drop(tail);

    settle().await;
    let consolidated = slot.load().expect("a lease is installed");
    assert_eq!(
        consolidated.remaining(),
        CostUnits(58),
        "the refusal folded the tail's 49 unspent units back in with the \
         ledger's 9, so the account's whole remaining balance is reachable"
    );
    consolidated
        .try_debit(CostUnits(51), t(0))
        .expect("the request the account could always fund is now admitted");
    drop(consolidated);

    let stats = manager.counters().snapshot();
    assert_eq!(stats.consolidated, 1, "exactly one consolidating rotation");
    assert_eq!(
        stats.consolidations_deferred, 0,
        "nothing was in flight to defer it"
    );
    manager.shutdown().await;
}

/// The units are folded in *atomically*, which is the half a holder cannot do
/// for itself. With the default `shrink_divisor` of 2, releasing 49 into a
/// balance of 9 and then acquiring re-grants only 29 — a consolidation that
/// shrank the holder. The floor is what forbids that.
#[tokio::test(start_paused = true)]
async fn consolidation_under_a_shrinking_policy_never_returns_less_than_it_folded() {
    let store = Arc::new(
        MemoryStore::new(GrantPolicy {
            shrink_divisor: 2,
            min_grant: CostUnits(1),
            max_ttl: SignedDuration::from_secs(3_600),
            reclaim_grace: SignedDuration::ZERO,
        })
        .unwrap(),
    );
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(58),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    let grant = store
        .acquire(ACCOUNT, CostUnits(49), SignedDuration::from_secs(60), t(0))
        .await
        .expect("the first grant is funded")
        .grant;
    assert_eq!(grant.units, CostUnits(29), "the policy shrinks it by half");

    let folded = store
        .consolidate(
            grant.lease_id,
            grant.fencing_token,
            grant.units,
            CostUnits(100),
            CostUnits::ZERO,
            SignedDuration::from_secs(60),
            t(1),
        )
        .await
        .expect("consolidation is funded by the units it returns")
        .grant;
    assert!(
        folded.units >= grant.units,
        "a consolidation may grow the holding or leave it alone, never shrink it: \
         {} folded in, {} granted",
        grant.units.get(),
        folded.units.get()
    );
    assert_eq!(
        folded.units,
        CostUnits(29),
        "the policy still caps growth at balance/2; the floor only forbids the downgrade"
    );
}

#[tokio::test(start_paused = true)]
async fn refill_publishes_exhaustion_and_a_topup_clears_it() {
    let store = store(0);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager =
        LeaseManager::spawn(store.clone(), slot.clone(), clock, manager_config()).unwrap();
    settle().await;
    assert!(
        slot.funding_evidence(t(0))
            .is_some_and(|remaining| remaining.is_zero())
    );
    tollgate_store::AdminStore::deposit(&*store, ACCOUNT, CostUnits(100))
        .await
        .unwrap();
    settle().await;
    assert!(
        !slot
            .funding_evidence(t(0))
            .is_some_and(|remaining| remaining.is_zero())
    );
    assert_eq!(slot.load_observed().unwrap().remaining(), CostUnits(100));
    manager.shutdown().await;
}

/// GL-131 end to end with `GrantPolicy::default()`: 60 units grant 30, and
/// before GL-131 every refusal-driven consolidation re-granted 30, so a 51-unit
/// quote the account could fund was refused until the period ended.
#[tokio::test(start_paused = true)]
async fn a_shrinking_policy_funds_a_quote_above_half_the_balance() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(60),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(
        store.clone(),
        Arc::clone(&slot),
        clock,
        LeaseManagerConfig {
            // Only the refusal can explain a rotation in this test.
            poll_interval: std::time::Duration::from_secs(60),
            ..manager_config()
        },
    )
    .unwrap();
    settle().await;
    let half = slot.load().expect("a lease is installed");
    assert_eq!(half.remaining(), CostUnits(30));
    assert!(matches!(
        half.try_debit(CostUnits(51), t(0)),
        Err(DenyReason::LeaseExhausted { .. })
    ));
    drop(half);
    settle().await;

    let grown = slot.load().expect("a lease is installed");
    assert_eq!(
        grown.remaining(),
        CostUnits(51),
        "grown to the refused quote"
    );
    grown
        .try_debit(CostUnits(51), t(0))
        .expect("the quote the account could always fund is admitted");
    drop(grown);
    assert_eq!(manager.counters().snapshot().consolidated, 1);
    manager.shutdown().await;
}

/// GL-130 end to end: one unit left and a quote of 252. The refusal-driven
/// consolidation re-grants the tail, whose evidence says the account cannot
/// fund the quote; a top-up reaches the slot through the next refusal.
#[tokio::test(start_paused = true)]
async fn refill_publishes_shortfall_and_a_topup_clears_it() {
    let store = store(1);
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager = LeaseManager::spawn(
        store.clone(),
        slot.clone(),
        clock,
        LeaseManagerConfig {
            // Only a refusal can explain a rotation in this test.
            poll_interval: std::time::Duration::from_secs(60),
            ..manager_config()
        },
    )
    .unwrap();
    settle().await;
    assert_eq!(slot.load_observed().unwrap().remaining(), CostUnits(1));
    assert_eq!(slot.funding_evidence(t(0)), Some(CostUnits(1)));

    let refuse_252 = || {
        let lease = slot.load().expect("a lease is installed");
        assert!(matches!(
            lease.try_debit(CostUnits(252), t(0)),
            Err(DenyReason::LeaseExhausted { .. })
        ));
    };
    refuse_252();
    settle().await;
    assert_eq!(
        slot.funding_evidence(t(0)),
        Some(CostUnits(1)),
        "the consolidated tail carries the same attested remainder"
    );

    tollgate_store::AdminStore::deposit(&*store, ACCOUNT, CostUnits(300))
        .await
        .unwrap();
    refuse_252();
    settle().await;
    assert_eq!(slot.funding_evidence(t(0)), Some(CostUnits(301)));
    slot.load()
        .expect("a lease is installed")
        .try_debit(CostUnits(252), t(0))
        .expect("the top-up funds the quote");
    manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_refill_gap_with_credit_held_elsewhere_is_not_exhaustion() {
    let store = store(100);
    let held = store
        .acquire(ACCOUNT, CostUnits(100), SignedDuration::from_secs(60), t(0))
        .await
        .unwrap()
        .grant;
    let slot = LeaseSlot::for_account(ACCOUNT);
    let clock = Arc::new(ManualClock::new(t(0)));
    let manager =
        LeaseManager::spawn(store.clone(), slot.clone(), clock, manager_config()).unwrap();
    settle().await;
    // The refusal attests the 100 units the other lease holds: any quote
    // within them keeps the lease gap's transient advice.
    assert_eq!(slot.funding_evidence(t(0)), Some(CostUnits(100)));
    assert!(slot.load_observed().is_none());
    store
        .release(held.lease_id, held.fencing_token, held.units, t(0))
        .await
        .unwrap();
    settle().await;
    assert!(slot.load_observed().is_some());
    assert!(
        !slot
            .funding_evidence(t(0))
            .is_some_and(|remaining| remaining.is_zero())
    );
    manager.shutdown().await;
}
