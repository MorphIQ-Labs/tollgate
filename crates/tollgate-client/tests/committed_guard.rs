//! The committed execution guard's two accounting guarantees: occupancy is
//! held for the guard's whole lifetime, and a failed commit gives back both
//! the concurrency permits and the unused queue slot.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jiff::Timestamp;
use tollgate_admission::{
    AdmissionEngine, ArcSwapSnapshotMap, Committed, LeaseSlot, NoCapacityPermit, NoGate, Principal,
    ReadyToStart, SnapshotMap,
};
use tollgate_client::{ManualClock, UsagePermit, UsageWriter, UsageWriterConfig};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CommitError, CostTable, CostUnits, DenyReason,
    DiscardedUsage, FencingToken, Generation, LeaseGrant, LeaseId, LocalLease, OpIndex,
    PermissionBits, RequestId, ResolvedLimits, UsageEvent,
};
use tollgate_store::{IngestError, IngestReport, UsageSink};

const ACCOUNT: AccountId = AccountId(1);
const PRINCIPAL: Principal = Principal(1);

#[derive(Clone, Copy)]
struct Operation;

impl OpIndex for Operation {
    fn index(&self) -> usize {
        0
    }
}

struct AcceptAll;

#[async_trait]
impl UsageSink for AcceptAll {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, IngestError> {
        Ok(IngestReport {
            unattributed: None,
            accepted: u64::try_from(events.len()).unwrap(),
            duplicate: 0,
            rejected: 0,
        })
    }
}

fn t(seconds: i64) -> Timestamp {
    Timestamp::from_second(seconds).unwrap()
}

fn engine() -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
    let limits = ResolvedLimits::new(64)
        .with_weighted_rate(1_000_000, 1_000_000)
        .with_concurrency(NonZeroU32::MIN, Some(NonZeroU32::MIN))
        .unwrap();
    let snapshot = Arc::new(
        AccountSnapshot::builder(
            ACCOUNT,
            Generation(1),
            AccountStatus::Active,
            t(1_000),
            PermissionBits::bit(0),
            limits,
            Arc::new(
                CostTable::builder(CostUnits(50), CostUnits(50))
                    .weight(&Operation, CostUnits(1))
                    .build(),
            ),
        )
        .build(),
    );
    let lease = Arc::new(LocalLease::new(
        LeaseGrant {
            lease_id: LeaseId(1),
            account_id: ACCOUNT,
            fencing_token: FencingToken(1),
            units: CostUnits(1_000),
            expires_at: t(1_000),
        },
        CostUnits::ZERO,
    ));
    let slot = LeaseSlot::for_account(ACCOUNT);
    slot.install(lease);
    engine.map().install(PRINCIPAL, snapshot, slot);
    engine
}

/// Drive the staged path up to the point of commit, binding `permit` as the
/// usage slot. The slot is chosen at admission now, so a committed charge
/// cannot reach execution without the queue capacity to bill it.
fn ready(
    engine: &AdmissionEngine<ArcSwapSnapshotMap>,
    permit: UsagePermit,
    now: Timestamp,
) -> Result<ReadyToStart<UsagePermit, NoCapacityPermit>, DenyReason> {
    engine
        .begin(PRINCIPAL, PermissionBits::bit(0), now)
        .and_then(|context| context.admit(&[(&Operation, 1)], permit, now))
        .and_then(|pending| {
            pending
                .acquire_capacity(&NoGate)
                .map_err(|(denied, _)| denied)
        })
}

/// An admission whose billing event is discarded: for the probes that are
/// about occupancy rather than usage.
fn probe(
    engine: &AdmissionEngine<ArcSwapSnapshotMap>,
    now: Timestamp,
) -> Result<Committed<tollgate_core::DiscardedUsageSlot, NoCapacityPermit>, DenyReason> {
    engine
        .begin(PRINCIPAL, PermissionBits::bit(0), now)
        .and_then(|context| context.admit(&[(&Operation, 1)], DiscardedUsage::new().slot(), now))
        .and_then(|pending| {
            pending
                .acquire_capacity(&NoGate)
                .map_err(|(denied, _)| denied)
        })
        .map(|ready| {
            ready
                .commit(RequestId(9_999), now)
                .expect("the probe commits against a live lease")
        })
}

#[tokio::test(flavor = "current_thread")]
async fn committed_charge_holds_concurrency_until_execution_guard_drops() {
    let engine = engine();
    let (recorder, writer) = UsageWriter::spawn(
        Arc::new(AcceptAll),
        Arc::new(ManualClock::new(t(0))),
        UsageWriterConfig {
            queue_capacity: 2,
            max_batch: 1,
            flush_interval: Duration::from_millis(1),
            retry_backoff: Duration::from_millis(1),
            shutdown_drain_deadline: Duration::from_secs(1),
            ingest_timeout: Duration::from_secs(1),
        },
    )
    .unwrap();

    let permit = recorder.try_reserve().unwrap();
    let charge = ready(&engine, permit, t(0))
        .expect("the first request admits")
        .commit(RequestId(1), t(0))
        .expect("a live lease commits");

    assert_eq!(
        probe(&engine, t(0)).unwrap_err(),
        DenyReason::ConcurrencyLimited,
        "committed work must remain in flight for the execution guard's lifetime"
    );

    drop(charge);
    drop(probe(&engine, t(0)).unwrap());
    drop(recorder);
    writer.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn failed_commit_releases_concurrency_and_accounting_capacity() {
    let engine = engine();
    let (recorder, writer) = UsageWriter::spawn(
        Arc::new(AcceptAll),
        Arc::new(ManualClock::new(t(0))),
        UsageWriterConfig {
            queue_capacity: 1,
            max_batch: 1,
            flush_interval: Duration::from_millis(1),
            retry_backoff: Duration::from_millis(1),
            shutdown_drain_deadline: Duration::from_secs(1),
            ingest_timeout: Duration::from_secs(1),
        },
    )
    .unwrap();

    let permit = recorder.try_reserve().unwrap();
    let error = match ready(&engine, permit, t(0))
        .expect("the request admits")
        .commit(RequestId(1), t(1_000))
    {
        Ok(_) => panic!("an expired lease cannot commit"),
        Err((error, _released)) => error,
    };
    assert_eq!(
        error,
        CommitError::Denied(DenyReason::FundingExpiredAtStart)
    );

    drop(probe(&engine, t(0)).expect("a failed commit releases both concurrency permits"));
    assert!(
        recorder.try_reserve().is_ok(),
        "a failed commit releases its unused queue permit"
    );

    drop(recorder);
    writer.shutdown().await.unwrap();
}
