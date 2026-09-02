// Exercises the deprecated one-shot surface on purpose: it is supported
// for a minor and must keep working.
#![allow(deprecated)]

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jiff::Timestamp;
use tollgate_admission::{
    AdmissionEngine, AdmissionRequest, ArcSwapSnapshotMap, LeaseSlot, Principal, SnapshotMap,
};
use tollgate_client::{ChargeGuard, ManualClock, UsageWriter, UsageWriterConfig};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CommitError, CostTable, CostUnits, DenyReason,
    FencingToken, Generation, LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, RequestId,
    ResolvedLimits, UsageEvent,
};
use tollgate_store::{IngestReport, StoreError, UsageSink};

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
    ) -> Result<IngestReport, StoreError> {
        Ok(IngestReport {
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

fn request() -> AdmissionRequest<'static, Operation> {
    AdmissionRequest {
        principal: PRINCIPAL,
        required: PermissionBits::bit(0),
        op: &Operation,
        items: 1,
    }
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

    let admitted = engine.admit(request(), t(0)).unwrap();
    let permit = recorder.try_reserve().unwrap();
    let charge = ChargeGuard::commit(admitted, permit, RequestId(1), t(0)).unwrap();

    assert_eq!(
        engine.admit(request(), t(0)).unwrap_err(),
        DenyReason::ConcurrencyLimited,
        "committed work must remain in flight for the execution guard's lifetime"
    );

    drop(charge);
    drop(engine.admit(request(), t(0)).unwrap());
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

    let admitted = engine.admit(request(), t(0)).unwrap();
    let permit = recorder.try_reserve().unwrap();
    let error = match ChargeGuard::commit(admitted, permit, RequestId(1), t(1_000)) {
        Ok(_) => panic!("an expired lease cannot commit"),
        Err(error) => error,
    };
    assert_eq!(error, CommitError::LeaseExpired);

    drop(
        engine
            .admit(request(), t(0))
            .expect("a failed commit releases both concurrency permits"),
    );
    assert!(
        recorder.try_reserve().is_ok(),
        "a failed commit releases its unused queue permit"
    );

    drop(recorder);
    writer.shutdown().await.unwrap();
}
