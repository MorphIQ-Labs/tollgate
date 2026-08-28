use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jiff::Timestamp;
use tollgate_admission::{
    AdmissionEngine, AdmissionRequest, ArcSwapSnapshotMap, LeaseSlot, Principal, SnapshotMap,
};
use tollgate_alloc_count::{AllocScope, Allocations};
use tollgate_auth::{HmacRegistry, SessionCredential};
use tollgate_client::{ChargeGuard, ManualClock, UsageRecorder, UsageWriter, UsageWriterConfig};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, EnforcementMode, FencingToken,
    Generation, LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, RequestId,
    ResolvedLimits, UsageEvent, UsageSource,
};
use tollgate_store::{IngestReport, StoreError, UsageSink};

tollgate_alloc_count::install!();

#[derive(Clone, Copy)]
struct PriceOp;

impl OpIndex for PriceOp {
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

fn now() -> Timestamp {
    Timestamp::from_second(1_755_600_000).unwrap()
}

fn far_future() -> Timestamp {
    Timestamp::from_second(4_102_444_800).unwrap()
}

fn writer_config() -> UsageWriterConfig {
    UsageWriterConfig {
        queue_capacity: 16,
        max_batch: 1,
        flush_interval: Duration::from_secs(60),
        retry_backoff: Duration::from_millis(10),
        shutdown_drain_deadline: Duration::from_secs(5),
        ingest_timeout: Duration::from_secs(5),
    }
}

fn warm_event(request_id: u128) -> UsageEvent {
    UsageEvent {
        request_id: RequestId(request_id),
        account_id: AccountId(1),
        source: UsageSource::Overage,
        units: CostUnits(1),
        occurred_at: now(),
    }
}

async fn wait_until_drained(recorder: &UsageRecorder) {
    for _ in 0..1_000 {
        let health = recorder.health();
        if health.queue_depth == 0 && health.unaccounted == 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("usage writer did not drain its warm-up event");
}

fn install_admission(principal: Principal) -> AdmissionEngine<ArcSwapSnapshotMap> {
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
    let snapshot = Arc::new(AccountSnapshot {
        account_id: AccountId(1),
        key_id: None,
        generation: Generation(1),
        status: AccountStatus::Active,
        enforcement_mode: EnforcementMode::Strict,
        valid_until: far_future(),
        permissions: PermissionBits::bit(0),
        limits: ResolvedLimits {
            max_items_per_request: 4_096,
            rate_units_per_second: u64::from(u32::MAX),
            rate_burst_units: u64::from(u32::MAX),
        },
        cost_table: Arc::new(
            CostTable::builder(CostUnits(50), CostUnits(50))
                .weight(&PriceOp, CostUnits(1))
                .build(),
        ),
    });
    let lease = Arc::new(LocalLease::new(
        LeaseGrant {
            lease_id: LeaseId(7),
            account_id: AccountId(1),
            fencing_token: FencingToken(3),
            units: CostUnits(u64::MAX / 2),
            expires_at: far_future(),
        },
        CostUnits::ZERO,
    ));
    let slot = LeaseSlot::for_account(AccountId(1));
    slot.install(lease);
    engine.map().install(principal, snapshot, slot);
    engine
}

fn request(principal: Principal) -> AdmissionRequest<'static, PriceOp> {
    AdmissionRequest {
        principal,
        required: PermissionBits::bit(0),
        op: &PriceOp,
        items: 64,
    }
}

fn record(scope: &str, attribution: &str, allocations: Allocations) {
    tollgate_alloc_count::record_if_requested!(scope, attribution, allocations).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn embedding_path_allocates_nothing_after_warmup() {
    let clock = Arc::new(ManualClock::new(now()));
    let (recorder, writer) =
        UsageWriter::spawn(Arc::new(AcceptAll), clock, writer_config()).unwrap();

    // Exercise more than one Tokio mpsc block boundary, with at most one
    // event in flight so released blocks are available for reuse.
    for request_id in 0..65 {
        recorder
            .try_reserve()
            .unwrap()
            .record(warm_event(request_id));
        wait_until_drained(&recorder).await;
    }

    let mut registry = HmacRegistry::new(b"allocation-test-secret");
    let principal = registry.register(b"credential-one");
    let session = SessionCredential::new();
    session
        .authenticate(Some(b"credential-one"), &registry, now())
        .expect("registered credential");
    black_box(
        session
            .authenticate(Some(b"credential-one"), &registry, now())
            .expect("warm cached credential"),
    );

    let engine = install_admission(principal);
    let warm = engine.admit(request(principal), now()).unwrap();
    black_box(warm.reservation.cancel());

    let (_, caller_buffer) = AllocScope::measure(|| {
        let mut body = Vec::with_capacity(128);
        body.extend_from_slice(black_box(b"caller-owned request body"));
        black_box(body);
    });
    record("caller/request_buffer", "caller", caller_buffer);
    assert!(
        !caller_buffer.is_allocation_free(),
        "caller-buffer line is the report's non-vacuous control"
    );

    let (_, request_id_allocation) = AllocScope::measure(|| {
        black_box(uuid::Uuid::new_v4().as_u128());
    });
    record("caller/request_id", "caller", request_id_allocation);

    let request_id = RequestId(uuid::Uuid::new_v4().as_u128());
    let ((), tollgate) = AllocScope::measure(|| {
        let authenticated = session
            .authenticate(Some(b"credential-one"), &registry, now())
            .expect("cached credential");
        let permit = recorder.try_reserve().expect("warmed queue has capacity");
        let admitted = engine.admit(request(authenticated), now()).unwrap();
        let (charge, units) =
            ChargeGuard::commit(&admitted.reservation, permit, request_id, now()).unwrap();
        black_box(units);
        drop(charge);
    });
    record("embedding/cached_auth_through_record", "tollgate", tollgate);
    assert!(
        tollgate.is_allocation_free(),
        "warmed embedding path allocated: {tollgate:?}"
    );
    wait_until_drained(&recorder).await;

    let (job, executor) = AllocScope::measure(|| tokio::spawn(async { black_box(()) }));
    record("consumer/executor_job", "consumer_executor", executor);
    assert!(
        !executor.is_allocation_free(),
        "executor-job line must remain separate and non-vacuous"
    );
    job.await.unwrap();

    drop(recorder);
    writer.shutdown().await.unwrap();
}
