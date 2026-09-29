use axum::body::Body;
use axum::extract::{ConnectInfo, Request};
use jiff::{SignedDuration, Timestamp};
use std::sync::Arc;
use std::time::Duration;
use tollgate_admission::{ArcSwapSnapshotMap, NoGate};
use tollgate_auth::{CredentialVerifier, Verified};
use tollgate_axum::{
    AdapterConfig, BearerAuth, InputLimits, RequestIdSource, RequestIdUnavailable, Tollgate,
    TollgateConnection,
};
use tollgate_client::{
    AccountLeaseConfig, Clock, InstanceRuntime, InstanceRuntimeConfig, ManualClock,
    SnapshotManagerConfig, TrackedPrincipals, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, Generation,
    LocalSharding, OpIndex, PermissionBits, Principal, PublishableSnapshot, RequestId,
    ResolvedLimits,
};
use tollgate_store::{AccountConfig, GrantPolicy, MemoryStore};

pub struct Verifier;
impl CredentialVerifier for Verifier {
    fn verify(&self, credential: &[u8]) -> Option<Verified> {
        (credential == b"demo-fixture-key").then(|| Verified::until(Principal(1), time(200)))
    }
}
#[derive(Default)]
pub struct TestIds(std::sync::atomic::AtomicU64);
impl RequestIdSource for TestIds {
    fn next_id(&self) -> Result<RequestId, RequestIdUnavailable> {
        Ok(RequestId(
            u128::from(self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)) + 1,
        ))
    }
}
#[derive(Clone, Copy)]
pub struct Op;
impl OpIndex for Op {
    fn index(&self) -> usize {
        0
    }
}
pub type Adapter = Tollgate<BearerAuth<Verifier>, ManualClock, TestIds, NoGate>;

pub fn time(n: i64) -> Timestamp {
    Timestamp::from_second(n).unwrap()
}
pub fn limits(bytes: usize) -> InputLimits {
    InputLimits::new(bytes, Duration::from_secs(1)).unwrap()
}

pub async fn fixture() -> (Adapter, InstanceRuntime, Arc<MemoryStore>, Arc<ManualClock>) {
    fixture_with(NoGate, 1, CapacityClass::Assured).await
}

pub async fn fixture_with<G: tollgate_admission::CapacityGate>(
    capacity: G,
    queue_capacity: usize,
    class: CapacityClass,
) -> (
    Tollgate<BearerAuth<Verifier>, ManualClock, TestIds, G>,
    InstanceRuntime,
    Arc<MemoryStore>,
    Arc<ManualClock>,
) {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1000),
        status: AccountStatus::Active,
        capacity_class: class,
    });
    let snapshot = AccountSnapshot::builder(
        AccountId(1),
        Generation(1),
        AccountStatus::Active,
        time(1000),
        PermissionBits::bit(0),
        ResolvedLimits::new(100),
        Arc::new(
            CostTable::builder(CostUnits::ZERO, CostUnits::ZERO)
                .weight(&Op, CostUnits(1))
                .build(),
        ),
    )
    .capacity_class(class)
    .build();
    store
        .publish_snapshot(
            Principal(1),
            PublishableSnapshot::try_new(Arc::new(snapshot)).unwrap(),
        )
        .unwrap();
    let clock = Arc::new(ManualClock::new(time(100)));
    let config = InstanceRuntimeConfig {
        snapshots: SnapshotManagerConfig {
            principals: TrackedPrincipals::Fixed(vec![Principal(1)]),
            refresh_interval: Duration::from_millis(20),
            unknown_ttl: SignedDuration::from_secs(1),
            revoked_ttl: SignedDuration::from_secs(1),
            retry_backoff: Duration::from_millis(5),
            max_concurrent_fetches: 1,
            fetch_timeout: Duration::from_millis(100),
            enumeration_timeout: Duration::from_millis(100),
        },
        leases: AccountLeaseConfig {
            target_grant: CostUnits(100),
            low_water: CostUnits(10),
            lease_ttl: SignedDuration::from_secs(300),
            expiry_safety_margin: SignedDuration::from_secs(2),
            poll_interval: Duration::from_millis(5),
            store_call_timeout: Duration::from_millis(100),
            shutdown_release_deadline: Duration::from_millis(100),
        },
        usage: UsageWriterConfig {
            queue_capacity,
            max_batch: 1,
            flush_interval: Duration::from_millis(5),
            retry_backoff: Duration::from_millis(5),
            shutdown_drain_deadline: Duration::from_millis(100),
            ingest_timeout: Duration::from_millis(100),
        },
        sharding: LocalSharding::SINGLE,
        snapshot_history_capacity: ArcSwapSnapshotMap::DEFAULT_GENERATION_CAPACITY,
        idle_account_linger: Duration::from_millis(30),
        manager_restart_backoff: Duration::from_millis(20),
        shutdown_deadline: Duration::from_millis(250),
    };
    let (runtime, handle) = InstanceRuntime::spawn(
        store.clone(),
        store.clone(),
        store.clone(),
        clock.clone(),
        config,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !handle.readiness(clock.now()).is_ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let adapter = Tollgate::new(AdapterConfig {
        runtime: handle,
        authenticator: BearerAuth::new(Arc::new(Verifier)),
        clock: clock.clone(),
        request_ids: TestIds::default(),
        capacity,
    });
    (adapter, runtime, store, clock)
}

pub fn request(body: Body) -> Request {
    let mut request = Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json")
        .header("authorization", "Bearer demo-fixture-key")
        .body(body)
        .unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(TollgateConnection::default()));
    request
}
