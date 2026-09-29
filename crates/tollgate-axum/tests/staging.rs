use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, Bytes, HttpBody};
use axum::extract::{ConnectInfo, DefaultBodyLimit, Request};
use axum::http::StatusCode;
use axum::routing::post;
use jiff::{SignedDuration, Timestamp};
use serde::Deserialize;
use tollgate_admission::{ArcSwapSnapshotMap, NoGate};
use tollgate_auth::{CredentialVerifier, Verified};
use tollgate_axum::{
    AdapterConfig, BearerAuth, InputLimits, InputLimitsError, Rejection, RequestIdSource,
    RequestIdUnavailable, Tollgate, TollgateConnection,
};
use tollgate_client::{
    AccountLeaseConfig, Clock, InstanceRuntime, InstanceRuntimeConfig, ManualClock,
    SnapshotManagerConfig, TrackedPrincipals, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, DenyReason,
    Generation, LocalSharding, OpIndex, PermissionBits, Principal, PublishableSnapshot, RequestId,
    ResolvedLimits,
};
use tollgate_store::{AccountConfig, GrantPolicy, MemoryStore};
use tower::ServiceExt;

struct Verifier;
impl CredentialVerifier for Verifier {
    fn verify(&self, credential: &[u8]) -> Option<Verified> {
        (credential == b"demo-fixture-key").then(|| Verified::until(Principal(1), time(200)))
    }
}
struct TestIds;
impl RequestIdSource for TestIds {
    fn next_id(&self) -> Result<RequestId, RequestIdUnavailable> {
        Ok(RequestId(1))
    }
}
#[derive(Clone, Copy)]
struct Op;
impl OpIndex for Op {
    fn index(&self) -> usize {
        0
    }
}
type Adapter = Tollgate<BearerAuth<Verifier>, ManualClock, TestIds, NoGate>;

fn time(n: i64) -> Timestamp {
    Timestamp::from_second(n).unwrap()
}
fn limits(bytes: usize) -> InputLimits {
    InputLimits::new(bytes, Duration::from_secs(1)).unwrap()
}

async fn fixture() -> (Adapter, InstanceRuntime, Arc<MemoryStore>, Arc<ManualClock>) {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1000),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
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
            queue_capacity: 1,
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
        request_ids: TestIds,
        capacity: NoGate,
    });
    (adapter, runtime, store, clock)
}

fn request(body: Body) -> Request {
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

struct UnreadBody(Arc<AtomicUsize>);
impl HttpBody for UnreadBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Infallible>>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}
#[derive(Debug, Deserialize)]
struct Input {
    items: u64,
}

#[test]
fn route_limits_refuse_zero_and_unrepresentable_deadlines() {
    assert_eq!(
        InputLimits::new(0, Duration::from_secs(1)).unwrap_err(),
        InputLimitsError::ZeroBytes
    );
    assert_eq!(
        InputLimits::new(1, Duration::ZERO).unwrap_err(),
        InputLimitsError::ZeroTimeout
    );
    assert_eq!(
        InputLimits::new(1, Duration::MAX).unwrap_err(),
        InputLimitsError::TimeoutOverflow
    );
    assert!(InputLimits::new(1, Duration::from_nanos(1)).is_ok());
}

#[tokio::test(start_paused = true)]
async fn identity_permission_and_backpressure_refuse_before_body_poll() {
    let (adapter, runtime, store, _) = fixture().await;
    for case in 0..4 {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut req = request(Body::new(UnreadBody(polls.clone())));
        let mut permission = PermissionBits::bit(0);
        let mut held = None;
        match case {
            0 => {
                req.headers_mut().remove("authorization");
            }
            1 => {
                req.extensions_mut()
                    .remove::<ConnectInfo<TollgateConnection>>();
            }
            2 => permission = PermissionBits::bit(1),
            _ => held = Some(adapter.runtime().recorder().try_reserve().unwrap()),
        }
        let result = adapter
            .prepare_json::<Input>(req, permission, limits(100))
            .await;
        match (case, result) {
            (0, Err(Rejection::Denied(DenyReason::UnknownPrincipal)))
            | (1, Err(Rejection::MissingConnection))
            | (2, Err(Rejection::Denied(DenyReason::MissingPermission)))
            | (3, Err(Rejection::Denied(DenyReason::AccountingBackpressure))) => {}
            _ => panic!("wrong pre-body rejection for case {case}"),
        }
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(held);
        drop(adapter.runtime().recorder().try_reserve().unwrap());
    }
    let counts = adapter.runtime().counters().snapshot();
    assert_eq!(counts.denied(), 3);
    assert_eq!(counts.denials[DenyReason::UnknownPrincipal.index()], 1);
    assert_eq!(counts.denials[DenyReason::MissingPermission.index()], 1);
    assert_eq!(
        counts.denials[DenyReason::AccountingBackpressure.index()],
        1
    );
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
    assert_eq!(store.balance(AccountId(1)), CostUnits(1000));
}

#[tokio::test(start_paused = true)]
async fn malformed_oversized_and_timed_out_input_release_accounting_capacity() {
    let (adapter, runtime, store, _) = fixture().await;
    for case in 0..4 {
        let mut req = match case {
            0 => request(Body::from("not JSON")),
            1 => request(Body::from(" ".repeat(101))),
            2 => request(Body::new(UnreadBody(Arc::default()))),
            _ => request(Body::from(r#"{"items":3}"#)),
        };
        if case == 3 {
            req.headers_mut().remove("content-type");
        }
        let result = adapter
            .prepare_json::<Input>(req, PermissionBits::bit(0), limits(100))
            .await;
        match result {
            Err(Rejection::BodyTooLarge) => assert_eq!(case, 1),
            Err(Rejection::Json(error)) => assert_eq!(
                error.status(),
                match case {
                    0 => StatusCode::BAD_REQUEST,
                    3 => StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    _ => panic!("wrong JSON error"),
                }
            ),
            Err(Rejection::BodyTimeout) => assert_eq!(case, 2),
            _ => panic!("input did not produce expected rejection"),
        }
        drop(adapter.runtime().recorder().try_reserve().unwrap());
    }
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
    assert_eq!(store.balance(AccountId(1)), CostUnits(1000));
}

#[tokio::test(start_paused = true)]
async fn prepared_input_keeps_its_original_context_and_one_permit() {
    let (adapter, runtime, store, _) = fixture().await;
    let prepared = adapter
        .prepare_json::<Input>(
            request(Body::from(r#"{"items":3}"#)),
            PermissionBits::bit(0),
            limits(100),
        )
        .await
        .unwrap();
    assert!(adapter.runtime().recorder().try_reserve().is_err());
    let (input, context, permit) = prepared.into_parts();
    assert_eq!(input.items, 3);
    assert_eq!(context.generation(), Generation(1));
    let pending = context
        .admit(&[(Op, input.items)], permit, time(100))
        .unwrap();
    drop(
        pending
            .acquire_capacity(&NoGate)
            .unwrap()
            .commit(RequestId(8), time(100))
            .unwrap(),
    );
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(3));
}

#[tokio::test(start_paused = true)]
async fn dropping_a_body_read_releases_the_reserved_slot() {
    let (adapter, runtime, store, _) = fixture().await;
    let polls = Arc::new(AtomicUsize::new(0));
    let req = request(Body::new(UnreadBody(polls.clone())));
    let worker = adapter.clone();
    let task = tokio::spawn(async move {
        worker
            .prepare_json::<Input>(req, PermissionBits::bit(0), limits(100))
            .await
    });
    for _ in 0..100 {
        if polls.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(polls.load(Ordering::SeqCst) > 0, "body read never began");
    assert!(adapter.runtime().recorder().try_reserve().is_err());
    task.abort();
    assert!(task.await.is_err());
    drop(adapter.runtime().recorder().try_reserve().unwrap());
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
    assert_eq!(store.balance(AccountId(1)), CostUnits(1000));
}

#[tokio::test(start_paused = true)]
async fn an_outer_body_limit_is_not_widened_and_unmatched_routes_are_unmetered() {
    let (adapter, runtime, _, _) = fixture().await;
    let route = post(move |req| {
        let adapter = adapter.clone();
        async move {
            match adapter
                .prepare_json::<Input>(req, PermissionBits::bit(0), limits(100))
                .await
            {
                Err(Rejection::BodyTooLarge) => StatusCode::PAYLOAD_TOO_LARGE,
                Err(Rejection::Json(error)) => error.status(),
                _ => StatusCode::OK,
            }
        }
    })
    .layer(DefaultBodyLimit::max(2));
    let router = axum::Router::new().route("/", route);
    assert_eq!(
        router
            .clone()
            .oneshot(request(Body::from(r#"{"items":3}"#)))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let req = Request::builder()
        .uri("/missing")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(req).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    assert_eq!(
        router.oneshot(req).await.unwrap().status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
}

#[tokio::test(start_paused = true)]
async fn cached_identity_still_expires_and_shutdown_closes_admission() {
    let (adapter, runtime, _, clock) = fixture().await;
    let connection = ConnectInfo(TollgateConnection::default());
    let make = || {
        let mut req = request(Body::from(r#"{"items":3}"#));
        req.extensions_mut().insert(connection.clone());
        req
    };
    drop(
        adapter
            .prepare_json::<Input>(make(), PermissionBits::bit(0), limits(100))
            .await
            .unwrap(),
    );
    clock.set(time(200));
    assert!(matches!(
        adapter
            .prepare_json::<Input>(make(), PermissionBits::bit(0), limits(100))
            .await,
        Err(Rejection::Denied(DenyReason::UnknownPrincipal))
    ));
    clock.set(time(100));
    adapter.runtime().request_shutdown();
    for _ in 0..100 {
        if adapter.runtime().recorder().is_closed() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(adapter.runtime().recorder().is_closed());
    assert!(matches!(
        adapter
            .prepare_json::<Input>(make(), PermissionBits::bit(0), limits(100))
            .await,
        Err(Rejection::Denied(DenyReason::AccountingBackpressure))
    ));
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
}
