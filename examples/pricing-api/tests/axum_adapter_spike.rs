//! GH-60: an unpublished feasibility spike for the route-wrapper boundary.
//!
//! Real admission guards run through Axum, but fixture identity and a recording
//! slot stand in for authentication and the bounded writer. This establishes
//! the generic/async ownership shape, not the complete adapter contract.

use std::future::Future;
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, post};
use jiff::{SignedDuration, Timestamp};
use serde::Deserialize;
use tollgate_admission::{
    AdmissionEngine, ArcSwapSnapshotMap, LeaseSlot, NoGate, RequestContext, SnapshotMap,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, FencingToken, Generation,
    LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, Principal, RequestId, ResolvedLimits,
    UsageEvent, UsageSlot,
};
use tower::ServiceExt;

#[derive(Clone, Copy)]
struct Quote;

impl OpIndex for Quote {
    fn index(&self) -> usize {
        0
    }
}

struct RecordingSlot(Arc<Mutex<Vec<UsageEvent>>>);

impl UsageSlot for RecordingSlot {
    fn record(self, event: UsageEvent) {
        self.0.lock().unwrap().push(event);
    }
}

// A buffered output type, not arbitrary IntoResponse: a streaming body cannot
// escape the guard that owns its execution lifetime.
struct Buffered(StatusCode, Bytes);

fn post_json<T, V, P, Validate, Execute, F>(
    prepare: P,
    validate: Validate,
    execute: Execute,
) -> MethodRouter
where
    T: serde::de::DeserializeOwned + Send + 'static,
    V: Send + 'static,
    P: Fn() -> Result<(RequestContext, RecordingSlot), StatusCode> + Clone + Send + Sync + 'static,
    Validate: Fn(T) -> Result<(V, u64), StatusCode> + Clone + Send + Sync + 'static,
    Execute: Fn(V, CostUnits) -> F + Clone + Send + Sync + 'static,
    F: Future<Output = Buffered> + Send + 'static,
{
    post(move |request: Request| {
        let prepare = prepare.clone();
        let validate = validate.clone();
        let execute = execute.clone();
        async move {
            let (context, slot) = match prepare() {
                Ok(staged) => staged,
                Err(status) => return status.into_response(),
            };
            let Json(input) = match Json::<T>::from_request(request, &()).await {
                Ok(input) => input,
                Err(rejection) => return rejection.into_response(),
            };
            let (input, quantity) = match validate(input) {
                Ok(validated) => validated,
                Err(status) => return status.into_response(),
            };
            let pending = match context.admit(&[(Quote, quantity)], slot, now()) {
                Ok(pending) => pending,
                Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
            };
            let ready = pending.acquire_capacity(&NoGate).unwrap();
            let committed = match ready.commit(RequestId(1), now()) {
                Ok(committed) => committed,
                Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
            };
            // No await or fallible extraction between commit and invoking the
            // business callback. Its factory and its future are both billable.
            let Buffered(status, body) = execute(input, committed.units()).await;
            drop(committed);
            (status, body).into_response()
        }
    })
    .layer(DefaultBodyLimit::max(64))
}

fn now() -> Timestamp {
    Timestamp::from_second(1_000).unwrap()
}

struct Fixture {
    engine: Arc<AdmissionEngine<ArcSwapSnapshotMap>>,
    events: Arc<Mutex<Vec<UsageEvent>>>,
    lease: Arc<LocalLease>,
}

impl Fixture {
    fn new() -> Self {
        let account = AccountId(1);
        let expiry = Timestamp::from_second(2_000).unwrap();
        let lease = Arc::new(LocalLease::with_safety_margin(
            LeaseGrant {
                lease_id: LeaseId(1),
                account_id: account,
                fencing_token: FencingToken(1),
                units: CostUnits(100),
                expires_at: expiry,
            },
            CostUnits::ZERO,
            SignedDuration::ZERO,
        ));
        let slot = LeaseSlot::for_account(account);
        drop(slot.replace(lease.clone()));
        let engine = Arc::new(AdmissionEngine::new(ArcSwapSnapshotMap::new()));
        engine
            .map()
            .install(
                Principal(1),
                Arc::new(
                    AccountSnapshot::builder(
                        account,
                        Generation(1),
                        AccountStatus::Active,
                        expiry,
                        PermissionBits::bit(0),
                        ResolvedLimits::new(100),
                        Arc::new(
                            CostTable::builder(CostUnits::ZERO, CostUnits::ZERO)
                                .weight(&Quote, CostUnits(2))
                                .build(),
                        ),
                    )
                    .build(),
                ),
                slot,
            )
            .unwrap();
        Self {
            engine,
            events: Arc::default(),
            lease,
        }
    }

    fn prepare(
        &self,
    ) -> impl Fn() -> Result<(RequestContext, RecordingSlot), StatusCode> + Clone + use<> {
        let engine = self.engine.clone();
        let events = self.events.clone();
        move || {
            let context = engine
                .begin(Principal(1), PermissionBits::bit(0), now())
                .map_err(|_| StatusCode::UNAUTHORIZED)?;
            Ok((context, RecordingSlot(events.clone())))
        }
    }
}

#[derive(Deserialize)]
struct Input {
    items: u64,
}

fn validate(input: Input) -> Result<(Input, u64), StatusCode> {
    if input.items == 0 {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    let count = input.items;
    Ok((input, count))
}

fn request(body: &str) -> Request {
    Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

async fn send(route: MethodRouter, body: &str) -> Response {
    axum::Router::new()
        .route("/", route)
        .oneshot(request(body))
        .await
        .unwrap()
}

#[tokio::test]
async fn spike_body_and_validation_rejections_never_invoke_business_work() {
    for (body, status) in [
        ("invalid".to_owned(), StatusCode::BAD_REQUEST),
        (" ".repeat(65), StatusCode::PAYLOAD_TOO_LARGE),
        (
            r#"{"items":0}"#.to_owned(),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let fixture = Fixture::new();
        let route = post_json(fixture.prepare(), validate, |_: Input, _| async {
            panic!("rejected input reached execution")
        });
        assert_eq!(send(route, &body).await.status(), status);
        assert!(fixture.events.lock().unwrap().is_empty());
        assert_eq!(fixture.lease.remaining(), CostUnits(100));
    }
}

#[tokio::test]
async fn spike_fixed_and_body_derived_quantities_use_real_admission() {
    for fixed in [false, true] {
        let fixture = Fixture::new();
        let quantity = if fixed { 1 } else { 3 };
        let route = post_json(
            fixture.prepare(),
            move |input: Input| {
                if fixed {
                    Ok((input, 1))
                } else {
                    validate(input)
                }
            },
            move |_: Input, units| async move {
                assert_eq!(units, CostUnits(quantity * 2));
                Buffered(StatusCode::OK, Bytes::from_static(b"done"))
            },
        );
        assert_eq!(send(route, r#"{"items":3}"#).await.status(), StatusCode::OK);
        assert_eq!(fixture.events.lock().unwrap().len(), 1);
        assert_eq!(fixture.lease.remaining(), CostUnits(100 - quantity * 2));
    }
}

#[tokio::test]
async fn spike_canceling_a_started_handler_emits_once() {
    let fixture = Fixture::new();
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let route = post_json(fixture.prepare(), validate, move |_: Input, _| {
        let signal = signal.clone();
        async move {
            signal.notify_one();
            std::future::pending::<Buffered>().await
        }
    });
    let task = tokio::spawn(send(route, r#"{"items":3}"#));
    entered.notified().await;
    assert!(fixture.events.lock().unwrap().is_empty());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(fixture.events.lock().unwrap().len(), 1);
    assert_eq!(fixture.lease.remaining(), CostUnits(94));
}
