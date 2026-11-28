//! The budget estimate as an application served through the adapter sees it
//! (INVARIANTS.md 20 and 28). Publication, refresh, usage ingestion and the
//! estimate's convergence are the production runtime's; the tests only spend,
//! deposit, read and wait.

mod support;

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::Request,
    http::{Method, StatusCode},
};
use jiff::Timestamp;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::*;
use tollgate_admission::{CapacityGate, ExecutionCapacityGate, ExecutionCapacityMode, NoGate};
use tollgate_axum::{
    AdapterConfig, BearerAuth, BufferedResponse, InputError, Rejection, RequestIdSource,
    RequestIdUnavailable, ResponseError, Tollgate, Validated, render_rejection,
};
use tollgate_client::{InstanceRuntime, ManualClock};
use tollgate_core::{
    AccountId, BudgetSchedule, BudgetView, CapacityClass, CostUnits, DenyReason, LocalSharding,
    PermissionBits,
};
use tollgate_store::{AdminStore, MemoryStore};
use tower::ServiceExt;

const ACCOUNT: AccountId = AccountId(1);
const ROUTE: PermissionBits = PermissionBits::bit(0);
const OPENING: u64 = 1_000;

fn get_request() -> Request {
    let mut request = request(Body::empty());
    *request.method_mut() = Method::GET;
    request
}

fn read<A, I, G>(adapter: &Tollgate<A, ManualClock, I, G>) -> Option<BudgetView>
where
    A: tollgate_axum::RequestAuthenticator,
{
    adapter
        .read_budget(&get_request().into_parts().0, ROUTE)
        .expect("the fixture credential reads")
}

/// Wait for the runtime's own refresh to deliver a balance.
async fn until_balance<A, I, G>(
    adapter: &Tollgate<A, ManualClock, I, G>,
    balance: u64,
) -> BudgetView
where
    A: tollgate_axum::RequestAuthenticator,
{
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(view) = read(adapter)
                && view.balance == CostUnits(balance)
            {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the estimate never reached {balance}: {:?}", read(adapter)))
}

/// The account on a monthly schedule, so observations carry a period end.
async fn scheduled<G: CapacityGate>(
    capacity: G,
) -> (
    Tollgate<BearerAuth<Verifier>, ManualClock, TestIds, G>,
    InstanceRuntime,
    Arc<MemoryStore>,
    Arc<ManualClock>,
    BudgetView,
) {
    let (adapter, runtime, store, clock) = fixture_with(capacity, 8, CapacityClass::Assured).await;
    AdminStore::set_budget_schedule(
        &*store,
        ACCOUNT,
        Some(BudgetSchedule::monthly(CostUnits(500))),
    )
    .await
    .unwrap();
    let view = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(view) = read(&adapter)
                && view.period_end.is_some()
            {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("a refresh delivers the schedule's period");
    assert_eq!(view.balance, CostUnits(OPENING));
    (adapter, runtime, store, clock, view)
}

async fn body(response: axum::response::Response) -> (StatusCode, String) {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn render_view(view: Option<BudgetView>) -> Result<BufferedResponse, ResponseError> {
    let view = view.expect("the store reports a budget");
    BufferedResponse::json(
        StatusCode::OK,
        &serde_json::json!({
            "balance": view.balance.get(),
            "period_end": view.period_end.map(|at: Timestamp| at.to_string()),
        }),
    )
}

#[tokio::test(start_paused = true)]
async fn budget_reads_report_one_observation_and_leave_the_instance_untouched() {
    let (adapter, runtime, store, clock, view) = scheduled(NoGate).await;
    let app = Router::new()
        .route(
            "/",
            adapter.get_budget(ROUTE, render_view, |error| render_rejection(error, None)),
        )
        .route(
            "/forbidden",
            adapter.get_budget(PermissionBits::bit(5), render_view, |error| {
                render_rejection(error, None)
            }),
        );
    // The only queue slots there are, all taken: a read needs none.
    let held: Vec<_> = (0..8)
        .map(|_| adapter.runtime().recorder().try_reserve().unwrap())
        .collect();
    assert!(adapter.runtime().recorder().try_reserve().is_err());
    let untouched = adapter.runtime().counters().snapshot();
    let expected = serde_json::json!({
        "balance": OPENING,
        "period_end": view.period_end.unwrap().to_string(),
    });

    for _ in 0..3 {
        let (status, text) = body(app.clone().oneshot(get_request()).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&text).unwrap(),
            expected
        );
    }
    assert_eq!(read(&adapter), Some(view));

    // Every refusal there is on this path: permission, no credential, an
    // unknown one, no connection state, and a credential that has expired.
    let mut forbidden = get_request();
    *forbidden.uri_mut() = "/forbidden".parse().unwrap();
    let mut absent = get_request();
    absent.headers_mut().remove("authorization");
    let mut unknown = get_request();
    unknown
        .headers_mut()
        .insert("authorization", "Bearer nobody".parse().unwrap());
    let mut unconnected = get_request();
    unconnected.extensions_mut().clear();
    for (refused, status, code) in [
        (forbidden, StatusCode::FORBIDDEN, "forbidden"),
        (absent, StatusCode::UNAUTHORIZED, "unknown-principal"),
        (unknown, StatusCode::UNAUTHORIZED, "unknown-principal"),
        (
            unconnected,
            StatusCode::INTERNAL_SERVER_ERROR,
            "missing-connection-state",
        ),
    ] {
        let (actual, text) = body(app.clone().oneshot(refused).await.unwrap()).await;
        assert_eq!(actual, status);
        assert!(text.contains(code), "{text}");
    }
    clock.set(time(200));
    assert!(matches!(
        adapter.read_budget(&get_request().into_parts().0, ROUTE),
        Err(Rejection::Denied(DenyReason::UnknownPrincipal))
    ));

    assert_eq!(adapter.runtime().counters().snapshot(), untouched);
    drop(held);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits::ZERO);
}

#[tokio::test(start_paused = true)]
async fn a_failed_budget_response_goes_to_the_error_handler() {
    let (adapter, runtime, _, _) = fixture_with(NoGate, 8, CapacityClass::Assured).await;
    let app = Router::new().route(
        "/",
        adapter.get_budget(
            ROUTE,
            |_| Err(ResponseError::Serialization),
            |error| {
                assert!(matches!(
                    error,
                    Rejection::Response(ResponseError::Serialization)
                ));
                render_rejection(error, None)
            },
        ),
    );
    let (status, text) = body(app.oneshot(get_request()).await.unwrap()).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(text.contains("response-failed"), "{text}");
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_budget_route_converges_on_settled_charges() {
    let (adapter, runtime, store, _) = fixture_with(NoGate, 8, CapacityClass::Assured).await;
    let app = Router::new()
        .route(
            "/",
            adapter.post(
                Op,
                ROUTE,
                || Ok(Validated::new((), 7)),
                |(), _| async { BufferedResponse::bytes(StatusCode::OK, "done") },
            ),
        )
        .route(
            "/budget",
            adapter.get_budget(ROUTE, render_view, |error| render_rejection(error, None)),
        );
    for _ in 0..2 {
        let response = app.clone().oneshot(request(Body::empty())).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    until_balance(&adapter, OPENING - 14).await;
    let mut budget = get_request();
    *budget.uri_mut() = "/budget".parse().unwrap();
    let (_, text) = body(app.oneshot(budget).await.unwrap()).await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&text).unwrap()["balance"],
        OPENING - 14
    );
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(14));
    runtime.shutdown().await.unwrap();
}

/// A request shed on accounting backpressure has one outcome, the refusal. It
/// is not also a context that was abandoned (INVARIANTS.md 20), on any route
/// that stages a request.
#[tokio::test(start_paused = true)]
async fn a_backpressure_shed_is_one_denial_and_abandons_nothing() {
    let (adapter, runtime, _, _) = fixture_with(NoGate, 8, CapacityClass::Assured).await;
    let seen = Seen::default();
    let silent = Router::new().route(
        "/silent",
        adapter.post(
            Op,
            ROUTE,
            || Ok(Validated::new((), 1)),
            |(), _| async { panic!("a shed request must not execute") },
        ),
    );
    let app = reporting_routes(&adapter, &seen).merge(silent);
    let held: Vec<_> = (0..8)
        .map(|_| adapter.runtime().recorder().try_reserve().unwrap())
        .collect();
    for path in ["/", "/fixed", "/silent"] {
        let response = app.clone().oneshot(to(path, "1")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    let prepared = adapter
        .prepare_json::<u64>(to("/", "1"), ROUTE, limits(100))
        .await;
    assert!(matches!(
        prepared,
        Err(Rejection::Denied(DenyReason::AccountingBackpressure))
    ));

    let counts = adapter.runtime().counters().snapshot();
    assert_eq!(counts.denied(), 4);
    assert_eq!(
        counts.denials[DenyReason::AccountingBackpressure.index()],
        4
    );
    assert_eq!(counts.contexts_abandoned, 0);
    assert_eq!(counts.admitted, 0);
    drop(held);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
}

/// What each callback of a reporting route was handed, in order.
type Seen = Arc<Mutex<Vec<(&'static str, Option<BudgetView>)>>>;

fn recording_renderer(
    seen: Seen,
) -> impl Fn(&Rejection, Option<tollgate_axum::ChargeMetadata>, Option<BudgetView>) -> BufferedResponse
+ Send
+ Sync
+ 'static {
    move |error, charge, budget| {
        let kind = match error {
            Rejection::Denied(DenyReason::UnknownPrincipal) => "unknown",
            Rejection::Denied(DenyReason::MissingPermission) => "permission",
            Rejection::Denied(DenyReason::AccountingBackpressure) => "backpressure",
            Rejection::Denied(DenyReason::RequestTooLarge { .. }) => "admission",
            Rejection::Denied(DenyReason::CapacityUnavailable) => "capacity",
            Rejection::Json(_) => "json",
            Rejection::InvalidInput(_) => "invalid",
            Rejection::UnexpectedBody => "unexpected-body",
            Rejection::RequestIdUnavailable => "request-id",
            Rejection::Response(_) => "response",
            other => panic!("unexpected rejection {other:?}"),
        };
        seen.lock().unwrap().push((kind, budget));
        render_rejection(error, charge)
    }
}

/// A JSON route and a bodyless one, both reporting; `items` of 17 is invalid.
fn reporting_routes<I: RequestIdSource, G: CapacityGate>(
    adapter: &Tollgate<BearerAuth<Verifier>, ManualClock, I, G>,
    seen: &Seen,
) -> Router {
    let (json, fixed) = (seen.clone(), seen.clone());
    Router::new()
        .route(
            "/",
            adapter.post_json_with_budget(
                Op,
                ROUTE,
                limits(100),
                |items: u64| {
                    if items == 17 {
                        return Err(InputError("unsupported batch"));
                    }
                    Ok(Validated::new((), items))
                },
                move |(), charge, budget| {
                    json.lock().unwrap().push(("json-execute", budget));
                    async move {
                        BufferedResponse::bytes(StatusCode::OK, charge.units_charged.to_string())
                    }
                },
                recording_renderer(seen.clone()),
            ),
        )
        .route(
            "/fixed",
            adapter.post_with_budget(
                Op,
                ROUTE,
                || Ok(Validated::new((), 3)),
                move |(), _, budget| {
                    fixed.lock().unwrap().push(("fixed-execute", budget));
                    async { BufferedResponse::bytes(StatusCode::OK, "fixed") }
                },
                recording_renderer(seen.clone()),
            ),
        )
        .route(
            "/fixed-invalid",
            adapter.post_with_budget(
                Op,
                ROUTE,
                || Err::<Validated<()>, _>(InputError("never valid")),
                |(), _, _| async { panic!("invalid input must not execute") },
                recording_renderer(seen.clone()),
            ),
        )
        .route(
            "/forbidden",
            adapter.post_with_budget(
                Op,
                PermissionBits::bit(5),
                || Ok(Validated::new((), 1)),
                |(), _, _| async { panic!("an unauthorized request must not execute") },
                recording_renderer(seen.clone()),
            ),
        )
}

fn to(path: &str, content: &'static str) -> Request {
    let mut request = request(Body::from(content));
    *request.uri_mut() = path.parse().unwrap();
    request
}

#[tokio::test(start_paused = true)]
async fn reporting_routes_hand_the_observation_to_whichever_callback_answers() {
    let (adapter, runtime, store, _, view) = scheduled(NoGate).await;
    let seen = Seen::default();
    let app = reporting_routes(&adapter, &seen);
    let mut unknown = to("/", "1");
    unknown
        .headers_mut()
        .insert("authorization", "Bearer nobody".parse().unwrap());
    // Refusals first: none of them spends, so every one sees the opening view.
    for (request, expected) in [
        (unknown, ("unknown", None)),
        (to("/forbidden", ""), ("permission", None)),
        (to("/", "{"), ("json", Some(view))),
        (to("/", "17"), ("invalid", Some(view))),
        (to("/", "101"), ("admission", Some(view))),
        (to("/fixed", "x"), ("unexpected-body", Some(view))),
        (to("/fixed-invalid", ""), ("invalid", Some(view))),
    ] {
        app.clone().oneshot(request).await.unwrap();
        assert_eq!(seen.lock().unwrap().pop(), Some(expected));
        assert!(seen.lock().unwrap().is_empty(), "one callback answered");
    }
    let held: Vec<_> = (0..8)
        .map(|_| adapter.runtime().recorder().try_reserve().unwrap())
        .collect();
    for path in ["/", "/fixed"] {
        app.clone().oneshot(to(path, "")).await.unwrap();
        assert_eq!(
            seen.lock().unwrap().pop(),
            Some(("backpressure", Some(view)))
        );
    }
    drop(held);

    // An admitted request reports the figure without its own charge.
    let response = app.clone().oneshot(to("/", "4")).await.unwrap();
    assert_eq!(body(response).await, (StatusCode::OK, "4".to_owned()));
    assert_eq!(
        seen.lock().unwrap().pop(),
        Some(("json-execute", Some(view)))
    );
    let settled = until_balance(&adapter, OPENING - 4).await;
    assert_eq!(settled.period_end, view.period_end);
    app.oneshot(to("/fixed", "")).await.unwrap();
    assert_eq!(
        seen.lock().unwrap().pop(),
        Some(("fixed-execute", Some(settled)))
    );
    runtime.shutdown().await.unwrap();
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(7));
}

#[tokio::test(start_paused = true)]
async fn refusals_after_input_carry_the_observation() {
    let gate = ExecutionCapacityGate::new(
        ExecutionCapacityMode::Uniform {
            total: NonZeroU32::new(1).unwrap(),
        },
        LocalSharding::SINGLE,
    )
    .unwrap()
    .unwrap();
    let (adapter, runtime, _, clock, view) = scheduled(gate).await;

    // The only unit of capacity is held by a running request.
    let started = Arc::new(tokio::sync::Notify::new());
    let witness = started.clone();
    let blocking = Router::new().route(
        "/",
        adapter.post(
            Op,
            ROUTE,
            || Ok(Validated::new((), 1)),
            move |(), _| {
                let witness = witness.clone();
                async move {
                    witness.notify_one();
                    std::future::pending::<Result<BufferedResponse, ResponseError>>().await
                }
            },
        ),
    );
    let running = tokio::spawn(blocking.oneshot(request(Body::empty())));
    started.notified().await;
    let seen = Seen::default();
    reporting_routes(&adapter, &seen)
        .oneshot(to("/fixed", ""))
        .await
        .unwrap();
    assert_eq!(seen.lock().unwrap().pop(), Some(("capacity", Some(view))));
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());

    let without_ids = Tollgate::new(AdapterConfig {
        runtime: adapter.runtime().clone(),
        authenticator: BearerAuth::new(Arc::new(Verifier)),
        clock,
        request_ids: || Err(RequestIdUnavailable),
        capacity: NoGate,
    });
    // The aborted request's charge settles on the runtime's own schedule.
    let view = until_balance(&adapter, OPENING - 1).await;
    reporting_routes(&without_ids, &seen)
        .oneshot(to("/fixed", ""))
        .await
        .unwrap();
    assert_eq!(seen.lock().unwrap().pop(), Some(("request-id", Some(view))));
    runtime.shutdown().await.unwrap();
}

/// One request is one observation: a failure after execution reports what
/// execution was given, even when the account has moved meanwhile.
#[tokio::test(start_paused = true)]
async fn a_failure_after_commit_reports_the_observation_execution_received() {
    let (adapter, runtime, store, _, view) = scheduled(NoGate).await;
    let seen = Seen::default();
    let (witness, live, ledger) = (seen.clone(), adapter.clone(), store.clone());
    let app = Router::new().route(
        "/",
        adapter.post_with_budget(
            Op,
            ROUTE,
            || Ok(Validated::new((), 3)),
            move |(), _, budget| {
                witness.lock().unwrap().push(("fixed-execute", budget));
                let (live, ledger) = (live.clone(), ledger.clone());
                async move {
                    AdminStore::deposit(&*ledger, ACCOUNT, CostUnits(50))
                        .await
                        .unwrap();
                    until_balance(&live, OPENING + 50).await;
                    Err(ResponseError::Serialization)
                }
            },
            recording_renderer(seen.clone()),
        ),
    );
    app.oneshot(request(Body::empty())).await.unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        vec![("fixed-execute", Some(view)), ("response", Some(view))]
    );
    runtime.shutdown().await.unwrap();
}
