mod support;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::StatusCode,
};
use serde::Deserialize;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use support::*;
use tollgate_admission::{ExecutionCapacityGate, ExecutionCapacityMode, NoGate};
use tollgate_axum::{
    AdapterConfig, BearerAuth, BufferedResponse, ChargeMetadata, InputError, Rejection,
    RequestIdUnavailable, ResponseError, Tollgate, Validated, render_rejection,
};
use tollgate_core::{
    AccountId, CapacityClass, CostUnits, LocalSharding, PermissionBits, RequestId,
};
use tower::ServiceExt;

#[derive(Deserialize)]
struct Input {
    items: u64,
}
fn validate(input: Input) -> Result<Validated<u64>, InputError> {
    if input.items == 17 {
        return Err(InputError("unsupported batch"));
    }
    Ok(Validated::new(input.items, input.items))
}
async fn problem(response: axum::response::Response, status: StatusCode, code: &str, units: u64) {
    assert_eq!(response.status(), status);
    assert_eq!(
        response.headers()["content-type"],
        "application/problem+json"
    );
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["status"], status.as_u16());
    assert_eq!(value["code"], code);
    assert_eq!(value["units_charged"], units);
}

#[tokio::test(start_paused = true)]
async fn validation_and_admission_refuse_without_constructing_business_future() {
    let (adapter, runtime, store, _) = fixture().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let witness = calls.clone();
    let app = Router::new().route(
        "/",
        adapter.post_json(
            Op,
            PermissionBits::bit(0),
            limits(100),
            validate,
            move |_, _| {
                witness.fetch_add(1, Ordering::SeqCst);
                async { BufferedResponse::bytes(StatusCode::OK, "done") }
            },
        ),
    );
    for (body, status, code) in [
        ("{", StatusCode::BAD_REQUEST, "malformed-body"),
        (
            r#"{"items":17}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid-input",
        ),
        (
            r#"{"items":0}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "empty-workload",
        ),
        (
            r#"{"items":101}"#,
            StatusCode::PAYLOAD_TOO_LARGE,
            "batch-too-large",
        ),
    ] {
        problem(
            app.clone()
                .oneshot(request(Body::from(body)))
                .await
                .unwrap(),
            status,
            code,
            0,
        )
        .await;
        assert!(adapter.runtime().recorder().try_reserve().is_ok());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits::ZERO);
}

#[tokio::test(start_paused = true)]
async fn body_and_fixed_quantities_preserve_committed_metadata_and_charge_errors() {
    let (adapter, runtime, store, _) = fixture_with(NoGate, 8, CapacityClass::Assured).await;
    let seen = Arc::new(Mutex::new(Vec::<ChargeMetadata>::new()));
    let witness = seen.clone();
    let app = Router::new()
        .route(
            "/",
            adapter.post_json(
                Op,
                PermissionBits::bit(0),
                limits(100),
                validate,
                move |input, charge| {
                    witness.lock().unwrap().push(charge);
                    async move {
                        assert_eq!(charge.units_charged, CostUnits(input));
                        BufferedResponse::json(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            &serde_json::json!({"units": charge.units_charged.get()}),
                        )
                    }
                },
            ),
        )
        .route(
            "/fixed",
            adapter.post(
                Op,
                PermissionBits::bit(0),
                || Ok(Validated::new((), 5)),
                |(), charge| async move {
                    assert_eq!(charge.units_charged, CostUnits(5));
                    BufferedResponse::bytes(StatusCode::ACCEPTED, "fixed")
                },
            ),
        );
    let response = app
        .clone()
        .oneshot(request(Body::from(r#"{"items":3}"#)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        to_bytes(response.into_body(), 100).await.unwrap(),
        r#"{"units":3}"#
    );
    let mut req = request(Body::empty());
    *req.uri_mut() = "/fixed".parse().unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(to_bytes(response.into_body(), 100).await.unwrap(), "fixed");
    let mut req = request(Body::from("unexpected"));
    *req.uri_mut() = "/fixed".parse().unwrap();
    problem(
        app.oneshot(req).await.unwrap(),
        StatusCode::BAD_REQUEST,
        "unexpected-body",
        0,
    )
    .await;
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 2);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(8));
    let charge = seen.lock().unwrap()[0];
    let event = store.settled_event(charge.request_id).unwrap();
    assert_eq!(event.units, charge.units_charged);
    assert_eq!(event.policy_revision, charge.policy_revision);
    assert!(store.settled_event(RequestId(2)).is_some());
}

#[tokio::test(start_paused = true)]
async fn unavailable_request_ids_release_the_slot_without_starting_work() {
    let (base, runtime, store, clock) = fixture().await;
    let adapter = Tollgate::new(AdapterConfig {
        runtime: base.runtime().clone(),
        authenticator: BearerAuth::new(Arc::new(Verifier)),
        clock,
        request_ids: || Err(RequestIdUnavailable),
        capacity: NoGate,
    });
    let app = Router::new().route(
        "/",
        adapter.post(
            Op,
            PermissionBits::bit(0),
            || Ok(Validated::new((), 3)),
            |(), _| async { panic!("unavailable ID must not execute") },
        ),
    );
    problem(
        app.oneshot(request(Body::empty())).await.unwrap(),
        StatusCode::SERVICE_UNAVAILABLE,
        "request-id-unavailable",
        0,
    )
    .await;
    assert!(adapter.runtime().recorder().try_reserve().is_ok());
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits::ZERO);
}

#[tokio::test(start_paused = true)]
async fn factory_panic_future_panic_and_abort_each_record_one_charge() {
    for mode in 0..3 {
        let (adapter, runtime, store, _) = fixture().await;
        let started = Arc::new(tokio::sync::Notify::new());
        let witness = started.clone();
        let app = Router::new().route(
            "/",
            adapter.post(
                Op,
                PermissionBits::bit(0),
                || Ok(Validated::new((), 3)),
                move |(), _| {
                    assert_ne!(mode, 0, "factory panic witness");
                    let witness = witness.clone();
                    async move {
                        assert_ne!(mode, 1, "future panic witness");
                        witness.notify_one();
                        std::future::pending::<Result<BufferedResponse, ResponseError>>().await
                    }
                },
            ),
        );
        // Merely constructing an unpolled request is not billable.
        drop(app.clone().oneshot(request(Body::empty())));
        let task = tokio::spawn(app.oneshot(request(Body::empty())));
        if mode == 2 {
            started.notified().await;
            task.abort();
        }
        let error = task.await.unwrap_err();
        assert_eq!(error.is_panic(), mode != 2);
        assert_eq!(error.is_cancelled(), mode == 2);
        assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
        assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(3));
    }
}

#[tokio::test(start_paused = true)]
async fn reserved_capacity_uses_account_class_and_returns_slots_after_abort() {
    for (class, admitted) in [(CapacityClass::BestEffort, 1), (CapacityClass::Assured, 2)] {
        let gate = ExecutionCapacityGate::new(
            ExecutionCapacityMode::Reserved {
                total: NonZeroU32::new(2).unwrap(),
                assured_reserve: NonZeroU32::new(1).unwrap(),
            },
            LocalSharding::SINGLE,
        )
        .unwrap()
        .unwrap();
        let (adapter, runtime, store, _) = fixture_with(gate.clone(), 8, class).await;
        let started = Arc::new(tokio::sync::Notify::new());
        let witness = started.clone();
        let app = Router::new().route(
            "/",
            adapter.post_json(
                Op,
                PermissionBits::bit(0),
                limits(100),
                validate,
                move |_, _| {
                    let witness = witness.clone();
                    async move {
                        witness.notify_one();
                        std::future::pending::<Result<BufferedResponse, ResponseError>>().await
                    }
                },
            ),
        );
        let mut tasks = Vec::new();
        for _ in 0..admitted {
            tasks.push(tokio::spawn(
                app.clone().oneshot(request(Body::from(r#"{"items":3}"#))),
            ));
            started.notified().await;
        }
        let mut req = request(Body::from(r#"{"items":3}"#));
        req.headers_mut()
            .insert("x-capacity-class", "assured".parse().unwrap());
        problem(
            app.oneshot(req).await.unwrap(),
            StatusCode::SERVICE_UNAVAILABLE,
            "capacity-unavailable",
            0,
        )
        .await;
        for task in tasks {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        let occupancy = gate.occupancy();
        assert_eq!(occupancy.shared_available, 1);
        assert_eq!(occupancy.reserve_available, 1);
        assert_eq!(
            runtime.shutdown().await.unwrap().usage.unwrap().accepted,
            admitted
        );
        assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(3 * admitted));
    }
}

struct CannotSerialize;
impl serde::Serialize for CannotSerialize {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("private application detail"))
    }
}
#[tokio::test(start_paused = true)]
async fn custom_error_renderer_receives_charge_only_after_execution() {
    let (adapter, runtime, store, _) = fixture_with(NoGate, 8, CapacityClass::Assured).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let witness = seen.clone();
    let app = Router::new().route(
        "/",
        adapter.post_json_with_error_handler(
            Op,
            PermissionBits::bit(0),
            limits(100),
            validate,
            |_, _| async { BufferedResponse::json(StatusCode::OK, &CannotSerialize) },
            move |error, charge| {
                witness.lock().unwrap().push(charge);
                match error {
                    Rejection::InvalidInput(InputError("unsupported batch"))
                    | Rejection::Response(ResponseError::Serialization) => (),
                    _ => panic!("unexpected error"),
                }
                render_rejection(error, charge)
            },
        ),
    );
    problem(
        app.clone()
            .oneshot(request(Body::from(r#"{"items":17}"#)))
            .await
            .unwrap(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid-input",
        0,
    )
    .await;
    problem(
        app.oneshot(request(Body::from(r#"{"items":3}"#)))
            .await
            .unwrap(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "response-failed",
        3,
    )
    .await;
    let observed = seen.lock().unwrap().clone();
    assert_eq!(observed.len(), 2);
    assert!(observed[0].is_none());
    assert_eq!(observed[1].unwrap().units_charged, CostUnits(3));
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(3));
}
