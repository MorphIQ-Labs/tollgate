//! In-process contract tests for the HTTP surface: wire shapes, status
//! codes, and the stable problem `code` strings the client transport relies
//! on.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use jiff::Timestamp;
use serde_json::{Value, json};
use tower::ServiceExt;

use tollgate_core::{AccountId, CostUnits, Principal};
use tollgate_store::{AccountConfig, GrantPolicy, ManualClock, MemoryStore};

use tollgate_server::{ServerState, router};

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

fn state() -> (Arc<MemoryStore>, axum::Router) {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    let router = router(ServerState {
        store: Arc::clone(&store),
        clock: Arc::new(ManualClock::new(t(0))),
    });
    (store, router)
}

async fn call(
    router: &axum::Router,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let request = match body {
        Some(body) => Request::builder()
            .method(method)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
        None => Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap(),
    };
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

#[tokio::test]
async fn lease_lifecycle_over_http() {
    let (store, router) = state();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1_000),
        active: true,
    });

    let (status, grant) = call(
        &router,
        "POST",
        "/v1/leases/acquire",
        Some(json!({"account_id": 1, "requested": 600, "ttl_seconds": 60})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Default grant policy halves near the edge: 1000/2 = 500.
    assert_eq!(grant["units"], 500);
    assert_eq!(grant["fencing_token"], 1);

    let (status, _) = call(
        &router,
        "POST",
        "/v1/leases/release",
        Some(json!({
            "lease_id": grant["lease_id"],
            "fencing_token": grant["fencing_token"],
            "unspent": 500
        })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(store.balance(AccountId(1)), CostUnits(1_000));
}

#[tokio::test]
async fn problem_codes_are_stable() {
    let (store, router) = state();

    // Unknown account.
    let (status, problem) = call(
        &router,
        "POST",
        "/v1/leases/acquire",
        Some(json!({"account_id": 9, "requested": 1, "ttl_seconds": 60})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(problem["code"], "unknown-account");

    // Invalid durations fail before any balance is debited.
    store.create_account(AccountConfig {
        account_id: AccountId(8),
        initial_balance: CostUnits(100),
        active: true,
    });
    let (status, problem) = call(
        &router,
        "POST",
        "/v1/leases/acquire",
        Some(json!({"account_id": 8, "requested": 10, "ttl_seconds": 0})),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(problem["code"], "invalid-ttl");
    assert_eq!(store.balance(AccountId(8)), CostUnits(100));

    // Fencing violation.
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(100),
        active: true,
    });
    let (_, grant) = call(
        &router,
        "POST",
        "/v1/leases/acquire",
        Some(json!({"account_id": 1, "requested": 10, "ttl_seconds": 60})),
    )
    .await;
    let (status, problem) = call(
        &router,
        "POST",
        "/v1/leases/release",
        Some(json!({"lease_id": grant["lease_id"], "fencing_token": 999, "unspent": 10})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(problem["code"], "fenced");

    // Unknown principal snapshot.
    let (status, problem) = call(&router, "GET", "/v1/snapshots/42", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(problem["code"], "unknown-principal");
}

#[tokio::test]
async fn admin_snapshot_roundtrip_and_probes() {
    let (_store, router) = state();

    let (status, _) = call(&router, "GET", "/livez", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&router, "GET", "/readyz", None).await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = call(
        &router,
        "POST",
        "/v1/admin/accounts",
        Some(json!({"account_id": 1, "initial_balance": 500, "active": true})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = call(
        &router,
        "POST",
        "/v1/admin/accounts/1/deposit",
        Some(json!({"units": 250})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Recreating an existing account is a surfaced conflict, never a silent
    // overwrite or no-op (review finding #7).
    let (status, problem) = call(
        &router,
        "POST",
        "/v1/admin/accounts",
        Some(json!({"account_id": 1, "initial_balance": 999, "active": true})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(problem["code"], "account-exists");

    let snapshot = json!({
        "account_id": 1,
        "key_id": null,
        "generation": 3,
        "status": "Active",
        "valid_until": "2100-01-01T00:00:00Z",
        "permissions": 1,
        "limits": {
            "max_items_per_request": 64,
            "rate_units_per_second": 1000,
            "rate_burst_units": 1000
        },
        "cost_table": {
            "fixed_request": 50,
            "minimum_charge": 50,
            "weights": [1]
        }
    });
    let (status, _) = call(
        &router,
        "PUT",
        "/v1/admin/snapshots/7",
        Some(json!({"snapshot": snapshot})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, fetched) = call(&router, "GET", "/v1/snapshots/7", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["generation"], 3);
    assert_eq!(fetched["cost_table"]["fixed_request"], 50);

    let (status, _) = call(&router, "DELETE", "/v1/admin/snapshots/7", None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, problem) = call(&router, "GET", "/v1/snapshots/7", None).await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(problem["code"], "revoked-principal");
    assert_eq!(problem["generation"], 3);
    let _ = Principal(7);
}

#[tokio::test]
async fn admin_refuses_snapshot_whose_batch_quote_exceeds_burst() {
    let (_store, router) = state();
    let snapshot = json!({
        "account_id": 1,
        "key_id": null,
        "generation": 1,
        "status": "Active",
        "valid_until": "2100-01-01T00:00:00Z",
        "permissions": 1,
        "limits": {
            "max_items_per_request": 64,
            "rate_units_per_second": 1000,
            "rate_burst_units": 113
        },
        "cost_table": {
            "fixed_request": 50,
            "minimum_charge": 50,
            "weights": [1]
        }
    });

    // The largest possible quote is 50 + 1 * 64 = 114, so publication is
    // refused instead of installing a plan that the request path cannot
    // admit at its documented batch cap.
    let (status, problem) = call(
        &router,
        "PUT",
        "/v1/admin/snapshots/7",
        Some(json!({"snapshot": snapshot})),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(problem["code"], "invalid-snapshot-limits");

    let (status, problem) = call(&router, "GET", "/v1/snapshots/7", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(problem["code"], "unknown-principal");
}
