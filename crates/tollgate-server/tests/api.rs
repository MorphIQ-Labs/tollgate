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

use tollgate_core::{AccountId, AccountStatus, CostUnits, Principal};
use tollgate_store::wire::API_PREFIX;
use tollgate_store::{AccountConfig, GrantPolicy, ManualClock, MemoryStore};

use tollgate_server::{ServerState, router};

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

fn id(value: u128) -> String {
    format!("{value:032x}")
}

fn api(path: &str) -> String {
    format!("{API_PREFIX}{path}")
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
        status: AccountStatus::Active,
    });

    let (status, grant) = call(
        &router,
        "POST",
        &api("/leases/acquire"),
        Some(json!({"account_id": id(1), "requested": 600, "ttl_seconds": 60})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Default grant policy halves near the edge: 1000/2 = 500.
    assert_eq!(grant["units"], 500);
    assert_eq!(grant["fencing_token"], 1);

    let (status, _) = call(
        &router,
        "POST",
        &api("/leases/release"),
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
        &api("/leases/acquire"),
        Some(json!({"account_id": id(9), "requested": 1, "ttl_seconds": 60})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(problem["code"], "unknown-account");

    // Invalid durations fail before any balance is debited.
    store.create_account(AccountConfig {
        account_id: AccountId(8),
        initial_balance: CostUnits(100),
        status: AccountStatus::Active,
    });
    let (status, problem) = call(
        &router,
        "POST",
        &api("/leases/acquire"),
        Some(json!({"account_id": id(8), "requested": 10, "ttl_seconds": 0})),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(problem["code"], "invalid-ttl");
    assert_eq!(store.balance(AccountId(8)), CostUnits(100));

    // Fencing violation.
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(100),
        status: AccountStatus::Active,
    });
    let (_, grant) = call(
        &router,
        "POST",
        &api("/leases/acquire"),
        Some(json!({"account_id": id(1), "requested": 10, "ttl_seconds": 60})),
    )
    .await;
    let (status, problem) = call(
        &router,
        "POST",
        &api("/leases/release"),
        Some(json!({"lease_id": grant["lease_id"], "fencing_token": 999, "unspent": 10})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(problem["code"], "fenced");

    // Unknown principal snapshot.
    let (status, problem) = call(
        &router,
        "GET",
        &api(&format!("/snapshots/{}", Principal(42))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(problem["code"], "unknown-principal");
}

#[tokio::test]
async fn identifier_failures_are_structured_and_never_unknown() {
    let (_store, router) = state();

    let (status, problem) = call(&router, "GET", &api("/snapshots/not-an-id"), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(problem["code"], "invalid-id");

    let (status, problem) = call(
        &router,
        "POST",
        &api("/leases/acquire"),
        Some(json!({"account_id": 1, "requested": 1, "ttl_seconds": 60})),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(problem["code"], "invalid-json");
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
        &api("/admin/accounts"),
        Some(json!({"account_id": id(1), "initial_balance": 500, "status": "Active"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = call(
        &router,
        "POST",
        &api(&format!("/admin/accounts/{}/deposit", AccountId(1))),
        Some(json!({"units": 250})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let status_path = api(&format!("/admin/accounts/{}/status", AccountId(1)));
    let (status, body) = call(
        &router,
        "POST",
        &status_path,
        Some(json!({"status": "Suspended"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["republished"], 0,
        "this account has no snapshots yet, and the response says so"
    );
    let (status, problem) = call(
        &router,
        "POST",
        &api("/leases/acquire"),
        Some(json!({"account_id": id(1), "requested": 1, "ttl_seconds": 60})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(problem["code"], "account-inactive");
    let (status, _) = call(
        &router,
        "POST",
        &status_path,
        Some(json!({"status": "Active"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Recreating an existing account is a surfaced conflict, never a silent
    // overwrite or no-op (review finding #7).
    let (status, problem) = call(
        &router,
        "POST",
        &api("/admin/accounts"),
        Some(json!({"account_id": id(1), "initial_balance": 999, "status": "Active"})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(problem["code"], "account-exists");

    let snapshot = json!({
        "account_id": id(1),
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
        &api(&format!("/admin/snapshots/{}", Principal(7))),
        Some(json!({"snapshot": snapshot})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, fetched) = call(
        &router,
        "GET",
        &api(&format!("/snapshots/{}", Principal(7))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["generation"], 3);
    assert_eq!(fetched["cost_table"]["fixed_request"], 50);

    let (status, _) = call(
        &router,
        "DELETE",
        &api(&format!("/admin/snapshots/{}", Principal(7))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, problem) = call(
        &router,
        "GET",
        &api(&format!("/snapshots/{}", Principal(7))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(problem["code"], "revoked-principal");
    assert_eq!(problem["generation"], 3);
    let _ = Principal(7);
}

#[tokio::test]
async fn admin_preserves_new_limit_fields_over_http() {
    let (store, router) = state();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1_000),
        status: AccountStatus::Active,
    });
    let snapshot = json!({
        "account_id": id(1),
        "key_id": null,
        "generation": 1,
        "status": "Active",
        "valid_until": "2100-01-01T00:00:00Z",
        "permissions": 1,
        "limits": {
            "max_items_per_request": 64,
            "rate_units_per_second": 1_000,
            "rate_burst_units": 2_000,
            "weighted_rate_enabled": false,
            "request_rate_per_second": 10,
            "request_burst": 20,
            "max_concurrent_requests": 4,
            "principal_max_concurrent_requests": 2
        },
        "cost_table": {
            "fixed_request": 1,
            "minimum_charge": 1,
            "weights": [1]
        }
    });
    let (status, _) = call(
        &router,
        "PUT",
        &api(&format!("/admin/snapshots/{}", Principal(8))),
        Some(json!({"snapshot": snapshot})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, fetched) = call(
        &router,
        "GET",
        &api(&format!("/snapshots/{}", Principal(8))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["limits"]["rate_units_per_second"], 1_000);
    assert_eq!(fetched["limits"]["rate_burst_units"], 2_000);
    assert_eq!(fetched["limits"]["weighted_rate_enabled"], false);
    assert_eq!(fetched["limits"]["request_rate_per_second"], 10);
    assert_eq!(fetched["limits"]["request_burst"], 20);
    assert_eq!(fetched["limits"]["max_concurrent_requests"], 4);
    assert_eq!(fetched["limits"]["principal_max_concurrent_requests"], 2);
}

/// A revision published over HTTP comes back over HTTP, in canonical form
/// (#94) — and a document that omits it is served as unstated rather than
/// refused.
///
/// The server is a *reader* in this rollout: it decodes into an
/// `AccountSnapshot` and reserializes. A field it did not know about would be
/// silently stripped in exactly this round trip, which is the failure mode the
/// deployment order (schema, then readers, then publication) exists to avoid
/// and the reason this is checked at the HTTP boundary rather than only in the
/// store.
#[tokio::test]
async fn admin_preserves_the_policy_revision_over_http() {
    let (store, router) = state();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1_000),
        status: AccountStatus::Active,
    });
    let revision = "5c".repeat(32);
    let base = json!({
        "account_id": id(1),
        "key_id": null,
        "generation": 1,
        "status": "Active",
        "valid_until": "2100-01-01T00:00:00Z",
        "permissions": 1,
        "limits": { "max_items_per_request": 64, "rate_units_per_second": 1_000, "rate_burst_units": 2_000 },
        "cost_table": { "fixed_request": 1, "minimum_charge": 1, "weights": [1] }
    });

    let mut stated = base.clone();
    stated["policy_revision"] = json!(revision);
    let (status, _) = call(
        &router,
        "PUT",
        &api(&format!("/admin/snapshots/{}", Principal(0x94))),
        Some(json!({ "snapshot": stated })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, fetched) = call(
        &router,
        "GET",
        &api(&format!("/snapshots/{}", Principal(0x94))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        fetched["policy_revision"], revision,
        "the server must not strip a field it merely carries"
    );

    // A publisher that predates the field: accepted, and served as unstated.
    let (status, _) = call(
        &router,
        "PUT",
        &api(&format!("/admin/snapshots/{}", Principal(0x95))),
        Some(json!({ "snapshot": base })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "a snapshot without a revision is not a malformed snapshot"
    );
    let (_, fetched) = call(
        &router,
        "GET",
        &api(&format!("/snapshots/{}", Principal(0x95))),
        None,
    )
    .await;
    assert_eq!(fetched["policy_revision"], "0".repeat(64));
}

/// A revision that is not canonical is refused at the boundary rather than
/// stored and later read back as something else. Two spellings of one revision
/// would silently look like two policies to the consumer comparing them.
#[tokio::test]
async fn admin_refuses_a_noncanonical_policy_revision() {
    let (store, router) = state();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1_000),
        status: AccountStatus::Active,
    });
    for bad in [
        "5C".repeat(32), // uppercase
        "5c".repeat(16), // identifier width, not revision width
        format!("0x{}", "5c".repeat(32)),
    ] {
        let snapshot = json!({
            "account_id": id(1),
            "key_id": null,
            "generation": 1,
            "status": "Active",
            "valid_until": "2100-01-01T00:00:00Z",
            "permissions": 1,
            "limits": { "max_items_per_request": 64, "rate_units_per_second": 1_000, "rate_burst_units": 2_000 },
            "cost_table": { "fixed_request": 1, "minimum_charge": 1, "weights": [1] },
            "policy_revision": bad,
        });
        let (status, _) = call(
            &router,
            "PUT",
            &api(&format!("/admin/snapshots/{}", Principal(0x97))),
            Some(json!({ "snapshot": snapshot })),
        )
        .await;
        assert_ne!(
            status,
            StatusCode::NO_CONTENT,
            "a non-canonical revision must not be accepted: {bad}"
        );
    }
}

#[tokio::test]
async fn admin_refuses_snapshot_whose_batch_quote_exceeds_burst() {
    let (_store, router) = state();
    let snapshot = json!({
        "account_id": id(1),
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
        &api(&format!("/admin/snapshots/{}", Principal(7))),
        Some(json!({"snapshot": snapshot})),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(problem["code"], "invalid-snapshot-limits");

    let (status, problem) = call(
        &router,
        "GET",
        &api(&format!("/snapshots/{}", Principal(7))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(problem["code"], "unknown-principal");
}

#[tokio::test]
async fn admin_refuses_weighted_rate_outside_governors_u32_domain() {
    let (_store, router) = state();
    let snapshot = json!({
        "account_id": id(1),
        "key_id": null,
        "generation": 1,
        "status": "Active",
        "valid_until": "2100-01-01T00:00:00Z",
        "permissions": 1,
        "limits": {
            "max_items_per_request": 64,
            "rate_units_per_second": u64::from(u32::MAX) + 1,
            "rate_burst_units": 1_000
        },
        "cost_table": {
            "fixed_request": 1,
            "minimum_charge": 1,
            "weights": [1]
        }
    });

    let (status, problem) = call(
        &router,
        "PUT",
        &api(&format!("/admin/snapshots/{}", Principal(7))),
        Some(json!({"snapshot": snapshot})),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(problem["code"], "invalid-snapshot-limits");
}

/// The admin status endpoint speaks the `AccountStatus` vocabulary, and the
/// wire break is loud in both directions (#51).
///
/// The last assertion is the one worth having: `{"active": false}` used to be
/// a valid suspension, and now that the same call also republishes every
/// snapshot of the account, silently reinterpreting it would be the
/// changed-the-meaning-of-an-existing-value failure the guidelines forbid. A
/// stale runbook must fail, not half-work.
#[tokio::test]
async fn account_status_endpoint_speaks_the_status_vocabulary() {
    let (_store, router) = state();
    let (status, _) = call(
        &router,
        "POST",
        &api("/admin/accounts"),
        Some(json!({"account_id": id(1), "initial_balance": 500, "status": "Active"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let status_path = api(&format!("/admin/accounts/{}/status", AccountId(1)));

    let (status, body) = call(
        &router,
        "POST",
        &status_path,
        Some(json!({"status": "Closed"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["republished"], 0);
    assert_eq!(body["unreadable"], 0);

    // Terminal, and it says so in the deny vocabulary rather than a 500.
    let (status, problem) = call(
        &router,
        "POST",
        &status_path,
        Some(json!({"status": "Active"})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(problem["code"], "account-closed");

    // An unknown account is still a 404, whichever status is asked for --
    // the contract #27 pinned, carried across the rename.
    let (status, problem) = call(
        &router,
        "POST",
        &api(&format!("/admin/accounts/{}/status", AccountId(9))),
        Some(json!({"status": "Suspended"})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(problem["code"], "unknown-account");

    // The old body no longer means anything, and fails loudly.
    let (status, problem) = call(
        &router,
        "POST",
        &status_path,
        Some(json!({"active": false})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "a pre-#51 body must be refused, never reinterpreted"
    );
    assert_eq!(problem["code"], "invalid-json");
}

/// Issue #61: an over-limit ingest body is refused as `batch-too-large`, not
/// as malformed JSON, and by a limit this crate declares rather than one axum
/// supplies.
///
/// The old spelling reported a 413 with code `invalid-json` and the title
/// "request body is not valid JSON for this endpoint", sending a client
/// looking for a syntax error in a payload it had serialised correctly. The
/// codes must differ because the client's response to them differs: one is
/// worth retrying and the other is refused identically forever.
#[tokio::test]
async fn an_oversized_ingest_body_is_refused_as_batch_too_large() {
    let (_store, router) = state();
    // Comfortably past MAX_INGEST_BODY_BYTES without building a real batch:
    // the limit is enforced on the body, before any of it is parsed.
    let filler = "x".repeat(tollgate_store::wire::MAX_INGEST_BODY_BYTES + 1);
    let request = Request::builder()
        .method("POST")
        .uri(api("/usage/ingest"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(format!("{{\"events\":\"{filler}\"}}")))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        problem["code"], "batch-too-large",
        "a 413 reported as invalid-json sends a client hunting for a syntax \
         error in a payload it serialised correctly; got {problem}"
    );
    assert!(
        problem["title"]
            .as_str()
            .unwrap()
            .contains(&tollgate_store::MAX_INGEST_BATCH.to_string()),
        "the refusal must name the limit a client should batch against; got {problem}"
    );
}
