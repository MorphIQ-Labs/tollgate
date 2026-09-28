//! In-process contract tests for the HTTP surface: wire shapes, status
//! codes, and the stable problem `code` strings the client transport relies
//! on.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use jiff::{SignedDuration, Timestamp};
use serde_json::{Value, json};
use tower::ServiceExt;

use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits, Principal};
use tollgate_store::wire::API_PREFIX;
use tollgate_store::{
    AccountConfig, AdminStore, GrantPolicy, LeaseAllocator, ManualClock, MemoryStore,
};

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
        security: common::security(),
        issuer: None,
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
    let token = if path.contains("/admin/") {
        common::OPERATOR
    } else {
        common::INSTANCE
    };
    let request = match body {
        Some(body) => Request::builder()
            .method(method)
            .uri(path)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
        None => Request::builder()
            .method(method)
            .uri(path)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    };
    common::send(router, request).await
}

#[tokio::test]
async fn lease_lifecycle_over_http() {
    let (store, router) = state();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1_000),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
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
        capacity_class: CapacityClass::Assured,
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
        capacity_class: CapacityClass::Assured,
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
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

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
    // overwrite or no-op (review finding GL-7).
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
        capacity_class: CapacityClass::Assured,
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
/// (GL-94) — and a document that omits it is served as unstated rather than
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
        capacity_class: CapacityClass::Assured,
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
        capacity_class: CapacityClass::Assured,
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
/// wire break is loud in both directions (GL-51).
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
    // the contract GL-27 pinned, carried across the rename.
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
        "a pre-GL-51 body must be refused, never reinterpreted"
    );
    assert_eq!(problem["code"], "invalid-json");
}

/// Issue GL-61: an over-limit ingest body is refused as `batch-too-large`, not
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
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", common::INSTANCE),
        )
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

/// The operator account read reports funding and billing as different things
/// (GL-121).
///
/// The distinction the issue asks the surface to preserve: a balance falls
/// when units go out on a lease that has not settled, and that is not spend.
/// A dashboard reading depletion alone would bill a customer for capacity it
/// still holds.
#[tokio::test]
async fn the_account_read_separates_outstanding_grants_from_settled_usage() {
    let (store, app) = state();
    let account = AccountId(1);
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: account,
            initial_balance: CostUnits(1_000),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();

    let (status, body) = call(
        &app,
        "GET",
        &api(&format!("/admin/accounts/{}", id(1))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["balance"], 1_000);
    assert_eq!(body["outstanding_lease_grants"], 0);
    assert_eq!(body["settled_usage"], 0);
    // The wire form is the variant name, as `SetStatusRequest` already sends it.
    assert_eq!(body["status"], "Active");
    assert_eq!(body["budget"], Value::Null, "no schedule is null, not zero");

    let grant = store
        .acquire(
            account,
            CostUnits(400),
            jiff::SignedDuration::from_secs(60),
            t(0),
        )
        .await
        .unwrap()
        .grant;

    let (_, held) = call(
        &app,
        "GET",
        &api(&format!("/admin/accounts/{}", id(1))),
        None,
    )
    .await;
    assert_eq!(
        held["outstanding_lease_grants"],
        grant.units.get(),
        "the units are out on lease"
    );
    assert_eq!(
        held["settled_usage"], 0,
        "and nothing has been billed for them"
    );
    assert!(
        held["balance"].as_u64().unwrap() < 1_000,
        "the balance fell, which is precisely why it is not the usage figure"
    );
}

/// An unknown account is 404, never a zeroed body: "does not exist" and
/// "exists with no funding" are different answers.
#[tokio::test]
async fn reading_an_unknown_account_is_not_an_empty_account() {
    let (_store, app) = state();
    let (status, body) = call(
        &app,
        "GET",
        &api(&format!("/admin/accounts/{}", id(77))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "unknown-account");
}

/// The read is operator-only. An instance credential authenticates for the
/// request path; it must not be able to read a customer's funding position.
#[tokio::test]
async fn an_instance_credential_cannot_read_an_account() {
    let (store, app) = state();
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: AccountId(1),
            initial_balance: CostUnits(10),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();

    let request = axum::http::Request::builder()
        .method("GET")
        .uri(api(&format!("/admin/accounts/{}", id(1))))
        .header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {}", common::INSTANCE),
        )
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, _) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "the instance role does not administer accounts"
    );
}

/// The issuer secret the conformance cases configure (GL-143). Test-only.
const ISSUER_SECRET: &str = "143a143a143a143a143a143a143a143a143a143a143a143a143a143a143a143a";

/// An issuer for tests, built the way the stock binary builds one: a security
/// manifest's `issuer` entry through `SecurityLoader::load`, `start` and
/// `issuer` (GL-143), not a registry constructed beside the configuration path.
async fn manifest_issuer() -> Option<Arc<dyn tollgate_auth::CredentialIssuer + Send + Sync>> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("security.json");
    std::fs::write(directory.path().join("issuer.secret"), ISSUER_SECRET).unwrap();
    std::fs::write(
        &path,
        json!({"issuer": {"secret_file": "issuer.secret"}}).to_string(),
    )
    .unwrap();
    let mut loader = tollgate_server::config::SecurityLoader::new(&path);
    let loaded = loader.load(t(0)).await.unwrap().unwrap();
    let _ = loader.start(loaded).unwrap();
    loader.issuer()
}

async fn issuing_state() -> (Arc<MemoryStore>, axum::Router) {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    let router = router(ServerState {
        security: common::security(),
        store: Arc::clone(&store),
        clock: Arc::new(ManualClock::new(t(0))),
        issuer: Some(
            manifest_issuer()
                .await
                .expect("the manifest names an issuer"),
        ),
    });
    (store, router)
}

/// A snapshot body for the key-bound route (GL-143). `key_id` is left unstated
/// unless given: the route binds it to the key in the path.
fn key_snapshot(account: u128, generation: u64, key: Option<u128>) -> Value {
    let mut snapshot = tollgate_core::AccountSnapshot::builder(
        AccountId(account),
        tollgate_core::Generation(generation),
        AccountStatus::Active,
        t(3_600),
        tollgate_core::PermissionBits(0),
        tollgate_core::ResolvedLimits::new(1),
        Arc::new(tollgate_core::CostTable::builder(CostUnits(1), CostUnits(1)).build()),
    )
    .build();
    snapshot.key_id = key.map(tollgate_core::KeyId);
    json!({ "snapshot": snapshot })
}

/// The principal an instance learns from its own key projection — the only
/// place a principal is served. Operators never need it.
async fn projected_principal(app: &axum::Router) -> String {
    let (status, page) = call(app, "GET", &api("/keys"), None).await;
    assert_eq!(status, StatusCode::OK);
    let keys = page["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 1, "one live credential in the projection");
    keys[0]["principal"].as_str().unwrap().to_owned()
}

async fn make_account(store: &MemoryStore, id: u128) {
    AdminStore::create_account(
        store,
        AccountConfig {
            account_id: AccountId(id),
            initial_balance: CostUnits(1_000),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();
}

/// A secret is disclosed once, and resending the request does not disclose it
/// again (GL-121).
///
/// The caller chooses the `key_id`, so a lost response is recoverable *as a
/// fact* — "your credential exists" — without the server reissuing or
/// repeating the secret. Nothing retained can reproduce it: what persists is
/// an HMAC.
#[tokio::test]
async fn a_credential_secret_is_disclosed_once_and_a_retry_is_a_conflict() {
    let (store, app) = issuing_state().await;
    make_account(&store, 1).await;

    let body = json!({"key_id": id(7), "max_active_keys": 3});
    let (status, first) = call(
        &app,
        "POST",
        &api(&format!("/admin/accounts/{}/keys", id(1))),
        Some(body.clone()),
    )
    .await;
    // 201, as `create_account` answers: a credential now exists that did not.
    assert_eq!(status, StatusCode::CREATED);
    let secret = first["secret"]
        .as_str()
        .expect("the secret is disclosed")
        .to_owned();
    // The encoding is the wire contract, so assert it rather than that the
    // field is non-empty: `!is_empty()` accepts any string at all, including a
    // constant, and a disclosed credential that is not the minted one is the
    // single failure this endpoint cannot have. 32 bytes, lowercase hex.
    assert_eq!(secret.len(), 64, "32 bytes as lowercase hex: {secret}");
    assert!(
        secret
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "lowercase hex only: {secret}"
    );

    let (again, repeat) = call(
        &app,
        "POST",
        &api(&format!("/admin/accounts/{}/keys", id(1))),
        Some(body),
    )
    .await;
    assert_eq!(
        again,
        StatusCode::CONFLICT,
        "the same key_id is a duplicate, not a second credential"
    );
    assert_eq!(repeat["code"], "credential-exists");
    assert!(
        repeat.get("secret").is_none(),
        "a conflict never carries a secret"
    );

    // And the listing never carries one either.
    let (_, listed) = call(
        &app,
        "GET",
        &api(&format!("/admin/accounts/{}/keys", id(1))),
        None,
    )
    .await;
    let entry = &listed["keys"][0];
    assert_eq!(entry["key_id"], id(7));
    assert!(entry.get("secret").is_none(), "no secret in a listing");
    assert!(entry.get("digest").is_none(), "no digest in a listing");
    assert!(
        entry.get("principal").is_none(),
        "no principal either: it is the digest's leading 128 bits"
    );
    assert_eq!(entry["live"], true);
}

/// The bound is enforced through HTTP, and refuses rather than exceeding.
#[tokio::test]
async fn issuance_refuses_past_the_active_key_bound() {
    let (store, app) = issuing_state().await;
    make_account(&store, 1).await;

    for key in 1..=2u128 {
        let (status, _) = call(
            &app,
            "POST",
            &api(&format!("/admin/accounts/{}/keys", id(1))),
            Some(json!({"key_id": id(key), "max_active_keys": 2})),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "credential {key} fits the bound"
        );
    }
    let (status, body) = call(
        &app,
        "POST",
        &api(&format!("/admin/accounts/{}/keys", id(1))),
        Some(json!({"key_id": id(3), "max_active_keys": 2})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "active-key-limit");
}

/// A credential belonging to another account cannot be revoked through this
/// account's path (GL-121).
///
/// The isolation an application backend administering one customer depends on:
/// a mistyped or guessed id must not retire someone else's credential.
#[tokio::test]
async fn revocation_is_bound_to_the_account_in_the_path() {
    let (store, app) = issuing_state().await;
    make_account(&store, 1).await;
    make_account(&store, 2).await;

    let (status, _) = call(
        &app,
        "POST",
        &api(&format!("/admin/accounts/{}/keys", id(2))),
        Some(json!({"key_id": id(99), "max_active_keys": 3})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Account 1 tries to revoke account 2's credential.
    let (status, body) = call(
        &app,
        "DELETE",
        &api(&format!("/admin/accounts/{}/keys/{}", id(1), id(99))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "unknown-credential");

    // And it is still live for its owner.
    let (_, listed) = call(
        &app,
        "GET",
        &api(&format!("/admin/accounts/{}/keys", id(2))),
        None,
    )
    .await;
    assert_eq!(listed["keys"][0]["live"], true, "untouched");

    // Its owner can revoke it, and a repeat reports that nothing was retired.
    let (status, first) = call(
        &app,
        "DELETE",
        &api(&format!("/admin/accounts/{}/keys/{}", id(2), id(99))),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["retired"], true);
    let (_, repeat) = call(
        &app,
        "DELETE",
        &api(&format!("/admin/accounts/{}/keys/{}", id(2), id(99))),
        None,
    )
    .await;
    assert_eq!(
        repeat["retired"], false,
        "already revoked is a fact to report, not an error"
    );
}

/// A full page offers a cursor; a short page ends the listing.
///
/// The cursor is decided by `summaries.len() == limit`, and the two directions
/// fail differently: a full page with no cursor silently truncates the
/// listing, and a short page carrying one sends the caller back for a page
/// that cannot exist. Both are checked, because an inverted comparison
/// produces exactly one of each and either alone would look like a quirk.
#[tokio::test]
async fn a_full_key_page_offers_a_cursor_and_a_short_one_does_not() {
    let (store, app) = issuing_state().await;
    make_account(&store, 1).await;
    for key in 1..=3u128 {
        let (status, _) = call(
            &app,
            "POST",
            &api(&format!("/admin/accounts/{}/keys", id(1))),
            Some(json!({"key_id": id(key), "max_active_keys": 10})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (_, full) = call(
        &app,
        "GET",
        &api(&format!("/admin/accounts/{}/keys?limit=2", id(1))),
        None,
    )
    .await;
    assert_eq!(full["keys"].as_array().expect("keys").len(), 2);
    assert_eq!(
        full["next_after"], full["keys"][1]["key_id"],
        "a full page hands back its last credential as the next cursor"
    );

    let (_, rest) = call(
        &app,
        "GET",
        &api(&format!(
            "/admin/accounts/{}/keys?limit=2&after={}",
            id(1),
            full["next_after"].as_str().expect("a cursor")
        )),
        None,
    )
    .await;
    assert_eq!(rest["keys"].as_array().expect("keys").len(), 1);
    assert!(
        rest["next_after"].is_null(),
        "a short page is the last page and ends the listing"
    );
}

/// A deployment with no issuer says so, rather than pretending.
#[tokio::test]
async fn a_server_without_an_issuer_reports_issuance_unsupported() {
    let (store, app) = state();
    make_account(&store, 1).await;
    let (status, body) = call(
        &app,
        "POST",
        &api(&format!("/admin/accounts/{}/keys", id(1))),
        Some(json!({"key_id": id(1), "max_active_keys": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["code"], "issuance-unsupported");
}

/// Setting a budget reports what it replaced.
#[tokio::test]
async fn setting_a_budget_over_http_reports_what_it_replaced() {
    let (store, app) = state();
    make_account(&store, 1).await;
    let path = api(&format!("/admin/accounts/{}/budget", id(1)));

    let schedule = json!({"allowance": 500, "period": "UtcCalendarMonth", "rollover": "None"});
    let (status, body) = call(&app, "PUT", &path, Some(json!({"budget": schedule}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["current"]["allowance"], 500);

    let (_, cleared) = call(&app, "PUT", &path, Some(json!({"budget": null}))).await;
    assert_eq!(
        cleared["current"],
        Value::Null,
        "clearing a schedule is a state, not an absence"
    );
}

/// The provisioning sequence `docs/ACCOUNT_ADMINISTRATION.md` publishes, run
/// end to end — and run twice, because every step it documents is described as
/// safe to repeat (GL-121).
///
/// A runbook nothing executes is a runbook that drifts. This is the executable
/// half of the conformance list at the end of that document.
#[tokio::test]
async fn the_documented_provisioning_sequence_is_repeatable() {
    let (_store, app) = issuing_state().await;
    let account = api(&format!("/admin/accounts/{}", id(1)));

    // 1. Create, suspended and unfunded.
    let create = json!({"account_id": id(1), "initial_balance": 0, "status": "Suspended"});
    let (first, _) = call(&app, "POST", &api("/admin/accounts"), Some(create.clone())).await;
    assert_eq!(first, StatusCode::CREATED);
    let (again, body) = call(&app, "POST", &api("/admin/accounts"), Some(create)).await;
    assert_eq!(
        again,
        StatusCode::CONFLICT,
        "repeating creation is a conflict"
    );
    assert_eq!(body["code"], "account-exists");

    // 2. Capacity class, twice.
    let class = api(&format!("/admin/accounts/{}/capacity-class", id(1)));
    for _ in 0..2 {
        let (status, _) = call(
            &app,
            "POST",
            &class,
            Some(json!({"capacity_class": "BestEffort"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    // 3. Budget, twice: the repeat must report itself as a no-op.
    let budget = api(&format!("/admin/accounts/{}/budget", id(1)));
    let schedule = json!({"allowance": 500, "period": "UtcCalendarMonth", "rollover": "None"});
    let (status, introduced) = call(&app, "PUT", &budget, Some(json!({"budget": schedule}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        introduced["previous"],
        Value::Null,
        "there was no schedule before"
    );
    assert_eq!(introduced["current"]["allowance"], 500);
    let (_, repeated) = call(&app, "PUT", &budget, Some(json!({"budget": schedule}))).await;
    assert_eq!(
        repeated["previous"], repeated["current"],
        "a repeat reports equal states, which is how the doc says a no-op looks"
    );

    // 4. Activate, twice.
    let status_path = api(&format!("/admin/accounts/{}/status", id(1)));
    for _ in 0..2 {
        let (code, _) = call(
            &app,
            "POST",
            &status_path,
            Some(json!({"status": "Active"})),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
    }

    // 5. Issue, then resend the identical request.
    let issue = api(&format!("/admin/accounts/{}/keys", id(1)));
    let request = json!({"key_id": id(42), "max_active_keys": 3});
    let (created, issued) = call(&app, "POST", &issue, Some(request.clone())).await;
    assert_eq!(created, StatusCode::CREATED);
    assert!(!issued["secret"].as_str().unwrap().is_empty());
    let (conflict, resent) = call(&app, "POST", &issue, Some(request)).await;
    assert_eq!(
        conflict,
        StatusCode::CONFLICT,
        "a resent issuance is a conflict"
    );
    assert_eq!(resent["code"], "credential-exists");
    assert!(resent.get("secret").is_none(), "and carries no secret");

    // 6. Bind the credential's policy by the handles the operator holds,
    //    twice: the repeat is a generation no-op, not an error.
    let binding = api(&format!(
        "/admin/accounts/{}/keys/{}/snapshot",
        id(1),
        id(42)
    ));
    let mut policy = key_snapshot(1, 1, None);
    policy["snapshot"]["capacity_class"] = json!("BestEffort");
    for _ in 0..2 {
        let (status, _) = call(&app, "PUT", &binding, Some(policy.clone())).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    // 7. The instance serves it under the credential's principal.
    let principal = projected_principal(&app).await;
    let (status, served) = call(&app, "GET", &api(&format!("/snapshots/{principal}")), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(served["key_id"], id(42));
    assert_eq!(served["generation"], 1);

    // The account the document describes: active, scheduled, one live key.
    let (_, view) = call(&app, "GET", &account, None).await;
    assert_eq!(view["status"], "Active");
    assert_eq!(view["capacity_class"], "BestEffort");
    assert_eq!(view["budget"]["allowance"], 500);
    let (_, keys) = call(&app, "GET", &issue, None).await;
    assert_eq!(keys["keys"].as_array().unwrap().len(), 1);
    assert_eq!(keys["keys"][0]["live"], true);

    // The funding equation the document invites a caller to check.
    let total = |k: &str| view[k].as_u64().unwrap();
    assert_eq!(
        total("deposited") + total("overage_recorded"),
        total("balance")
            + total("outstanding_lease_grants")
            + total("settled_usage")
            + total("settlement_loss")
            + total("expired_allowance"),
        "the funding equation holds exactly"
    );
}

#[path = "../../tollgate-store/tests/support/delegating.rs"]
mod delegating;

#[tokio::test]
async fn an_omitted_budget_is_rejected_without_clearing_the_schedule() {
    let (store, app) = state();
    make_account(&store, 1).await;
    let path = api(&format!("/admin/accounts/{}/budget", id(1)));
    let schedule = json!({"allowance": 500, "period": "UtcCalendarMonth", "rollover": "None"});
    assert_eq!(
        call(&app, "PUT", &path, Some(json!({"budget": schedule})))
            .await
            .0,
        StatusCode::OK
    );
    let (status, problem) = call(&app, "PUT", &path, Some(json!({}))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(problem["code"], "invalid-json");
    assert_eq!(
        store
            .account_view(AccountId(1))
            .await
            .unwrap()
            .unwrap()
            .schedule
            .unwrap()
            .allowance,
        CostUnits(500)
    );
    let (status, cleared) = call(&app, "PUT", &path, Some(json!({"budget": null}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cleared["previous"], schedule);
    assert!(cleared["current"].is_null());
    assert!(
        store
            .account_view(AccountId(1))
            .await
            .unwrap()
            .unwrap()
            .schedule
            .is_none()
    );
}

#[tokio::test]
async fn both_credential_listings_validate_queries_before_backend_reads() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tollgate_store::{KeyDirectory, KeySource};
    let reads = Arc::new(AtomicUsize::new(0));
    let directory_reads = Arc::clone(&reads);
    let projection_reads = Arc::clone(&reads);
    let store =
        delegating::DelegatingStore::wrapping(MemoryStore::new(GrantPolicy::default()).unwrap())
            .on_account_keys(move |inner, account, after, limit| {
                directory_reads.fetch_add(1, Ordering::SeqCst);
                async move { inner.account_keys(account, after, limit).await }
            })
            .on_active_keys_page(move |inner, now, after, limit| {
                projection_reads.fetch_add(1, Ordering::SeqCst);
                async move { inner.active_keys_page(now, after, limit).await }
            });
    let app = router(ServerState {
        security: common::security(),
        issuer: None,
        store: Arc::new(store),
        clock: Arc::new(ManualClock::new(t(0))),
    });
    let capture = common::EventCapture::default();
    capture
        .during(async {
            for (path, token) in [
                (
                    api(&format!("/admin/accounts/{}/keys", id(1))),
                    common::OPERATOR,
                ),
                (api("/keys"), common::INSTANCE),
            ] {
                let oversized = format!("limit={}", tollgate_store::MAX_KEY_PAGE_LIMIT + 1);
                for (query, status, code) in [
                    ("after=bad", StatusCode::BAD_REQUEST, "invalid-query"),
                    ("limit=wat", StatusCode::BAD_REQUEST, "invalid-query"),
                    ("unexpected=1", StatusCode::BAD_REQUEST, "invalid-query"),
                    ("limit=0", StatusCode::UNPROCESSABLE_ENTITY, "invalid-limit"),
                    (
                        oversized.as_str(),
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "invalid-limit",
                    ),
                ] {
                    let previous_reads = reads.load(Ordering::SeqCst);
                    let response = app
                        .clone()
                        .oneshot(
                            Request::builder()
                                .uri(format!("{path}?{query}"))
                                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(response.status(), status, "{query}");
                    assert_eq!(
                        response.headers()[header::CONTENT_TYPE],
                        "application/problem+json"
                    );
                    let bytes = response.into_body().collect().await.unwrap().to_bytes();
                    let problem: Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(problem["code"], code);
                    assert_eq!(
                        reads.load(Ordering::SeqCst),
                        previous_reads,
                        "invalid input never reaches a backend"
                    );
                }
                for limit in [1, tollgate_store::MAX_KEY_PAGE_LIMIT] {
                    assert_eq!(
                        call(&app, "GET", &format!("{path}?limit={limit}"), None)
                            .await
                            .0,
                        StatusCode::OK
                    );
                }
            }
        })
        .await;
    assert_eq!(
        reads.load(Ordering::SeqCst),
        4,
        "both valid boundaries reach both backends"
    );
    assert!(
        capture
            .events()
            .iter()
            .all(|event| event.target != "tollgate::diagnostics"),
        "client validation must not report a backend incident"
    );
}

#[tokio::test]
async fn credential_audits_name_the_key_and_actual_lifecycle_transition() {
    use tollgate_core::KeyId;
    use tollgate_store::{AdminState, KeyDirectory};
    let (store, app) = issuing_state().await;
    make_account(&store, 1).await;
    let path = api(&format!("/admin/accounts/{}/keys", id(1)));
    let capture = common::EventCapture::default();
    let mut secret = String::new();
    let mut digest = String::new();
    capture
        .during(async {
            let (status, issued) = call(
                &app,
                "POST",
                &path,
                Some(json!({"key_id": id(7), "max_active_keys": 1})),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED);
            secret = issued["secret"].as_str().unwrap().to_owned();
            digest = format!("{:?}", store.active_keys(t(0)).await.unwrap()[0].digest);
            assert_eq!(
                call(
                    &app,
                    "POST",
                    &path,
                    Some(json!({"key_id": id(7), "max_active_keys": 1}))
                )
                .await
                .0,
                StatusCode::CONFLICT
            );
            for retired in [true, false] {
                let (status, result) =
                    call(&app, "DELETE", &format!("{path}/{}", id(7)), None).await;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(result["retired"], retired);
            }
        })
        .await;
    let events = capture.events();
    let confirmed: Vec<_> = events
        .iter()
        .filter(|event| {
            event.target == "tollgate::audit"
                && event
                    .fields
                    .get("outcome")
                    .is_some_and(|outcome| outcome == "confirmed")
        })
        .collect();
    let unrevoked = AdminState::Credential {
        account_id: AccountId(1),
        key_id: KeyId(7),
        revoked: false,
    };
    let revoked = AdminState::Credential {
        account_id: AccountId(1),
        key_id: KeyId(7),
        revoked: true,
    };
    assert_eq!(confirmed.len(), 3);
    for (event, (action, before, after)) in confirmed.iter().zip([
        ("issue_key", AdminState::Absent, unrevoked),
        ("revoke_key", unrevoked, revoked),
        ("revoke_key", revoked, revoked),
    ]) {
        assert_eq!(event.fields["actor"], "test-operator");
        assert_eq!(event.fields["action"], action);
        assert_eq!(
            event.fields["resource"],
            format!("{}/keys/{}", id(1), id(7))
        );
        assert_eq!(event.fields["before"], format!("{before:?}"));
        assert_eq!(event.fields["after"], format!("{after:?}"));
    }
    let failed: Vec<_> = events
        .iter()
        .filter(|event| {
            event
                .fields
                .get("outcome")
                .is_some_and(|outcome| outcome == "failed")
        })
        .collect();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].fields["resource"],
        confirmed[0].fields["resource"]
    );
    assert!(!failed[0].fields.contains_key("before") && !failed[0].fields.contains_key("after"));
    let rendered = format!("{events:?}");
    assert!(!rendered.contains(&secret));
    assert!(!rendered.contains(&digest));
    assert!(!rendered.contains("principal"));
    assert!(!rendered.contains(ISSUER_SECRET));
}

/// Binding by key is bound to the account in the path, as revocation is: a
/// foreign or unknown `key_id` answers 404 and publishes nothing (GL-143).
#[tokio::test]
async fn a_key_snapshot_is_bound_to_the_account_in_the_path() {
    let (store, app) = issuing_state().await;
    make_account(&store, 1).await;
    make_account(&store, 2).await;
    let (status, _) = call(
        &app,
        "POST",
        &api(&format!("/admin/accounts/{}/keys", id(1))),
        Some(json!({"key_id": id(7), "max_active_keys": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    for (account, key) in [(2, 7), (1, 8)] {
        let path = api(&format!(
            "/admin/accounts/{}/keys/{}/snapshot",
            id(account),
            id(key)
        ));
        let (status, problem) =
            call(&app, "PUT", &path, Some(key_snapshot(account, 1, None))).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "account {account}, key {key}"
        );
        assert_eq!(problem["code"], "unknown-credential");
        let (status, problem) = call(&app, "DELETE", &path, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(problem["code"], "unknown-credential");
    }
    let (_, catalogue) = call(&app, "GET", &api("/snapshots"), None).await;
    assert_eq!(
        catalogue["principals"],
        json!([]),
        "nothing was published: {catalogue}"
    );
}

/// A snapshot naming a different credential, or another account, is refused
/// rather than rewritten; one naming the path's key is accepted (GL-143).
#[tokio::test]
async fn a_key_snapshot_naming_another_key_is_refused() {
    let (store, app) = issuing_state().await;
    make_account(&store, 1).await;
    make_account(&store, 2).await;
    call(
        &app,
        "POST",
        &api(&format!("/admin/accounts/{}/keys", id(1))),
        Some(json!({"key_id": id(7), "max_active_keys": 1})),
    )
    .await;
    let path = api(&format!(
        "/admin/accounts/{}/keys/{}/snapshot",
        id(1),
        id(7)
    ));
    for body in [key_snapshot(1, 1, Some(8)), key_snapshot(2, 1, None)] {
        let (status, problem) = call(&app, "PUT", &path, Some(body)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(problem["code"], "invalid-credential-binding");
    }
    let (status, _) = call(&app, "PUT", &path, Some(key_snapshot(1, 1, Some(7)))).await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "stating the path's own key is fine"
    );
}

/// Revocation is terminal: a retired credential is never granted a snapshot
/// again, and withdrawing its snapshot is the second half of revoking it
/// (GL-143, INVARIANTS.md GL-27).
#[tokio::test]
async fn a_retired_key_cannot_be_granted_a_snapshot_but_can_be_withdrawn() {
    let (store, app) = issuing_state().await;
    make_account(&store, 1).await;
    let keys = api(&format!("/admin/accounts/{}/keys", id(1)));
    call(
        &app,
        "POST",
        &keys,
        Some(json!({"key_id": id(7), "max_active_keys": 1})),
    )
    .await;
    let principal = projected_principal(&app).await;
    let binding = format!("{keys}/{}/snapshot", id(7));
    let (status, _) = call(&app, "PUT", &binding, Some(key_snapshot(1, 1, None))).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = call(&app, "DELETE", &format!("{keys}/{}", id(7)), None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, problem) = call(&app, "PUT", &binding, Some(key_snapshot(1, 2, None))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(problem["code"], "credential-retired");
    let (status, served) = call(&app, "GET", &api(&format!("/snapshots/{principal}")), None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "revocation alone leaves the snapshot in place"
    );
    assert_eq!(served["generation"], 1);

    let (status, _) = call(&app, "DELETE", &binding, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, problem) = call(&app, "GET", &api(&format!("/snapshots/{principal}")), None).await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(problem["code"], "revoked-principal");
}

/// Key-bound snapshot audits name `{account}/keys/{key}/snapshot` and the
/// actual transition; no principal reaches a response or the audit target.
#[tokio::test]
async fn key_snapshot_audits_name_the_key_and_never_the_principal() {
    use tollgate_store::AdminState;
    let (store, app) = issuing_state().await;
    make_account(&store, 1).await;
    call(
        &app,
        "POST",
        &api(&format!("/admin/accounts/{}/keys", id(1))),
        Some(json!({"key_id": id(7), "max_active_keys": 1})),
    )
    .await;
    let principal = projected_principal(&app).await;
    let binding = api(&format!(
        "/admin/accounts/{}/keys/{}/snapshot",
        id(1),
        id(7)
    ));
    let capture = common::EventCapture::default();
    let mut bodies = Vec::new();
    capture
        .during(async {
            for (method, body) in [
                ("PUT", Some(key_snapshot(1, 1, None))),
                ("PUT", Some(key_snapshot(1, 1, None))),
                ("DELETE", None),
            ] {
                let (status, response) = call(&app, method, &binding, body).await;
                assert_eq!(status, StatusCode::NO_CONTENT);
                bodies.push(response);
            }
        })
        .await;
    let audit: Vec<_> = capture
        .events()
        .into_iter()
        .filter(|event| event.target == "tollgate::audit")
        .collect();
    let confirmed: Vec<_> = audit
        .iter()
        .filter(|event| {
            event
                .fields
                .get("outcome")
                .is_some_and(|o| o == "confirmed")
        })
        .collect();
    let live = AdminState::Snapshot {
        generation: tollgate_core::Generation(1),
        revoked: false,
    };
    let tombstone = AdminState::Snapshot {
        generation: tollgate_core::Generation(1),
        revoked: true,
    };
    assert_eq!(confirmed.len(), 3);
    for (event, (action, before, after)) in confirmed.iter().zip([
        ("publish_key_snapshot", AdminState::Absent, live),
        ("publish_key_snapshot", live, live),
        ("remove_key_snapshot", live, tombstone),
    ]) {
        assert_eq!(event.fields["action"], action);
        assert_eq!(
            event.fields["resource"],
            format!("{}/keys/{}/snapshot", id(1), id(7))
        );
        assert_eq!(event.fields["before"], format!("{before:?}"));
        assert_eq!(event.fields["after"], format!("{after:?}"));
    }
    let rendered = format!("{audit:?}{bodies:?}");
    assert!(!rendered.contains(&principal), "{rendered}");
    assert!(!rendered.contains(ISSUER_SECRET));
}

/// An embedder's issuer must mint the presented form (GL-143). One that hands
/// out bytes no header can carry is refused before its record is stored, so
/// no credential exists that its owner could never present.
#[tokio::test]
async fn an_issuer_minting_an_unpresentable_secret_is_refused_before_storage() {
    use tollgate_store::KeyDirectory;
    struct Minting(&'static [u8]);
    impl tollgate_auth::CredentialIssuer for Minting {
        fn mint(
            &self,
            key_id: tollgate_core::KeyId,
        ) -> Result<tollgate_auth::MintedKey, tollgate_auth::EntropyUnavailable> {
            Ok(tollgate_auth::MintedKey {
                key_id,
                principal: Principal(7),
                digest: [7; 32],
                secret: self.0.to_vec().into(),
            })
        }
    }
    // Not text; empty; text a header cannot carry verbatim. Then one that is
    // presentable, so the refusals are about the secret and nothing else.
    for (secret, presentable) in [
        (&[0x00, 0xff, 0x10][..], false),
        (b"", false),
        (b"two words", false),
        (b"presentable-fixture-credential", true),
    ] {
        let store = MemoryStore::new(GrantPolicy::default()).unwrap();
        let app = router(ServerState {
            security: common::security(),
            store: Arc::clone(&store),
            clock: Arc::new(ManualClock::new(t(0))),
            issuer: Some(Arc::new(Minting(secret))),
        });
        make_account(&store, 1).await;
        let (status, body) = call(
            &app,
            "POST",
            &api(&format!("/admin/accounts/{}/keys", id(1))),
            Some(json!({"key_id": id(7), "max_active_keys": 1})),
        )
        .await;
        let stored = store.active_keys(t(0)).await.unwrap();
        if presentable {
            assert_eq!(status, StatusCode::CREATED);
            assert_eq!(body["secret"], "presentable-fixture-credential");
            assert_eq!(stored.len(), 1);
        } else {
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{secret:?}");
            assert_eq!(body["code"], "issuer-misconfigured");
            assert!(body.get("secret").is_none());
            assert!(stored.is_empty(), "nothing was stored for {secret:?}");
        }
    }
}

#[tokio::test]
async fn deposit_overflow_is_a_permanent_client_error() {
    let (store, router) = state();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(u64::MAX),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    // Both the top-up overflow and, after a grant, the lifetime-total
    // overflow must retain the same permanent refusal over HTTP.
    for acquire_first in [false, true] {
        if acquire_first {
            store
                .acquire(
                    AccountId(1),
                    CostUnits(10),
                    SignedDuration::from_secs(60),
                    t(0),
                )
                .await
                .unwrap();
        }
        let before = store.conservation(AccountId(1)).unwrap();
        let (status, problem) = call(
            &router,
            "POST",
            &api(&format!("/admin/accounts/{}/deposit", AccountId(1))),
            Some(json!({"units": 1})),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(problem["status"], 422);
        assert_eq!(problem["code"], "balance-overflow");
        assert_eq!(store.conservation(AccountId(1)).unwrap(), before);
    }
}
