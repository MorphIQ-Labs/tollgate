//! The `provisioner` role (#39): a self-service account service's credential,
//! scoped so that compromising it cannot fund, close, grant `Assured`, reach
//! an operator's accounts, or undo an operator's suspension.
mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tollgate_core::{
    AccountId, AccountStatus, CapacityClass, CostUnits, EnforcementMode, KeyId, Principal,
};
use tollgate_server::{ServerState, router};
use tollgate_store::{
    AccountView, AdminAuthority, AdminStore, GrantPolicy, MemoryStore, SystemClock,
};

const ISSUER_SECRET: &str = "fixture-provisioner-issuer-secret-39-only-0123456789";

fn app() -> (Arc<MemoryStore>, axum::Router) {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    let issuer = Arc::new(tollgate_auth::HmacRegistry::new(ISSUER_SECRET.as_bytes()));
    let router = router(ServerState {
        store: Arc::clone(&store),
        clock: Arc::new(SystemClock),
        security: common::security(),
        issuer: Some(issuer),
    });
    (store, router)
}

fn request(token: &str, method: &str, path: impl Into<String>, body: &Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path.into())
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(if body.is_null() {
            Body::empty()
        } else {
            Body::from(body.to_string())
        })
        .unwrap()
}

async fn call(
    app: &axum::Router,
    token: &str,
    method: &str,
    path: impl Into<String>,
    body: Value,
) -> (StatusCode, Value) {
    common::send(app, request(token, method, path, &body)).await
}

/// A key snapshot the store accepts for an active, best-effort account.
fn key_snapshot(account: AccountId, mode: EnforcementMode) -> Value {
    use tollgate_store::Clock as _;
    let snapshot = tollgate_core::AccountSnapshot::builder(
        account,
        tollgate_core::Generation(1),
        AccountStatus::Active,
        SystemClock
            .now()
            .checked_add(jiff::SignedDuration::from_hours(1))
            .unwrap(),
        tollgate_core::PermissionBits(0),
        tollgate_core::ResolvedLimits::new(1),
        Arc::new(tollgate_core::CostTable::builder(CostUnits(1), CostUnits(1)).build()),
    )
    .capacity_class(CapacityClass::BestEffort)
    .enforcement_mode(mode)
    .build();
    json!({ "snapshot": snapshot })
}

fn budget(allowance: u64) -> Value {
    json!({"budget": {"allowance": allowance, "period": "UtcCalendarMonth", "rollover": "None"}})
}

async fn view(store: &MemoryStore, account: AccountId) -> Option<AccountView> {
    AdminStore::account_view(store, account).await.unwrap()
}

/// Create and activate an account the way a signup service does, returning
/// the account and one issued key.
async fn provisioned(app: &axum::Router, account: AccountId) -> KeyId {
    let p = common::PROVISIONER;
    let base = format!("/v1/admin/accounts/{account}");
    let (status, _) = call(
        app,
        p,
        "POST",
        "/v1/admin/accounts",
        json!({"account_id": account, "initial_balance": 0, "status": "Suspended"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = call(
        app,
        p,
        "POST",
        format!("{base}/status"),
        json!({"status": "Active"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let key = KeyId(account.0 + 1_000);
    let (status, _) = call(
        app,
        p,
        "POST",
        format!("{base}/keys"),
        json!({"key_id": key, "max_active_keys": 4}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    key
}

fn audit(capture: &common::EventCapture) -> Vec<std::collections::BTreeMap<String, String>> {
    capture
        .events()
        .into_iter()
        .filter(|event| event.target == "tollgate::audit")
        .map(|event| event.fields)
        .collect()
}

#[tokio::test]
async fn a_provisioner_completes_every_self_service_call() {
    let (store, app) = app();
    let capture = common::EventCapture::default();
    let account = AccountId(7);
    let base = format!("/v1/admin/accounts/{account}");
    let p = common::PROVISIONER;
    capture
        .during(async {
            let (status, _) = call(
                &app,
                p,
                "POST",
                "/v1/admin/accounts",
                json!({"account_id": account, "initial_balance": 0, "status": "Suspended"}),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED);

            // The store fixed the shape, `BestEffort` included, and recorded
            // who made it.
            let (status, body) = call(&app, p, "GET", &base, Value::Null).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["status"], "Suspended");
            assert_eq!(body["capacity_class"], "BestEffort");
            assert_eq!(body["origin"], "Provisioner");
            assert_eq!(body["status_set_by"], "Provisioner");
            assert_eq!(body["deposited"], 0);

            let (status, _) = call(
                &app,
                p,
                "POST",
                format!("{base}/capacity-class"),
                json!({"capacity_class": "BestEffort"}),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            // The ceiling is inclusive.
            let (status, body) = call(
                &app,
                p,
                "PUT",
                format!("{base}/budget"),
                budget(common::PROVISIONER_MAX_BUDGET),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let (status, _) = call(
                &app,
                p,
                "PUT",
                format!("{base}/budget"),
                json!({"budget": null}),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            let (status, _) = call(
                &app,
                p,
                "POST",
                format!("{base}/status"),
                json!({"status": "Active"}),
            )
            .await;
            assert_eq!(status, StatusCode::OK);

            let key = KeyId(70);
            let (status, body) = call(
                &app,
                p,
                "POST",
                format!("{base}/keys"),
                json!({"key_id": key, "max_active_keys": 4}),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
            let (status, body) = call(&app, p, "GET", format!("{base}/keys"), Value::Null).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["keys"][0]["key_id"], json!(key));
            let (status, body) = call(
                &app,
                p,
                "PUT",
                format!("{base}/keys/{key}/snapshot"),
                key_snapshot(account, EnforcementMode::Strict),
            )
            .await;
            assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
            let (status, _) = call(
                &app,
                p,
                "DELETE",
                format!("{base}/keys/{key}/snapshot"),
                Value::Null,
            )
            .await;
            assert_eq!(status, StatusCode::NO_CONTENT);
            let (status, _) =
                call(&app, p, "DELETE", format!("{base}/keys/{key}"), Value::Null).await;
            assert_eq!(status, StatusCode::OK);
        })
        .await;

    let view = view(&store, account).await.unwrap();
    assert_eq!(view.status, AccountStatus::Active);
    assert_eq!(view.status_set_by, AdminAuthority::Provisioner);
    assert_eq!(view.capacity_class, CapacityClass::BestEffort);
    let events = audit(&capture);
    let confirmed: Vec<_> = events
        .iter()
        .filter(|fields| fields.get("outcome").map(String::as_str) == Some("confirmed"))
        .collect();
    assert_eq!(confirmed.len(), 9, "{events:#?}");
    for fields in confirmed {
        assert_eq!(fields["actor"], "test-provisioner");
        assert_eq!(fields["role"], "provisioner");
    }
    assert!(
        events
            .iter()
            .all(|fields| !format!("{fields:?}").contains(common::PROVISIONER)),
        "no credential in the audit trail"
    );
}

#[tokio::test]
async fn a_provisioner_is_refused_and_audited_before_the_store_moves() {
    let (store, app) = app();
    let account = AccountId(8);
    let key = provisioned(&app, account).await;
    let base = format!("/v1/admin/accounts/{account}");
    let fresh = AccountId(9);
    let principal = Principal(1);
    let over = budget(common::PROVISIONER_MAX_BUDGET + 1);
    let elastic = key_snapshot(
        account,
        EnforcementMode::Elastic {
            overage_cap: CostUnits(1),
        },
    );
    // (method, path, body, action the audit record names)
    let refusals: Vec<(&str, String, Value, &str)> = vec![
        (
            "POST",
            format!("{base}/deposit"),
            json!({"units": 25}),
            "POST /v1/admin/accounts/{account}/deposit",
        ),
        (
            "PUT",
            format!("/v1/admin/snapshots/{principal}"),
            key_snapshot(account, EnforcementMode::Strict),
            "PUT /v1/admin/snapshots/{principal}",
        ),
        (
            "DELETE",
            format!("/v1/admin/snapshots/{principal}"),
            Value::Null,
            "DELETE /v1/admin/snapshots/{principal}",
        ),
        (
            "POST",
            "/v1/admin/accounts".into(),
            json!({"account_id": fresh, "initial_balance": 5, "status": "Suspended"}),
            "create_account",
        ),
        (
            "POST",
            "/v1/admin/accounts".into(),
            json!({"account_id": fresh, "initial_balance": 0, "status": "Active"}),
            "create_account",
        ),
        (
            "POST",
            format!("{base}/status"),
            json!({"status": "Suspended"}),
            "set_account_status",
        ),
        (
            "POST",
            format!("{base}/status"),
            json!({"status": "Closed"}),
            "set_account_status",
        ),
        (
            "POST",
            format!("{base}/capacity-class"),
            json!({"capacity_class": "Assured"}),
            "set_capacity_class",
        ),
        ("PUT", format!("{base}/budget"), over, "set_budget_schedule"),
        (
            "PUT",
            format!("{base}/keys/{key}/snapshot"),
            elastic,
            "publish_key_snapshot",
        ),
    ];
    for (method, path, body, action) in refusals {
        let before = view(&store, account).await;
        let capture = common::EventCapture::default();
        let (status, problem) = capture
            .during(call(&app, common::PROVISIONER, method, &path, body))
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {problem}");
        assert_eq!(problem["code"], "scope-forbidden", "{method} {path}");
        assert_eq!(
            view(&store, account).await,
            before,
            "{method} {path} moved the account"
        );
        assert!(
            view(&store, fresh).await.is_none(),
            "{method} {path} created an account"
        );
        let events = audit(&capture);
        assert_eq!(
            events.len(),
            1,
            "{method} {path}: exactly one refusal record, no start: {events:#?}"
        );
        let fields = &events[0];
        assert_eq!(fields["outcome"], "refused");
        assert_eq!(fields["actor"], "test-provisioner");
        assert_eq!(fields["role"], "provisioner");
        assert_eq!(fields["action"], action, "{method} {path}");
        assert_eq!(fields["code"], "scope-forbidden");
    }
    // The key snapshot refused above was never stored: publishing a strict one
    // is a first publication, not a replacement.
    let (status, _) = call(
        &app,
        common::PROVISIONER,
        "PUT",
        format!("{base}/keys/{key}/snapshot"),
        key_snapshot(account, EnforcementMode::Strict),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn a_provisioner_cannot_reach_an_account_an_operator_created() {
    let (store, app) = app();
    let account = AccountId(10);
    let (status, _) = call(
        &app,
        common::OPERATOR,
        "POST",
        "/v1/admin/accounts",
        json!({"account_id": account, "initial_balance": 500, "status": "Active"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let key = KeyId(100);
    let base = format!("/v1/admin/accounts/{account}");
    let (status, _) = call(
        &app,
        common::OPERATOR,
        "POST",
        format!("{base}/keys"),
        json!({"key_id": key, "max_active_keys": 4}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let before = view(&store, account).await.unwrap();
    assert_eq!(before.origin, AdminAuthority::Operator);

    for (method, path, body) in [
        ("GET", base.clone(), Value::Null),
        ("PUT", format!("{base}/budget"), budget(1)),
        (
            "POST",
            format!("{base}/capacity-class"),
            json!({"capacity_class": "BestEffort"}),
        ),
        (
            "POST",
            format!("{base}/status"),
            json!({"status": "Active"}),
        ),
        (
            "POST",
            format!("{base}/keys"),
            json!({"key_id": KeyId(101), "max_active_keys": 4}),
        ),
        ("GET", format!("{base}/keys"), Value::Null),
        ("DELETE", format!("{base}/keys/{key}"), Value::Null),
        (
            "PUT",
            format!("{base}/keys/{key}/snapshot"),
            key_snapshot(account, EnforcementMode::Strict),
        ),
        ("DELETE", format!("{base}/keys/{key}/snapshot"), Value::Null),
    ] {
        let capture = common::EventCapture::default();
        let (status, problem) = capture
            .during(call(&app, common::PROVISIONER, method, &path, body))
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {problem}");
        assert_eq!(
            problem["code"], "account-not-provisioned",
            "{method} {path}"
        );
        assert_eq!(
            view(&store, account).await.unwrap(),
            before,
            "{method} {path}"
        );
        let events = audit(&capture);
        assert!(
            events.iter().any(|fields| {
                fields.get("role").map(String::as_str) == Some("provisioner")
                    && fields.get("actor").map(String::as_str) == Some("test-provisioner")
                    && fields.get("code").map(String::as_str) == Some("account-not-provisioned")
            }),
            "{method} {path}: {events:#?}"
        );
        assert!(
            events
                .iter()
                .all(|fields| fields.get("outcome").map(String::as_str) != Some("confirmed")),
            "{method} {path}"
        );
    }
    // The operator's key was never touched.
    let (status, body) = call(
        &app,
        common::OPERATOR,
        "GET",
        format!("{base}/keys"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["keys"].as_array().unwrap().len(), 1);
    assert_eq!(body["keys"][0]["live"], true);

    // An unknown account is 404 for a provisioner too, not a scope refusal.
    let (status, problem) = call(
        &app,
        common::PROVISIONER,
        "GET",
        format!("/v1/admin/accounts/{}/keys", AccountId(404)),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(problem["code"], "unknown-account");
}

#[tokio::test]
async fn an_operator_suspension_holds_against_provisioner_activation() {
    let (store, app) = app();
    let account = AccountId(11);
    provisioned(&app, account).await;
    let status_path = format!("/v1/admin/accounts/{account}/status");

    let (status, _) = call(
        &app,
        common::OPERATOR,
        "POST",
        &status_path,
        json!({"status": "Suspended"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let held = view(&store, account).await.unwrap();
    assert_eq!(held.status_set_by, AdminAuthority::Operator);

    // Retrying signup cannot lift an abuse suspension.
    let capture = common::EventCapture::default();
    let (status, problem) = capture
        .during(call(
            &app,
            common::PROVISIONER,
            "POST",
            &status_path,
            json!({"status": "Active"}),
        ))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(problem["code"], "operator-hold");
    assert_eq!(view(&store, account).await.unwrap(), held);
    assert!(
        audit(&capture)
            .iter()
            .any(|fields| fields["outcome"] == "failed"
                && fields["role"] == "provisioner"
                && fields["code"] == "operator-hold")
    );

    // An operator lifts it; a provisioner's repeat is then a no-op that keeps
    // the operator as the status author.
    let (status, _) = call(
        &app,
        common::OPERATOR,
        "POST",
        &status_path,
        json!({"status": "Active"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &app,
        common::PROVISIONER,
        "POST",
        &status_path,
        json!({"status": "Active"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let lifted = view(&store, account).await.unwrap();
    assert_eq!(lifted.status, AccountStatus::Active);
    assert_eq!(lifted.status_set_by, AdminAuthority::Operator);

    // Closed stays terminal whoever asks.
    let (status, _) = call(
        &app,
        common::OPERATOR,
        "POST",
        &status_path,
        json!({"status": "Closed"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, problem) = call(
        &app,
        common::PROVISIONER,
        "POST",
        &status_path,
        json!({"status": "Active"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(problem["code"], "account-closed");
}

#[tokio::test]
async fn a_refused_role_is_audited_with_its_actor_role_and_action() {
    let (_store, app) = app();
    for (token, method, path, actor, role, action) in [
        (
            common::INSTANCE,
            "POST",
            "/v1/admin/accounts",
            "test-instance",
            "instance",
            "POST /v1/admin/accounts",
        ),
        (
            common::OPERATOR,
            "POST",
            "/v1/leases/acquire",
            "test-operator",
            "operator",
            "POST /v1/leases/acquire",
        ),
        (
            common::PROVISIONER,
            "GET",
            "/v1/keys",
            "test-provisioner",
            "provisioner",
            "GET /v1/keys",
        ),
    ] {
        let capture = common::EventCapture::default();
        let (status, problem) = capture
            .during(call(&app, token, method, path, json!({})))
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}");
        assert_eq!(problem["code"], "scope-forbidden");
        let events = audit(&capture);
        assert_eq!(events.len(), 1, "{events:#?}");
        let fields = &events[0];
        assert_eq!(fields["outcome"], "refused");
        assert_eq!(fields["actor"], actor);
        assert_eq!(fields["role"], role);
        assert_eq!(fields["action"], action);
        assert_eq!(fields["resource"], path);
        assert!(
            !format!("{fields:?}").contains(token),
            "no credential logged"
        );
    }
}
