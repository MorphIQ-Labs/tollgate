//! Contract tests for the example service: the embedding is where the
//! product's guarantees become user-visible HTTP behavior.

#![allow(
    clippy::disallowed_methods,
    reason = "the example service reads the clock at its HTTP edge, so a test of that surface \
              observes the same instants it does"
)]

use std::num::NonZeroU32;

use axum::body::Body;
use axum::extract::connect_info::MockConnectInfo;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use pricing_api::{
    AppRuntime, DEMO_ACCOUNT, DEMO_API_KEY, DEMO_POLICY_REVISION, PricingConnection, build_app,
    build_app_with_capacity, build_app_with_mode, demo_tenants,
};
use tollgate_admission::{CommitRefusal, ExecutionCapacityMode};
use tollgate_core::{CostUnits, DenyReason, EnforcementMode, LocalSharding, RequestId};
use tollgate_store::{AllocateError, SnapshotSource};

/// Every test router needs a connection to authenticate against, because the
/// price route takes `ConnectInfo<PricingConnection>` — that requirement is
/// deliberate (#2), and `price_route_requires_connection_context` is the test
/// that keeps it from being quietly optional.
async fn build_test_app(deposit: u64, admission_enabled: bool) -> (axum::Router, AppRuntime) {
    let (router, runtime) = build_app(deposit, admission_enabled).await;
    (
        router.layer(MockConnectInfo(PricingConnection::default())),
        runtime,
    )
}

/// The same, for the enforcement-mode tests #1 added.
async fn build_test_app_with_mode(
    deposit: u64,
    admission_enabled: bool,
    mode: EnforcementMode,
) -> (axum::Router, AppRuntime) {
    let (router, runtime) = build_app_with_mode(deposit, admission_enabled, mode).await;
    (
        router.layer(MockConnectInfo(PricingConnection::default())),
        runtime,
    )
}

fn price_body(contracts: usize) -> Value {
    let contract =
        json!({"spot": 100.0, "strike": 105.0, "rate": 0.05, "vol": 0.2, "tte_years": 0.25});
    json!({ "contracts": vec![contract; contracts] })
}

/// Send a request and decode its JSON reply.
///
/// The `oneshot` / status / collect / `from_slice` tail below was written out
/// five times in this file. Every helper here is now a wrapper around it.
async fn send(router: &axum::Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn price_request(auth: Option<&str>, body: Body) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/price")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(key) = auth {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    builder.body(body).unwrap()
}

async fn call(router: &axum::Router, auth: Option<&str>, body: Value) -> (StatusCode, Value) {
    send(router, price_request(auth, Body::from(body.to_string()))).await
}

/// The same request with an unparsed body, for the malformed-input cases.
async fn call_raw(router: &axum::Router, auth: Option<&str>, body: &str) -> (StatusCode, Value) {
    send(router, price_request(auth, Body::from(body.to_owned()))).await
}

async fn metrics(router: &axum::Router) -> Value {
    send(
        router,
        Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .1
}

async fn ready(router: &axum::Router) -> StatusCode {
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn wait_ready(router: &axum::Router) {
    for _ in 0..200 {
        if ready(router).await == StatusCode::OK {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("service never became ready");
}

/// Elastic readiness may be satisfied entirely by overage headroom. Tests
/// whose next request must use a lease observe that separate prerequisite.
/// No requests have spent this first grant, so its refill signal is unarmed.
async fn wait_for_first_grant(router: &axum::Router, units: u64) {
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if metrics(router).await["total_lease_remaining"]
                .as_u64()
                .is_some_and(|remaining| remaining >= units)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        observed.is_ok(),
        "the first grant never funded {units} units: {}",
        metrics(router).await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorized_request_prices_and_charges() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;

    let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(14)).await;
    assert_eq!(status, StatusCode::OK);
    // fixed 50 + 14 contracts × 1 = 64 units.
    assert_eq!(body["metadata"]["units_charged"], 64);
    assert_eq!(body["prices"].as_array().unwrap().len(), 14);
    // A call is worth something sane (BSM 100/105 call ≈ 2.5).
    let price = body["prices"][0].as_f64().unwrap();
    assert!(price > 1.0 && price < 10.0, "price: {price}");

    // The committed charge reaches the billing ledger.
    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert_eq!(store.usage_recorded(DEMO_ACCOUNT), CostUnits(64));
}

/// #94's acceptance criterion: the response metadata and the usage record
/// refer to the same policy revision.
///
/// This is the whole point of the field and nothing else proves it. A
/// consumer resolves its customer-visible metadata — plan name, schedule
/// version — from the value it returns, so if the response could name one
/// policy while the bill named another, every such description would be
/// unverifiable. The two are one value by construction here, because the
/// handler reads it off the committed guard, which reads it off the event it
/// will emit; this test is what keeps that construction from drifting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_response_and_the_bill_name_the_same_policy_revision() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;

    let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(14)).await;
    assert_eq!(status, StatusCode::OK);
    let reported = body["metadata"]["policy_revision"]
        .as_str()
        .expect("the response states a policy revision")
        .to_string();
    assert_eq!(
        reported,
        DEMO_POLICY_REVISION.to_string(),
        "the response names the revision the snapshot was published with"
    );
    let request_id: RequestId = body["metadata"]["request_id"]
        .as_str()
        .expect("the response states a request id")
        .parse()
        .expect("the request id is canonical");

    let store = runtime.store.clone();
    runtime.shutdown().await;

    let settled = store
        .settled_event(request_id)
        .expect("the committed charge reached the ledger");
    assert_eq!(
        settled.policy_revision.to_string(),
        reported,
        "the bill must name exactly the policy the response reported"
    );
    assert_eq!(settled.units, CostUnits(64));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn billing_ledger_matches_charges_after_shutdown() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;

    let mut charged = 0u64;
    for _ in 0..5 {
        let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
        assert_eq!(status, StatusCode::OK);
        charged += body["metadata"]["units_charged"].as_u64().unwrap();
    }
    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert_eq!(store.usage_recorded(DEMO_ACCOUNT), CostUnits(charged));
    let conservation = store.conservation(DEMO_ACCOUNT).unwrap();
    assert!(conservation.holds());
    assert_eq!(conservation.settlement_loss, CostUnits::ZERO);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_or_bad_credentials_deny() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;

    let (status, body) = call(&router, None, price_body(1)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unknown-principal");
    assert_eq!(body["units_charged"], 0);

    let (status, _) = call(&router, Some("wrong-key"), price_body(1)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert_eq!(store.usage_recorded(DEMO_ACCOUNT), CostUnits::ZERO);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authentication_and_begin_precede_body_decoding() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;

    let (status, body) = call_raw(&router, None, "{ definitely not json").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unknown-principal");
    assert_eq!(body["units_charged"], 0);

    runtime.shutdown().await;
}

/// The optimization is part of server construction, not an optional handler
/// fast path. Omitting connection-scoped state must be visible immediately
/// instead of silently restoring per-request verification.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn price_route_requires_connection_context() {
    let (router, runtime) = build_app(100_000, true).await;
    wait_ready(&router).await;

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/price")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {DEMO_API_KEY}"))
                .body(Body::from(price_body(1).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/problem+json"
    );
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["code"], "missing-connection-state");
    assert_eq!(body["units_charged"], 0);

    runtime.shutdown().await;
}

/// Issue #2: caching proves only credential identity. Authorization remains a
/// fresh snapshot-map decision on every request, so revocation still reaches
/// an already-authenticated persistent connection through the normal push
/// path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_principal_still_observes_snapshot_revocation() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;

    for _ in 0..2 {
        let (status, _) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
        assert_eq!(status, StatusCode::OK);
    }

    let principals = runtime
        .store
        .principals()
        .await
        .expect("memory snapshot enumeration")
        .expect("memory store supports enumeration");
    let [principal] = principals.as_slice() else {
        panic!("the demo must publish exactly one principal: {principals:?}");
    };
    runtime.store.remove_snapshot(*principal);

    let mut revoked = false;
    for _ in 0..200 {
        let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
        if status == StatusCode::UNAUTHORIZED {
            assert_eq!(body["code"], "unknown-principal");
            revoked = true;
            break;
        }
        assert_eq!(status, StatusCode::OK, "unexpected response: {body}");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        revoked,
        "cached credential identity must not outlive snapshot authorization"
    );

    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_cap_denies_with_zero_charge() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;

    let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(1_025)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["code"], "batch-too-large");
    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert_eq!(store.usage_recorded(DEMO_ACCOUNT), CostUnits::ZERO);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_quota_returns_429_and_never_overspends() {
    // Tiny deposit: 200 units funds at most three 51-unit requests.
    let (router, runtime) = build_test_app(200, true).await;
    wait_ready(&router).await;

    let mut ok = 0;
    let mut denied = 0;
    for _ in 0..10 {
        let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
        match status {
            StatusCode::OK => ok += 1,
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => denied += 1,
            // Once usage has settled and a refusal has consolidated the tail,
            // the ledger attests the 47 units left cannot fund 51 (#130).
            StatusCode::PAYMENT_REQUIRED => {
                assert_eq!(body["code"], "balance-insufficient", "{body}");
                denied += 1;
            }
            other => panic!("unexpected status {other}"),
        }
    }
    assert!(ok <= 3, "overspend: {ok} requests admitted");
    assert!(denied >= 7);
    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert!(store.usage_recorded(DEMO_ACCOUNT).get() <= 200);
}

/// #131 over HTTP: the example halves grants, so 60 units grant 30 and a
/// 51-unit request is refused once. The refusal-driven consolidation grows the
/// lease to the refused quote, and the same request is then admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quote_above_half_the_balance_is_funded_after_one_refusal() {
    let (router, runtime) = build_test_app(60, true).await;
    wait_ready(&router).await;
    wait_for_first_grant(&router, 30).await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
            match status {
                StatusCode::OK => {
                    assert_eq!(body["metadata"]["units_charged"], 51);
                    break;
                }
                StatusCode::TOO_MANY_REQUESTS => {
                    assert_eq!(body["code"], "quota-exhausted", "{body}");
                }
                other => panic!("unexpected status {other}: {body}"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a consolidation must grow the lease to the refused quote");
    assert!(
        metrics(&router).await["refill"]["acquired"]
            .as_u64()
            .unwrap()
            >= 2,
        "the first grant and the consolidation that grew it"
    );
    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert_eq!(store.usage_recorded(DEMO_ACCOUNT), CostUnits(51));
}

/// #130 over HTTP: funding left, but less than the quote, is 402 and not
/// retryable at that quote; a top-up admits the same request again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quote_above_remaining_funding_is_payment_required_until_a_top_up() {
    let (router, runtime) = build_test_app(110, true).await;
    wait_ready(&router).await;
    wait_for_first_grant(&router, 51).await;
    let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // 59 units remain and 20 contracts quote 70. Until usage settles the
    // ledger cannot attest that, and the honest answer is the transient lease
    // refusal; afterwards a refusal-driven consolidation carries the evidence.
    let body = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(20)).await;
            match status {
                StatusCode::PAYMENT_REQUIRED => break body,
                StatusCode::TOO_MANY_REQUESTS => {
                    assert_eq!(body["code"], "quota-exhausted", "{body}");
                }
                other => panic!("unexpected status {other}: {body}"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("settled usage and a consolidation must attest the shortfall");
    assert_eq!(body["code"], "balance-insufficient", "{body}");
    assert_eq!(body["units_charged"], 0);
    assert!(
        metrics(&router).await["denials"]["balance_insufficient"]
            .as_u64()
            .unwrap()
            > 0
    );
    // A quote the remaining funding could cover is never told it cannot be.
    let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
    assert_ne!(status, StatusCode::PAYMENT_REQUIRED, "{body}");

    runtime.store.deposit(DEMO_ACCOUNT, CostUnits(200)).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(20)).await;
            if status == StatusCode::OK {
                break;
            }
            assert!(
                matches!(
                    status,
                    StatusCode::PAYMENT_REQUIRED | StatusCode::TOO_MANY_REQUESTS
                ),
                "{status}: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the top-up must clear the evidence");
    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert!(store.conservation(DEMO_ACCOUNT).unwrap().holds());
}

/// The elastic twin of `exhausted_quota_returns_429_and_never_overspends`,
/// end to end over HTTP. The same tiny deposit, and the same requests — but
/// the account keeps serving past what it paid for, every unfunded unit is
/// recorded. Cap exhaustion returns a retryable 503 because a subsequent
/// lease can fund the same request without changing the local overage cap.
///
/// This is also the test that pins the wiring reading the mode: a
/// `enforcement_mode()` that ignored the published snapshot would report no
/// cap here, admit nothing on credit, and fail on the first assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_elastic_account_serves_past_its_deposit_and_bills_the_overage() {
    // The cap must fund at least one worst-case request (50 fixed + 1 x 1024
    // items), or publication refuses it; 1_074 is exactly that bound, which
    // makes it 21 whole 51-unit requests of credit.
    const CAP: u64 = 1_074;
    let (router, runtime) = build_test_app_with_mode(
        200,
        true,
        EnforcementMode::Elastic {
            overage_cap: CostUnits(CAP),
        },
    )
    .await;
    wait_ready(&router).await;
    wait_for_first_grant(&router, 51).await;

    assert_eq!(
        metrics(&router).await["total_overage_cap"],
        json!(CAP),
        "the published mode must reach the operator surface"
    );

    let mut ok = 0;
    let mut capacity_unavailable = 0;
    for _ in 0..40 {
        let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
        match status {
            StatusCode::OK => ok += 1,
            StatusCode::SERVICE_UNAVAILABLE => {
                assert_eq!(body["code"], "overage-cap-exhausted");
                capacity_unavailable += 1;
            }
            // Once the credit is spent, the account's own funding decides:
            // none left, or less than one 51-unit quote (#130). Which one
            // depends on how far usage has settled when the refusal lands.
            StatusCode::PAYMENT_REQUIRED => {
                assert!(
                    matches!(
                        body["code"].as_str(),
                        Some("balance-exhausted" | "balance-insufficient")
                    ),
                    "{body}"
                );
                capacity_unavailable += 1;
            }
            other => panic!("unexpected status {other}: {body}"),
        }
    }

    // Strict would have stopped when the lease ran out. 200 units funds at
    // most three 51-unit requests, so anything past that was served on credit.
    assert!(ok > 3, "elastic must serve past the deposit, admitted {ok}");
    assert!(
        capacity_unavailable > 0,
        "the cap must eventually refuse when no refill is available"
    );

    let after = metrics(&router).await;
    let admitted = after["admitted"].as_u64().unwrap();
    let on_credit = after["admitted_overage"].as_u64().unwrap();
    assert_eq!(admitted, ok, "every 200 the caller saw is an admission");
    assert!(on_credit > 0, "some requests were served on credit");
    assert!(
        admitted > on_credit,
        "the lease funded the first requests, so `admitted` is the total and \
         `admitted_overage` a qualifier inside it — never a sibling to add"
    );
    assert_elastic_totals(&after, ok, on_credit, capacity_unavailable);
    assert!(on_credit * 51 <= CAP);
    let store = runtime.store.clone();
    runtime.shutdown().await;

    // The point of the whole design: spend beyond the deposit is *recorded*,
    // and the ledger still balances because overage funds what it bills.
    assert_elastic_bill(&store, 200, ok, on_credit);
    assert!(
        store.usage_recorded(DEMO_ACCOUNT).get() > 200,
        "the account was billed past its deposit"
    );
}

fn assert_elastic_totals(after: &Value, admitted: u64, on_credit: u64, denied: u64) {
    assert_eq!(after["admitted"], admitted);
    assert_eq!(after["units_admitted"], admitted * 51);
    assert_eq!(after["admitted_overage"], on_credit);
    assert_eq!(after["units_admitted_overage"], on_credit * 51);
    assert_eq!(after["total_overage_spent"], on_credit * 51);
    assert_eq!(after["committed_at_overage"], 0);
    assert_eq!(after["units_committed_at_overage"], 0);
    assert_eq!(after["execution_started"], admitted);
    assert_eq!(after["denied"], denied);
    assert_eq!(
        after["denials"]["overage_cap_exhausted"].as_u64().unwrap()
            + after["denials"]["balance_exhausted"].as_u64().unwrap()
            + after["denials"]["balance_insufficient"].as_u64().unwrap(),
        denied
    );
}

fn assert_elastic_bill(
    store: &tollgate_store::MemoryStore,
    deposited: u64,
    admitted: u64,
    on_credit: u64,
) {
    let ledger = store.conservation(DEMO_ACCOUNT).unwrap();
    assert_eq!(ledger.deposited, CostUnits(deposited));
    assert_eq!(ledger.settled_usage, CostUnits(admitted * 51));
    assert_eq!(store.usage_recorded(DEMO_ACCOUNT), CostUnits(admitted * 51));
    assert_eq!(ledger.overage_recorded, CostUnits(on_credit * 51));
    assert_eq!(ledger.settlement_loss, CostUnits::ZERO);
    assert_eq!(ledger.expired, CostUnits::ZERO);
    assert!(ledger.holds(), "conservation violated: {ledger:?}");
}

/// Hold funding at zero until after the HTTP assertions: no scheduler can
/// deliver the first grant early. Then publish the deposit and observe the
/// first installed grant before requiring a lease-funded admission.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn elastic_readiness_serves_before_the_first_grant_and_recovers_after_funding() {
    const CAP: u64 = 1_074;
    let (router, runtime) = build_test_app_with_mode(
        0,
        true,
        EnforcementMode::Elastic {
            overage_cap: CostUnits(CAP),
        },
    )
    .await;
    wait_ready(&router).await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if metrics(&router).await["refill"]["refusals"]["balance_exhausted"]
                .as_u64()
                .unwrap()
                > 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the zero balance must be confirmed before testing its HTTP classification");
    let before = metrics(&router).await;
    assert_eq!(before.get("total_lease_remaining"), Some(&Value::Null));
    assert_eq!(before["total_overage_cap"], CAP);

    // 21 x 51 = 1,071 fits; the remaining three units cannot fund request 22.
    // Readiness therefore never established the old `admitted > on_credit`
    // premise: all of these successful requests are legitimate overage.
    for request in 0..40 {
        let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
        if request < 21 {
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["metadata"]["units_charged"], 51);
        } else {
            assert_eq!(status, StatusCode::PAYMENT_REQUIRED, "{body}");
            assert_eq!(body["code"], "balance-exhausted");
            assert_eq!(body["units_charged"], 0);
        }
    }
    let unfunded = metrics(&router).await;
    assert_eq!(unfunded.get("total_lease_remaining"), Some(&Value::Null));
    assert_elastic_totals(&unfunded, 21, 21, 19);

    runtime.store.deposit(DEMO_ACCOUNT, CostUnits(200)).unwrap();
    wait_for_first_grant(&router, 51).await;
    let (status, body) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
    assert_eq!(status, StatusCode::OK, "the lease funds the retry: {body}");
    assert_eq!(body["metadata"]["units_charged"], 51);
    let funded = metrics(&router).await;
    assert_elastic_totals(&funded, 22, 21, 19);
    assert_eq!(funded["total_overage_cap"], CAP);

    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert_elastic_bill(&store, 200, 22, 21);
}

/// Issue #37: an instance that is refusing everything must not look like one
/// serving nothing. The counters are the request path's only voice, so the
/// scrape has to distinguish the two — and attribute each refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_separate_admissions_from_each_kind_of_refusal() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;

    let before = metrics(&router).await;
    assert_eq!(before["admitted"], 0);
    assert_eq!(before["denied"], 0);
    // Every reason is present up front, so a zero means "has not happened"
    // rather than "no such counter".
    assert_eq!(
        before["denials"].as_object().unwrap().len(),
        DenyReason::COUNT,
        "every reason must be exported, including the ones at zero"
    );

    // Two admissions: 14 contracts quote 64 units, 1 contract quotes 51.
    for items in [14, 1] {
        let (status, _) = call(&router, Some(DEMO_API_KEY), price_body(items)).await;
        assert_eq!(status, StatusCode::OK);
    }
    // Three refusals of two distinct kinds, one of them raised before the
    // engine is ever consulted.
    let (status, _) = call(&router, None, price_body(1)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&router, Some("wrong-key"), price_body(1)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&router, Some(DEMO_API_KEY), price_body(1_025)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

    let after = metrics(&router).await;
    assert_eq!(after["admitted"], 2);
    assert_eq!(after["units_admitted"], 115, "64 + 51 units quoted");
    assert_eq!(after["denied"], 3);
    assert_eq!(after["denials"]["unknown_principal"], 2);
    assert_eq!(after["denials"]["request_too_large"], 1);
    assert_eq!(
        after["denials"]["rate_limited"], 0,
        "a reason that did not occur must stay at zero"
    );
    // The off-path gauges: what is left to spend, and until when. A lease can
    // be refused for either reason, so both are exported.
    assert!(after["total_lease_remaining"].as_u64().is_some());
    assert!(after["earliest_lease_usable_until"].as_str().is_some());

    // The later phases are exported too, and they partition `admitted`
    // exactly. Both requests ran their kernel to completion, so nothing was
    // cancelled, shed, or refused at execution start (INVARIANTS.md #20).
    assert_eq!(after["execution_started"], 2);
    assert_eq!(after["canceled_before_start"], 0);
    assert_eq!(after["capacity_shed"], 0);
    assert_eq!(after["refused_at_start"], 0);
    assert_eq!(
        after["execution_started"].as_u64().unwrap()
            + after["canceled_before_start"].as_u64().unwrap()
            + after["capacity_shed"].as_u64().unwrap()
            + after["refused_at_start"].as_u64().unwrap(),
        after["admitted"].as_u64().unwrap(),
        "every admitted request must reach exactly one terminal counter"
    );
    // The two credentials that could not be resolved were refused before a
    // context existed, so they abandoned none.
    assert_eq!(after["contexts_abandoned"], 0);
    assert_eq!(after["committed_at_overage"], 0, "the lease funded both");
    // Present-at-zero, exactly as `denials` is: a reader must be able to tell
    // "has not happened" from "no such counter".
    assert_eq!(
        after["commit_refusals"].as_object().unwrap().len(),
        CommitRefusal::COUNT,
        "every commit refusal must be exported, including the ones at zero"
    );
    assert_eq!(after["commit_refusals"]["funding_expired"], 0);

    runtime.shutdown().await;
}

/// Issue #38: the accounting numbers were returned once, from a graceful
/// shutdown — the one case where loss is least likely. They are readable
/// while the service runs now, and they agree with the admission counters
/// about backpressure, so the two views cannot drift into contradicting each
/// other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_report_accounting_health_while_running() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;

    let before = metrics(&router).await;
    let accounting = &before["accounting"];
    assert_eq!(accounting["accepted"], 0);
    assert_eq!(accounting["lost"], 0);
    assert_eq!(accounting["shed"], 0);
    assert!(
        accounting["queue_capacity"].as_u64().unwrap() > 0,
        "the shed point must be visible, not implied"
    );
    assert!(
        accounting["last_ingest_at"].is_null(),
        "nothing has been billed yet"
    );

    for _ in 0..3 {
        let (status, _) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
        assert_eq!(status, StatusCode::OK);
    }
    // The writer flushes on its own interval; poll until the charges land
    // rather than sleeping a fixed amount.
    let mut after = metrics(&router).await;
    for _ in 0..200 {
        if after["accounting"]["accepted"].as_u64() == Some(3) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        after = metrics(&router).await;
    }

    let accounting = &after["accounting"];
    assert_eq!(
        accounting["accepted"], 3,
        "three billed charges, visible without shutting anything down"
    );
    assert_eq!(accounting["rejected"], 0);
    assert_eq!(accounting["unaccounted"], 0, "all three have an outcome");
    assert!(
        accounting["last_ingest_at"].is_string(),
        "the sink has answered, so there is a time to age from"
    );
    assert!(accounting["ingest_age_seconds"].as_i64().is_some());

    // The two backpressure counters are different views of one event: the
    // service's deny vocabulary, and the accounting subsystem's own tally.
    assert_eq!(
        accounting["shed"], after["denials"]["accounting_backpressure"],
        "the deny vocabulary and the queue's own count must agree"
    );

    runtime.shutdown().await;
}

/// Issue #4's counter set is only complete once the refill and snapshot
/// planes are scrapeable too: both were visible as `tracing` events with
/// nothing to threshold on. The `unresolved` gauge in particular is what
/// disambiguates an `unknown_principal` spike — distribution failure, or
/// callers presenting keys nobody published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_report_refill_and_snapshot_health() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;

    let body = metrics(&router).await;

    let refill = &body["refill"];
    assert!(
        refill["acquired"].as_u64().unwrap() >= 1,
        "readiness implies a lease was granted: {refill}"
    );
    assert!(refill["acquired_units"].as_u64().unwrap() > 0);
    assert_eq!(refill["refused"], 0, "a healthy allocator refuses nothing");
    assert_eq!(refill["acquire_timeouts"], 0);
    assert_eq!(refill["abandoned"], 0);
    assert_eq!(
        refill["refusals"].as_object().unwrap().len(),
        AllocateError::COUNT,
        "every refusal reason is exported, including the ones at zero"
    );

    let snapshots = &body["snapshots"];
    assert!(
        snapshots["refresh_attempts"].as_u64().unwrap() >= 1,
        "the refresh loop is running: {snapshots}"
    );
    assert_eq!(snapshots["refresh_failures"], 0);
    assert_eq!(snapshots["refresh_timeouts"], 0);
    assert_eq!(snapshots["refused_updates"], 0);
    assert_eq!(snapshots["history_evictions"], 0);
    assert_eq!(snapshots["publication_failures"], 0);
    assert_eq!(
        snapshots["unresolved"], 0,
        "readiness is true, so nothing may be unresolved — the gauge and the \
         readiness bit are computed from one pass"
    );

    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn not_ready_until_lease_arrives() {
    let (router, runtime) = build_test_app(100_000, true).await;
    // Immediately after boot the slot may be empty: readiness must reflect
    // it rather than serving guaranteed denials (INVARIANTS.md #10). We only
    // assert the transition completes.
    wait_ready(&router).await;
    assert_eq!(ready(&router).await, StatusCode::OK);
    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_falls_when_background_planes_stop() {
    let (router, runtime) = build_test_app(100_000, true).await;
    wait_ready(&router).await;
    runtime.shutdown().await;
    assert_eq!(ready(&router).await, StatusCode::SERVICE_UNAVAILABLE);
}

/// The baseline configuration had no test at all until #16 rewrote the code
/// that distinguishes it: the load gate was its only exercise, and that lane
/// is manual and non-gating. Transport and kernel only — no credential
/// required, because there is no admission to present one to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_prices_without_admission() {
    let (router, runtime) = build_test_app(100_000, false).await;

    let (status, body) = call(&router, None, price_body(14)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["prices"].as_array().unwrap().len(), 14);
    let price = body["prices"][0].as_f64().unwrap();
    assert!(price > 1.0 && price < 10.0, "price: {price}");
    assert_eq!(body["metadata"]["units_charged"], 0);
    assert_eq!(body["metadata"]["request_id"], "baseline");

    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert_eq!(store.usage_recorded(DEMO_ACCOUNT), CostUnits::ZERO);
}

/// #16's acceptance criterion: readiness must not depend on machinery that was
/// never installed. No lease is ever stocked here, and no background task
/// exists whose death could change the answer — so 200 before shutdown and
/// 200 after it, the exact counterpart of
/// `readiness_falls_when_background_planes_stop`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_is_ready_before_any_lease() {
    let (router, runtime) = build_test_app(100_000, false).await;
    assert_eq!(ready(&router).await, StatusCode::OK);
    runtime.shutdown().await;
    assert_eq!(ready(&router).await, StatusCode::OK);
}

/// `accounting`, `refill` and `snapshots` are three views of one plane, so
/// they are absent together or present together. That correlation used to be
/// five parallel `Option`s in `AppState`, provable only by reading every
/// construction site; #16 made it one `Option`, and this is its witness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_metrics_omit_the_uninstalled_planes() {
    let (router, runtime) = build_test_app(100_000, false).await;
    let (status, _) = call(&router, None, price_body(1)).await;
    assert_eq!(status, StatusCode::OK);

    let body = metrics(&router).await;
    assert!(
        body["accounting"].is_null(),
        "no writer was spawned: {body}"
    );
    assert!(body["refill"].is_null(), "no lease manager was spawned");
    assert!(
        body["snapshots"].is_null(),
        "no snapshot manager was spawned"
    );
    // Nothing ever stocked the slot, so the gauges read off it report absence
    // rather than a zero that would read as an exhausted lease.
    assert!(body["total_lease_remaining"].is_null());
    assert!(body["earliest_lease_usable_until"].is_null());
    // The request-path counters stay present: the engine is built in both
    // configurations, and a baseline request simply never reaches it.
    assert_eq!(body["admitted"], 0);
    assert_eq!(body["denied"], 0);
    assert_eq!(
        body["denials"].as_object().unwrap().len(),
        DenyReason::COUNT
    );

    runtime.shutdown().await;
}

/// #99 end to end: two accounts of different classes serve one instance, and
/// each start is attributed to the class that made it.
///
/// The pool arithmetic is proved by unit tests and the shedding by the load
/// gate's mixed-saturation scenario, which can actually saturate. What this
/// adds is that the class survives the whole embedding — account creation,
/// snapshot publication against a ledger that owns the class, credential
/// resolution, admission, and the gate — and reaches the metrics an operator
/// reads. It is the first test here where two accounts of *different* classes
/// serve one instance, which is the situation the class exists for.
#[tokio::test]
async fn two_classes_share_an_instance_and_each_start_is_attributed() {
    let tenants = demo_tenants(1, 1);
    let (assured, best_effort) = (tenants[0].clone(), tenants[1].clone());
    let (router, runtime) = build_app_with_capacity(
        1_000_000,
        true,
        LocalSharding::SINGLE,
        &tenants,
        // One shared unit and one reserve unit: the smallest configuration in
        // which the class changes an outcome at all.
        ExecutionCapacityMode::Reserved {
            total: NonZeroU32::new(2).unwrap(),
            assured_reserve: NonZeroU32::new(1).unwrap(),
        },
    )
    .await;
    let router = router.layer(MockConnectInfo(PricingConnection::default()));
    // Readiness now covers every tenant, so this waits for both accounts'
    // leases and snapshots rather than only the primary's.
    wait_ready(&router).await;

    // Both classes are served while capacity is free: the reserve exists to
    // be unreachable under load, not to refuse work there is room for.
    for tenant in [&assured, &best_effort] {
        let (status, _) = call(&router, Some(&tenant.api_key), price_body(1)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{} must be served while the instance has room",
            tenant.api_key
        );
    }

    let metrics = metrics(&router).await;
    assert_eq!(metrics["capacity"]["shared_total"], 1);
    assert_eq!(metrics["capacity"]["reserve_total"], 1);
    // Every permit was released on drop, so the instance is idle again.
    assert_eq!(metrics["capacity"]["shared_available"], 1);
    assert_eq!(metrics["capacity"]["reserve_available"], 1);
    // The per-class breakdown attributes each start to the class that made
    // it, and sums to the total beside it.
    assert_eq!(metrics["execution_started_by_class"]["Assured"], 1);
    assert_eq!(metrics["execution_started_by_class"]["BestEffort"], 1);
    assert_eq!(metrics["execution_started"], 2);

    runtime.shutdown().await;
}

/// A disabled instance reports no capacity at all, rather than zeroes that
/// read as an exhausted one.
#[tokio::test]
async fn a_disabled_instance_reports_no_capacity_rather_than_an_empty_one() {
    let (router, runtime) = build_test_app(1_000_000, true).await;
    wait_ready(&router).await;
    let (status, _) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
    assert_eq!(status, StatusCode::OK);

    let metrics = metrics(&router).await;
    assert!(
        metrics["capacity"].is_null(),
        "a disabled gate has no pools to report: {}",
        metrics["capacity"]
    );
    // The class breakdown is still present and still sums, because the
    // counters exist whether or not a gate does.
    assert_eq!(metrics["execution_started_by_class"]["Assured"], 1);
    assert_eq!(metrics["execution_started_by_class"]["BestEffort"], 0);
    assert_eq!(metrics["capacity_shed"], 0);

    runtime.shutdown().await;
}

#[tokio::test]
async fn extractor_failures_are_zero_charge_problem_json_with_fixed_counters() {
    for (content_type, body, status, code) in [
        (
            "application/json",
            "{".to_owned(),
            StatusCode::BAD_REQUEST,
            "malformed-body",
        ),
        (
            "text/plain",
            "{}".to_owned(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported-media-type",
        ),
        (
            "application/json",
            " ".repeat(2 * 1024 * 1024 + 1),
            StatusCode::PAYLOAD_TOO_LARGE,
            "body-too-large",
        ),
    ] {
        let (router, runtime) = build_test_app(10_000, true).await;
        wait_ready(&router).await;
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/price")
                    .header(header::AUTHORIZATION, format!("Bearer {DEMO_API_KEY}"))
                    .header(header::CONTENT_TYPE, content_type)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/problem+json"
        );
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["code"], code);
        assert_eq!(body["units_charged"], 0);
        let counters = metrics(&router).await;
        assert_eq!(counters["input_rejections"][code], 1);
        assert_eq!(counters["contexts_abandoned"], 1);
        assert_eq!(counters["admitted"], 0);
        runtime.shutdown().await;
    }
}

#[tokio::test]
async fn http_shutdown_joins_the_listener_and_settles_usage_before_returning() {
    let (router, runtime) = build_test_app(10_000, true).await;
    wait_ready(&router).await;
    let store = runtime.store.clone();
    let (status, response) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
    assert_eq!(status, StatusCode::OK);
    let charged = response["metadata"]["units_charged"].as_u64().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server_router = router.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, server_router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    runtime.shutdown_server(server, stop).await.unwrap();
    assert_eq!(store.usage_recorded(DEMO_ACCOUNT), CostUnits(charged));
    assert_eq!(store.balance(DEMO_ACCOUNT), CostUnits(10_000 - charged));
    assert_eq!(ready(&router).await, StatusCode::SERVICE_UNAVAILABLE);
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn http_quiescence_is_bounded_by_the_runtime_deadline() {
    struct Exited(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for Exited {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }
    let (router, runtime) = build_test_app(10_000, true).await;
    wait_ready(&router).await;
    let store = runtime.store.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let (exited, exit) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let _exited = Exited(Some(exited));
        let _ = stopped.await;
        std::future::pending::<std::io::Result<()>>().await
    });
    let began = tokio::time::Instant::now();
    let error = runtime.shutdown_server(server, stop).await.unwrap_err();
    assert_eq!(error, "HTTP quiescence deadline expired");
    assert_eq!(began.elapsed(), std::time::Duration::from_secs(15));
    exit.await.unwrap();
    assert_eq!(store.balance(DEMO_ACCOUNT), CostUnits(10_000));
    assert_eq!(ready(&router).await, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test(start_paused = true)]
async fn cancelling_http_shutdown_aborts_the_server_before_or_after_first_poll() {
    struct Exited(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for Exited {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }
    for poll in [false, true] {
        let (router, runtime) = build_test_app(10_000, true).await;
        wait_ready(&router).await;
        let store = runtime.store.clone();
        let principals = store.principals().await.unwrap().unwrap();
        let principal = principals[0];
        let tollgate_store::SnapshotResolution::Present(before) =
            store.snapshot(principal).await.unwrap()
        else {
            panic!("demo principal must resolve");
        };
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (draining, drain) = tokio::sync::oneshot::channel();
        let (exited, exit) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let _exited = Exited(Some(exited));
            entered.send(()).unwrap();
            let _ = stopped.await;
            let _ = draining.send(());
            std::future::pending::<std::io::Result<()>>().await
        });
        entry.await.unwrap();
        let shutdown = runtime.shutdown_server(server, stop);
        if poll {
            let shutdown = tokio::spawn(shutdown);
            drain.await.unwrap();
            shutdown.abort();
            assert!(shutdown.await.unwrap_err().is_cancelled());
        } else {
            drop(shutdown);
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), exit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ready(&router).await, StatusCode::SERVICE_UNAVAILABLE);
        tokio::time::advance(std::time::Duration::from_secs(3_600)).await;
        tokio::task::yield_now().await;
        let tollgate_store::SnapshotResolution::Present(after) =
            store.snapshot(principal).await.unwrap()
        else {
            panic!("shutdown preserves the published snapshot");
        };
        assert_eq!(after.generation, before.generation);
    }
}

#[tokio::test(start_paused = true)]
async fn demo_credentials_are_durable_and_revocation_reaches_new_verification() {
    use tollgate_store::KeyDirectory;
    let (router, runtime) = build_test_app(100_000, true).await;
    let now = jiff::Timestamp::now();
    let keys = runtime.store.active_keys(now).await.unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].account_id, DEMO_ACCOUNT);
    wait_ready(&router).await;
    assert_eq!(
        call(&router, Some(DEMO_API_KEY), price_body(14)).await.0,
        StatusCode::OK
    );
    runtime.store.revoke_key(keys[0].key_id, now).await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    // Missing credentials clear this connection's cached proof. The next
    // request must consult the newly replaced verifier, while the account's
    // snapshot remains active and funded.
    assert_eq!(
        call(&router, None, price_body(14)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&router, Some(DEMO_API_KEY), price_body(14)).await.0,
        StatusCode::UNAUTHORIZED
    );
    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert_eq!(store.usage_recorded(DEMO_ACCOUNT), CostUnits(64));
    let activity = store.credential_activity(&[keys[0].key_id]).await.unwrap();
    assert!(matches!(
        activity[0].state,
        tollgate_store::CredentialActivityState::Committed { .. }
    ));
}
