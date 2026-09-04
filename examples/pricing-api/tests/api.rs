//! Contract tests for the example service: the embedding is where the
//! product's guarantees become user-visible HTTP behavior.

use axum::body::Body;
use axum::extract::connect_info::MockConnectInfo;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use pricing_api::{
    AppRuntime, DEMO_ACCOUNT, DEMO_API_KEY, PricingConnection, build_app, build_app_with_mode,
};
use tollgate_admission::CommitRefusal;
use tollgate_core::{CostUnits, DenyReason, EnforcementMode};
use tollgate_store::SnapshotSource;

/// Every test router needs a connection to authenticate against, because the
/// price route takes `ConnectInfo<PricingConnection>` — that requirement is
/// deliberate (#2), and `price_route_requires_connection_context` is the test
/// that keeps it from being quietly optional.
fn build_test_app(deposit: u64, admission_enabled: bool) -> (axum::Router, AppRuntime) {
    let (router, runtime) = build_app(deposit, admission_enabled);
    (
        router.layer(MockConnectInfo(PricingConnection::default())),
        runtime,
    )
}

/// The same, for the enforcement-mode tests #1 added.
fn build_test_app_with_mode(
    deposit: u64,
    admission_enabled: bool,
    mode: EnforcementMode,
) -> (axum::Router, AppRuntime) {
    let (router, runtime) = build_app_with_mode(deposit, admission_enabled, mode);
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

async fn call(router: &axum::Router, auth: Option<&str>, body: Value) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/price")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(key) = auth {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    let response = router
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn call_raw(router: &axum::Router, auth: Option<&str>, body: &str) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/price")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(key) = auth {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    let response = router
        .clone()
        .oneshot(builder.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn metrics(router: &axum::Router) -> Value {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorized_request_prices_and_charges() {
    let (router, runtime) = build_test_app(100_000, true);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn billing_ledger_matches_charges_after_shutdown() {
    let (router, runtime) = build_test_app(100_000, true);
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
    let (router, runtime) = build_test_app(100_000, true);
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
    let (router, runtime) = build_test_app(100_000, true);
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
    let (router, runtime) = build_app(100_000, true);
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

    runtime.shutdown().await;
}

/// Issue #2: caching proves only credential identity. Authorization remains a
/// fresh snapshot-map decision on every request, so revocation still reaches
/// an already-authenticated persistent connection through the normal push
/// path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_principal_still_observes_snapshot_revocation() {
    let (router, runtime) = build_test_app(100_000, true);
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
    let (router, runtime) = build_test_app(100_000, true);
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
    let (router, runtime) = build_test_app(200, true);
    wait_ready(&router).await;

    let mut ok = 0;
    let mut denied = 0;
    for _ in 0..10 {
        let (status, _) = call(&router, Some(DEMO_API_KEY), price_body(1)).await;
        match status {
            StatusCode::OK => ok += 1,
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => denied += 1,
            other => panic!("unexpected status {other}"),
        }
    }
    assert!(ok <= 3, "overspend: {ok} requests admitted");
    assert!(denied >= 7);
    let store = runtime.store.clone();
    runtime.shutdown().await;
    assert!(store.usage_recorded(DEMO_ACCOUNT).get() <= 200);
}

/// The elastic twin of `exhausted_quota_returns_429_and_never_overspends`,
/// end to end over HTTP. The same tiny deposit, and the same requests — but
/// the account keeps serving past what it paid for, every unfunded unit is
/// recorded, and the refusal when the cap runs out is a 402 that says so
/// rather than a 429 inviting a retry that cannot help.
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
    );
    wait_ready(&router).await;

    assert_eq!(
        metrics(&router).await["overage_cap"],
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
            other => panic!("unexpected status {other}: {body}"),
        }
    }

    // Strict would have stopped when the lease ran out. 200 units funds only
    // a handful of 51-unit requests — and fewer than three, because the grant
    // policy shrinks near exhaustion — so anything past that was served on
    // credit.
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
    assert!(after["overage_spent"].as_u64().unwrap() <= CAP);
    assert_eq!(
        after["denials"]["overage_cap_exhausted"],
        json!(capacity_unavailable)
    );

    let store = runtime.store.clone();
    runtime.shutdown().await;

    // The point of the whole design: spend beyond the deposit is *recorded*,
    // and the ledger still balances because overage funds what it bills.
    let conservation = store.conservation(DEMO_ACCOUNT).unwrap();
    assert!(
        conservation.overage_recorded > CostUnits::ZERO,
        "unfunded spend must be recorded, not forgiven"
    );
    assert!(
        store.usage_recorded(DEMO_ACCOUNT).get() > 200,
        "the account was billed past its deposit"
    );
    assert!(
        conservation.holds(),
        "conservation violated: {conservation:?}"
    );
}

/// Issue #37: an instance that is refusing everything must not look like one
/// serving nothing. The counters are the request path's only voice, so the
/// scrape has to distinguish the two — and attribute each refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_separate_admissions_from_each_kind_of_refusal() {
    let (router, runtime) = build_test_app(100_000, true);
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
    assert!(after["lease_remaining"].as_u64().is_some());
    assert!(after["lease_usable_until"].as_str().is_some());

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
    let (router, runtime) = build_test_app(100_000, true);
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
    let (router, runtime) = build_test_app(100_000, true);
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
        9,
        "every refusal reason is exported, including the ones at zero"
    );

    let snapshots = &body["snapshots"];
    assert!(
        snapshots["refresh_attempts"].as_u64().unwrap() >= 1,
        "the refresh loop is running: {snapshots}"
    );
    assert_eq!(snapshots["refresh_failures"], 0);
    assert_eq!(
        snapshots["unresolved"], 0,
        "readiness is true, so nothing may be unresolved — the gauge and the \
         readiness bit are computed from one pass"
    );

    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn not_ready_until_lease_arrives() {
    let (router, runtime) = build_test_app(100_000, true);
    // Immediately after boot the slot may be empty: readiness must reflect
    // it rather than serving guaranteed denials (INVARIANTS.md #10). We only
    // assert the transition completes.
    wait_ready(&router).await;
    assert_eq!(ready(&router).await, StatusCode::OK);
    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_falls_when_background_planes_stop() {
    let (router, runtime) = build_test_app(100_000, true);
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
    let (router, runtime) = build_test_app(100_000, false);

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
    let (router, runtime) = build_test_app(100_000, false);
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
    let (router, runtime) = build_test_app(100_000, false);
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
    assert!(body["lease_remaining"].is_null());
    assert!(body["lease_usable_until"].is_null());
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
