//! Contract tests for the example service: the embedding is where the
//! product's guarantees become user-visible HTTP behavior.

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use pricing_api::{DEMO_ACCOUNT, DEMO_API_KEY, build_app};
use tollgate_core::CostUnits;

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
    let (router, runtime) = build_app(100_000, true);
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
    let (router, runtime) = build_app(100_000, true);
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
    let (router, runtime) = build_app(100_000, true);
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
async fn batch_cap_denies_with_zero_charge() {
    let (router, runtime) = build_app(100_000, true);
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
    let (router, runtime) = build_app(200, true);
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

/// Issue #37: an instance that is refusing everything must not look like one
/// serving nothing. The counters are the request path's only voice, so the
/// scrape has to distinguish the two — and attribute each refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_separate_admissions_from_each_kind_of_refusal() {
    let (router, runtime) = build_app(100_000, true);
    wait_ready(&router).await;

    let before = metrics(&router).await;
    assert_eq!(before["admitted"], 0);
    assert_eq!(before["denied"], 0);
    // Every reason is present up front, so a zero means "has not happened"
    // rather than "no such counter".
    assert_eq!(
        before["denials"].as_object().unwrap().len(),
        14,
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

    runtime.shutdown().await;
}

/// Issue #38: the accounting numbers were returned once, from a graceful
/// shutdown — the one case where loss is least likely. They are readable
/// while the service runs now, and they agree with the admission counters
/// about backpressure, so the two views cannot drift into contradicting each
/// other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_report_accounting_health_while_running() {
    let (router, runtime) = build_app(100_000, true);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn not_ready_until_lease_arrives() {
    let (router, runtime) = build_app(100_000, true);
    // Immediately after boot the slot may be empty: readiness must reflect
    // it rather than serving guaranteed denials (INVARIANTS.md #10). We only
    // assert the transition completes.
    wait_ready(&router).await;
    assert_eq!(ready(&router).await, StatusCode::OK);
    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_falls_when_background_planes_stop() {
    let (router, runtime) = build_app(100_000, true);
    wait_ready(&router).await;
    runtime.shutdown().await;
    assert_eq!(ready(&router).await, StatusCode::SERVICE_UNAVAILABLE);
}
