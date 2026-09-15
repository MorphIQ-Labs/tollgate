//! Backend diagnostics are untrusted data, even after HTTP authentication.
mod common;

use axum::response::IntoResponse;
use http_body_util::BodyExt;
use tollgate_server::error::ApiError;
use tollgate_store::{
    AllocateError, CreateAccountError, IngestError, PublishSnapshotError, SetStatusError,
    StoreError,
};

type Conversion = fn(StoreError) -> ApiError;

#[tokio::test]
async fn every_backend_error_conversion_keeps_opaque_details_out_of_responses_and_debug() {
    use tracing_subscriber::layer::SubscriberExt;
    let capture = common::EventCapture::default();
    // This current-thread test never spawns; conversions and response decoding
    // remain inside this dispatcher, including any accidental diagnostic log.
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let conversions: [(Conversion, u16, &str, &str); 7] = [
        (ApiError::from, 503, "storage", "backend unavailable"),
        (
            |error| AllocateError::Storage(error).into(),
            503,
            "storage",
            "backend unavailable",
        ),
        (
            |error| CreateAccountError::Storage(error).into(),
            503,
            "storage",
            "backend unavailable",
        ),
        (
            |error| SetStatusError::Storage(error).into(),
            503,
            "storage",
            "backend unavailable",
        ),
        (
            |error| PublishSnapshotError::Storage(error).into(),
            503,
            "storage",
            "backend unavailable",
        ),
        (
            |error| IngestError::Unavailable(error).into(),
            503,
            "storage",
            "backend unavailable",
        ),
        (
            |error| IngestError::Refused(error).into(),
            422,
            "usage-refused",
            "usage batch refused",
        ),
    ];
    // Deliberately public test data, exercising more than URL userinfo.
    for detail in [
        "postgres://fixture-user:fixture-sensitive-70@localhost/db",
        "postgres://localhost/db?password=fixture-sensitive-70",
        "host=localhost password='fixture-sensitive-70' dbname=example",
        "constraint violated: row contains fixture-sensitive-70",
        "Authorization: Bearer fixture-sensitive-70\nforged log event",
        "private key bytes: fixture-sensitive-70; unicode: λ🔐",
        "fixture-sensitive-70",
        "",
    ] {
        for (convert, status, code, title) in conversions {
            let error = convert(StoreError(detail.into()));
            assert_eq!(error.title, title);
            assert!(!format!("{error:?}").contains("fixture-sensitive-70"));
            let response = error.into_response();
            assert_eq!(response.status().as_u16(), status);
            assert_eq!(
                response.headers()["content-type"],
                "application/problem+json"
            );
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["status"], status);
            assert_eq!(body["code"], code);
            assert_eq!(body["title"], title);
            assert!(body.get("generation").is_none());
            let id = body["error_id"].as_str().unwrap();
            assert_eq!(id.len(), 32);
            assert!(u128::from_str_radix(id, 16).is_ok());
            assert!(!String::from_utf8_lossy(&bytes).contains("fixture-sensitive-70"));
            // Existing clients ignore additive diagnostic extensions.
            let legacy: tollgate_store::wire::Problem = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(legacy.status, status);
            assert_eq!(legacy.code, code);
        }
    }
    assert!(!format!("{:?}", capture.events()).contains("fixture-sensitive-70"));
}

#[tokio::test]
async fn router_correlates_backend_failures_without_logging_request_or_error_payloads() {
    use axum::{body::Body, http::Request};
    use std::sync::Arc;
    use tollgate_core::{
        AccountId, AccountStatus, CapacityClass, CostUnits, PolicyRevision, RequestId, UsageEvent,
        UsageSource,
    };
    use tollgate_server::{ServerState, router};
    use tollgate_store::{AccountConfig, GrantPolicy, MemoryStore, SystemClock, UsageSink};
    use tower::ServiceExt;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::layer::SubscriberExt;

    const FORGED_ID: &str = "00000000000000000000000000000070aa";
    let account = AccountId(70);
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    tollgate_store::AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: account,
            initial_balance: CostUnits(u64::MAX),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::BestEffort,
        },
    )
    .await
    .unwrap();
    let at = jiff::Timestamp::from_second(0).unwrap();
    let event = |id, units| {
        UsageEvent::new(
            RequestId(id),
            account,
            UsageSource::Overage,
            CostUnits(units),
            at,
            PolicyRevision::UNSTATED,
            None,
        )
    };
    store.ingest(&[event(1, u64::MAX)], at).await.unwrap();
    let app = router(ServerState {
        store,
        clock: Arc::new(SystemClock),
        security: common::security(),
        issuer: None,
    });
    let capture = common::EventCapture::default();
    let expected = async {
        let mut expected = Vec::new();
        for (path, token, body, status, code, route) in [
            (
                format!(
                    "/v1/admin/accounts/{account}/deposit?password=fixture-request-sensitive-70"
                ),
                common::OPERATOR,
                serde_json::json!({"units": 1, "private_note": "fixture-request-sensitive-70"}),
                503,
                "storage",
                "/v1/admin/accounts/{account}/deposit",
            ),
            (
                "/v1/usage/ingest?password=fixture-request-sensitive-70".into(),
                common::INSTANCE,
                serde_json::to_value(tollgate_store::wire::IngestRequest {
                    events: vec![event(2, 1)],
                })
                .unwrap(),
                422,
                "usage-refused",
                "/v1/usage/ingest",
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .header("authorization", format!("Bearer {token}"))
                        .header("content-type", "application/json")
                        .header("x-request-id", FORGED_ID)
                        .header("x-error-id", FORGED_ID)
                        .header("x-private-fixture", "fixture-request-sensitive-70")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), status);
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(json["code"], code);
            let id = json["error_id"].as_str().unwrap().to_owned();
            assert_eq!(id.len(), 32);
            assert!(u128::from_str_radix(&id, 16).is_ok());
            assert_ne!(id, FORGED_ID);
            expected.push((id, status.to_string(), code, route));
        }
        let response = app.clone().oneshot(Request::builder().method("POST")
            .uri("/v1/leases/acquire")
            .header("authorization", format!("Bearer {}", common::INSTANCE))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::json!({"account_id": account, "requested": 1, "ttl_seconds": 0}).to_string())).unwrap()).await.unwrap();
        assert_eq!(response.status().as_u16(), 422);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "invalid-ttl");
        assert!(body.get("error_id").is_none());
        // Ordinary domain failures and healthy probes do not create a backend
        // incident, and authentication still carries its challenge header.
        for (path, status) in [("/livez", 200), ("/v1/snapshots", 401)] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), status);
            if status == 401 {
                assert_eq!(
                    response.headers()["www-authenticate"],
                    "Bearer realm=\"tollgate-control\""
                );
                let bytes = response.into_body().collect().await.unwrap().to_bytes();
                let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert!(body.get("error_id").is_none());
            }
        }
        expected
    }
    .with_subscriber(tracing_subscriber::registry().with(capture.clone()))
    .await;
    assert_ne!(expected[0].0, expected[1].0);
    let events = capture.events();
    let incidents: Vec<_> = events
        .iter()
        .filter(|event| event.target == "tollgate::diagnostics")
        .collect();
    assert_eq!(incidents.len(), expected.len());
    for (event, (id, status, code, route)) in incidents.iter().zip(expected) {
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(event.fields["error_id"], id);
        assert_eq!(event.fields["error_id_unavailable"], "false");
        assert_eq!(event.fields["status"], status);
        assert_eq!(event.fields["code"], code);
        assert_eq!(event.fields["route"], route);
    }
    let logs = format!("{events:?}");
    for private in [
        "fixture-request-sensitive-70",
        FORGED_ID,
        common::OPERATOR,
        common::INSTANCE,
    ] {
        assert!(!logs.contains(private));
    }
}

#[tokio::test]
async fn tombstone_responses_preserve_generation_without_creating_backend_incidents() {
    let response = ApiError::revoked(tollgate_core::Generation(70)).into_response();
    assert_eq!(response.status().as_u16(), 410);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let problem: tollgate_store::wire::Problem = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(problem.code, "revoked-principal");
    assert_eq!(problem.generation, Some(tollgate_core::Generation(70)));
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(body.get("error_id").is_none());
}
