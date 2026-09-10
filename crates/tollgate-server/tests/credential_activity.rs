mod common;

use axum::{
    Router,
    body::Body,
    http::{Request, Response, StatusCode, header},
    routing::post,
};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::Arc;
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, Generation,
    KeyId, PermissionBits, PolicyRevision, Principal, RequestId, ResolvedLimits, UsageEvent,
    UsageSource,
};
use tollgate_store::{
    AccountConfig, GrantPolicy, IngestReport, ManualClock, MemoryStore, UsageSink,
};
use tower::ServiceExt;

fn event() -> UsageEvent {
    UsageEvent::new(
        RequestId(105),
        AccountId(1),
        UsageSource::Overage,
        CostUnits(1),
        jiff::Timestamp::UNIX_EPOCH,
        PolicyRevision::UNSTATED,
        Some(KeyId(1)),
    )
}

#[tokio::test]
async fn usage_acknowledgements_require_complete_bounded_valid_evidence() {
    let valid = r#"{"accepted":1,"duplicate":0,"rejected":0,"unattributed":0}"#;
    for (status, range, body, expected) in [
        (200, false, valid.to_owned(), Some(Some(0))),
        (
            200,
            false,
            r#"{"accepted":1,"duplicate":0,"rejected":0}"#.into(),
            Some(None),
        ),
        (206, false, valid.to_owned(), None),
        (200, true, valid.to_owned(), None),
        (
            200,
            false,
            valid.replace("\"accepted\":1", "\"accepted\":0"),
            None,
        ),
        (
            200,
            false,
            valid.replace("\"unattributed\":0", "\"unattributed\":2"),
            None,
        ),
        (
            200,
            false,
            " ".repeat(tollgate_store::wire::MAX_INGEST_REPORT_BYTES + 1),
            None,
        ),
        (200, false, "fixture-sensitive-invalid-json".into(), None),
    ] {
        let app = Router::new().route(
            "/v1/usage/ingest",
            post(move || {
                let body = body.clone();
                async move {
                    let mut response = Response::builder().status(status);
                    if range {
                        response = response.header(header::CONTENT_RANGE, "bytes 0-1/2");
                    }
                    response.body(Body::from(body)).unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = tollgate_client::HttpStore::new(format!("http://{address}")).unwrap();
        let result = client.ingest(&[event()], jiff::Timestamp::UNIX_EPOCH).await;
        match expected {
            Some(attribution) => assert_eq!(result.unwrap().unattributed, attribution),
            None => {
                let error = result.unwrap_err();
                assert!(error.is_retryable());
                assert!(!error.to_string().contains("fixture-sensitive-invalid-json"));
            }
        }
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }
}

#[tokio::test]
async fn lease_grants_reject_partial_success_responses() {
    use tollgate_core::{FencingToken, LeaseId};
    use tollgate_store::LeaseAllocator;
    for (status, range) in [(200, false), (206, false), (200, true)] {
        let body = serde_json::to_string(&tollgate_core::LeaseGrant {
            lease_id: LeaseId(1),
            account_id: AccountId(1),
            fencing_token: FencingToken(1),
            units: CostUnits(1),
            expires_at: jiff::Timestamp::from_second(30).unwrap(),
        })
        .unwrap();
        let app = Router::new().fallback(move || {
            let body = body.clone();
            async move {
                let mut response = Response::builder().status(status);
                if range {
                    response = response.header(header::CONTENT_RANGE, "bytes 0-1/2");
                }
                response.body(Body::from(body)).unwrap()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = tollgate_client::HttpStore::new(format!("http://{address}")).unwrap();
        let ttl = jiff::SignedDuration::from_secs(30);
        let now = jiff::Timestamp::UNIX_EPOCH;
        let grants = [
            client.acquire(AccountId(1), CostUnits(1), ttl, now).await,
            client
                .consolidate(
                    LeaseId(1),
                    FencingToken(1),
                    CostUnits(0),
                    CostUnits(1),
                    ttl,
                    now,
                )
                .await,
        ];
        for grant in grants {
            assert_eq!(grant.is_ok(), status == 200 && !range);
        }
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }
}

#[tokio::test]
async fn invalid_published_key_binding_has_a_structured_code() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(10),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    let snapshot = AccountSnapshot::builder(
        AccountId(1),
        Generation(1),
        AccountStatus::Active,
        jiff::Timestamp::from_second(100).unwrap(),
        PermissionBits::bit(0),
        ResolvedLimits::new(1),
        Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
    )
    .key_id(KeyId(404))
    .build();
    let app = tollgate_server::router(tollgate_server::ServerState {
        store,
        clock: Arc::new(ManualClock::new(jiff::Timestamp::UNIX_EPOCH)),
        security: common::security(),
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/admin/snapshots/{}", Principal(1)))
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", common::OPERATOR),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"snapshot":snapshot}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let problem: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(problem["code"], "invalid-credential-binding");
}

#[test]
fn old_consumers_ignore_additive_attribution_fields() {
    #[derive(serde::Deserialize)]
    struct OldReport {
        accepted: u64,
        duplicate: u64,
        rejected: u64,
    }
    let value = serde_json::to_value(IngestReport {
        accepted: 2,
        duplicate: 1,
        rejected: 0,
        unattributed: Some(1),
    })
    .unwrap();
    let old: OldReport = serde_json::from_value(value).unwrap();
    assert_eq!((old.accepted, old.duplicate, old.rejected), (2, 1, 0));
}
