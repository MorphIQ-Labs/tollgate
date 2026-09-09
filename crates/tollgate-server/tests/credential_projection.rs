mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use jiff::Timestamp;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tollgate_auth::HmacRegistry;
use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits, KeyId};
use tollgate_server::{ServerState, router};
use tollgate_store::{
    AccountConfig, GrantPolicy, KeyDirectory, KeyRecord, KeySource, ManualClock, MemoryStore,
};
use tower::ServiceExt;

const SECRET: &[u8] = b"fixture-http-key-projection-hmac-secret-108";
fn t(second: i64) -> Timestamp {
    Timestamp::from_second(second).unwrap()
}

#[tokio::test]
async fn only_instances_receive_active_keys_at_the_server_clock_without_caching() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(100),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    let issuer = HmacRegistry::new(SECRET);
    let mut issued = Vec::new();
    for (id, not_after) in [(1, None), (2, Some(t(100))), (3, Some(t(101))), (4, None)] {
        let key = issuer.mint(KeyId(id)).unwrap();
        store
            .insert_key(KeyRecord {
                key_id: key.key_id,
                account_id: AccountId(1),
                principal: key.principal,
                digest: key.digest,
                not_after,
            })
            .await
            .unwrap();
        issued.push(key);
    }
    store.revoke_key(KeyId(4), t(99)).await.unwrap();
    let clock = Arc::new(ManualClock::new(t(100)));
    let app = router(ServerState {
        store: store.clone(),
        clock: clock.clone(),
        security: common::security(),
    });
    for (credential, expected) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(common::OPERATOR), StatusCode::FORBIDDEN),
        (Some(common::INSTANCE), StatusCode::OK),
    ] {
        let mut request = Request::builder().uri("/v1/keys");
        if let Some(token) = credential {
            request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        if expected != StatusCode::OK {
            continue;
        }
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let keys = body["keys"].as_array().unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0]["principal"], issued[0].principal.to_string());
        assert_eq!(
            keys[0]["digest"],
            json!(
                issued[0]
                    .digest
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            )
        );
        assert_eq!(keys[1]["principal"], issued[2].principal.to_string());
        assert_eq!(keys[1]["not_after"], json!(t(101)));
        assert!(keys[0].get("account_id").is_none() && keys[0]["key_id"] == json!(KeyId(1)));
        assert!(keys[0].get("secret").is_none());
    }
    let query = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/keys?active_at=0")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", common::INSTANCE),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(query.status(), StatusCode::BAD_REQUEST);
    let problem: serde_json::Value =
        serde_json::from_slice(&query.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(problem["code"], "invalid-query");
    clock.set(t(101));
    store.revoke_key(KeyId(1), t(101)).await.unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/keys")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", common::INSTANCE),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["keys"], json!([]));
    assert_eq!(body["as_of"], json!(t(101)));
    assert_eq!(body["next_after"], json!(null));
}

#[tokio::test]
async fn http_projection_refuses_partial_ambiguous_or_unsuccessful_responses() {
    let response = Arc::new(Mutex::new((
        StatusCode::OK,
        axum::http::HeaderMap::new(),
        String::new(),
    )));
    let handler = response.clone();
    let app = axum::Router::new().route(
        "/v1/keys",
        axum::routing::get(move || {
            let handler = handler.clone();
            async move { handler.lock().unwrap().clone() }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async { stopped.await.unwrap() })
            .await
            .unwrap();
    });
    let http = common::http(format!("http://{address}"));
    let key = HmacRegistry::new(SECRET).mint(KeyId(1)).unwrap();
    let record = json!({"key_id":key.key_id, "principal":key.principal,
        "digest":key.digest.iter().map(|b| format!("{b:02x}")).collect::<String>(), "not_after":null});
    let envelope = |keys| json!({"revision":1,"as_of":t(100),"keys":keys,"next_after":null});
    let set = |status, body: String| {
        *response.lock().unwrap() = (status, axum::http::HeaderMap::new(), body);
    };
    let limit = tollgate_store::DEFAULT_KEY_PAGE_LIMIT;
    set(
        StatusCode::OK,
        envelope(json!([record.clone()])).to_string(),
    );
    let keys = http.active_keys_page(t(0), None, limit).await.unwrap();
    assert_eq!(keys.as_of(), t(100)); // caller cannot backdate the server
    assert_eq!(keys.records().len(), 1);
    assert_eq!(keys.records()[0].digest, key.digest);
    let mut missing_expiry = record.clone();
    missing_expiry.as_object_mut().unwrap().remove("not_after");
    let mut wrong_digest = record.clone();
    wrong_digest["digest"] = json!("0".repeat(64));
    let mut missing_cursor = envelope(json!([]));
    missing_cursor.as_object_mut().unwrap().remove("next_after");
    let mut nonadvancing = envelope(json!([record.clone()]));
    nonadvancing["next_after"] = json!(KeyId(0));
    let mut maximal = envelope(json!([])).to_string();
    maximal.push_str(&" ".repeat(tollgate_store::wire::MAX_KEYS_BODY_BYTES - maximal.len()));
    set(StatusCode::OK, maximal);
    assert!(http.active_keys_page(t(100), None, limit).await.is_ok());
    for bad in [
        "{\"keys\":[".to_owned(),
        "{}".to_owned(),
        envelope(json!([record.clone(), record.clone()])).to_string(),
        envelope(json!([missing_expiry])).to_string(),
        envelope(json!([wrong_digest])).to_string(),
        missing_cursor.to_string(),
        nonadvancing.to_string(),
        " ".repeat(tollgate_store::wire::MAX_KEYS_BODY_BYTES + 1),
    ] {
        set(StatusCode::OK, bad);
        assert!(http.active_keys_page(t(100), None, limit).await.is_err());
    }
    for code in [
        "authentication-required",
        "scope-forbidden",
        "invalid-query",
        "invalid-limit",
    ] {
        set(
            StatusCode::BAD_REQUEST,
            json!({"status":400,"code":code,"title":"fixture-sensitive-error"}).to_string(),
        );
        let error = http
            .active_keys_page(t(100), None, limit)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(code));
        assert!(!error.contains("fixture-sensitive-error"));
    }
    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::NO_CONTENT,
        StatusCode::PARTIAL_CONTENT,
        StatusCode::CREATED,
    ] {
        set(status, envelope(json!([])).to_string());
        assert!(
            http.active_keys_page(t(100), None, limit).await.is_err(),
            "{status}"
        );
    }
    set(StatusCode::OK, envelope(json!([])).to_string());
    response
        .lock()
        .unwrap()
        .1
        .insert(header::CONTENT_RANGE, "bytes 0-1/2".parse().unwrap());
    assert!(http.active_keys_page(t(100), None, limit).await.is_err());
    set(StatusCode::OK, envelope(json!([])).to_string());
    assert!(
        http.active_keys_page(t(100), None, limit)
            .await
            .unwrap()
            .records()
            .is_empty()
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn key_query_errors_are_structured_and_authentication_precedes_input_validation() {
    let app = router(ServerState {
        store: MemoryStore::new(GrantPolicy::default()).unwrap(),
        clock: Arc::new(ManualClock::new(t(100))),
        security: common::security(),
    });
    for (query, status, code) in [
        ("limit=0", StatusCode::UNPROCESSABLE_ENTITY, "invalid-limit"),
        (
            "limit=4097",
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid-limit",
        ),
        ("limit=no", StatusCode::BAD_REQUEST, "invalid-query"),
        ("after=no", StatusCode::BAD_REQUEST, "invalid-query"),
        ("active_at=0", StatusCode::BAD_REQUEST, "invalid-query"),
        ("limit=1&limit=2", StatusCode::BAD_REQUEST, "invalid-query"),
    ] {
        for authenticated in [false, true] {
            let mut request = Request::builder().uri(format!("/v1/keys?{query}"));
            if authenticated {
                request = request.header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", common::INSTANCE),
                );
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                if authenticated {
                    status
                } else {
                    StatusCode::UNAUTHORIZED
                }
            );
            if authenticated {
                assert_eq!(
                    response.headers()[header::CONTENT_TYPE],
                    "application/problem+json"
                );
                let problem: serde_json::Value = serde_json::from_slice(
                    &response.into_body().collect().await.unwrap().to_bytes(),
                )
                .unwrap();
                assert_eq!(problem["code"], code);
            }
        }
    }
}

#[tokio::test]
async fn chunked_key_responses_enforce_the_body_limit_without_content_length() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let cap = tollgate_store::wire::MAX_KEYS_BODY_BYTES;
    for length in [cap, cap + 1] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = vec![0; 8192];
            let mut used = 0;
            while !request[..used].windows(4).any(|w| w == b"\r\n\r\n") {
                let count = socket.read(&mut request[used..]).await?;
                if count == 0 {
                    return Ok::<_, std::io::Error>(());
                }
                used += count;
            }
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await?;
            let mut body = serde_json::to_vec(
                &json!({"revision":1,"as_of":t(100),"keys":[],"next_after":null}),
            )
            .unwrap();
            body.resize(length, b' '); // valid JSON even above the transport bound
            for chunk in body.chunks(16384) {
                socket
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await?;
                socket.write_all(chunk).await?;
                socket.write_all(b"\r\n").await?;
            }
            socket.write_all(b"0\r\n\r\n").await
        });
        let http = common::http(format!("http://{address}"));
        let result = http
            .active_keys_page(t(100), None, tollgate_store::DEFAULT_KEY_PAGE_LIMIT)
            .await;
        assert_eq!(result.is_ok(), length == cap);
        // A client that refuses an oversized stream may close before EOF.
        let _connection_result = server.await.unwrap();
    }
}

#[tokio::test]
async fn snapshot_siblings_also_refuse_partial_successful_reads() {
    use tollgate_core::{AccountSnapshot, CostTable, Generation, PermissionBits, ResolvedLimits};
    use tollgate_store::SnapshotSource;
    let snapshot = AccountSnapshot::builder(
        AccountId(1),
        Generation(1),
        AccountStatus::Active,
        t(200),
        PermissionBits::ALL,
        ResolvedLimits::new(64),
        Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
    )
    .build();
    let snapshot = serde_json::to_string(&snapshot).unwrap();
    let status = Arc::new(Mutex::new((StatusCode::OK, axum::http::HeaderMap::new())));
    let state = status.clone();
    let catalogue_state = status.clone();
    let app = axum::Router::new()
        .route(
            "/v1/snapshots/{principal}",
            axum::routing::get(move || {
                let state = state.clone();
                let snapshot = snapshot.clone();
                async move {
                    let (status, headers) = state.lock().unwrap().clone();
                    (status, headers, snapshot)
                }
            }),
        )
        .route(
            "/v1/snapshots",
            axum::routing::get(move || {
                let state = catalogue_state.clone();
                async move {
                    let (status, headers) = state.lock().unwrap().clone();
                    (status, headers, "{\"principals\":[]}")
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let http = common::http(format!("http://{address}"));
    let principal = tollgate_core::Principal(1);
    assert!(http.snapshot(principal).await.is_ok());
    assert_eq!(http.principals().await.unwrap(), Some(vec![]));
    for code in [
        StatusCode::PARTIAL_CONTENT,
        StatusCode::CREATED,
        StatusCode::NO_CONTENT,
    ] {
        status.lock().unwrap().0 = code;
        assert!(http.snapshot(principal).await.is_err());
        assert!(http.principals().await.is_err());
    }
    status.lock().unwrap().0 = StatusCode::OK;
    status
        .lock()
        .unwrap()
        .1
        .insert(header::CONTENT_RANGE, "bytes 0-1/2".parse().unwrap());
    assert!(http.snapshot(principal).await.is_err());
    assert!(http.principals().await.is_err());
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
