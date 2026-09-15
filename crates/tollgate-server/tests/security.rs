//! Actual HTTP/TLS boundaries, including existing keep-alive connections.
mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tollgate_auth::{CredentialVerifier, HmacRegistry};
use tollgate_client::{BearerToken, HttpStore, HttpStoreConfig, StaticBearer};
use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits, Principal};
use tollgate_server::security::{ControlIdentity, Role, SecurityPolicy, ServerSecurity};
use tollgate_server::transport::{TlsConfig, certificate_fingerprint};
use tollgate_server::{ServerState, router, serve};
use tollgate_store::{
    AccountConfig, Clock, GrantPolicy, LeaseAllocator, MemoryStore, SnapshotSource, SystemClock,
};
use tower::ServiceExt;

fn state(security: Arc<ServerSecurity>) -> ServerState<MemoryStore> {
    ServerState {
        store: MemoryStore::new(GrantPolicy::default()).unwrap(),
        clock: Arc::new(SystemClock),
        security,
        issuer: None,
    }
}

#[tokio::test]
async fn every_control_plane_route_requires_its_own_role_before_decoding() {
    let app = router(state(common::security()));
    for (method, path, expected_role) in [
        ("POST", "/v1/leases/acquire", Role::Instance),
        ("POST", "/v1/leases/release", Role::Instance),
        ("POST", "/v1/leases/consolidate", Role::Instance),
        ("POST", "/v1/leases/reclaim", Role::Instance),
        ("GET", "/v1/snapshots", Role::Instance),
        ("GET", "/v1/keys", Role::Instance),
        ("GET", "/v1/snapshots/invalid", Role::Instance),
        ("POST", "/v1/usage/ingest", Role::Instance),
        ("POST", "/v1/admin/accounts", Role::Operator),
        ("POST", "/v1/admin/accounts/invalid/deposit", Role::Operator),
        ("POST", "/v1/admin/accounts/invalid/status", Role::Operator),
        (
            "POST",
            "/v1/admin/accounts/invalid/capacity-class",
            Role::Operator,
        ),
        ("PUT", "/v1/admin/snapshots/invalid", Role::Operator),
        ("DELETE", "/v1/admin/snapshots/invalid", Role::Operator),
    ] {
        for token in [
            None,
            Some("bad-token"),
            Some(common::INSTANCE),
            Some(common::OPERATOR),
        ] {
            let mut request = Request::builder()
                .method(method)
                .uri(path)
                .header(header::CONTENT_TYPE, "application/json");
            if let Some(token) = token {
                request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::from("{invalid body")).unwrap())
                .await
                .unwrap();
            let expected = match token {
                None | Some("bad-token") => Some(StatusCode::UNAUTHORIZED),
                Some(common::INSTANCE) if expected_role == Role::Operator => {
                    Some(StatusCode::FORBIDDEN)
                }
                Some(common::OPERATOR) if expected_role == Role::Instance => {
                    Some(StatusCode::FORBIDDEN)
                }
                _ => None,
            };
            if let Some(expected) = expected {
                assert_eq!(response.status(), expected, "{method} {path}");
                assert_eq!(
                    response.headers()[header::CONTENT_TYPE],
                    "application/problem+json"
                );
                if expected == StatusCode::UNAUTHORIZED {
                    assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
                }
                let problem: Value = serde_json::from_slice(
                    &response.into_body().collect().await.unwrap().to_bytes(),
                )
                .unwrap();
                assert_eq!(
                    problem["code"],
                    if expected == StatusCode::UNAUTHORIZED {
                        "authentication-required"
                    } else {
                        "scope-forbidden"
                    }
                );
            } else {
                assert_ne!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
                assert_ne!(response.status(), StatusCode::FORBIDDEN, "{path}");
            }
        }
    }
    for (path, expected) in [
        ("/livez", StatusCode::OK),
        ("/readyz", StatusCode::SERVICE_UNAVAILABLE),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            expected
        );
    }
}

#[tokio::test]
async fn ambiguous_framing_and_forged_peer_headers_authenticate_nobody() {
    let app = router(state(common::security()));
    for headers in [
        vec![
            ("authorization", format!("Bearer {}", common::OPERATOR)),
            ("authorization", format!("Bearer {}", common::INSTANCE)),
        ],
        vec![("authorization", format!("Basic {}", common::OPERATOR))],
        vec![("authorization", format!("Bearer  {}", common::OPERATOR))],
        vec![
            ("x-forwarded-client-cert", "test-operator".into()),
            ("x-goog-authenticated-user-email", "test-operator".into()),
        ],
    ] {
        let mut request = Request::builder().method("POST").uri("/v1/admin/accounts");
        for (name, value) in headers {
            request = request.header(name, value);
        }
        assert_eq!(
            app.clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
}

fn replacement_policy(token: &str, role: Role) -> SecurityPolicy {
    let verifier = Arc::new(HmacRegistry::new(b"replacement-fixture-secret"));
    verifier.install_credentials([token.as_bytes()]);
    let principal = verifier.verify(token.as_bytes()).unwrap().principal;
    SecurityPolicy::new()
        .with_bearer(
            verifier,
            [(
                principal,
                ControlIdentity::new("replacement", role).unwrap(),
            )],
        )
        .unwrap()
}

#[tokio::test]
async fn bearer_rotation_changes_existing_connections_and_preserves_usage_for_retry() {
    let security = common::security();
    let server_state = state(Arc::clone(&security));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve(
        listener,
        server_state,
        Duration::from_secs(60),
        async {
            stopped.await.unwrap();
        },
    ));
    let bearer = StaticBearer::new(BearerToken::new(common::INSTANCE).unwrap());
    let http = HttpStore::with_config(
        format!("http://{address}"),
        HttpStoreConfig {
            bearer: Some(bearer.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(http.principals().await.unwrap().is_some());
    const REPLACEMENT: &str = "rotated-instance-credential-fixture";
    security
        .replace(replacement_policy(REPLACEMENT, Role::Instance), None)
        .unwrap();
    assert!(
        http.principals()
            .await
            .unwrap_err()
            .0
            .contains("authentication-required")
    );
    let error = tollgate_store::UsageSink::ingest(&*http, &[], SystemClock.now())
        .await
        .unwrap_err();
    assert!(error.is_retryable(), "rotation must not discard usage");
    bearer.replace(BearerToken::new(REPLACEMENT).unwrap());
    assert!(http.principals().await.unwrap().is_some());
    assert!(
        tollgate_store::UsageSink::ingest(&*http, &[], SystemClock.now())
            .await
            .is_ok()
    );
    security
        .replace(replacement_policy(REPLACEMENT, Role::Operator), None)
        .unwrap();
    assert!(
        tollgate_store::UsageSink::ingest(&*http, &[], SystemClock.now())
            .await
            .unwrap_err()
            .is_retryable()
    );
    bearer.revoke();
    assert!(http.principals().await.unwrap_err().0.contains("revoked"));
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn mtls_and_bearer_fund_and_settle_over_tls_and_revocation_affects_keepalive() {
    let certificates = common::certificates();
    let tls = TlsConfig::from_pem(
        certificates.server.as_bytes(),
        certificates.server_key.as_bytes(),
        Some(certificates.ca.as_bytes()),
    )
    .unwrap();
    let policy = common::policy()
        .with_certificate(
            certificate_fingerprint(certificates.client.as_bytes()).unwrap(),
            ControlIdentity::new("test-instance", Role::Instance).unwrap(),
        )
        .unwrap();
    let security = ServerSecurity::new(policy, Some(tls.clone())).unwrap();
    let server_state = state(Arc::clone(&security));
    server_state.store.create_account(AccountConfig {
        account_id: AccountId(1),
        initial_balance: CostUnits(1000),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    let store = Arc::clone(&server_state.store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve(
        listener,
        server_state,
        Duration::from_secs(60),
        async {
            stopped.await.unwrap();
        },
    ));
    let base = format!("https://{address}");
    let unmapped = HttpStore::with_config(
        &base,
        HttpStoreConfig {
            root_ca_pem: Some(certificates.ca.as_bytes().to_vec()),
            identity_pem: Some(
                format!(
                    "{}{}",
                    certificates.other_client, certificates.other_client_key
                )
                .into_bytes()
                .into(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        unmapped
            .principals()
            .await
            .unwrap_err()
            .0
            .contains("scope-forbidden"),
        "a second CA-trusted certificate must not inherit the mapped leaf's role"
    );
    let mtls = HttpStore::with_config(
        &base,
        HttpStoreConfig {
            root_ca_pem: Some(certificates.ca.as_bytes().to_vec()),
            identity_pem: Some(
                format!("{}{}", certificates.client, certificates.client_key)
                    .into_bytes()
                    .into(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    let bearer = HttpStore::with_config(
        &base,
        HttpStoreConfig {
            root_ca_pem: Some(certificates.ca.as_bytes().to_vec()),
            bearer: Some(StaticBearer::new(
                BearerToken::new(common::INSTANCE).unwrap(),
            )),
            ..Default::default()
        },
    )
    .unwrap();
    let dual_credential = StaticBearer::new(BearerToken::new(common::INSTANCE).unwrap());
    let both = HttpStore::with_config(
        &base,
        HttpStoreConfig {
            root_ca_pem: Some(certificates.ca.as_bytes().to_vec()),
            identity_pem: Some(
                format!("{}{}", certificates.client, certificates.client_key)
                    .into_bytes()
                    .into(),
            ),
            bearer: Some(dual_credential.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        both.principals().await.is_ok(),
        "matching evidence may authenticate together"
    );
    dual_credential.replace(BearerToken::new(common::OPERATOR).unwrap());
    assert!(
        both.principals()
            .await
            .unwrap_err()
            .0
            .contains("authentication-required"),
        "conflicting certificate and bearer identities must not select a preferred role"
    );
    let stalled = tokio::net::TcpStream::connect(address).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), bearer.principals())
            .await
            .unwrap()
            .is_ok(),
        "one stalled TLS peer cannot block another handshake"
    );
    drop(stalled);
    for http in [&mtls, &bearer] {
        let grant = http
            .acquire(
                AccountId(1),
                CostUnits(100),
                jiff::SignedDuration::from_secs(60),
                SystemClock.now(),
            )
            .await
            .unwrap();
        http.release(
            grant.lease_id,
            grant.fencing_token,
            grant.units,
            SystemClock.now(),
        )
        .await
        .unwrap();
        assert_eq!(store.balance(AccountId(1)), CostUnits(1000));
    }
    let probe = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(certificates.ca.as_bytes()).unwrap())
        .build()
        .unwrap();
    assert_eq!(
        probe
            .get(format!("{base}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert!(
        HttpStore::new(&base)
            .unwrap()
            .snapshot(Principal(1))
            .await
            .is_err(),
        "untrusted server certificate"
    );
    assert!(
        mtls.reconfigure(HttpStoreConfig {
            root_ca_pem: Some(b"invalid CA".to_vec()),
            ..Default::default()
        })
        .is_err()
    );
    assert!(
        mtls.principals().await.is_ok(),
        "invalid replacement preserves the working transport"
    );
    security.replace(common::policy(), Some(tls)).unwrap();
    assert!(
        mtls.principals()
            .await
            .unwrap_err()
            .0
            .contains("scope-forbidden")
    );
    assert!(bearer.principals().await.is_ok());
    // Rotate the listener certificate, trust root, and client identity while
    // background managers can keep holding this same Arc<HttpStore>.
    let replacement = common::certificates();
    let tls = TlsConfig::from_pem(
        replacement.server.as_bytes(),
        replacement.server_key.as_bytes(),
        Some(replacement.ca.as_bytes()),
    )
    .unwrap();
    let policy = common::policy()
        .with_certificate(
            certificate_fingerprint(replacement.client.as_bytes()).unwrap(),
            ControlIdentity::new("replacement-instance", Role::Instance).unwrap(),
        )
        .unwrap();
    security.replace(policy, Some(tls)).unwrap();
    assert!(
        mtls.principals()
            .await
            .unwrap_err()
            .0
            .contains("authentication-required"),
        "the replacement CA must revoke the old peer even on keep-alive"
    );
    assert!(
        mtls.reconfigure(HttpStoreConfig {
            root_ca_pem: Some(b"invalid CA".to_vec()),
            ..Default::default()
        })
        .is_err()
    );
    mtls.reconfigure(HttpStoreConfig {
        root_ca_pem: Some(replacement.ca.into_bytes()),
        identity_pem: Some(
            format!("{}{}", replacement.client, replacement.client_key)
                .into_bytes()
                .into(),
        ),
        ..Default::default()
    })
    .unwrap();
    assert!(mtls.principals().await.is_ok());
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn exposed_plaintext_is_rejected_before_the_server_starts() {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let error = serve(
        listener,
        state(common::security()),
        Duration::from_secs(5),
        std::future::pending(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn unsafe_client_urls_and_deadlines_are_rejected_without_io() {
    for base in [
        "http://example.com",
        "http://localhost.evil",
        "http://10.0.0.1",
        "ftp://localhost",
        "https://user:password@example.com",
        "https://example.com?token=secret",
        "https://example.com#fragment",
    ] {
        assert!(HttpStore::new(base).is_err(), "{base}");
    }
    for base in [
        "http://127.0.0.1:1",
        "http://[::1]:1",
        "http://localhost:1",
        "https://example.com",
    ] {
        assert!(HttpStore::new(base).is_ok(), "{base}");
    }
    for timeout in [Duration::ZERO, Duration::MAX] {
        assert!(
            HttpStore::with_timeouts("http://127.0.0.1", timeout, Duration::from_secs(1)).is_err()
        );
        assert!(
            HttpStore::with_timeouts("http://127.0.0.1", Duration::from_secs(1), timeout).is_err()
        );
    }
}

#[tokio::test]
async fn google_tokens_require_signature_issuer_audience_subject_and_live_expiry() {
    use tollgate_server::google::GoogleVerifier;
    use tollgate_store::ManualClock;
    let fixture: Value = serde_json::from_str(include_str!("fixtures/google-tokens.json")).unwrap();
    let jwks = serde_json::to_vec(&fixture["jwks"]).unwrap();
    let now = jiff::Timestamp::from_second(1_700_000_100).unwrap();
    let until = now
        .checked_add(jiff::SignedDuration::from_secs(300))
        .unwrap();
    let verifier =
        Arc::new(GoogleVerifier::from_jwks("https://control.example.test", &jwks, until).unwrap());
    let identity = ControlIdentity::new("cloud-run-service", Role::Instance).unwrap();
    let security = ServerSecurity::new(
        SecurityPolicy::new()
            .with_bearer(
                verifier.clone(),
                [(GoogleVerifier::principal("1234567890"), identity)],
            )
            .unwrap(),
        None,
    )
    .unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let mut server = state(Arc::clone(&security));
    server.clock = clock.clone();
    let app = router(server);
    for (name, token) in fixture["tokens"].as_object().unwrap() {
        let token = token.as_str().unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/snapshots")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if name == "valid" {
                StatusCode::OK
            } else {
                StatusCode::UNAUTHORIZED
            },
            "{name}"
        );
    }
    let valid = fixture["tokens"]["valid"].as_str().unwrap();
    let mut corrupted = valid.as_bytes().to_vec();
    let signature = valid.rfind('.').unwrap() + 1;
    corrupted[signature] = if corrupted[signature] == b'A' {
        b'B'
    } else {
        b'A'
    };
    assert!(verifier.verify(&corrupted).is_none());
    let proof = verifier.verify(valid.as_bytes()).unwrap();
    assert_eq!(
        proof.reusable_until,
        Some(until),
        "key-set freshness bounds long-lived tokens"
    );
    clock.set(until);
    assert_eq!(
        app.oneshot(
            Request::builder()
                .uri("/v1/snapshots")
                .header(header::AUTHORIZATION, format!("Bearer {valid}"))
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap()
        .status(),
        StatusCode::UNAUTHORIZED
    );

    let empty_roles = SecurityPolicy::new().with_bearer(verifier, []).unwrap();
    let security = ServerSecurity::new(empty_roles, None).unwrap();
    let mut server = state(security);
    server.clock = Arc::new(ManualClock::new(now));
    assert_eq!(
        router(server)
            .oneshot(
                Request::builder()
                    .uri("/v1/snapshots")
                    .header(header::AUTHORIZATION, format!("Bearer {valid}"))
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn a_hung_credential_provider_is_inside_the_http_deadline() {
    struct Hung;
    #[async_trait::async_trait]
    impl tollgate_client::BearerProvider for Hung {
        async fn token(&self) -> Result<BearerToken, tollgate_store::StoreError> {
            std::future::pending().await
        }
    }
    let http = HttpStore::with_config(
        "http://127.0.0.1:1",
        HttpStoreConfig {
            request_timeout: Duration::from_millis(10),
            bearer: Some(Arc::new(Hung)),
            ..Default::default()
        },
    )
    .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(1), http.principals())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.0.contains("credential deadline expired"));
}

#[tokio::test]
async fn control_plane_redirects_are_not_followed() {
    let (contacted, mut contacts) = tokio::sync::mpsc::unbounded_channel();
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_address = target.local_addr().unwrap();
    let target_task = tokio::spawn(async move {
        let accepted = target.accept().await.unwrap();
        contacted.send(()).unwrap();
        drop(accepted);
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/v1/snapshots",
        axum::routing::get(move || async move {
            axum::response::Redirect::temporary(&format!("http://{target_address}/steal"))
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    assert!(
        common::http(format!("http://{address}"))
            .principals()
            .await
            .is_err()
    );
    assert!(contacts.try_recv().is_err());
    target_task.abort();
    server.abort();
}

#[tokio::test]
async fn every_admin_mutation_logs_its_actor_and_the_backend_receipt() {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::layer::SubscriberExt;
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<HashMap<String, String>>>>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() != "tollgate::audit" {
                return;
            }
            #[derive(Default)]
            struct Fields(HashMap<String, String>);
            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0.insert(field.name().into(), format!("{value:?}"));
                }
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    self.0.insert(field.name().into(), value.into());
                }
            }
            let mut fields = Fields::default();
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
    }
    let capture = Capture::default();
    let app = router(state(common::security()));
    let account = AccountId(1).to_string();
    let principal = Principal(1).to_string();
    let snapshot = tollgate_core::AccountSnapshot::builder(
        AccountId(1),
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
    .build();
    let requests = [
        (
            "POST",
            "/v1/admin/accounts".to_owned(),
            json!({"account_id": account, "initial_balance": 100, "status": "Active"}),
        ),
        (
            "POST",
            format!("/v1/admin/accounts/{account}/deposit"),
            json!({"units": 25}),
        ),
        (
            "PUT",
            format!("/v1/admin/snapshots/{principal}"),
            json!({"snapshot": snapshot}),
        ),
        (
            "POST",
            format!("/v1/admin/accounts/{account}/status"),
            json!({"status": "Suspended"}),
        ),
        (
            "POST",
            format!("/v1/admin/accounts/{account}/capacity-class"),
            json!({"capacity_class": "BestEffort"}),
        ),
        (
            "DELETE",
            format!("/v1/admin/snapshots/{principal}"),
            Value::Null,
        ),
    ];
    async {
        for (method, path, body) in requests {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header(
                            header::AUTHORIZATION,
                            format!("Bearer {}", common::OPERATOR),
                        )
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(response.status().is_success(), "{}", response.status());
        }
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/admin/accounts/{}/deposit", AccountId(999)))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", common::OPERATOR),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"units":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    .with_subscriber(tracing_subscriber::registry().with(capture.clone()))
    .await;
    let events = capture.0.lock().unwrap();
    let confirmed: Vec<_> = events
        .iter()
        .filter(|event| {
            event
                .get("outcome")
                .is_some_and(|outcome| outcome == "confirmed")
        })
        .collect();
    assert_eq!(confirmed.len(), 6);
    for event in &confirmed {
        assert_eq!(event["actor"], "test-operator");
        assert!(
            event.contains_key("before") && event.contains_key("after") && event.contains_key("at")
        );
        assert_eq!(
            events
                .iter()
                .filter(|other| other.get("operation_id") == event.get("operation_id"))
                .count(),
            2
        );
    }
    let deposit = confirmed
        .iter()
        .find(|event| event["action"] == "deposit")
        .unwrap();
    assert!(deposit["before"].contains("100"));
    assert!(deposit["after"].contains("125"));
    let removed = confirmed
        .iter()
        .find(|event| event["action"] == "remove_snapshot")
        .unwrap();
    assert!(removed["before"].contains("revoked: false"));
    assert!(removed["after"].contains("revoked: true"));
    let failed: Vec<_> = events
        .iter()
        .filter(|event| event.get("outcome").is_some_and(|v| v == "failed"))
        .collect();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["actor"], "test-operator");
    assert!(!failed[0].contains_key("before") && !failed[0].contains_key("after"));
    let rendered = format!("{events:?}");
    assert!(!rendered.contains(common::INSTANCE) && !rendered.contains(common::OPERATOR));
}
