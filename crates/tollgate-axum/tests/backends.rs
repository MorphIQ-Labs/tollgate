mod support;

use axum::{Router, body::Body, http::StatusCode};
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};
use support::*;
use tollgate_admission::NoGate;
use tollgate_auth::{CredentialVerifier, HmacRegistry};
use tollgate_axum::{AdapterConfig, BearerAuth, BufferedResponse, Tollgate, Validated};
use tollgate_client::{
    BearerToken, Clock, HttpStore, HttpStoreConfig, InstanceRuntime, StaticBearer,
};
use tollgate_core::{AccountId, CostUnits, PermissionBits};
use tollgate_server::security::{ControlIdentity, Role, SecurityPolicy, ServerSecurity};
use tower::ServiceExt;

#[tokio::test]
async fn http_store_runtime_executes_from_local_state_and_drains_committed_usage() {
    let (_, previous, store, clock) = fixture().await;
    previous.shutdown().await.unwrap();
    // Public fixture credentials, never deployment defaults.
    let key = "adapter-loopback-fixture-instance";
    let registry = Arc::new(HmacRegistry::new(b"adapter-loopback-fixture-secret"));
    registry.install_credentials([key.as_bytes()]);
    let identity = registry.verify(key.as_bytes()).unwrap().principal;
    let security = ServerSecurity::new(
        SecurityPolicy::new()
            .with_bearer(
                registry,
                [(
                    identity,
                    ControlIdentity::new("adapter-test", Role::Instance).unwrap(),
                )],
            )
            .unwrap(),
        None,
    )
    .unwrap();
    let server = tollgate_server::router(tollgate_server::ServerState {
        store: store.clone(),
        clock: clock.clone(),
        security,
        issuer: None,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });
    let backend = HttpStore::with_config(
        format!("http://{address}"),
        HttpStoreConfig {
            bearer: Some(StaticBearer::new(BearerToken::new(key).unwrap())),
            ..Default::default()
        },
    )
    .unwrap();
    let (runtime, handle) = InstanceRuntime::spawn(
        backend.clone(),
        backend.clone(),
        backend,
        clock.clone(),
        runtime_config(8),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !handle.readiness(clock.now()).is_ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let adapter = Tollgate::new(AdapterConfig {
        runtime: handle,
        authenticator: BearerAuth::new(Arc::new(Verifier)),
        clock,
        request_ids: TestIds::default(),
        capacity: NoGate,
    });
    let app = Router::new().route(
        "/",
        adapter.post(
            Op,
            PermissionBits::bit(0),
            || Ok(Validated::new((), 3)),
            |(), _| async { BufferedResponse::bytes(StatusCode::OK, "ok") },
        ),
    );
    let mut future = std::pin::pin!(app.oneshot(request(Body::empty())));
    // A transport round trip cannot complete in this one synchronous poll.
    let Poll::Ready(response) = future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("admission tried to wait on the remote control plane");
    };
    assert_eq!(response.unwrap().status(), StatusCode::OK);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(3));
    server_task.abort();
    let _ = server_task.await;
}
