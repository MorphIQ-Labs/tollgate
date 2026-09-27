//! Readiness tells the truth about the store behind it (INVARIANTS.md GL-10).
//!
//! A server whose source of truth is unreachable must not attract traffic,
//! and — since GL-36 — must also say *why* rather than only answering 503.

mod common;

use std::sync::Arc;

use tollgate_store::{StoreError, SystemClock};

use tollgate_server::{ServerState, router};

#[path = "../../tollgate-store/tests/support/delegating.rs"]
mod delegating;
use delegating::{DelegatingStore, RejectingStore, rejecting};

/// A backend that answers only `ping`, and answers it however the test says.
/// Everything else rejects: a readiness probe must not depend on it, and the
/// panic is that assertion. Delegating the rest to a real store would delete
/// the claim this file exists to make.
fn ping_only(healthy: bool) -> DelegatingStore<RejectingStore> {
    rejecting("a readiness probe must not touch the store")
        // The probe does reach the credential projection, and must find it
        // empty rather than unreachable.
        .on_active_keys_page(|_, now, after, limit| async move {
            tollgate_store::KeyPage::try_new(0, now, after, limit, vec![], None)
        })
        .on_ping(move |_| async move {
            if healthy {
                Ok(())
            } else {
                Err(StoreError(
                    "store unreachable: password=fixture-readiness-sensitive-70".into(),
                ))
            }
        })
}

async fn readyz_status(healthy: bool) -> axum::http::StatusCode {
    use tower::ServiceExt as _;

    let app = router(ServerState {
        security: common::security(),
        issuer: None,
        store: Arc::new(ping_only(healthy)),
        clock: Arc::new(SystemClock),
    });
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/readyz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    response.status()
}

/// The failing half: a store that cannot answer must not read as ready.
/// Fail-closed correctness must not masquerade as availability.
#[tokio::test]
async fn readyz_is_503_when_the_store_cannot_answer() {
    let capture = common::EventCapture::default();
    let status = capture.during(readyz_status(false)).await;
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    let events = capture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].level, tracing::Level::WARN);
    assert_eq!(events[0].fields["operation"], "readiness");
    assert_eq!(events[0].fields["code"], "storage");
    assert!(!format!("{events:?}").contains("fixture-readiness-sensitive-70"));
}

/// Bare routers have no maintenance owner and cannot advertise readiness.
/// The running-server tests pin the healthy 200 converse.
#[tokio::test]
async fn a_router_without_maintenance_cannot_advertise_readiness() {
    assert_eq!(
        readyz_status(true).await,
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
}
