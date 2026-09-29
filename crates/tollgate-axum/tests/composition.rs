mod support;

use axum::{
    Router,
    body::{Body, Bytes, HttpBody, to_bytes},
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
};
use serde::Deserialize;
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use support::*;
use tollgate_admission::NoGate;
use tollgate_axum::{AdapterConfig, BearerAuth, BufferedResponse, Tollgate, Validated};
use tollgate_client::Clock;
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, Generation, PermissionBits,
    PolicyRevision, Principal, PublishableSnapshot, ResolvedLimits,
};
use tower::ServiceExt;

struct PausedBody {
    started: Arc<tokio::sync::Notify>,
    receive: tokio::sync::oneshot::Receiver<Bytes>,
    done: bool,
}
impl HttpBody for PausedBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Infallible>>> {
        if self.done {
            return Poll::Ready(None);
        }
        self.started.notify_one();
        match Pin::new(&mut self.receive).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(bytes) => {
                self.done = true;
                Poll::Ready(bytes.ok().map(|bytes| Ok(http_body::Frame::data(bytes))))
            }
        }
    }
}
fn paused() -> (
    Body,
    Arc<tokio::sync::Notify>,
    tokio::sync::oneshot::Sender<Bytes>,
) {
    let started = Arc::new(tokio::sync::Notify::new());
    let (send, receive) = tokio::sync::oneshot::channel();
    (
        Body::new(PausedBody {
            started: started.clone(),
            receive,
            done: false,
        }),
        started,
        send,
    )
}
#[derive(Deserialize)]
struct Input {
    items: u64,
}
fn route(adapter: &Adapter, calls: Arc<AtomicUsize>) -> axum::routing::MethodRouter {
    adapter.post_json(
        Op,
        PermissionBits::bit(0),
        limits(100),
        |input: Input| Ok(Validated::new((), input.items)),
        move |(), charge| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { BufferedResponse::json(StatusCode::OK, &charge.units_charged.get()) }
        },
    )
}
async fn code(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["units_charged"], 0);
    value["code"].as_str().unwrap().to_owned()
}

#[tokio::test(start_paused = true)]
async fn policy_republication_during_body_read_does_not_reprice_the_pinned_request() {
    let (adapter, runtime, store, _) = fixture().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route("/", route(&adapter, calls.clone()));
    let (body, started, send) = paused();
    let task = tokio::spawn(app.clone().oneshot(request(body)));
    started.notified().await;
    let snapshot = AccountSnapshot::builder(
        AccountId(1),
        Generation(2),
        AccountStatus::Active,
        time(1000),
        PermissionBits::bit(0),
        ResolvedLimits::new(100),
        Arc::new(
            CostTable::builder(CostUnits::ZERO, CostUnits::ZERO)
                .weight(&Op, CostUnits(2))
                .build(),
        ),
    )
    .policy_revision(PolicyRevision([2; 32]))
    .build();
    store
        .publish_snapshot(
            Principal(1),
            PublishableSnapshot::try_new(Arc::new(snapshot)).unwrap(),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if adapter
                .runtime()
                .begin(Principal(1), PermissionBits::bit(0), time(100))
                .unwrap()
                .generation()
                == Generation(2)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    send.send(Bytes::from_static(br#"{"items":3}"#)).unwrap();
    let response = task.await.unwrap().unwrap();
    assert_eq!(to_bytes(response.into_body(), 100).await.unwrap(), "3");
    // Let the one-slot writer drain before a second independent request.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let response = app
        .oneshot(request(Body::from(r#"{"items":3}"#)))
        .await
        .unwrap();
    assert_eq!(to_bytes(response.into_body(), 100).await.unwrap(), "6");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 2);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(9));
    assert_eq!(
        store
            .settled_event(tollgate_core::RequestId(1))
            .unwrap()
            .policy_revision,
        PolicyRevision::UNSTATED
    );
    assert_eq!(
        store
            .settled_event(tollgate_core::RequestId(2))
            .unwrap()
            .policy_revision,
        PolicyRevision([2; 32])
    );
}

#[tokio::test(start_paused = true)]
async fn expiry_during_body_read_and_outer_timeout_before_start_charge_nothing() {
    for expire in [false, true] {
        let (adapter, runtime, store, clock) = fixture().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route("/", route(&adapter, calls.clone()));
        let (body, started, send) = paused();
        let task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(100), app.oneshot(request(body))).await
        });
        started.notified().await;
        if expire {
            clock.set(time(1000));
            send.send(Bytes::from_static(br#"{"items":3}"#)).unwrap();
            let response = task.await.unwrap().unwrap().unwrap();
            assert_eq!(code(response).await, "policy-stale");
        } else {
            assert!(task.await.unwrap().is_err());
            drop(send);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(adapter.runtime().recorder().try_reserve().is_ok());
        assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
        assert_eq!(store.usage_recorded(AccountId(1)), CostUnits::ZERO);
    }
}

struct CommitClock(AtomicUsize);
impl Clock for CommitClock {
    fn now(&self) -> jiff::Timestamp {
        // begin/admit use valid funding; only commit sees the lease lapse.
        time(if self.0.fetch_add(1, Ordering::SeqCst) < 2 {
            100
        } else {
            400
        })
    }
}
#[tokio::test(start_paused = true)]
async fn funding_expiry_at_commit_never_constructs_the_handler() {
    let (base, runtime, store, _) = fixture().await;
    let adapter = Tollgate::new(AdapterConfig {
        runtime: base.runtime().clone(),
        authenticator: BearerAuth::new(Arc::new(Verifier)),
        clock: Arc::new(CommitClock(AtomicUsize::new(0))),
        request_ids: TestIds::default(),
        capacity: NoGate,
    });
    let app = Router::new().route(
        "/",
        adapter.post(
            Op,
            PermissionBits::bit(0),
            || Ok(Validated::new((), 3)),
            |(), _| async { panic!("expired funding must not execute") },
        ),
    );
    assert_eq!(
        code(app.oneshot(request(Body::empty())).await.unwrap()).await,
        "funding-expired-at-start"
    );
    assert!(adapter.runtime().recorder().try_reserve().is_ok());
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 0);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits::ZERO);
}

async fn outer_timeout(req: Request, next: Next) -> axum::response::Response {
    use axum::response::IntoResponse;
    tokio::time::timeout(Duration::from_millis(50), next.run(req))
        .await
        .unwrap_or_else(|_| StatusCode::GATEWAY_TIMEOUT.into_response())
}
#[tokio::test(start_paused = true)]
async fn canceling_timeout_after_start_records_once_and_unmetered_routes_bypass_admission() {
    let (adapter, runtime, store, _) = fixture().await;
    let app = Router::new()
        .route(
            "/",
            adapter.post(
                Op,
                PermissionBits::bit(0),
                || Ok(Validated::new((), 3)),
                |(), _| std::future::pending(),
            ),
        )
        .route_layer(middleware::from_fn(outer_timeout))
        .route("/health", axum::routing::get(|| async { "ok" }));
    let response = app.clone().oneshot(request(Body::empty())).await.unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    for (uri, status) in [
        ("/health", StatusCode::OK),
        ("/missing", StatusCode::NOT_FOUND),
        ("/", StatusCode::METHOD_NOT_ALLOWED),
    ] {
        let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        assert_eq!(app.clone().oneshot(req).await.unwrap().status(), status);
    }
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(3));
}

#[tokio::test(start_paused = true)]
async fn strict_refuses_expired_funding_while_elastic_records_explicit_overage() {
    use tollgate_core::{CapacityClass, EnforcementMode, UsageSource};
    for elastic in [false, true] {
        let mode = if elastic {
            EnforcementMode::Elastic {
                overage_cap: CostUnits(100),
            }
        } else {
            EnforcementMode::Strict
        };
        let (base, runtime, store, _) = fixture_mode(NoGate, 8, CapacityClass::Assured, mode).await;
        let adapter = Tollgate::new(AdapterConfig {
            runtime: base.runtime().clone(),
            authenticator: BearerAuth::new(Arc::new(Verifier)),
            clock: Arc::new(CommitClock(AtomicUsize::new(0))),
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
        let response = app.oneshot(request(Body::empty())).await.unwrap();
        assert_eq!(
            response.status(),
            if elastic {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        );
        assert_eq!(
            runtime.shutdown().await.unwrap().usage.unwrap().accepted,
            u64::from(elastic)
        );
        if elastic {
            let event = store.settled_event(tollgate_core::RequestId(1)).unwrap();
            assert!(matches!(event.source, UsageSource::Overage));
            assert_eq!(event.units, CostUnits(3));
        } else {
            assert_eq!(store.usage_recorded(AccountId(1)), CostUnits::ZERO);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn revocation_during_read_preserves_pinned_identity_and_refuses_the_next_request() {
    let (adapter, runtime, store, _) = fixture().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route("/", route(&adapter, calls.clone()));
    let (body, started, send) = paused();
    let task = tokio::spawn(app.clone().oneshot(request(body)));
    started.notified().await;
    store.remove_snapshot(Principal(1));
    tokio::time::timeout(Duration::from_millis(500), async {
        while adapter
            .runtime()
            .begin(Principal(1), PermissionBits::bit(0), time(100))
            .is_ok()
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    send.send(Bytes::from_static(br#"{"items":3}"#)).unwrap();
    assert_eq!(task.await.unwrap().unwrap().status(), StatusCode::OK);
    let response = app
        .oneshot(request(Body::from(r#"{"items":3}"#)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(code(response).await, "unknown-principal");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(3));
}

#[tokio::test(start_paused = true)]
async fn shutdown_drains_a_running_charge_and_refuses_new_execution() {
    let (adapter, runtime, store, _) = fixture().await;
    let started = Arc::new(tokio::sync::Notify::new());
    let finish = Arc::new(tokio::sync::Notify::new());
    let start_witness = started.clone();
    let finish_witness = finish.clone();
    let app = Router::new().route(
        "/",
        adapter.post(
            Op,
            PermissionBits::bit(0),
            || Ok(Validated::new((), 3)),
            move |(), _| {
                let started = start_witness.clone();
                let finish = finish_witness.clone();
                async move {
                    started.notify_one();
                    finish.notified().await;
                    BufferedResponse::bytes(StatusCode::OK, "done")
                }
            },
        ),
    );
    let request_task = tokio::spawn(app.clone().oneshot(request(Body::empty())));
    started.notified().await;
    let shutdown = tokio::spawn(runtime.shutdown());
    for _ in 0..100 {
        if adapter.runtime().recorder().is_closed() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(adapter.runtime().recorder().is_closed());
    assert_eq!(
        code(app.oneshot(request(Body::empty())).await.unwrap()).await,
        "accounting-busy"
    );
    finish.notify_one();
    assert_eq!(
        request_task.await.unwrap().unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(shutdown.await.unwrap().unwrap().usage.unwrap().accepted, 1);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(3));
}

struct TwoTenants;
impl tollgate_auth::CredentialVerifier for TwoTenants {
    fn verify(&self, key: &[u8]) -> Option<tollgate_auth::Verified> {
        let principal = match key {
            b"demo-fixture-key" => Principal(1),
            b"demo-second-fixture-key" => Principal(2),
            _ => return None,
        };
        Some(tollgate_auth::Verified::indefinite(principal))
    }
}
#[tokio::test(start_paused = true)]
async fn one_accounts_concurrency_limit_does_not_block_another_account() {
    use std::num::NonZeroU32;
    use tollgate_core::CapacityClass;
    let (_, previous, store, clock) = fixture().await;
    previous.shutdown().await.unwrap();
    store.create_account(tollgate_store::AccountConfig {
        account_id: AccountId(2),
        initial_balance: CostUnits(1000),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    for tenant in 1..=2 {
        let snapshot = AccountSnapshot::builder(
            AccountId(tenant),
            Generation(2),
            AccountStatus::Active,
            time(1000),
            PermissionBits::bit(0),
            ResolvedLimits::new(100)
                .with_concurrency(NonZeroU32::new(1).unwrap(), None)
                .unwrap(),
            Arc::new(
                CostTable::builder(CostUnits::ZERO, CostUnits::ZERO)
                    .weight(&Op, CostUnits(1))
                    .build(),
            ),
        )
        .build();
        store
            .publish_snapshot(
                Principal(tenant),
                PublishableSnapshot::try_new(Arc::new(snapshot)).unwrap(),
            )
            .unwrap();
    }
    let mut config = runtime_config(8);
    config.snapshots.principals =
        tollgate_client::TrackedPrincipals::Fixed(vec![Principal(1), Principal(2)]);
    let (runtime, handle) = tollgate_client::InstanceRuntime::spawn(
        store.clone(),
        store.clone(),
        store.clone(),
        clock.clone(),
        config,
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
        authenticator: BearerAuth::new(Arc::new(TwoTenants)),
        clock,
        request_ids: TestIds::default(),
        capacity: NoGate,
    });
    let started = Arc::new(tokio::sync::Notify::new());
    let witness = started.clone();
    let app = Router::new().route(
        "/",
        adapter.post(
            Op,
            PermissionBits::bit(0),
            || Ok(Validated::new((), 3)),
            move |(), _| {
                let witness = witness.clone();
                async move {
                    witness.notify_one();
                    std::future::pending().await
                }
            },
        ),
    );
    let first = tokio::spawn(app.clone().oneshot(request(Body::empty())));
    started.notified().await;
    assert_eq!(
        code(app.clone().oneshot(request(Body::empty())).await.unwrap()).await,
        "concurrency-limited"
    );
    let mut second_request = request(Body::empty());
    second_request.headers_mut().insert(
        "authorization",
        "Bearer demo-second-fixture-key".parse().unwrap(),
    );
    let second = tokio::spawn(app.oneshot(second_request));
    started.notified().await;
    for task in [first, second] {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 2);
    for tenant in 1..=2 {
        assert_eq!(store.usage_recorded(AccountId(tenant)), CostUnits(3));
    }
}

#[tokio::test(start_paused = true)]
async fn wrapper_refuses_identity_input_backpressure_and_funding_without_running_work() {
    use tollgate_core::CapacityClass;
    let (adapter, runtime, store, _) = fixture_with(NoGate, 8, CapacityClass::Assured).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let witness = calls.clone();
    let app = Router::new().route(
        "/",
        adapter.post_json(
            Op,
            PermissionBits::bit(0),
            limits(100),
            |input: Input| Ok(Validated::new((), input.items)),
            move |(), _| {
                witness.fetch_add(1, Ordering::SeqCst);
                async { BufferedResponse::bytes(StatusCode::OK, "done") }
            },
        ),
    );
    let mut unknown = request(Body::from(r#"{"items":3}"#));
    unknown.headers_mut().remove("authorization");
    assert_eq!(
        code(app.clone().oneshot(unknown).await.unwrap()).await,
        "unknown-principal"
    );
    let mut wrong_type = request(Body::from(r#"{"items":3}"#));
    wrong_type.headers_mut().remove("content-type");
    assert_eq!(
        code(app.clone().oneshot(wrong_type).await.unwrap()).await,
        "unsupported-media-type"
    );
    let oversized = request(Body::from(format!(
        "{{\"items\":3,\"padding\":\"{}\"}}",
        "x".repeat(101)
    )));
    assert_eq!(
        code(app.clone().oneshot(oversized).await.unwrap()).await,
        "body-too-large"
    );
    let mut held = Vec::new();
    while let Ok(permit) = adapter.runtime().recorder().try_reserve() {
        held.push(permit);
    }
    assert_eq!(
        code(
            app.clone()
                .oneshot(request(Body::from(r#"{"items":3}"#)))
                .await
                .unwrap()
        )
        .await,
        "accounting-busy"
    );
    drop(held);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        app.clone()
            .oneshot(request(Body::from(r#"{"items":60}"#)))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        code(
            app.oneshot(request(Body::from(r#"{"items":60}"#)))
                .await
                .unwrap()
        )
        .await,
        "quota-exhausted"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.shutdown().await.unwrap().usage.unwrap().accepted, 1);
    assert_eq!(store.usage_recorded(AccountId(1)), CostUnits(60));
}
