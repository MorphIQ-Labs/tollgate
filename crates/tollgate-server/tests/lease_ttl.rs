//! Lease TTLs preserve their value across the HTTP boundary.

mod common;

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};
use tollgate_client::HttpStore;
use tollgate_core::{AccountId, AccountStatus, CapacityClass, CostUnits};
use tollgate_server::{Backend, ServerState, serve};
use tollgate_store::{
    AccountConfig, AllocateError, GrantPolicy, LeaseAllocator, ManualClock, MemoryStore,
};

const ACCOUNT: AccountId = AccountId(76);
// Above the old wire range so saturation cannot hide behind the policy clamp.
const MAX_TTL: SignedDuration = SignedDuration::from_secs(u32::MAX as i64 + 100);

fn now() -> Timestamp {
    Timestamp::from_second(100).unwrap()
}

fn ttls() -> [SignedDuration; 7] {
    [
        SignedDuration::from_nanos(1),
        SignedDuration::from_millis(500),
        SignedDuration::from_millis(1_500),
        SignedDuration::from_secs(60),
        SignedDuration::from_secs(i64::from(u32::MAX)),
        SignedDuration::from_secs(i64::from(u32::MAX) + 1),
        SignedDuration::MAX,
    ]
}

struct TestServer<S: Backend> {
    base: String,
    http: Arc<HttpStore>,
    store: Arc<S>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl<S: Backend> Drop for TestServer<S> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn policy() -> GrantPolicy {
    GrantPolicy {
        max_ttl: MAX_TTL,
        ..GrantPolicy::default()
    }
}

async fn server() -> TestServer<MemoryStore> {
    let store = MemoryStore::new(policy()).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(100),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    serve_store(store).await
}

async fn serve_store<S: Backend>(store: Arc<S>) -> TestServer<S> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let http = common::http(base.clone());
    let task = tokio::spawn(serve(
        listener,
        ServerState {
            security: common::security(),
            issuer: None,
            store: store.clone(),
            clock: Arc::new(ManualClock::new(now())),
        },
        std::time::Duration::from_secs(60),
        std::future::pending(),
    ));
    TestServer {
        base,
        http,
        store,
        task,
    }
}

/// Shares the database with the mirrored backend suite. Cargo runs binaries
/// sequentially; the mutation profile assigns this test to its Postgres group.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_and_http_preserve_ttl_across_acquire_and_consolidation() {
    use tollgate_store::AdminStore;
    use tollgate_store_postgres::PostgresStore;

    let Ok(url) = std::env::var("TOLLGATE_PG_URL") else {
        assert!(
            std::env::var_os("TOLLGATE_REQUIRE_PG").is_none(),
            "Postgres is required"
        );
        eprintln!("SKIPPED: TOLLGATE_PG_URL not set");
        return;
    };
    let store = PostgresStore::connect(&url, policy()).await.unwrap();
    tollgate_store_postgres::test_support::truncate_all(&store)
        .await
        .unwrap();
    AdminStore::create_account(
        &*store,
        AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(100),
            status: AccountStatus::Active,
            capacity_class: CapacityClass::Assured,
        },
    )
    .await
    .unwrap();
    let server = serve_store(store.clone()).await;
    for ttl in ttls() {
        for allocator in [&*store as &dyn LeaseAllocator, &*server.http] {
            let original = allocator
                .acquire(ACCOUNT, CostUnits(10), ttl, now())
                .await
                .unwrap()
                .grant;
            let expected = now().checked_add(ttl.min(MAX_TTL)).unwrap();
            assert_eq!(original.expires_at, expected);
            let grant = allocator
                .consolidate(
                    original.lease_id,
                    original.fencing_token,
                    original.units,
                    CostUnits(10),
                    ttl,
                    now(),
                )
                .await
                .unwrap()
                .grant;
            assert_eq!(grant.expires_at, expected);
            allocator
                .release(grant.lease_id, grant.fencing_token, grant.units, now())
                .await
                .unwrap();
            assert_eq!(store.balance(ACCOUNT).await.unwrap(), CostUnits(100));
            assert!(store.conservation(ACCOUNT).await.unwrap().unwrap().holds());
        }
    }
}

#[tokio::test]
async fn http_acquire_preserves_positive_ttl() {
    let server = server().await;
    for ttl in ttls() {
        let grant = server
            .http
            .acquire(ACCOUNT, CostUnits(10), ttl, Timestamp::MAX)
            .await
            .unwrap_or_else(|error| panic!("acquire TTL {ttl}: {error}"))
            .grant;
        assert_eq!(
            grant.expires_at,
            now().checked_add(ttl.min(MAX_TTL)).unwrap()
        );
        server
            .http
            .release(grant.lease_id, grant.fencing_token, grant.units, now())
            .await
            .unwrap();
        assert_eq!(server.store.balance(ACCOUNT), CostUnits(100));
        assert!(server.store.conservation(ACCOUNT).unwrap().holds());
    }
}

#[tokio::test]
async fn http_consolidation_preserves_positive_ttl() {
    let server = server().await;
    for ttl in ttls() {
        let original = server
            .http
            .acquire(ACCOUNT, CostUnits(10), SignedDuration::from_secs(60), now())
            .await
            .unwrap()
            .grant;
        let grant = server
            .http
            .consolidate(
                original.lease_id,
                original.fencing_token,
                original.units,
                CostUnits(10),
                ttl,
                Timestamp::MAX,
            )
            .await
            .unwrap_or_else(|error| panic!("consolidation TTL {ttl}: {error}"))
            .grant;
        assert_eq!(
            grant.expires_at,
            now().checked_add(ttl.min(MAX_TTL)).unwrap()
        );
        server
            .http
            .release(grant.lease_id, grant.fencing_token, grant.units, now())
            .await
            .unwrap();
        assert_eq!(server.store.balance(ACCOUNT), CostUnits(100));
        assert!(server.store.conservation(ACCOUNT).unwrap().holds());
    }
}

#[tokio::test]
async fn invalid_wire_ttls_never_debit_or_settle_a_lease() {
    use serde_json::json;

    let server = server().await;
    let original = server
        .http
        .acquire(ACCOUNT, CostUnits(10), SignedDuration::from_secs(60), now())
        .await
        .unwrap()
        .grant;
    for ttl in [
        json!({"ttl_seconds": 0}),
        json!({"ttl_seconds": 0, "ttl": "PT0S"}),
        json!({"ttl_seconds": 0, "ttl": "-PT0.5S"}),
        json!({"ttl_seconds": 60, "ttl": "PT0.5S"}),
        json!({"ttl_seconds": 60, "ttl": "PT60S"}),
    ] {
        for (operation, mut body) in [
            ("acquire", json!({"account_id": ACCOUNT, "requested": 10})),
            (
                "consolidate",
                json!({
                    "lease_id": original.lease_id, "fencing_token": original.fencing_token,
                    "unspent": 10, "requested": 10,
                }),
            ),
        ] {
            body.as_object_mut()
                .unwrap()
                .extend(ttl.as_object().unwrap().clone());
            let response = reqwest::Client::new()
                .post(format!("{}/v1/leases/{operation}", server.base))
                .bearer_auth(common::INSTANCE)
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
            let problem: tollgate_store::wire::Problem = response.json().await.unwrap();
            assert_eq!(problem.code, "invalid-ttl");
            assert_eq!(server.store.balance(ACCOUNT), CostUnits(90));
            let ledger = server.store.conservation(ACCOUNT).unwrap();
            assert_eq!(ledger.active_lease_grants, CostUnits(10));
            assert_eq!(ledger.settled_usage, CostUnits::ZERO);
            assert!(ledger.holds());
        }
    }
    server
        .http
        .release(
            original.lease_id,
            original.fencing_token,
            original.units,
            now(),
        )
        .await
        .unwrap();
    assert_eq!(server.store.balance(ACCOUNT), CostUnits(100));
}

#[tokio::test]
async fn legacy_servers_reject_precise_ttls_and_invalid_input_never_reaches_http() {
    use axum::{Json, Router, extract::State, response::IntoResponse, routing::post};
    use serde::Deserialize;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tollgate_core::{FencingToken, LeaseGrant, LeaseId};

    #[derive(Deserialize)]
    struct LegacyRequest {
        ttl_seconds: u32,
    }
    #[derive(Default)]
    struct Observed {
        requests: AtomicUsize,
        allocations: AtomicUsize,
    }
    async fn legacy(
        State(observed): State<Arc<Observed>>,
        Json(request): Json<LegacyRequest>,
    ) -> axum::response::Response {
        observed.requests.fetch_add(1, Ordering::Relaxed);
        if request.ttl_seconds == 0 {
            return (
                reqwest::StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "type": "about:blank", "title": "lease TTL must be positive",
                    "status": 422, "code": "invalid-ttl",
                })),
            )
                .into_response();
        }
        observed.allocations.fetch_add(1, Ordering::Relaxed);
        Json(LeaseGrant {
            lease_id: LeaseId(1),
            account_id: ACCOUNT,
            fencing_token: FencingToken(1),
            units: CostUnits(10),
            expires_at: now()
                .checked_add(SignedDuration::from_secs(i64::from(request.ttl_seconds)).min(MAX_TTL))
                .unwrap(),
        })
        .into_response()
    }
    let observed = Arc::new(Observed::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = HttpStore::new(format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let router = Router::new()
        .route("/v1/leases/acquire", post(legacy))
        .route("/v1/leases/consolidate", post(legacy))
        .with_state(observed.clone());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    // Abort on assertion failure too; no detached fixture listener remains.
    struct Stop(tokio::task::JoinHandle<()>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _stop = Stop(task);
    for ttl in ttls() {
        let acquire = http.acquire(ACCOUNT, CostUnits(10), ttl, now()).await;
        let consolidate = http
            .consolidate(
                LeaseId(1),
                FencingToken(1),
                CostUnits(10),
                CostUnits(10),
                ttl,
                now(),
            )
            .await;
        if ttl == SignedDuration::from_secs(60)
            || ttl == SignedDuration::from_secs(i64::from(u32::MAX))
        {
            assert_eq!(
                acquire.unwrap().grant.expires_at,
                now().checked_add(ttl.min(MAX_TTL)).unwrap()
            );
            assert_eq!(
                consolidate.unwrap().grant.expires_at,
                now().checked_add(ttl.min(MAX_TTL)).unwrap()
            );
        } else {
            assert_eq!(acquire.unwrap_err(), AllocateError::InvalidTtl);
            assert_eq!(consolidate.unwrap_err(), AllocateError::InvalidTtl);
        }
    }
    assert_eq!(observed.requests.load(Ordering::Relaxed), 14);
    assert_eq!(observed.allocations.load(Ordering::Relaxed), 4);
    for ttl in [
        SignedDuration::ZERO,
        SignedDuration::from_nanos(-1),
        SignedDuration::MIN,
    ] {
        assert_eq!(
            http.acquire(ACCOUNT, CostUnits(10), ttl, now())
                .await
                .unwrap_err(),
            AllocateError::InvalidTtl
        );
        assert_eq!(
            http.consolidate(
                LeaseId(1),
                FencingToken(1),
                CostUnits(10),
                CostUnits(10),
                ttl,
                now()
            )
            .await
            .unwrap_err(),
            AllocateError::InvalidTtl
        );
    }
    assert_eq!(observed.requests.load(Ordering::Relaxed), 14);
}
