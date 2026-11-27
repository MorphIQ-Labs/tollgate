//! The service against a remote control plane, here in process so a failure
//! points at this crate. `tollgate-e2e` drives the same topology as separate
//! processes over PostgreSQL.

#![allow(
    clippy::disallowed_methods,
    reason = "a test over real loopback TCP observes the wall clock the server and service run on"
)]

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::connect_info::MockConnectInfo;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use jiff::{SignedDuration, Timestamp};
use serde_json::{Value, json};
use tower::ServiceExt;

use pricing_api::{ControlPlaneTiming, PricingConnection, RemoteConfig, build_remote_app};
use tollgate_auth::{CredentialVerifier, HmacRegistry};
use tollgate_client::{BearerToken, SystemClock};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, Generation,
    KeyId, LocalSharding, OpIndex, PermissionBits, PublishableSnapshot, ResolvedLimits,
};
use tollgate_server::security::{ControlIdentity, Role, SecurityPolicy, ServerSecurity};
use tollgate_store::{AccountConfig, GrantPolicy, KeyDirectory, KeyRecord, MemoryStore};

// Public test fixtures.
const TOKEN: &str = "pricing-remote-fixture-instance-credential";
const SECRET: &[u8] = b"pricing-remote-fixture-issuer-secret";
const CUSTOMER: &str = "pricing-remote-fixture-customer-key";
const ACCOUNT: AccountId = AccountId(7);
const BALANCE: u64 = 100_000;
/// The schedule below: 50 per request and 1 for its one contract.
const CHARGE: u64 = 51;

/// The service's only operation, at its index in the cost table.
struct PriceOp;
impl OpIndex for PriceOp {
    fn index(&self) -> usize {
        0
    }
}

async fn control_plane() -> (Arc<MemoryStore>, String) {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(BALANCE),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    let (principal, digest) = HmacRegistry::new(SECRET).digest_credential(CUSTOMER.as_bytes());
    KeyDirectory::insert_key(
        &*store,
        KeyRecord {
            key_id: KeyId(7),
            account_id: ACCOUNT,
            principal,
            digest,
            not_after: None,
        },
    )
    .await
    .unwrap();
    let snapshot = AccountSnapshot::builder(
        ACCOUNT,
        Generation(1),
        AccountStatus::Active,
        Timestamp::now() + SignedDuration::from_hours(1),
        PermissionBits(1),
        ResolvedLimits::new(16).with_weighted_rate(1_000_000, 1_000_000),
        Arc::new(
            CostTable::builder(CostUnits(50), CostUnits(50))
                .weight(&PriceOp, CostUnits(1))
                .build(),
        ),
    )
    .key_id(KeyId(7))
    .build();
    store
        .publish_snapshot(
            principal,
            PublishableSnapshot::try_new(Arc::new(snapshot)).unwrap(),
        )
        .unwrap();

    let registry = Arc::new(HmacRegistry::new(b"pricing-remote-fixture-control"));
    registry.install_credentials([TOKEN.as_bytes()]);
    let identity = registry.verify(TOKEN.as_bytes()).unwrap().principal;
    let policy = SecurityPolicy::new()
        .with_bearer(
            registry,
            [(
                identity,
                ControlIdentity::new("pricing-test", Role::Instance).unwrap(),
            )],
        )
        .unwrap();
    let router = tollgate_server::router(tollgate_server::ServerState {
        store: store.clone(),
        clock: Arc::new(SystemClock),
        security: ServerSecurity::new(policy, None).unwrap(),
        issuer: None,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (store, url)
}

fn config(control_url: String) -> RemoteConfig {
    RemoteConfig {
        control_url,
        control_token: BearerToken::new(TOKEN).unwrap(),
        credential_secret: SECRET.to_vec(),
        timing: ControlPlaneTiming {
            refresh_interval: Duration::from_millis(50),
            ..ControlPlaneTiming::REMOTE
        },
        target_grant: CostUnits(1_000),
        low_water: CostUnits(250),
        sharding: LocalSharding::SINGLE,
    }
}

async fn send(router: &axum::Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn get(path: &str, key: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(path);
    if let Some(key) = key {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    builder.body(Body::empty()).unwrap()
}

fn price() -> Request<Body> {
    let contract =
        json!({"spot": 100.0, "strike": 105.0, "rate": 0.05, "vol": 0.2, "tte_years": 0.25});
    Request::builder()
        .method("POST")
        .uri("/v1/price")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {CUSTOMER}"))
        .body(Body::from(json!({ "contracts": [contract] }).to_string()))
        .unwrap()
}

async fn until(what: &str, mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test]
async fn a_remote_control_plane_funds_prices_reports_and_bills() {
    let (store, url) = control_plane().await;
    let (router, runtime) = build_remote_app(config(url)).await.unwrap();
    let router = router.layer(MockConnectInfo(PricingConnection::default()));

    until("the first priced response", async || {
        send(&router, price()).await.0 == StatusCode::OK
    })
    .await;
    assert_eq!(send(&router, get("/readyz", None)).await.0, StatusCode::OK);
    // One request was served, and the estimate converges on its charge.
    until("the estimate to include the charge", async || {
        let (status, body) = send(&router, get("/v1/budget", Some(CUSTOMER))).await;
        status == StatusCode::OK
            && body["balance"] == json!(BALANCE - CHARGE)
            && body["period_end"] == Value::Null
    })
    .await;
    for key in [None, Some("not-a-customer")] {
        assert_eq!(
            send(&router, get("/v1/budget", key)).await.0,
            StatusCode::UNAUTHORIZED
        );
    }

    runtime.shutdown().await;
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(CHARGE));
}

/// A credential secret the manager refuses stops startup, rather than serving
/// with nothing able to verify.
#[tokio::test]
async fn startup_refuses_a_configuration_that_cannot_verify_credentials() {
    let (_, url) = control_plane().await;
    let mut refused = config(url);
    refused.credential_secret = Vec::new();
    let Err(error) = build_remote_app(refused).await else {
        panic!("an empty secret must be refused");
    };
    assert!(error.starts_with("credential manager"), "{error}");
}

/// The cadence an instance gets when its operator configures none must be one
/// its managers accept: the embedded example's slower refresh would exceed the
/// credential evidence window and stop startup.
#[tokio::test]
async fn the_default_remote_cadence_starts() {
    let (_, url) = control_plane().await;
    let mut defaults = config(url);
    defaults.timing = ControlPlaneTiming::REMOTE;
    let (_, runtime) = build_remote_app(defaults)
        .await
        .expect("the default remote timing is valid");
    runtime.shutdown().await;
}
