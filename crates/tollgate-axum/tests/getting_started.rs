//! Executable Axum guide. Excerpts in docs/AXUM.md are checked against this file.
use axum::{
    Router,
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
};
use jiff::SignedDuration;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tollgate_admission::{ExecutionCapacityGate, ExecutionCapacityMode};
use tollgate_auth::{CredentialVerifier, HmacRegistry};
use tollgate_axum::{
    AdapterConfig, BearerAuth, BufferedResponse, InputError, InputLimits, Tollgate,
    TollgateConnection, Validated,
};
use tollgate_client::{
    AccountLeaseConfig, Clock, InstanceRuntime, InstanceRuntimeConfig, SnapshotManagerConfig,
    SystemClock, TrackedPrincipals, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, Generation,
    LocalSharding, OpIndex, PermissionBits, PublishableSnapshot, RequestId, ResolvedLimits,
};
use tollgate_store::{AccountConfig, GrantPolicy, MemoryStore};
use tower::ServiceExt;

#[derive(Clone, Copy)]
enum Op {
    Quote,
    Report,
}
impl OpIndex for Op {
    fn index(&self) -> usize {
        *self as usize
    }
}
const CALL: PermissionBits = PermissionBits::bit(0);
#[derive(Deserialize)]
struct QuoteInput {
    items: Vec<String>,
}

#[tokio::test]
async fn axum_getting_started() {
    let store = MemoryStore::new(GrantPolicy::default()).expect("default grant policy is valid");
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let account = AccountId(1);
    store.create_account(AccountConfig {
        account_id: account,
        initial_balance: CostUnits(10_000),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });

    // The plan, compiled once per policy change and never per request:
    // 5 units per request, plus 1 per quote and 20 per report.
    let costs = CostTable::builder(CostUnits(5), CostUnits(5))
        .weight(&Op::Quote, CostUnits(1))
        .weight(&Op::Report, CostUnits(20))
        .build();
    let valid_until = clock
        .now()
        .checked_add(SignedDuration::from_hours(1))
        .expect("an hour from now is a valid timestamp");
    let snapshot = AccountSnapshot::builder(
        account,
        Generation(1),
        AccountStatus::Active,
        valid_until,
        CALL,
        ResolvedLimits::new(64),
        Arc::new(costs),
    )
    .build();

    // The caller's credential resolves to this principal.
    // Demonstration credentials only; production uses KeyManager's verifier.
    let verifier = Arc::new(HmacRegistry::new(b"axum-guide-demo-secret"));
    verifier.install_credentials([b"axum-guide-demo-key".as_slice()]);
    let caller = verifier.verify(b"axum-guide-demo-key").unwrap().principal;
    store
        .publish_snapshot(
            caller,
            PublishableSnapshot::try_new(Arc::new(snapshot)).expect("a consistent snapshot"),
        )
        .expect("the account exists");

    let config = InstanceRuntimeConfig {
        snapshots: SnapshotManagerConfig {
            // Serve every principal the store knows about.
            principals: TrackedPrincipals::All { seed: vec![caller] },
            refresh_interval: Duration::from_secs(30),
            unknown_ttl: SignedDuration::from_secs(60),
            revoked_ttl: SignedDuration::from_hours(1),
            retry_backoff: Duration::from_millis(200),
            max_concurrent_fetches: 16,
            fetch_timeout: Duration::from_secs(5),
            enumeration_timeout: Duration::from_secs(30),
        },
        leases: AccountLeaseConfig {
            // Draw 1,000 units at a time; refill below 100.
            target_grant: CostUnits(1_000),
            low_water: CostUnits(100),
            lease_ttl: SignedDuration::from_secs(60),
            expiry_safety_margin: SignedDuration::from_secs(2),
            poll_interval: Duration::from_millis(20),
            store_call_timeout: Duration::from_secs(5),
            shutdown_release_deadline: Duration::from_secs(5),
        },
        usage: UsageWriterConfig {
            queue_capacity: 4_096,
            max_batch: 256,
            flush_interval: Duration::from_millis(25),
            retry_backoff: Duration::from_millis(50),
            shutdown_drain_deadline: Duration::from_secs(5),
            ingest_timeout: Duration::from_secs(5),
        },
        sharding: LocalSharding::SINGLE,
        snapshot_history_capacity:
            tollgate_admission::ArcSwapSnapshotMap::DEFAULT_GENERATION_CAPACITY,
        idle_account_linger: Duration::from_secs(1),
        manager_restart_backoff: Duration::from_millis(200),
        shutdown_deadline: Duration::from_secs(15),
    };

    // One store plays all three control-plane roles here: the snapshot
    // source, the lease allocator, and the usage sink.
    let (runtime, handle) = InstanceRuntime::spawn(
        store.clone(),
        store.clone(),
        store.clone(),
        Arc::clone(&clock),
        config,
    )
    .expect("a valid runtime configuration");

    // Ready means: snapshots loaded, a lease in hand, and usage accounting up.
    while !handle.readiness(clock.now()).is_ready() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let capacity = ExecutionCapacityGate::new(
        ExecutionCapacityMode::Reserved {
            total: std::num::NonZeroU32::new(8).unwrap(),
            assured_reserve: std::num::NonZeroU32::new(2).unwrap(),
        },
        LocalSharding::SINGLE,
    )
    .unwrap()
    .unwrap();
    let tollgate = Tollgate::new(AdapterConfig {
        runtime: handle,
        authenticator: BearerAuth::new(verifier),
        clock,
        request_ids: || Ok(RequestId(uuid::Uuid::new_v4().as_u128())),
        capacity,
    });

    let app = Router::new()
        .route("/health", axum::routing::get(|| async { "ok" }))
        .route(
            "/report",
            tollgate.post(
                Op::Report,
                CALL,
                || Ok(Validated::new((), 1)),
                |(), charge| async move {
                    BufferedResponse::json(
                        StatusCode::OK,
                        &serde_json::json!({
                            "report": "complete", "units_charged": charge.units_charged.get(),
                        }),
                    )
                },
            ),
        )
        .route(
            "/quote",
            tollgate.post_json(
                Op::Quote,
                CALL,
                InputLimits::new(4096, Duration::from_secs(5)).unwrap(),
                |input: QuoteInput| {
                    if input.items.iter().any(String::is_empty) {
                        return Err(InputError("item names must be nonempty"));
                    }
                    let quantity = u64::try_from(input.items.len())
                        .map_err(|_| InputError("too many items"))?;
                    Ok(Validated::new(input, quantity))
                },
                |input, charge| async move {
                    BufferedResponse::json(
                        StatusCode::OK,
                        &serde_json::json!({
                            "items": input.items,
                            "request_id": charge.request_id.to_string(),
                            "units_charged": charge.units_charged.get(),
                            "policy_revision": charge.policy_revision.to_string(),
                        }),
                    )
                },
            ),
        );

    // A real listener installs this connection state via
    // app.into_make_service_with_connect_info::<TollgateConnection>().
    let connection = ConnectInfo(TollgateConnection::default());
    for (path, body, status) in [
        ("/report", "", StatusCode::OK),
        ("/quote", r#"{"items":["a","b","c"]}"#, StatusCode::OK),
        (
            "/quote",
            r#"{"items":[""]}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let mut request = Request::builder()
            .method("POST")
            .uri(path)
            .header("authorization", "Bearer axum-guide-demo-key")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        request.extensions_mut().insert(connection.clone());
        assert_eq!(app.clone().oneshot(request).await.unwrap().status(), status);
    }
    let health = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(health).await.unwrap().status(),
        StatusCode::OK
    );
    drop(app); // Stop HTTP admission before draining the owned runtime.
    let report = runtime.shutdown().await.unwrap();
    assert_eq!(report.usage.unwrap().accepted, 2);
    assert_eq!(store.usage_recorded(account), CostUnits(33)); // report 25 + three quotes 8
}
