//! The via-server topology, end to end over real loopback TCP: the same
//! instance stack from the direct-store no-double-spend test — admission
//! engine, lease manager, usage writer — running over `HttpStore` against a
//! live `tollgate-server`. Pluggability made executable: the client code is
//! identical, only the `Arc<dyn LeaseAllocator>`/`Arc<dyn UsageSink>` differ.

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{
    AdmissionEngine, AdmissionRequest, ArcSwapSnapshotMap, LeaseSlot, SnapshotMap,
};
use tollgate_client::{
    HttpStore, LeaseManager, LeaseManagerConfig, SlotRegistry, SnapshotManager,
    SnapshotManagerConfig, SystemClock, UsageWriter, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, DenyReason, FencingToken,
    Generation, LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, Principal, RequestId,
    ResolvedLimits,
};
use tollgate_store::{
    AccountConfig, GrantPolicy, MemoryStore, SnapshotResolution, SnapshotSource as _,
};

use tollgate_server::{ServerState, serve};

const ACCOUNT: AccountId = AccountId(1);
const PRINCIPAL: Principal = Principal(7);
const DEPOSIT: u64 = 5_000;
const COST_PER_REQUEST: u64 = 51;

#[tokio::test]
async fn zero_reclaim_interval_is_rejected() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let error = serve(
        listener,
        ServerState {
            store,
            clock: Arc::new(SystemClock),
        },
        std::time::Duration::ZERO,
        std::future::pending(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[derive(Clone, Copy)]
struct PriceOp;
impl OpIndex for PriceOp {
    fn index(&self) -> usize {
        0
    }
}

fn snapshot() -> Arc<AccountSnapshot> {
    Arc::new(AccountSnapshot {
        account_id: ACCOUNT,
        key_id: None,
        generation: Generation(1),
        status: AccountStatus::Active,
        valid_until: Timestamp::from_second(4_102_444_800).unwrap(),
        permissions: PermissionBits::bit(0),
        limits: ResolvedLimits {
            max_items_per_request: 64,
            rate_units_per_second: u64::from(u32::MAX),
            rate_burst_units: u64::from(u32::MAX),
        },
        cost_table: Arc::new(
            CostTable::builder(CostUnits(50), CostUnits(50))
                .weight(&PriceOp, CostUnits(1))
                .build(),
        ),
    })
}

#[tokio::test]
async fn http_negative_ttl_refetches_without_push() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(
        listener,
        ServerState {
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock),
        },
        std::time::Duration::from_secs(60),
        async move {
            let _ = stop_rx.await;
        },
    ));

    let http = HttpStore::new(format!("http://{address}"));
    let map = Arc::new(ArcSwapSnapshotMap::new());
    let engine = AdmissionEngine::new(Arc::clone(&map));
    let slots = SlotRegistry::new();
    let manager = SnapshotManager::spawn(
        http.clone(),
        map,
        Arc::clone(&slots),
        Arc::new(SystemClock),
        SnapshotManagerConfig {
            principals: vec![PRINCIPAL],
            refresh_interval: std::time::Duration::from_secs(60),
            negative_ttl: SignedDuration::from_millis(100),
            retry_backoff: std::time::Duration::from_millis(20),
            max_concurrent_fetches: 1,
        },
    )
    .unwrap();

    let mut ready = manager.ready();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !*ready.borrow() {
            ready.changed().await.unwrap();
        }
    })
    .await
    .expect("initial unknown resolution must make the manager ready");
    assert!(matches!(
        engine.admit(
            AdmissionRequest {
                principal: PRINCIPAL,
                required: PermissionBits::bit(0),
                op: &PriceOp,
                items: 1,
            },
            Timestamp::now(),
        ),
        Err(DenyReason::UnknownPrincipal)
    ));

    // HttpStore has no push stream. Publication can therefore become visible
    // only through the negative-TTL targeted pull; the 60s full refresh must
    // not determine recovery latency.
    store.publish_snapshot(PRINCIPAL, snapshot());
    slots.slot(ACCOUNT).install(Arc::new(LocalLease::new(
        LeaseGrant {
            lease_id: LeaseId(1),
            account_id: ACCOUNT,
            fencing_token: FencingToken(1),
            units: CostUnits(1_000),
            expires_at: Timestamp::now()
                .checked_add(SignedDuration::from_secs(60))
                .unwrap(),
        },
        CostUnits::ZERO,
    )));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match engine.admit(
                AdmissionRequest {
                    principal: PRINCIPAL,
                    required: PermissionBits::bit(0),
                    op: &PriceOp,
                    items: 1,
                },
                Timestamp::now(),
            ) {
                Ok(admitted) => {
                    admitted.reservation.cancel();
                    break;
                }
                Err(DenyReason::UnknownPrincipal) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(other) => panic!("unexpected deny while waiting for refetch: {other}"),
            }
        }
    })
    .await
    .expect("negative TTL must trigger a targeted HTTP refetch");

    store.remove_snapshot(PRINCIPAL);
    assert!(matches!(
        http.snapshot(PRINCIPAL).await.unwrap(),
        SnapshotResolution::Revoked {
            generation: Generation(1)
        }
    ));

    manager.shutdown().await;
    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_stack_over_loopback_http() {
    // Server side: memory backend, system clock, real listener on an
    // ephemeral port.
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(DEPOSIT),
        active: true,
    });
    store.publish_snapshot(PRINCIPAL, snapshot());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(
        listener,
        ServerState {
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock),
        },
        std::time::Duration::from_millis(200),
        async move {
            let _ = stop_rx.await;
        },
    ));

    // Instance side: everything over HTTP.
    let http = HttpStore::new(format!("http://{address}"));
    let clock = Arc::new(SystemClock);

    // Cold fetch of the snapshot through the transport (pull path), plus the
    // negative case for an unknown principal.
    let SnapshotResolution::Present(fetched) = http.snapshot(PRINCIPAL).await.unwrap() else {
        panic!("published snapshot must be present");
    };
    assert_eq!(fetched.generation, Generation(1));
    assert!(matches!(
        http.snapshot(Principal(999)).await.unwrap(),
        SnapshotResolution::Unknown
    ));

    let slot = LeaseSlot::empty();
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
    engine.map().install(PRINCIPAL, fetched, Arc::clone(&slot));

    let manager = LeaseManager::spawn(
        http.clone(),
        Arc::clone(&slot),
        clock.clone(),
        LeaseManagerConfig {
            account: ACCOUNT,
            target_grant: CostUnits(2_000),
            low_water: CostUnits(500),
            lease_ttl: SignedDuration::from_secs(3_600),
            expiry_safety_margin: SignedDuration::from_secs(2),
            poll_interval: std::time::Duration::from_millis(10),
            store_call_timeout: std::time::Duration::from_secs(5),
            shutdown_release_deadline: std::time::Duration::from_secs(10),
        },
    )
    .unwrap();
    let (recorder, writer) = UsageWriter::spawn(
        http.clone(),
        clock,
        UsageWriterConfig {
            queue_capacity: 64,
            max_batch: 16,
            flush_interval: std::time::Duration::from_millis(10),
            retry_backoff: std::time::Duration::from_millis(10),
            shutdown_drain_deadline: std::time::Duration::from_secs(60),
            ingest_timeout: std::time::Duration::from_secs(5),
        },
    )
    .unwrap();

    // Spend the account down to stable denial through the admission engine.
    let mut committed_units = 0u64;
    let mut request_seq = 0u128;
    let mut quiet_rounds = 0;
    for _ in 0..300 {
        let mut round_commits = 0;
        for _ in 0..10 {
            let Ok(permit) = recorder.try_reserve() else {
                continue;
            };
            match engine.admit(
                AdmissionRequest {
                    principal: PRINCIPAL,
                    required: PermissionBits::bit(0),
                    op: &PriceOp,
                    items: 1,
                },
                Timestamp::now(),
            ) {
                Ok(admitted) => {
                    admitted
                        .reservation
                        .commit_at_execution_start(Timestamp::now())
                        .unwrap();
                    request_seq += 1;
                    let event = admitted
                        .reservation
                        .usage_event(RequestId(request_seq), Timestamp::now())
                        .unwrap();
                    permit.record(event);
                    committed_units += admitted.quote.total.get();
                    round_commits += 1;
                }
                Err(
                    DenyReason::LeaseUnavailable
                    | DenyReason::LeaseExhausted { .. }
                    | DenyReason::LeaseExpired,
                ) => {}
                Err(other) => panic!("unexpected deny: {other}"),
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        if round_commits == 0 {
            quiet_rounds += 1;
            if quiet_rounds >= 5 {
                break;
            }
        } else {
            quiet_rounds = 0;
        }
    }
    assert!(quiet_rounds >= 5, "account never drained over HTTP");
    assert!(committed_units <= DEPOSIT);
    assert!(
        committed_units >= DEPOSIT - 5 * COST_PER_REQUEST,
        "underspend: {committed_units}"
    );

    // Orderly shutdown: flush billing, then release leases, then stop the
    // server.
    let stats = writer.shutdown().await.unwrap();
    assert_eq!(stats.lost, 0);
    assert_eq!(stats.rejected, 0);
    manager.shutdown().await;
    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();

    // Zero drift between admission's committed units and the billing ledger,
    // through a real network transport.
    assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(committed_units));
    assert_eq!(store.balance(ACCOUNT), CostUnits(DEPOSIT - committed_units));
    let conservation = store.conservation(ACCOUNT).unwrap();
    assert!(conservation.holds(), "conservation: {conservation:?}");
    assert_eq!(conservation.settlement_loss, CostUnits::ZERO);
}
