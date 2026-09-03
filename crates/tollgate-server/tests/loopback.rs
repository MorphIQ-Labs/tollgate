//! The via-server topology, end to end over real loopback TCP: the same
//! instance stack from the direct-store no-double-spend test — admission
//! engine, lease manager, usage writer — running over `HttpStore` against a
//! live `tollgate-server`. Pluggability made executable: the client code is
//! identical, only the `Arc<dyn LeaseAllocator>`/`Arc<dyn UsageSink>` differ.
//!
//! The workspace denies discarding a fallible call (issue #36), because that
//! is how production failures went unseen. This harness's oneshot teardown
//! signals are the exception the rule is not aimed at: the test's assertions
//! are what fail if shutdown misbehaves, and a receiver that has already gone
//! away is the normal end of a test.
#![allow(clippy::let_underscore_must_use)]

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{AdmissionEngine, ArcSwapSnapshotMap, LeaseSlot, NoGate, SnapshotMap};
use tollgate_client::{
    HttpStore, LeaseManager, LeaseManagerConfig, SlotRegistry, SnapshotManager,
    SnapshotManagerConfig, SystemClock, TrackedPrincipals, UsageWriter, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, DenyReason, DiscardedUsage,
    FencingToken, Generation, KeyId, LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits,
    Principal, PublishableSnapshot, RequestId, ResolvedLimits, UsageEvent, UsageSource,
};
use tollgate_store::wire::{API_PREFIX, MAX_INGEST_BODY_BYTES};
use tollgate_store::{
    AccountConfig, GrantPolicy, MemoryStore, SnapshotResolution, SnapshotSource as _, UsageSink,
};

use tollgate_server::{ServerState, router, serve};

const ACCOUNT: AccountId = AccountId((1u128 << 127) | 1);
const PRINCIPAL: Principal = Principal((1u128 << 127) | 7);
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
    Arc::new(
        AccountSnapshot::builder(
            ACCOUNT,
            Generation(1),
            AccountStatus::Active,
            Timestamp::from_second(4_102_444_800).unwrap(),
            PermissionBits::bit(0),
            ResolvedLimits::new(64).with_weighted_rate(u64::from(u32::MAX), u64::from(u32::MAX)),
            Arc::new(
                CostTable::builder(CostUnits(50), CostUnits(50))
                    .weight(&PriceOp, CostUnits(1))
                    .build(),
            ),
        )
        .key_id(KeyId((1u128 << 127) | 2))
        .build(),
    )
}

fn publishable(snapshot: Arc<AccountSnapshot>) -> PublishableSnapshot {
    PublishableSnapshot::try_new(snapshot).expect("test snapshot limits are valid")
}

#[tokio::test]
async fn http_store_rejects_invalid_snapshot_from_legacy_server() {
    use axum::Json;
    use axum::routing::get;

    let mut invalid = (*snapshot()).clone();
    invalid.limits = ResolvedLimits::new(64).with_weighted_rate(u64::from(u32::MAX), 113);
    let invalid = Arc::new(invalid);
    let app = axum::Router::new().route(
        &format!("{API_PREFIX}/snapshots/{{principal}}"),
        get(move || {
            let invalid = Arc::clone(&invalid);
            async move { Json(invalid) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = stop_rx.await;
            })
            .await
            .unwrap();
    });

    let http = HttpStore::new(format!("http://{address}"));
    let error = http.snapshot(PRINCIPAL).await.unwrap_err();
    assert!(
        error.0.contains("invalid snapshot from server") && error.0.contains("exceeding the burst"),
        "unexpected error: {error}"
    );

    let _ = stop_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn an_unstructured_route_404_is_not_a_confirmed_unknown_principal() {
    let app = axum::Router::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = stop_rx.await;
            })
            .await
            .unwrap();
    });

    let http = HttpStore::new(format!("http://{address}"));
    let error = http.snapshot(PRINCIPAL).await.unwrap_err();
    assert!(
        error.0.contains("unstructured 404") && !error.0.contains("unknown-principal response"),
        "unexpected error: {error}"
    );

    let _ = stop_tx.send(());
    server.await.unwrap();
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
            principals: TrackedPrincipals::Fixed(vec![PRINCIPAL]),
            refresh_interval: std::time::Duration::from_secs(60),
            unknown_ttl: SignedDuration::from_millis(100),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(20),
            max_concurrent_fetches: 1,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
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
        engine.begin(PRINCIPAL, PermissionBits::bit(0), Timestamp::now()),
        Err(DenyReason::UnknownPrincipal)
    ));

    // HttpStore has no push stream. Publication can therefore become visible
    // only through the negative-TTL targeted pull; the 60s full refresh must
    // not determine recovery latency.
    store.publish_snapshot(PRINCIPAL, publishable(snapshot()));
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
            match engine
                .begin(PRINCIPAL, PermissionBits::bit(0), Timestamp::now())
                .and_then(|context| {
                    context.admit(
                        &[(PriceOp, 1)],
                        DiscardedUsage::new().slot(),
                        Timestamp::now(),
                    )
                }) {
                Ok(pending) => {
                    pending.cancel();
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
        status: AccountStatus::Active,
    });
    store.publish_snapshot(PRINCIPAL, publishable(snapshot()));

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

    let slot = LeaseSlot::for_account(ACCOUNT);
    let engine = AdmissionEngine::new(ArcSwapSnapshotMap::new());
    engine
        .map()
        .install(PRINCIPAL, fetched.into_inner(), Arc::clone(&slot));

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
            match engine
                .begin(PRINCIPAL, PermissionBits::bit(0), Timestamp::now())
                .and_then(|context| context.admit(&[(PriceOp, 1)], permit, Timestamp::now()))
            {
                Ok(pending) => {
                    request_seq += 1;
                    let committed = pending.acquire_capacity(&NoGate).unwrap();
                    let committed = committed
                        .commit(RequestId(request_seq), Timestamp::now())
                        .unwrap();
                    committed_units += committed.units().get();
                    round_commits += 1;
                    drop(committed);
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

/// #48 over the transport that needs it most. `HttpStore::subscribe` is a
/// closed channel — cross-process push is a deferred seam — so the periodic
/// refresh is the *only* way a new customer reaches an HTTP-transport
/// instance, and `GET /v1/snapshots` is the only way it learns the set exists.
#[tokio::test]
async fn http_instance_discovers_a_principal_published_after_it_started() {
    const LATER: Principal = Principal(8);

    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(1_000_000),
        status: AccountStatus::Active,
    });
    store.publish_snapshot(PRINCIPAL, publishable(snapshot()));

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
    let slots = SlotRegistry::new();
    let manager = SnapshotManager::spawn(
        http.clone(),
        Arc::clone(&map) as Arc<dyn SnapshotMap>,
        Arc::clone(&slots),
        Arc::new(SystemClock),
        SnapshotManagerConfig {
            // Nothing seeded: everything this instance serves is discovered.
            principals: TrackedPrincipals::All { seed: Vec::new() },
            refresh_interval: std::time::Duration::from_millis(50),
            unknown_ttl: SignedDuration::from_secs(30),
            revoked_ttl: SignedDuration::from_secs(3_600),
            retry_backoff: std::time::Duration::from_millis(20),
            max_concurrent_fetches: 4,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(30),
        },
    )
    .unwrap();

    let mut ready = manager.ready();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !*ready.borrow() {
            ready.changed().await.unwrap();
        }
    })
    .await
    .expect("the enumerated principal must resolve");
    assert!(
        map.get(&PRINCIPAL).is_some(),
        "discovered through GET /v1/snapshots, not configuration"
    );
    assert!(map.get(&LATER).is_none(), "not published yet");

    // Provision a customer against the running control plane.
    store.publish_snapshot(LATER, publishable(snapshot()));

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while map.get(&LATER).is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a principal published after start must reach an HTTP instance");

    manager.shutdown().await;
    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();
}

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_second(seconds).unwrap()
}

/// Issue #61, end to end: a batch the server refuses for size comes back
/// classified terminal, so the writer will drop it rather than retry forever.
///
/// This is the round trip the wedge actually lived on. The unit tests either
/// side of it prove the server labels the refusal and the writer honours a
/// refusal; only this one proves the label survives the wire — that a real 413
/// from a real `DefaultBodyLimit` becomes `IngestError::Refused` rather than
/// the opaque "server returned 413" that every retry loop treated as an
/// outage.
#[tokio::test]
async fn an_oversized_batch_comes_back_refused_not_retryable() {
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(1_000_000),
        status: AccountStatus::Active,
    });
    let app = router(ServerState {
        store: Arc::clone(&store),
        clock: Arc::new(tollgate_client::ManualClock::new(at(0))),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = stop_rx.await;
            })
            .await
            .unwrap();
    });

    // Past the declared body limit, with the count derived from it rather
    // than guessed: an overage event of this shape serialises to 163 bytes
    // plus a separating comma, so this is comfortably over with no reliance
    // on a number that would rot if a field were added. Built directly rather
    // than through `UsageWriter`, whose `max_batch` validation now makes an
    // oversized batch unreachable — which is the point: the transport must
    // still classify correctly for the sinks and versions validation cannot
    // reach.
    let events_needed = MAX_INGEST_BODY_BYTES / 164 + 1_000;
    let events: Vec<UsageEvent> = (0..events_needed as u128)
        .map(|i| UsageEvent {
            request_id: RequestId(i),
            account_id: ACCOUNT,
            source: UsageSource::Overage,
            units: CostUnits(1),
            occurred_at: at(0),
        })
        .collect();

    let http = HttpStore::new(format!("http://{address}"));
    let error = UsageSink::ingest(&*http, &events, at(0))
        .await
        .expect_err("a body past the limit must be refused");
    assert!(
        !error.is_retryable(),
        "an oversized batch retried forever is the outage this fix exists to \
         prevent; got {error}"
    );
    assert!(
        error.to_string().contains("batch-too-large"),
        "the refusal must carry the server's own code; got {error}"
    );

    let _ = stop_tx.send(());
    server.await.unwrap();
}

/// Issue #61: 408 and 429 are the 4xx statuses that describe the moment, not
/// the payload, so they stay retryable.
///
/// They are the entire reason the classification is not simply
/// `is_client_error()`, and nothing exercised them: the mutation gate found
/// both `&&` operators in that condition unconstrained, meaning a build that
/// treated a rate-limit response as permanent would have shipped. A 429
/// classified terminal drops a batch the server was only asking us to slow
/// down about — a silent, permanent loss of billable events under load, which
/// is when a 429 is most likely.
#[tokio::test]
async fn a_rate_limited_or_timed_out_ingest_stays_retryable() {
    use axum::http::StatusCode;
    use axum::routing::post;

    for status in [StatusCode::TOO_MANY_REQUESTS, StatusCode::REQUEST_TIMEOUT] {
        let app = axum::Router::new().route(
            &format!("{API_PREFIX}/usage/ingest"),
            post(move || async move { status }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await
                .unwrap();
        });

        let http = HttpStore::new(format!("http://{address}"));
        let event = UsageEvent {
            request_id: RequestId(1),
            account_id: ACCOUNT,
            source: UsageSource::Overage,
            units: CostUnits(1),
            occurred_at: at(0),
        };
        let error = UsageSink::ingest(&*http, &[event], at(0))
            .await
            .expect_err("the server refused");
        assert!(
            error.is_retryable(),
            "{status} says try again later; classifying it terminal drops \
             billable events under exactly the load that produces it"
        );

        let _ = stop_tx.send(());
        server.await.unwrap();
    }
}
