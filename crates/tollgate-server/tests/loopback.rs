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
#![allow(
    clippy::disallowed_methods,
    reason = "an end-to-end test over real loopback TCP against a live server: wall-clock time is \
              what the server itself runs on, so the harness must speak the same instants"
)]
#![allow(
    clippy::let_underscore_must_use,
    reason = "test teardown discards results the assertions have already read"
)]

mod common;

use std::sync::Arc;

use jiff::{SignedDuration, Timestamp};

use tollgate_admission::{AdmissionEngine, ArcSwapSnapshotMap, NoGate, SnapshotMap};
use tollgate_client::{
    SlotRegistry, SnapshotManager, SnapshotManagerConfig, SystemClock, TrackedPrincipals,
    UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CostTable, CostUnits, DenyReason,
    DiscardedUsage, FencingToken, Generation, KeyId, LeaseGrant, LeaseId, LocalLease, OpIndex,
    PermissionBits, PolicyRevision, Principal, PublishableSnapshot, RequestId, ResolvedLimits,
    UsageEvent, UsageSource,
};
#[path = "../../tollgate-store/tests/support/delegating.rs"]
mod delegating;
use delegating::DelegatingStore;

use tollgate_store::wire::{API_PREFIX, MAX_INGEST_BODY_BYTES};
use tollgate_store::{
    AccountConfig, AllocateError, GrantPolicy, LeaseAllocator as _, MemoryStore,
    SnapshotResolution, SnapshotSource as _, UsageSink,
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
            security: common::security(),
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

/// The consuming application's policy identity for the loopback fixture.
const REVISION: PolicyRevision = PolicyRevision([0xa7; 32]);

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
        // A stated revision, so the end-to-end path carries a real value
        // rather than the zero an omission would also produce (#94).
        .policy_revision(REVISION)
        .build(),
    )
}

fn publishable(snapshot: Arc<AccountSnapshot>) -> PublishableSnapshot {
    PublishableSnapshot::try_new(snapshot).expect("test snapshot limits are valid")
}

fn runtime_config(principal: Principal) -> tollgate_client::InstanceRuntimeConfig {
    tollgate_client::InstanceRuntimeConfig {
        snapshot_history_capacity:
            tollgate_admission::ArcSwapSnapshotMap::DEFAULT_GENERATION_CAPACITY,
        snapshots: SnapshotManagerConfig {
            principals: TrackedPrincipals::All {
                seed: vec![principal],
            },
            refresh_interval: std::time::Duration::from_millis(20),
            unknown_ttl: SignedDuration::from_secs(1),
            revoked_ttl: SignedDuration::from_secs(1),
            retry_backoff: std::time::Duration::from_millis(10),
            max_concurrent_fetches: 4,
            fetch_timeout: std::time::Duration::from_secs(5),
            enumeration_timeout: std::time::Duration::from_secs(5),
        },
        leases: tollgate_client::AccountLeaseConfig {
            target_grant: CostUnits(2_000),
            low_water: CostUnits(500),
            lease_ttl: SignedDuration::from_secs(3_600),
            expiry_safety_margin: SignedDuration::from_secs(2),
            poll_interval: std::time::Duration::from_millis(10),
            store_call_timeout: std::time::Duration::from_secs(5),
            shutdown_release_deadline: std::time::Duration::from_secs(10),
        },
        usage: UsageWriterConfig {
            queue_capacity: 64,
            max_batch: 16,
            flush_interval: std::time::Duration::from_millis(10),
            retry_backoff: std::time::Duration::from_millis(10),
            shutdown_drain_deadline: std::time::Duration::from_secs(60),
            ingest_timeout: std::time::Duration::from_secs(5),
        },
        sharding: tollgate_core::LocalSharding::SINGLE,
        idle_account_linger: std::time::Duration::from_millis(30),
        manager_restart_backoff: std::time::Duration::from_millis(10),
        shutdown_deadline: std::time::Duration::from_secs(70),
    }
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

    let http = common::http(format!("http://{address}"));
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

    let http = common::http(format!("http://{address}"));
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
            security: common::security(),
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock),
        },
        std::time::Duration::from_secs(60),
        async move {
            let _ = stop_rx.await;
        },
    ));

    let http = common::http(format!("http://{address}"));
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
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot()))
        .expect("snapshot fixture matches its account and credential");
    drop(
        slots.slot(ACCOUNT).replace(Arc::new(LocalLease::new(
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
        ))),
    );
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

/// #109: the fold has to survive the transport, and as *one* request. Split
/// into a release and an acquire over HTTP it would reopen exactly the gap the
/// operation closes — another instance taking the returned units, and the
/// grant policy sizing the replacement below what was handed back. Pinned here
/// because it is a wire contract: a server that does not route
/// `/leases/consolidate` leaves the via-server topology stranding the tail of
/// every allowance while the direct-store topology does not.
#[tokio::test]
async fn a_consolidation_folds_the_tail_grant_over_http() {
    let store = MemoryStore::new(GrantPolicy {
        shrink_divisor: 1,
        min_grant: CostUnits(1),
        max_ttl: SignedDuration::from_secs(3_600),
        reclaim_grace: SignedDuration::ZERO,
    })
    .unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(58),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(
        listener,
        ServerState {
            security: common::security(),
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock),
        },
        std::time::Duration::from_millis(200),
        async move {
            let _ = stop_rx.await;
        },
    ));
    let http = common::http(format!("http://{address}"));

    let tail = http
        .acquire(ACCOUNT, CostUnits(49), SignedDuration::from_secs(60), at(0))
        .await
        .unwrap();
    assert_eq!(tail.units, CostUnits(49), "49 held, 9 left in the ledger");

    let folded = http
        .consolidate(
            tail.lease_id,
            tail.fencing_token,
            tail.units,
            CostUnits(100),
            SignedDuration::from_secs(60),
            at(1),
        )
        .await
        .unwrap();
    assert_eq!(folded.units, CostUnits(58), "one lease, the whole balance");
    assert_ne!(folded.lease_id, tail.lease_id);

    // The old capability is spent server-side, so the transport carried the
    // settlement half too and not just the grant.
    assert_eq!(
        http.release(tail.lease_id, tail.fencing_token, CostUnits(0), at(2))
            .await
            .unwrap_err(),
        AllocateError::LeaseNotActive
    );

    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_stack_over_loopback_http() {
    full_stack(common::TransportMode::LoopbackBearer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_stack_over_tls_bearer() {
    full_stack(common::TransportMode::TlsBearer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_stack_over_mtls() {
    full_stack(common::TransportMode::Mtls).await;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GrantOperation {
    Acquire,
    Consolidate,
}

struct HeldGrant {
    grant: LeaseGrant,
    deliver: tokio::sync::oneshot::Sender<()>,
}

/// Real HTTP operations, with a controlled handoff of one committed grant to
/// the manager. Cancelling this handoff models losing the allocation result;
/// it does not simulate a TCP stack or assert that cancellation rolls back I/O.
///
/// `HttpStore` implements four of the seven store traits and has no
/// `AdminStore` or `StoreHealth` anywhere in the client crate. The wrapper
/// still works because `DelegatingStore<S>` is generic in `S` with each trait
/// impl bounded on `S`, so it implements exactly the traits its inner store
/// does — no more, and nothing invented to fill the gap.
fn held_grant_allocator(
    http: &Arc<tollgate_client::HttpStore>,
    operation: GrantOperation,
    held: tokio::sync::mpsc::UnboundedSender<HeldGrant>,
) -> Arc<DelegatingStore<tollgate_client::HttpStore>> {
    async fn hold(
        held: &tokio::sync::mpsc::UnboundedSender<HeldGrant>,
        grant: LeaseGrant,
    ) -> LeaseGrant {
        let (deliver, received) = tokio::sync::oneshot::channel();
        held.send(HeldGrant { grant, deliver }).unwrap();
        received
            .await
            .expect("the test retains the delivery handle");
        grant
    }

    let acquired = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (on_acquire, on_consolidate) = (held.clone(), held);
    Arc::new(
        DelegatingStore::wrapping(Arc::clone(http))
            .on_acquire(move |http, account, requested, ttl, now| {
                let (held, acquired) = (on_acquire.clone(), Arc::clone(&acquired));
                async move {
                    let grant = http.acquire(account, requested, ttl, now).await?;
                    let previous = acquired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(if operation == GrantOperation::Acquire && previous == 1 {
                        hold(&held, grant).await
                    } else {
                        grant
                    })
                }
            })
            .on_consolidate(move |http, lease, fence, unspent, requested, ttl, now| {
                let held = on_consolidate.clone();
                async move {
                    let grant = http
                        .consolidate(lease, fence, unspent, requested, ttl, now)
                        .await?;
                    Ok(if operation == GrantOperation::Consolidate {
                        hold(&held, grant).await
                    } else {
                        grant
                    })
                }
            }),
    )
}

async fn wait_for_observation(
    mut observed: impl FnMut() -> bool,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !observed() {
            tokio::task::yield_now().await;
        }
    })
    .await
}

/// Shutdown returns every confirmed, quiesced grant. A cancelled acquisition
/// may have committed an unknown capability, which must remain reported and
/// conserved until expiry. Observe both stages instead of assuming liquidity.
async fn assert_shutdown_accounting(
    store: &MemoryStore,
    handle: &tollgate_client::RuntimeHandle,
    stopped: &tollgate_client::RuntimeShutdownReport,
    deposited: CostUnits,
    committed: CostUnits,
    reclaim_at: Timestamp,
) -> Vec<tollgate_store::ReclaimedLease> {
    assert!(
        !stopped.background_failed && !stopped.deadline_expired,
        "{stopped:?}"
    );
    assert!(stopped.unfinished_accounts.is_empty(), "{stopped:?}");
    assert!(!stopped.snapshots.as_ref().unwrap().task_died);
    assert_eq!(stopped.accounts.len(), 1);
    let account = &stopped.accounts[&ACCOUNT];
    assert!(!account.task_died, "{account:?}");
    assert_eq!(account.abandoned, 0, "no request still holds a known grant");
    let usage = stopped.usage.as_ref().unwrap();
    assert_eq!(usage.lost, 0);
    assert_eq!(usage.rejected, 0);
    assert_eq!(usage.unresolved, 0);
    assert!(!usage.counter_overflow);

    let report = handle.report();
    assert!(!report.counter_overflow, "{report:?}");
    assert_eq!(report.unrecovered_grants, 0, "{report:?}");
    assert_eq!(report.accounting.unaccounted, 0);
    let refill = report.refill.unwrap();
    assert_eq!(
        refill.acquired, refill.released,
        "every confirmed grant settled"
    );
    assert_eq!(refill.abandoned, 0);
    let accounts = handle.account_reports(reclaim_at);
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].account, ACCOUNT);
    assert_eq!(accounts[0].uncertain_acquires, report.uncertain_acquires);

    let before = store.conservation(ACCOUNT).unwrap();
    assert!(before.holds(), "ledger: {before:?}; runtime: {report:?}");
    assert_eq!(before.deposited, deposited);
    assert_eq!(before.overage_recorded, CostUnits::ZERO);
    assert_eq!(store.usage_recorded(ACCOUNT), committed);
    assert_eq!(before.settled_usage, committed);
    assert_eq!(before.settlement_loss, CostUnits::ZERO);
    assert_eq!(before.expired, CostUnits::ZERO);
    if report.uncertain_acquires == 0 {
        assert_eq!(before.active_lease_grants, CostUnits::ZERO, "{report:?}");
    }

    // No server task remains. Advance only the store's explicit time input;
    // there is no sleep or extra measurement in this expiry observation.
    let reclaimed = store.reclaim_expired(reclaim_at).await.unwrap();
    assert!(
        u64::try_from(reclaimed.len()).unwrap() <= report.uncertain_acquires,
        "only reported unanswered acquisitions may remain: {reclaimed:?}; {report:?}"
    );
    let returned = reclaimed.iter().fold(CostUnits::ZERO, |sum, lease| {
        assert_eq!(lease.account_id, ACCOUNT);
        sum.checked_add(lease.reclaimed).unwrap()
    });
    assert_eq!(
        returned, before.active_lease_grants,
        "unanswered grants funded no usage"
    );
    let after = store.conservation(ACCOUNT).unwrap();
    assert!(after.holds(), "{after:?}");
    assert_eq!(after.active_lease_grants, CostUnits::ZERO);
    assert_eq!(after.balance, deposited.checked_sub(committed).unwrap());
    assert_eq!(store.usage_recorded(ACCOUNT), committed);
    assert_eq!(after.settled_usage, committed);
    assert_eq!(after.settlement_loss, CostUnits::ZERO);
    assert_eq!(after.expired, CostUnits::ZERO);
    reclaimed
}

#[tokio::test]
async fn shutdown_accounts_for_unanswered_grants_over_http() {
    controlled_shutdown(common::TransportMode::LoopbackBearer).await;
}

#[tokio::test]
async fn shutdown_accounts_for_unanswered_grants_over_tls() {
    controlled_shutdown(common::TransportMode::TlsBearer).await;
}

#[tokio::test]
async fn shutdown_accounts_for_unanswered_grants_over_mtls() {
    controlled_shutdown(common::TransportMode::Mtls).await;
}

async fn controlled_shutdown(mode: common::TransportMode) {
    for operation in [GrantOperation::Acquire, GrantOperation::Consolidate] {
        for deliver_result in [false, true] {
            // Freeze business time inside the generated TLS certificate's
            // validity window; expiry checks below advance only this input.
            let now = Timestamp::now();
            let clock = Arc::new(tollgate_client::ManualClock::new(now));
            let store = MemoryStore::new(GrantPolicy::default()).unwrap();
            store.create_account(AccountConfig {
                account_id: ACCOUNT,
                initial_balance: CostUnits(104),
                status: AccountStatus::Active,
                capacity_class: CapacityClass::Assured,
            });
            store
                .publish_snapshot(PRINCIPAL, publishable(snapshot()))
                .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (security, http) = common::transport(mode, listener.local_addr().unwrap());
            let (stop, stopping) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(serve(
                listener,
                ServerState {
                    security,
                    store: store.clone(),
                    clock: clock.clone(),
                },
                std::time::Duration::from_millis(200),
                async move {
                    let _ = stopping.await;
                },
            ));
            let (held, mut pending) = tokio::sync::mpsc::unbounded_channel();
            let allocator = held_grant_allocator(&http, operation, held);
            assert!(matches!(
                http.snapshot(PRINCIPAL).await.unwrap(),
                SnapshotResolution::Present(_)
            ));
            let mut config = runtime_config(PRINCIPAL);
            // 104 deposited -> 52 granted -> 51 billed, leaving 53 unspent.
            // A crossing acquires 26 beside the old lease; a refusal folds
            // its one-unit tail into a 26-unit replacement. Both can leave
            // exactly the reported 27-versus-53 balance if the reply is lost.
            config.leases.low_water = match operation {
                GrantOperation::Acquire => CostUnits(51),
                GrantOperation::Consolidate => CostUnits::ZERO,
            };
            let (runtime, handle) = tollgate_client::InstanceRuntime::spawn(
                http.clone(),
                allocator,
                http,
                clock,
                config,
            )
            .unwrap();
            wait_for_observation(|| handle.readiness(now).is_ready())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "readiness: {:?}; runtime: {:?}; accounts: {:?}",
                        handle.readiness(now),
                        handle.report(),
                        handle.account_reports(now)
                    )
                });
            let admitted = handle
                .begin(PRINCIPAL, PermissionBits::bit(0), now)
                .unwrap()
                .admit(
                    &[(PriceOp, 1)],
                    handle.recorder().try_reserve().unwrap(),
                    now,
                )
                .unwrap();
            let committed = admitted
                .acquire_capacity(&NoGate)
                .unwrap()
                .commit(RequestId(1), now)
                .unwrap();
            assert_eq!(committed.units(), CostUnits(51));
            drop(committed);
            if operation == GrantOperation::Consolidate {
                let context = handle
                    .begin(PRINCIPAL, PermissionBits::bit(0), now)
                    .unwrap();
                assert!(matches!(
                    context.admit(
                        &[(PriceOp, 1)],
                        handle.recorder().try_reserve().unwrap(),
                        now,
                    ),
                    Err(DenyReason::LeaseExhausted { .. })
                ));
            }
            let held = tokio::time::timeout(std::time::Duration::from_secs(5), pending.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(held.grant.units, CostUnits(26));
            let mut deliver = Some(held.deliver);
            if deliver_result {
                deliver.take().unwrap().send(()).unwrap();
                wait_for_observation(|| handle.report().refill.unwrap().acquired == 2)
                    .await
                    .unwrap();
            }
            let stopped = runtime.shutdown().await.unwrap();
            if let Some(deliver) = deliver {
                assert!(deliver.is_closed(), "shutdown cancelled the pending result");
            }
            stop.send(()).unwrap();
            server.await.unwrap().unwrap();
            let report = handle.report();
            let ledger = store.conservation(ACCOUNT).unwrap();
            assert!(ledger.holds(), "{ledger:?}");
            assert_eq!(store.usage_recorded(ACCOUNT), CostUnits(51));
            assert_eq!(report.uncertain_acquires, u64::from(!deliver_result));
            assert_eq!(
                handle.account_reports(now)[0].uncertain_acquires,
                report.uncertain_acquires
            );
            assert!(stopped.unfinished_accounts.is_empty());
            assert_eq!(
                ledger.balance,
                CostUnits(if deliver_result { 53 } else { 27 })
            );
            assert_eq!(
                ledger.active_lease_grants,
                CostUnits(if deliver_result { 0 } else { 26 })
            );
            // Expiry alone is not enough: the allocator also promises grace.
            assert!(
                store
                    .reclaim_expired(held.grant.expires_at)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(store.conservation(ACCOUNT).unwrap(), ledger);
            let reclaim_at = held
                .grant
                .expires_at
                .checked_add(GrantPolicy::default().reclaim_grace)
                .unwrap()
                .checked_add(SignedDuration::from_nanos(1))
                .unwrap();
            let reclaimed = assert_shutdown_accounting(
                &store,
                &handle,
                &stopped,
                CostUnits(104),
                CostUnits(51),
                reclaim_at,
            )
            .await;
            assert_eq!(reclaimed.len(), usize::from(!deliver_result));
            if !deliver_result {
                assert_eq!(reclaimed[0].lease_id, held.grant.lease_id);
                assert_eq!(reclaimed[0].reclaimed, CostUnits(26));
            }
            assert_eq!(
                handle.report().uncertain_acquires,
                report.uncertain_acquires,
                "expiry does not rewrite the runtime's historical observation"
            );
        }
    }
}

async fn full_stack(mode: common::TransportMode) {
    // Server side: memory backend, system clock, real listener on an
    // ephemeral port.
    let store = MemoryStore::new(GrantPolicy::default()).unwrap();
    store.create_account(AccountConfig {
        account_id: ACCOUNT,
        initial_balance: CostUnits(DEPOSIT),
        status: AccountStatus::Active,
        capacity_class: CapacityClass::Assured,
    });
    const HMAC_SECRET: &[u8] = b"fixture-customer-credential-hmac-secret-108";
    let minted = tollgate_auth::HmacRegistry::new(HMAC_SECRET)
        .mint(KeyId((1u128 << 127) | 108))
        .unwrap();
    let principal = minted.principal;
    tollgate_store::KeyDirectory::insert_key(
        &*store,
        tollgate_store::KeyRecord {
            key_id: minted.key_id,
            account_id: ACCOUNT,
            principal,
            digest: minted.digest,
            not_after: None,
        },
    )
    .await
    .unwrap();
    let second = tollgate_auth::HmacRegistry::new(HMAC_SECRET)
        .mint(tollgate_core::KeyId(109))
        .unwrap();
    tollgate_store::KeyDirectory::insert_key(
        &*store,
        tollgate_store::KeyRecord {
            key_id: second.key_id,
            account_id: ACCOUNT,
            principal: second.principal,
            digest: second.digest,
            not_after: None,
        },
    )
    .await
    .unwrap();
    let mut keyed = (*snapshot()).clone();
    keyed.key_id = Some(minted.key_id);
    store
        .publish_snapshot(principal, publishable(Arc::new(keyed)))
        .expect("snapshot fixture matches its account and credential");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (security, http) = common::transport(mode, address);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(
        listener,
        ServerState {
            security,
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock),
        },
        std::time::Duration::from_millis(200),
        async move {
            let _ = stop_rx.await;
        },
    ));

    // Instance side: everything over HTTP.
    let clock = Arc::new(SystemClock);
    let keys = tollgate_client::KeyManager::spawn(
        http.clone(),
        HMAC_SECRET,
        clock.clone(),
        tollgate_client::KeyManagerConfig {
            refresh_interval: std::time::Duration::from_millis(20),
            page_limit: std::num::NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let verifier = keys.verifier();
    let key_monitor = keys.monitor();
    let session = tollgate_auth::SessionCredential::new();

    // Cold fetch of the snapshot through the transport (pull path), plus the
    // negative case for an unknown principal.
    let SnapshotResolution::Present(fetched) = http.snapshot(principal).await.unwrap() else {
        panic!("published snapshot must be present");
    };
    assert_eq!(fetched.generation, Generation(1));
    // The revision survived store -> HTTP -> client. The server decodes into
    // an `AccountSnapshot` and reserializes, so a field it did not carry would
    // be stripped exactly here (#94).
    assert_eq!(fetched.policy_revision, REVISION);
    assert!(matches!(
        http.snapshot(Principal(999)).await.unwrap(),
        SnapshotResolution::Unknown
    ));

    let (runtime, engine) = tollgate_client::InstanceRuntime::spawn(
        http.clone(),
        http.clone(),
        http.clone(),
        clock,
        runtime_config(principal),
    )
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !(engine.readiness(Timestamp::now()).is_ready()
            && key_monitor.report(Timestamp::now()).ready)
        {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let projected = key_monitor.report(Timestamp::now());
    assert_eq!(projected.projected_keys, 2);
    assert!(projected.stats.pages >= 2);
    assert_eq!(
        tollgate_auth::CredentialVerifier::verify(&verifier, &second.secret)
            .unwrap()
            .principal,
        second.principal
    );
    let recorder = engine.recorder();

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
            let verified = session
                .authenticate(Some(&minted.secret), &verifier, Timestamp::now())
                .expect("HTTP-projected customer key must authenticate");
            assert_eq!(verified, principal);
            match engine
                .begin(verified, PermissionBits::bit(0), Timestamp::now())
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
    let report = runtime.shutdown().await.unwrap();
    let stats = report.usage.as_ref().unwrap();
    assert_eq!(stats.unattributed, 0);
    assert_eq!(stats.attribution_unreported_batches, 0);
    let activity = tollgate_store::KeyDirectory::credential_activity(&*store, &[minted.key_id])
        .await
        .unwrap();
    assert!(matches!(
        activity[0].state,
        tollgate_store::CredentialActivityState::Committed { .. }
    ));
    let key_shutdown = keys.shutdown().await;
    assert!(!key_shutdown.task_failed && !key_shutdown.deadline_expired);
    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();

    let policy = GrantPolicy::default();
    let reclaim_at = Timestamp::now()
        .checked_add(policy.max_ttl)
        .unwrap()
        .checked_add(policy.reclaim_grace)
        .unwrap();
    assert_shutdown_accounting(
        &store,
        &engine,
        &report,
        CostUnits(DEPOSIT),
        CostUnits(committed_units),
        reclaim_at,
    )
    .await;
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
        capacity_class: CapacityClass::Assured,
    });
    store
        .publish_snapshot(PRINCIPAL, publishable(snapshot()))
        .expect("snapshot fixture matches its account and credential");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(
        listener,
        ServerState {
            security: common::security(),
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock),
        },
        std::time::Duration::from_secs(60),
        async move {
            let _ = stop_rx.await;
        },
    ));

    let http = common::http(format!("http://{address}"));
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
    store
        .publish_snapshot(LATER, publishable(snapshot()))
        .expect("snapshot fixture matches its account and credential");

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
        capacity_class: CapacityClass::Assured,
    });
    let app = router(ServerState {
        security: common::security(),
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

    // Past the declared body limit, and *measured* past it rather than
    // estimated. This previously divided the limit by a hardcoded 163-byte
    // guess, under a comment claiming no reliance on a number that would rot
    // if a field were added — and then #94 added a field, taking the body from
    // 8% over the limit to 64% over. Being far over is not harmlessly safer:
    // the server rejects and closes while the client is still writing, so the
    // clean 413 this test asserts becomes a connection reset often enough to
    // fail intermittently.
    //
    // Deriving the count from the encoded size keeps the batch just over the
    // limit, where the server reads the body and answers with its own code,
    // and keeps it there for whatever the next field costs.
    //
    // Built directly rather than through `UsageWriter`, whose `max_batch`
    // validation now makes an oversized batch unreachable — which is the
    // point: the transport must still classify correctly for the sinks and
    // versions validation cannot reach.
    let event = |i: u128| {
        UsageEvent::new(
            RequestId(i),
            ACCOUNT,
            UsageSource::Overage,
            CostUnits(1),
            at(0),
            PolicyRevision::UNSTATED,
            None,
        )
    };
    let encoded_event = serde_json::to_string(&event(0))
        .expect("a usage event serialises")
        .len()
        + 1; // the separating comma
    let events_needed = MAX_INGEST_BODY_BYTES / encoded_event + 64;
    let events: Vec<UsageEvent> = (0..events_needed as u128).map(event).collect();

    let http = common::http(format!("http://{address}"));
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

        let http = common::http(format!("http://{address}"));
        let event = UsageEvent::new(
            RequestId(1),
            ACCOUNT,
            UsageSource::Overage,
            CostUnits(1),
            at(0),
            PolicyRevision::UNSTATED,
            None,
        );
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
