//! The control-plane HTTP service.
//!
//! Latency-tolerant by design — its contract is correctness (no double-spend,
//! fencing, idempotent ingest), enforced by whatever [`tollgate_store`] backend
//! it is built over. The service is generic over that backend: anything
//! implementing the four store traits serves identically, which is the
//! pluggability seam the Postgres backend drops into.
//!
//! Surface (`/v1`):
//! - `POST /v1/leases/acquire`, `POST /v1/leases/release`,
//!   `POST /v1/leases/consolidate`, `POST /v1/leases/reclaim` — fenced lease
//!   lifecycle.
//! - `GET /v1/snapshots` — the principal catalogue (#48).
//! - `GET /v1/snapshots/{principal}` — compiled snapshot fetch.
//! - `POST /v1/usage/ingest` — idempotent usage batches.
//! - `POST /v1/admin/accounts`, `POST /v1/admin/accounts/{id}/deposit`,
//!   `POST /v1/admin/accounts/{id}/status`,
//!   `POST /v1/admin/accounts/{id}/capacity-class`,
//!   `PUT/DELETE /v1/admin/snapshots/{principal}` — administration.
//! - `GET /livez`, `GET /readyz` — probes.
//!
//! All protected handlers require verified instance or operator evidence.
//! [`serve`] owns TLS and refuses exposed plaintext; [`security::ServerSecurity`]
//! atomically rotates roles, credentials and TLS. Administrative audit events
//! carry receipts captured by the backend at mutation. See
//! `docs/CONTROL_PLANE_SECURITY.md` for deployment and audit collection.

pub mod config;
pub mod error;
pub mod google;
pub mod security;
pub mod transport;

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use jiff::SignedDuration;
use tracing::Instrument as _;

use tollgate_core::{AccountId, CapacityClass, CostUnits, Principal, PublishableSnapshot};
use tollgate_store::wire::{
    API_PREFIX, AcquireRequest, AcquireResponse, ConsolidateRequest, ConsolidateResponse,
    CreateAccountRequest, DepositRequest, IngestRequest, MAX_INGEST_BODY_BYTES,
    MAX_SNAPSHOT_BODY_BYTES, PrincipalsResponse, PublishSnapshotRequest, ReleaseRequest,
    SetCapacityClassRequest, SetStatusRequest, SetStatusResponse,
};
use tollgate_store::{
    AccountConfig, AdminStore, Clock, DEFAULT_RECLAIM_BATCH_LIMIT, DEFAULT_ROLLOVER_BATCH_LIMIT,
    IngestReport, LeaseAllocator, ReclaimBatch, ReclaimedLease, SnapshotResolution, SnapshotSource,
    StoreError, StoreHealth, UsageSink,
};

use crate::error::{ApiError, ApiJson, ApiPath};
use crate::security::{Authorization, InstanceIdentity, OperatorIdentity, Role, ServerSecurity};

/// Everything the handlers need. `S` is the storage backend; the clock is the
/// single place wall time enters the server.
pub struct ServerState<S> {
    pub store: Arc<S>,
    pub clock: Arc<dyn Clock>,
    pub security: Arc<ServerSecurity>,
}

impl<S> Clone for ServerState<S> {
    fn clone(&self) -> Self {
        ServerState {
            store: Arc::clone(&self.store),
            clock: Arc::clone(&self.clock),
            security: Arc::clone(&self.security),
        }
    }
}

/// The full store bound the server needs from a backend.
pub trait Backend:
    LeaseAllocator + SnapshotSource + UsageSink + AdminStore + StoreHealth + Send + Sync + 'static
{
}
impl<T> Backend for T where
    T: LeaseAllocator
        + SnapshotSource
        + UsageSink
        + AdminStore
        + StoreHealth
        + Send
        + Sync
        + 'static
{
}

pub fn router<S: Backend>(state: ServerState<S>) -> Router {
    let authorization = |role| Authorization {
        security: Arc::clone(&state.security),
        clock: Arc::clone(&state.clock),
        role,
    };
    let instance = Router::new()
        .route("/leases/acquire", post(acquire::<S>))
        .route("/leases/release", post(release::<S>))
        .route("/leases/consolidate", post(consolidate::<S>))
        .route("/leases/reclaim", post(reclaim::<S>))
        .route("/snapshots", get(list_principals::<S>))
        .route("/snapshots/{principal}", get(fetch_snapshot::<S>))
        .route(
            "/usage/ingest",
            post(ingest::<S>).layer(DefaultBodyLimit::max(MAX_INGEST_BODY_BYTES)),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            authorization(Role::Instance),
            security::authorize,
        ));
    let operator = Router::new()
        .route("/accounts", post(create_account::<S>))
        .route("/accounts/{account}/deposit", post(deposit::<S>))
        .route("/accounts/{account}/status", post(set_status::<S>))
        .route(
            "/accounts/{account}/capacity-class",
            post(set_capacity_class::<S>),
        )
        .route(
            "/snapshots/{principal}",
            put(publish_snapshot::<S>)
                .delete(remove_snapshot::<S>)
                .layer(DefaultBodyLimit::max(MAX_SNAPSHOT_BODY_BYTES)),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            authorization(Role::Operator),
            security::authorize,
        ));
    Router::new()
        .route("/livez", get(async || StatusCode::OK))
        .route("/readyz", get(readyz::<S>))
        .nest(API_PREFIX, instance.nest("/admin", operator))
        .with_state(state)
}

/// Serve until `shutdown` resolves, running the maintenance sweep every
/// `reclaim_interval` in the background (INVARIANTS.md #9's server half).
pub async fn serve<S: Backend>(
    listener: tokio::net::TcpListener,
    state: ServerState<S>,
    reclaim_interval: std::time::Duration,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    if reclaim_interval.is_zero() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reclaim_interval must be positive",
        ));
    }
    let listener = transport::SecureListener::new(listener, Arc::clone(&state.security))?;
    let sweep_store = Arc::clone(&state.store);
    let sweep_clock = Arc::clone(&state.clock);
    let sweeper = MaintenanceTask(tokio::spawn(
        maintenance_sweep(sweep_store, sweep_clock, reclaim_interval).instrument(
            tracing::info_span!(
                "maintenance_sweep",
                interval_ms = reclaim_interval.as_millis()
            ),
        ),
    ));
    let result = axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<transport::PeerIdentity>(),
    )
    .with_graceful_shutdown(shutdown)
    .await;
    drop(sweeper);
    result
}

struct MaintenanceTask(tokio::task::JoinHandle<()>);
impl Drop for MaintenanceTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Reclaim expired leases and roll due budget periods forever, on the
/// configured interval.
///
/// This is INVARIANTS.md #9's server half, and it is the only thing that
/// returns units stranded by a crashed holder. A sweep that fails every tick
/// breaks that guarantee indefinitely while `/readyz` still answers, because
/// a store can serve `ping` and fail `reclaim_expired` — so the failure must
/// be said out loud. Both outcomes are reported: silence about success would
/// leave "the sweep is running but finding nothing" and "the sweep stopped"
/// indistinguishable.
///
/// The rollover pass (#97) shares this tick rather than owning a timer,
/// because it needs the same three things and gets them here already: a frozen
/// cutoff, a bounded drain, and a failure that is reported rather than
/// swallowed. It is also the only trigger — without it a schedule is a stored
/// intention nothing ever acts on — and a boundary the pass misses is not lost
/// but late, since the store crosses it on the next tick that sees it. The two
/// passes are independent: one failing must not stop the other, because a
/// stuck rollover stranding quota would be a strictly worse outcome than a
/// late allowance.
async fn maintenance_sweep<S: Backend>(
    store: Arc<S>,
    clock: Arc<dyn Clock>,
    interval: std::time::Duration,
) {
    #[derive(Default)]
    struct Progress {
        leases: u128,
        units: u128,
        batches: u64,
    }

    impl Progress {
        fn record(&mut self, batch: &ReclaimBatch) -> Result<(), StoreError> {
            let batch_leases =
                u128::from(u64::try_from(batch.len()).map_err(|_| {
                    StoreError("reclaim batch lease count exceeds u64 range".into())
                })?);
            let batch_units = batch.reclaimed().iter().try_fold(0u128, |total, lease| {
                total
                    .checked_add(u128::from(lease.reclaimed.get()))
                    .ok_or_else(|| StoreError("reclaim batch unit total overflow".into()))
            })?;
            let leases = self
                .leases
                .checked_add(batch_leases)
                .ok_or_else(|| StoreError("reclaim cycle lease count overflow".into()))?;
            let units = self
                .units
                .checked_add(batch_units)
                .ok_or_else(|| StoreError("reclaim cycle unit total overflow".into()))?;
            let batches = self
                .batches
                .checked_add(1)
                .ok_or_else(|| StoreError("reclaim cycle batch count overflow".into()))?;
            *self = Progress {
                leases,
                units,
                batches,
            };
            Ok(())
        }
    }

    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut consecutive_failures: u64 = 0;
    loop {
        tick.tick().await;
        // Freeze the cutoff for this cycle. A continuously advancing cutoff
        // could make a busy allocator feed the drain forever; a fixed one is
        // a finite backlog and still returns all quota expired at this tick.
        let now = clock.now();
        let mut progress = Progress::default();
        let outcome = loop {
            match store
                .reclaim_expired_batch(now, DEFAULT_RECLAIM_BATCH_LIMIT)
                .await
            {
                Ok(batch) => {
                    if let Err(error) = progress.record(&batch) {
                        break Err(error);
                    }
                    if !batch.is_saturated() {
                        break Ok(());
                    }
                    // MemoryStore can finish a batch without an .await that
                    // yields. Let request handlers run between backlog chunks;
                    // PostgreSQL benefits from the same explicit fairness.
                    tokio::task::yield_now().await;
                }
                Err(error) => break Err(error),
            }
        };

        roll_due_periods(store.as_ref(), now).await;

        match outcome {
            Ok(()) => {
                if consecutive_failures > 0 {
                    tracing::info!(
                        after_failures = consecutive_failures,
                        "reclaim sweep recovered"
                    );
                    consecutive_failures = 0;
                }
                if progress.leases > 0 {
                    tracing::info!(
                        leases = %progress.leases,
                        units = %progress.units,
                        batches = progress.batches,
                        "reclaimed expired leases"
                    );
                }
            }
            Err(error) => {
                consecutive_failures += 1;
                tracing::warn!(
                    %error,
                    consecutive_failures,
                    reclaimed_leases = %progress.leases,
                    reclaimed_units = %progress.units,
                    completed_batches = progress.batches,
                    "reclaim sweep failed; completed batches stay committed and remaining expired leases stay stranded until it recovers"
                );
            }
        }
    }
}

/// Drain the rollover pass for one tick.
///
/// Bounded per transaction and drained to a partial batch, exactly as the
/// reclaim loop above is, and for a sharper reason: every scheduled account is
/// due at the same instant, so the first tick after midnight on the 1st has
/// the whole scheduled population to cross. A single unbounded statement there
/// would hold locks across the entire account table.
///
/// Failures are reported and the tick ends. Retrying inside the tick would
/// spin against a store that is down; the next tick is the retry, and until it
/// succeeds the affected accounts keep spending last period's allowance, which
/// is late rather than wrong.
async fn roll_due_periods<S: Backend>(store: &S, now: jiff::Timestamp) {
    let mut accounts: u64 = 0;
    let mut batches: u64 = 0;
    loop {
        match store
            .roll_due_periods(now, DEFAULT_ROLLOVER_BATCH_LIMIT)
            .await
        {
            Ok(batch) => {
                let Some(total) = u64::try_from(batch.len())
                    .ok()
                    .and_then(|rolled| accounts.checked_add(rolled))
                    .zip(batches.checked_add(1))
                else {
                    // Unreachable short of a backend ignoring the batch limit
                    // forever, and still not a place to wrap: a counter that
                    // silently restarts would under-report a boundary that
                    // rolled more accounts than it claimed.
                    tracing::warn!(
                        accounts,
                        batches,
                        "budget rollover progress counter overflowed; stopping this tick"
                    );
                    return;
                };
                (accounts, batches) = (total.0, total.1);
                if !batch.is_saturated() {
                    break;
                }
                // MemoryStore can finish a batch without an .await that
                // yields; let request handlers run between chunks.
                tokio::task::yield_now().await;
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    rolled_accounts = accounts,
                    completed_batches = batches,
                    "budget rollover failed; accounts past their boundary keep last period's allowance until it recovers"
                );
                return;
            }
        }
    }
    if accounts > 0 {
        tracing::info!(accounts, batches, "rolled budget periods");
    }
}

/// Ready only when the backing store answers (review finding #11): a server
/// whose source of truth is unreachable must not attract traffic.
async fn readyz<S: Backend>(State(state): State<ServerState<S>>) -> StatusCode {
    match state.store.ping().await {
        Ok(()) => StatusCode::OK,
        Err(error) => {
            // The 503 says "not ready"; only the event says why, and "why"
            // is the difference between a restart and a page.
            tracing::warn!(%error, "readiness probe failed: the store did not answer");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn acquire<S: Backend>(
    _identity: InstanceIdentity,
    State(state): State<ServerState<S>>,
    ApiJson(request): ApiJson<AcquireRequest>,
) -> Result<Json<AcquireResponse>, ApiError> {
    let grant = state
        .store
        .acquire(
            request.account_id,
            request.requested,
            SignedDuration::from_secs(i64::from(request.ttl_seconds)),
            state.clock.now(),
        )
        .await?;
    Ok(Json(grant))
}

async fn release<S: Backend>(
    _identity: InstanceIdentity,
    State(state): State<ServerState<S>>,
    ApiJson(request): ApiJson<ReleaseRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .store
        .release(
            request.lease_id,
            request.fencing_token,
            request.unspent,
            state.clock.now(),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Return a lease's unspent units and re-grant against the restored balance,
/// in one backend transaction.
///
/// It is a route rather than two calls the instance could make itself for the
/// reason the trait method exists: split across the wire, another instance can
/// take the returned units in the gap, and the grant policy can hand back less
/// than was returned (`LeaseAllocator::consolidate`).
async fn consolidate<S: Backend>(
    _identity: InstanceIdentity,
    State(state): State<ServerState<S>>,
    ApiJson(request): ApiJson<ConsolidateRequest>,
) -> Result<Json<ConsolidateResponse>, ApiError> {
    let grant = state
        .store
        .consolidate(
            request.lease_id,
            request.fencing_token,
            request.unspent,
            request.requested,
            SignedDuration::from_secs(i64::from(request.ttl_seconds)),
            state.clock.now(),
        )
        .await?;
    Ok(Json(grant))
}

async fn reclaim<S: Backend>(
    _identity: InstanceIdentity,
    State(state): State<ServerState<S>>,
) -> Result<Json<Vec<ReclaimedLease>>, ApiError> {
    Ok(Json(state.store.reclaim_expired(state.clock.now()).await?))
}

async fn fetch_snapshot<S: Backend>(
    _identity: InstanceIdentity,
    State(state): State<ServerState<S>>,
    ApiPath(principal): ApiPath<Principal>,
) -> Result<Json<Arc<tollgate_core::AccountSnapshot>>, ApiError> {
    match state.store.snapshot(principal).await? {
        SnapshotResolution::Present(snapshot) => Ok(Json(snapshot.into_inner())),
        SnapshotResolution::Revoked { generation } => Err(ApiError::revoked(generation)),
        SnapshotResolution::Unknown => Err(ApiError::not_found("unknown-principal", "no snapshot")),
    }
}

/// The principal catalogue, for instances that track every customer rather
/// than a configured slice (#48).
///
/// A backend that cannot enumerate answers 501 rather than an empty list: an
/// empty catalogue and an unsupported one lead an instance to opposite
/// conclusions — forget everything, or keep what you were configured with —
/// and conflating them would silently strand it on a stale set.
async fn list_principals<S: Backend>(
    _identity: InstanceIdentity,
    State(state): State<ServerState<S>>,
) -> Result<Json<PrincipalsResponse>, ApiError> {
    match state.store.principals().await? {
        Some(principals) => Ok(Json(PrincipalsResponse { principals })),
        None => Err(ApiError::not_implemented(
            "enumeration-unsupported",
            "this backend cannot list principals",
        )),
    }
}

async fn ingest<S: Backend>(
    _identity: InstanceIdentity,
    State(state): State<ServerState<S>>,
    ApiJson(request): ApiJson<IngestRequest>,
) -> Result<Json<IngestReport>, ApiError> {
    Ok(Json(
        state
            .store
            .ingest(&request.events, state.clock.now())
            .await?,
    ))
}

async fn create_account<S: Backend>(
    operator: OperatorIdentity,
    State(state): State<ServerState<S>>,
    ApiJson(request): ApiJson<CreateAccountRequest>,
) -> Result<StatusCode, ApiError> {
    operator
        .run(
            "create_account",
            request.account_id,
            state.clock.as_ref(),
            state.store.create_account(AccountConfig {
                account_id: request.account_id,
                initial_balance: request.initial_balance,
                status: request.status,
                capacity_class: CapacityClass::Assured,
            }),
        )
        .await?;
    Ok(StatusCode::CREATED)
}

async fn deposit<S: Backend>(
    operator: OperatorIdentity,
    State(state): State<ServerState<S>>,
    ApiPath(account): ApiPath<AccountId>,
    ApiJson(request): ApiJson<DepositRequest>,
) -> Result<StatusCode, ApiError> {
    if request.units == CostUnits::ZERO {
        return Err(ApiError::bad_request(
            "zero-deposit",
            "deposit must be positive",
        ));
    }
    operator
        .run(
            "deposit",
            account,
            state.clock.as_ref(),
            state.store.deposit(account, request.units),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn set_status<S: Backend>(
    operator: OperatorIdentity,
    State(state): State<ServerState<S>>,
    ApiPath(account): ApiPath<AccountId>,
    ApiJson(request): ApiJson<SetStatusRequest>,
) -> Result<Json<SetStatusResponse>, ApiError> {
    // 200 with the blast radius, not 204: the ledger half is one row, but the
    // snapshot half is however many credentials the account has, and an
    // operator has no other way to learn which (#51).
    let change = operator
        .run(
            "set_account_status",
            account,
            state.clock.as_ref(),
            state.store.set_account_status(account, request.status),
        )
        .await?;
    Ok(Json(SetStatusResponse {
        republished: change.republished,
        unreadable: change.unreadable,
    }))
}

async fn set_capacity_class<S: Backend>(
    operator: OperatorIdentity,
    State(state): State<ServerState<S>>,
    ApiPath(account): ApiPath<AccountId>,
    ApiJson(request): ApiJson<SetCapacityClassRequest>,
) -> Result<Json<SetStatusResponse>, ApiError> {
    // 200 with the blast radius, for the reason `set_status` returns one: the
    // ledger half is one row and the snapshot half is however many credentials
    // the account has (#99).
    let change = operator
        .run(
            "set_capacity_class",
            account,
            state.clock.as_ref(),
            state
                .store
                .set_capacity_class(account, request.capacity_class),
        )
        .await?;
    Ok(Json(SetStatusResponse {
        republished: change.republished,
        unreadable: change.unreadable,
    }))
}

async fn publish_snapshot<S: Backend>(
    operator: OperatorIdentity,
    State(state): State<ServerState<S>>,
    ApiPath(principal): ApiPath<Principal>,
    ApiJson(request): ApiJson<PublishSnapshotRequest>,
) -> Result<StatusCode, ApiError> {
    let snapshot = PublishableSnapshot::try_new(request.snapshot)?;
    operator
        .run(
            "publish_snapshot",
            principal,
            state.clock.as_ref(),
            state.store.publish_snapshot(principal, snapshot),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_snapshot<S: Backend>(
    operator: OperatorIdentity,
    State(state): State<ServerState<S>>,
    ApiPath(principal): ApiPath<Principal>,
) -> Result<StatusCode, ApiError> {
    operator
        .run(
            "remove_snapshot",
            principal,
            state.clock.as_ref(),
            state.store.remove_snapshot(principal),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
