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
//!   `POST /v1/leases/reclaim` — fenced lease lifecycle.
//! - `GET /v1/snapshots/{principal}` — compiled snapshot fetch.
//! - `POST /v1/usage/ingest` — idempotent usage batches.
//! - `POST /v1/admin/accounts`, `POST /v1/admin/accounts/{id}/deposit`,
//!   `POST /v1/admin/accounts/{id}/status`,
//!   `PUT/DELETE /v1/admin/snapshots/{principal}` — administration.
//! - `GET /livez`, `GET /readyz` — probes.
//!
//! The PoC binds to loopback and carries no authentication of its own; the
//! control-plane credential story (HMAC-verified operator keys) is a
//! documented seam in `docs/DESIGN.md`, not an accident.

pub mod error;

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use jiff::SignedDuration;
use tracing::Instrument as _;

use tollgate_core::{AccountId, CostUnits, Principal};
use tollgate_store::wire::{
    AcquireRequest, AcquireResponse, CreateAccountRequest, DepositRequest, IngestRequest,
    PublishSnapshotRequest, ReleaseRequest, SetStatusRequest,
};
use tollgate_store::{
    AccountConfig, AdminStore, Clock, IngestReport, LeaseAllocator, ReclaimedLease,
    SnapshotResolution, SnapshotSource, StoreHealth, UsageSink,
};

use crate::error::ApiError;

/// Everything the handlers need. `S` is the storage backend; the clock is the
/// single place wall time enters the server.
pub struct ServerState<S> {
    pub store: Arc<S>,
    pub clock: Arc<dyn Clock>,
}

impl<S> Clone for ServerState<S> {
    fn clone(&self) -> Self {
        ServerState {
            store: Arc::clone(&self.store),
            clock: Arc::clone(&self.clock),
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
    Router::new()
        .route("/livez", get(async || StatusCode::OK))
        .route("/readyz", get(readyz::<S>))
        .route("/v1/leases/acquire", post(acquire::<S>))
        .route("/v1/leases/release", post(release::<S>))
        .route("/v1/leases/reclaim", post(reclaim::<S>))
        .route("/v1/snapshots/{principal}", get(fetch_snapshot::<S>))
        .route("/v1/usage/ingest", post(ingest::<S>))
        .route("/v1/admin/accounts", post(create_account::<S>))
        .route("/v1/admin/accounts/{account}/deposit", post(deposit::<S>))
        .route("/v1/admin/accounts/{account}/status", post(set_status::<S>))
        .route(
            "/v1/admin/snapshots/{principal}",
            put(publish_snapshot::<S>).delete(remove_snapshot::<S>),
        )
        .with_state(state)
}

/// Serve until `shutdown` resolves, running the expiry-reclaim sweep every
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
    let sweep_store = Arc::clone(&state.store);
    let sweep_clock = Arc::clone(&state.clock);
    let sweeper = tokio::spawn(
        reclaim_sweep(sweep_store, sweep_clock, reclaim_interval).instrument(tracing::info_span!(
            "reclaim_sweep",
            interval_ms = reclaim_interval.as_millis()
        )),
    );
    let result = axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await;
    sweeper.abort();
    result
}

/// Reclaim expired leases forever, on the configured interval.
///
/// This is INVARIANTS.md #9's server half, and it is the only thing that
/// returns units stranded by a crashed holder. A sweep that fails every tick
/// breaks that guarantee indefinitely while `/readyz` still answers, because
/// a store can serve `ping` and fail `reclaim_expired` — so the failure must
/// be said out loud. Both outcomes are reported: silence about success would
/// leave "the sweep is running but finding nothing" and "the sweep stopped"
/// indistinguishable.
async fn reclaim_sweep<S: Backend>(
    store: Arc<S>,
    clock: Arc<dyn Clock>,
    interval: std::time::Duration,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut consecutive_failures: u64 = 0;
    loop {
        tick.tick().await;
        match store.reclaim_expired(clock.now()).await {
            Ok(reclaimed) => {
                if consecutive_failures > 0 {
                    tracing::info!(
                        after_failures = consecutive_failures,
                        "reclaim sweep recovered"
                    );
                    consecutive_failures = 0;
                }
                if !reclaimed.is_empty() {
                    let units: u64 = reclaimed.iter().map(|lease| lease.reclaimed.get()).sum();
                    tracing::info!(leases = reclaimed.len(), units, "reclaimed expired leases");
                }
            }
            Err(error) => {
                consecutive_failures += 1;
                tracing::warn!(
                    %error,
                    consecutive_failures,
                    "reclaim sweep failed; expired leases stay stranded until it recovers"
                );
            }
        }
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
    State(state): State<ServerState<S>>,
    Json(request): Json<AcquireRequest>,
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
    State(state): State<ServerState<S>>,
    Json(request): Json<ReleaseRequest>,
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

async fn reclaim<S: Backend>(
    State(state): State<ServerState<S>>,
) -> Result<Json<Vec<ReclaimedLease>>, ApiError> {
    Ok(Json(state.store.reclaim_expired(state.clock.now()).await?))
}

async fn fetch_snapshot<S: Backend>(
    State(state): State<ServerState<S>>,
    Path(principal): Path<u128>,
) -> Result<Json<Arc<tollgate_core::AccountSnapshot>>, ApiError> {
    match state.store.snapshot(Principal(principal)).await? {
        SnapshotResolution::Present(snapshot) => Ok(Json(snapshot)),
        SnapshotResolution::Revoked { generation } => Err(ApiError::revoked(generation)),
        SnapshotResolution::Unknown => Err(ApiError::not_found("unknown-principal", "no snapshot")),
    }
}

async fn ingest<S: Backend>(
    State(state): State<ServerState<S>>,
    Json(request): Json<IngestRequest>,
) -> Result<Json<IngestReport>, ApiError> {
    Ok(Json(
        state
            .store
            .ingest(&request.events, state.clock.now())
            .await?,
    ))
}

async fn create_account<S: Backend>(
    State(state): State<ServerState<S>>,
    Json(request): Json<CreateAccountRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .store
        .create_account(AccountConfig {
            account_id: request.account_id,
            initial_balance: request.initial_balance,
            active: request.active,
        })
        .await?;
    Ok(StatusCode::CREATED)
}

async fn deposit<S: Backend>(
    State(state): State<ServerState<S>>,
    Path(account): Path<u128>,
    Json(request): Json<DepositRequest>,
) -> Result<StatusCode, ApiError> {
    if request.units == CostUnits::ZERO {
        return Err(ApiError::bad_request(
            "zero-deposit",
            "deposit must be positive",
        ));
    }
    state
        .store
        .deposit(AccountId(account), request.units)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn set_status<S: Backend>(
    State(state): State<ServerState<S>>,
    Path(account): Path<u128>,
    Json(request): Json<SetStatusRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .store
        .set_active(AccountId(account), request.active)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn publish_snapshot<S: Backend>(
    State(state): State<ServerState<S>>,
    Path(principal): Path<u128>,
    Json(request): Json<PublishSnapshotRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .store
        .publish_snapshot(Principal(principal), request.snapshot)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_snapshot<S: Backend>(
    State(state): State<ServerState<S>>,
    Path(principal): Path<u128>,
) -> Result<StatusCode, ApiError> {
    state.store.remove_snapshot(Principal(principal)).await?;
    Ok(StatusCode::NO_CONTENT)
}
