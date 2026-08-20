//! A minimal "concrete API" built on the tollgate stack — the
//! pluggability acceptance test from the plan: can a real web service embed
//! the abstract product without the product knowing anything about pricing?
//!
//! Request flow (the production hot path from the design thread):
//!
//! ```text
//! Authorization: Bearer <key>
//!   → HMAC-SHA256 verify (constant-time), derive Principal fingerprint
//!   → reserve usage-writer permit          (shed on backpressure, #8)
//!   → AdmissionEngine::admit               (snapshot/permissions/rate/lease)
//!   → commit_at_execution_start
//!   → price (toy Black-Scholes kernel)
//!   → permit.record(usage event)
//!   → respond { prices, metadata: { request_id, units_charged } }
//! ```
//!
//! The embedded topology runs `MemoryStore` in-process; pointing the same
//! stack at a `tollgate-server` is a one-line swap to `HttpStore` (see the
//! loopback test in tollgate-server). Readiness reports 503 until the account's
//! lease slot is stocked (INVARIANTS.md #10).

use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use hmac::{Hmac, Mac};
use jiff::{SignedDuration, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use tollgate_admission::{AdmissionEngine, AdmissionRequest, ArcSwapSnapshotMap, LeaseSlot};
use tollgate_client::{
    ChargeGuard, Clock, LeaseManager, LeaseManagerConfig, SlotRegistry, SnapshotManager,
    SnapshotManagerConfig, SystemClock, UsageRecorder, UsageWriter, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, DenyReason, Generation,
    OpIndex, PermissionBits, Principal, RequestId, ResolvedLimits,
};
use tollgate_store::{AccountConfig, GrantPolicy, MemoryStore};

type HmacSha256 = Hmac<Sha256>;

// ---- the service's own vocabulary, mapped onto the abstract product ----

#[derive(Clone, Copy)]
enum Op {
    Price,
}

impl OpIndex for Op {
    fn index(&self) -> usize {
        0
    }
}

const PERMISSION_PRICE: PermissionBits = PermissionBits(1);

// ---- credential verification (the auth seam) ---------------------------

/// API keys verified by HMAC-SHA256 under a server secret. The truncated MAC
/// is the [`Principal`] fingerprint the admission layer is keyed by; the
/// full MAC is compared in constant time so truncation can never be the
/// deciding comparison.
struct AuthRegistry {
    secret: Vec<u8>,
    keys: HashMap<u128, [u8; 32]>,
}

impl AuthRegistry {
    fn mac(&self, api_key: &str) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("any key length works");
        mac.update(api_key.as_bytes());
        mac.finalize().into_bytes().into()
    }

    fn fingerprint(mac: &[u8; 32]) -> u128 {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&mac[..16]);
        u128::from_be_bytes(bytes)
    }

    fn register(&mut self, api_key: &str) -> Principal {
        let mac = self.mac(api_key);
        let fingerprint = Self::fingerprint(&mac);
        self.keys.insert(fingerprint, mac);
        Principal(fingerprint)
    }

    fn verify(&self, api_key: &str) -> Option<Principal> {
        let mac = self.mac(api_key);
        let fingerprint = Self::fingerprint(&mac);
        let stored = self.keys.get(&fingerprint)?;
        if stored.ct_eq(&mac).into() {
            Some(Principal(fingerprint))
        } else {
            None
        }
    }
}

// ---- wire types --------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct PriceRequest {
    pub contracts: Vec<Contract>,
}

#[derive(Debug, Deserialize)]
pub struct Contract {
    pub spot: f64,
    pub strike: f64,
    pub rate: f64,
    pub vol: f64,
    pub tte_years: f64,
}

#[derive(Debug, Serialize)]
pub struct PriceResponse {
    pub prices: Vec<f64>,
    pub metadata: ResponseMetadata,
}

/// Mirrors ferro-risk's wire handle: enough for a client to reconcile the
/// charge post-hoc.
#[derive(Debug, Serialize)]
pub struct ResponseMetadata {
    pub request_id: String,
    pub units_charged: u64,
}

#[derive(Debug, Serialize)]
struct Problem {
    status: u16,
    code: &'static str,
    title: String,
    units_charged: u64,
}

// ---- app ---------------------------------------------------------------

struct AppState {
    auth: AuthRegistry,
    engine: AdmissionEngine<Arc<ArcSwapSnapshotMap>>,
    recorder: Option<UsageRecorder>,
    slot: Arc<LeaseSlot>,
    /// Refill-task health; channel closure exposes panic or abort.
    lease_manager_health: Option<tokio::sync::watch::Receiver<bool>>,
    /// Snapshot-manager readiness: true while every tracked principal has a
    /// fresh resolution; channel closure also exposes task failure.
    snapshots_ready: Option<tokio::sync::watch::Receiver<bool>>,
    /// `false` builds the no-admission baseline the load gate compares
    /// against: same transport, same kernel, zero quota machinery.
    admission_enabled: bool,
}

/// Background tasks to shut down in order: writer (flush) before manager
/// (release) — INVARIANTS.md ordering.
pub struct AppRuntime {
    pub store: Arc<MemoryStore>,
    manager: Option<LeaseManager>,
    writer: Option<UsageWriter>,
    snapshots: Option<SnapshotManager>,
    /// Demo control plane: periodically republishes the account snapshot
    /// with extended validity and a bumped generation.
    republisher: Option<tokio::task::JoinHandle<()>>,
}

impl AppRuntime {
    pub async fn shutdown(self) {
        if let Some(republisher) = self.republisher {
            republisher.abort();
        }
        if let Some(writer) = self.writer {
            let stats = writer.shutdown().await;
            if stats.lost > 0 || stats.rejected > 0 {
                eprintln!(
                    "usage-writer shutdown: {} rejected, {} lost",
                    stats.rejected, stats.lost
                );
            }
        }
        if let Some(manager) = self.manager {
            manager.shutdown().await;
        }
        if let Some(snapshots) = self.snapshots {
            snapshots.shutdown().await;
        }
    }
}

pub const DEMO_API_KEY: &str = "demo-key-1";
pub const DEMO_ACCOUNT: AccountId = AccountId(1);

/// Build the service. `deposit` funds the demo account; `admission_enabled:
/// false` is the load-gate baseline.
pub fn build_app(deposit: u64, admission_enabled: bool) -> (axum::Router, AppRuntime) {
    let store = MemoryStore::new(GrantPolicy::default()).expect("default grant policy is valid");
    store.create_account(AccountConfig {
        account_id: DEMO_ACCOUNT,
        initial_balance: CostUnits(deposit),
        active: true,
    });

    // Startup compilation: the service's schedule and entitlements become a
    // CostTable + AccountSnapshot once per policy generation — never per
    // request. Validity is bounded (1h) and a demo control-plane task
    // republishes with extended validity; the SnapshotManager keeps running
    // instances current (review finding #5 — no more install-once-forever).
    let clock: Arc<SystemClock> = Arc::new(SystemClock);
    let compile_snapshot = {
        let clock = Arc::clone(&clock);
        move |generation: u64| {
            Arc::new(AccountSnapshot {
                account_id: DEMO_ACCOUNT,
                key_id: None,
                generation: Generation(generation),
                status: AccountStatus::Active,
                valid_until: clock
                    .now()
                    .checked_add(SignedDuration::from_secs(3_600))
                    .expect("valid_until in range"),
                permissions: PERMISSION_PRICE,
                limits: ResolvedLimits {
                    max_items_per_request: 1_024,
                    rate_units_per_second: 5_000_000,
                    rate_burst_units: 10_000_000,
                },
                cost_table: Arc::new(
                    CostTable::builder(CostUnits(50), CostUnits(50))
                        .weight(&Op::Price, CostUnits(1))
                        .build(),
                ),
            })
        }
    };

    let mut auth = AuthRegistry {
        secret: b"demo-server-secret-rotate-me".to_vec(),
        keys: HashMap::new(),
    };
    let principal = auth.register(DEMO_API_KEY);
    store.publish_snapshot(principal, compile_snapshot(1));

    let map = Arc::new(ArcSwapSnapshotMap::new());
    let engine = AdmissionEngine::new(Arc::clone(&map));
    let slots = SlotRegistry::new();
    let slot = slots.slot(DEMO_ACCOUNT);

    let (manager, recorder, writer, snapshots, republisher, snapshots_ready, lease_manager_health) =
        if admission_enabled {
            let snapshots = SnapshotManager::spawn(
                store.clone(),
                map,
                Arc::clone(&slots),
                clock.clone(),
                SnapshotManagerConfig {
                    principals: vec![principal],
                    refresh_interval: std::time::Duration::from_secs(30),
                    negative_ttl: SignedDuration::from_secs(60),
                    retry_backoff: std::time::Duration::from_millis(200),
                    max_concurrent_fetches: 16,
                },
            )
            .expect("snapshot-manager configuration is valid");
            let snapshots_ready = snapshots.ready();
            let republisher = tokio::spawn({
                let store = store.clone();
                let compile_snapshot = compile_snapshot.clone();
                async move {
                    let mut generation = 1u64;
                    let mut tick = tokio::time::interval(std::time::Duration::from_secs(300));
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    tick.tick().await; // immediate first tick — gen 1 published above
                    loop {
                        tick.tick().await;
                        generation += 1;
                        store.publish_snapshot(principal, compile_snapshot(generation));
                    }
                }
            });
            let manager = LeaseManager::spawn(
                store.clone(),
                Arc::clone(&slot),
                clock.clone(),
                LeaseManagerConfig {
                    account: DEMO_ACCOUNT,
                    target_grant: CostUnits((deposit / 4).clamp(1_000, 1_000_000)),
                    low_water: CostUnits((deposit / 16).clamp(250, 250_000)),
                    lease_ttl: SignedDuration::from_secs(60),
                    expiry_safety_margin: SignedDuration::from_secs(2),
                    poll_interval: std::time::Duration::from_millis(20),
                },
            )
            .expect("lease-manager configuration is valid");
            let lease_manager_health = manager.health();
            let (recorder, writer) = UsageWriter::spawn(
                store.clone(),
                clock,
                UsageWriterConfig {
                    queue_capacity: 4_096,
                    max_batch: 256,
                    flush_interval: std::time::Duration::from_millis(25),
                    retry_backoff: std::time::Duration::from_millis(50),
                },
            );
            (
                Some(manager),
                Some(recorder),
                Some(writer),
                Some(snapshots),
                Some(republisher),
                Some(snapshots_ready),
                Some(lease_manager_health),
            )
        } else {
            (None, None, None, None, None, None, None)
        };

    let state = Arc::new(AppState {
        auth,
        engine,
        recorder,
        slot,
        lease_manager_health,
        snapshots_ready,
        admission_enabled,
    });

    let router = axum::Router::new()
        .route("/livez", get(async || StatusCode::OK))
        .route("/readyz", get(readyz))
        .route("/v1/price", post(price))
        .with_state(state);
    (
        router,
        AppRuntime {
            store,
            manager,
            writer,
            snapshots,
            republisher,
        },
    )
}

/// INVARIANTS.md #10: fail-closed correctness must not masquerade as
/// availability. Snapshot freshness/task health, lease usability, and the
/// accounting writer must all be healthy.
async fn readyz(State(state): State<Arc<AppState>>) -> StatusCode {
    let snapshots_ready = state
        .snapshots_ready
        .as_ref()
        .map(|receiver| receiver.has_changed().is_ok() && *receiver.borrow())
        .unwrap_or(true);
    let now = Timestamp::now();
    let lease_ready = state
        .slot
        .load()
        .is_some_and(|lease| now < lease.usable_until() && !lease.remaining().is_zero());
    let lease_manager_ready = state
        .lease_manager_health
        .as_ref()
        .map(|receiver| receiver.has_changed().is_ok() && *receiver.borrow())
        .unwrap_or(true);
    let writer_ready = state
        .recorder
        .as_ref()
        .is_none_or(|recorder| !recorder.is_closed());
    if !state.admission_enabled
        || (snapshots_ready && lease_ready && lease_manager_ready && writer_ready)
    {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

fn problem(status: StatusCode, code: &'static str, title: impl Into<String>) -> Response {
    let body = Problem {
        status: status.as_u16(),
        code,
        title: title.into(),
        units_charged: 0,
    };
    let mut response = (status, Json(body)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/problem+json"),
    );
    response
}

fn deny_response(reason: DenyReason) -> Response {
    let (status, code) = match reason {
        DenyReason::UnknownPrincipal => (StatusCode::UNAUTHORIZED, "unknown-principal"),
        DenyReason::AccountSuspended
        | DenyReason::AccountClosed
        | DenyReason::MissingPermission => (StatusCode::FORBIDDEN, "forbidden"),
        DenyReason::SnapshotExpired => (StatusCode::FORBIDDEN, "policy-stale"),
        DenyReason::RequestTooLarge { .. } => (StatusCode::PAYLOAD_TOO_LARGE, "batch-too-large"),
        DenyReason::UnpricedOperation | DenyReason::CostOverflow => {
            (StatusCode::UNPROCESSABLE_ENTITY, "unpriceable")
        }
        DenyReason::RateLimited => (StatusCode::TOO_MANY_REQUESTS, "rate-limited"),
        DenyReason::LeaseUnavailable | DenyReason::LeaseExpired => {
            (StatusCode::SERVICE_UNAVAILABLE, "quota-unavailable")
        }
        DenyReason::LeaseExhausted { .. } => (StatusCode::TOO_MANY_REQUESTS, "quota-exhausted"),
        DenyReason::AccountingBackpressure => (StatusCode::SERVICE_UNAVAILABLE, "accounting-busy"),
    };
    problem(status, code, reason.to_string())
}

async fn price(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<PriceRequest>,
) -> Response {
    if !state.admission_enabled {
        // Load-gate baseline: transport + kernel only.
        let prices: Vec<f64> = request.contracts.iter().map(black_scholes_call).collect();
        return Json(PriceResponse {
            prices,
            metadata: ResponseMetadata {
                request_id: "baseline".to_string(),
                units_charged: 0,
            },
        })
        .into_response();
    }

    // 1. Credential → principal (verification happens here, once; the
    //    admission engine only ever sees the fingerprint).
    let Some(principal) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .and_then(|key| state.auth.verify(key))
    else {
        return deny_response(DenyReason::UnknownPrincipal);
    };

    // 2. Accounting capacity before admission (INVARIANTS.md #8).
    let recorder = state.recorder.as_ref().expect("admission implies recorder");
    let Ok(permit) = recorder.try_reserve() else {
        return deny_response(DenyReason::AccountingBackpressure);
    };

    // 3. One-call admission.
    let items = request.contracts.len() as u64;
    let admitted = match state.engine.admit(
        AdmissionRequest {
            principal,
            required: PERMISSION_PRICE,
            op: &Op::Price,
            items,
        },
        Timestamp::now(),
    ) {
        Ok(admitted) => admitted,
        Err(reason) => return deny_response(reason),
    };

    // 4. Execution starts: the charge commits, and ChargeGuard binds the
    //    billing event to the reserved permit *now* — a panic or task abort
    //    during the kernel still emits the event on drop, so a spent lease
    //    is never unbilled (review finding #3).
    // Random 128-bit ids: idempotency keys are global, so ids must be
    // collision-free across instances and restarts — a process-local counter
    // would make a second instance's legitimate usage read as duplicates
    // (review finding #6).
    let request_id = RequestId(uuid::Uuid::new_v4().as_u128());
    let (_charge, units) =
        match ChargeGuard::commit(&admitted.reservation, permit, request_id, Timestamp::now()) {
            Ok(committed) => committed,
            Err(_) => return deny_response(DenyReason::LeaseExpired),
        };

    let prices: Vec<f64> = request.contracts.iter().map(black_scholes_call).collect();

    Json(PriceResponse {
        prices,
        metadata: ResponseMetadata {
            request_id: request_id.to_string(),
            units_charged: units.get(),
        },
    })
    .into_response()
}

// ---- toy pricing kernel ------------------------------------------------

/// European call via Black-Scholes with an erf-based normal CDF. A stand-in
/// for a real kernel: enough arithmetic to be non-trivial, no dependencies.
fn black_scholes_call(c: &Contract) -> f64 {
    let sqrt_t = c.tte_years.max(1e-9).sqrt();
    let d1 = ((c.spot / c.strike).ln() + (c.rate + 0.5 * c.vol * c.vol) * c.tte_years)
        / (c.vol * sqrt_t);
    let d2 = d1 - c.vol * sqrt_t;
    c.spot * norm_cdf(d1) - c.strike * (-c.rate * c.tte_years).exp() * norm_cdf(d2)
}

fn norm_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Abramowitz–Stegun 7.1.26; |error| < 1.5e-7 — fine for a demo kernel.
fn erf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let poly = t
        * (0.254_829_592
            + t * (-0.284_496_736
                + t * (1.421_413_741 + t * (-1.453_152_027 + t * 1.061_405_429))));
    sign * (1.0 - poly * (-x * x).exp())
}
