//! A minimal "concrete API" built on the tollgate stack — the
//! pluggability acceptance test from the plan: can a real web service embed
//! the abstract product without the product knowing anything about pricing?
//!
//! Request flow (the production hot path from the design thread):
//!
//! ```text
//! Authorization: Bearer <key>
//!   → connection-cache hit or HMAC-SHA256 verify, derive Principal
//!   → AdmissionEngine::begin               (one lookup + route permission)
//!   → reserve usage-writer permit          (shed on backpressure, #8)
//!   → read/decode under the pinned context
//!   → RequestContext::admit                (shape/rate/concurrency/funding)
//!   → acquire NoGate + commit
//!   → price (toy Black-Scholes kernel)
//!   → permit.record(usage event)
//!   → respond { prices, metadata: { request_id, units_charged } }
//! ```
//!
//! The embedded topology runs `MemoryStore` in-process; pointing the same
//! stack at a `tollgate-server` is a one-line swap to `HttpStore` (see the
//! loopback test in tollgate-server). Readiness reports 503 until the account's
//! lease slot is stocked (INVARIANTS.md #10).

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{ConnectInfo, FromRequest, FromRequestParts, State, connect_info::Connected};
use axum::http::{Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use jiff::{SignedDuration, Timestamp};
use serde::{Deserialize, Serialize};

use tollgate_admission::{
    AdmissionEngine, ArcSwapSnapshotMap, LeaseSlot, MapEntry, NoGate, RequestContext, SnapshotMap,
};
use tollgate_auth::{CredentialVerifier, HmacRegistry, SessionCredential};
use tollgate_client::{
    Clock, LeaseCounters, LeaseManager, LeaseManagerConfig, SlotRegistry, SnapshotCounters,
    SnapshotManager, SnapshotManagerConfig, SystemClock, TrackedPrincipals, UsagePermit,
    UsageRecorder, UsageWriter, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CommitError, CostTable, CostUnits, DenyReason,
    EnforcementMode, Generation, LocalSharding, OpIndex, PermissionBits, Principal,
    PublishableSnapshot, RequestId, ResolvedLimits,
};
use tollgate_store::{AccountConfig, GrantPolicy, MemoryStore};

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

/// Per-connection authentication state, bound to an accepted TCP connection
/// by Axum's `ConnectInfo`.
///
/// The whole of the mechanism lives in `tollgate-auth`; what an embedder
/// supplies is the two transport-specific parts the library deliberately does
/// not know about — what a "session" is here (one accepted connection) and how
/// to get credential bytes out of the wire format (strip `Bearer `).
#[derive(Clone, Default)]
pub struct PricingConnection {
    session: SessionCredential,
}

impl Connected<axum::serve::IncomingStream<'_, tokio::net::TcpListener>> for PricingConnection {
    fn connect_info(_stream: axum::serve::IncomingStream<'_, tokio::net::TcpListener>) -> Self {
        Self::default()
    }
}

impl PricingConnection {
    /// Resolve an `Authorization` header to a principal for this connection.
    ///
    /// The bearer prefix is stripped *before* the library sees anything, so
    /// what is cached and what is verified are the same bytes by construction.
    fn authenticate(
        &self,
        authorization: Option<&axum::http::HeaderValue>,
        verifier: &HmacRegistry,
        now: Timestamp,
    ) -> Option<Principal> {
        let credential = authorization
            .map(axum::http::HeaderValue::as_bytes)
            .and_then(|value| value.strip_prefix(b"Bearer "));
        // A header that is present but not a bearer token authenticates as
        // nobody *and* clears any prior proof, which `None` is exactly.
        self.session.authenticate(credential, verifier, now)
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

/// The request body plus the evidence obtained before Axum consumes it.
struct PriceInput {
    request: PriceRequest,
    staged: Option<(RequestContext, UsagePermit)>,
}

impl FromRequest<Arc<AppState>> for PriceInput {
    type Rejection = Response;

    async fn from_request(
        request: Request<axum::body::Body>,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        if state.admission.is_none() {
            let Json(request) = Json::<PriceRequest>::from_request(request, state)
                .await
                .map_err(IntoResponse::into_response)?;
            return Ok(Self {
                request,
                staged: None,
            });
        }

        let (mut parts, body) = request.into_parts();
        let ConnectInfo(connection) =
            ConnectInfo::<PricingConnection>::from_request_parts(&mut parts, state)
                .await
                .map_err(IntoResponse::into_response)?;
        let now = Timestamp::now();
        let Some(principal) =
            connection.authenticate(parts.headers.get(header::AUTHORIZATION), &state.auth, now)
        else {
            state
                .engine
                .counters()
                .record_deny(&DenyReason::UnknownPrincipal);
            return Err(deny_response(DenyReason::UnknownPrincipal));
        };
        let context = state
            .engine
            .begin(principal, PERMISSION_PRICE, now)
            .map_err(deny_response)?;
        let admission = state
            .admission
            .as_ref()
            .expect("the branch above proved admission is enabled");
        let permit = admission.recorder.try_reserve().map_err(|_| {
            state
                .engine
                .counters()
                .record_deny(&DenyReason::AccountingBackpressure);
            deny_response(DenyReason::AccountingBackpressure)
        })?;

        let request = Request::from_parts(parts, body);
        let Json(request) = Json::<PriceRequest>::from_request(request, state)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Self {
            request,
            staged: Some((context, permit)),
        })
    }
}

// ---- app ---------------------------------------------------------------

struct AppState {
    auth: HmacRegistry,
    /// Built in both configurations and honest to read in either: the engine's
    /// counters and the slot's contents say what actually happened, which is
    /// why neither ever needed an `expect`.
    engine: AdmissionEngine<Arc<ArcSwapSnapshotMap>>,
    slot: Arc<LeaseSlot>,
    /// `None` is the no-admission baseline the load gate compares against:
    /// same transport, same kernel, zero quota machinery. The `Option` *is*
    /// the switch, so there is no separate flag left to disagree with it.
    admission: Option<AdmissionRuntime>,
}

/// Everything the quota machinery installs, held together because it is
/// installed together (#16). As parallel `Option`s the correlation was
/// unprovable, and a request handler paid for it with an `expect`.
struct AdmissionRuntime {
    recorder: UsageRecorder,
    /// Refill-task health; channel closure exposes panic or abort.
    lease_manager_health: tokio::sync::watch::Receiver<bool>,
    /// Snapshot-manager readiness: true while every tracked principal has a
    /// fresh resolution; channel closure also exposes task failure.
    snapshots_ready: tokio::sync::watch::Receiver<bool>,
    /// Refill and snapshot counters. Held as the shared handles rather than
    /// the managers themselves, which are moved into `AppRuntime` for
    /// shutdown and so are out of a handler's reach.
    lease_counters: Arc<LeaseCounters>,
    snapshot_counters: Arc<SnapshotCounters>,
}

pub struct AppRuntime {
    pub store: Arc<MemoryStore>,
    background: Option<Background>,
}

/// The background tasks, present or absent as one — the same collapse
/// `AdmissionRuntime` makes on the state side (#16). Shutdown ordering is the
/// reason it matters here: writer (flush) before manager (release) is an
/// INVARIANTS.md requirement, and as three independent `Option`s it held only
/// so long as all three agreed.
struct Background {
    manager: LeaseManager,
    writer: UsageWriter,
    snapshots: SnapshotManager,
    /// Demo control plane: periodically republishes the account snapshot
    /// with extended validity and a bumped generation.
    republisher: tokio::task::JoinHandle<()>,
}

impl AppRuntime {
    pub async fn shutdown(self) {
        let Some(background) = self.background else {
            return;
        };
        background.republisher.abort();
        // Always reported: a clean shutdown is itself the operator's evidence
        // that nothing was lost or left unresolved, and a dead writer must
        // never look like one.
        match background.writer.shutdown().await {
            Ok(stats) => tracing::info!(
                accepted = stats.accepted,
                duplicate = stats.duplicate,
                rejected = stats.rejected,
                lost = stats.lost,
                unresolved = stats.unresolved,
                "usage-writer shutdown"
            ),
            Err(error) => {
                tracing::error!(
                    unaccounted = error.unaccounted,
                    panicked = error.panicked,
                    "usage-writer shutdown failed"
                );
            }
        }
        // Reported unconditionally, like the writer's line above: the released
        // count is the operator's evidence that leases came back, not just the
        // absence of bad news.
        let report = background.manager.shutdown().await;
        tracing::info!(
            released = report.released,
            abandoned = report.abandoned,
            task_died = report.task_died,
            "lease-manager shutdown; abandoned leases settle at TTL reclaim"
        );
        background.snapshots.shutdown().await;
    }
}

pub const DEMO_API_KEY: &str = "demo-key-1";
pub const DEMO_ACCOUNT: AccountId = AccountId(1);

/// Well inside the one-hour validity each published snapshot carries, so a
/// republish that is late or skipped still leaves many attempts before
/// anything can age out.
const REPUBLISH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

/// The demo control plane: republish the account snapshot on `period`, each
/// time with a bumped generation and extended validity, so a long-running
/// example never serves a snapshot that has aged past `valid_until`.
///
/// The generation must *advance*, not merely change: a backend drops a
/// republish whose generation does not exceed the one it holds
/// (INVARIANTS.md #3's anti-resurrection watermark), so a stalled counter
/// would silently stop extending validity rather than fail visibly.
async fn republish_snapshots(
    store: Arc<MemoryStore>,
    principal: Principal,
    compile: impl Fn(u64) -> PublishableSnapshot,
    period: std::time::Duration,
) {
    let mut generation = 1u64;
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // immediate first tick — gen 1 published at build time
    loop {
        tick.tick().await;
        generation += 1;
        store.publish_snapshot(principal, compile(generation));
    }
}

/// The demo's refill sizing, derived from the deposit: acquire a quarter of it
/// at a time, and start topping up at a sixteenth. Both are clamped to an
/// absolute range so a trivial or enormous deposit still yields a lease worth
/// holding — and the clamps are chosen so `low_water` stays strictly below
/// `target_grant` at every deposit, which is what makes early refill mean
/// anything (`LocalLease::new`).
fn refill_sizing(deposit: u64) -> (CostUnits, CostUnits) {
    (
        CostUnits((deposit / 4).clamp(1_000, 1_000_000)),
        CostUnits((deposit / 16).clamp(250, 250_000)),
    )
}

/// Build the service. `deposit` funds the demo account; `admission_enabled:
/// false` is the load-gate baseline.
pub fn build_app(deposit: u64, admission_enabled: bool) -> (axum::Router, AppRuntime) {
    build_app_with(
        deposit,
        admission_enabled,
        EnforcementMode::Strict,
        LocalSharding::SINGLE,
    )
}

/// The demo stack with the account's enforcement mode chosen by the caller.
///
/// Exists because an example that can only ever be `Strict` cannot show what
/// `Elastic` does — and, less obviously, cannot *test* the wiring that reads
/// the mode: a `enforcement_mode()` that ignored the snapshot entirely and
/// returned `Strict` would satisfy every assertion. The mutation gate found
/// exactly that.
pub fn build_app_with_mode(
    deposit: u64,
    admission_enabled: bool,
    enforcement_mode: EnforcementMode,
) -> (axum::Router, AppRuntime) {
    build_app_with(
        deposit,
        admission_enabled,
        enforcement_mode,
        LocalSharding::SINGLE,
    )
}

/// Build the service with an explicit instance-local hot-path shard count.
/// Keep one shard unless profiling shows sustained same-account saturation.
pub fn build_app_with_sharding(
    deposit: u64,
    admission_enabled: bool,
    sharding: LocalSharding,
) -> (axum::Router, AppRuntime) {
    build_app_with(
        deposit,
        admission_enabled,
        EnforcementMode::Strict,
        sharding,
    )
}

/// Both knobs at once. Each public builder above fixes one of them, because
/// every current caller varies one and takes the default for the other; the
/// body lives here so neither knob's default is written twice.
fn build_app_with(
    deposit: u64,
    admission_enabled: bool,
    enforcement_mode: EnforcementMode,
    sharding: LocalSharding,
) -> (axum::Router, AppRuntime) {
    let store = MemoryStore::new(GrantPolicy::default()).expect("default grant policy is valid");
    store.create_account(AccountConfig {
        account_id: DEMO_ACCOUNT,
        initial_balance: CostUnits(deposit),
        status: AccountStatus::Active,
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
            let snapshot = Arc::new(
                AccountSnapshot::builder(
                    DEMO_ACCOUNT,
                    Generation(generation),
                    AccountStatus::Active,
                    clock
                        .now()
                        .checked_add(SignedDuration::from_secs(3_600))
                        .expect("valid_until in range"),
                    PERMISSION_PRICE,
                    ResolvedLimits::new(1_024).with_weighted_rate(5_000_000, 10_000_000),
                    Arc::new(
                        CostTable::builder(CostUnits(50), CostUnits(50))
                            .weight(&Op::Price, CostUnits(1))
                            .build(),
                    ),
                )
                .enforcement_mode(enforcement_mode)
                .build(),
            );
            PublishableSnapshot::try_new(snapshot)
                .expect("example pricing schedule must fit inside its burst")
        }
    };

    let mut auth = HmacRegistry::new(b"demo-server-secret-rotate-me");
    let principal = auth.register(DEMO_API_KEY.as_bytes());
    store.publish_snapshot(principal, compile_snapshot(1));

    let map = Arc::new(ArcSwapSnapshotMap::with_sharding(sharding));
    let engine = AdmissionEngine::new(Arc::clone(&map));
    let slots = SlotRegistry::with_sharding(sharding);
    let slot = slots.slot(DEMO_ACCOUNT);

    // Either the whole quota machinery is installed, or none of it is. The
    // branch yields both halves — what the handlers read and what shutdown
    // owns — so no caller downstream has to re-establish that they agree.
    let (admission, background) = if admission_enabled {
        let snapshots = SnapshotManager::spawn(
            store.clone(),
            map,
            Arc::clone(&slots),
            clock.clone(),
            SnapshotManagerConfig {
                // Stateless: any instance may serve any customer, so this
                // one tracks everything the store knows rather than a list
                // fixed at boot (#48). Seeded with the demo key so the
                // example still works against a source that cannot
                // enumerate.
                principals: TrackedPrincipals::All {
                    seed: vec![principal],
                },
                refresh_interval: std::time::Duration::from_secs(30),
                unknown_ttl: SignedDuration::from_secs(60),
                revoked_ttl: SignedDuration::from_secs(3_600),
                retry_backoff: std::time::Duration::from_millis(200),
                max_concurrent_fetches: 16,
            },
        )
        .expect("snapshot-manager configuration is valid");
        let republisher = tokio::spawn(republish_snapshots(
            store.clone(),
            principal,
            compile_snapshot.clone(),
            REPUBLISH_INTERVAL,
        ));
        let (target_grant, low_water) = refill_sizing(deposit);
        let manager = LeaseManager::spawn(
            store.clone(),
            Arc::clone(&slot),
            clock.clone(),
            LeaseManagerConfig {
                account: DEMO_ACCOUNT,
                target_grant,
                low_water,
                lease_ttl: SignedDuration::from_secs(60),
                expiry_safety_margin: SignedDuration::from_secs(2),
                poll_interval: std::time::Duration::from_millis(20),
                store_call_timeout: std::time::Duration::from_secs(5),
                shutdown_release_deadline: std::time::Duration::from_secs(10),
            },
        )
        .expect("lease-manager configuration is valid");
        let (recorder, writer) = UsageWriter::spawn(
            store.clone(),
            clock,
            UsageWriterConfig {
                queue_capacity: 4_096,
                max_batch: 256,
                flush_interval: std::time::Duration::from_millis(25),
                retry_backoff: std::time::Duration::from_millis(50),
                // Bounded well inside the lease TTL so late events are
                // still billable against a live lease.
                shutdown_drain_deadline: std::time::Duration::from_secs(5),
                ingest_timeout: std::time::Duration::from_secs(5),
            },
        )
        .expect("usage-writer configuration is valid");
        // Read while the managers are still in scope; the handles outlive the
        // tasks, so a handler keeps reading after a plane dies.
        let admission = AdmissionRuntime {
            recorder,
            lease_manager_health: manager.health(),
            snapshots_ready: snapshots.ready(),
            lease_counters: manager.counters(),
            snapshot_counters: snapshots.counters(),
        };
        let background = Background {
            manager,
            writer,
            snapshots,
            republisher,
        };
        (Some(admission), Some(background))
    } else {
        (None, None)
    };

    let state = Arc::new(AppState {
        auth,
        engine,
        slot,
        admission,
    });

    let router = axum::Router::new()
        .route("/livez", get(async || StatusCode::OK))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/v1/price", post(price))
        .with_state(state);
    (router, AppRuntime { store, background })
}

/// INVARIANTS.md #10: fail-closed correctness must not masquerade as
/// availability. Snapshot freshness/task health, lease usability, and the
/// accounting writer must all be healthy.
async fn readyz(State(state): State<Arc<AppState>>) -> StatusCode {
    let Some(admission) = state.admission.as_ref() else {
        // The load-gate baseline installs no quota machinery, so there is no
        // snapshot to go stale, no lease to exhaust and no queue to back up.
        // Serving at all is the whole condition.
        return StatusCode::OK;
    };
    let readiness = Readiness {
        snapshots_fresh: plane_healthy(&admission.snapshots_ready),
        lease_usable: quota_usable(
            &state.slot,
            enforcement_mode(state.as_ref()),
            Timestamp::now(),
        ),
        refill_healthy: plane_healthy(&admission.lease_manager_health),
        writer_healthy: !admission.recorder.is_closed(),
    };
    if readiness.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// The four independent conditions readiness rests on, named rather than
/// positional.
struct Readiness {
    snapshots_fresh: bool,
    lease_usable: bool,
    refill_healthy: bool,
    writer_healthy: bool,
}

impl Readiness {
    /// Any one condition false withdraws the instance from rotation. Pure, so
    /// each can be pinned on its own: through the HTTP surface these four only
    /// ever move together, because the demo's grant policy never shrinks a
    /// lease to exactly zero while its planes are alive.
    fn is_ready(&self) -> bool {
        self.snapshots_fresh && self.lease_usable && self.refill_healthy && self.writer_healthy
    }
}

/// A background plane's health bit. Both halves are load-bearing: a task that
/// panicked or was aborted drops its sender, but its last published value
/// stays readable forever, so `borrow()` alone would report a dead plane as
/// healthy for the rest of the process's life.
fn plane_healthy(health: &tokio::sync::watch::Receiver<bool>) -> bool {
    health.has_changed().is_ok() && *health.borrow()
}

/// Whether this instance can still fund work for the demo account — the same
/// comparisons the request path makes, so readiness and admission cannot
/// disagree about the boundary. "Usable *through* `expires_at - margin`"
/// (INVARIANTS.md #12) is `now < usable_until`: at that instant exactly, the
/// request path already denies, and readiness must not still be advertising.
///
/// Under `EnforcementMode::Elastic` an empty or absent lease is not the end of
/// the answer. An elastic account with overage headroom *is* admissible, and
/// reporting it unready would pull from rotation exactly the instances the
/// mode exists to keep serving — turning a feature that prevents false denials
/// into one that causes them (INVARIANTS.md #10).
fn quota_usable(slot: &LeaseSlot, mode: EnforcementMode, now: Timestamp) -> bool {
    let lease_usable = slot
        .load()
        .is_some_and(|lease| now < lease.usable_until() && !lease.remaining().is_zero());
    lease_usable
        || mode
            .overage_cap()
            .is_some_and(|cap| !slot.overage().headroom(cap).is_zero())
}

/// The demo account's currently published enforcement mode, read from the
/// snapshot the request path itself would read.
///
/// Not cached beside the slot: a republish changes the mode, and readiness
/// answering from a stale copy is precisely the disagreement between
/// readiness and admission that INVARIANTS.md #10 forbids.
fn enforcement_mode(state: &AppState) -> EnforcementMode {
    // `CredentialVerifier::verify` takes the credential bytes, and returns
    // the validity alongside the identity; readiness only needs the identity.
    let Some(principal) = state
        .auth
        .verify(DEMO_API_KEY.as_bytes())
        .map(|verified| verified.principal)
    else {
        return EnforcementMode::Strict;
    };
    match state.engine.map().get(&principal) {
        Some(MapEntry::Present(admission)) => admission.snapshot.enforcement_mode,
        _ => EnforcementMode::Strict,
    }
}

/// What this instance admitted, refused, and has left to spend.
///
/// JSON rather than an exposition format on purpose: naming the metrics is
/// the decision, and issue #38 owns continuous export while #39 owns the
/// operator documentation. Choosing a wire format here would pre-empt both.
#[derive(Debug, Serialize)]
pub struct Metrics {
    /// Requests admitted by the engine.
    pub admitted: u64,
    /// Units quoted by admitted requests — *not* units billed. Usage events
    /// are billing truth; a request admitted and then cancelled before
    /// execution is counted here and charged nothing.
    pub units_admitted: u64,
    /// Every refusal, whatever the reason.
    pub denied: u64,
    /// Refusals by reason. Every reason is present, so a zero says "this has
    /// not happened" rather than leaving the reader to guess whether the
    /// counter exists. Ordered, so two scrapes diff cleanly.
    pub denials: BTreeMap<&'static str, u64>,
    /// Units left on the installed lease, absent when no lease is installed
    /// (cold start, expiry, or control-plane invalidation). Read off the
    /// shared slot, never from the request path.
    pub lease_remaining: Option<u64>,
    /// Requests admitted with no lease behind them, and the units they were
    /// quoted. Included in `admitted` / `units_admitted`, never instead of
    /// them.
    ///
    /// The pair an operator watches for an elastic account: it climbing is
    /// credit being extended, and it is a leading indicator of an invoice the
    /// way `accounting.rejected` is a leading indicator of billing loss.
    pub admitted_overage: u64,
    pub units_admitted_overage: u64,
    /// Unfunded units currently outstanding on this instance, and the cap
    /// bounding them.
    ///
    /// **Per instance.** Fleet exposure is this cap times the number of
    /// instances, because the counter behind it is a local atomic — the same
    /// scope every other local mechanism here has. `null` for a strict
    /// account, which extends no credit at all.
    pub overage_spent: u64,
    pub overage_cap: Option<u64>,
    /// How long the installed lease may still be spent against — the
    /// `expires_at - safety_margin` bound of INVARIANTS.md #12, not the raw
    /// expiry. Paired with `lease_remaining`, since a lease can be refused
    /// for either reason.
    pub lease_usable_until: Option<String>,
    /// Billing health, absent only when admission is disabled (the load-gate
    /// baseline runs no accounting at all).
    pub accounting: Option<Accounting>,
    /// Refill health: whether this instance can keep its lease stocked.
    pub refill: Option<Refill>,
    /// Snapshot-distribution health.
    pub snapshots: Option<Snapshots>,
}

/// What the refill task has done. The counter that matters here is
/// `refused`, broken down by reason: a `lease_exhausted` denial with balance
/// still in the account is a refill problem, and this says which one.
#[derive(Debug, Serialize)]
pub struct Refill {
    /// Acquires that returned a grant, and the units they carried.
    pub acquired: u64,
    pub acquired_units: u64,
    /// Acquires the allocator did not answer in time. Not a refusal — the
    /// grant may well have been made and simply not reported.
    pub acquire_timeouts: u64,
    /// Every acquire refusal, and the breakdown by reason. Every reason is
    /// present, so a zero reads as "has not happened".
    pub refused: u64,
    pub refusals: BTreeMap<&'static str, u64>,
    /// Leases the allocator no longer holds open for this instance.
    pub released: u64,
    /// Leases a shutdown budget could not return. Like `accounting.lost`,
    /// this can only move at shutdown.
    pub abandoned: u64,
}

/// What the snapshot task has done.
#[derive(Debug, Serialize)]
pub struct Snapshots {
    /// Fetches attempted, one per principal per pass. A rate that has fallen
    /// to zero means the refresh loop itself has stopped.
    pub refresh_attempts: u64,
    /// Fetches the source could not answer.
    pub refresh_failures: u64,
    /// Enumerations the source could not answer (#48). Its own counter
    /// because its consequence is different: fetch failures make known
    /// principals stale, which `unresolved` shows, while enumeration failures
    /// mean *new* principals never appear — invisible in every other number,
    /// since everything already tracked keeps working.
    pub discovery_failures: u64,
    /// Principals with no currently valid resolution — the gauge that makes
    /// readiness false, and the disambiguator for an `unknown_principal`
    /// spike: nonzero means distribution, zero means credentials.
    pub unresolved: u64,
}

/// What the usage writer has done with the charges handed to it — readable at
/// any time, not only from a graceful shutdown (#38).
#[derive(Debug, Serialize)]
pub struct Accounting {
    /// Events the sink recorded.
    pub accepted: u64,
    /// Events whose request id the sink had already recorded — idempotent
    /// replay, not loss (INVARIANTS.md #7).
    pub duplicate: u64,
    /// Events the sink *refused*: unknown lease, lease-capability mismatch,
    /// or no remaining lease capacity. Bounded billing loss, and the number
    /// reconciliation watches.
    pub rejected: u64,
    /// Events a final flush could not deliver. Note this stays zero while the
    /// process runs — the steady-state path retries a failing sink forever, so
    /// loss is only declared at shutdown. It is not the runtime alarm; the
    /// three fields below are.
    pub lost: u64,
    /// Charges queued with no billing outcome yet.
    pub unaccounted: u64,
    /// Requests refused for want of queue capacity (INVARIANTS.md #8).
    pub shed: u64,
    /// Queue occupancy against its shed point: backpressure is visible here
    /// *before* it starts refusing requests.
    pub queue_depth: usize,
    pub queue_capacity: usize,
    /// When the sink last answered, and how long ago. `null` means it never
    /// has — a fresh process, or one that has never reached its sink.
    pub last_ingest_at: Option<String>,
    /// The signal that separates "no traffic" from "the sink has been
    /// unreachable for twenty minutes".
    pub ingest_age_seconds: Option<i64>,
}

/// The counters are read off the engine, not the request path: this handler
/// does the loads, the formatting and the allocation that INVARIANTS.md #5
/// keeps out of `admit`.
async fn metrics(State(state): State<Arc<AppState>>) -> Json<Metrics> {
    let counters = state.engine.counters().snapshot();
    let lease = state.slot.load();
    let now = Timestamp::now();
    // Three views of one plane, so they are absent together or present
    // together — a guarantee of the type now, not of this handler.
    let admission = state.admission.as_ref();
    Json(Metrics {
        admitted: counters.admitted,
        units_admitted: counters.units_admitted,
        denied: counters.denied(),
        denials: counters.denials_by_name().collect(),
        lease_remaining: lease.as_ref().map(|lease| lease.remaining().get()),
        admitted_overage: counters.admitted_overage,
        units_admitted_overage: counters.units_admitted_overage,
        overage_spent: state.slot.overage().spent().get(),
        overage_cap: enforcement_mode(state.as_ref())
            .overage_cap()
            .map(CostUnits::get),
        lease_usable_until: lease.map(|lease| lease.usable_until().to_string()),
        accounting: admission.map(|admission| {
            let health = admission.recorder.health();
            Accounting {
                accepted: health.stats.accepted,
                duplicate: health.stats.duplicate,
                rejected: health.stats.rejected,
                lost: health.stats.lost,
                unaccounted: health.unaccounted,
                shed: health.shed,
                queue_depth: health.queue_depth,
                queue_capacity: health.queue_capacity,
                last_ingest_at: health.last_ingest_at.map(|at| at.to_string()),
                ingest_age_seconds: health.ingest_age(now).map(|age| age.as_secs()),
            }
        }),
        refill: admission.map(|admission| {
            let stats = admission.lease_counters.snapshot();
            Refill {
                acquired: stats.acquired,
                acquired_units: stats.acquired_units,
                acquire_timeouts: stats.acquire_timeouts,
                refused: stats.refused(),
                refusals: stats.refusals_by_name().collect(),
                released: stats.released,
                abandoned: stats.abandoned,
            }
        }),
        snapshots: admission.map(|admission| {
            let stats = admission.snapshot_counters.snapshot();
            Snapshots {
                refresh_attempts: stats.refresh_attempts,
                refresh_failures: stats.refresh_failures,
                discovery_failures: stats.discovery_failures,
                unresolved: stats.unresolved,
            }
        }),
    })
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
        DenyReason::SnapshotExpired => (StatusCode::SERVICE_UNAVAILABLE, "policy-stale"),
        DenyReason::RequestTooLarge { .. } => (StatusCode::PAYLOAD_TOO_LARGE, "batch-too-large"),
        DenyReason::UnpricedOperation | DenyReason::CostOverflow => {
            (StatusCode::UNPROCESSABLE_ENTITY, "unpriceable")
        }
        DenyReason::RateLimited => (StatusCode::TOO_MANY_REQUESTS, "rate-limited"),
        DenyReason::RequestRateLimited => (StatusCode::TOO_MANY_REQUESTS, "request-rate-limited"),
        DenyReason::ConcurrencyLimited => (StatusCode::TOO_MANY_REQUESTS, "concurrency-limited"),
        // Deliberately not 429 and deliberately its own code: this request
        // can never be admitted under the current plan, so inviting a retry
        // would be a lie, and folding it into "unpriceable" would hide which
        // half of the schedule is wrong.
        DenyReason::UnpriceableUnderLimits { .. } => {
            (StatusCode::UNPROCESSABLE_ENTITY, "unpriceable-under-limits")
        }
        DenyReason::LeaseUnavailable | DenyReason::LeaseExpired => {
            (StatusCode::SERVICE_UNAVAILABLE, "quota-unavailable")
        }
        DenyReason::LeaseExhausted { .. } => (StatusCode::TOO_MANY_REQUESTS, "quota-exhausted"),
        // The local overage cap does not refill, but this reason is observed
        // only after a lease failed to fund the request. The background lease
        // manager may install a new grant from existing central balance, so a
        // payment-required response would claim knowledge this process does
        // not have.
        DenyReason::OverageCapExhausted { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, "overage-cap-exhausted")
        }
        DenyReason::OverageCapTemporarilyExhausted { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "overage-cap-temporarily-exhausted",
        ),
        DenyReason::OverageCommitInProgress { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "overage-commit-in-progress",
        ),
        DenyReason::AccountingBackpressure => (StatusCode::SERVICE_UNAVAILABLE, "accounting-busy"),
        DenyReason::EmptyWorkload => (StatusCode::UNPROCESSABLE_ENTITY, "empty-workload"),
        DenyReason::FundingExpiredAtStart => {
            (StatusCode::SERVICE_UNAVAILABLE, "funding-expired-at-start")
        }
    };
    problem(status, code, reason.to_string())
}

async fn price(input: PriceInput) -> Response {
    // One destructuring, not a flag check followed by an unwrap that has to
    // agree with it: the same `let` that rules out the baseline is what hands
    // this handler the recorder (#16).
    let PriceInput { request, staged } = input;
    let Some((context, permit)) = staged else {
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
    };

    // Authentication, begin, accounting backpressure, and body decoding have
    // already occurred in `PriceInput`, in that order. The owned context is
    // the proof that this body is still governed by the same generation.
    let items = request.contracts.len() as u64;
    let pending = match context.admit(&[(Op::Price, items)], permit, Timestamp::now()) {
        Ok(pending) => pending,
        Err(reason) => return deny_response(reason),
    };
    let ready = match pending.acquire_capacity(&NoGate) {
        Ok(ready) => ready,
        Err((reason, _released)) => return deny_response(reason),
    };

    // Execution starts only after the type-state owns funding, accounting,
    // account concurrency, and the selected execution-capacity permit.
    // Random 128-bit ids: idempotency keys are global, so ids must be
    // collision-free across instances and restarts — a process-local counter
    // would make a second instance's legitimate usage read as duplicates
    // (review finding #6).
    let request_id = RequestId(uuid::Uuid::new_v4().as_u128());
    let committed = match ready.commit(request_id, Timestamp::now()) {
        Ok(committed) => committed,
        Err((CommitError::Denied(reason), _released)) => return deny_response(reason),
        Err((_cancelled, _released)) => {
            return deny_response(DenyReason::FundingExpiredAtStart);
        }
    };
    let units = committed.units();

    let prices: Vec<f64> = request.contracts.iter().map(black_scholes_call).collect();
    drop(committed);

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

#[cfg(test)]
mod tests {
    use super::*;
    use tollgate_core::{FencingToken, LeaseGrant, LeaseId, LocalLease, Reservation};
    use tollgate_store::{SnapshotResolution, SnapshotSource};

    #[test]
    fn stale_policy_is_a_transient_service_failure() {
        assert_eq!(
            deny_response(DenyReason::SnapshotExpired).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            DenyReason::SnapshotExpired.retry(),
            tollgate_core::Retry::Transient
        );
    }

    // The cache's own behaviour — verify-once, per-session isolation,
    // invalidation ordering — is tested in `tollgate-auth`, where it now
    // lives. What remains this crate's to prove is the wiring: that the
    // connection factory is installed, and that a cached principal is still
    // subject to admission on every request.

    fn healthy() -> Readiness {
        Readiness {
            snapshots_fresh: true,
            lease_usable: true,
            refill_healthy: true,
            writer_healthy: true,
        }
    }

    /// The canonical embedder must preserve the core retry contract. Both
    /// refundable and committed local saturation are operational because a
    /// lease refill can recover either; publication has its own stable code.
    #[tokio::test]
    async fn overage_retry_classes_map_to_distinct_http_contracts() {
        for (reason, expected_status, expected_code) in [
            (
                DenyReason::OverageCapTemporarilyExhausted {
                    spent: CostUnits(100),
                    overage_cap: CostUnits(100),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "overage-cap-temporarily-exhausted",
            ),
            (
                DenyReason::OverageCapExhausted {
                    spent: CostUnits(100),
                    overage_cap: CostUnits(100),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "overage-cap-exhausted",
            ),
            (
                DenyReason::OverageCommitInProgress {
                    spent: CostUnits(100),
                    overage_cap: CostUnits(100),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "overage-commit-in-progress",
            ),
        ] {
            let response = deny_response(reason);
            assert_eq!(response.status(), expected_status);
            let body = axum::body::to_bytes(response.into_body(), 4_096)
                .await
                .unwrap();
            let problem: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(problem["code"], expected_code);
        }
    }

    /// Snapshot expiry is repaired by background publication, so the
    /// canonical adapter must expose an operational outage rather than a
    /// permanent authorization failure.
    #[tokio::test]
    async fn snapshot_expiry_maps_to_a_transient_service_outage() {
        let reason = DenyReason::SnapshotExpired;
        assert_eq!(reason.retry(), tollgate_core::Retry::Transient);

        let response = deny_response(reason);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), 4_096)
            .await
            .unwrap();
        let problem: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(problem["code"], "policy-stale");
        assert_eq!(problem["units_charged"], 0);
    }

    /// INVARIANTS.md #10: fail-closed correctness must not masquerade as
    /// availability. Each condition alone is sufficient to withdraw the
    /// instance from rotation — asserted here because the HTTP surface can
    /// never exhibit them one at a time.
    #[test]
    fn any_single_unhealthy_condition_withdraws_from_rotation() {
        assert!(healthy().is_ready());

        let mut snapshots = healthy();
        snapshots.snapshots_fresh = false;
        assert!(
            !snapshots.is_ready(),
            "a stale snapshot denies every request"
        );

        let mut lease = healthy();
        lease.lease_usable = false;
        assert!(!lease.is_ready(), "no usable lease denies every request");

        let mut refill = healthy();
        refill.refill_healthy = false;
        assert!(!refill.is_ready(), "a dead refill task cannot restock");

        let mut writer = healthy();
        writer.writer_healthy = false;
        assert!(!writer.is_ready(), "a closed queue sheds every request");
    }

    /// A panicked or aborted task drops its sender but leaves its last value
    /// readable forever, so the published bit alone would report a dead plane
    /// as healthy for the rest of the process's life.
    #[test]
    fn a_plane_that_died_while_healthy_is_not_healthy() {
        let (sender, receiver) = tokio::sync::watch::channel(true);
        assert!(plane_healthy(&receiver));

        sender.send_replace(false);
        assert!(!plane_healthy(&receiver), "the plane said it is unhealthy");

        let (sender, receiver) = tokio::sync::watch::channel(true);
        drop(sender);
        assert!(
            !plane_healthy(&receiver),
            "the last value still reads true; the closed channel is the evidence"
        );
    }

    fn lease_expiring_at(expires_at: Timestamp, units: u64) -> Arc<LocalLease> {
        Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id: LeaseId(1),
                account_id: DEMO_ACCOUNT,
                fencing_token: FencingToken(1),
                units: CostUnits(units),
                expires_at,
            },
            CostUnits(0),
        ))
    }

    /// Readiness and the request path must agree about the window's edge:
    /// `LocalLease::try_debit` denies at `now >= usable_until`, so readiness
    /// must stop advertising at the same instant rather than one tick later
    /// (INVARIANTS.md #12).
    #[test]
    fn readiness_closes_the_lease_window_exactly_when_debits_do() {
        let now = Timestamp::now();
        let slot = LeaseSlot::for_account(DEMO_ACCOUNT);
        assert!(
            !quota_usable(&slot, EnforcementMode::Strict, now),
            "an empty slot funds nothing"
        );

        let lease = lease_expiring_at(now, 100);
        slot.install(Arc::clone(&lease));
        assert!(
            lease.try_debit(CostUnits(1), now).is_err(),
            "the request path denies at the boundary",
        );
        assert!(
            !quota_usable(&slot, EnforcementMode::Strict, now),
            "so readiness must not still be advertising at it",
        );

        slot.install(lease_expiring_at(
            now.checked_add(SignedDuration::from_secs(60)).unwrap(),
            100,
        ));
        assert!(quota_usable(&slot, EnforcementMode::Strict, now));

        slot.install(lease_expiring_at(
            now.checked_add(SignedDuration::from_secs(60)).unwrap(),
            0,
        ));
        assert!(
            !quota_usable(&slot, EnforcementMode::Strict, now),
            "a live lease with nothing left funds nothing either",
        );
    }

    /// The mode's whole purpose, stated as a readiness property: an elastic
    /// account with headroom keeps its instance in rotation on exactly the
    /// states a strict one is withdrawn for, and leaves rotation when the
    /// headroom is gone (INVARIANTS.md #10).
    #[test]
    fn readiness_counts_overage_headroom_for_an_elastic_account() {
        let now = Timestamp::now();
        let elastic = EnforcementMode::Elastic {
            overage_cap: CostUnits(100),
        };
        let slot = LeaseSlot::for_account(DEMO_ACCOUNT);

        // No lease at all, and a live lease with nothing left: both deny under
        // `Strict`, and both are exactly what elastic mode serves through.
        assert!(!quota_usable(&slot, EnforcementMode::Strict, now));
        assert!(quota_usable(&slot, elastic, now));

        slot.install(lease_expiring_at(
            now.checked_add(SignedDuration::from_secs(60)).unwrap(),
            0,
        ));
        assert!(!quota_usable(&slot, EnforcementMode::Strict, now));
        assert!(quota_usable(&slot, elastic, now));

        // Spending the cap withdraws the instance, because at that point it
        // really cannot admit anything.
        let overage =
            Reservation::reserve_overage(slot.overage(), CostUnits(100), CostUnits(100)).unwrap();
        overage.commit_at_execution_start(now).unwrap();
        assert!(
            !quota_usable(&slot, elastic, now),
            "a spent cap is not admissible, and readiness must say so"
        );

        // A cap raised by a republish restores readiness with no other change.
        assert!(quota_usable(
            &slot,
            EnforcementMode::Elastic {
                overage_cap: CostUnits(200)
            },
            now
        ));
    }

    /// Early refill only means something while `low_water` sits below the
    /// grant it is meant to top up; the clamps must not invert that at any
    /// deposit, including the ones where both of them bind.
    #[test]
    fn refill_sizing_keeps_low_water_under_the_grant() {
        for deposit in [0, 1, 200, 4_000, 16_000, 100_000, 4_000_000, u64::MAX] {
            let (target, low) = refill_sizing(deposit);
            assert!(
                low < target,
                "deposit {deposit}: low water {low:?} must sit under grant {target:?}",
            );
        }
        // The documented sizing, pinned: a quarter and a sixteenth, so a
        // change to either is a deliberate one.
        assert_eq!(
            refill_sizing(100_000),
            (CostUnits(25_000), CostUnits(6_250))
        );
        assert_eq!(refill_sizing(200), (CostUnits(1_000), CostUnits(250)));
    }

    /// The demo control plane's only job is to keep validity ahead of the
    /// clock, and it can only do that if each republish *advances* the
    /// generation: a backend drops one that does not, so a stalled counter
    /// stops extending validity without ever failing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn republishing_advances_the_generation_each_time() {
        let store = MemoryStore::new(GrantPolicy::default()).expect("valid policy");
        let principal = Principal(7);
        let compile = |generation: u64| {
            PublishableSnapshot::try_new(Arc::new(
                AccountSnapshot::builder(
                    DEMO_ACCOUNT,
                    Generation(generation),
                    AccountStatus::Active,
                    Timestamp::now()
                        .checked_add(SignedDuration::from_secs(3_600))
                        .unwrap(),
                    PERMISSION_PRICE,
                    ResolvedLimits::new(1_024).with_weighted_rate(5_000_000, 10_000_000),
                    Arc::new(
                        CostTable::builder(CostUnits(50), CostUnits(50))
                            .weight(&Op::Price, CostUnits(1))
                            .build(),
                    ),
                )
                .build(),
            ))
            .expect("schedule fits inside its burst")
        };
        store.publish_snapshot(principal, compile(1));

        let task = tokio::spawn(republish_snapshots(
            store.clone(),
            principal,
            compile,
            std::time::Duration::from_millis(1),
        ));

        let mut generation = Generation(1);
        for _ in 0..500 {
            if generation.0 >= 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let SnapshotResolution::Present(snapshot) =
                store.snapshot(principal).await.expect("memory store")
            else {
                panic!("the principal was published before the task started");
            };
            assert!(
                snapshot.generation >= generation,
                "generations never move backward",
            );
            generation = snapshot.generation;
        }
        task.abort();
        assert!(
            generation.0 >= 3,
            "the republisher must keep advancing; reached {generation:?}",
        );
    }
}
