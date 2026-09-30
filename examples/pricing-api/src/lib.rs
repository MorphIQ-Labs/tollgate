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
//!   → reserve usage-writer permit          (shed on backpressure, GL-8)
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
//! lease slot is stocked (INVARIANTS.md GL-10).

#![allow(
    clippy::disallowed_methods,
    reason = "the embedder is where business time legitimately enters the system (GL-100). \
              The library takes a `Timestamp` at every boundary precisely so that an \
              application reads the clock once, at its own edge, and passes the instant \
              down -- which is what makes admission replayable from its inputs. Reading \
              it here is that design working, not an escape from it; a service wanting \
              a controllable clock substitutes `tollgate_store::Clock` at these sites."
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{FromRequest, State};
use axum::http::{Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use jiff::{SignedDuration, Timestamp};
use serde::{Deserialize, Serialize};

use tollgate_admission::{CapacityGate, ExecutionCapacityGate, ExecutionCapacityMode, NoGate};
use tollgate_auth::HmacRegistry;
use tollgate_axum::{
    AdapterConfig, BearerAuth, BufferedResponse, ChargeMetadata, InputError, InputLimits,
    Rejection, Tollgate, Validated,
};
use tollgate_client::{
    Clock, SnapshotManagerConfig, SystemClock, TrackedPrincipals, UsageWriterConfig,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CapacityClass, CommitError, CostTable, CostUnits,
    DenyReason, EnforcementMode, Generation, LocalSharding, OpIndex, PermissionBits,
    PolicyRevision, Principal, PublishableSnapshot, RequestId, ResolvedLimits,
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

/// Per-connection cache supplied by the reusable Axum adapter. The alias keeps
/// existing listener setup compatible with this example.
pub use tollgate_axum::TollgateConnection as PricingConnection;

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

/// The response's charge handle: enough for a client to reconcile the
/// charge post-hoc.
#[derive(Debug, Serialize)]
pub struct ResponseMetadata {
    pub request_id: String,
    pub units_charged: u64,
    /// Which of the publisher's policies priced this request (GL-94).
    ///
    /// Read off the committed guard rather than looked up again, so it is the
    /// revision the usage event carries by construction. A real consumer
    /// resolves its customer-visible metadata — plan name, schedule version,
    /// whatever it publishes — from this value locally, with no I/O, and can
    /// state that the description matches the charge rather than hoping two
    /// lookups agreed.
    pub policy_revision: String,
}

#[derive(Debug, Serialize)]
struct Problem {
    status: u16,
    code: &'static str,
    title: String,
    units_charged: u64,
}

/// All JSON input failures use the service's problem response contract.
struct ApiJson<T>(T);
impl<T: serde::de::DeserializeOwned + Send> FromRequest<Arc<AppState>> for ApiJson<T> {
    type Rejection = Response;
    async fn from_request(
        request: Request<axum::body::Body>,
        state: &Arc<AppState>,
    ) -> Result<Self, Response> {
        Json::<T>::from_request(request, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(|error| {
                let status = error.status();
                let (index, code) = match status {
                    StatusCode::UNSUPPORTED_MEDIA_TYPE => (1, "unsupported-media-type"),
                    StatusCode::PAYLOAD_TOO_LARGE => (2, "body-too-large"),
                    _ => (0, "malformed-body"),
                };
                state.input_rejections[index].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                problem(status, code, error.body_text())
            })
    }
}

// ---- app ---------------------------------------------------------------

struct AppState {
    auth: tollgate_client::KeyVerifier,
    keys: Option<tollgate_client::KeyManagerMonitor>,
    baseline_counters: tollgate_admission::AdmissionCounters,
    capacity: Option<ExecutionCapacityGate>,
    admission: Option<tollgate_client::RuntimeHandle>,
    input_rejections: [std::sync::atomic::AtomicU64; 4],
}

impl AppState {
    fn counters(&self) -> &tollgate_admission::AdmissionCounters {
        self.admission
            .as_ref()
            .map_or(&self.baseline_counters, |runtime| runtime.counters())
    }
}

pub struct AppRuntime {
    pub store: Arc<MemoryStore>,
    background: Option<Background>,
}

struct Background {
    keys: tollgate_client::KeyManager,
    runtime: tollgate_client::InstanceRuntime,
    republishers: tokio::task::JoinSet<()>,
}

/// Keeps server ownership across cancellation, including before first poll.
struct ServerTask(tokio::task::JoinHandle<std::io::Result<()>>);
impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl AppRuntime {
    /// Wait for a fixed load-test fixture to be fully funded before warmup.
    /// Production readiness deliberately permits partially funded `All`
    /// populations; a controlled workload requires every named account.
    pub async fn wait_for_accounts(
        &self,
        accounts: &[AccountId],
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        let background = self.background.as_ref().ok_or("admission is disabled")?;
        let handle = background.runtime.handle();
        tokio::time::timeout(timeout, async {
            loop {
                let now = Timestamp::now();
                if handle.readiness(now).is_ready()
                    && fixture_accounts_funded(accounts, &handle.account_reports(now))
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "fixture accounts did not become ready within their deadline".to_owned())
    }

    pub async fn shutdown(self) {
        if let Some(mut background) = self.background {
            background.republishers.abort_all();
            let report = background.keys.shutdown().await;
            tracing::info!(?report, "credential manager shutdown");
            match background.runtime.shutdown().await {
                Ok(report) => tracing::info!(?report, "instance-runtime shutdown"),
                Err(error) => tracing::error!(%error, "instance supervisor died during shutdown"),
            }
        }
    }

    /// Stop HTTP and accounting admission together, bounding HTTP quiescence
    /// by the same deadline that drains billing and releases grants.
    pub fn shutdown_server(
        self,
        server: tokio::task::JoinHandle<std::io::Result<()>>,
        stop: tokio::sync::oneshot::Sender<()>,
    ) -> impl std::future::Future<Output = Result<(), String>> {
        let server = ServerTask(server);
        async move {
            let mut server = server;
            let deadline = self.background.as_ref().map_or_else(
                || tokio::time::Instant::now() + std::time::Duration::from_secs(15),
                |background| background.runtime.handle().request_shutdown(),
            );
            if stop.send(()).is_err() {
                tracing::debug!("HTTP server already stopped");
            }
            let result = match tokio::time::timeout_at(deadline, &mut server.0).await {
                Ok(Ok(result)) => result.map_err(|error| error.to_string()),
                Ok(Err(error)) => Err(format!("HTTP server task failed: {error}")),
                Err(_) => {
                    server.0.abort();
                    if let Err(error) = (&mut server.0).await {
                        tracing::warn!(%error, "HTTP quiescence deadline expired");
                    }
                    Err("HTTP quiescence deadline expired".into())
                }
            };
            self.shutdown().await;
            result
        }
    }
}

fn fixture_accounts_funded(
    accounts: &[AccountId],
    reports: &[tollgate_client::AccountReport],
) -> bool {
    accounts.iter().all(|account| {
        reports
            .iter()
            .any(|report| report.account == *account && report.fundable)
    })
}

pub const DEMO_API_KEY: &str = "demo-key-1";
pub const DEMO_ACCOUNT: AccountId = AccountId(1);

/// One demo tenant: an account, the credential that reaches it, and the
/// execution-capacity class its work belongs to.
///
/// The example served exactly one account until GL-99, which is why the two
/// `load/` witnesses could not be written: capacity class is *account-owned*,
/// so a publish carrying a class the ledger disagrees with is refused. Mixed
/// assured and best-effort traffic therefore needs more than one account —
/// and so, in turn, more than one lease manager, slot, and republisher. That
/// is the shape this type generalises, not a knob added for its own sake.
#[derive(Clone, Debug)]
pub struct DemoTenant {
    pub account: AccountId,
    pub api_key: String,
    pub capacity_class: CapacityClass,
}

/// The single assured tenant the example has always served.
#[must_use]
pub fn demo_tenant() -> DemoTenant {
    DemoTenant {
        account: DEMO_ACCOUNT,
        api_key: DEMO_API_KEY.to_owned(),
        capacity_class: CapacityClass::Assured,
    }
}

/// `assured` assured tenants followed by `best_effort` best-effort ones.
///
/// The first is always [`demo_tenant`], so every existing caller and test
/// keeps the account and key it already uses; the rest are numbered from
/// there. Accounts are distinct because the class is, and keys are distinct
/// because a principal is what a request arrives as.
#[must_use]
pub fn demo_tenants(assured: usize, best_effort: usize) -> Vec<DemoTenant> {
    // No special case for the first tenant: index zero already yields
    // `DEMO_ACCOUNT` and `DEMO_API_KEY` from the numbering below, so
    // short-circuiting to `demo_tenant()` was a second spelling of the same
    // rule — and a wrong one, because it also forced the class. It returned an
    // *assured* tenant for `demo_tenants(0, 1)`.
    (0..assured + best_effort)
        .map(|index| DemoTenant {
            account: AccountId(index as u128 + 1),
            api_key: format!("demo-key-{}", index + 1),
            capacity_class: if index < assured {
                CapacityClass::Assured
            } else {
                CapacityClass::BestEffort
            },
        })
        .collect()
}

/// The example's application-policy identity (GL-94). A real publisher would
/// hash the resolved product records it compiled into the snapshot; a fixed
/// value is enough to demonstrate that the response metadata and the usage
/// event name the same policy.
pub const DEMO_POLICY_REVISION: PolicyRevision = PolicyRevision([0x5e; 32]);

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
/// (INVARIANTS.md GL-3's anti-resurrection watermark), so a stalled counter
/// would silently stop extending validity rather than fail visibly.
async fn republish_snapshots(
    store: Arc<MemoryStore>,
    tenant: DemoTenant,
    principal: Principal,
    compile: impl Fn(&DemoTenant, u64) -> PublishableSnapshot,
    period: std::time::Duration,
) {
    let mut generation = 1u64;
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // immediate first tick — gen 1 published at build time
    loop {
        tick.tick().await;
        let Some(next) = generation.checked_add(1) else {
            tracing::error!(%principal, "snapshot publisher exhausted its generation");
            return;
        };
        generation = next;
        if let Err(error) = store.publish_snapshot(principal, compile(&tenant, generation)) {
            tracing::warn!(%principal, generation, %error,
                "snapshot refresh refused; preserving the last published state");
        }
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
pub async fn build_app(deposit: u64, admission_enabled: bool) -> (axum::Router, AppRuntime) {
    build_app_with(
        deposit,
        admission_enabled,
        EnforcementMode::Strict,
        LocalSharding::SINGLE,
        &[demo_tenant()],
        ExecutionCapacityMode::Disabled,
    )
    .await
}

/// The demo stack with the account's enforcement mode chosen by the caller.
///
/// Exists because an example that can only ever be `Strict` cannot show what
/// `Elastic` does — and, less obviously, cannot *test* the wiring that reads
/// the mode: a `enforcement_mode()` that ignored the snapshot entirely and
/// returned `Strict` would satisfy every assertion. The mutation gate found
/// exactly that.
pub async fn build_app_with_mode(
    deposit: u64,
    admission_enabled: bool,
    enforcement_mode: EnforcementMode,
) -> (axum::Router, AppRuntime) {
    build_app_with(
        deposit,
        admission_enabled,
        enforcement_mode,
        LocalSharding::SINGLE,
        &[demo_tenant()],
        ExecutionCapacityMode::Disabled,
    )
    .await
}

/// Build the service with an explicit instance-local hot-path shard count.
/// Keep one shard unless profiling shows sustained same-account saturation.
pub async fn build_app_with_sharding(
    deposit: u64,
    admission_enabled: bool,
    sharding: LocalSharding,
) -> (axum::Router, AppRuntime) {
    build_app_with(
        deposit,
        admission_enabled,
        EnforcementMode::Strict,
        sharding,
        &[demo_tenant()],
        ExecutionCapacityMode::Disabled,
    )
    .await
}

/// The multi-tenant, capacity-gated stack: what GL-99's load witnesses measure.
///
/// Separate from the builders above because it varies the two knobs they fix
/// by construction — how many accounts exist and how their classes are
/// divided, and whether an execution-capacity gate is installed at all. A
/// mixed-class workload needs both: the class is account-owned, so assured and
/// best-effort traffic cannot share an account, and it changes no outcome
/// unless a gate is configured to run out of capacity.
pub async fn build_app_with_capacity(
    deposit: u64,
    admission_enabled: bool,
    sharding: LocalSharding,
    tenants: &[DemoTenant],
    capacity: ExecutionCapacityMode,
) -> (axum::Router, AppRuntime) {
    build_app_with(
        deposit,
        admission_enabled,
        EnforcementMode::Strict,
        sharding,
        tenants,
        capacity,
    )
    .await
}

/// Both knobs at once. Each public builder above fixes one of them, because
/// every current caller varies one and takes the default for the other; the
/// body lives here so neither knob's default is written twice.
async fn build_app_with(
    deposit: u64,
    admission_enabled: bool,
    enforcement_mode: EnforcementMode,
    sharding: LocalSharding,
    tenants: &[DemoTenant],
    capacity: ExecutionCapacityMode,
) -> (axum::Router, AppRuntime) {
    assert!(
        !tenants.is_empty(),
        "a service with no tenant can serve nobody"
    );
    let store = MemoryStore::new(GrantPolicy::default()).expect("default grant policy is valid");
    for tenant in tenants {
        store.create_account(AccountConfig {
            account_id: tenant.account,
            initial_balance: CostUnits(deposit),
            status: AccountStatus::Active,
            // The ledger owns the class; every snapshot published below only
            // carries it. Creating the account with it is what lets the two
            // agree (GL-99).
            capacity_class: tenant.capacity_class,
        });
    }

    // Startup compilation: the service's schedule and entitlements become a
    // CostTable + AccountSnapshot once per policy generation — never per
    // request. Validity is bounded (1h) and a demo control-plane task
    // republishes with extended validity; the SnapshotManager keeps running
    // instances current (review finding GL-5 — no more install-once-forever).
    let clock: Arc<SystemClock> = Arc::new(SystemClock);
    let compile_snapshot = {
        let clock = Arc::clone(&clock);
        move |tenant: &DemoTenant, generation: u64| {
            let snapshot = Arc::new(
                AccountSnapshot::builder(
                    tenant.account,
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
                // Carried, never chosen here: the ledger decided it at
                // account creation, and a snapshot that disagreed would be
                // refused at publication (GL-99).
                .capacity_class(tenant.capacity_class)
                // A real publisher hashes the product records it compiled;
                // this example states a fixed one, which is enough to show
                // the value reaching both the response and the bill (GL-94).
                .policy_revision(DEMO_POLICY_REVISION)
                .key_id(tollgate_core::KeyId(tenant.account.0))
                .build(),
            );
            PublishableSnapshot::try_new(snapshot)
                .expect("example pricing schedule must fit inside its burst")
        }
    };

    // Public demo tokens are retained for the documented curl and paired load
    // fixtures. Production issuance uses mint(); both paths persist digests
    // before starting the same supported background projection.
    const DEMO_HMAC_SECRET: &[u8] = b"demo-server-secret-rotate-me-108-fixture";
    let issuer = HmacRegistry::new(DEMO_HMAC_SECRET);
    let mut principals = Vec::with_capacity(tenants.len());
    for tenant in tenants {
        let (principal, digest) = issuer.digest_credential(tenant.api_key.as_bytes());
        tollgate_store::KeyDirectory::insert_key(
            &*store,
            tollgate_store::KeyRecord {
                key_id: tollgate_core::KeyId(tenant.account.0),
                account_id: tenant.account,
                principal,
                digest,
                not_after: None,
            },
        )
        .await
        .expect("demo tenants have distinct accounts and credentials");
        principals.push(principal);
        store
            .publish_snapshot(principal, compile_snapshot(tenant, 1))
            .expect("initial demo snapshot matches the newly created account and credential");
    }
    let (auth, keys, key_manager) = if admission_enabled {
        let manager = tollgate_client::KeyManager::spawn(
            store.clone(),
            DEMO_HMAC_SECRET,
            clock.clone(),
            tollgate_client::KeyManagerConfig::default(),
        )
        .expect("demo credential timing configuration is valid");
        (manager.verifier(), Some(manager.monitor()), Some(manager))
    } else {
        (tollgate_client::KeyVerifier::default(), None, None)
    };

    // `Disabled` composes no gate at all rather than one that always admits,
    // so a demo that never configures capacity allocates no pool (GL-99).
    let capacity = ExecutionCapacityGate::new(capacity, sharding)
        .expect("example capacity configuration is valid");

    // Either the whole quota machinery is installed, or none of it is. The
    // branch yields both halves — what the handlers read and what shutdown
    // owns — so no caller downstream has to re-establish that they agree.
    let (admission, background) = if admission_enabled {
        let (target_grant, low_water) = refill_sizing(deposit);
        let (runtime, admission) = tollgate_client::InstanceRuntime::spawn(
            store.clone(),
            store.clone(),
            store.clone(),
            clock,
            tollgate_client::InstanceRuntimeConfig {
                snapshot_history_capacity:
                    tollgate_admission::ArcSwapSnapshotMap::DEFAULT_GENERATION_CAPACITY,
                snapshots: SnapshotManagerConfig {
                    // Stateless: any instance may serve any customer, so this
                    // one tracks everything the store knows rather than a list
                    // fixed at boot (GL-48). Seeded with the demo key so the
                    // example still works against a source that cannot
                    // enumerate.
                    principals: TrackedPrincipals::All {
                        seed: principals.clone(),
                    },
                    refresh_interval: std::time::Duration::from_secs(30),
                    unknown_ttl: SignedDuration::from_secs(60),
                    revoked_ttl: SignedDuration::from_secs(3_600),
                    retry_backoff: std::time::Duration::from_millis(200),
                    max_concurrent_fetches: 16,
                    // Above the slowest fetch this deployment's source
                    // legitimately makes, not against its fast path: an
                    // abandoned fetch keeps the principal's previous resolution
                    // and retries with backoff, so a value under real source
                    // latency would refresh nothing while looking healthy. Five
                    // seconds against an in-process store that answers in
                    // microseconds is deliberate headroom — the bound exists to
                    // keep the sweep returning (GL-103), not to police latency.
                    fetch_timeout: std::time::Duration::from_secs(5),
                    enumeration_timeout: std::time::Duration::from_secs(30),
                },
                leases: tollgate_client::AccountLeaseConfig {
                    target_grant,
                    low_water,
                    lease_ttl: SignedDuration::from_secs(60),
                    expiry_safety_margin: SignedDuration::from_secs(2),
                    poll_interval: std::time::Duration::from_millis(20),
                    store_call_timeout: std::time::Duration::from_secs(5),
                    shutdown_release_deadline: std::time::Duration::from_secs(10),
                },
                usage: UsageWriterConfig {
                    queue_capacity: 4_096,
                    max_batch: 256,
                    flush_interval: std::time::Duration::from_millis(25),
                    retry_backoff: std::time::Duration::from_millis(50),
                    shutdown_drain_deadline: std::time::Duration::from_secs(5),
                    ingest_timeout: std::time::Duration::from_secs(5),
                },
                sharding,
                idle_account_linger: std::time::Duration::from_secs(1),
                manager_restart_backoff: std::time::Duration::from_millis(200),
                shutdown_deadline: std::time::Duration::from_secs(15),
            },
        )
        .expect("example runtime configuration is valid");
        let mut republishers = tokio::task::JoinSet::new();
        for (tenant, principal) in tenants.iter().zip(&principals) {
            republishers.spawn(republish_snapshots(
                store.clone(),
                tenant.clone(),
                *principal,
                compile_snapshot.clone(),
                REPUBLISH_INTERVAL,
            ));
        }
        let background = Background {
            keys: key_manager.expect("admission owns credential refresh"),
            runtime,
            republishers,
        };
        (Some(admission), Some(background))
    } else {
        (None, None)
    };

    let state = Arc::new(AppState {
        auth,
        keys,
        baseline_counters: tollgate_admission::AdmissionCounters::default(),
        input_rejections: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
        capacity,
        admission,
    });

    let price_route = match state.admission.clone() {
        Some(runtime) => match state.capacity.clone() {
            Some(gate) => metered_price(state.clone(), runtime, gate),
            None => metered_price(state.clone(), runtime, NoGate),
        },
        None => post(baseline_price),
    };

    let router = axum::Router::new()
        .route("/livez", get(async || StatusCode::OK))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/v1/price", price_route)
        .with_state(state);
    (router, AppRuntime { store, background })
}

async fn readyz(State(state): State<Arc<AppState>>) -> StatusCode {
    let now = Timestamp::now();
    if state
        .keys
        .as_ref()
        .is_none_or(|keys| keys.report(now).ready)
        && state
            .admission
            .as_ref()
            .is_none_or(|runtime| runtime.readiness(now).is_ready())
    {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// The execution-capacity gate's configuration and current occupancy, mirrored
/// for the wire the way `Accounting` and `Refill` mirror their library stats.
///
/// Pool sizes and counts only: a capacity metric labelled by account is a
/// cardinality incident waiting for a busy tenant. The free counts are live
/// reads and therefore estimates — no decision reads them.
#[derive(Debug, Serialize)]
pub struct Capacity {
    pub shared_total: u32,
    pub shared_available: u32,
    /// Zero under `Uniform`, which has no reserve rather than an empty one.
    pub reserve_total: u32,
    pub reserve_available: u32,
}

/// What this instance admitted, refused, and has left to spend.
///
/// JSON rather than an exposition format on purpose: naming the metrics is
/// the decision, and issue GL-38 owns continuous export while GL-39 owns the
/// operator documentation. Choosing a wire format here would pre-empt both.
#[derive(Debug, Serialize)]
pub struct Metrics {
    /// Requests admitted by the engine.
    pub admitted: u64,
    /// Units quoted by admitted requests — *not* units billed. Usage events
    /// are billing truth; a request admitted and then cancelled before
    /// execution is counted here and charged nothing.
    pub units_admitted: u64,
    /// Pre-admission policy and authentication refusals. Extractor failures
    /// have their own input_rejections counters.
    pub denied: u64,
    /// Refusals by reason. Every reason is present, so a zero says "this has
    /// not happened" rather than leaving the reader to guess whether the
    /// counter exists. Ordered, so two scrapes diff cleanly.
    ///
    /// Pre-admission only. A request refused at execution start was already
    /// counted under `admitted`, and is reported by `refused_at_start` below.
    pub denials: BTreeMap<&'static str, u64>,
    /// Contexts authorized by stage one that never reached stage two: a body
    /// that failed to read, a client that went away. Neither an admission nor
    /// a refusal, and visible so a flood of them cannot look like an idle
    /// instance.
    pub contexts_abandoned: u64,
    /// Extractor refusals, labelled only by the four fixed service codes.
    pub input_rejections: BTreeMap<&'static str, u64>,
    /// Aggregate diagnostic counters could not be represented exactly.
    pub counter_overflow: bool,
    pub managed_accounts: usize,
    pub unfundable_accounts: usize,
    /// What became of every admitted request. These four partition `admitted`
    /// exactly, so an operator can see where work is going without inferring
    /// it from a difference.
    pub execution_started: u64,
    pub canceled_before_start: u64,
    pub capacity_shed: u64,
    pub refused_at_start: u64,
    /// The two capacity outcomes broken down by class, labelled by the enum
    /// tags alone so cardinality stays bounded (GL-99). Each sums to the total
    /// beside it; neither replaces one, so a reader never has to add two
    /// numbers to get one.
    pub execution_started_by_class: BTreeMap<&'static str, u64>,
    pub capacity_shed_by_class: BTreeMap<&'static str, u64>,
    /// Configured pools and free units, or `None` when no gate is installed —
    /// which is the honest answer for a disabled instance, rather than zeroes
    /// that read as an exhausted one.
    pub capacity: Option<Capacity>,
    /// Refusals at execution start by reason, in the same always-present,
    /// ordered form as `denials`.
    pub commit_refusals: BTreeMap<&'static str, u64>,
    /// Commits that settled against overage because their funding lease
    /// lapsed after admission.
    ///
    /// Disjoint from `admitted_overage`: that is credit extended because no
    /// lease could fund the request, this is credit extended because the lease
    /// that *did* fund it expired before the work began. Both climb toward an
    /// invoice; they mean different things about why.
    pub committed_at_overage: u64,
    pub units_committed_at_overage: u64,
    /// Sum of units on all installed instance leases; absent when none exist.
    /// Diagnostic estimates are read off-path.
    pub total_lease_remaining: Option<u128>,
    /// Requests admitted with no lease behind them, and the units they were
    /// quoted. Included in `admitted` / `units_admitted`, never instead of
    /// them.
    ///
    /// The pair an operator watches for an elastic account: it climbing is
    /// credit being extended, and it is a leading indicator of an invoice the
    /// way `accounting.rejected` is a leading indicator of billing loss.
    pub admitted_overage: u64,
    pub units_admitted_overage: u64,
    /// Lifetime overage spent across retained account slots, and the sum of
    /// currently eligible accounts' largest published elastic caps. Neither
    /// aggregate is a fleet limit or an admission decision.
    pub total_overage_spent: u128,
    pub total_overage_cap: Option<u128>,
    /// Earliest installed lease usability deadline among eligible accounts.
    /// Uses `expires_at - safety_margin`, the same boundary as admission.
    pub earliest_lease_usable_until: Option<String>,
    /// Billing health, absent only when admission is disabled (the load-gate
    /// baseline runs no accounting at all).
    pub accounting: Option<Accounting>,
    /// Refill health: whether this instance can keep its lease stocked.
    pub refill: Option<Refill>,
    /// Snapshot-distribution health.
    pub snapshots: Option<Snapshots>,
    /// Admission exchanges that lost a race to another core (GL-134, GL-139): which accounts,
    /// if any, are hot enough here to be worth sharding. Cumulative lower
    /// bounds; compare two scrapes.
    pub contention: Option<Contention>,
}

/// Per-account funding-line contention on this instance.
#[derive(Debug, Serialize)]
pub struct Contention {
    pub contended_exchanges: u64,
    /// At most eight accounts, most contended first.
    pub hottest: Vec<HotAccount>,
}

#[derive(Debug, Serialize)]
pub struct HotAccount {
    pub account: String,
    pub contended_exchanges: u64,
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
    /// Leases retirement or shutdown could not return within its budget.
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
    /// Fetches abandoned at the configured timeout, distinct from a source
    /// returning a failure.
    pub refresh_timeouts: u64,
    /// Enumerations the source could not answer (GL-48). Its own counter
    /// because its consequence is different: fetch failures make known
    /// principals stale, which `unresolved` shows, while enumeration failures
    /// mean *new* principals never appear — invisible in every other number,
    /// since everything already tracked keeps working.
    pub discovery_failures: u64,
    /// Stale or revoked updates refused across pushes and refreshes; excludes
    /// an unchanged positive at the already installed generation.
    pub refused_updates: u64,
    /// History reclaimed under snapshot capacity pressure.
    pub history_evictions: u64,
    /// Publications refused by history retention or a superseded source read.
    pub publication_failures: u64,
    /// Principals unresolved at the last pass. Readiness also applies the
    /// configured Fixed/All rule and requires the background task to be alive.
    pub unresolved: u64,
}

/// What the usage writer has done with the charges handed to it — readable at
/// any time, not only from a graceful shutdown (GL-38).
#[derive(Debug, Serialize)]
pub struct Accounting {
    /// Events the sink recorded.
    pub accepted: u64,
    /// Events whose request id the sink had already recorded — idempotent
    /// replay, not loss (INVARIANTS.md GL-7).
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
    /// Requests refused for want of queue capacity (INVARIANTS.md GL-8).
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
/// does the loads, the formatting and the allocation that INVARIANTS.md GL-5
/// keeps out of `admit`.
async fn metrics(State(state): State<Arc<AppState>>) -> Json<Metrics> {
    let counters = state.counters().snapshot();
    let now = Timestamp::now();
    // Three views of one plane, so they are absent together or present
    // together — a guarantee of the type now, not of this handler.
    let admission = state.admission.as_ref();
    let report = admission.map(|runtime| runtime.report());
    let funding = admission
        .map(|runtime| runtime.funding(now))
        .unwrap_or_default();
    Json(Metrics {
        admitted: counters.admitted,
        units_admitted: counters.units_admitted,
        denied: counters.denied(),
        denials: counters.denials_by_name().collect(),
        contexts_abandoned: counters.contexts_abandoned,
        input_rejections: [
            "malformed-body",
            "unsupported-media-type",
            "body-too-large",
            "missing-connection-state",
        ]
        .into_iter()
        .zip(
            state
                .input_rejections
                .iter()
                .map(|counter| counter.load(std::sync::atomic::Ordering::Relaxed)),
        )
        .collect(),
        counter_overflow: report
            .as_ref()
            .is_some_and(|report| report.counter_overflow),
        managed_accounts: report.as_ref().map_or(0, |report| report.managed_accounts),
        unfundable_accounts: admission
            .map_or(0, |runtime| runtime.readiness(now).unfundable_accounts),
        execution_started: counters.execution_started,
        canceled_before_start: counters.canceled_before_start,
        capacity_shed: counters.capacity_shed,
        execution_started_by_class: counters.execution_started_by_class_name().collect(),
        capacity_shed_by_class: counters.capacity_shed_by_class_name().collect(),
        capacity: state.capacity.as_ref().map(|gate| {
            let occupancy = gate.occupancy();
            Capacity {
                shared_total: occupancy.shared_total,
                shared_available: occupancy.shared_available,
                reserve_total: occupancy.reserve_total,
                reserve_available: occupancy.reserve_available,
            }
        }),
        refused_at_start: counters.refused_at_start(),
        commit_refusals: counters.commit_refusals_by_name().collect(),
        committed_at_overage: counters.committed_at_overage,
        units_committed_at_overage: counters.units_committed_at_overage,
        total_lease_remaining: funding.total_lease_remaining,
        admitted_overage: counters.admitted_overage,
        units_admitted_overage: counters.units_admitted_overage,
        total_overage_spent: funding.total_overage_spent,
        total_overage_cap: funding.total_overage_cap,
        earliest_lease_usable_until: funding.earliest_lease_usable_until.map(|at| at.to_string()),
        accounting: admission.map(|admission| {
            let health = admission.recorder().health();
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
        refill: report.as_ref().and_then(|report| {
            let stats = report.refill?;
            Some(Refill {
                acquired: stats.acquired,
                acquired_units: stats.acquired_units,
                acquire_timeouts: stats.acquire_timeouts,
                refused: stats.refused(),
                refusals: stats.refusals_by_name().collect(),
                released: stats.released,
                abandoned: stats.abandoned,
            })
        }),
        snapshots: report.as_ref().map(|report| {
            let stats = report.snapshots;
            Snapshots {
                refresh_attempts: stats.refresh_attempts,
                refresh_failures: stats.refresh_failures,
                refresh_timeouts: stats.refresh_timeouts,
                discovery_failures: stats.discovery_failures,
                refused_updates: stats.refused_updates,
                history_evictions: stats.history_evictions,
                publication_failures: stats.publication_failures,
                unresolved: stats.unresolved,
            }
        }),
        contention: report.as_ref().map(|report| Contention {
            contended_exchanges: report.contention.contended_exchanges,
            hottest: report
                .contention
                .hottest
                .iter()
                .map(|&(account, contended_exchanges)| HotAccount {
                    account: account.to_string(),
                    contended_exchanges,
                })
                .collect(),
        }),
    })
}

fn problem(status: StatusCode, code: &'static str, title: impl Into<String>) -> Response {
    buffered_problem(status, code, title).into_response()
}

fn buffered_problem(
    status: StatusCode,
    code: &'static str,
    title: impl Into<String>,
) -> BufferedResponse {
    BufferedResponse::json(
        status,
        &Problem {
            status: status.as_u16(),
            code,
            title: title.into(),
            units_charged: 0,
        },
    )
    .expect("primitive problem fields serialize")
    .with_header(
        header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/problem+json"),
    )
}

#[cfg(test)]
fn deny_response(reason: DenyReason) -> Response {
    deny_buffered(reason).into_response()
}

fn deny_buffered(reason: DenyReason) -> BufferedResponse {
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
        DenyReason::BalanceExhausted => (StatusCode::PAYMENT_REQUIRED, "balance-exhausted"),
        // Not retryable at this quote: the account's remaining funding,
        // counting units held in leases, is below it. A smaller request, a
        // top-up, or the next period can succeed.
        DenyReason::BalanceInsufficient { .. } => {
            (StatusCode::PAYMENT_REQUIRED, "balance-insufficient")
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
        // 503, not 429: the caller did nothing wrong and slowing down would
        // not help — the instance is full, and capacity returns as requests
        // finish rather than as a quota refills (GL-99).
        DenyReason::CapacityUnavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "capacity-unavailable")
        }
    };
    buffered_problem(status, code, reason.to_string())
}

fn metered_price<G: CapacityGate>(
    state: Arc<AppState>,
    runtime: tollgate_client::RuntimeHandle,
    capacity: G,
) -> axum::routing::MethodRouter<Arc<AppState>> {
    let adapter = Tollgate::new(AdapterConfig {
        runtime,
        authenticator: BearerAuth::new(Arc::new(state.auth.clone())),
        clock: Arc::new(SystemClock),
        request_ids: || Ok(RequestId(uuid::Uuid::new_v4().as_u128())),
        capacity,
    });
    adapter
        .post_json_with_error_handler(
            Op::Price,
            PERMISSION_PRICE,
            InputLimits::new(2 * 1024 * 1024, std::time::Duration::from_secs(30))
                .expect("positive bounded route limits"),
            |request: PriceRequest| {
                let quantity = u64::try_from(request.contracts.len())
                    .map_err(|_| InputError("too many contracts"))?;
                Ok(Validated::new(request, quantity))
            },
            |request, charge| async move {
                BufferedResponse::json(
                    StatusCode::OK,
                    &PriceResponse {
                        prices: request.contracts.iter().map(black_scholes_call).collect(),
                        metadata: ResponseMetadata {
                            request_id: charge.request_id.to_string(),
                            units_charged: charge.units_charged.get(),
                            policy_revision: charge.policy_revision.to_string(),
                        },
                    },
                )
            },
            move |error, charge| pricing_rejection(&state, error, charge),
        )
        .with_state(())
}

fn pricing_rejection(
    state: &AppState,
    error: &Rejection,
    charge: Option<ChargeMetadata>,
) -> BufferedResponse {
    match error {
        Rejection::Denied(reason) | Rejection::Commit(CommitError::Denied(reason)) => {
            deny_buffered(*reason)
        }
        Rejection::Commit(CommitError::Cancelled | CommitError::AlreadyReleased) => {
            deny_buffered(DenyReason::FundingExpiredAtStart)
        }
        Rejection::Json(error) => {
            let status = error.status();
            let (index, code) = match status {
                StatusCode::UNSUPPORTED_MEDIA_TYPE => (1, "unsupported-media-type"),
                StatusCode::PAYLOAD_TOO_LARGE => (2, "body-too-large"),
                _ => (0, "malformed-body"),
            };
            state.input_rejections[index].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            buffered_problem(status, code, error.body_text())
        }
        Rejection::BodyTooLarge => {
            state.input_rejections[2].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            buffered_problem(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body-too-large",
                "Failed to buffer the request body: length limit exceeded",
            )
        }
        Rejection::MissingConnection => {
            state.input_rejections[3].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            buffered_problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "missing-connection-state",
                "Connection authentication state is unavailable",
            )
        }
        _ => tollgate_axum::render_rejection(error, charge),
    }
}

async fn baseline_price(ApiJson(request): ApiJson<PriceRequest>) -> Response {
    // Load-gate baseline: transport + kernel, with admission explicitly off.
    Json(PriceResponse {
        prices: request.contracts.iter().map(black_scholes_call).collect(),
        metadata: ResponseMetadata {
            request_id: "baseline".to_string(),
            units_charged: 0,
            policy_revision: PolicyRevision::UNSTATED.to_string(),
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
    use std::collections::BTreeSet;
    use tollgate_store::{SnapshotResolution, SnapshotSource};

    #[test]
    fn fixture_readiness_requires_each_named_account_to_be_funded() {
        let report = |account, fundable| tollgate_client::AccountReport {
            account: AccountId(account),
            phase: tollgate_client::AccountPhase::Running,
            eligible: true,
            fundable,
            task_healthy: true,
            restarts: 0,
            unrecovered_grants: 0,
            uncertain_acquires: 0,
            refill: None,
        };
        let accounts = [AccountId(1), AccountId(2)];
        assert!(!fixture_accounts_funded(&accounts, &[]));
        assert!(!fixture_accounts_funded(&accounts, &[report(1, true)]));
        assert!(!fixture_accounts_funded(
            &accounts,
            &[report(1, true), report(2, false)]
        ));
        // An equally large funded population does not prove these identities.
        assert!(!fixture_accounts_funded(
            &accounts,
            &[report(1, true), report(3, true)]
        ));
        assert!(fixture_accounts_funded(
            &accounts,
            &[report(2, true), report(1, true)]
        ));
    }

    #[tokio::test]
    async fn fixture_readiness_wait_is_bounded_and_observes_shutdown() {
        let (_, baseline) = build_app(10_000, false).await;
        assert_eq!(
            baseline
                .wait_for_accounts(&[DEMO_ACCOUNT], std::time::Duration::from_secs(1))
                .await,
            Err("admission is disabled".to_owned())
        );
        baseline.shutdown().await;

        let tenants = demo_tenants(1, 1);
        let (_, runtime) = build_app_with_capacity(
            100_000,
            true,
            LocalSharding::SINGLE,
            &tenants,
            ExecutionCapacityMode::Disabled,
        )
        .await;
        let accounts: Vec<_> = tenants.iter().map(|tenant| tenant.account).collect();
        runtime
            .wait_for_accounts(&accounts, std::time::Duration::from_secs(2))
            .await
            .unwrap();

        // Aggregate readiness is already true, but this identity never exists.
        assert_eq!(
            runtime
                .wait_for_accounts(
                    &[AccountId(u128::MAX)],
                    std::time::Duration::from_millis(20)
                )
                .await,
            Err("fixture accounts did not become ready within their deadline".to_owned())
        );
        let _ = runtime
            .background
            .as_ref()
            .unwrap()
            .runtime
            .handle()
            .request_shutdown();
        assert_eq!(
            runtime
                .wait_for_accounts(&accounts, std::time::Duration::from_millis(20))
                .await,
            Err("fixture accounts did not become ready within their deadline".to_owned())
        );
        runtime.shutdown().await;
    }

    /// The adapter's defaults share most status codes with the example but
    /// not its titles or canceled-commit code. Pin the application's complete
    /// pre-execution response contract at the custom renderer boundary.
    #[tokio::test]
    async fn adapter_rejections_preserve_the_pricing_problem_contract() {
        let state = AppState {
            auth: tollgate_client::KeyVerifier::default(),
            keys: None,
            baseline_counters: tollgate_admission::AdmissionCounters::default(),
            capacity: None,
            admission: None,
            input_rejections: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
        };
        for (error, status, code, title, units) in [
            (
                Rejection::Denied(DenyReason::UnknownPrincipal),
                StatusCode::UNAUTHORIZED,
                "unknown-principal",
                DenyReason::UnknownPrincipal.to_string(),
                serde_json::json!(0),
            ),
            (
                Rejection::Commit(CommitError::Denied(DenyReason::FundingExpiredAtStart)),
                StatusCode::SERVICE_UNAVAILABLE,
                "funding-expired-at-start",
                DenyReason::FundingExpiredAtStart.to_string(),
                serde_json::json!(0),
            ),
            (
                Rejection::Commit(CommitError::Cancelled),
                StatusCode::SERVICE_UNAVAILABLE,
                "funding-expired-at-start",
                DenyReason::FundingExpiredAtStart.to_string(),
                serde_json::json!(0),
            ),
            (
                Rejection::Commit(CommitError::AlreadyReleased),
                StatusCode::SERVICE_UNAVAILABLE,
                "funding-expired-at-start",
                DenyReason::FundingExpiredAtStart.to_string(),
                serde_json::json!(0),
            ),
            (
                Rejection::Commit(CommitError::AlreadyCommitted),
                StatusCode::INTERNAL_SERVER_ERROR,
                "charge-state-invalid",
                "charge-state-invalid".to_owned(),
                serde_json::Value::Null,
            ),
        ] {
            let response = pricing_rejection(&state, &error, None).into_response();
            assert_eq!(response.status(), status);
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                "application/problem+json"
            );
            let bytes = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                body,
                serde_json::json!({
                    "status": status.as_u16(), "code": code, "title": title, "units_charged": units,
                })
            );
        }
        assert!(
            state
                .input_rejections
                .iter()
                .all(|counter| counter.load(std::sync::atomic::Ordering::Relaxed) == 0)
        );
    }

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
        let compile = |tenant: &DemoTenant, generation: u64| {
            PublishableSnapshot::try_new(Arc::new(
                AccountSnapshot::builder(
                    tenant.account,
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
        let tenant = demo_tenant();
        store
            .publish_snapshot(principal, compile(&tenant, 1))
            .expect("snapshot fixture matches its account and credential");

        let task = tokio::spawn(republish_snapshots(
            store.clone(),
            tenant,
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

    /// `demo_tenants` produces the split it promises: the primary first, then
    /// the assured, then the best-effort, on distinct accounts and keys.
    ///
    /// Mutation testing asked for it — `assured + best_effort` could become a
    /// subtraction and `index < assured` could invert, with every other test
    /// still green because nothing counted what came out.
    #[test]
    fn demo_tenants_splits_assured_before_best_effort() {
        for (assured, best_effort) in [(1usize, 1usize), (2, 3), (5, 5), (1, 0), (0, 1)] {
            let tenants = demo_tenants(assured, best_effort);
            assert_eq!(
                tenants.len(),
                assured + best_effort,
                "{assured}+{best_effort} produced {} tenants",
                tenants.len()
            );
            for (index, tenant) in tenants.iter().enumerate() {
                let expected = if index < assured {
                    CapacityClass::Assured
                } else {
                    CapacityClass::BestEffort
                };
                assert_eq!(tenant.capacity_class, expected, "tenant {index}");
            }
            // Distinct accounts and distinct credentials, because the class is
            // account-owned and a request arrives as a principal.
            let accounts: BTreeSet<_> = tenants.iter().map(|t| t.account).collect();
            let keys: BTreeSet<_> = tenants.iter().map(|t| t.api_key.clone()).collect();
            assert_eq!(accounts.len(), tenants.len());
            assert_eq!(keys.len(), tenants.len());
        }

        // The first tenant is always the one every existing caller and test
        // already uses, so nothing they pinned moved.
        let first = &demo_tenants(3, 2)[0];
        assert_eq!(first.account, DEMO_ACCOUNT);
        assert_eq!(first.api_key, DEMO_API_KEY);
        assert_eq!(first.capacity_class, CapacityClass::Assured);
    }

    /// GL-99's refusal reaches a caller as 503 `capacity-unavailable`.
    ///
    /// Deliberately not 429: the caller did nothing wrong and slowing down
    /// would not help, because capacity returns as requests finish rather than
    /// as a quota refills. Nothing else pinned this arm — the load gate is
    /// where the shed is *produced*, and a scenario that must saturate a pool
    /// to assert a status code would be a flaky home for the contract.
    #[tokio::test]
    async fn a_capacity_refusal_is_a_retryable_503_and_not_a_rate_limit() {
        let response = deny_response(DenyReason::CapacityUnavailable);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("a problem document is small");
        let problem: serde_json::Value =
            serde_json::from_slice(&bytes).expect("problem documents are JSON");
        assert_eq!(problem["code"], "capacity-unavailable");
    }
}
