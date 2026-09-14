//! Supported instance lifecycle. All maps, timers, supervision, and reports
//! here are control-plane work. `RuntimeHandle::begin` delegates directly to
//! the staged engine; usage permits remain the shutdown/admission barrier.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jiff::Timestamp;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tollgate_admission::{AdmissionCounters, AdmissionEngine, ArcSwapSnapshotMap, RequestContext};
use tollgate_core::{AccountId, DenyReason, LocalSharding, PermissionBits, Principal};
use tollgate_store::{Clock, LeaseAllocator, SnapshotSource, UsageSink};

use crate::registry::AccountBinding;
pub use crate::registry::RuntimeFundingReport;
use crate::{
    AccountLeaseConfig, LeaseCounters, LeaseManager, LeaseManagerReport, LeaseStats, SlotRegistry,
    SnapshotCounters, SnapshotManager, SnapshotManagerConfig, SnapshotManagerReport, SnapshotStats,
    TrackedPrincipals, UsageRecorder, UsageWriter, UsageWriterConfig, WriterHealth,
    WriterShutdownError, WriterStats,
};

#[derive(Debug, Clone)]
pub struct InstanceRuntimeConfig {
    pub snapshots: SnapshotManagerConfig,
    pub leases: AccountLeaseConfig,
    pub usage: UsageWriterConfig,
    pub sharding: LocalSharding,
    /// Retained snapshot histories, including in-flight authoritative reads.
    /// Cover the simultaneously served principal set; exceeding it evicts
    /// principals until a fresh source read can reconstruct their history.
    pub snapshot_history_capacity: std::num::NonZeroUsize,
    /// Time with no fresh active principal before returning routine grants.
    /// Zero requests immediate retirement; this is not a traffic-idle timer.
    pub idle_account_linger: Duration,
    pub manager_restart_backoff: Duration,
    /// One budget, measured from the first shutdown request.
    pub shutdown_deadline: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceRuntimeConfigError(pub String);
impl std::fmt::Display for InstanceRuntimeConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for InstanceRuntimeConfigError {}

impl InstanceRuntimeConfig {
    pub fn validate(&self) -> Result<(), InstanceRuntimeConfigError> {
        let error = |text: String| InstanceRuntimeConfigError(text);
        self.snapshots
            .validate()
            .map_err(|e| error(e.to_string()))?;
        if matches!(&self.snapshots.principals, TrackedPrincipals::Fixed(principals)
            if principals.len() > self.snapshot_history_capacity.get())
        {
            return Err(error(
                "fixed principals exceed snapshot_history_capacity".into(),
            ));
        }
        self.leases.validate().map_err(|e| error(e.to_string()))?;
        self.usage.validate().map_err(|e| error(e.to_string()))?;
        let phases = self
            .usage
            .shutdown_drain_deadline
            .checked_add(self.leases.shutdown_release_deadline)
            .ok_or_else(|| error("shutdown phase budgets overflow".into()))?;
        if self.shutdown_deadline < phases {
            return Err(error(
                "shutdown_deadline must cover usage drain and lease release".into(),
            ));
        }
        if self.manager_restart_backoff.is_zero() {
            return Err(error("manager_restart_backoff must be positive".into()));
        }
        for duration in [
            self.shutdown_deadline,
            self.idle_account_linger,
            self.manager_restart_backoff,
            self.snapshots.refresh_interval,
            self.snapshots.retry_backoff,
            self.snapshots.fetch_timeout,
            self.snapshots.enumeration_timeout,
            self.leases.poll_interval,
            self.leases.store_call_timeout,
            self.usage.flush_interval,
            self.usage.retry_backoff,
            self.usage.ingest_timeout,
        ] {
            if Instant::now().checked_add(duration).is_none() {
                return Err(error(
                    "runtime duration exceeds the monotonic clock domain".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountPhase {
    Running,
    Lingering,
    Retiring,
    Backoff,
    Dormant,
    Faulted,
}

#[derive(Debug, Clone)]
pub struct AccountReport {
    pub account: AccountId,
    pub phase: AccountPhase,
    pub eligible: bool,
    pub fundable: bool,
    pub task_healthy: bool,
    pub restarts: u64,
    /// Known grants lost with a dead task, excluding its recoverable current
    /// slot. Unanswered acquires are reported separately, never counted exact.
    pub unrecovered_grants: u64,
    /// Attempts whose grant outcome is unknown, including interrupted calls.
    /// This counts uncertain attempts, not confirmed grants or units.
    pub uncertain_acquires: u64,
    /// None reports counter overflow, never a wrapped or partial total.
    pub refill: Option<LeaseStats>,
}

#[derive(Debug, Clone)]
pub struct RuntimeReadiness {
    pub stopping: bool,
    pub snapshots_ready: bool,
    pub background_healthy: bool,
    pub accounting_healthy: bool,
    pub eligible_accounts: usize,
    pub unfundable_accounts: usize,
    pub unmanaged_accounts: usize,
    pub unresolved_principals: u64,
    ready: bool,
}
impl RuntimeReadiness {
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeReport {
    pub retained_accounts: usize,
    pub managed_accounts: usize,
    pub lingering_accounts: usize,
    pub retiring_accounts: usize,
    pub restarting_accounts: usize,
    pub manager_restarts: u64,
    pub unrecovered_grants: u64,
    pub uncertain_acquires: u64,
    pub counter_overflow: bool,
    pub refill: Option<LeaseStats>,
    pub snapshots: SnapshotStats,
    pub accounting: WriterHealth,
}

#[derive(Debug)]
pub enum RuntimeWriterError {
    Task(WriterShutdownError),
    Deadline(WriterHealth),
}
#[derive(Debug)]
pub struct RuntimeShutdownReport {
    pub usage: Result<WriterStats, RuntimeWriterError>,
    pub snapshots: Option<SnapshotManagerReport>,
    pub accounts: BTreeMap<AccountId, LeaseManagerReport>,
    pub unfinished_accounts: Vec<AccountId>,
    pub background_failed: bool,
    pub deadline_expired: bool,
}

struct Observation {
    phase: AccountPhase,
    health: Option<watch::Receiver<bool>>,
    counters: Option<Arc<LeaseCounters>>,
    settled: Option<LeaseStats>,
    inherited_grants: u64,
    restarts: u64,
    unrecovered: u64,
    uncertain: u64,
    overflow: bool,
}
fn add_counter(counter: &mut u64, value: u64, overflow: &mut bool) {
    if let Some(sum) = counter.checked_add(value) {
        *counter = sum;
    } else {
        *counter = u64::MAX;
        *overflow = true;
    }
}

impl Observation {
    fn uncertain_acquires(&self) -> Option<u64> {
        self.uncertain.checked_add(
            self.counters
                .as_ref()
                .map_or(0, |c| c.snapshot().uncertain_acquires),
        )
    }

    fn stats(&self) -> Option<LeaseStats> {
        self.settled?.checked_add(
            self.counters
                .as_ref()
                .map_or(LeaseStats::ZERO, |c| c.snapshot()),
        )
    }
    fn healthy(&self) -> bool {
        self.phase != AccountPhase::Faulted && self.health.as_ref().is_none_or(plane_healthy)
    }
}

struct Shared {
    engine: AdmissionEngine<Arc<ArcSwapSnapshotMap>>,
    slots: Arc<SlotRegistry>,
    recorder: UsageRecorder,
    snapshots_ready: watch::Receiver<bool>,
    snapshot_counters: Arc<SnapshotCounters>,
    observations: Mutex<BTreeMap<AccountId, Observation>>,
    stop: watch::Sender<Option<Instant>>,
    stopping: AtomicBool,
    failed: AtomicBool,
    stopped: AtomicBool,
    all: bool,
}

impl Shared {
    fn request_shutdown(&self, budget: Duration) -> Instant {
        self.stopping.store(true, Ordering::Release);
        // Sample time inside the first publication: concurrent callers and
        // background failures share one immutable, authoritative deadline.
        self.stop.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(Instant::now() + budget);
                true
            } else {
                false
            }
        });
        self.stop
            .borrow()
            .expect("shutdown request published its deadline")
    }
}

/// Cloneable request and diagnostic surface. It exposes no snapshot-map
/// mutators: the snapshot manager is the runtime's sole publication owner.
#[derive(Clone)]
pub struct RuntimeHandle {
    shared: Arc<Shared>,
    budget: Duration,
}
impl RuntimeHandle {
    #[must_use]
    pub fn funding(&self, now: Timestamp) -> RuntimeFundingReport {
        self.shared.slots.funding(now)
    }
    pub fn begin(
        &self,
        principal: Principal,
        required: PermissionBits,
        now: Timestamp,
    ) -> Result<RequestContext, DenyReason> {
        self.shared.engine.begin(principal, required, now)
    }
    #[must_use]
    pub fn recorder(&self) -> &UsageRecorder {
        &self.shared.recorder
    }
    #[must_use]
    pub fn counters(&self) -> &AdmissionCounters {
        self.shared.engine.counters()
    }

    /// Start the total deadline and withdraw readiness. Queue closure in the
    /// supervisor makes subsequent permit reservations refuse; already issued
    /// permits stay valid for draining. Repeated calls never extend the bound.
    pub fn request_shutdown(&self) -> Instant {
        self.shared.request_shutdown(self.budget)
    }

    #[must_use]
    pub fn readiness(&self, now: Timestamp) -> RuntimeReadiness {
        let stopping = self.shared.stopping.load(Ordering::Acquire);
        let (tracked, unresolved) = self.shared.slots.resolution_counts(now);
        let snapshots_ready = plane_healthy(&self.shared.snapshots_ready)
            && if self.shared.all {
                tracked == 0 || unresolved < tracked
            } else {
                unresolved == 0
            };
        let bindings = self.shared.slots.bindings();
        let eligible_accounts = bindings.iter().filter(|b| b.eligible(now)).count();
        let unfundable_accounts = bindings
            .iter()
            .filter(|b| b.eligible(now) && !b.fundable(now))
            .count();
        let observations = self
            .shared
            .observations
            .lock()
            .expect("runtime observations poisoned");
        let unmanaged_accounts = bindings
            .iter()
            .filter(|binding| {
                binding.eligible(now)
                    && !observations.get(&binding.account).is_some_and(|o| {
                        matches!(o.phase, AccountPhase::Running | AccountPhase::Lingering)
                            && o.healthy()
                    })
            })
            .count();
        let background_healthy = !self.shared.failed.load(Ordering::Acquire)
            && !self.shared.stopped.load(Ordering::Acquire)
            && self.shared.snapshots_ready.has_changed().is_ok()
            && unmanaged_accounts == 0
            && observations
                .values()
                .all(|o| o.phase != AccountPhase::Faulted);
        let accounting = self.shared.recorder.health();
        let accounting_healthy = !self.shared.recorder.is_closed()
            && accounting.stats.lost == 0
            && accounting.stats.rejected == 0
            && accounting.queue_depth < accounting.queue_capacity;
        let funded = if bindings.is_empty() {
            true
        } else if self.shared.all {
            eligible_accounts > unfundable_accounts
        } else {
            eligible_accounts > 0 && unfundable_accounts == 0
        };
        let ready =
            !stopping && snapshots_ready && background_healthy && accounting_healthy && funded;
        RuntimeReadiness {
            stopping,
            snapshots_ready,
            background_healthy,
            accounting_healthy,
            eligible_accounts,
            unfundable_accounts,
            unmanaged_accounts,
            unresolved_principals: unresolved as u64,
            ready,
        }
    }

    /// Per-account diagnostics, deliberately separate from metric labels.
    #[must_use]
    pub fn account_reports(&self, now: Timestamp) -> Vec<AccountReport> {
        let bindings: BTreeMap<_, _> = self
            .shared
            .slots
            .bindings()
            .into_iter()
            .map(|b| (b.account, b))
            .collect();
        self.shared
            .observations
            .lock()
            .expect("runtime observations poisoned")
            .iter()
            .map(|(&account, o)| {
                let binding = bindings.get(&account);
                AccountReport {
                    account,
                    phase: o.phase,
                    eligible: binding.is_some_and(|b| b.eligible(now)),
                    fundable: binding.is_some_and(|b| b.fundable(now)),
                    task_healthy: o.healthy(),
                    restarts: o.restarts,
                    unrecovered_grants: o.unrecovered,
                    uncertain_acquires: o.uncertain_acquires().unwrap_or(u64::MAX),
                    refill: o.stats(),
                }
            })
            .collect()
    }

    #[must_use]
    pub fn report(&self) -> RuntimeReport {
        let observations = self
            .shared
            .observations
            .lock()
            .expect("runtime observations poisoned");
        let mut report = RuntimeReport {
            retained_accounts: self.shared.slots.retained_slots(),
            managed_accounts: 0,
            lingering_accounts: 0,
            retiring_accounts: 0,
            restarting_accounts: 0,
            manager_restarts: 0,
            unrecovered_grants: 0,
            uncertain_acquires: 0,
            counter_overflow: false,
            refill: Some(LeaseStats::ZERO),
            snapshots: self.shared.snapshot_counters.snapshot(),
            accounting: self.shared.recorder.health(),
        };
        for o in observations.values() {
            report.counter_overflow |= o.overflow;
            let uncertain = o.uncertain_acquires();
            report.counter_overflow |= uncertain.is_none();
            match o.phase {
                AccountPhase::Running => report.managed_accounts += 1,
                AccountPhase::Lingering => {
                    report.managed_accounts += 1;
                    report.lingering_accounts += 1;
                }
                AccountPhase::Retiring => report.retiring_accounts += 1,
                AccountPhase::Backoff => report.restarting_accounts += 1,
                AccountPhase::Dormant | AccountPhase::Faulted => {}
            }
            report.refill = report.refill.and_then(|sum| sum.checked_add(o.stats()?));
            for (sum, value) in [
                (&mut report.manager_restarts, o.restarts),
                (&mut report.unrecovered_grants, o.unrecovered),
                (
                    &mut report.uncertain_acquires,
                    uncertain.unwrap_or(u64::MAX),
                ),
            ] {
                if let Some(next) = sum.checked_add(value) {
                    *sum = next;
                } else {
                    *sum = u64::MAX;
                    report.counter_overflow = true;
                }
            }
        }
        report.counter_overflow |= report.refill.is_none();
        report
    }
}

/// Unique owner of every task. Cancelling shutdown or dropping this value
/// aborts the supervisor, whose owned managers and JoinSets abort their tasks.
#[must_use = "retain the runtime and await shutdown to settle usage and leases"]
pub struct InstanceRuntime {
    handle: RuntimeHandle,
    task: Option<JoinHandle<RuntimeShutdownReport>>,
}
impl InstanceRuntime {
    #[must_use]
    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }
    pub fn spawn(
        source: Arc<dyn SnapshotSource>,
        allocator: Arc<dyn LeaseAllocator>,
        sink: Arc<dyn UsageSink>,
        clock: Arc<dyn Clock>,
        config: InstanceRuntimeConfig,
    ) -> Result<(Self, RuntimeHandle), InstanceRuntimeConfigError> {
        config.validate()?;
        let map = Arc::new(ArcSwapSnapshotMap::with_capacities(
            config.sharding,
            ArcSwapSnapshotMap::DEFAULT_MAX_NEGATIVE_ENTRIES,
            config.snapshot_history_capacity,
        ));
        let (slots, changes) = SlotRegistry::observed(config.sharding);
        let (recorder, writer) = UsageWriter::spawn(sink, Arc::clone(&clock), config.usage)
            .map_err(|e| InstanceRuntimeConfigError(e.to_string()))?;
        let snapshots = SnapshotManager::spawn(
            source,
            map.clone(),
            Arc::clone(&slots),
            Arc::clone(&clock),
            config.snapshots.clone(),
        )
        .map_err(|e| InstanceRuntimeConfigError(e.to_string()))?;
        let (stop, stopping) = watch::channel(None);
        let shared = Arc::new(Shared {
            engine: AdmissionEngine::new(map),
            slots,
            recorder,
            snapshots_ready: snapshots.ready(),
            snapshot_counters: snapshots.counters(),
            observations: Mutex::new(BTreeMap::new()),
            stop,
            stopping: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            all: matches!(config.snapshots.principals, TrackedPrincipals::All { .. }),
        });
        let handle = RuntimeHandle {
            shared: Arc::clone(&shared),
            budget: config.shutdown_deadline,
        };
        let task = tokio::spawn(supervise(
            shared, allocator, clock, config, snapshots, writer, changes, stopping,
        ));
        Ok((
            Self {
                handle: handle.clone(),
                task: Some(task),
            },
            handle,
        ))
    }
    pub async fn shutdown(mut self) -> Result<RuntimeShutdownReport, tokio::task::JoinError> {
        self.handle.request_shutdown();
        let report = self
            .task
            .as_mut()
            .expect("runtime owns its supervisor")
            .await;
        drop(self.task.take());
        report
    }
}
impl Drop for InstanceRuntime {
    fn drop(&mut self) {
        self.handle.shared.stopping.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            tracing::warn!(
                unaccounted = self.handle.shared.recorder.health().unaccounted,
                "instance runtime dropped; aborting tasks, unfinished grants require TTL reclaim"
            );
            task.abort();
        }
    }
}

fn plane_healthy(health: &watch::Receiver<bool>) -> bool {
    health.has_changed().is_ok() && *health.borrow()
}

struct Managed {
    manager: Option<LeaseManager>,
    binding: AccountBinding,
    desired: bool,
    timer: Option<Instant>,
    restart_after_death: bool,
    monitor: Option<tokio::task::Id>,
}
struct Exit {
    account: AccountId,
    report: LeaseManagerReport,
}

struct Supervisor {
    shared: Arc<Shared>,
    allocator: Arc<dyn LeaseAllocator>,
    clock: Arc<dyn Clock>,
    config: InstanceRuntimeConfig,
    accounts: BTreeMap<AccountId, Managed>,
    timers: BTreeSet<(Instant, AccountId)>,
    monitors: JoinSet<AccountId>,
    cleanup: JoinSet<Exit>,
}
impl Supervisor {
    fn phase(&self, account: AccountId, phase: AccountPhase) {
        self.shared
            .observations
            .lock()
            .expect("runtime observations poisoned")
            .get_mut(&account)
            .expect("managed account has observations")
            .phase = phase;
    }
    fn arm(&mut self, account: AccountId, after: Duration) {
        let record = self
            .accounts
            .get_mut(&account)
            .expect("timer belongs to account");
        if let Some(old) = record.timer.take() {
            self.timers.remove(&(old, account));
        }
        let at = Instant::now() + after;
        record.timer = Some(at);
        self.timers.insert((at, account));
    }
    fn disarm(&mut self, account: AccountId) {
        if let Some(at) = self
            .accounts
            .get_mut(&account)
            .expect("managed account")
            .timer
            .take()
        {
            self.timers.remove(&(at, account));
        }
    }
    fn start(&mut self, account: AccountId) {
        let record = self.accounts.get_mut(&account).expect("managed account");
        let inherited_grants = u64::from(record.binding.slot.load().is_some());
        let manager = LeaseManager::spawn(
            Arc::clone(&self.allocator),
            Arc::clone(&record.binding.slot),
            Arc::clone(&self.clock),
            self.config.leases.for_account(account),
        )
        .expect("runtime validated the complete lease configuration before spawning");
        let mut health = manager.health();
        let mut observations = self
            .shared
            .observations
            .lock()
            .expect("runtime observations poisoned");
        let o = observations.get_mut(&account).expect("managed observation");
        if record.restart_after_death {
            add_counter(&mut o.restarts, 1, &mut o.overflow);
        }
        o.inherited_grants = inherited_grants;
        o.health = Some(health.clone());
        o.counters = Some(manager.counters());
        o.phase = AccountPhase::Running;
        record.restart_after_death = false;
        record.manager = Some(manager);
        record.monitor = Some(
            self.monitors
                .spawn(async move {
                    while plane_healthy(&health) {
                        if health.changed().await.is_err() {
                            break;
                        }
                    }
                    account
                })
                .id(),
        );
    }
    fn reconcile(&mut self, binding: AccountBinding, now: Timestamp) {
        let account = binding.account;
        let desired = binding.eligible(now);
        let record = self.accounts.entry(account).or_insert_with(|| {
            self.shared
                .observations
                .lock()
                .expect("runtime observations poisoned")
                .insert(
                    account,
                    Observation {
                        phase: AccountPhase::Dormant,
                        health: None,
                        counters: None,
                        settled: Some(LeaseStats::ZERO),
                        inherited_grants: 0,
                        restarts: 0,
                        unrecovered: 0,
                        uncertain: 0,
                        overflow: false,
                    },
                );
            Managed {
                manager: None,
                binding: binding.clone(),
                desired: false,
                timer: None,
                restart_after_death: false,
                monitor: None,
            }
        });
        record.binding = binding;
        record.desired = desired;
        let phase = self
            .shared
            .observations
            .lock()
            .expect("runtime observations poisoned")[&account]
            .phase;
        match (desired, phase) {
            (true, AccountPhase::Dormant) => {
                self.disarm(account);
                self.start(account);
            }
            (true, AccountPhase::Lingering) => {
                self.disarm(account);
                self.phase(account, AccountPhase::Running);
            }
            (false, AccountPhase::Running) => {
                self.phase(account, AccountPhase::Lingering);
                self.arm(account, self.config.idle_account_linger);
            }
            (false, AccountPhase::Backoff) => {
                self.disarm(account);
                self.phase(account, AccountPhase::Dormant);
            }
            _ => {}
        }
    }

    fn retire(&mut self, account: AccountId, deadline: Instant) {
        self.disarm(account);
        if let Some(manager) = self
            .accounts
            .get_mut(&account)
            .expect("managed account")
            .manager
            .take()
        {
            self.phase(account, AccountPhase::Retiring);
            manager.stop_at(deadline);
            self.cleanup.spawn(async move {
                Exit {
                    account,
                    report: manager.shutdown().await,
                }
            });
        }
    }
    fn completed(&mut self, exit: Exit, restart: bool) {
        let account = exit.account;
        let mut observations = self
            .shared
            .observations
            .lock()
            .expect("runtime observations poisoned");
        let o = observations.get_mut(&account).expect("managed observation");
        // Retirement has already taken the manager handle out of the live
        // map. Persistent fault evidence still belongs to this joined task,
        // including faults raised by the final release pass itself.
        let faulted = o.counters.as_ref().is_some_and(|c| c.integrity_fault());
        if faulted {
            self.shared.failed.store(true, Ordering::Release);
            self.shared.request_shutdown(self.config.shutdown_deadline);
        }
        let pending = o.counters.as_ref().is_some_and(|c| c.acquire_pending());
        let last = o.counters.take().map_or(LeaseStats::ZERO, |c| c.snapshot());
        add_counter(&mut o.uncertain, u64::from(pending), &mut o.overflow);
        add_counter(&mut o.uncertain, last.uncertain_acquires, &mut o.overflow);
        if exit.report.task_died {
            // At task termination these counters are stable. Current slot
            // ownership survives; parked grants and unknown acquire outcomes
            // do not. Report them as crash exposure, never routine cleanup.
            let current = u64::from(self.accounts[&account].binding.slot.load().is_some());
            // A replacement may have released the capability inherited
            // from its predecessor. Include that opening inventory before
            // subtracting releases, or a second crash hides a parked grant.
            let unresolved = (u128::from(last.acquired) + u128::from(o.inherited_grants))
                .saturating_sub(u128::from(last.released))
                .saturating_sub(u128::from(last.abandoned))
                .saturating_sub(u128::from(current));
            let unresolved = u64::try_from(unresolved).unwrap_or_else(|_| {
                o.overflow = true;
                u64::MAX
            });
            add_counter(&mut o.unrecovered, unresolved, &mut o.overflow);
            self.accounts
                .get_mut(&account)
                .expect("managed account")
                .restart_after_death = true;
            tracing::error!(%account, unrecovered_grants = unresolved, uncertain_acquires = o.uncertain,
                "lease manager died; unrecovered grants return only at TTL reclaim");
        }
        o.settled = o.settled.and_then(|old| old.checked_add(last));
        o.health = None;
        let retry = restart && !faulted && self.accounts[&account].desired;
        o.phase = if faulted {
            AccountPhase::Faulted
        } else if retry {
            AccountPhase::Backoff
        } else {
            AccountPhase::Dormant
        };
        drop(observations);
        if retry {
            self.arm(account, self.config.manager_restart_backoff);
        }
    }
}

/// Withdraw the supervisor's liveness even before aborted children are polled.
struct SupervisorLiveness(Arc<Shared>);
impl Drop for SupervisorLiveness {
    fn drop(&mut self) {
        self.0.stopped.store(true, Ordering::Release);
    }
}

// A background task owns its collaborators for the process's life rather than
// borrowing them per call, and grouping them into a struct would name a thing
// that exists only to satisfy the lint: every field is already reachable from
// `Shared`, and the split is which handles this loop must keep alive.
#[allow(
    clippy::too_many_arguments,
    reason = "supervisor entry point: every argument is a handle it must keep alive for the process"
)]
async fn supervise(
    shared: Arc<Shared>,
    allocator: Arc<dyn LeaseAllocator>,
    clock: Arc<dyn Clock>,
    config: InstanceRuntimeConfig,
    snapshots: SnapshotManager,
    writer: UsageWriter,
    mut changes: watch::Receiver<()>,
    mut stop: watch::Receiver<Option<Instant>>,
) -> RuntimeShutdownReport {
    let _liveness = SupervisorLiveness(Arc::clone(&shared));
    let mut ready = snapshots.ready();
    let mut supervisor = Supervisor {
        shared: Arc::clone(&shared),
        allocator,
        clock,
        config,
        accounts: BTreeMap::new(),
        timers: BTreeSet::new(),
        monitors: JoinSet::new(),
        cleanup: JoinSet::new(),
    };
    let deadline = loop {
        if let Some(deadline) = *stop.borrow() {
            break deadline;
        }
        let now = supervisor.clock.now();
        for binding in shared.slots.drain_changes(now) {
            supervisor.reconcile(binding, now);
        }
        let expiry = shared.slots.next_expiry().map(|at| {
            let delay =
                std::time::Duration::try_from(now.duration_until(at)).unwrap_or(Duration::ZERO);
            Instant::now() + delay.min(Duration::from_secs(3_600))
        });
        let timer = supervisor.timers.first().map(|(at, _)| *at);
        let wake = [expiry, timer]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(3_600));
        tokio::select! {
            changed = stop.changed() => { if changed.is_err() { break shared.request_shutdown(supervisor.config.shutdown_deadline); } }
            changed = changes.changed() => { if changed.is_err() { shared.failed.store(true, Ordering::Release); break shared.request_shutdown(supervisor.config.shutdown_deadline); } }
            () = async { while ready.changed().await.is_ok() {} } => { shared.failed.store(true, Ordering::Release); break shared.request_shutdown(supervisor.config.shutdown_deadline); }
            () = shared.recorder.closed() => { shared.failed.store(true, Ordering::Release); break shared.request_shutdown(supervisor.config.shutdown_deadline); }
            exit = supervisor.cleanup.join_next(), if !supervisor.cleanup.is_empty() => {
                match exit {
                    Some(Ok(exit)) => supervisor.completed(exit, true),
                    _ => { shared.failed.store(true, Ordering::Release); break shared.request_shutdown(supervisor.config.shutdown_deadline); }
                }
            }
            notice = supervisor.monitors.join_next_with_id(), if !supervisor.monitors.is_empty() => {
                if let Some(Ok((id, account))) = notice {
                    if supervisor.accounts[&account].monitor != Some(id) { continue; }
                    if let Some(manager) = &supervisor.accounts[&account].manager {
                        if manager.counters().integrity_fault() {
                            supervisor.phase(account, AccountPhase::Faulted);
                            shared.failed.store(true, Ordering::Release);
                            break shared.request_shutdown(supervisor.config.shutdown_deadline);
                        }
                        supervisor.retire(account, Instant::now() + supervisor.config.leases.shutdown_release_deadline);
                    }
                } else { shared.failed.store(true, Ordering::Release); break shared.request_shutdown(supervisor.config.shutdown_deadline); }
            }
            () = tokio::time::sleep_until(wake) => {
                while let Some(&(at, account)) = supervisor.timers.first() {
                    if at > Instant::now() { break; }
                    supervisor.disarm(account);
                    if supervisor.accounts[&account].desired {
                        if supervisor.accounts[&account].manager.is_none() { supervisor.start(account); }
                    } else { supervisor.retire(account, Instant::now() + supervisor.config.leases.shutdown_release_deadline); }
                }
            }
        }
    };
    shared.stopping.store(true, Ordering::Release);
    for record in supervisor.accounts.values() {
        if let Some(manager) = &record.manager {
            manager.pause_refills();
        }
    }
    supervisor.monitors.abort_all();
    writer.stop_at(deadline.min(Instant::now() + supervisor.config.usage.shutdown_drain_deadline));
    let snapshot_report = tokio::time::timeout_at(deadline, snapshots.shutdown())
        .await
        .ok();
    let usage = match tokio::time::timeout_at(deadline, writer.shutdown()).await {
        Ok(Ok(stats)) => Ok(stats),
        Ok(Err(error)) => Err(RuntimeWriterError::Task(error)),
        Err(_) => Err(RuntimeWriterError::Deadline(shared.recorder.health())),
    };
    let accounts: Vec<_> = supervisor.accounts.keys().copied().collect();
    for account in accounts {
        supervisor.retire(account, deadline);
    }
    let mut returned = BTreeMap::new();
    while !supervisor.cleanup.is_empty() {
        match tokio::time::timeout_at(deadline, supervisor.cleanup.join_next()).await {
            Ok(Some(Ok(exit))) => {
                returned.insert(exit.account, exit.report);
                supervisor.completed(exit, false);
            }
            Ok(Some(Err(_))) => {
                shared.failed.store(true, Ordering::Release);
            }
            Ok(None) => break,
            Err(_) => {
                supervisor.cleanup.abort_all();
                break;
            }
        }
    }
    let unfinished_accounts = shared
        .observations
        .lock()
        .expect("runtime observations poisoned")
        .iter()
        .filter(|(_, o)| o.phase == AccountPhase::Retiring)
        .map(|(&account, _)| account)
        .collect();
    RuntimeShutdownReport {
        usage,
        snapshots: snapshot_report,
        accounts: returned,
        unfinished_accounts,
        background_failed: shared.failed.load(Ordering::Acquire),
        deadline_expired: Instant::now() >= deadline,
    }
}

#[cfg(test)]
mod health_tests {
    use super::*;
    fn diagnostic_handle() -> (RuntimeHandle, UsageWriter, watch::Sender<bool>) {
        let (recorder, writer) = UsageWriter::spawn(
            tollgate_store::MemoryStore::new(tollgate_store::GrantPolicy::default()).unwrap(),
            Arc::new(crate::ManualClock::new(
                Timestamp::from_second(100).unwrap(),
            )),
            UsageWriterConfig {
                queue_capacity: 1,
                max_batch: 1,
                flush_interval: Duration::from_millis(1),
                retry_backoff: Duration::from_millis(1),
                shutdown_drain_deadline: Duration::from_millis(10),
                ingest_timeout: Duration::from_millis(1),
            },
        )
        .unwrap();
        let (snapshots_alive, snapshots_ready) = watch::channel(true);
        let (stop, _) = watch::channel(None);
        let handle = RuntimeHandle {
            shared: Arc::new(Shared {
                engine: AdmissionEngine::new(Arc::new(ArcSwapSnapshotMap::default())),
                slots: Arc::new(SlotRegistry::default()),
                recorder,
                snapshots_ready,
                snapshot_counters: Arc::new(SnapshotCounters::default()),
                observations: Mutex::new(BTreeMap::new()),
                stop,
                stopping: AtomicBool::new(false),
                failed: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                all: true,
            }),
            budget: Duration::from_millis(20),
        };
        (handle, writer, snapshots_alive)
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_exit_withdraws_readiness_before_children_receive_their_abort() {
        let (handle, writer, _snapshots_alive) = diagnostic_handle();
        let now = Timestamp::from_second(100).unwrap();
        let liveness = SupervisorLiveness(Arc::clone(&handle.shared));
        assert!(handle.readiness(now).is_ready());
        drop(liveness);
        assert!(!handle.readiness(now).background_healthy);
        assert!(!handle.readiness(now).is_ready());
        assert!(
            !handle.recorder().is_closed(),
            "child shutdown has not been polled yet"
        );
        writer.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_component_counter_overflow_survives_aggregation_with_clean_lease_stats() {
        let (handle, writer, _snapshots_alive) = diagnostic_handle();
        let mut observation = Observation {
            phase: AccountPhase::Dormant,
            health: None,
            counters: None,
            settled: Some(LeaseStats::ZERO),
            inherited_grants: 0,
            restarts: u64::MAX,
            unrecovered: 0,
            uncertain: 0,
            overflow: false,
        };
        add_counter(&mut observation.restarts, 1, &mut observation.overflow);
        handle
            .shared
            .observations
            .lock()
            .unwrap()
            .insert(AccountId(1), observation);
        let report = handle.report();
        assert!(report.counter_overflow);
        assert_eq!(report.manager_restarts, u64::MAX);
        assert_eq!(report.refill, Some(LeaseStats::ZERO));
        writer.shutdown().await.unwrap();
    }

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
}
