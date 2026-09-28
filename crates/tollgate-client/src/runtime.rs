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
use tollgate_core::{
    AccountId, DenyReason, LocalSharding, PermissionBits, Principal, ShardOccupancy,
};
use tollgate_store::{Clock, LeaseAllocator, SnapshotSource, UsageSink};

use crate::registry::AccountBinding;
pub use crate::registry::RuntimeFundingReport;
use crate::{
    AccountLeaseConfig, LeaseCounters, LeaseManager, LeaseManagerReport, LeaseStats, SlotRegistry,
    SnapshotCounters, SnapshotManager, SnapshotManagerConfig, SnapshotManagerReport, SnapshotStats,
    TrackedPrincipals, UsageRecorder, UsageWriter, UsageWriterConfig, WriterHealth,
    WriterShutdownError, WriterStats,
};

/// Everything an [`InstanceRuntime`] needs to run one instance's control
/// plane: snapshot distribution, per-account lease refill, usage accounting,
/// local sharding, account lifecycle timing and the shutdown budget.
///
/// There are no defaults; every value is a deployment decision.
/// [`validate`](Self::validate) runs before any task starts (INVARIANTS.md 16),
/// and `docs/GETTING_STARTED.md` walks through a worked configuration.
#[derive(Debug, Clone)]
pub struct InstanceRuntimeConfig {
    /// Snapshot distribution: which principals to serve, and how often and
    /// how patiently to fetch them. Validated by
    /// [`SnapshotManagerConfig::validate`]; a [`TrackedPrincipals::Fixed`]
    /// list must also fit `snapshot_history_capacity`.
    pub snapshots: SnapshotManagerConfig,
    /// Lease refill settings applied to every account the runtime discovers.
    /// Validated by [`AccountLeaseConfig::validate`]. Its
    /// `shutdown_release_deadline` is one of the two phases
    /// `shutdown_deadline` must cover.
    pub leases: AccountLeaseConfig,
    /// The bounded usage queue and its writer. Validated by
    /// [`UsageWriterConfig::validate`]. Its `shutdown_drain_deadline` is the
    /// other phase `shutdown_deadline` must cover.
    pub usage: UsageWriterConfig,
    /// Instance-local shard layout for the snapshot map and every account's
    /// lease slot. [`LocalSharding::SINGLE`] is the unsharded layout; more
    /// shards trade per-account memory for less cache-line sharing between
    /// request-serving threads, and help only while those threads do not
    /// outnumber the shards ([`RuntimeReport::sharding`] reports whether they
    /// do). See `docs/LOCAL_SHARDING.md`.
    pub sharding: LocalSharding,
    /// Retained snapshot histories, including in-flight authoritative reads.
    /// Cover the simultaneously served principal set; exceeding it evicts
    /// principals until a fresh source read can reconstruct their history.
    ///
    /// Counted in principals. Too small evicts live principals, which deny
    /// until the next authoritative read restores them and show up in
    /// [`SnapshotStats::history_evictions`]; larger costs memory per retained
    /// history. Must be at least the length of a
    /// [`TrackedPrincipals::Fixed`] list.
    pub snapshot_history_capacity: std::num::NonZeroUsize,
    /// Time with no fresh active principal before returning routine grants.
    /// Zero requests immediate retirement; this is not a traffic-idle timer.
    ///
    /// An account becomes ineligible when its last active, unexpired
    /// snapshot is removed, revoked, suspended or expires; its lease manager
    /// then lingers for this long and is retired (releasing its lease) unless
    /// a fresh active principal returns first. Too short churns lease
    /// acquire and release when an account's snapshots briefly lapse; too
    /// long holds granted units on an instance that can no longer spend
    /// them. Must fit the monotonic clock.
    pub idle_account_linger: Duration,
    /// Delay before restarting an account's lease manager that exited while
    /// the account was still eligible. The account is in
    /// [`AccountPhase::Backoff`] meanwhile, with no manager refilling its
    /// slot.
    ///
    /// Too short retries a failing allocator or a crashing task in a tight
    /// loop; too long leaves the account's slot unrefilled, and readiness
    /// counts it unmanaged, for the whole delay. An integrity fault is never
    /// restarted: it shuts the runtime down instead (INVARIANTS.md 31). Must
    /// be positive.
    pub manager_restart_backoff: Duration,
    /// One budget, measured from the first shutdown request.
    ///
    /// Covers the usage drain, the snapshot manager's stop and every
    /// account's lease release; a background failure that triggers shutdown
    /// starts it too. Must be at least
    /// `usage.shutdown_drain_deadline + leases.shutdown_release_deadline`.
    /// Too short leaves usage unresolved and leases abandoned to TTL reclaim
    /// (INVARIANTS.md 9), reported in [`RuntimeShutdownReport`]; the value is
    /// also how long an orchestrator must allow the process to stop.
    pub shutdown_deadline: Duration,
}

/// Why an [`InstanceRuntimeConfig`] was refused, as a human-readable
/// description of the first rule it broke. Also carries a component
/// configuration error raised while spawning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceRuntimeConfigError(pub String);
impl std::fmt::Display for InstanceRuntimeConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for InstanceRuntimeConfigError {}

impl InstanceRuntimeConfig {
    /// Check the whole configuration without starting anything: each
    /// component's own `validate`, a fixed principal list within
    /// `snapshot_history_capacity`, a `shutdown_deadline` that covers the
    /// usage drain plus lease release, a positive `manager_restart_backoff`,
    /// and every duration representable on the monotonic clock.
    /// [`InstanceRuntime::spawn`] calls this first.
    ///
    /// # Errors
    ///
    /// The first rule the configuration breaks.
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

/// Where an account's lease manager is in the runtime's lifecycle.
///
/// An account is *eligible* while at least one of its principals has an
/// active, unexpired snapshot; the runtime keeps at most one manager per
/// account (INVARIANTS.md 31).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountPhase {
    /// Eligible, with a manager refilling its slot.
    Running,
    /// No longer eligible, but its manager is still running until
    /// [`InstanceRuntimeConfig::idle_account_linger`] expires. Becoming
    /// eligible again cancels the linger and returns it to `Running`.
    Lingering,
    /// Its manager has been told to stop and is releasing its leases; the
    /// runtime has not yet joined it. A replacement waits for that join.
    Retiring,
    /// Its manager exited while the account was still eligible; a new one
    /// starts after [`InstanceRuntimeConfig::manager_restart_backoff`].
    Backoff,
    /// No manager: the account is known but not eligible, or its manager was
    /// retired.
    Dormant,
    /// Its manager recorded an accounting-integrity fault, such as the store
    /// rejecting a release as an accounting error. Terminal: the runtime
    /// shuts down rather than restart it.
    Faulted,
}

/// One account's lifecycle and refill diagnostics, from
/// [`RuntimeHandle::account_reports`]. Kept out of metric labels because the
/// account set is unbounded.
#[derive(Debug, Clone)]
pub struct AccountReport {
    /// The account reported on.
    pub account: AccountId,
    /// Its lease manager's current lifecycle phase.
    pub phase: AccountPhase,
    /// At least one of its principals has an active, unexpired snapshot at
    /// the supplied time.
    pub eligible: bool,
    /// It is eligible and can fund work now: an active snapshot and either a
    /// usable lease with units remaining or, under elastic enforcement,
    /// overage headroom. Eligible but not fundable means requests deny for
    /// want of funding.
    pub fundable: bool,
    /// Its manager is not faulted and, if running, its health watch is still
    /// true. False means the task died or recorded a fault.
    pub task_healthy: bool,
    /// Times a replacement manager started after an unexpected exit. A
    /// rising count is a manager that keeps dying.
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

/// Whether this instance can admit work now, and which condition withdrew it
/// when it cannot (INVARIANTS.md 10). From [`RuntimeHandle::readiness`].
///
/// [`is_ready`](Self::is_ready) is the probe's answer; the fields are its
/// inputs, for diagnosis.
#[derive(Debug, Clone)]
pub struct RuntimeReadiness {
    /// Shutdown has been requested, or the runtime owner was dropped.
    /// Readiness is withdrawn for good.
    pub stopping: bool,
    /// The snapshot task is alive and ready, and current resolutions meet the
    /// tracking rule: with [`TrackedPrincipals::Fixed`] every tracked
    /// principal is resolved; with [`TrackedPrincipals::All`] at least one is,
    /// or none is tracked.
    pub snapshots_ready: bool,
    /// The supervisor and snapshot task are alive, no background failure has
    /// been recorded, no account is faulted, and every eligible account has a
    /// healthy running or lingering manager.
    pub background_healthy: bool,
    /// The usage writer is open, has recorded no `lost` or `rejected`
    /// events, and its queue is below capacity. A sticky `lost` or `rejected`
    /// keeps this false for the rest of the process's life.
    pub accounting_healthy: bool,
    /// Accounts with at least one active, unexpired snapshot.
    pub eligible_accounts: usize,
    /// Eligible accounts that cannot fund work now: no usable lease with
    /// units remaining and no elastic overage headroom. With `Fixed`
    /// tracking any such account withdraws readiness; with `All`, readiness
    /// needs only some eligible account to be fundable.
    pub unfundable_accounts: usize,
    /// Eligible accounts without a healthy running or lingering lease
    /// manager: starting, backing off after a crash, retiring or faulted.
    /// Nonzero withdraws readiness through `background_healthy`.
    pub unmanaged_accounts: usize,
    /// Tracked principals with no currently valid resolution (neither an
    /// unexpired snapshot nor an unexpired negative entry).
    pub unresolved_principals: u64,
    ready: bool,
}
impl RuntimeReadiness {
    /// True only when the runtime is not stopping, snapshots are ready,
    /// background tasks and accounting are healthy, and the funding rule
    /// holds: with no account holding a published snapshot, trivially; with
    /// `All` tracking, some
    /// eligible account is fundable; with `Fixed`, at least one account is
    /// eligible and every eligible account is fundable.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready
    }
}

/// Instance-wide aggregate of the runtime's accounts, refill, snapshot and
/// accounting counters, from [`RuntimeHandle::report`]. Safe to expose as
/// metrics: it carries no per-account labels except the bounded
/// [`ContentionReport::hottest`] list.
#[derive(Debug, Clone)]
pub struct RuntimeReport {
    /// Accounts with a lease slot on this instance. Slots are kept for the
    /// process's life because they carry irreversible overage spend, so this
    /// only grows.
    pub retained_accounts: usize,
    /// Accounts with a live lease manager: running or lingering.
    pub managed_accounts: usize,
    /// Of `managed_accounts`, those no longer eligible and waiting out
    /// [`InstanceRuntimeConfig::idle_account_linger`].
    pub lingering_accounts: usize,
    /// Accounts whose manager is releasing its leases and not yet joined.
    pub retiring_accounts: usize,
    /// Accounts waiting out [`InstanceRuntimeConfig::manager_restart_backoff`]
    /// after their manager exited unexpectedly.
    pub restarting_accounts: usize,
    /// Manager restarts after an unexpected exit, summed across accounts.
    /// Nonzero means a lease manager died; a rising count means one keeps
    /// dying.
    pub manager_restarts: u64,
    /// Summed across accounts: see [`AccountReport::unrecovered_grants`].
    /// Nonzero is crash exposure — granted units that return only at TTL
    /// reclaim (INVARIANTS.md 9).
    pub unrecovered_grants: u64,
    /// Summed across accounts: see [`AccountReport::uncertain_acquires`].
    pub uncertain_acquires: u64,
    /// Some counter in this report, or one it aggregates, exceeded `u64` and
    /// is saturated. Totals are then lower bounds, never wrapped values.
    pub counter_overflow: bool,
    /// Refill counters summed across every account, including managers that
    /// have since retired or died. `None` when the sum overflowed.
    pub refill: Option<LeaseStats>,
    /// The snapshot task's counters.
    pub snapshots: SnapshotStats,
    /// The usage writer's accounting health.
    pub accounting: WriterHealth,
    /// What this instance's shard layout is carrying (GL-124).
    ///
    /// Sharding buys one thing — a request-serving thread writing to lines no
    /// peer writes — and that holds only while the affinities handed out do not
    /// outnumber the shards. When it stops holding, the instance degrades
    /// toward the unsharded cost while looking exactly like the contention the
    /// layout was enabled to remove, so it is reported rather than left to be
    /// inferred from latency. `ShardOccupancy::is_crowded` is the question;
    /// `docs/LOCAL_SHARDING.md` is what to do about the answer.
    pub sharding: ShardOccupancy,
    /// Admission exchanges that lost a race to another core, per account:
    /// lease and overage debits and concurrency-gauge acquisitions (GL-134, GL-139).
    ///
    /// Which accounts, if any, are hot enough on this instance that their
    /// funding line is written from several cores at once — the condition
    /// opt-in lease sharding exists for (`docs/LOCAL_SHARDING.md`). Cumulative
    /// since the process started, so read it as a rate between reports.
    pub contention: ContentionReport,
}

/// How contended this instance's account funding lines have been.
///
/// Counts are lower bounds: see [`LocalLease::contended_debits`] for what a
/// lost-race counter cannot see.
///
/// [`LocalLease::contended_debits`]: tollgate_core::LocalLease::contended_debits
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentionReport {
    /// Lost exchanges across every retained account.
    pub contended_exchanges: u64,
    /// The most-contended accounts, most contended first, ties by account id;
    /// at most [`ContentionReport::HOTTEST`] and only accounts with a nonzero
    /// count. Bounded so the report never grows with the account table.
    pub hottest: Vec<(AccountId, u64)>,
}

impl ContentionReport {
    /// How many accounts [`hottest`](Self::hottest) names at most.
    pub const HOTTEST: usize = 8;

    fn collect(slots: Vec<(AccountId, Arc<tollgate_admission::LeaseSlot>)>) -> Self {
        Self::rank(
            slots
                .into_iter()
                .map(|(account, slot)| (account, slot.contended_exchanges())),
        )
    }

    fn rank(counts: impl Iterator<Item = (AccountId, u64)>) -> Self {
        let mut counted: Vec<(AccountId, u64)> = counts.filter(|&(_, count)| count > 0).collect();
        let contended_exchanges = counted
            .iter()
            .fold(0u64, |total, &(_, count)| total.saturating_add(count));
        // A total order, so the registry's hash order never reaches the
        // report: most contended first, then by account.
        counted.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        counted.truncate(Self::HOTTEST);
        Self {
            contended_exchanges,
            hottest: counted,
        }
    }
}

/// Why the usage writer produced no terminal [`WriterStats`] during runtime
/// shutdown.
#[derive(Debug)]
pub enum RuntimeWriterError {
    /// The writer task panicked or was aborted; the error carries a lower
    /// bound on committed charges it left with no billing record.
    Task(WriterShutdownError),
    /// The runtime's shutdown deadline expired before the writer finished.
    /// Carries its accounting health read at that moment; its `unaccounted`
    /// and queue depth are the charges still without an outcome.
    Deadline(WriterHealth),
}
/// What [`InstanceRuntime::shutdown`] observed, component by component.
/// Nothing here is silent: every shortfall names what it left behind.
#[derive(Debug)]
pub struct RuntimeShutdownReport {
    /// The usage writer's terminal counters, or why there are none. Nonzero
    /// `lost` or `unresolved` are committed charges that were not billed.
    pub usage: Result<WriterStats, RuntimeWriterError>,
    /// The snapshot manager's shutdown report, or `None` when it did not
    /// stop within the deadline.
    pub snapshots: Option<SnapshotManagerReport>,
    /// Lease release outcomes for every account whose manager was joined
    /// during shutdown. Nonzero `abandoned` counts leases left to TTL
    /// reclaim (INVARIANTS.md 9).
    pub accounts: BTreeMap<AccountId, LeaseManagerReport>,
    /// Accounts whose manager was still retiring when shutdown finished: its
    /// release outcome is unknown and its leases settle at TTL reclaim.
    pub unfinished_accounts: Vec<AccountId>,
    /// A background component failed during the runtime's life or its
    /// shutdown: a task exited unexpectedly, a join failed, or an account
    /// recorded an integrity fault. Such a failure is also what starts an
    /// unrequested shutdown.
    pub background_failed: bool,
    /// Shutdown finished at or after the deadline, so some phase may have
    /// been cut short; the other fields say which.
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
    /// Instance-wide funding estimates at `now`: lease units in hand, overage
    /// spent and capped, and the earliest lease usability deadline. A
    /// diagnostic read that takes the registry lock; admission never reads
    /// it.
    #[must_use]
    pub fn funding(&self, now: Timestamp) -> RuntimeFundingReport {
        self.shared.slots.funding(now)
    }
    /// Begin staged admission for `principal` at the caller-supplied `now`:
    /// one snapshot lookup, the account status check and the route's
    /// `required` permission check. Request path: no I/O, no blocking lock
    /// and no clock read (INVARIANTS.md 5). The returned [`RequestContext`]
    /// retains the principal's snapshot generation for the rest of the
    /// request (INVARIANTS.md 26).
    ///
    /// # Errors
    ///
    /// The [`DenyReason`] for an unknown principal, an inactive account, an
    /// expired snapshot or a missing permission, counted in
    /// [`counters`](Self::counters). Nothing is charged.
    pub fn begin(
        &self,
        principal: Principal,
        required: PermissionBits,
        now: Timestamp,
    ) -> Result<RequestContext, DenyReason> {
        self.shared.engine.begin(principal, required, now)
    }
    /// The usage queue's request-side handle. Reserve a permit with
    /// [`UsageRecorder::try_reserve`] before admitting work, so a full queue
    /// sheds before anything is charged (INVARIANTS.md 8).
    #[must_use]
    pub fn recorder(&self) -> &UsageRecorder {
        &self.shared.recorder
    }
    /// What this instance has admitted and refused, per outcome.
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

    /// Whether this instance can admit work at `now`, with the inputs to that
    /// answer. Serve a readiness probe from
    /// [`is_ready`](RuntimeReadiness::is_ready) so no traffic reaches an
    /// instance that would refuse it (INVARIANTS.md 10). Control plane: takes
    /// the registry and observation locks.
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

    /// Instance-wide counters across accounts, refill, snapshots, accounting,
    /// sharding and contention. Control plane: takes the observation and
    /// registry locks.
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
            sharding: self.shared.slots.sharding().occupancy(),
            contention: ContentionReport::collect(self.shared.slots.slots()),
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
    /// Another clone of the runtime's request and diagnostic handle.
    #[must_use]
    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }
    /// Validate `config`, then start the snapshot manager, the usage writer
    /// and the supervisor that runs one lease manager per eligible account.
    /// Returns the unique owner, which must be retained and shut down, and a
    /// first [`RuntimeHandle`].
    ///
    /// `source`, `allocator` and `sink` may be the same backend. `clock`
    /// supplies control-plane timestamps. Must be called within a Tokio
    /// runtime. Nothing starts when validation fails.
    ///
    /// # Errors
    ///
    /// [`InstanceRuntimeConfigError`] for any configuration rule
    /// [`InstanceRuntimeConfig::validate`] or a component's spawn refuses.
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
    /// Request shutdown (starting the total deadline if no one has yet) and
    /// wait for the supervisor to finish it: stop discovery, pause refills,
    /// drain the usage writer, stop the snapshot manager, then release every
    /// account's leases, all within
    /// [`InstanceRuntimeConfig::shutdown_deadline`]. Stop the application's
    /// listeners and quiesce its request tasks alongside, bounded by the same
    /// deadline. Cancelling this future aborts the supervisor.
    ///
    /// # Errors
    ///
    /// The supervisor task's [`JoinError`](tokio::task::JoinError) if it
    /// panicked or was cancelled; its report is then unavailable.
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
        let inherited_grants = u64::from(record.binding.slot.load_observed().is_some());
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
            let current = u64::from(
                self.accounts[&account]
                    .binding
                    .slot
                    .load_observed()
                    .is_some(),
            );
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

#[cfg(test)]
mod contention_tests {
    use super::*;

    /// Most contended first, ties by account, zero counts omitted, capped at
    /// `HOTTEST`, and the total counts every account — including the ones the
    /// cap drops.
    #[test]
    fn contention_report_ranks_by_count_then_account() {
        let counts =
            (0..20u128).map(|account| (AccountId(account), u64::try_from(account % 4).unwrap()));
        let report = ContentionReport::rank(counts);
        assert_eq!(
            report.contended_exchanges,
            (0..20u64).map(|a| a % 4).sum::<u64>()
        );
        assert_eq!(report.hottest.len(), ContentionReport::HOTTEST);
        assert_eq!(
            report.hottest,
            [
                (AccountId(3), 3),
                (AccountId(7), 3),
                (AccountId(11), 3),
                (AccountId(15), 3),
                (AccountId(19), 3),
                (AccountId(2), 2),
                (AccountId(6), 2),
                (AccountId(10), 2),
            ]
        );
        assert_eq!(
            ContentionReport::rank(std::iter::empty()),
            ContentionReport {
                contended_exchanges: 0,
                hottest: Vec::new()
            },
            "an uncontended instance names no account"
        );
        assert_eq!(
            ContentionReport::rank([(AccountId(1), u64::MAX), (AccountId(2), 5)].into_iter())
                .contended_exchanges,
            u64::MAX,
            "the total saturates"
        );
    }
}
