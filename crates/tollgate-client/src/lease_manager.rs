//! Background lease refill: the only code that talks to the allocator on an
//! instance's behalf.
//!
//! The manager polls its account's [`LeaseSlot`] and keeps it stocked:
//!
//! - empty or expired slot → acquire and install (recovery from cold start,
//!   INVARIANTS.md #10's readiness signal comes from observing the slot);
//! - live lease at or below low-water → acquire a *replacement* and install
//!   it. The superseded lease is **not** released immediately: in-flight
//!   reservations may still hold it. It is parked instead, and released back
//!   to the allocator once it has *quiesced* — when the manager holds the
//!   only remaining outer `Arc` and no independently reference-counted local
//!   view remains, no reservation exists and none can be created (the slot no
//!   longer points at it), so its remaining count is final and the release
//!   cannot race a debit or credit.
//!   Until quiescence the over-reservation is bounded by one grant per
//!   rotation, and TTL reclaim remains the backstop.
//! - allocator refusal with an expired/empty slot → the slot is cleared and
//!   stays cleared: requests deny (`LeaseUnavailable`), fail-closed, while
//!   the manager keeps retrying in the background (INVARIANTS.md #5).
//!
//! Graceful shutdown releases the current lease's remaining units. The
//! embedding service must stop admitting and flush its usage writer *before*
//! shutting the manager down — releasing first would make honest usage
//! events land on a settled lease and be rejected.
//!
//! Every allocator call is wall-clock bounded and the shutdown carries a
//! total budget (INVARIANTS.md #18), so a backend that hangs rather than
//! answering cannot park the refill loop or stall shutdown. Leases the budget
//! could not return are counted in [`LeaseManagerReport::abandoned`] and
//! settle at TTL reclaim.
//!
//! Those bounds are per *pass*, not per call, because the cost of a pass is
//! what the loop's two latency promises are made of. A steady-state release
//! pass carries one `store_call_timeout` across every parked lease and each
//! call within it is additionally capped by that budget, so neither refill
//! latency (#6) nor shutdown latency (#18) grows with the number of parked
//! leases. Both long awaits in the loop body — the release pass and
//! `acquire` — are raced against the shutdown watch, so the signal is acted
//! on where it arrives rather than at the next loop top (issue #78).
//!
//! What a release refusal *means* differs by variant (issue #42): a storage
//! error or timeout is retried next tick, a settled lease is dropped, a
//! fenced release also clears the slot because the store rejected a
//! capability copied from the grant and local lease identity can no longer be
//! trusted, and an invalid release — the store rejecting the claim as an
//! accounting bug — drops readiness, because local counts that disagree with
//! the store's are not a safe basis for further spending.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use atomic_waker::AtomicWaker;
use jiff::SignedDuration;
use tokio::sync::watch;
use tracing::Instrument as _;

use tollgate_admission::LeaseSlot;
use tollgate_core::{AccountId, CostUnits, LeaseGrant, LocalLease, RefillSignal, RefillVerdict};
use tollgate_store::{AllocateError, LeaseAllocator};

use tollgate_store::Clock;

#[derive(Debug, Clone, Copy)]
pub struct LeaseManagerConfig {
    pub account: AccountId,
    /// Units requested per acquire; the allocator's grant policy may shrink
    /// the actual grant near exhaustion.
    pub target_grant: CostUnits,
    /// Refill threshold installed into each `LocalLease`.
    pub low_water: CostUnits,
    pub lease_ttl: SignedDuration,
    /// Local safety margin: installed leases stop accepting debits and
    /// commits at `expires_at - margin`. Size it to cover worst-case
    /// allocator/holder clock skew plus the longest request the service
    /// executes; the allocator's reclaim grace covers the other side
    /// (review finding #1).
    pub expiry_safety_margin: SignedDuration,
    /// How often the slot is inspected. Refill latency is bounded by this
    /// plus one allocator round-trip — all off the request path.
    pub poll_interval: std::time::Duration,
    /// Wall-clock bound on one allocator call (acquire or release). An
    /// allocator that hangs rather than erroring would otherwise park the
    /// refill task forever. Must be positive.
    pub store_call_timeout: std::time::Duration,
    /// Total budget for returning leases at shutdown, across every parked
    /// lease. Leases still unreleased when it expires are reported and left
    /// to TTL reclaim (INVARIANTS.md #9). Must be positive.
    pub shutdown_release_deadline: std::time::Duration,
}

/// Instance-wide lease settings applied to each discovered account.
/// Field semantics and validation are those of [`LeaseManagerConfig`].
#[derive(Debug, Clone, Copy)]
pub struct AccountLeaseConfig {
    pub target_grant: CostUnits,
    pub low_water: CostUnits,
    pub lease_ttl: SignedDuration,
    pub expiry_safety_margin: SignedDuration,
    pub poll_interval: std::time::Duration,
    pub store_call_timeout: std::time::Duration,
    pub shutdown_release_deadline: std::time::Duration,
}

impl AccountLeaseConfig {
    #[must_use]
    pub fn for_account(self, account: AccountId) -> LeaseManagerConfig {
        LeaseManagerConfig {
            account,
            target_grant: self.target_grant,
            low_water: self.low_water,
            lease_ttl: self.lease_ttl,
            expiry_safety_margin: self.expiry_safety_margin,
            poll_interval: self.poll_interval,
            store_call_timeout: self.store_call_timeout,
            shutdown_release_deadline: self.shutdown_release_deadline,
        }
    }

    pub fn validate(&self) -> Result<(), LeaseManagerConfigError> {
        self.for_account(AccountId(0)).validate()
    }
}

/// What a graceful shutdown managed to return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseManagerReport {
    /// Leases the allocator accepted, or already considered settled.
    pub released: u64,
    /// Leases still held when the shutdown budget ran out. Their units come
    /// back at TTL reclaim, not at shutdown — reported, never silent.
    pub abandoned: u64,
    /// The task died (panic or abort) instead of reporting: the counts above
    /// are what is known, not what happened. Never `true` for a task that
    /// completed its own shutdown.
    pub task_died: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseManagerConfigError(pub &'static str);

impl std::fmt::Display for LeaseManagerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for LeaseManagerConfigError {}

impl LeaseManagerConfig {
    pub fn validate(&self) -> Result<(), LeaseManagerConfigError> {
        if self.target_grant.is_zero() {
            return Err(LeaseManagerConfigError("target_grant must be positive"));
        }
        if self.low_water >= self.target_grant {
            return Err(LeaseManagerConfigError(
                "low_water must be below target_grant",
            ));
        }
        if self.lease_ttl <= SignedDuration::ZERO {
            return Err(LeaseManagerConfigError("lease_ttl must be positive"));
        }
        if self.expiry_safety_margin < SignedDuration::ZERO {
            return Err(LeaseManagerConfigError(
                "expiry_safety_margin must not be negative",
            ));
        }
        if self.expiry_safety_margin >= self.lease_ttl {
            return Err(LeaseManagerConfigError(
                "expiry_safety_margin must be shorter than lease_ttl",
            ));
        }
        if self.poll_interval.is_zero() {
            return Err(LeaseManagerConfigError("poll_interval must be positive"));
        }
        if self.store_call_timeout.is_zero() {
            return Err(LeaseManagerConfigError(
                "store_call_timeout must be positive",
            ));
        }
        if self.shutdown_release_deadline.is_zero() {
            return Err(LeaseManagerConfigError(
                "shutdown_release_deadline must be positive",
            ));
        }
        Ok(())
    }
}

/// The refill task's doorbell: rung by the debit that drains a lease past low
/// water, answered by the loop below.
///
/// Lock-free by construction, because [`RefillSignal::request_refill`] runs on
/// the request path (INVARIANTS.md #5, #6). `AtomicWaker` is the primitive
/// built for exactly this handoff; tokio's `Notify` was rejected because
/// `notify_one` takes an internal mutex whenever a waiter is parked, which is
/// the normal state of this task.
///
/// The claim is bounded and worth stating precisely: *this code* takes no
/// lock. `Waker::wake` then enters the runtime's scheduler, whose internals
/// are not ours to characterise. What is ours is that the handoff is a pair of
/// atomics and that it happens at most once per lease rotation.
#[derive(Debug, Default)]
struct RefillRequests {
    waker: AtomicWaker,
    pending: AtomicBool,
}

impl RefillSignal for RefillRequests {
    fn request_refill(&self) {
        // Release/Acquire pairs with `requested`: the flag must be visible
        // before the wake, or the woken task could observe neither.
        self.pending.store(true, Ordering::Release);
        self.waker.wake();
    }
}

impl RefillRequests {
    /// Wait for a lease to ask for replacement.
    ///
    /// Cancel-safe, which matters because this is one arm of a `select!`: a
    /// request is consumed only on the path that returns `Ready`, so losing
    /// the race to another arm cannot swallow it.
    async fn requested(&self) {
        std::future::poll_fn(|cx| {
            if self.pending.swap(false, Ordering::Acquire) {
                return std::task::Poll::Ready(());
            }
            self.waker.register(cx.waker());
            // Re-check after registering. A request landing between the first
            // check and the registration would otherwise be waited on
            // forever — the poll interval would eventually cover it, but the
            // whole point of #10 is not to wait for that.
            if self.pending.swap(false, Ordering::Acquire) {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
    }
}

/// What the refill task has done, readable at any time.
///
/// [`LeaseManagerReport`] says what happened *at shutdown*, which leaves the
/// running instance silent: whether refill is keeping up, and why the
/// allocator is refusing when it does, were visible only as `tracing` events
/// with nothing to threshold on (#4). These counters are the scrapeable half.
///
/// Written only by the refill task, so unlike the usage writer's counters they
/// need no cache-line padding: one writer cannot contend with itself.
/// Statistics use `Relaxed`; control-state evidence uses acquire/release.
#[derive(Debug)]
pub struct LeaseCounters {
    acquired: AtomicU64,
    acquired_units: AtomicU64,
    acquire_timeouts: AtomicU64,
    uncertain_acquires: AtomicU64,
    acquire_pending: AtomicBool,
    integrity_fault: AtomicBool,
    acquire_refused: [AtomicU64; AllocateError::COUNT],
    released: AtomicU64,
    abandoned: AtomicU64,
    consolidated: AtomicU64,
    consolidations_deferred: AtomicU64,
}

impl LeaseCounters {
    #[must_use]
    pub const fn new() -> Self {
        LeaseCounters {
            acquired: AtomicU64::new(0),
            acquired_units: AtomicU64::new(0),
            acquire_timeouts: AtomicU64::new(0),
            uncertain_acquires: AtomicU64::new(0),
            acquire_pending: AtomicBool::new(false),
            integrity_fault: AtomicBool::new(false),
            acquire_refused: [const { AtomicU64::new(0) }; AllocateError::COUNT],
            released: AtomicU64::new(0),
            abandoned: AtomicU64::new(0),
            consolidated: AtomicU64::new(0),
            consolidations_deferred: AtomicU64::new(0),
        }
    }

    pub(crate) fn acquire_pending(&self) -> bool {
        self.acquire_pending.load(Ordering::Acquire)
    }

    pub(crate) fn integrity_fault(&self) -> bool {
        self.integrity_fault.load(Ordering::Acquire)
    }

    fn record_integrity_fault(&self, health: &watch::Sender<bool>) {
        self.integrity_fault.store(true, Ordering::Release);
        crate::signal(health, false, "lease-manager health");
    }

    fn record_acquired(&self, units: CostUnits) {
        self.acquired.fetch_add(1, Ordering::Relaxed);
        self.acquired_units
            .fetch_add(units.get(), Ordering::Relaxed);
    }

    /// An allocator call that exceeded `store_call_timeout`. Counted apart
    /// from a refusal because it is not a domain answer: the allocator may
    /// well have granted the lease and simply failed to say so in time.
    fn record_acquire_timeout(&self) {
        self.acquire_timeouts.fetch_add(1, Ordering::Relaxed);
        self.uncertain_acquires.fetch_add(1, Ordering::Relaxed);
    }

    /// Record both sides of a completed consolidation together: one grant
    /// acquired and its predecessor settled. Omitting either side corrupts
    /// the runtime's terminal grant inventory.
    fn record_consolidated(&self, units: CostUnits) {
        self.record_acquired(units);
        self.record_released();
        self.consolidated.fetch_add(1, Ordering::Relaxed);
    }

    fn record_consolidation_deferred(&self) {
        self.consolidations_deferred.fetch_add(1, Ordering::Relaxed);
    }

    fn record_acquire_refused(&self, error: &AllocateError) {
        self.acquire_refused[error.index()].fetch_add(1, Ordering::Relaxed);
        if matches!(error, AllocateError::Storage(_)) {
            self.uncertain_acquires.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A lease the allocator is no longer holding open for this instance —
    /// released cleanly, or refused in a way that means it is already settled.
    fn record_released(&self) {
        self.released.fetch_add(1, Ordering::Relaxed);
    }

    fn record_abandoned(&self) {
        self.abandoned.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> LeaseStats {
        LeaseStats {
            acquired: self.acquired.load(Ordering::Relaxed),
            acquired_units: self.acquired_units.load(Ordering::Relaxed),
            acquire_timeouts: self.acquire_timeouts.load(Ordering::Relaxed),
            uncertain_acquires: self.uncertain_acquires.load(Ordering::Relaxed),
            acquire_refused: std::array::from_fn(|slot| {
                self.acquire_refused[slot].load(Ordering::Relaxed)
            }),
            released: self.released.load(Ordering::Relaxed),
            abandoned: self.abandoned.load(Ordering::Relaxed),
            consolidated: self.consolidated.load(Ordering::Relaxed),
            consolidations_deferred: self.consolidations_deferred.load(Ordering::Relaxed),
        }
    }
}

impl Default for LeaseCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// A reading of [`LeaseCounters`], safe to serialise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseStats {
    /// Acquires that returned a grant.
    pub acquired: u64,
    /// Units granted across those acquires. Adaptive allocation can return
    /// less than `target_grant`, so this is not `acquired × target_grant`.
    pub acquired_units: u64,
    /// Acquires the allocator did not answer within `store_call_timeout`.
    pub acquire_timeouts: u64,
    /// Acquires or consolidations that timed out or returned `Storage`.
    /// Each may have committed an unanswered grant; the count survives a
    /// clean shutdown as well as task death. An interrupted in-flight call
    /// is tracked separately until the runtime joins its manager.
    pub uncertain_acquires: u64,
    /// Acquire refusals per [`AllocateError::index`] slot.
    pub acquire_refused: [u64; AllocateError::COUNT],
    /// Leases the allocator no longer holds open for this instance.
    pub released: u64,
    /// Leases the shutdown budget could not return; their units come back at
    /// TTL reclaim (INVARIANTS.md #9). Like the writer's `lost`, this can only
    /// move at shutdown — it is a confirmation, not an early warning.
    pub abandoned: u64,
    /// Rotations that folded a refused lease's unspent units into their
    /// replacement, counted apart from `acquired` because each one records
    /// that this instance *did* refuse work the account could fund. A rising
    /// rate is the signal that `target_grant` is undersized against the
    /// largest quote the service prices (#109).
    pub consolidated: u64,
    /// Consolidations that could not run because the refused lease still had
    /// a reservation in flight, so the slot kept serving it. Retried on the
    /// next refusal or tick; a rising count against a flat `consolidated`
    /// means requests never leave the lease idle long enough.
    pub consolidations_deferred: u64,
}

impl LeaseStats {
    pub const ZERO: Self = Self {
        acquired: 0,
        acquired_units: 0,
        acquire_timeouts: 0,
        uncertain_acquires: 0,
        acquire_refused: [0; AllocateError::COUNT],
        released: 0,
        abandoned: 0,
        consolidated: 0,
        consolidations_deferred: 0,
    };

    /// Aggregate observations without wrapping an operator's counter.
    pub fn checked_add(self, other: Self) -> Option<Self> {
        let mut acquire_refused = [0; AllocateError::COUNT];
        for (i, value) in acquire_refused.iter_mut().enumerate() {
            *value = self.acquire_refused[i].checked_add(other.acquire_refused[i])?;
        }
        // The public refused() total must also remain representable.
        acquire_refused
            .iter()
            .try_fold(0_u64, |sum, value| sum.checked_add(*value))?;
        Some(Self {
            acquired: self.acquired.checked_add(other.acquired)?,
            acquired_units: self.acquired_units.checked_add(other.acquired_units)?,
            acquire_timeouts: self.acquire_timeouts.checked_add(other.acquire_timeouts)?,
            uncertain_acquires: self
                .uncertain_acquires
                .checked_add(other.uncertain_acquires)?,
            acquire_refused,
            released: self.released.checked_add(other.released)?,
            abandoned: self.abandoned.checked_add(other.abandoned)?,
            consolidated: self.consolidated.checked_add(other.consolidated)?,
            consolidations_deferred: self
                .consolidations_deferred
                .checked_add(other.consolidations_deferred)?,
        })
    }
    /// Refusals paired with their stable labels, in slot order.
    pub fn refusals_by_name(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        AllocateError::NAMES
            .iter()
            .copied()
            .zip(self.acquire_refused.iter().copied())
    }

    /// Every acquire refusal, whatever the reason.
    #[must_use]
    pub fn refused(&self) -> u64 {
        self.acquire_refused.iter().sum()
    }
}

/// Handle to the refill task.
pub struct LeaseManager {
    shutdown: watch::Sender<bool>,
    health: watch::Receiver<bool>,
    handle: Option<tokio::task::JoinHandle<LeaseManagerReport>>,
    counters: Arc<LeaseCounters>,
    deadline: Arc<crate::ShutdownDeadline>,
    paused: Arc<AtomicBool>,
}

impl LeaseManager {
    pub fn spawn(
        allocator: Arc<dyn LeaseAllocator>,
        slot: Arc<LeaseSlot>,
        clock: Arc<dyn Clock>,
        config: LeaseManagerConfig,
    ) -> Result<Self, LeaseManagerConfigError> {
        config.validate()?;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (health_tx, health) = crate::task_health::TaskHealth::channel(true);
        let account = config.account;
        let counters = Arc::new(LeaseCounters::new());
        let task_counters = Arc::clone(&counters);
        let deadline = Arc::new(crate::ShutdownDeadline::default());
        let task_deadline = Arc::clone(&deadline);
        let paused = Arc::new(AtomicBool::new(false));
        let task_paused = Arc::clone(&paused);
        let handle = tokio::spawn(
            async move {
                run(
                    allocator,
                    slot,
                    clock,
                    config,
                    shutdown_rx,
                    health_tx.sender(),
                    &task_counters,
                    &task_deadline,
                    &task_paused,
                )
                .await
            }
            .instrument(tracing::info_span!("lease_manager", %account)),
        );
        Ok(LeaseManager {
            shutdown,
            health,
            handle: Some(handle),
            counters,
            deadline,
            paused,
        })
    }

    /// The refill task's running counters.
    ///
    /// Returns the shared handle rather than a snapshot: a service keeps this
    /// in its request state while the [`LeaseManager`] itself is usually moved
    /// into whatever owns shutdown, and the counters outlive the task, so a
    /// reader keeps working after the refill task dies.
    #[must_use]
    pub fn counters(&self) -> Arc<LeaseCounters> {
        Arc::clone(&self.counters)
    }

    /// True while the refill task is alive *and* its accounting still agrees
    /// with the store. The task owns a publisher that stores false before
    /// closing on normal return, panic or cancellation. Channel closure also
    /// remains available to observers that distinguish exit from a live fault.
    #[must_use]
    pub fn health(&self) -> watch::Receiver<bool> {
        self.health.clone()
    }

    /// Stop initiating refills while the runtime drains accounting. A call
    /// already in flight remains owned and is bounded by store_call_timeout.
    pub(crate) fn pause_refills(&self) {
        self.paused.store(true, Ordering::Release);
    }

    pub(crate) fn stop_at(&self, deadline: tokio::time::Instant) {
        self.deadline.constrain(deadline);
        crate::signal(&self.shutdown, true, "lease-manager shutdown");
    }

    /// Signal the task, wait for it to return what it can of its leases, and
    /// report what it managed. Bounded by `shutdown_release_deadline`: a hung
    /// allocator cannot stall this call.
    pub async fn shutdown(mut self) -> LeaseManagerReport {
        crate::signal(&self.shutdown, true, "lease-manager shutdown");
        let died = LeaseManagerReport {
            released: 0,
            abandoned: 0,
            task_died: true,
        };
        // Borrow the handle so cancellation still runs Drop's abort.
        match self.handle.as_mut() {
            // A task that died reports nothing it can substantiate; the flag
            // says so rather than a zero that reads like a clean run (#41).
            Some(handle) => handle.await.unwrap_or(died),
            None => died,
        }
    }
}

impl Drop for LeaseManager {
    fn drop(&mut self) {
        // Dropped without shutdown(): abort rather than leave a detached
        // task spinning against a dead watch channel. Held leases settle by
        // TTL reclaim (INVARIANTS.md #9) — graceful code calls shutdown().
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    allocator: Arc<dyn LeaseAllocator>,
    slot: Arc<LeaseSlot>,
    clock: Arc<dyn Clock>,
    config: LeaseManagerConfig,
    mut shutdown: watch::Receiver<bool>,
    health: &watch::Sender<bool>,
    counters: &LeaseCounters,
    shutdown_deadline: &crate::ShutdownDeadline,
    paused: &AtomicBool,
) -> LeaseManagerReport {
    let mut tick = tokio::time::interval(config.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Rung by the debit that crosses low water. The interval remains the
    // backstop for the two cases no debit can announce: a cold start, where
    // there is no lease to spend, and a usability-window rollover, where the
    // lease lapses rather than drains.
    let refill = Arc::new(RefillRequests::default());
    // Superseded leases waiting for quiescence before their unspent units go
    // back to the allocator.
    let mut parked: Vec<Arc<LocalLease>> = Vec::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            // Falls through to the level check below like every other arm —
            // deliberately doing nothing here. An arm that returned or
            // continued early would reintroduce review finding #2, where a
            // shutdown observed concurrently is consumed and never acted on.
            () = refill.requested() => {}
            changed = shutdown.changed() => {
                // Err = handle dropped without shutdown(); stop rather than
                // spin against a dead channel.
                if changed.is_err() {
                    break;
                }
            }
        }
        if *shutdown.borrow() {
            break;
        }

        if paused.load(Ordering::Acquire) {
            continue;
        }

        let now = clock.now();
        // One budget for the pass, so a wedged backend costs this tick one
        // store call's worth of wall clock whatever `parked.len()` is, and
        // the refill below is not queued behind it (#6, #78).
        let pass_deadline = tokio::time::Instant::now() + config.store_call_timeout;
        if release_quiesced(
            &allocator,
            &mut parked,
            &clock,
            &config,
            &slot,
            health,
            counters,
            &mut shutdown,
            pass_deadline,
        )
        .await
            == ReleasePass::ShutdownObserved
        {
            break;
        }
        let rotation = match slot.load() {
            None => Rotation::Acquire,
            Some(lease) if now >= lease.usable_until() => {
                // This inspection is not an in-flight request. Keeping its
                // Arc through the release pass would make our own grant
                // appear busy and defer its refund until after acquisition.
                drop(lease);
                // Close the slot, but retain the grant: while it is still in
                // the allocator's grace window its unspent capacity can be
                // released and immediately reused. Reservations that loaded
                // it before `take` keep an Arc and delay release safely.
                if let Some(old) = slot.take() {
                    parked.push(old);
                }
                // The rollover's own pass shares this tick's budget, so
                // closing the slot cannot buy a second full pass.
                if release_quiesced(
                    &allocator,
                    &mut parked,
                    &clock,
                    &config,
                    &slot,
                    health,
                    counters,
                    &mut shutdown,
                    pass_deadline,
                )
                .await
                    == ReleasePass::ShutdownObserved
                {
                    break;
                }
                Rotation::Acquire
            }
            Some(lease) => match lease.refill_due_or_rearm() {
                RefillVerdict::Idle => Rotation::Idle,
                RefillVerdict::Draining => Rotation::Acquire,
                // The lease refused work this account may well be able to
                // fund, so the units it still holds are the ones the next
                // grant needs. Acquiring beside it would ask for a grant
                // sized against a balance those units are missing from, and
                // install the smaller answer (#109).
                RefillVerdict::Refused => Rotation::Consolidate,
            },
        };
        if rotation == Rotation::Idle {
            continue;
        }

        // A hung allocator must not park the refill loop: a timeout takes the
        // same path as a refusal, so the slot fails closed and the next tick
        // tries again. The signal is raced against the call for the same
        // reason it is raced inside the release pass — an acquire abandoned
        // here settles nothing, and the slot it would have filled is one the
        // shutdown phase is about to drain anyway (#78).
        if paused.load(Ordering::Acquire) || *shutdown.borrow() {
            continue;
        }

        if rotation == Rotation::Consolidate {
            match consolidate_live_lease(
                &allocator,
                &mut parked,
                &clock,
                &config,
                &slot,
                &refill,
                health,
                counters,
                &mut shutdown,
            )
            .await
            {
                Consolidation::ShutdownObserved => break,
                Consolidation::Installed | Consolidation::KeptServing => continue,
                // The grant this would have folded in is gone, so the fold has
                // nothing left to protect and the ordinary path below is both
                // correct and what an empty slot needs.
                Consolidation::AcquireInstead => {}
            }
        }
        counters.acquire_pending.store(true, Ordering::Release);
        let acquire = tokio::time::timeout(
            config.store_call_timeout,
            allocator.acquire(config.account, config.target_grant, config.lease_ttl, now),
        );
        let acquired = tokio::select! {
            outcome = acquire => outcome,
            _ = shutdown.changed() => {
                tracing::debug!("shutdown observed during an acquire; abandoning the refill");
                break;
            }
        };
        counters.acquire_pending.store(false, Ordering::Release);
        match acquired {
            Ok(Ok(grant)) => {
                counters.record_acquired(grant.units);
                // Rotation: install the fresh lease and park the superseded
                // one until it quiesces (module docs).
                if let Some(old) = slot.replace(install_lease(grant, &config, &slot, &refill)) {
                    parked.push(old);
                }
            }
            // Denied, backend down, or too slow: nothing to install. The slot
            // keeps whatever live lease it still has (spend continues until
            // exhaustion/expiry); an empty slot stays empty — deny.
            //
            // Level follows consequence, not cause: refusal during ordinary
            // rotation is routine and stays `debug`, but the same refusal
            // against an empty slot means this instance is denying every
            // request, which is the condition an operator must see.
            outcome => {
                let serving = slot.load().is_some();
                // A timeout is not a domain answer — the allocator may have
                // granted and merely failed to say so — so it is counted
                // apart from the refusals rather than folded into one of them.
                let reason: &dyn std::fmt::Display = match &outcome {
                    Ok(Err(error)) => {
                        counters.record_acquire_refused(error);
                        error
                    }
                    _ => {
                        counters.record_acquire_timeout();
                        &"allocator timed out"
                    }
                };
                if serving {
                    tracing::debug!(%reason, "lease acquire refused; still serving");
                } else {
                    tracing::warn!(
                        %reason,
                        "lease acquire refused with an empty slot; requests are denied"
                    );
                }
            }
        }
    }

    // Graceful shutdown: return what's left of the current and parked leases.
    //
    // The comment that used to stand here said every lease had quiesced,
    // because callers flush usage and stop admitting first. That is the
    // documented lifecycle (INVARIANTS.md #13), but `parked` at this moment
    // holds — by construction — exactly the leases the last pass determined
    // were *not* quiesced, and reading `remaining()` on one of those reads a
    // number that is not final. A request task still holding a view can debit
    // after the release credits the account, and total committed usage then
    // exceeds the allocation with a usage event to prove it (#1, #62).
    //
    // Caller discipline is the enforcement tier the standards call drift-prone,
    // and the predicate that makes it unnecessary already exists in
    // `release_quiesced`. So it is applied here too: a lease still holding an
    // outside view is waited for inside the shutdown budget, and abandoned
    // rather than released if it never quiesces. Abandoning returns the units
    // at TTL reclaim (#9); releasing units that may still be spent is the one
    // outcome that cannot be undone.
    if let Some(lease) = slot.take() {
        parked.push(lease);
    }
    let deadline = shutdown_deadline.within(config.shutdown_release_deadline);
    let mut report = LeaseManagerReport {
        released: 0,
        abandoned: 0,
        task_died: false,
    };
    for lease in parked {
        let grant = lease.grant();
        // Wait for the view to drop, inside the same budget the releases
        // share. A quiesced lease passes this immediately; one still held
        // costs a few polls and then, if the budget runs out first, is
        // abandoned with its units named — which is what the old path did to
        // an unfinished *call*, applied to an unfinished *reservation*.
        if !wait_for_quiescence(&lease, deadline).await {
            report.abandoned += 1;
            counters.record_abandoned();
            tracing::warn!(
                lease = %grant.lease_id,
                units = lease.remaining().get(),
                "lease still held by an in-flight request at the shutdown \
                 deadline; abandoned rather than released, because releasing \
                 units that may still be spent cannot be undone"
            );
            continue;
        }
        // One budget across every lease, and no single call may outlast the
        // per-call timeout inside it. Whatever the budget cannot cover is
        // reported abandoned and settles at TTL reclaim (INVARIANTS.md #9).
        let call_deadline = deadline.min(tokio::time::Instant::now() + config.store_call_timeout);
        match tokio::time::timeout_at(
            call_deadline,
            allocator.release(
                grant.lease_id,
                grant.fencing_token,
                lease.remaining(),
                clock.now(),
            ),
        )
        .await
        {
            Ok(Ok(())) => {
                report.released += 1;
                counters.record_released();
            }
            Ok(Err(error)) => match release_failure(&error) {
                ReleaseFailure::Settled | ReleaseFailure::Fenced => {
                    report.released += 1;
                    counters.record_released();
                    tracing::warn!(lease = %grant.lease_id, %error,
                        "lease no longer belongs to this manager; considered settled");
                }
                ReleaseFailure::Retryable => {
                    report.abandoned += 1;
                    counters.record_abandoned();
                    tracing::warn!(lease = %grant.lease_id, %error,
                        "release unconfirmed at shutdown; grant requires TTL reclaim");
                }
                ReleaseFailure::Integrity => {
                    report.abandoned += 1;
                    counters.record_abandoned();
                    counters.record_integrity_fault(health);
                    tracing::error!(lease = %grant.lease_id, %error,
                        "release violated the allocator contract; grant abandoned and integrity fault retained");
                }
            },
            Err(_) => {
                report.abandoned += 1;
                counters.record_abandoned();
                tracing::warn!(
                    lease = %grant.lease_id,
                    units = lease.remaining().get(),
                    "shutdown budget expired with a lease unreturned; \
                     its units settle at TTL reclaim"
                );
            }
        }
    }
    report
}

/// The same refusal has the same ownership meaning during refill and
/// shutdown. Only the decision to retry a storage failure depends on phase.
#[derive(Clone, Copy)]
enum ReleaseFailure {
    Settled,
    Fenced,
    Retryable,
    Integrity,
}
fn release_failure(error: &AllocateError) -> ReleaseFailure {
    match error {
        AllocateError::UnknownLease | AllocateError::LeaseNotActive => ReleaseFailure::Settled,
        AllocateError::Fenced => ReleaseFailure::Fenced,
        AllocateError::Storage(_) => ReleaseFailure::Retryable,
        AllocateError::InvalidRelease
        | AllocateError::UnknownAccount
        | AllocateError::AccountInactive
        | AllocateError::InsufficientBalance
        | AllocateError::InvalidTtl => ReleaseFailure::Integrity,
    }
}

/// Wrap a fresh grant for local spending, in this slot's layout and wired to
/// this task's doorbell.
///
/// Adaptive allocation may return less than `target_grant`; low water is
/// capped below the actual grant so a fresh tail grant does not rotate again
/// without serving any work. That cap is also why a refusal has to be its own
/// signal: it puts a shrunken grant *above* its own mark, where no crossing
/// can ever occur (#109).
///
/// Each fresh lease gets its own unrung flags. A single-counter lease
/// therefore rings once for the whole rotation; a sharded one rings at most
/// once per shard between aggregate checks, which is what
/// `LocalLease::refill_due_or_rearm` clears whenever the aggregate shows an
/// early crossing was premature (INVARIANTS.md #6).
fn install_lease(
    grant: LeaseGrant,
    config: &LeaseManagerConfig,
    slot: &Arc<LeaseSlot>,
    refill: &Arc<RefillRequests>,
) -> Arc<LocalLease> {
    let low_water = CostUnits(
        config
            .low_water
            .get()
            .min(grant.units.get().saturating_sub(1)),
    );
    Arc::new(
        LocalLease::with_sharding(
            grant,
            low_water,
            config.expiry_safety_margin,
            slot.sharding(),
        )
        .with_refill(Arc::clone(refill) as Arc<dyn RefillSignal>),
    )
}

/// What the refill loop should do with the slot this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rotation {
    /// Nothing asked for anything.
    Idle,
    /// Acquire the next lease. The slot keeps serving whatever it has while
    /// the call is in flight, which is what makes an anticipatory low-water
    /// rotation invisible to requests.
    Acquire,
    /// Fold the live lease's unspent units into its replacement, because it
    /// refused work rather than merely approached its mark (#109).
    Consolidate,
}

/// What a failed consolidation left behind, which decides whether the lease
/// may go back into the slot.
///
/// The distinction that matters is not "did it work" but "does this instance
/// still own spendable units". Reinstating a lease the store already settled
/// would spend units it has credited back to the account — a double spend, and
/// the one outcome worse than the refusal being fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConsolidationFailure {
    /// A domain answer from inside the transaction, so it rolled back and the
    /// lease is still active and still ours.
    RolledBack,
    /// The lease is settled, or was never ours to settle. Nothing to put back.
    Settled,
    /// The store never answered. The transaction may have committed, so the
    /// lease must not serve again; it may equally still be active, so it must
    /// not be dropped. Park it and let the release pass find out which.
    Ambiguous,
    /// The unspent claim did not fit the lease the store holds. The
    /// transaction rolled back, so the lease is still ours, but this instance
    /// and the ledger disagree about a grant — the same accounting fault a
    /// bad release reports.
    Integrity,
}

fn consolidation_failure(error: &AllocateError) -> ConsolidationFailure {
    match error {
        AllocateError::UnknownLease | AllocateError::LeaseNotActive | AllocateError::Fenced => {
            ConsolidationFailure::Settled
        }
        // Legitimate near a period's end and the reason consolidation was
        // attempted at all: the account has nothing left to add. Not the
        // integrity fault the same code means for a *release*, which never
        // draws a grant (#109).
        AllocateError::InsufficientBalance
        | AllocateError::UnknownAccount
        | AllocateError::AccountInactive
        | AllocateError::InvalidTtl => ConsolidationFailure::RolledBack,
        AllocateError::InvalidRelease => ConsolidationFailure::Integrity,
        AllocateError::Storage(_) => ConsolidationFailure::Ambiguous,
    }
}

/// Fold the live lease's unspent units into its replacement, atomically.
///
/// The slot is emptied first, because `unspent` has to be exact and a lease
/// still reachable from the slot can still be debited. That is a deny window,
/// so it is kept to the take-and-check itself: quiescence is *tested*, never
/// waited for. A lease with a reservation in flight goes straight back and the
/// next refused debit rings again — and in the state this exists to fix there
/// are no successful requests holding the lease, so the test passes on the
/// first attempt exactly when it matters.
#[allow(clippy::too_many_arguments)]
async fn consolidate_live_lease(
    allocator: &Arc<dyn LeaseAllocator>,
    parked: &mut Vec<Arc<LocalLease>>,
    clock: &Arc<dyn Clock>,
    config: &LeaseManagerConfig,
    slot: &Arc<LeaseSlot>,
    refill: &Arc<RefillRequests>,
    health: &watch::Sender<bool>,
    counters: &LeaseCounters,
    shutdown: &mut watch::Receiver<bool>,
) -> Consolidation {
    let Some(live) = slot.take() else {
        // Another path emptied the slot between the verdict and here; an
        // ordinary acquire is what an empty slot needs.
        return Consolidation::AcquireInstead;
    };
    if Arc::strong_count(&live) > 1 || !live.is_only_local_view() {
        counters.record_consolidation_deferred();
        slot.install(live);
        return Consolidation::KeptServing;
    }
    // Exact: the predicate above establishes that no request can debit or
    // refund, which is the condition `LocalLease::remaining` documents.
    let unspent = live.remaining();
    let grant = *live.grant();
    counters.acquire_pending.store(true, Ordering::Release);
    let call = tokio::time::timeout(
        config.store_call_timeout,
        allocator.consolidate(
            grant.lease_id,
            grant.fencing_token,
            unspent,
            config.target_grant,
            config.lease_ttl,
            clock.now(),
        ),
    );
    let outcome = tokio::select! {
        outcome = call => outcome,
        _ = shutdown.changed() => {
            tracing::debug!(
                lease = %grant.lease_id,
                "shutdown observed during a consolidation; parking the grant"
            );
            // Abandoned mid-call, so the same ambiguity as a timeout: the
            // shutdown phase releases it and learns whether it was settled.
            // Keep acquire_pending: the replacement capability may already
            // exist, and releasing the predecessor cannot recover that grant.
            parked.push(live);
            return Consolidation::ShutdownObserved;
        }
    };
    counters.acquire_pending.store(false, Ordering::Release);

    let error: AllocateError = match outcome {
        Ok(Ok(fresh)) => {
            counters.record_consolidated(fresh.units);
            tracing::debug!(
                superseded = %grant.lease_id,
                lease = %fresh.lease_id,
                folded = unspent.get(),
                units = fresh.units.get(),
                "consolidated a refused lease into a larger grant"
            );
            // The store settled `live` inside the same transaction that issued
            // this grant, so it must not be parked: releasing it again would
            // claim units the ledger has already credited back.
            drop(live);
            slot.install(install_lease(fresh, config, slot, refill));
            return Consolidation::Installed;
        }
        Ok(Err(error)) => error,
        Err(_) => {
            counters.record_acquire_timeout();
            tracing::warn!(
                lease = %grant.lease_id,
                "consolidation timed out; parking the grant because the store may have settled it"
            );
            parked.push(live);
            return Consolidation::KeptServing;
        }
    };
    counters.record_acquire_refused(&error);
    match consolidation_failure(&error) {
        ConsolidationFailure::RolledBack => {
            tracing::debug!(
                lease = %grant.lease_id, %error,
                "consolidation refused; the grant is untouched and keeps serving"
            );
            slot.install(live);
            Consolidation::KeptServing
        }
        ConsolidationFailure::Integrity => {
            counters.record_integrity_fault(health);
            tracing::error!(
                lease = %grant.lease_id, units = unspent.get(), %error,
                "consolidation violated the allocator contract; readiness withdrawn"
            );
            slot.install(live);
            Consolidation::KeptServing
        }
        ConsolidationFailure::Settled => {
            counters.record_released();
            tracing::warn!(
                lease = %grant.lease_id, %error,
                "the store no longer holds this grant open; acquiring a fresh one"
            );
            drop(live);
            Consolidation::AcquireInstead
        }
        ConsolidationFailure::Ambiguous => {
            tracing::warn!(
                lease = %grant.lease_id, %error,
                "consolidation failed without saying whether it committed; parking the grant"
            );
            parked.push(live);
            Consolidation::KeptServing
        }
    }
}

/// The outcome of one consolidation attempt, in the terms the loop acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Consolidation {
    /// A larger grant is in the slot.
    Installed,
    /// The slot holds a lease that may still serve, or the grant is parked for
    /// the release pass. Either way this tick is done.
    KeptServing,
    /// There is nothing to fold in; take the ordinary acquire path.
    AcquireInstead,
    ShutdownObserved,
}

/// Whether `lease` has quiesced, waiting until `deadline` for it to.
///
/// The same predicate `release_quiesced` uses: the local binding must be the
/// sole outer handle, and a sharded slot must have no independently
/// reference-counted locality alias left. Together those mean no request can
/// still reserve or return units, so `remaining()` is final.
///
/// Polled rather than notified because a lease view is dropped by whatever
/// task holds it, with nothing to signal on; the interval is short against a
/// shutdown budget measured in seconds, and a quiesced lease returns on the
/// first check without sleeping at all.
async fn wait_for_quiescence(lease: &Arc<LocalLease>, deadline: tokio::time::Instant) -> bool {
    const POLL: std::time::Duration = std::time::Duration::from_millis(5);
    loop {
        if Arc::strong_count(lease) == 1 && lease.is_only_local_view() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(POLL.min(deadline - tokio::time::Instant::now())).await;
    }
}

/// What ended a release pass. A pass that did not run to completion leaves
/// every lease it did not settle parked for the next one, so no outcome here
/// can strand units: the caller either loops again or enters the shutdown
/// release phase, and both see the full parked list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReleasePass {
    /// Every parked lease was examined.
    Complete,
    /// The pass budget ran out first. Unexamined leases keep their place at
    /// the front of the queue, so the next pass starts with them.
    BudgetExpired,
    /// The shutdown signal arrived mid-pass. The caller must stop looping and
    /// enter the shutdown release phase, which has its own budget.
    ShutdownObserved,
}

/// Release every parked lease that has quiesced; keep the rest parked. Each
/// parked lease is examined at most once per pass, and the pass as a whole is
/// wall-clock bounded — not merely each call within it.
///
/// One budget across the pass is what keeps this off both latency paths. With
/// a per-call bound only, a pass against a wedged backend cost
/// `parked.len() * store_call_timeout`, which the refill that rang the
/// low-water doorbell waited behind (#6) and which a shutdown signal could
/// not interrupt until the next loop top (#18) — issue #78, where six parked
/// leases turned a stated 10s shutdown budget into ~75s.
///
/// A lease that consumes budget without settling yields its place: it goes
/// behind the leases this pass never reached, so a single permanently hung
/// lease cannot starve the rest of the queue pass after pass.
///
/// The allocator's answer decides what the refusal *means* (issue #42):
/// storage errors and timeouts keep the lease parked for the next tick; a
/// settled lease is dropped; a fenced release also closes the slot, because
/// rejection of the grant's own lease-scoped capability means local identity
/// has diverged from the store and spending must stop; and an invalid release
/// means local accounting disagrees with the store's, which is not a state in
/// which continuing to spend is safe — readiness drops and stays down.
// The loop's context, passed through rather than captured: same shape and
// same reason as `snapshot_manager::refresh_all_cancellable`, which carries
// the same allow. Bundling these into a struct for one of the two and not the
// other would trade a lint for an asymmetry.
#[allow(clippy::too_many_arguments)]
async fn release_quiesced(
    allocator: &Arc<dyn LeaseAllocator>,
    parked: &mut Vec<Arc<LocalLease>>,
    clock: &Arc<dyn Clock>,
    config: &LeaseManagerConfig,
    slot: &Arc<LeaseSlot>,
    health: &watch::Sender<bool>,
    counters: &LeaseCounters,
    shutdown: &mut watch::Receiver<bool>,
    deadline: tokio::time::Instant,
) -> ReleasePass {
    // Ownership discipline for the whole pass: `queue` holds what has not
    // been examined and `retry` what was examined without settling. Every
    // early return writes both back, so a pass that stops early — for budget
    // or for shutdown — cannot drop a lease on the floor. Taking the list and
    // rebuilding it only at the end would strand every taken lease if this
    // future were ever cancelled instead.
    let mut queue = std::mem::take(parked).into_iter();
    let mut retry = Vec::new();
    while let Some(lease) = queue.next() {
        if tokio::time::Instant::now() >= deadline {
            // Unexamined leases first: they have not had a turn this pass.
            *parked = std::iter::once(lease).chain(queue).chain(retry).collect();
            return ReleasePass::BudgetExpired;
        }
        // The local binding must be the sole outer handle, and sharded slots
        // must have no independently reference-counted locality alias left.
        // Together those conditions mean no request can still reserve or
        // return units before the aggregate is released.
        if Arc::strong_count(&lease) > 1 || !lease.is_only_local_view() {
            retry.push(lease);
            continue;
        }
        let grant = lease.grant();
        // No single call may outlast the per-call timeout, and none may
        // outlast what is left of the pass budget — the same shape the
        // shutdown release phase uses, for the same reason.
        let call_deadline = deadline.min(tokio::time::Instant::now() + config.store_call_timeout);
        let call = tokio::time::timeout_at(
            call_deadline,
            allocator.release(
                grant.lease_id,
                grant.fencing_token,
                lease.remaining(),
                clock.now(),
            ),
        );
        let lease_id = grant.lease_id;
        // Racing the signal here is what makes the documented shutdown bound
        // true: a signal arriving inside a hung release is acted on now, not
        // after this call's timeout and the rest of the pass. The abandoned
        // call settles nothing locally, so the lease is re-parked exactly as
        // a timeout would leave it and the shutdown phase releases it again
        // under its own budget.
        let call_outcome = tokio::select! {
            outcome = call => outcome,
            _ = shutdown.changed() => {
                tracing::debug!(
                    lease = %lease_id,
                    "shutdown observed during a release; entering the shutdown release phase"
                );
                *parked = std::iter::once(lease).chain(queue).chain(retry).collect();
                return ReleasePass::ShutdownObserved;
            }
        };
        match call_outcome {
            Err(_) => {
                tracing::debug!(lease = %lease_id, "release timed out; retrying next tick");
                retry.push(lease);
            }
            Ok(Ok(())) => counters.record_released(),
            Ok(Err(error)) => match release_failure(&error) {
                ReleaseFailure::Retryable => {
                    tracing::warn!(lease = %lease_id, %error, "release failed; retrying next tick");
                    retry.push(lease);
                }
                ReleaseFailure::Settled => {
                    counters.record_released();
                    tracing::debug!(lease = %lease_id, %error, "lease was already settled");
                }
                ReleaseFailure::Fenced => {
                    counters.record_released();
                    tracing::warn!(lease = %lease_id,
                        "store rejected this lease's capability; clearing the slot so this instance stops serving");
                    // The current slot is a different funded grant. Keep it
                    // parked until its own request readers quiesce; dropping
                    // it here silently stranded its units (#62).
                    if let Some(current) = slot.take() {
                        retry.push(current);
                    }
                }
                ReleaseFailure::Integrity => {
                    counters.record_abandoned();
                    counters.record_integrity_fault(health);
                    tracing::error!(lease = %lease_id, units = lease.remaining().get(), %error,
                        "release violated the allocator contract; grant abandoned and readiness withdrawn");
                }
            },
        }
    }
    // Reached only by examining every lease; the two early returns above own
    // the partial cases and their ordering.
    *parked = retry;
    ReleasePass::Complete
}

#[cfg(test)]
mod tests {
    #[test]
    fn aggregate_refill_counters_reject_overflow_in_fields_and_totals() {
        let mut left = super::LeaseStats::ZERO;
        left.acquired_units = u64::MAX;
        let mut right = super::LeaseStats::ZERO;
        right.acquired_units = 1;
        assert!(left.checked_add(right).is_none());
        left = super::LeaseStats::ZERO;
        right = super::LeaseStats::ZERO;
        left.acquire_refused[0] = u64::MAX;
        right.acquire_refused[1] = 1;
        assert!(left.checked_add(right).is_none());
        right.acquire_refused[1] = 0;
        assert_eq!(left.checked_add(right).unwrap().refused(), u64::MAX);
        left = super::LeaseStats::ZERO;
        right = super::LeaseStats::ZERO;
        left.uncertain_acquires = u64::MAX;
        right.uncertain_acquires = 1;
        assert!(left.checked_add(right).is_none());
    }
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use jiff::Timestamp;
    use tollgate_core::{FencingToken, LeaseGrant, LeaseId};
    use tollgate_store::{AllocateError, ReclaimBatch, StoreError, SystemClock};

    use super::*;

    #[test]
    fn only_ambiguous_acquire_outcomes_increase_uncertainty() {
        let counters = LeaseCounters::new();
        for error in [
            AllocateError::UnknownAccount,
            AllocateError::AccountInactive,
            AllocateError::InsufficientBalance,
            AllocateError::UnknownLease,
            AllocateError::Fenced,
            AllocateError::LeaseNotActive,
            AllocateError::InvalidRelease,
            AllocateError::InvalidTtl,
        ] {
            counters.record_acquire_refused(&error);
        }
        assert_eq!(counters.snapshot().uncertain_acquires, 0);
        counters.record_acquire_refused(&AllocateError::Storage(StoreError("lost reply".into())));
        assert_eq!(counters.snapshot().uncertain_acquires, 1);
        counters.record_acquire_timeout();
        assert_eq!(counters.snapshot().uncertain_acquires, 2);
    }

    /// The doorbell's two orderings, tested directly rather than inferred from
    /// end-to-end behaviour: a lost request would show up only as refill
    /// silently reverting to the poll interval, which every other test would
    /// still pass.
    ///
    /// Signal first, then wait — the request must survive until someone asks.
    #[tokio::test]
    async fn a_request_raised_before_the_wait_is_not_lost() {
        let refill = RefillRequests::default();
        refill.request_refill();

        tokio::time::timeout(std::time::Duration::from_secs(5), refill.requested())
            .await
            .expect("a request raised before the wait must complete it");
    }

    /// Wait first, then signal — the ordinary case, where the task is parked
    /// and the debit wakes it.
    #[tokio::test]
    async fn a_request_raised_during_the_wait_wakes_it() {
        let refill = Arc::new(RefillRequests::default());
        let signal = Arc::clone(&refill);
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            signal.request_refill();
        });

        tokio::time::timeout(std::time::Duration::from_secs(5), refill.requested())
            .await
            .expect("a request raised while waiting must wake the waiter");
    }

    /// One request satisfies one wait: the flag is consumed, so the next wait
    /// blocks until something rings again. Without this, a single crossing
    /// would spin the refill loop forever.
    #[tokio::test(start_paused = true)]
    async fn a_request_is_consumed_by_the_wait_it_completes() {
        let refill = RefillRequests::default();
        refill.request_refill();
        refill.requested().await;

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), refill.requested())
                .await
                .is_err(),
            "the request was already answered; waiting again must block"
        );
    }

    /// Repeated requests before anyone waits collapse into one wake — the
    /// counterpart to `LocalLease` signalling at most once per lease.
    #[tokio::test(start_paused = true)]
    async fn repeated_requests_collapse_into_one_wake() {
        let refill = RefillRequests::default();
        for _ in 0..10 {
            refill.request_refill();
        }
        refill.requested().await;

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), refill.requested())
                .await
                .is_err(),
            "ten requests are still one outstanding refill, not ten"
        );
    }

    /// How the scripted allocator answers a release for a given lease.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Refusal {
        Storage,
        Fenced,
        InvalidRelease,
        LeaseNotActive,
        UnknownLease,
        /// Never resolves — stands in for a wedged backend.
        Hang,
    }

    /// Records successful releases; answers scripted lease ids with the
    /// scripted refusal.
    struct ScriptedAllocator {
        released: Mutex<Vec<LeaseId>>,
        refusals: HashMap<LeaseId, Refusal>,
    }

    impl ScriptedAllocator {
        fn new(refusals: impl IntoIterator<Item = (LeaseId, Refusal)>) -> Arc<Self> {
            Arc::new(Self {
                released: Mutex::new(Vec::new()),
                refusals: refusals.into_iter().collect(),
            })
        }

        fn released(&self) -> Vec<LeaseId> {
            self.released.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl LeaseAllocator for ScriptedAllocator {
        async fn acquire(
            &self,
            _account: AccountId,
            _requested: CostUnits,
            _ttl: SignedDuration,
            _now: Timestamp,
        ) -> Result<LeaseGrant, AllocateError> {
            unreachable!("release_quiesced never acquires")
        }

        async fn release(
            &self,
            lease_id: LeaseId,
            _fencing_token: FencingToken,
            _unspent: CostUnits,
            _now: Timestamp,
        ) -> Result<(), AllocateError> {
            match self.refusals.get(&lease_id) {
                Some(Refusal::Storage) => {
                    Err(AllocateError::Storage(StoreError("scripted outage".into())))
                }
                Some(Refusal::Fenced) => Err(AllocateError::Fenced),
                Some(Refusal::InvalidRelease) => Err(AllocateError::InvalidRelease),
                Some(Refusal::LeaseNotActive) => Err(AllocateError::LeaseNotActive),
                Some(Refusal::UnknownLease) => Err(AllocateError::UnknownLease),
                Some(Refusal::Hang) => std::future::pending().await,
                None => {
                    self.released.lock().unwrap().push(lease_id);
                    Ok(())
                }
            }
        }

        async fn consolidate(
            &self,
            _lease_id: LeaseId,
            _fencing_token: FencingToken,
            _unspent: CostUnits,
            _requested: CostUnits,
            _ttl: SignedDuration,
            _now: Timestamp,
        ) -> Result<LeaseGrant, AllocateError> {
            unreachable!("release_quiesced never consolidates")
        }

        async fn reclaim_expired_batch(
            &self,
            _now: Timestamp,
            _limit: std::num::NonZeroUsize,
        ) -> Result<ReclaimBatch, StoreError> {
            unreachable!("release_quiesced never reclaims")
        }
    }

    /// Answers consolidations from a script, so each failure mode can be
    /// tested for the thing that actually matters: whether the lease is put
    /// back where requests can reach it.
    struct ConsolidatingAllocator {
        answer: Mutex<Option<Result<LeaseGrant, AllocateError>>>,
        calls: AtomicU64,
    }

    impl ConsolidatingAllocator {
        fn new(answer: Result<LeaseGrant, AllocateError>) -> Arc<Self> {
            Arc::new(Self {
                answer: Mutex::new(Some(answer)),
                calls: AtomicU64::new(0),
            })
        }
    }

    #[async_trait]
    impl LeaseAllocator for ConsolidatingAllocator {
        async fn acquire(
            &self,
            _account: AccountId,
            _requested: CostUnits,
            _ttl: SignedDuration,
            _now: Timestamp,
        ) -> Result<LeaseGrant, AllocateError> {
            unreachable!("these tests drive consolidation directly")
        }

        async fn release(
            &self,
            _lease_id: LeaseId,
            _fencing_token: FencingToken,
            _unspent: CostUnits,
            _now: Timestamp,
        ) -> Result<(), AllocateError> {
            Ok(())
        }

        async fn consolidate(
            &self,
            _lease_id: LeaseId,
            _fencing_token: FencingToken,
            _unspent: CostUnits,
            _requested: CostUnits,
            _ttl: SignedDuration,
            _now: Timestamp,
        ) -> Result<LeaseGrant, AllocateError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.answer
                .lock()
                .unwrap()
                .take()
                .expect("one scripted consolidation per test")
        }

        async fn reclaim_expired_batch(
            &self,
            _now: Timestamp,
            _limit: std::num::NonZeroUsize,
        ) -> Result<ReclaimBatch, StoreError> {
            unreachable!("these tests never reclaim")
        }
    }

    fn grant(id: u128, units: u64) -> LeaseGrant {
        LeaseGrant {
            lease_id: LeaseId(id),
            account_id: AccountId(1),
            fencing_token: FencingToken(1),
            units: CostUnits(units),
            expires_at: Timestamp::from_second(3_600).unwrap(),
        }
    }

    /// Drive one consolidation against a slot holding `lease`, and report what
    /// the slot and the parked list hold afterwards.
    async fn consolidate_once(
        allocator: Arc<dyn LeaseAllocator>,
        harness: &Harness,
        lease: Arc<LocalLease>,
    ) -> (Consolidation, Option<u128>, Vec<u128>) {
        harness.slot.install(lease);
        let mut parked = Vec::new();
        let (_tx, mut shutdown) = watch::channel(false);
        let outcome = consolidate_live_lease(
            &allocator,
            &mut parked,
            &clock(),
            &harness.config,
            &harness.slot,
            &Arc::new(RefillRequests::default()),
            &harness.health,
            &harness.counters,
            &mut shutdown,
        )
        .await;
        let served = harness.slot.load().map(|l| l.grant().lease_id.0);
        let parked_ids = parked.iter().map(|l| l.grant().lease_id.0).collect();
        (outcome, served, parked_ids)
    }

    /// The happy path: the store settled the old lease inside the same
    /// transaction that issued the new one, so parking it would release units
    /// the ledger has already credited back — a double refund.
    #[tokio::test(start_paused = true)]
    async fn a_successful_consolidation_installs_the_grant_and_parks_nothing() {
        let harness = Harness::new();
        let allocator = ConsolidatingAllocator::new(Ok(grant(2, 500)));
        let (outcome, served, parked) = consolidate_once(
            Arc::clone(&allocator) as Arc<dyn LeaseAllocator>,
            &harness,
            parked_lease(1),
        )
        .await;

        assert_eq!(outcome, Consolidation::Installed);
        assert_eq!(served, Some(2), "the larger grant is serving");
        assert!(
            parked.is_empty(),
            "the superseded lease was already settled"
        );
        let stats = harness.stats();
        assert_eq!(stats.consolidated, 1);
        assert_eq!(stats.acquired, 1);
        assert_eq!(stats.released, 1, "consolidation settled its predecessor");
        assert!(!harness.counters.acquire_pending());
        assert_eq!(stats.acquired_units, 500);
    }

    /// A refusal from inside the transaction rolled it back, so the lease is
    /// still active and still ours: it goes back into the slot rather than
    /// leaving this instance denying everything until the next acquire.
    #[tokio::test(start_paused = true)]
    async fn a_rolled_back_consolidation_returns_the_lease_to_the_slot() {
        for error in [
            AllocateError::InsufficientBalance,
            AllocateError::AccountInactive,
            AllocateError::UnknownAccount,
        ] {
            let harness = Harness::new();
            let allocator = ConsolidatingAllocator::new(Err(error.clone()));
            let (outcome, served, parked) = consolidate_once(
                Arc::clone(&allocator) as Arc<dyn LeaseAllocator>,
                &harness,
                parked_lease(1),
            )
            .await;

            assert_eq!(outcome, Consolidation::KeptServing, "{error}");
            assert_eq!(
                served,
                Some(1),
                "still serving the untouched lease: {error}"
            );
            assert!(parked.is_empty(), "{error}");
            assert!(harness.is_healthy(), "an ordinary refusal is not a fault");
        }
    }

    /// The store may have committed. Reinstating would spend units it has
    /// already credited back, and dropping would strand them — so the grant is
    /// parked and the release pass finds out which happened.
    #[tokio::test(start_paused = true)]
    async fn an_ambiguous_consolidation_parks_the_grant_rather_than_reinstating_it() {
        let harness = Harness::new();
        let allocator = ConsolidatingAllocator::new(Err(AllocateError::Storage(StoreError(
            "connection reset".into(),
        ))));
        let (outcome, served, parked) = consolidate_once(
            Arc::clone(&allocator) as Arc<dyn LeaseAllocator>,
            &harness,
            parked_lease(1),
        )
        .await;

        assert_eq!(outcome, Consolidation::KeptServing);
        assert_eq!(
            served, None,
            "the slot fails closed rather than double-spending"
        );
        assert_eq!(parked, vec![1], "and the release pass settles it");
    }

    /// A capability the store no longer honours is not ours to reinstate, and
    /// there is nothing to fold in either — so the ordinary acquire path is
    /// what an empty slot needs.
    #[tokio::test(start_paused = true)]
    async fn a_settled_lease_falls_through_to_an_ordinary_acquire() {
        for error in [
            AllocateError::UnknownLease,
            AllocateError::LeaseNotActive,
            AllocateError::Fenced,
        ] {
            let harness = Harness::new();
            let allocator = ConsolidatingAllocator::new(Err(error.clone()));
            let (outcome, served, parked) = consolidate_once(
                Arc::clone(&allocator) as Arc<dyn LeaseAllocator>,
                &harness,
                parked_lease(1),
            )
            .await;

            assert_eq!(outcome, Consolidation::AcquireInstead, "{error}");
            assert_eq!(served, None, "{error}");
            assert!(parked.is_empty(), "the store already settled it: {error}");
        }
    }

    /// An over-claimed fold is a client accounting bug, and it is reported as
    /// one — but the transaction still rolled back, so the lease keeps serving
    /// while readiness is withdrawn.
    #[tokio::test(start_paused = true)]
    async fn an_over_claimed_fold_withdraws_readiness_and_keeps_serving() {
        let harness = Harness::new();
        let allocator = ConsolidatingAllocator::new(Err(AllocateError::InvalidRelease));
        let (outcome, served, parked) = consolidate_once(
            Arc::clone(&allocator) as Arc<dyn LeaseAllocator>,
            &harness,
            parked_lease(1),
        )
        .await;

        assert_eq!(outcome, Consolidation::KeptServing);
        assert_eq!(served, Some(1));
        assert!(parked.is_empty());
        assert!(!harness.is_healthy(), "an accounting fault is never silent");
    }

    /// `unspent` has to be exact, so a lease a request can still debit is not
    /// one this may fold. It goes straight back — the deny window is the
    /// take-and-check, never a wait.
    #[tokio::test(start_paused = true)]
    async fn a_consolidation_defers_while_a_reservation_is_in_flight() {
        let harness = Harness::new();
        let allocator = ConsolidatingAllocator::new(Ok(grant(2, 500)));
        let lease = parked_lease(1);
        // Stands in for a request that loaded the lease and has not finished.
        let _in_flight = Arc::clone(&lease);

        let (outcome, served, parked) = consolidate_once(
            Arc::clone(&allocator) as Arc<dyn LeaseAllocator>,
            &harness,
            lease,
        )
        .await;

        assert_eq!(outcome, Consolidation::KeptServing);
        assert_eq!(served, Some(1), "the slot keeps serving it");
        assert!(parked.is_empty());
        assert_eq!(
            allocator.calls.load(Ordering::SeqCst),
            0,
            "no store call is made against an inexact aggregate"
        );
        assert_eq!(harness.stats().consolidations_deferred, 1);
    }

    /// `release_quiesced`'s collaborators, with the parts these tests do not
    /// vary held at sensible defaults.
    struct Harness {
        config: LeaseManagerConfig,
        slot: Arc<LeaseSlot>,
        health: watch::Sender<bool>,
        healthy: watch::Receiver<bool>,
        counters: LeaseCounters,
    }

    impl Harness {
        fn new() -> Self {
            let (health, healthy) = watch::channel(true);
            Self {
                counters: LeaseCounters::new(),
                config: LeaseManagerConfig {
                    account: AccountId(1),
                    target_grant: CostUnits(1_000),
                    low_water: CostUnits(250),
                    lease_ttl: SignedDuration::from_secs(60),
                    expiry_safety_margin: SignedDuration::ZERO,
                    poll_interval: std::time::Duration::from_millis(5),
                    store_call_timeout: std::time::Duration::from_millis(50),
                    shutdown_release_deadline: std::time::Duration::from_secs(10),
                },
                slot: LeaseSlot::for_account(AccountId(1)),
                health,
                healthy,
            }
        }

        /// A pass with a budget wide enough not to be the subject: the
        /// tests that are about the budget set their own.
        async fn release_quiesced(
            &self,
            allocator: &Arc<dyn LeaseAllocator>,
            parked: &mut Vec<Arc<LocalLease>>,
        ) -> ReleasePass {
            self.release_pass(allocator, parked, std::time::Duration::from_secs(3_600))
                .await
        }

        async fn release_pass(
            &self,
            allocator: &Arc<dyn LeaseAllocator>,
            parked: &mut Vec<Arc<LocalLease>>,
            budget: std::time::Duration,
        ) -> ReleasePass {
            let (_tx, mut shutdown) = watch::channel(false);
            release_quiesced(
                allocator,
                parked,
                &clock(),
                &self.config,
                &self.slot,
                &self.health,
                &self.counters,
                &mut shutdown,
                tokio::time::Instant::now() + budget,
            )
            .await
        }

        fn stats(&self) -> LeaseStats {
            self.counters.snapshot()
        }

        fn is_healthy(&self) -> bool {
            *self.healthy.borrow()
        }
    }

    fn parked_lease(id: u128) -> Arc<LocalLease> {
        Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id: LeaseId(id),
                account_id: AccountId(1),
                fencing_token: FencingToken(1),
                units: CostUnits(100),
                expires_at: Timestamp::from_second(3_600).unwrap(),
            },
            CostUnits(10),
        ))
    }

    fn clock() -> Arc<dyn Clock> {
        Arc::new(SystemClock)
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_distinguishes_settled_leases_from_unconfirmed_or_invalid_releases() {
        for (refusal, released, faulted) in [
            (Refusal::Storage, 0, false),
            (Refusal::Fenced, 1, false),
            (Refusal::InvalidRelease, 0, true),
            (Refusal::LeaseNotActive, 1, false),
            (Refusal::UnknownLease, 1, false),
            (Refusal::Hang, 0, false),
        ] {
            let harness = Harness::new();
            harness.slot.install(parked_lease(1));
            let manager = LeaseManager::spawn(
                ScriptedAllocator::new([(LeaseId(1), refusal)]),
                harness.slot,
                Arc::new(crate::ManualClock::new(Timestamp::from_second(0).unwrap())),
                harness.config,
            )
            .unwrap();
            let counters = manager.counters();
            let health = manager.health();
            let report = manager.shutdown().await;
            assert_eq!(report.released, released);
            assert_eq!(report.abandoned, 1 - released);
            assert_eq!(counters.snapshot().released, released);
            assert_eq!(counters.snapshot().abandoned, 1 - released);
            assert_eq!(counters.integrity_fault(), faulted);
            assert!(
                !*health.borrow(),
                "every stopped task is unhealthy, including clean stops"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_deadline_shortens_the_managers_actual_release_pass() {
        let harness = Harness::new();
        harness.slot.install(parked_lease(1));
        let manager = LeaseManager::spawn(
            ScriptedAllocator::new([(LeaseId(1), Refusal::Hang)]),
            harness.slot,
            Arc::new(crate::ManualClock::new(Timestamp::from_second(0).unwrap())),
            harness.config,
        )
        .unwrap();
        let began = tokio::time::Instant::now();
        manager.stop_at(began + std::time::Duration::from_millis(5));
        let report = tokio::time::timeout(std::time::Duration::from_millis(6), manager.shutdown())
            .await
            .unwrap();
        assert_eq!(report.abandoned, 1);
        assert!(!report.task_died);
        assert_eq!(began.elapsed(), std::time::Duration::from_millis(5));
    }

    #[tokio::test]
    async fn storage_failure_does_not_skip_the_next_parked_lease() {
        let scripted = ScriptedAllocator::new([(LeaseId(1), Refusal::Storage)]);
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let harness = Harness::new();
        let mut parked = vec![parked_lease(1), parked_lease(2)];

        harness.release_quiesced(&allocator, &mut parked).await;

        assert_eq!(scripted.released(), [LeaseId(2)]);
        assert_eq!(parked.len(), 1, "only the failed lease stays parked");
        assert_eq!(parked[0].grant().lease_id, LeaseId(1));
    }

    #[tokio::test]
    async fn unquiesced_lease_is_never_released() {
        let scripted = ScriptedAllocator::new([]);
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let harness = Harness::new();
        let lease = parked_lease(7);
        let in_flight = Arc::clone(&lease);
        let mut parked = vec![lease];

        harness.release_quiesced(&allocator, &mut parked).await;
        assert!(scripted.released().is_empty());
        assert_eq!(parked.len(), 1, "held lease stays parked");
        assert_eq!(
            harness.stats().released,
            0,
            "a lease still on our books has not been released"
        );

        drop(in_flight);
        harness.release_quiesced(&allocator, &mut parked).await;
        assert_eq!(scripted.released(), [LeaseId(7)]);
        assert!(parked.is_empty());
        assert_eq!(harness.stats().released, 1);
    }

    /// Genuinely settled: the store is not holding these open, so dropping
    /// them is correct and nothing else changes (issue #42).
    #[tokio::test]
    async fn settled_refusals_drop_silently() {
        for refusal in [Refusal::LeaseNotActive, Refusal::UnknownLease] {
            let scripted = ScriptedAllocator::new([(LeaseId(3), refusal)]);
            let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
            let harness = Harness::new();
            harness.slot.install(parked_lease(99));
            let mut parked = vec![parked_lease(3)];

            harness.release_quiesced(&allocator, &mut parked).await;

            assert!(scripted.released().is_empty());
            assert!(parked.is_empty(), "settled lease is not retried");
            assert!(harness.is_healthy(), "settlement is not a health event");
            assert!(harness.slot.load().is_some(), "the slot is untouched");
            assert_eq!(
                harness.stats().released,
                1,
                "already settled still means the store no longer holds it"
            );
        }
    }

    /// The store rejected the capability copied from this grant, so local
    /// lease identity is no longer trustworthy and spending must stop
    /// (issue #42).
    #[tokio::test]
    async fn fenced_release_clears_the_slot() {
        let scripted = ScriptedAllocator::new([(LeaseId(3), Refusal::Fenced)]);
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let harness = Harness::new();
        harness.slot.install(parked_lease(99));
        let mut parked = vec![parked_lease(3)];

        harness.release_quiesced(&allocator, &mut parked).await;

        assert!(
            harness.slot.load().is_none(),
            "an instance with divergent lease identity must stop serving"
        );
        // The slot's lease is a different, live, funded one — this loop walks
        // `parked`, and the slot holds whatever the last rotation installed.
        // It must stay on the books: clearing the slot is how the instance
        // stops serving, not how its units stop existing (#62).
        //
        // The previous spelling asserted only that `parked` was empty, which
        // conflated "the fenced lease is not retried" with "nothing else is
        // parked" — and so never asked where the slot's units went while they
        // were being dropped on the floor.
        let ids: Vec<_> = parked.iter().map(|l| l.grant().lease_id).collect();
        assert_eq!(
            ids,
            [LeaseId(99)],
            "the fenced lease is not retried, and the slot's live lease is not lost"
        );
        assert_eq!(
            parked[0].remaining(),
            CostUnits(100),
            "its unspent units are still accounted for"
        );
    }

    /// The store rejected the claim as an accounting bug; continuing to spend
    /// on divergent local counts is not safe (issue #42).
    #[tokio::test]
    async fn invalid_release_fails_readiness() {
        let scripted = ScriptedAllocator::new([(LeaseId(3), Refusal::InvalidRelease)]);
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let harness = Harness::new();
        let mut parked = vec![parked_lease(3)];

        assert!(harness.is_healthy());
        harness.release_quiesced(&allocator, &mut parked).await;

        assert!(
            !harness.is_healthy(),
            "accounting divergence must drop readiness"
        );
        assert!(harness.counters.integrity_fault());
        assert_eq!(harness.counters.snapshot().abandoned, 1);
        assert_eq!(harness.counters.snapshot().released, 0);
    }

    /// A wedged backend cannot park the refill loop: the call is bounded and
    /// the lease stays parked for the next tick (issue #34).
    #[tokio::test(start_paused = true)]
    async fn hung_release_times_out_and_reparks() {
        let scripted = ScriptedAllocator::new([(LeaseId(5), Refusal::Hang)]);
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let harness = Harness::new();
        let mut parked = vec![parked_lease(5), parked_lease(6)];

        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            harness.release_quiesced(&allocator, &mut parked),
        )
        .await
        .expect("a hung release must not stall the pass");

        assert_eq!(scripted.released(), [LeaseId(6)], "the pass continues");
        assert_eq!(parked.len(), 1, "the timed-out lease is retried, not lost");
        assert_eq!(parked[0].grant().lease_id, LeaseId(5));
        assert!(
            harness.is_healthy(),
            "a timeout is not accounting divergence"
        );
    }

    /// Issue #78: the pass carries one budget, so its cost does not scale
    /// with the parked count. Six hung leases under a one-call budget cost
    /// one call's wall clock, not six.
    #[tokio::test(start_paused = true)]
    async fn a_release_pass_costs_one_budget_whatever_the_parked_count() {
        let scripted = ScriptedAllocator::new((1..=6).map(|id| (LeaseId(id), Refusal::Hang)));
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let harness = Harness::new();
        let mut parked: Vec<_> = (1..=6).map(parked_lease).collect();
        let budget = std::time::Duration::from_millis(50);

        let began = tokio::time::Instant::now();
        let outcome = harness.release_pass(&allocator, &mut parked, budget).await;
        let elapsed = began.elapsed();

        assert_eq!(outcome, ReleasePass::BudgetExpired);
        assert!(
            elapsed < budget * 2,
            "a pass over six hung leases took {elapsed:?}, which is per-call not per-pass"
        );
        assert_eq!(parked.len(), 6, "every lease is still parked, none dropped");
    }

    /// Issue #78: a lease that consumes the budget without settling goes
    /// behind the ones the pass never reached, so it cannot starve them.
    #[tokio::test(start_paused = true)]
    async fn a_lease_that_eats_the_budget_yields_its_place() {
        let scripted = ScriptedAllocator::new([(LeaseId(1), Refusal::Hang)]);
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let harness = Harness::new();
        let mut parked = vec![parked_lease(1), parked_lease(2), parked_lease(3)];
        let budget = std::time::Duration::from_millis(50);

        let outcome = harness.release_pass(&allocator, &mut parked, budget).await;

        assert_eq!(outcome, ReleasePass::BudgetExpired);
        let order: Vec<_> = parked.iter().map(|l| l.grant().lease_id).collect();
        assert_eq!(
            order,
            [LeaseId(2), LeaseId(3), LeaseId(1)],
            "the hung lease must not hold the front of the queue every pass"
        );
        assert_eq!(scripted.released(), [], "nothing settled under a hung head");
    }

    /// Issue #78: the signal is observed inside a hung release, and every
    /// lease — the one in flight included — stays parked for the shutdown
    /// release phase to attempt under its own budget.
    #[tokio::test(start_paused = true)]
    async fn shutdown_during_a_release_reparks_every_lease() {
        let scripted = ScriptedAllocator::new([(LeaseId(1), Refusal::Hang)]);
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let harness = Harness::new();
        let mut parked = vec![parked_lease(1), parked_lease(2), parked_lease(3)];
        let (tx, mut shutdown) = watch::channel(false);
        let clock = clock();

        let pass = release_quiesced(
            &allocator,
            &mut parked,
            &clock,
            &harness.config,
            &harness.slot,
            &harness.health,
            &harness.counters,
            &mut shutdown,
            // A budget far past the signal, so the signal is what ends it.
            tokio::time::Instant::now() + std::time::Duration::from_secs(3_600),
        );
        let signal = async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tx.send(true).unwrap();
        };
        let (outcome, ()) = tokio::join!(pass, signal);

        assert_eq!(outcome, ReleasePass::ShutdownObserved);
        assert_eq!(
            parked.len(),
            3,
            "the in-flight lease and the unexamined ones all survive the pass"
        );
        assert_eq!(parked[0].grant().lease_id, LeaseId(1));
    }
}
