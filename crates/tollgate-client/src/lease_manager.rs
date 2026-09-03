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
use tollgate_core::{AccountId, CostUnits, LocalLease, RefillSignal};
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
/// `Relaxed` throughout — nothing is published through them.
#[derive(Debug)]
pub struct LeaseCounters {
    acquired: AtomicU64,
    acquired_units: AtomicU64,
    acquire_timeouts: AtomicU64,
    acquire_refused: [AtomicU64; AllocateError::COUNT],
    released: AtomicU64,
    abandoned: AtomicU64,
}

impl LeaseCounters {
    #[must_use]
    pub const fn new() -> Self {
        LeaseCounters {
            acquired: AtomicU64::new(0),
            acquired_units: AtomicU64::new(0),
            acquire_timeouts: AtomicU64::new(0),
            acquire_refused: [const { AtomicU64::new(0) }; AllocateError::COUNT],
            released: AtomicU64::new(0),
            abandoned: AtomicU64::new(0),
        }
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
    }

    fn record_acquire_refused(&self, error: &AllocateError) {
        self.acquire_refused[error.index()].fetch_add(1, Ordering::Relaxed);
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
            acquire_refused: std::array::from_fn(|slot| {
                self.acquire_refused[slot].load(Ordering::Relaxed)
            }),
            released: self.released.load(Ordering::Relaxed),
            abandoned: self.abandoned.load(Ordering::Relaxed),
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
    /// Acquire refusals per [`AllocateError::index`] slot.
    pub acquire_refused: [u64; AllocateError::COUNT],
    /// Leases the allocator no longer holds open for this instance.
    pub released: u64,
    /// Leases the shutdown budget could not return; their units come back at
    /// TTL reclaim (INVARIANTS.md #9). Like the writer's `lost`, this can only
    /// move at shutdown — it is a confirmation, not an early warning.
    pub abandoned: u64,
}

impl LeaseStats {
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
        let (health_tx, health) = watch::channel(true);
        let account = config.account;
        let counters = Arc::new(LeaseCounters::new());
        let task_counters = Arc::clone(&counters);
        let handle = tokio::spawn(
            async move {
                let report = run(
                    allocator,
                    slot,
                    clock,
                    config,
                    shutdown_rx,
                    &health_tx,
                    &task_counters,
                )
                .await;
                crate::signal(&health_tx, false, "lease-manager health");
                report
            }
            .instrument(tracing::info_span!("lease_manager", %account)),
        );
        Ok(LeaseManager {
            shutdown,
            health,
            handle: Some(handle),
            counters,
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
    /// with the store. Channel closure also means the task exited (including
    /// panic/abort), so readiness probes should check both the current value
    /// and `Receiver::has_changed().is_ok()`.
    #[must_use]
    pub fn health(&self) -> watch::Receiver<bool> {
        self.health.clone()
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
        match self.handle.take() {
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

async fn run(
    allocator: Arc<dyn LeaseAllocator>,
    slot: Arc<LeaseSlot>,
    clock: Arc<dyn Clock>,
    config: LeaseManagerConfig,
    mut shutdown: watch::Receiver<bool>,
    health: &watch::Sender<bool>,
    counters: &LeaseCounters,
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
        let needs_acquire = match slot.load() {
            None => true,
            Some(lease) if now >= lease.usable_until() => {
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
                true
            }
            Some(lease) => lease.refill_due_or_rearm(),
        };
        if !needs_acquire {
            continue;
        }

        // A hung allocator must not park the refill loop: a timeout takes the
        // same path as a refusal, so the slot fails closed and the next tick
        // tries again. The signal is raced against the call for the same
        // reason it is raced inside the release pass — an acquire abandoned
        // here settles nothing, and the slot it would have filled is one the
        // shutdown phase is about to drain anyway (#78).
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
        match acquired {
            Ok(Ok(grant)) => {
                counters.record_acquired(grant.units);
                // Rotation: install the fresh lease and park the superseded
                // one until it quiesces (module docs). Adaptive allocation
                // may return less than target_grant; cap low-water below the
                // actual grant so a fresh tail grant does not immediately
                // rotate without serving any work.
                let low_water = CostUnits(
                    config
                        .low_water
                        .get()
                        .min(grant.units.get().saturating_sub(1)),
                );
                let fresh = Arc::new(
                    LocalLease::with_sharding(
                        grant,
                        low_water,
                        config.expiry_safety_margin,
                        slot.sharding(),
                    )
                    // Each fresh lease gets the doorbell and its own
                    // unrung shard flags. A single-counter lease therefore
                    // rings once for the whole rotation; a sharded one rings
                    // at most once per shard between aggregate checks, which
                    // is what `refill_due_or_rearm` above clears whenever the
                    // aggregate shows an early crossing was premature
                    // (INVARIANTS.md #6).
                    .with_refill(Arc::clone(&refill) as Arc<dyn RefillSignal>),
                );
                if let Some(old) = slot.replace(fresh) {
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
    let deadline = tokio::time::Instant::now() + config.shutdown_release_deadline;
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
            // A refusal still means the store is no longer holding this lease
            // open for us — only an unfinished call leaves it outstanding.
            // The count says how many; the event says which refusal, which
            // the count alone cannot distinguish from a clean release.
            Ok(Ok(())) => {
                report.released += 1;
                counters.record_released();
            }
            Ok(Err(error)) => {
                report.released += 1;
                counters.record_released();
                tracing::warn!(
                    lease = %grant.lease_id,
                    %error,
                    "lease refused at shutdown; the store considers it settled"
                );
            }
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
            // Unfinished or unreachable: nothing was settled, try next tick.
            Err(_) => {
                tracing::debug!(lease = %lease_id, "release timed out; retrying next tick");
                retry.push(lease);
            }
            Ok(Err(AllocateError::Storage(error))) => {
                tracing::warn!(
                    lease = %lease_id,
                    %error,
                    "release failed against the store; retrying next tick"
                );
                retry.push(lease);
            }
            // Settled, or nothing to settle. Every arm below stops tracking
            // the lease, so each counts as released; only the two retry arms
            // above leave it on our books.
            Ok(Ok(())) => counters.record_released(),
            Ok(Err(error @ (AllocateError::LeaseNotActive | AllocateError::UnknownLease))) => {
                counters.record_released();
                tracing::debug!(lease = %lease_id, %error, "lease was already settled");
            }
            Ok(Err(AllocateError::Fenced)) => {
                counters.record_released();
                tracing::warn!(
                    lease = %lease_id,
                    "store rejected this lease's capability; clearing the slot so this instance stops serving"
                );
                // The slot holds a *different* lease from the fenced one —
                // this loop walks `parked`, and the current lease is whatever
                // the last rotation installed. Discarding what `take` hands
                // back dropped a live, funded lease on the floor: never
                // released, never re-parked, never counted abandoned, and
                // named by no event, so an operator saw `released += 1` and
                // `abandoned == 0` while its units stranded for a full TTL
                // (#62).
                //
                // Parked rather than released here, so the quiescence guard at
                // the top of this loop applies to it on the next pass: a
                // request that loaded it before the slot was cleared still
                // holds a view.
                //
                // `Option` is not `#[must_use]`, so the workspace's
                // `let_underscore_must_use` deny — invariant 19's mechanical
                // half — could not see this. Binding it is what makes the
                // discard impossible to write again by accident.
                if let Some(current) = slot.take() {
                    retry.push(current);
                }
            }
            Ok(Err(AllocateError::InvalidRelease)) => {
                counters.record_released();
                tracing::error!(
                    lease = %lease_id,
                    units = lease.remaining().get(),
                    "the store rejected this release as an accounting error; \
                     local counts disagree with the ledger and readiness is dropping"
                );
                crate::signal(health, false, "lease-manager health");
            }
            // Acquire-only refusals; a release cannot produce them. Reaching
            // this arm means the allocator's contract changed under us.
            Ok(Err(error)) => {
                counters.record_released();
                tracing::error!(
                    lease = %lease_id,
                    %error,
                    "allocator returned an acquire-only refusal to a release"
                );
            }
        }
    }
    // Reached only by examining every lease; the two early returns above own
    // the partial cases and their ordering.
    *parked = retry;
    ReleasePass::Complete
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use jiff::Timestamp;
    use tollgate_core::{FencingToken, LeaseGrant, LeaseId};
    use tollgate_store::{AllocateError, ReclaimBatch, StoreError, SystemClock};

    use super::*;

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

        async fn reclaim_expired_batch(
            &self,
            _now: Timestamp,
            _limit: std::num::NonZeroUsize,
        ) -> Result<ReclaimBatch, StoreError> {
            unreachable!("release_quiesced never reclaims")
        }
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
