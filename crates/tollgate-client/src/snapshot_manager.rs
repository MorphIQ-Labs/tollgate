//! Background snapshot distribution: initial load, push subscription with
//! lag recovery, periodic refresh, and revocation (review finding #5).
//!
//! The manager keeps an admission map stocked for a tracked set of
//! principals from a [`SnapshotSource`]:
//!
//! - **Initial load** — every tracked principal is resolved (installed, or
//!   negative-cached when the source confirms it unknown) before the
//!   [`ready`](SnapshotManager::ready) watch becomes true, so readiness never
//!   precedes admissibility (INVARIANTS.md #10). It returns to false when a
//!   resolution expires or the manager exits. Source errors keep retrying;
//!   readiness waits.
//! - **Pushes** — subscribed updates install immediately (the map's
//!   generation monotonicity discards stale or reordered pushes). A lagged
//!   subscription triggers a full refetch, so a burst of missed pushes can
//!   only delay freshness, never lose it.
//! - **Periodic refresh** — re-fetches every tracked principal, which is
//!   also how *revocation* propagates: a principal the source no longer
//!   knows is removed from the map and negative-cached. The revocation
//!   window is therefore bounded by the refresh interval (plus snapshot
//!   `valid_until`, which fails closed on its own).
//!
//! Lease slots come from a shared [`SlotRegistry`], so every principal of an
//! account observes the account's one slot — the same instance the account's
//! `LeaseManager` refills.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use jiff::SignedDuration;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::Instrument as _;

use tollgate_admission::{LeaseSlot, SnapshotMap, SnapshotUpdate};
use tollgate_core::{AccountId, Generation, Principal};
use tollgate_store::{Clock, SnapshotResolution, SnapshotSource, StoreError};

/// Account → lease-slot registry shared between the snapshot manager (which
/// installs the slot into admission state) and lease managers (which stock
/// it).
#[derive(Default)]
pub struct SlotRegistry {
    inner: Mutex<HashMap<AccountId, Arc<LeaseSlot>>>,
}

impl SlotRegistry {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The account's slot, created empty on first use.
    #[must_use]
    pub fn slot(&self, account: AccountId) -> Arc<LeaseSlot> {
        Arc::clone(
            self.inner
                .lock()
                .expect("slot registry poisoned")
                .entry(account)
                .or_insert_with(LeaseSlot::empty),
        )
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotManagerConfig {
    /// The principals this instance serves. Static for the PoC; a dynamic
    /// discovery seam is documented in docs/DESIGN.md.
    pub principals: Vec<Principal>,
    /// Full refetch cadence — also the revocation propagation bound.
    pub refresh_interval: std::time::Duration,
    /// How long a confirmed-negative principal stays valid before the
    /// manager schedules a targeted control-plane refetch.
    pub negative_ttl: SignedDuration,
    /// Backoff between initial-load retries while the source is down.
    pub retry_backoff: std::time::Duration,
    /// Maximum snapshot fetches in flight during a full refresh.
    pub max_concurrent_fetches: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotManagerConfigError(pub &'static str);

impl std::fmt::Display for SnapshotManagerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for SnapshotManagerConfigError {}

impl SnapshotManagerConfig {
    pub fn validate(&self) -> Result<(), SnapshotManagerConfigError> {
        if self.refresh_interval.is_zero() {
            return Err(SnapshotManagerConfigError(
                "refresh_interval must be positive",
            ));
        }
        if self.negative_ttl <= SignedDuration::ZERO {
            return Err(SnapshotManagerConfigError("negative_ttl must be positive"));
        }
        if self.retry_backoff.is_zero() {
            return Err(SnapshotManagerConfigError("retry_backoff must be positive"));
        }
        if self.max_concurrent_fetches == 0 {
            return Err(SnapshotManagerConfigError(
                "max_concurrent_fetches must be positive",
            ));
        }
        let distinct: HashSet<_> = self.principals.iter().copied().collect();
        if distinct.len() != self.principals.len() {
            return Err(SnapshotManagerConfigError(
                "principals must not contain duplicates",
            ));
        }
        Ok(())
    }
}

/// What a snapshot-manager shutdown observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotManagerReport {
    /// The task panicked or was aborted rather than stopping on request. Its
    /// snapshots stopped refreshing at that moment, whatever the map still
    /// holds.
    pub task_died: bool,
}

/// What the snapshot task has done, readable at any time.
///
/// [`ready`](SnapshotManager::ready) answers one bit — every principal
/// resolved, or not. That is the right shape for a readiness probe and the
/// wrong shape for diagnosis: it cannot say whether one principal is
/// unresolved or a thousand, nor whether the source has been failing all
/// morning (#4). These counters are the scrapeable half, and the
/// `unresolved` gauge is computed from the same pass that decides readiness,
/// so the two cannot disagree.
///
/// Written only by the snapshot task; `Relaxed` throughout.
#[derive(Debug)]
pub struct SnapshotCounters {
    refresh_attempts: AtomicU64,
    refresh_failures: AtomicU64,
    unresolved: AtomicU64,
}

impl SnapshotCounters {
    #[must_use]
    pub const fn new() -> Self {
        SnapshotCounters {
            refresh_attempts: AtomicU64::new(0),
            refresh_failures: AtomicU64::new(0),
            unresolved: AtomicU64::new(0),
        }
    }

    /// One fetch of one principal, whatever its outcome.
    fn record_attempt(&self) {
        self.refresh_attempts.fetch_add(1, Ordering::Relaxed);
    }

    /// A fetch the source refused or could not answer. The principal keeps
    /// its previous resolution, so this is not itself a loss of authorization
    /// — only a loss of freshness, which `unresolved` reports once the old
    /// resolution lapses.
    fn record_failure(&self) {
        self.refresh_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn set_unresolved(&self, principals: u64) {
        self.unresolved.store(principals, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> SnapshotStats {
        SnapshotStats {
            refresh_attempts: self.refresh_attempts.load(Ordering::Relaxed),
            refresh_failures: self.refresh_failures.load(Ordering::Relaxed),
            unresolved: self.unresolved.load(Ordering::Relaxed),
        }
    }
}

impl Default for SnapshotCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// A reading of [`SnapshotCounters`], safe to serialise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotStats {
    /// Fetches attempted, one per principal per pass.
    pub refresh_attempts: u64,
    /// Fetches the source could not answer.
    pub refresh_failures: u64,
    /// Principals with no currently valid resolution — a gauge, not a total.
    /// Nonzero is exactly the condition that makes `ready` false, and the
    /// count says how much of the tracked set is affected.
    pub unresolved: u64,
}

/// Handle to the snapshot task.
pub struct SnapshotManager {
    shutdown: watch::Sender<bool>,
    ready: watch::Receiver<bool>,
    handle: Option<tokio::task::JoinHandle<()>>,
    counters: Arc<SnapshotCounters>,
}

impl SnapshotManager {
    pub fn spawn(
        source: Arc<dyn SnapshotSource>,
        map: Arc<dyn SnapshotMap>,
        slots: Arc<SlotRegistry>,
        clock: Arc<dyn Clock>,
        config: SnapshotManagerConfig,
    ) -> Result<Self, SnapshotManagerConfigError> {
        config.validate()?;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (ready_tx, ready) = watch::channel(false);
        let principals = config.principals.len();
        let counters = Arc::new(SnapshotCounters::new());
        let task_counters = Arc::clone(&counters);
        let handle = tokio::spawn(
            run(
                source,
                map,
                slots,
                clock,
                config,
                shutdown_rx,
                ready_tx,
                task_counters,
            )
            .instrument(tracing::info_span!("snapshot_manager", principals)),
        );
        Ok(SnapshotManager {
            shutdown,
            ready,
            handle: Some(handle),
            counters,
        })
    }

    /// The snapshot task's running counters.
    ///
    /// Returns the shared handle for the same reason
    /// [`LeaseManager::counters`](crate::LeaseManager::counters) does: a
    /// service keeps it in request state while the manager itself is moved
    /// into whatever owns shutdown, and the counters outlive the task.
    #[must_use]
    pub fn counters(&self) -> Arc<SnapshotCounters> {
        Arc::clone(&self.counters)
    }

    /// True while every tracked principal has a currently valid positive or
    /// negative resolution. The sender closes if the manager task exits, so
    /// callers can include task health in their readiness probe.
    #[must_use]
    pub fn ready(&self) -> watch::Receiver<bool> {
        self.ready.clone()
    }

    /// Signal the task and wait for it to stop, reporting whether it got
    /// there on its own.
    ///
    /// Its two peers already return what they know at shutdown; this one
    /// returned nothing, so a manager that panicked mid-refresh was visible
    /// only as snapshots quietly going stale — indistinguishable from a
    /// control plane with nothing to say (issue #36).
    pub async fn shutdown(mut self) -> SnapshotManagerReport {
        crate::signal(&self.shutdown, true, "snapshot-manager shutdown");
        let Some(handle) = self.handle.take() else {
            return SnapshotManagerReport { task_died: true };
        };
        match handle.await {
            Ok(()) => SnapshotManagerReport { task_died: false },
            Err(error) => {
                tracing::error!(%error, "snapshot manager task died before shutdown completed");
                SnapshotManagerReport { task_died: true }
            }
        }
    }
}

impl Drop for SnapshotManager {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Resolution {
    Present {
        deadline: jiff::Timestamp,
        generation: Generation,
    },
    Negative {
        deadline: jiff::Timestamp,
        next_refetch: jiff::Timestamp,
        generation: Option<Generation>,
    },
}

impl Resolution {
    /// Test-only since #22: production code reads deadlines out of the
    /// ordered indexes rather than out of the resolution, and this survives
    /// as the naive reference's accessor — the thing the property test checks
    /// the indexes against.
    #[cfg(test)]
    fn deadline(self) -> jiff::Timestamp {
        match self {
            Resolution::Present { deadline, .. } | Resolution::Negative { deadline, .. } => {
                deadline
            }
        }
    }

    fn generation(self) -> Option<Generation> {
        match self {
            Resolution::Present { generation, .. } => Some(generation),
            Resolution::Negative { generation, .. } => generation,
        }
    }
}

/// When nothing is scheduled: re-examine in an hour rather than never.
const IDLE_WAKEUP: std::time::Duration = std::time::Duration::from_secs(3_600);

/// The tracked principals' resolutions, with the three ordered questions the
/// manager asks of them answered by index rather than by rescanning.
///
/// Every answer here used to be a full scan of the map, and one of them ran
/// once per completed fetch inside the refresh sweep — so a sweep of N
/// principals did N × O(N) work (#22). That is invisible at today's static
/// principal counts and becomes the binding constraint under the dynamic
/// discovery seam `docs/DESIGN.md` defers.
///
/// **The map is private on purpose.** Six call sites mutate resolutions, and
/// each must keep three indexes in step; maintained by hand that is exactly
/// the convention-upheld-by-caller-discipline that AGENTS.md's enforcement
/// ladder says drifts, and a drifted index here is silent — it surfaces only
/// as readiness that is wrong. Going through methods makes a desync
/// unrepresentable instead of merely tested.
///
/// Indexes hold `(deadline, principal)` so ordering is total: `Principal` is
/// `Ord`, so two principals sharing an instant cannot collide.
#[derive(Debug)]
struct Resolutions {
    /// How many principals this instance tracks. Config forbids duplicates,
    /// so this is exactly the denominator readiness is measured against.
    tracked: usize,
    /// Authoritative per-principal state. Never shrinks: an expired
    /// resolution keeps its generation watermark, which must outlive it or a
    /// replayed older generation could resurrect a revoked principal
    /// (INVARIANTS.md #15).
    by_principal: HashMap<Principal, Resolution>,
    /// Live `Present` deadlines, drained as they pass.
    present: BTreeSet<(jiff::Timestamp, Principal)>,
    /// Live `Negative` deadlines, drained as they pass.
    negative: BTreeSet<(jiff::Timestamp, Principal)>,
    /// Every `Negative`'s `next_refetch`, including ones already due —
    /// deliberately *not* drained by time, because a past refetch time is the
    /// signal to retry now, not something to forget.
    refetch: BTreeSet<(jiff::Timestamp, Principal)>,
}

impl Resolutions {
    fn new(tracked: usize) -> Self {
        Resolutions {
            tracked,
            by_principal: HashMap::with_capacity(tracked),
            present: BTreeSet::new(),
            negative: BTreeSet::new(),
            refetch: BTreeSet::new(),
        }
    }

    /// The generation watermark, if this principal has ever been resolved.
    fn generation_of(&self, principal: Principal) -> Option<Generation> {
        self.by_principal
            .get(&principal)
            .and_then(|resolution| resolution.generation())
    }

    /// Record a resolution, replacing whatever this principal held.
    ///
    /// The single write path: index bookkeeping cannot be skipped because
    /// there is nowhere else to write.
    fn insert(&mut self, principal: Principal, resolution: Resolution) {
        if let Some(previous) = self.by_principal.insert(principal, resolution) {
            self.forget(principal, previous);
        }
        match resolution {
            Resolution::Present { deadline, .. } => {
                self.present.insert((deadline, principal));
            }
            Resolution::Negative {
                deadline,
                next_refetch,
                ..
            } => {
                self.negative.insert((deadline, principal));
                self.refetch.insert((next_refetch, principal));
            }
        }
    }

    /// Push a negative principal's next attempt out, after a failed refetch.
    ///
    /// The one in-place edit the manager makes. It moves `next_refetch` only,
    /// never `deadline`, so it changes when the retry fires and never whether
    /// the instance is ready.
    fn back_off(&mut self, principal: Principal, retry_at: jiff::Timestamp) {
        let Some(Resolution::Negative { next_refetch, .. }) = self.by_principal.get_mut(&principal)
        else {
            return;
        };
        let previous = std::mem::replace(next_refetch, retry_at);
        self.refetch.remove(&(previous, principal));
        self.refetch.insert((retry_at, principal));
    }

    /// Drop a superseded resolution's index entries.
    fn forget(&mut self, principal: Principal, resolution: Resolution) {
        match resolution {
            Resolution::Present { deadline, .. } => {
                self.present.remove(&(deadline, principal));
            }
            Resolution::Negative {
                deadline,
                next_refetch,
                ..
            } => {
                self.negative.remove(&(deadline, principal));
                self.refetch.remove(&(next_refetch, principal));
            }
        }
    }

    /// Drop deadline entries that `now` has passed, so the live sets hold
    /// exactly the still-valid resolutions.
    ///
    /// Amortized O(1) per entry: each is drained at most once per insert.
    fn expire_through(&mut self, now: jiff::Timestamp) {
        // Pop the expired front, rather than splitting the set. `split_off`
        // reads naturally but rebuilds the collection on *every* call even
        // when nothing has expired, which is O(N) per query — and since the
        // sweep queries once per completed fetch, that reintroduces exactly
        // the quadratic this change exists to remove. Peeking the front is
        // O(1) when nothing is due, and each entry is popped at most once per
        // insert.
        for set in [&mut self.present, &mut self.negative] {
            while let Some((deadline, _)) = set.first() {
                if *deadline > now {
                    break;
                }
                set.pop_first();
            }
        }
    }

    /// Principals with no currently valid resolution.
    ///
    /// Readiness is derived from this rather than computed beside it: a
    /// separate predicate could drift from the gauge an operator reads,
    /// leaving `ready` false with `unresolved` at zero and nothing to explain.
    fn unresolved(&mut self, now: jiff::Timestamp) -> usize {
        self.expire_through(now);
        self.tracked - self.present.len() - self.negative.len()
    }

    /// How long until the earliest resolution lapses — when readiness could
    /// next change on its own.
    fn next_readiness_check(&mut self, now: jiff::Timestamp) -> std::time::Duration {
        self.expire_through(now);
        let earliest = match (self.present.first(), self.negative.first()) {
            (Some((a, _)), Some((b, _))) => Some((*a).min(*b)),
            (Some((only, _)), None) | (None, Some((only, _))) => Some(*only),
            (None, None) => None,
        };
        earliest.map_or(IDLE_WAKEUP, |deadline| until(deadline, now))
    }

    /// How long until the control plane next has work: a live positive
    /// lapsing, or a negative becoming due for another attempt.
    fn next_control_wakeup(&mut self, now: jiff::Timestamp) -> std::time::Duration {
        self.expire_through(now);
        let earliest = match (self.present.first(), self.refetch.first()) {
            (Some((a, _)), Some((b, _))) => Some((*a).min(*b)),
            (Some((only, _)), None) | (None, Some((only, _))) => Some(*only),
            (None, None) => None,
        };
        earliest.map_or(IDLE_WAKEUP, |deadline| until(deadline, now))
    }

    /// Negative principals whose next attempt is due.
    fn due_for_refetch(&self, now: jiff::Timestamp) -> Vec<Principal> {
        self.refetch
            .range(..(next_instant(now), Principal(0)))
            .map(|(_, principal)| *principal)
            .collect()
    }
}

/// The smallest instant strictly after `now`, so a `..(bound)` range includes
/// everything at or before `now`. Saturates at the representable maximum.
fn next_instant(now: jiff::Timestamp) -> jiff::Timestamp {
    now.checked_add(SignedDuration::from_nanos(1))
        .unwrap_or(jiff::Timestamp::MAX)
}

/// Time remaining until `deadline`, floored at zero for one already passed.
fn until(deadline: jiff::Timestamp, now: jiff::Timestamp) -> std::time::Duration {
    if deadline <= now {
        return std::time::Duration::ZERO;
    }
    u64::try_from(deadline.duration_since(now).as_nanos())
        .map(std::time::Duration::from_nanos)
        .unwrap_or(std::time::Duration::MAX)
}

fn update_ready(
    ready: &watch::Sender<bool>,
    resolutions: &mut Resolutions,
    clock: &Arc<dyn Clock>,
    counters: &SnapshotCounters,
) {
    let outstanding = resolutions.unresolved(clock.now());
    counters.set_unresolved(outstanding as u64);
    let now_ready = outstanding == 0;
    // Readiness transitions are the operator-visible half of INVARIANTS #10;
    // report the edges, not every recomputation.
    if ready.borrow().ne(&now_ready) {
        tracing::info!(ready = now_ready, "snapshot readiness changed");
    }
    crate::signal(ready, now_ready, "snapshot-manager readiness");
}

fn after_std(now: jiff::Timestamp, duration: std::time::Duration) -> jiff::Timestamp {
    let nanos = i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX);
    now.checked_add(SignedDuration::from_nanos(nanos))
        .unwrap_or(jiff::Timestamp::MAX)
}

fn negative_deadline(now: jiff::Timestamp, config: &SnapshotManagerConfig) -> jiff::Timestamp {
    now.checked_add(config.negative_ttl)
        .unwrap_or(jiff::Timestamp::MAX)
}

/// Merge a negative resolution with the locally observed generation. `None`
/// rejects an older revocation; `Some(None)` accepts an unversioned unknown.
fn merge_negative_generation(
    local: Option<Generation>,
    incoming: Option<Generation>,
) -> Option<Option<Generation>> {
    if matches!((local, incoming), (Some(current), Some(incoming)) if incoming < current) {
        return None;
    }
    Some(match (local, incoming) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    })
}

/// Refresh a set of principals with bounded concurrency, then apply every
/// positive and negative result in one logical map write. A shutdown signal
/// aborts outstanding source futures immediately.
#[allow(clippy::too_many_arguments)]
async fn refresh_all_cancellable(
    source: &Arc<dyn SnapshotSource>,
    map: &Arc<dyn SnapshotMap>,
    slots: &Arc<SlotRegistry>,
    clock: &Arc<dyn Clock>,
    config: &SnapshotManagerConfig,
    principals: &[Principal],
    resolutions: &mut Resolutions,
    shutdown: &mut watch::Receiver<bool>,
    ready: &watch::Sender<bool>,
    counters: &SnapshotCounters,
) -> Option<Vec<Principal>> {
    let mut pending = principals.iter().copied();
    let mut tasks = JoinSet::<(Principal, Result<SnapshotResolution, StoreError>)>::new();
    for _ in 0..config.max_concurrent_fetches {
        let Some(principal) = pending.next() else {
            break;
        };
        let source = Arc::clone(source);
        tasks.spawn(async move { (principal, source.snapshot(principal).await) });
    }

    let mut results = Vec::with_capacity(principals.len());
    while !tasks.is_empty() {
        // A source future may hang indefinitely. Keep readiness tied to the
        // actual resolution deadlines even while this sweep is in flight.
        let readiness_check = tokio::time::sleep(resolutions.next_readiness_check(clock.now()));
        tokio::pin!(readiness_check);
        tokio::select! {
            joined = tasks.join_next() => {
                match joined {
                    Some(Ok(result)) => results.push(result),
                    // A fetch task that panicked leaves its principal simply
                    // absent from the results, indistinguishable from one
                    // that failed cleanly — so the panic itself must be said
                    // out loud.
                    Some(Err(error)) => {
                        tracing::error!(%error, "snapshot fetch task died");
                    }
                    None => {}
                }
                if let Some(principal) = pending.next() {
                    let source = Arc::clone(source);
                    tasks.spawn(async move { (principal, source.snapshot(principal).await) });
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tasks.abort_all();
                    return None;
                }
            }
            _ = &mut readiness_check => {
                update_ready(ready, resolutions, clock, counters);
            }
        }
    }

    let mut updates = Vec::with_capacity(results.len());
    let mut completed = HashSet::with_capacity(results.len());
    for (principal, result) in results {
        // One attempt per principal per pass, counted whatever the outcome:
        // an attempt rate that has gone to zero is itself the signal that the
        // refresh loop has stopped.
        counters.record_attempt();
        match result {
            Ok(SnapshotResolution::Present(snapshot)) => {
                if resolutions
                    .generation_of(principal)
                    .is_some_and(|generation| snapshot.generation <= generation)
                {
                    continue;
                }
                let snapshot = snapshot.into_inner();
                completed.insert(principal);
                let slot = slots.slot(snapshot.account_id);
                resolutions.insert(
                    principal,
                    Resolution::Present {
                        deadline: snapshot.valid_until,
                        generation: snapshot.generation,
                    },
                );
                updates.push(SnapshotUpdate::Present {
                    principal,
                    snapshot,
                    lease: slot,
                });
            }
            Ok(SnapshotResolution::Revoked {
                generation: incoming,
            }) => {
                let now = clock.now();
                let until = negative_deadline(now, config);
                let local_generation = resolutions.generation_of(principal);
                let Some(generation) = merge_negative_generation(local_generation, Some(incoming))
                else {
                    continue;
                };
                completed.insert(principal);
                resolutions.insert(
                    principal,
                    Resolution::Negative {
                        deadline: until,
                        next_refetch: until,
                        generation,
                    },
                );
                updates.push(SnapshotUpdate::Negative {
                    principal,
                    until,
                    generation: Some(incoming),
                });
            }
            Ok(SnapshotResolution::Unknown) => {
                completed.insert(principal);
                let now = clock.now();
                let until = negative_deadline(now, config);
                let generation = resolutions.generation_of(principal);
                resolutions.insert(
                    principal,
                    Resolution::Negative {
                        deadline: until,
                        next_refetch: until,
                        generation,
                    },
                );
                updates.push(SnapshotUpdate::Negative {
                    principal,
                    until,
                    generation: None,
                });
            }
            // The principal keeps whatever resolution it already had and
            // stays pending for the next sweep. Without this event a source
            // that is down looks exactly like one with nothing to say.
            Err(error) => {
                counters.record_failure();
                tracing::warn!(
                    principal = principal.0,
                    %error,
                    "snapshot fetch failed; principal keeps its previous resolution"
                );
            }
        }
    }
    if !updates.is_empty() {
        map.apply_many_at(updates, clock.now());
    }
    Some(
        principals
            .iter()
            .copied()
            .filter(|principal| !completed.contains(principal))
            .collect(),
    )
}

#[allow(clippy::too_many_arguments)]
async fn run(
    source: Arc<dyn SnapshotSource>,
    map: Arc<dyn SnapshotMap>,
    slots: Arc<SlotRegistry>,
    clock: Arc<dyn Clock>,
    config: SnapshotManagerConfig,
    mut shutdown: watch::Receiver<bool>,
    ready: watch::Sender<bool>,
    counters: Arc<SnapshotCounters>,
) {
    let mut updates = source.subscribe();
    let tracked: HashSet<Principal> = config.principals.iter().copied().collect();
    let mut resolutions = Resolutions::new(config.principals.len());

    // Initial load: retry until every tracked principal is resolved, then
    // report ready. The map denies (fail closed) for anything unresolved in
    // the meantime.
    let mut pending: Vec<Principal> = config.principals.clone();
    while !pending.is_empty() {
        if *shutdown.borrow() {
            return;
        }
        let Some(failed) = refresh_all_cancellable(
            &source,
            &map,
            &slots,
            &clock,
            &config,
            &pending,
            &mut resolutions,
            &mut shutdown,
            &ready,
            &counters,
        )
        .await
        else {
            return;
        };
        pending = failed;
        update_ready(&ready, &mut resolutions, &clock, &counters);
        if !pending.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(config.retry_backoff) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }
    update_ready(&ready, &mut resolutions, &clock, &counters);

    let mut tick = tokio::time::interval(config.refresh_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.reset(); // the initial load already counts as a refresh
    let mut updates_closed = false;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let control_wakeup = tokio::time::sleep(resolutions.next_control_wakeup(clock.now()));
        tokio::pin!(control_wakeup);
        tokio::select! {
            push = updates.recv(), if !updates_closed => match push {
                Ok(push) => {
                    if tracked.contains(&push.principal) {
                        match push.resolution {
                            SnapshotResolution::Present(snapshot) => {
                                if resolutions.generation_of(push.principal)
                                    .is_some_and(|generation| snapshot.generation <= generation)
                                {
                                    continue;
                                }
                                let snapshot = snapshot.into_inner();
                                let slot = slots.slot(snapshot.account_id);
                                resolutions.insert(
                                    push.principal,
                                    Resolution::Present {
                                        deadline: snapshot.valid_until,
                                        generation: snapshot.generation,
                                    },
                                );
                                map.apply_many_at(
                                    vec![SnapshotUpdate::Present {
                                        principal: push.principal,
                                        snapshot,
                                        lease: slot,
                                    }],
                                    clock.now(),
                                );
                            }
                            resolution @ (SnapshotResolution::Revoked { .. }
                            | SnapshotResolution::Unknown) => {
                                let incoming = match resolution {
                                    SnapshotResolution::Revoked { generation } => Some(generation),
                                    SnapshotResolution::Unknown => None,
                                    SnapshotResolution::Present(_) => unreachable!(),
                                };
                                let local_generation = resolutions.generation_of(push.principal);
                                let Some(generation) =
                                    merge_negative_generation(local_generation, incoming)
                                else {
                                    continue;
                                };
                                let now = clock.now();
                                let until = negative_deadline(now, &config);
                                resolutions.insert(
                                    push.principal,
                                    Resolution::Negative {
                                        deadline: until,
                                        next_refetch: until,
                                        generation,
                                    },
                                );
                                map.apply_many_at(
                                    vec![SnapshotUpdate::Negative {
                                        principal: push.principal,
                                        until,
                                        generation: incoming,
                                    }],
                                    now,
                                );
                            }
                        }
                        update_ready(&ready, &mut resolutions, &clock, &counters);
                    }
                }
                // Lagged: missed pushes — refetch everything rather than
                // guess what was dropped.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    if refresh_all_cancellable(
                        &source, &map, &slots, &clock, &config, &config.principals,
                        &mut resolutions, &mut shutdown,
                        &ready,
                        &counters,
                    ).await.is_none() {
                        return;
                    }
                    update_ready(&ready, &mut resolutions, &clock, &counters);
                }
                // Push stream gone (e.g. HTTP transport): periodic refresh
                // remains the freshness path.
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    updates_closed = true;
                }
            },
            _ = tick.tick() => {
                if refresh_all_cancellable(
                    &source, &map, &slots, &clock, &config, &config.principals,
                    &mut resolutions, &mut shutdown,
                    &ready,
                    &counters,
                ).await.is_none() {
                    return;
                }
                update_ready(&ready, &mut resolutions, &clock, &counters);
            }
            _ = &mut control_wakeup => {
                let now = clock.now();
                update_ready(&ready, &mut resolutions, &clock, &counters);
                let due = resolutions.due_for_refetch(now);
                if !due.is_empty() {
                    let Some(failed) = refresh_all_cancellable(
                        &source, &map, &slots, &clock, &config, &due,
                        &mut resolutions, &mut shutdown, &ready, &counters,
                    ).await else {
                        return;
                    };
                    let retry_at = after_std(clock.now(), config.retry_backoff);
                    for principal in failed {
                        resolutions.back_off(principal, retry_at);
                    }
                    update_ready(&ready, &mut resolutions, &clock, &counters);
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn t(seconds: i64) -> jiff::Timestamp {
        jiff::Timestamp::from_second(seconds).unwrap()
    }

    #[test]
    fn control_wakeup_tracks_future_positive_deadline_once() {
        let principal = Principal(1);
        let mut resolutions = Resolutions::new(1);
        resolutions.insert(
            principal,
            Resolution::Present {
                deadline: t(15),
                generation: Generation(1),
            },
        );
        assert_eq!(
            resolutions.next_control_wakeup(t(10)),
            std::time::Duration::from_secs(5)
        );

        resolutions.insert(
            principal,
            Resolution::Present {
                deadline: t(10),
                generation: Generation(1),
            },
        );
        assert_eq!(
            resolutions.next_control_wakeup(t(10)),
            std::time::Duration::from_secs(3_600)
        );
    }

    #[test]
    fn control_wakeup_tracks_negative_refetch_deadline() {
        let principal = Principal(1);
        let mut resolutions = Resolutions::new(1);
        resolutions.insert(
            principal,
            Resolution::Negative {
                deadline: t(15),
                next_refetch: t(15),
                generation: None,
            },
        );
        assert_eq!(
            resolutions.next_control_wakeup(t(10)),
            std::time::Duration::from_secs(5)
        );
        assert_eq!(
            resolutions.next_control_wakeup(t(15)),
            std::time::Duration::ZERO
        );
    }

    // ---- #22: the indexed set must answer what the scans answered --------

    /// The pre-#22 implementations, kept verbatim as the reference the
    /// indexed set is checked against. If these and `Resolutions` ever
    /// disagree, the refactor changed behaviour — which is the one thing it
    /// must not do.
    mod naive {
        use super::*;

        pub(super) fn unresolved(
            principals: &[Principal],
            resolutions: &HashMap<Principal, Resolution>,
            now: jiff::Timestamp,
        ) -> usize {
            principals
                .iter()
                .filter(|principal| {
                    !resolutions
                        .get(principal)
                        .is_some_and(|resolution| now < resolution.deadline())
                })
                .count()
        }

        pub(super) fn next_readiness_check(
            resolutions: &HashMap<Principal, Resolution>,
            now: jiff::Timestamp,
        ) -> std::time::Duration {
            resolutions
                .values()
                .map(|resolution| resolution.deadline())
                .filter(|deadline| *deadline > now)
                .map(|deadline| deadline.duration_since(now).as_nanos())
                .min()
                .and_then(|nanos| u64::try_from(nanos).ok())
                .map(std::time::Duration::from_nanos)
                .unwrap_or_else(|| std::time::Duration::from_secs(3_600))
        }

        pub(super) fn next_control_wakeup(
            resolutions: &HashMap<Principal, Resolution>,
            now: jiff::Timestamp,
        ) -> std::time::Duration {
            resolutions
                .values()
                .filter_map(|resolution| match resolution {
                    Resolution::Present { deadline, .. } if *deadline > now => Some(*deadline),
                    Resolution::Present { .. } => None,
                    Resolution::Negative { next_refetch, .. } => Some(*next_refetch),
                })
                .map(|deadline| {
                    if deadline <= now {
                        std::time::Duration::ZERO
                    } else {
                        let nanos = deadline.duration_since(now).as_nanos();
                        u64::try_from(nanos)
                            .map(std::time::Duration::from_nanos)
                            .unwrap_or(std::time::Duration::MAX)
                    }
                })
                .min()
                .unwrap_or_else(|| std::time::Duration::from_secs(3_600))
        }

        pub(super) fn due_for_refetch(
            resolutions: &HashMap<Principal, Resolution>,
            now: jiff::Timestamp,
        ) -> Vec<Principal> {
            let mut due: Vec<_> = resolutions
                .iter()
                .filter_map(|(principal, resolution)| match resolution {
                    Resolution::Negative { next_refetch, .. } if *next_refetch <= now => {
                        Some(*principal)
                    }
                    _ => None,
                })
                .collect();
            due.sort_unstable();
            due
        }
    }

    /// One step of the manager's life, as the property test drives it.
    #[derive(Debug, Clone, Copy)]
    enum Step {
        Present { principal: u8, deadline: i64 },
        Negative { principal: u8, deadline: i64 },
        BackOff { principal: u8, retry_in: i64 },
        Advance { seconds: i64 },
    }

    const TRACKED: usize = 6;

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            (0..TRACKED as u8, 0i64..400).prop_map(|(principal, deadline)| Step::Present {
                principal,
                deadline
            }),
            (0..TRACKED as u8, 0i64..400).prop_map(|(principal, deadline)| Step::Negative {
                principal,
                deadline
            }),
            (0..TRACKED as u8, 0i64..200).prop_map(|(principal, retry_in)| Step::BackOff {
                principal,
                retry_in
            }),
            (0i64..50).prop_map(|seconds| Step::Advance { seconds }),
        ]
    }

    proptest! {
        /// Every question the indexed set answers must match the scan it
        /// replaced, after any sequence of resolutions, backoffs and clock
        /// advances (#22). Time only moves forward, as it does in the
        /// manager.
        #[test]
        fn indexed_resolutions_answer_exactly_what_scanning_answered(
            steps in proptest::collection::vec(step(), 1..60),
        ) {
            let principals: Vec<Principal> =
                (0..TRACKED as u128).map(Principal).collect();
            let mut indexed = Resolutions::new(TRACKED);
            let mut reference: HashMap<Principal, Resolution> = HashMap::new();
            let mut now = t(0);

            for step in steps {
                match step {
                    Step::Present { principal, deadline } => {
                        let principal = Principal(u128::from(principal));
                        let resolution = Resolution::Present {
                            deadline: t(deadline),
                            generation: Generation(1),
                        };
                        indexed.insert(principal, resolution);
                        reference.insert(principal, resolution);
                    }
                    Step::Negative { principal, deadline } => {
                        let principal = Principal(u128::from(principal));
                        let resolution = Resolution::Negative {
                            deadline: t(deadline),
                            next_refetch: t(deadline),
                            generation: None,
                        };
                        indexed.insert(principal, resolution);
                        reference.insert(principal, resolution);
                    }
                    Step::BackOff { principal, retry_in } => {
                        let principal = Principal(u128::from(principal));
                        let retry_at = t(now.as_second() + retry_in);
                        indexed.back_off(principal, retry_at);
                        if let Some(Resolution::Negative { next_refetch, .. }) =
                            reference.get_mut(&principal)
                        {
                            *next_refetch = retry_at;
                        }
                    }
                    Step::Advance { seconds } => {
                        now = t(now.as_second() + seconds);
                    }
                }

                prop_assert_eq!(
                    indexed.unresolved(now),
                    naive::unresolved(&principals, &reference, now),
                    "unresolved disagreed at {:?}", now
                );
                prop_assert_eq!(
                    indexed.next_readiness_check(now),
                    naive::next_readiness_check(&reference, now),
                    "next_readiness_check disagreed at {:?}", now
                );
                prop_assert_eq!(
                    indexed.next_control_wakeup(now),
                    naive::next_control_wakeup(&reference, now),
                    "next_control_wakeup disagreed at {:?}", now
                );
                let mut due = indexed.due_for_refetch(now);
                due.sort_unstable();
                prop_assert_eq!(
                    due,
                    naive::due_for_refetch(&reference, now),
                    "due_for_refetch disagreed at {:?}", now
                );
            }
        }
    }

    /// A resolution inserted already expired counts as unresolved at once —
    /// it never enters the live index, so nothing has to expire it later.
    #[test]
    fn a_resolution_inserted_expired_is_never_live() {
        let mut resolutions = Resolutions::new(1);
        resolutions.insert(
            Principal(0),
            Resolution::Present {
                deadline: t(5),
                generation: Generation(1),
            },
        );
        assert_eq!(resolutions.unresolved(t(10)), 1);
        // And asking again does not double-count or underflow.
        assert_eq!(resolutions.unresolved(t(10)), 1);
        assert_eq!(resolutions.unresolved(t(20)), 1);
    }

    /// Re-resolving a principal replaces its index entry rather than adding
    /// one: the live count is per principal, not per insert.
    #[test]
    fn re_resolving_a_principal_does_not_double_count_it() {
        let mut resolutions = Resolutions::new(2);
        for deadline in [t(50), t(60), t(60), t(70)] {
            resolutions.insert(
                Principal(0),
                Resolution::Present {
                    deadline,
                    generation: Generation(1),
                },
            );
        }
        assert_eq!(
            resolutions.unresolved(t(10)),
            1,
            "one principal resolved, one still outstanding"
        );
        assert_eq!(resolutions.next_readiness_check(t(10)), secs(60));
    }

    /// A negative superseded by a positive must leave neither a stale
    /// deadline nor a stale refetch behind.
    #[test]
    fn a_positive_replacing_a_negative_clears_both_of_its_indexes() {
        let mut resolutions = Resolutions::new(1);
        resolutions.insert(
            Principal(0),
            Resolution::Negative {
                deadline: t(30),
                next_refetch: t(30),
                generation: None,
            },
        );
        resolutions.insert(
            Principal(0),
            Resolution::Present {
                deadline: t(90),
                generation: Generation(2),
            },
        );

        assert_eq!(resolutions.unresolved(t(40)), 0, "the positive is live");
        assert!(
            resolutions.due_for_refetch(t(40)).is_empty(),
            "the superseded negative must not still ask to be refetched"
        );
        assert_eq!(resolutions.next_control_wakeup(t(40)), secs(50));
    }

    /// The generation watermark outlives the resolution that carried it, or a
    /// replayed older generation could resurrect a revoked principal
    /// (INVARIANTS.md #15).
    #[test]
    fn an_expired_resolution_keeps_its_generation() {
        let mut resolutions = Resolutions::new(1);
        resolutions.insert(
            Principal(0),
            Resolution::Negative {
                deadline: t(30),
                next_refetch: t(30),
                generation: Some(Generation(7)),
            },
        );
        assert_eq!(resolutions.unresolved(t(100)), 1, "expired");
        assert_eq!(
            resolutions.generation_of(Principal(0)),
            Some(Generation(7)),
            "the watermark must survive the expiry"
        );
    }

    fn secs(seconds: u64) -> std::time::Duration {
        std::time::Duration::from_secs(seconds)
    }

    #[test]
    fn deadline_helpers_are_exact_in_the_supported_domain() {
        let config = SnapshotManagerConfig {
            principals: vec![Principal(1)],
            refresh_interval: std::time::Duration::from_secs(60),
            negative_ttl: SignedDuration::from_secs(30),
            retry_backoff: std::time::Duration::from_secs(2),
            max_concurrent_fetches: 1,
        };
        assert_eq!(negative_deadline(t(10), &config), t(40));
        assert_eq!(after_std(t(10), config.retry_backoff), t(12));
    }

    #[test]
    fn negative_generation_merge_rejects_only_older_revocations() {
        assert_eq!(
            merge_negative_generation(Some(Generation(5)), Some(Generation(4))),
            None
        );
        assert_eq!(
            merge_negative_generation(Some(Generation(5)), Some(Generation(5))),
            Some(Some(Generation(5)))
        );
        assert_eq!(
            merge_negative_generation(Some(Generation(5)), Some(Generation(6))),
            Some(Some(Generation(6)))
        );
        assert_eq!(
            merge_negative_generation(Some(Generation(5)), None),
            Some(Some(Generation(5)))
        );
        assert_eq!(merge_negative_generation(None, None), Some(None));
    }
}
