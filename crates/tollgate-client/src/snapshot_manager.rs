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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use jiff::SignedDuration;
use tokio::sync::watch;
use tokio::task::JoinSet;

use tollgate_admission::{LeaseSlot, SnapshotMap, SnapshotUpdate};
use tollgate_core::{AccountId, Generation, Principal};
use tollgate_store::{Clock, SnapshotSource, StoreError};

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
    /// How long a confirmed-unknown principal stays negative-cached before
    /// the next miss may retry resolution.
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

/// Handle to the snapshot task.
pub struct SnapshotManager {
    shutdown: watch::Sender<bool>,
    ready: watch::Receiver<bool>,
    handle: Option<tokio::task::JoinHandle<()>>,
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
        let handle = tokio::spawn(run(
            source,
            map,
            slots,
            clock,
            config,
            shutdown_rx,
            ready_tx,
        ));
        Ok(SnapshotManager {
            shutdown,
            ready,
            handle: Some(handle),
        })
    }

    /// True while every tracked principal has a currently valid positive or
    /// negative resolution. The sender closes if the manager task exits, so
    /// callers can include task health in their readiness probe.
    #[must_use]
    pub fn ready(&self) -> watch::Receiver<bool> {
        self.ready.clone()
    }

    pub async fn shutdown(mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
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
        generation: Option<Generation>,
    },
}

impl Resolution {
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

fn is_ready(
    principals: &[Principal],
    resolutions: &HashMap<Principal, Resolution>,
    now: jiff::Timestamp,
) -> bool {
    principals.iter().all(|principal| {
        resolutions
            .get(principal)
            .is_some_and(|resolution| now < resolution.deadline())
    })
}

fn update_ready(
    ready: &watch::Sender<bool>,
    principals: &[Principal],
    resolutions: &HashMap<Principal, Resolution>,
    clock: &Arc<dyn Clock>,
) {
    let _ = ready.send(is_ready(principals, resolutions, clock.now()));
}

fn next_readiness_check(
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
    resolutions: &mut HashMap<Principal, Resolution>,
    shutdown: &mut watch::Receiver<bool>,
    ready: &watch::Sender<bool>,
) -> Option<Vec<Principal>> {
    let mut pending = principals.iter().copied();
    let mut tasks = JoinSet::<(
        Principal,
        Result<Option<Arc<tollgate_core::AccountSnapshot>>, StoreError>,
    )>::new();
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
        let readiness_check = tokio::time::sleep(next_readiness_check(resolutions, clock.now()));
        tokio::pin!(readiness_check);
        tokio::select! {
            joined = tasks.join_next() => {
                if let Some(Ok(result)) = joined {
                    results.push(result);
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
                update_ready(ready, &config.principals, resolutions, clock);
            }
        }
    }

    let mut updates = Vec::with_capacity(results.len());
    let mut completed = HashSet::with_capacity(results.len());
    for (principal, result) in results {
        match result {
            Ok(Some(snapshot)) => {
                completed.insert(principal);
                if resolutions
                    .get(&principal)
                    .and_then(|resolution| resolution.generation())
                    .is_some_and(|generation| snapshot.generation <= generation)
                {
                    continue;
                }
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
            Ok(None) => {
                completed.insert(principal);
                let now = clock.now();
                let until = now
                    .checked_add(config.negative_ttl)
                    .unwrap_or(jiff::Timestamp::MAX);
                let generation = resolutions
                    .get(&principal)
                    .and_then(|resolution| resolution.generation());
                resolutions.insert(
                    principal,
                    Resolution::Negative {
                        deadline: until,
                        generation,
                    },
                );
                updates.push(SnapshotUpdate::Negative {
                    principal,
                    until,
                    generation: None,
                });
            }
            Err(_) => {}
        }
    }
    if !updates.is_empty() {
        map.apply_many(updates);
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
) {
    let mut updates = source.subscribe();
    let tracked: HashSet<Principal> = config.principals.iter().copied().collect();
    let mut resolutions = HashMap::with_capacity(config.principals.len());

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
        )
        .await
        else {
            return;
        };
        pending = failed;
        update_ready(&ready, &config.principals, &resolutions, &clock);
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
    update_ready(&ready, &config.principals, &resolutions, &clock);

    let mut tick = tokio::time::interval(config.refresh_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.reset(); // the initial load already counts as a refresh
    let mut updates_closed = false;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let readiness_check = tokio::time::sleep(next_readiness_check(&resolutions, clock.now()));
        tokio::pin!(readiness_check);
        tokio::select! {
            push = updates.recv(), if !updates_closed => match push {
                Ok(push) => {
                    if tracked.contains(&push.principal) {
                        match push.snapshot {
                            Some(snapshot) => {
                                if resolutions
                                    .get(&push.principal)
                                    .and_then(|resolution| resolution.generation())
                                    .is_some_and(|generation| snapshot.generation <= generation)
                                {
                                    continue;
                                }
                                let slot = slots.slot(snapshot.account_id);
                                resolutions.insert(
                                    push.principal,
                                    Resolution::Present {
                                        deadline: snapshot.valid_until,
                                        generation: snapshot.generation,
                                    },
                                );
                                map.install(push.principal, snapshot, slot);
                            }
                            None => {
                                if matches!(
                                    (
                                        resolutions
                                            .get(&push.principal)
                                            .and_then(|resolution| resolution.generation()),
                                        push.generation,
                                    ),
                                    (Some(current), Some(incoming)) if incoming < current
                                ) {
                                    continue;
                                }
                                let now = clock.now();
                                let until = now
                                    .checked_add(config.negative_ttl)
                                    .unwrap_or(jiff::Timestamp::MAX);
                                let local_generation = resolutions
                                    .get(&push.principal)
                                    .and_then(|resolution| resolution.generation());
                                let generation = match (local_generation, push.generation) {
                                    (Some(a), Some(b)) => Some(a.max(b)),
                                    (Some(a), None) => Some(a),
                                    (None, Some(b)) => Some(b),
                                    (None, None) => None,
                                };
                                resolutions.insert(
                                    push.principal,
                                    Resolution::Negative {
                                        deadline: until,
                                        generation,
                                    },
                                );
                                map.install_negative_at_generation(
                                    push.principal,
                                    until,
                                    push.generation,
                                );
                            }
                        }
                        update_ready(&ready, &config.principals, &resolutions, &clock);
                    }
                }
                // Lagged: missed pushes — refetch everything rather than
                // guess what was dropped.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    if refresh_all_cancellable(
                        &source, &map, &slots, &clock, &config, &config.principals,
                        &mut resolutions, &mut shutdown,
                        &ready,
                    ).await.is_none() {
                        return;
                    }
                    update_ready(&ready, &config.principals, &resolutions, &clock);
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
                ).await.is_none() {
                    return;
                }
                update_ready(&ready, &config.principals, &resolutions, &clock);
            }
            _ = &mut readiness_check => {
                update_ready(&ready, &config.principals, &resolutions, &clock);
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}
