//! Background snapshot distribution: initial load, push subscription with
//! lag recovery, periodic refresh, and revocation (review finding #5).
//!
//! The manager keeps an admission map stocked for a tracked set of
//! principals from a [`SnapshotSource`]:
//!
//! - **Initial load** — every tracked principal is resolved (installed, or
//!   negative-cached when the source confirms it unknown) before the
//!   [`ready`](SnapshotManager::ready) watch flips true, so readiness never
//!   precedes admissibility (INVARIANTS.md #10). Source errors keep
//!   retrying; readiness waits.
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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use jiff::SignedDuration;
use tokio::sync::watch;

use tollgate_admission::{LeaseSlot, SnapshotMap};
use tollgate_core::{AccountId, Principal};
use tollgate_store::{Clock, SnapshotSource};

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
    ) -> Self {
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
        SnapshotManager {
            shutdown,
            ready,
            handle: Some(handle),
        }
    }

    /// Flips true once every tracked principal has been resolved (installed
    /// or confirmed unknown) — the instance-side half of INVARIANTS.md #10.
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

/// Resolve one principal: install, or negative-cache a confirmed unknown
/// (removing any stale entry — this is the revocation path). Returns false
/// on source error.
async fn resolve(
    source: &Arc<dyn SnapshotSource>,
    map: &Arc<dyn SnapshotMap>,
    slots: &Arc<SlotRegistry>,
    clock: &Arc<dyn Clock>,
    config: &SnapshotManagerConfig,
    principal: Principal,
) -> bool {
    match source.snapshot(principal).await {
        Ok(Some(snapshot)) => {
            let slot = slots.slot(snapshot.account_id);
            map.install(principal, snapshot, slot);
            true
        }
        Ok(None) => {
            map.remove(&principal);
            let until = clock
                .now()
                .checked_add(config.negative_ttl)
                .unwrap_or_else(|_| clock.now());
            map.install_negative(principal, until);
            true
        }
        Err(_) => false,
    }
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

    // Initial load: retry until every tracked principal is resolved, then
    // report ready. The map denies (fail closed) for anything unresolved in
    // the meantime.
    let mut pending: Vec<Principal> = config.principals.clone();
    while !pending.is_empty() {
        if *shutdown.borrow() {
            return;
        }
        let mut still_pending = Vec::new();
        for principal in pending {
            if !resolve(&source, &map, &slots, &clock, &config, principal).await {
                still_pending.push(principal);
            }
        }
        pending = still_pending;
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
    let _ = ready.send(true);

    let mut tick = tokio::time::interval(config.refresh_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.reset(); // the initial load already counts as a refresh
    loop {
        if *shutdown.borrow() {
            return;
        }
        tokio::select! {
            push = updates.recv() => match push {
                Ok(push) => {
                    if config.principals.contains(&push.principal) {
                        let slot = slots.slot(push.snapshot.account_id);
                        // Generation monotonicity in the map discards stale
                        // or reordered pushes.
                        map.install(push.principal, push.snapshot, slot);
                    }
                }
                // Lagged: missed pushes — refetch everything rather than
                // guess what was dropped.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    for principal in &config.principals {
                        let _ = resolve(&source, &map, &slots, &clock, &config, *principal).await;
                    }
                }
                // Push stream gone (e.g. HTTP transport): periodic refresh
                // remains the freshness path.
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tokio::select! {
                        _ = tick.tick() => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() { return; }
                        }
                    }
                    for principal in &config.principals {
                        let _ = resolve(&source, &map, &slots, &clock, &config, *principal).await;
                    }
                }
            },
            _ = tick.tick() => {
                for principal in &config.principals {
                    let _ = resolve(&source, &map, &slots, &clock, &config, *principal).await;
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
