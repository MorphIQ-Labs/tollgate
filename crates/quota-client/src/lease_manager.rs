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
//!   only remaining `Arc` (`Arc::strong_count == 1`), no reservation exists
//!   and none can be created (the slot no longer points at it), so its
//!   remaining count is final and the release cannot race a debit or credit.
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

use std::sync::Arc;

use jiff::SignedDuration;
use tokio::sync::watch;

use quota_admission::LeaseSlot;
use quota_core::{AccountId, CostUnits, LocalLease};
use quota_store::LeaseAllocator;

use crate::clock::Clock;

#[derive(Debug, Clone, Copy)]
pub struct LeaseManagerConfig {
    pub account: AccountId,
    /// Units requested per acquire; the allocator's grant policy may shrink
    /// the actual grant near exhaustion.
    pub target_grant: CostUnits,
    /// Refill threshold installed into each `LocalLease`.
    pub low_water: CostUnits,
    pub lease_ttl: SignedDuration,
    /// How often the slot is inspected. Refill latency is bounded by this
    /// plus one allocator round-trip — all off the request path.
    pub poll_interval: std::time::Duration,
}

/// Handle to the refill task.
pub struct LeaseManager {
    shutdown: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
}

impl LeaseManager {
    pub fn spawn(
        allocator: Arc<dyn LeaseAllocator>,
        slot: Arc<LeaseSlot>,
        clock: Arc<dyn Clock>,
        config: LeaseManagerConfig,
    ) -> Self {
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(run(allocator, slot, clock, config, shutdown_rx));
        LeaseManager { shutdown, handle }
    }

    /// Signal the task, wait for it to release the current lease and exit.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.handle.await;
    }
}

async fn run(
    allocator: Arc<dyn LeaseAllocator>,
    slot: Arc<LeaseSlot>,
    clock: Arc<dyn Clock>,
    config: LeaseManagerConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(config.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Superseded leases waiting for quiescence before their unspent units go
    // back to the allocator.
    let mut parked: Vec<Arc<LocalLease>> = Vec::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = shutdown.changed() => {}
        }
        if *shutdown.borrow() {
            break;
        }

        let now = clock.now();
        release_quiesced(&allocator, &mut parked, &clock).await;
        let needs_acquire = match slot.load() {
            None => true,
            Some(lease) if now >= lease.grant().expires_at => {
                // Expired: fail closed immediately rather than letting the
                // request path keep hitting LeaseExpired on a dead lease.
                slot.clear();
                true
            }
            Some(lease) => lease.needs_refill(),
        };
        if !needs_acquire {
            continue;
        }

        match allocator
            .acquire(config.account, config.target_grant, config.lease_ttl, now)
            .await
        {
            Ok(grant) => {
                // Rotation: install the fresh lease and park the superseded
                // one until it quiesces (module docs).
                if let Some(old) = slot.load() {
                    parked.push(old);
                }
                slot.install(Arc::new(LocalLease::new(grant, config.low_water)));
            }
            Err(_) => {
                // Denied or backend down: nothing to install. The slot keeps
                // whatever live lease it still has (spend continues until
                // exhaustion/expiry); an empty slot stays empty — deny.
            }
        }
    }

    // Graceful shutdown: return what's left of the current and parked
    // leases. Callers flushed usage and stopped admitting first, so every
    // lease has quiesced and `remaining` is exactly granted - used.
    if let Some(lease) = slot.load() {
        slot.clear();
        parked.push(lease);
    }
    for lease in parked {
        let grant = lease.grant();
        let _ = allocator
            .release(
                grant.lease_id,
                grant.fencing_token,
                lease.remaining(),
                clock.now(),
            )
            .await;
    }
}

/// Release every parked lease that has quiesced; keep the rest parked.
/// Terminal allocator refusals (fenced, already settled) drop the lease —
/// the store has already accounted for it; storage errors keep it parked for
/// retry next tick.
async fn release_quiesced(
    allocator: &Arc<dyn LeaseAllocator>,
    parked: &mut Vec<Arc<LocalLease>>,
    clock: &Arc<dyn Clock>,
) {
    let mut index = 0;
    while index < parked.len() {
        if Arc::strong_count(&parked[index]) > 1 {
            index += 1;
            continue;
        }
        let lease = parked.swap_remove(index);
        let grant = lease.grant();
        if let Err(quota_store::AllocateError::Storage(_)) = allocator
            .release(
                grant.lease_id,
                grant.fencing_token,
                lease.remaining(),
                clock.now(),
            )
            .await
        {
            parked.push(lease);
            index += 1;
        }
    }
}
