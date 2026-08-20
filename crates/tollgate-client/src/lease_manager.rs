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

use tollgate_admission::LeaseSlot;
use tollgate_core::{AccountId, CostUnits, LocalLease};
use tollgate_store::LeaseAllocator;

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
        Ok(())
    }
}

/// Handle to the refill task.
pub struct LeaseManager {
    shutdown: watch::Sender<bool>,
    health: watch::Receiver<bool>,
    handle: Option<tokio::task::JoinHandle<()>>,
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
        let handle = tokio::spawn(async move {
            run(allocator, slot, clock, config, shutdown_rx).await;
            let _ = health_tx.send(false);
        });
        Ok(LeaseManager {
            shutdown,
            health,
            handle: Some(handle),
        })
    }

    /// True while the refill task is alive. Channel closure also means the
    /// task exited (including panic/abort), so readiness probes should check
    /// both the current value and `Receiver::has_changed().is_ok()`.
    #[must_use]
    pub fn health(&self) -> watch::Receiver<bool> {
        self.health.clone()
    }

    /// Signal the task, wait for it to release the current lease and exit.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
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
) {
    let mut tick = tokio::time::interval(config.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Superseded leases waiting for quiescence before their unspent units go
    // back to the allocator.
    let mut parked: Vec<Arc<LocalLease>> = Vec::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {}
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
        release_quiesced(&allocator, &mut parked, &clock).await;
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
                release_quiesced(&allocator, &mut parked, &clock).await;
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
                let fresh = Arc::new(LocalLease::with_safety_margin(
                    grant,
                    low_water,
                    config.expiry_safety_margin,
                ));
                if let Some(old) = slot.replace(fresh) {
                    parked.push(old);
                }
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
    if let Some(lease) = slot.take() {
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
        if let Err(tollgate_store::AllocateError::Storage(_)) = allocator
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
