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
/// Each parked lease is examined exactly once per pass. Terminal allocator
/// refusals (fenced, already settled) drop the lease — the store has already
/// accounted for it; storage errors keep it parked for retry next tick.
async fn release_quiesced(
    allocator: &Arc<dyn LeaseAllocator>,
    parked: &mut Vec<Arc<LocalLease>>,
    clock: &Arc<dyn Clock>,
) {
    let mut retry = Vec::new();
    for lease in std::mem::take(parked) {
        // The local binding holds what was the vec's sole reference, so a
        // count above one still means an in-flight reservation holds it.
        if Arc::strong_count(&lease) > 1 {
            retry.push(lease);
            continue;
        }
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
            retry.push(lease);
        }
    }
    *parked = retry;
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use jiff::Timestamp;
    use tollgate_core::{FencingToken, LeaseGrant, LeaseId};
    use tollgate_store::{AllocateError, ReclaimedLease, StoreError, SystemClock};

    use super::*;

    /// Records successful releases; refuses scripted lease ids with the
    /// scripted error.
    struct ScriptedAllocator {
        released: Mutex<Vec<LeaseId>>,
        storage_failures: HashSet<LeaseId>,
        fenced: HashSet<LeaseId>,
    }

    impl ScriptedAllocator {
        fn new(
            storage_failures: impl Into<HashSet<LeaseId>>,
            fenced: impl Into<HashSet<LeaseId>>,
        ) -> Arc<Self> {
            Arc::new(Self {
                released: Mutex::new(Vec::new()),
                storage_failures: storage_failures.into(),
                fenced: fenced.into(),
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
            if self.storage_failures.contains(&lease_id) {
                return Err(AllocateError::Storage(StoreError("scripted outage".into())));
            }
            if self.fenced.contains(&lease_id) {
                return Err(AllocateError::Fenced);
            }
            self.released.lock().unwrap().push(lease_id);
            Ok(())
        }

        async fn reclaim_expired(
            &self,
            _now: Timestamp,
        ) -> Result<Vec<ReclaimedLease>, StoreError> {
            unreachable!("release_quiesced never reclaims")
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
        let scripted = ScriptedAllocator::new([LeaseId(1)], []);
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let mut parked = vec![parked_lease(1), parked_lease(2)];

        release_quiesced(&allocator, &mut parked, &clock()).await;

        assert_eq!(scripted.released(), [LeaseId(2)]);
        assert_eq!(parked.len(), 1, "only the failed lease stays parked");
        assert_eq!(parked[0].grant().lease_id, LeaseId(1));
    }

    #[tokio::test]
    async fn unquiesced_lease_is_never_released() {
        let scripted = ScriptedAllocator::new([], []);
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let lease = parked_lease(7);
        let in_flight = Arc::clone(&lease);
        let mut parked = vec![lease];

        release_quiesced(&allocator, &mut parked, &clock()).await;
        assert!(scripted.released().is_empty());
        assert_eq!(parked.len(), 1, "held lease stays parked");

        drop(in_flight);
        release_quiesced(&allocator, &mut parked, &clock()).await;
        assert_eq!(scripted.released(), [LeaseId(7)]);
        assert!(parked.is_empty());
    }

    #[tokio::test]
    async fn terminal_refusal_drops_the_lease() {
        let scripted = ScriptedAllocator::new([], [LeaseId(3)]);
        let allocator: Arc<dyn LeaseAllocator> = Arc::clone(&scripted) as _;
        let mut parked = vec![parked_lease(3)];

        release_quiesced(&allocator, &mut parked, &clock()).await;

        assert!(scripted.released().is_empty());
        assert!(parked.is_empty(), "settled lease is not retried");
    }
}
