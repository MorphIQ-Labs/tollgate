//! The in-memory reference backend.
//!
//! A single mutex over plain maps: writes happen at control-plane frequency,
//! so contention is irrelevant, and the simplicity makes the settlement rules
//! auditable. A real backend (Postgres) must reproduce exactly these rules —
//! the shared correctness suite in `tests/` runs against both.
//!
//! Conservation ledger (checked by `MemoryStore::conservation`): for every
//! account,
//! `initial deposits == balance + Σ active-lease grants + Σ settled usage + Σ settlement losses`.
//! Usage recorded against a still-active lease lives *inside* that lease's
//! grant (the grant was debited whole at acquire), so it only stands alone in
//! the equation once the lease settles. A settlement loss is billing a
//! released/expired lease could not account for (usage that never arrived
//! before settlement).
//!
//! # This backend never forgets
//!
//! Nothing here is ever deleted, and two of the maps therefore grow with what
//! the process has *done* rather than with what it currently holds:
//!
//! - `usage` is the idempotency index and keeps one event per request served,
//!   for the life of the process. It is the unbounded term, and bounding it
//!   needs a dedup-window retention decision rather than a deletion — see the
//!   deferred list in `docs/DESIGN.md`.
//! - `leases` keeps settled records, so it grows with lease rotations. They
//!   are retained because a straggling usage event may still have to be
//!   fenced and rejected against one (see [`UsageSink::ingest`]).
//! - `snapshots` keeps revoked principals as tombstones, deliberately: that is
//!   INVARIANTS.md #15's anti-resurrection watermark, and the population is
//!   bounded by the number of principals rather than by traffic.
//!
//! The *sweep* cost does not follow that growth. Active leases are indexed
//! (see [`crate::leases`]), so reclaim and [`MemoryStore::conservation`] walk
//! the live population, not the historical one (#23). Memory still does grow,
//! so this backend suits development and demos but not soak or load testing;
//! [`MemoryStore::stored_records`] reports the numbers, and the server logs
//! them once per sweep.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, CostUnits, FencingToken, Generation, LeaseGrant, LeaseId, Principal,
    PublishableSnapshot, UsageEvent,
};

use crate::leases::{LeaseRecord, Leases, Settled};
pub use crate::traits::{AccountConfig, Conservation};
use crate::traits::{
    AdminStore, AllocateError, CreateAccountError, GrantPolicy, GrantPolicyError, IngestReport,
    LeaseAllocator, ReclaimBatch, ReclaimedLease, SnapshotPush, SnapshotResolution, SnapshotSource,
    StoreError, StoreHealth, UsageSink,
};

#[derive(Debug)]
struct AccountRecord {
    balance: CostUnits,
    deposited: CostUnits,
    active: bool,
    next_fence: u64,
    /// Usage accepted into the billing ledger.
    usage_recorded: CostUnits,
    /// Billing lost at settlement: usage that had not arrived when a lease
    /// was released/reclaimed. Bounded by construction; reconciliation
    /// watches it.
    settlement_loss: CostUnits,
}

#[derive(Debug)]
struct SnapshotRecord {
    generation: Generation,
    snapshot: Option<PublishableSnapshot>,
}

#[derive(Default)]
struct Inner {
    accounts: HashMap<AccountId, AccountRecord>,
    leases: Leases,
    snapshots: HashMap<Principal, SnapshotRecord>,
    usage: HashMap<tollgate_core::RequestId, UsageEvent>,
    next_lease_id: u128,
}

/// What the in-memory ledger is currently holding.
///
/// This backend never forgets, so the first two numbers climb with traffic for
/// the life of the process while the third returns to the live population.
/// Reported so "grows without bound" is a number an operator can watch rather
/// than a claim in a doc comment (#23).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredRecords {
    /// Usage events retained for idempotency: one per request served, kept
    /// forever. This is the term that grows without bound.
    pub usage_events: usize,
    /// Lease records, active and settled together — grows with rotations.
    pub leases: usize,
    /// Of those, the ones still active: the only records the reclaim sweep and
    /// [`MemoryStore::conservation`] examine. Steady-state, unlike the two
    /// above, which is what makes the sweep's cost independent of them.
    pub active_leases: usize,
}

pub struct MemoryStore {
    inner: Mutex<Inner>,
    policy: GrantPolicy,
    push: broadcast::Sender<SnapshotPush>,
}

impl MemoryStore {
    pub fn new(policy: GrantPolicy) -> Result<Arc<Self>, GrantPolicyError> {
        policy.validate()?;
        let (push, _) = broadcast::channel(256);
        Ok(Arc::new(MemoryStore {
            inner: Mutex::new(Inner::default()),
            policy,
            push,
        }))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // Lock poisoning would mean a panic while holding the ledger; the
        // reference backend treats that as unrecoverable.
        self.inner.lock().expect("memory store lock poisoned")
    }

    // ---- admin / control-plane surface -------------------------------

    /// Test/bootstrap convenience: create a fresh account, panicking on a
    /// duplicate. Production paths use [`AdminStore::create_account`].
    pub fn create_account(&self, config: AccountConfig) {
        self.try_create_account(config)
            .expect("account already exists");
    }

    /// Create an account. Never destructive: an existing account (with its
    /// balance, ledger totals, fencing sequence, and leases) is left
    /// untouched and the caller told (review finding #7).
    pub fn try_create_account(&self, config: AccountConfig) -> Result<(), CreateAccountError> {
        let mut inner = self.lock();
        if inner.accounts.contains_key(&config.account_id) {
            return Err(CreateAccountError::AlreadyExists);
        }
        inner.accounts.insert(
            config.account_id,
            AccountRecord {
                balance: config.initial_balance,
                deposited: config.initial_balance,
                active: config.active,
                next_fence: 1,
                usage_recorded: CostUnits::ZERO,
                settlement_loss: CostUnits::ZERO,
            },
        );
        Ok(())
    }

    /// Add balance to an existing account (top-up).
    pub fn deposit(&self, account: AccountId, units: CostUnits) -> Result<(), AllocateError> {
        let mut inner = self.lock();
        let record = inner
            .accounts
            .get_mut(&account)
            .ok_or(AllocateError::UnknownAccount)?;
        record.balance = record
            .balance
            .checked_add(units)
            .ok_or_else(|| AllocateError::Storage(StoreError("balance overflow".into())))?;
        record.deposited = record
            .deposited
            .checked_add(units)
            .ok_or_else(|| AllocateError::Storage(StoreError("deposit overflow".into())))?;
        Ok(())
    }

    pub fn set_active(&self, account: AccountId, active: bool) {
        if let Some(record) = self.lock().accounts.get_mut(&account) {
            record.active = active;
        }
    }

    /// Bind (or replace) a principal's compiled snapshot and push it to
    /// subscribers. Generation-monotonic: a replayed or reordered publish
    /// carrying an older (or equal) generation is a no-op — matching the
    /// Postgres backend, which enforces the same rule in its upsert (review
    /// finding #5's backend-divergence note).
    pub fn publish_snapshot(&self, principal: Principal, snapshot: PublishableSnapshot) {
        {
            let mut inner = self.lock();
            if let Some(existing) = inner.snapshots.get(&principal)
                && existing.generation >= snapshot.generation
            {
                return;
            }
            inner.snapshots.insert(
                principal,
                SnapshotRecord {
                    generation: snapshot.generation,
                    snapshot: Some(snapshot.clone()),
                },
            );
        }
        self.push_to_subscribers(SnapshotPush {
            principal,
            resolution: SnapshotResolution::Present(snapshot),
        });
    }

    /// Broadcast a control-plane change. No receivers is not a failure — a
    /// pull via `snapshot()` still observes it — but how many instances the
    /// push actually reached is the difference between "propagated in
    /// milliseconds" and "propagated at the next refresh interval", so it is
    /// reported rather than discarded.
    fn push_to_subscribers(&self, push: SnapshotPush) {
        let principal = push.principal;
        let subscribers = self.push.send(push).unwrap_or(0);
        tracing::debug!(
            principal = principal.0,
            subscribers,
            "snapshot pushed to subscribers"
        );
    }

    pub fn remove_snapshot(&self, principal: Principal) {
        let generation = {
            let mut inner = self.lock();
            let Some(record) = inner.snapshots.get_mut(&principal) else {
                return;
            };
            if record.snapshot.is_none() {
                return;
            }
            record.snapshot = None;
            record.generation
        };
        self.push_to_subscribers(SnapshotPush {
            principal,
            resolution: SnapshotResolution::Revoked { generation },
        });
    }

    // ---- reconciliation / test surface -------------------------------

    /// The sums are recomputed from the lease records every time. The index
    /// narrows *which* records are read (#23) and is never the source of the
    /// numbers: this function exists to catch ledger bugs, and one that read a
    /// running total maintained by the same writers that might be wrong could
    /// not catch them.
    #[must_use]
    pub fn conservation(&self, account: AccountId) -> Option<Conservation> {
        let inner = self.lock();
        let record = inner.accounts.get(&account)?;
        let mut active_grants = CostUnits::ZERO;
        let mut active_used = CostUnits::ZERO;
        for lease in inner.leases.active_of(account) {
            active_grants = active_grants
                .checked_add(lease.granted)
                .expect("grant sum overflow");
            active_used = active_used
                .checked_add(lease.used)
                .expect("used sum overflow");
        }
        Some(Conservation {
            deposited: record.deposited,
            balance: record.balance,
            active_lease_grants: active_grants,
            settled_usage: record
                .usage_recorded
                .checked_sub(active_used)
                .expect("active usage never exceeds recorded usage"),
            settlement_loss: record.settlement_loss,
        })
    }

    #[must_use]
    pub fn usage_recorded(&self, account: AccountId) -> CostUnits {
        self.lock()
            .accounts
            .get(&account)
            .map(|a| a.usage_recorded)
            .unwrap_or(CostUnits::ZERO)
    }

    #[must_use]
    pub fn balance(&self, account: AccountId) -> CostUnits {
        self.lock()
            .accounts
            .get(&account)
            .map(|a| a.balance)
            .unwrap_or(CostUnits::ZERO)
    }

    /// Lease records examined by the sweep and by `conservation` since this
    /// store was created. The bound #23 claims is about work, so only a count
    /// of records actually looked at can witness it.
    #[cfg(test)]
    fn leases_examined(&self) -> usize {
        self.lock().leases.examined()
    }

    /// What this backend is currently holding — see [`StoredRecords`], and the
    /// module docs for why two of the three only ever climb.
    #[must_use]
    pub fn stored_records(&self) -> StoredRecords {
        let inner = self.lock();
        StoredRecords {
            usage_events: inner.usage.len(),
            leases: inner.leases.len(),
            active_leases: inner.leases.active_len(),
        }
    }
}

#[async_trait]
impl LeaseAllocator for MemoryStore {
    async fn acquire(
        &self,
        account: AccountId,
        requested: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<LeaseGrant, AllocateError> {
        if ttl <= SignedDuration::ZERO {
            return Err(AllocateError::InvalidTtl);
        }
        let policy = self.policy;
        let ttl = if ttl > policy.max_ttl {
            policy.max_ttl
        } else {
            ttl
        };
        // Compute every fallible value before mutating the in-memory ledger;
        // an overflow must not debit balance without creating a lease.
        let expires_at = now
            .checked_add(ttl)
            .map_err(|e| AllocateError::Storage(StoreError(format!("ttl overflow: {e}"))))?;
        let mut inner = self.lock();
        let next_lease_id = inner
            .next_lease_id
            .checked_add(1)
            .ok_or_else(|| AllocateError::Storage(StoreError("lease id overflow".into())))?;
        let record = inner
            .accounts
            .get_mut(&account)
            .ok_or(AllocateError::UnknownAccount)?;
        if !record.active {
            return Err(AllocateError::AccountInactive);
        }
        let granted = policy
            .grant(requested, record.balance)
            .ok_or(AllocateError::InsufficientBalance)?;
        let next_fence = record
            .next_fence
            .checked_add(1)
            .ok_or_else(|| AllocateError::Storage(StoreError("fencing token overflow".into())))?;
        record.balance = record
            .balance
            .checked_sub(granted)
            .expect("grant never exceeds balance");
        let fencing_token = FencingToken(record.next_fence);
        record.next_fence = next_fence;

        inner.next_lease_id = next_lease_id;
        let lease_id = LeaseId(next_lease_id);
        inner.leases.open(
            lease_id,
            LeaseRecord::opened(account, fencing_token, granted, expires_at),
        );
        Ok(LeaseGrant {
            lease_id,
            account_id: account,
            fencing_token,
            units: granted,
            expires_at,
        })
    }

    async fn release(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        now: Timestamp,
    ) -> Result<(), AllocateError> {
        let mut inner = self.lock();
        let lease = inner
            .leases
            .get(lease_id)
            .ok_or(AllocateError::UnknownLease)?;
        if lease.fencing_token != fencing_token {
            return Err(AllocateError::Fenced);
        }
        // A lease that lapsed before the release arrived settles by expiry
        // reclaim instead; the late releaser is told, not silently absorbed.
        // Releases are accepted through the grace window: a holder shutting
        // down slowly may reach here after `expires_at` but before the sweep
        // settles the lease. Only a settled (or grace-exhausted) lease
        // refuses.
        let release_deadline = lease
            .expires_at
            .checked_add(self.policy.reclaim_grace)
            .unwrap_or(Timestamp::MAX);
        if !lease.is_active() || now >= release_deadline {
            return Err(AllocateError::LeaseNotActive);
        }
        // granted = used + unspent + loss; a claim that doesn't fit is a
        // client accounting bug. The loss is *provisional*: usage events for
        // this lease that were committed but not yet flushed at release time
        // still fit in the gap and convert loss back into billed usage when
        // they arrive (see `ingest`).
        let spent_plus_unspent = lease
            .used
            .checked_add(unspent)
            .ok_or(AllocateError::InvalidRelease)?;
        let loss = lease
            .granted
            .checked_sub(spent_plus_unspent)
            .ok_or(AllocateError::InvalidRelease)?;
        let account_id = lease.account_id;
        assert!(
            inner.leases.settle(lease_id, Settled::Released, unspent),
            "the lease was active a line ago, under this same lock"
        );
        let record = inner
            .accounts
            .get_mut(&account_id)
            .expect("lease account exists");
        record.balance = record
            .balance
            .checked_add(unspent)
            .expect("release credit overflow");
        record.settlement_loss = record
            .settlement_loss
            .checked_add(loss)
            .expect("loss overflow");
        Ok(())
    }

    async fn reclaim_expired_batch(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError> {
        let mut inner = self.lock();
        // Walks the active index, oldest expiry first, and stops at the first
        // lease that is not yet due — so the cost is what is being reclaimed,
        // not what the process has ever leased (#23).
        let expired = inner
            .leases
            .reclaimable(now, self.policy.reclaim_grace, limit.get());
        let mut reclaimed = Vec::with_capacity(expired.len());
        for lease_id in expired {
            let lease = inner.leases.get(lease_id).expect("just listed");
            let credit = lease
                .granted
                .checked_sub(lease.used)
                .expect("usage never exceeds grant");
            let account_id = lease.account_id;
            assert!(
                inner.leases.settle(lease_id, Settled::Expired, credit),
                "reclaimable only yields active leases"
            );
            let record = inner
                .accounts
                .get_mut(&account_id)
                .expect("lease account exists");
            record.balance = record
                .balance
                .checked_add(credit)
                .expect("reclaim credit overflow");
            reclaimed.push(ReclaimedLease {
                lease_id,
                account_id,
                reclaimed: credit,
            });
        }
        // One line per sweep rather than one per batch: a drain calls this
        // until a batch comes back unsaturated, and that last call carries the
        // post-sweep numbers.
        if reclaimed.len() < limit.get() {
            let held = StoredRecords {
                usage_events: inner.usage.len(),
                leases: inner.leases.len(),
                active_leases: inner.leases.active_len(),
            };
            tracing::debug!(
                usage_events = held.usage_events,
                leases = held.leases,
                active_leases = held.active_leases,
                "memory store holdings; usage events and settled leases are never reclaimed"
            );
        }
        ReclaimBatch::try_new(reclaimed, limit)
    }
}

#[async_trait]
impl StoreHealth for MemoryStore {
    async fn ping(&self) -> Result<(), StoreError> {
        Ok(())
    }
}

#[async_trait]
impl AdminStore for MemoryStore {
    async fn create_account(&self, config: AccountConfig) -> Result<(), CreateAccountError> {
        MemoryStore::try_create_account(self, config)
    }

    async fn deposit(&self, account: AccountId, units: CostUnits) -> Result<(), AllocateError> {
        MemoryStore::deposit(self, account, units)
    }

    async fn set_active(&self, account: AccountId, active: bool) -> Result<(), AllocateError> {
        let mut inner = self.lock();
        let record = inner
            .accounts
            .get_mut(&account)
            .ok_or(AllocateError::UnknownAccount)?;
        record.active = active;
        Ok(())
    }

    async fn publish_snapshot(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
    ) -> Result<(), StoreError> {
        MemoryStore::publish_snapshot(self, principal, snapshot);
        Ok(())
    }

    async fn remove_snapshot(&self, principal: Principal) -> Result<(), StoreError> {
        MemoryStore::remove_snapshot(self, principal);
        Ok(())
    }
}

#[async_trait]
impl SnapshotSource for MemoryStore {
    async fn snapshot(&self, principal: Principal) -> Result<SnapshotResolution, StoreError> {
        Ok(match self.lock().snapshots.get(&principal) {
            Some(record) => match &record.snapshot {
                Some(snapshot) => SnapshotResolution::Present(snapshot.clone()),
                None => SnapshotResolution::Revoked {
                    generation: record.generation,
                },
            },
            None => SnapshotResolution::Unknown,
        })
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        self.push.subscribe()
    }
}

#[async_trait]
impl UsageSink for MemoryStore {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, StoreError> {
        let mut inner = self.lock();
        let mut report = IngestReport::default();
        for event in events {
            if inner.usage.contains_key(&event.request_id) {
                report.duplicate += 1;
                continue;
            }
            // Fencing check: the (lease, token, account) triple must name a
            // known lease. Then the conservation check: the event must fit in
            // `granted - used - credited`. For an active lease `credited` is
            // zero (plain capacity check). For a released lease the gap is
            // exactly the provisional settlement loss, so a straggler that
            // was committed before release converts loss into billed usage.
            // For an expired lease the reclaim credited the full remainder —
            // nothing fits, so stragglers stay rejected (they'd double-count).
            let Some(lease) = inner.leases.get_mut(event.lease_id) else {
                report.rejected += 1;
                continue;
            };
            if lease.fencing_token != event.fencing_token || lease.account_id != event.account_id {
                report.rejected += 1;
                continue;
            }
            let was_settled = !lease.is_active();
            let capacity = lease
                .used
                .checked_add(lease.credited())
                .and_then(|committed| lease.granted.checked_sub(committed));
            let fits = matches!(capacity, Some(cap) if event.units <= cap);
            if !fits {
                report.rejected += 1;
                continue;
            }
            lease.used = lease
                .used
                .checked_add(event.units)
                .expect("fits within grant");
            let account_id = lease.account_id;
            let record = inner
                .accounts
                .get_mut(&account_id)
                .expect("lease account exists");
            record.usage_recorded = record
                .usage_recorded
                .checked_add(event.units)
                .expect("usage overflow");
            if was_settled {
                // The units move from provisional loss to billed usage.
                record.settlement_loss = record
                    .settlement_loss
                    .checked_sub(event.units)
                    .expect("straggler fits within recorded loss");
            }
            inner.usage.insert(event.request_id, *event);
            report.accepted += 1;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    const ACCOUNT: AccountId = AccountId(1);
    const TTL: SignedDuration = SignedDuration::from_secs(60);

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    /// Grants exactly what is asked for, so a test can open a precise number
    /// of leases without the default policy's halving getting in the way.
    fn exact_grants() -> GrantPolicy {
        GrantPolicy {
            shrink_divisor: 1,
            min_grant: CostUnits(1),
            max_ttl: SignedDuration::from_secs(300),
            reclaim_grace: SignedDuration::ZERO,
        }
    }

    fn store_with(balance: u64) -> Arc<MemoryStore> {
        let store = MemoryStore::new(exact_grants()).expect("policy is valid");
        store.create_account(AccountConfig {
            account_id: ACCOUNT,
            initial_balance: CostUnits(balance),
            active: true,
        });
        store
    }

    /// Build a store holding `settled` long-settled leases plus one that is
    /// due for reclaim, sweep it, and report how many lease records the sweep
    /// had to look at.
    async fn examined_by_a_sweep_past(settled: usize) -> usize {
        let store = store_with(1_000_000);
        for _ in 0..settled {
            let lease = store
                .acquire(ACCOUNT, CostUnits(1), TTL, t(0))
                .await
                .expect("funded");
            store
                .release(lease.lease_id, lease.fencing_token, lease.units, t(1))
                .await
                .expect("active");
        }
        let due = store
            .acquire(ACCOUNT, CostUnits(1), TTL, t(0))
            .await
            .expect("funded");

        let before = store.leases_examined();
        let batch = store
            .reclaim_expired_batch(t(120), NonZeroUsize::new(64).unwrap())
            .await
            .expect("sweep");
        assert_eq!(batch.len(), 1, "exactly the one expired lease is settled");
        assert_eq!(batch.reclaimed()[0].lease_id, due.lease_id);
        store.leases_examined() - before
    }

    /// #23's whole claim: the sweep's cost follows the live population, not
    /// what the process has ever leased. Two settled populations two orders of
    /// magnitude apart must cost the sweep the same, which is a stronger
    /// statement than any absolute constant — and the one that fails if the
    /// index is removed and the filter goes back over the whole table.
    #[tokio::test]
    async fn sweeping_examines_only_live_leases() {
        let few = examined_by_a_sweep_past(10).await;
        let many = examined_by_a_sweep_past(1_000).await;
        assert_eq!(
            few, many,
            "1,000 settled leases cost the sweep {many} record reads against \
             {few} for 10; the sweep is walking history again"
        );
        assert!(
            many <= 4,
            "one live lease should not cost {many} record reads"
        );
    }

    /// The same for the other scan. `conservation` is the ledger checker, so
    /// it stays a recomputation from the lease records — the index only
    /// narrows which records it reads.
    #[tokio::test]
    async fn conservation_examines_only_live_leases() {
        async fn examined_by_conservation_past(settled: usize) -> usize {
            let store = store_with(1_000_000);
            for _ in 0..settled {
                let lease = store
                    .acquire(ACCOUNT, CostUnits(1), TTL, t(0))
                    .await
                    .expect("funded");
                store
                    .release(lease.lease_id, lease.fencing_token, lease.units, t(1))
                    .await
                    .expect("active");
            }
            let _live = store.acquire(ACCOUNT, CostUnits(5), TTL, t(0)).await;

            let before = store.leases_examined();
            let conservation = store.conservation(ACCOUNT).expect("account exists");
            assert!(
                conservation.holds(),
                "conservation violated: {conservation:?}"
            );
            assert_eq!(conservation.active_lease_grants, CostUnits(5));
            store.leases_examined() - before
        }

        let few = examined_by_conservation_past(10).await;
        let many = examined_by_conservation_past(1_000).await;
        assert_eq!(
            few, many,
            "conservation read {many} records against {few}; it is summing \
             over settled leases again"
        );
    }
}
