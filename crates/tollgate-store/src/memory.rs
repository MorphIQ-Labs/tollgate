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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountSnapshot, CostUnits, FencingToken, LeaseGrant, LeaseId, Principal, UsageEvent,
};

use crate::traits::{
    AdminStore, AllocateError, CreateAccountError, GrantPolicy, IngestReport, LeaseAllocator,
    ReclaimedLease, SnapshotPush, SnapshotSource, StoreError, UsageSink,
};

/// Admin-side inputs when creating an account.
#[derive(Debug, Clone, Copy)]
pub struct AccountConfig {
    pub account_id: AccountId,
    pub initial_balance: CostUnits,
    /// Inactive accounts refuse leases but keep their ledger.
    pub active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseState {
    Active,
    Released,
    Expired,
}

#[derive(Debug)]
struct LeaseRecord {
    account_id: AccountId,
    fencing_token: FencingToken,
    granted: CostUnits,
    /// Usage recorded against this lease so far.
    used: CostUnits,
    /// Units credited back to the account at settlement (release `unspent`,
    /// or the full remainder at expiry reclaim). Zero while active.
    credited: CostUnits,
    expires_at: Timestamp,
    state: LeaseState,
}

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

#[derive(Default)]
struct Inner {
    accounts: HashMap<AccountId, AccountRecord>,
    leases: HashMap<LeaseId, LeaseRecord>,
    snapshots: HashMap<Principal, Arc<AccountSnapshot>>,
    usage: HashMap<tollgate_core::RequestId, UsageEvent>,
    next_lease_id: u128,
}

/// Per-account conservation view for tests and reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Conservation {
    pub deposited: CostUnits,
    pub balance: CostUnits,
    pub active_lease_grants: CostUnits,
    /// Usage billed against leases that have settled (released or expired).
    /// Usage on active leases is inside `active_lease_grants`.
    pub settled_usage: CostUnits,
    pub settlement_loss: CostUnits,
}

impl Conservation {
    /// `deposited == balance + active grants + settled usage + loss`, exactly.
    #[must_use]
    pub fn holds(&self) -> bool {
        let mut sum = self.balance;
        for part in [
            self.active_lease_grants,
            self.settled_usage,
            self.settlement_loss,
        ] {
            match sum.checked_add(part) {
                Some(next) => sum = next,
                None => return false,
            }
        }
        sum == self.deposited
    }
}

pub struct MemoryStore {
    inner: Mutex<Inner>,
    policy: GrantPolicy,
    push: broadcast::Sender<SnapshotPush>,
}

impl MemoryStore {
    #[must_use]
    pub fn new(policy: GrantPolicy) -> Arc<Self> {
        let (push, _) = broadcast::channel(256);
        Arc::new(MemoryStore {
            inner: Mutex::new(Inner::default()),
            policy,
            push,
        })
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
    pub fn publish_snapshot(&self, principal: Principal, snapshot: Arc<AccountSnapshot>) {
        {
            let mut inner = self.lock();
            if let Some(existing) = inner.snapshots.get(&principal)
                && existing.generation >= snapshot.generation
            {
                return;
            }
            inner.snapshots.insert(principal, Arc::clone(&snapshot));
        }
        // No receivers is fine: pull via `snapshot()` still observes it.
        let _ = self.push.send(SnapshotPush {
            principal,
            snapshot,
        });
    }

    pub fn remove_snapshot(&self, principal: Principal) {
        self.lock().snapshots.remove(&principal);
    }

    // ---- reconciliation / test surface -------------------------------

    #[must_use]
    pub fn conservation(&self, account: AccountId) -> Option<Conservation> {
        let inner = self.lock();
        let record = inner.accounts.get(&account)?;
        let mut active_grants = CostUnits::ZERO;
        let mut active_used = CostUnits::ZERO;
        for lease in inner.leases.values() {
            if lease.account_id == account && lease.state == LeaseState::Active {
                active_grants = active_grants
                    .checked_add(lease.granted)
                    .expect("grant sum overflow");
                active_used = active_used
                    .checked_add(lease.used)
                    .expect("used sum overflow");
            }
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
        let policy = self.policy;
        let mut inner = self.lock();
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
        record.balance = record
            .balance
            .checked_sub(granted)
            .expect("grant never exceeds balance");
        let fencing_token = FencingToken(record.next_fence);
        record.next_fence += 1;

        let ttl = if ttl > policy.max_ttl {
            policy.max_ttl
        } else {
            ttl
        };
        inner.next_lease_id += 1;
        let lease_id = LeaseId(inner.next_lease_id);
        let expires_at = now
            .checked_add(ttl)
            .map_err(|e| AllocateError::Storage(StoreError(format!("ttl overflow: {e}"))))?;
        inner.leases.insert(
            lease_id,
            LeaseRecord {
                account_id: account,
                fencing_token,
                granted,
                used: CostUnits::ZERO,
                credited: CostUnits::ZERO,
                expires_at,
                state: LeaseState::Active,
            },
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
            .get_mut(&lease_id)
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
            .unwrap_or(lease.expires_at);
        if lease.state != LeaseState::Active || now >= release_deadline {
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
        lease.state = LeaseState::Released;
        lease.credited = unspent;
        let account_id = lease.account_id;
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

    async fn reclaim_expired(&self, now: Timestamp) -> Result<Vec<ReclaimedLease>, StoreError> {
        let mut inner = self.lock();
        let mut reclaimed = Vec::new();
        let expired: Vec<LeaseId> = inner
            .leases
            .iter()
            .filter(|(_, l)| {
                let reclaim_at = l
                    .expires_at
                    .checked_add(self.policy.reclaim_grace)
                    .unwrap_or(l.expires_at);
                l.state == LeaseState::Active && now >= reclaim_at
            })
            .map(|(id, _)| *id)
            .collect();
        for lease_id in expired {
            let lease = inner.leases.get_mut(&lease_id).expect("just listed");
            lease.state = LeaseState::Expired;
            let credit = lease
                .granted
                .checked_sub(lease.used)
                .expect("usage never exceeds grant");
            lease.credited = credit;
            let account_id = lease.account_id;
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
        Ok(reclaimed)
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
        snapshot: Arc<AccountSnapshot>,
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
    async fn snapshot(
        &self,
        principal: Principal,
    ) -> Result<Option<Arc<AccountSnapshot>>, StoreError> {
        Ok(self.lock().snapshots.get(&principal).cloned())
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
            let Some(lease) = inner.leases.get_mut(&event.lease_id) else {
                report.rejected += 1;
                continue;
            };
            if lease.fencing_token != event.fencing_token || lease.account_id != event.account_id {
                report.rejected += 1;
                continue;
            }
            let was_settled = lease.state != LeaseState::Active;
            let capacity = lease
                .used
                .checked_add(lease.credited)
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
