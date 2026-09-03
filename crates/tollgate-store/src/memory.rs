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
//!   are retained because a straggling usage event must still be matched to
//!   its lease capability and checked against settlement capacity (see
//!   [`UsageSink::ingest`]).
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

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountStatus, CostUnits, FencingToken, Generation, KeyId, LeaseGrant, LeaseId,
    Principal, PublishableSnapshot, UsageEvent, UsageSource,
};

use crate::leases::{LeaseRecord, Leases, Settled};
pub use crate::traits::{AccountConfig, Conservation, StatusChange};
use crate::traits::{
    AdminStore, AllocateError, CreateAccountError, GrantPolicy, GrantPolicyError, IngestReport,
    KeyDirectory, KeyError, KeyRecord, LeaseAllocator, PUSH_CHANNEL_CAPACITY, PublishSnapshotError,
    ReclaimBatch, ReclaimedLease, Revocation, SetStatusError, SnapshotPush, SnapshotResolution,
    SnapshotSource, StoreError, StoreHealth, UsageSink, pushes_exceed_capacity,
};

#[derive(Debug)]
struct AccountRecord {
    balance: CostUnits,
    deposited: CostUnits,
    /// Mirrors `tollgate_accounts.status`. An [`AccountStatus`] rather than a
    /// bool so `Closed` is representable and terminality can be checked here
    /// instead of inferred from snapshots (#51).
    status: AccountStatus,
    next_fence: u64,
    /// Usage accepted into the billing ledger.
    usage_recorded: CostUnits,
    /// Unfunded units billed under elastic enforcement: the second funding
    /// term of the conservation equation. Monotonic, like `deposited` and
    /// `usage_recorded`; only a deposit settles it, and settling it does not
    /// reduce it.
    overage_recorded: CostUnits,
    /// Billing lost at settlement: usage that had not arrived when a lease
    /// was released/reclaimed. Bounded by construction; reconciliation
    /// watches it.
    settlement_loss: CostUnits,
}

/// A principal's stored snapshot state.
///
/// An enum rather than `{ generation, snapshot: Option<_> }` so the generation
/// is stored exactly once (#54). The struct held it twice whenever a snapshot
/// was present — once in the field and once inside the snapshot — with nothing
/// but caller discipline keeping them equal, the same shape PostgreSQL had
/// between its column and its JSONB.
///
/// The field could not simply be deleted: revoking sets the snapshot aside, and
/// its generation is then the only surviving watermark, without which
/// INVARIANTS.md #15's anti-resurrection rule would be unimplementable here. So
/// each state carries the generation in exactly one place instead.
///
/// Variants named for the [`SnapshotResolution`] they map onto, since
/// `SnapshotSource::snapshot` is the only place a reader meets them.
///
/// Note this makes memory's inability to un-revoke structural, where a
/// PostgreSQL tombstone keeps its JSON and could in principle be resurrected.
/// Identical behaviour today; if un-revoking is ever added, this is the backend
/// that changes shape.
#[derive(Debug)]
enum SnapshotRecord {
    /// Live: the generation is the snapshot's own.
    Present(PublishableSnapshot),
    /// Revoked: the snapshot is gone and the watermark is all that remains.
    Revoked(Generation),
}

impl SnapshotRecord {
    /// The record's generation, wherever this state keeps it.
    fn generation(&self) -> Generation {
        match self {
            SnapshotRecord::Present(snapshot) => snapshot.generation,
            SnapshotRecord::Revoked(generation) => *generation,
        }
    }
}

#[derive(Default)]
struct Inner {
    accounts: HashMap<AccountId, AccountRecord>,
    leases: Leases,
    snapshots: HashMap<Principal, SnapshotRecord>,
    usage: HashMap<tollgate_core::RequestId, UsageEvent>,
    /// Credential records, live and retired alike. A revocation sets
    /// `revoked_at` rather than removing the row: the tombstone is what makes
    /// "already retired" distinguishable from "never existed", and a removed
    /// row would let a replayed issuance resurrect the credential.
    keys: HashMap<KeyId, StoredKey>,
    next_lease_id: u128,
}

/// A credential as this backend holds it: the durable record plus its
/// retirement, which is the one mutable field.
#[derive(Debug, Clone)]
struct StoredKey {
    record: KeyRecord,
    revoked_at: Option<Timestamp>,
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
        let (push, _) = broadcast::channel(PUSH_CHANNEL_CAPACITY);
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
                status: config.status,
                next_fence: 1,
                usage_recorded: CostUnits::ZERO,
                overage_recorded: CostUnits::ZERO,
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
        // Both sums are computed before either lands, the rule `acquire`
        // states and `set_account_status` splits into plan/apply. Assigning
        // as they were computed left the ledger permanently short when the
        // second overflowed: conservation keeps `balance <= deposited`, so
        // `deposited` reaches the ceiling first, and a refused top-up on a
        // fully-spent account still credited `balance` (#57). `PostgresStore`
        // moves both columns in one statement, where an overflow aborts it
        // and nothing moves.
        let balance = record
            .balance
            .checked_add(units)
            .ok_or_else(|| AllocateError::Storage(StoreError("balance overflow".into())))?;
        let deposited = record
            .deposited
            .checked_add(units)
            .ok_or_else(|| AllocateError::Storage(StoreError("deposit overflow".into())))?;
        record.balance = balance;
        record.deposited = deposited;
        Ok(())
    }

    /// Bind (or replace) a principal's compiled snapshot and push it to
    /// subscribers. Generation-monotonic: a replayed or reordered publish
    /// carrying an older (or equal) generation is a no-op — matching the
    /// Postgres backend, which enforces the same rule in its upsert (review
    /// finding #5's backend-divergence note).
    pub fn publish_snapshot(&self, principal: Principal, snapshot: PublishableSnapshot) {
        let published = {
            let mut inner = self.lock();
            publish_locked(&mut inner, principal, snapshot)
        };
        if let Some(snapshot) = published {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Present(snapshot),
            });
        }
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
            %principal,
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
            // Already revoked: return without pushing. Dropping this arm would
            // emit a second `Revoked` push for an unchanged record — harmless
            // to a generation-monotonic subscriber, but it is a push for a
            // change that did not happen.
            let SnapshotRecord::Present(snapshot) = record else {
                return;
            };
            // Read the watermark before the snapshot holding it is replaced.
            let generation = snapshot.generation;
            *record = SnapshotRecord::Revoked(generation);
            generation
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
            overage_recorded: record.overage_recorded,
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
        if record.status != AccountStatus::Active {
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
        // Planned, validated, then applied. `ReclaimBatch::try_new` is the
        // last fallible step, and running it after a loop that had already
        // settled leases and credited balances would return `Err` over a
        // ledger that had moved. Unreachable while `reclaimable` respects the
        // limit, but the structure is the defect, and it is one refactor away
        // from being reachable (#57).
        let mut reclaimed = Vec::with_capacity(expired.len());
        let mut balances: HashMap<AccountId, CostUnits> = HashMap::new();
        for &lease_id in &expired {
            let lease = inner.leases.get(lease_id).expect("just listed");
            let credit = lease
                .granted
                .checked_sub(lease.used)
                .expect("usage never exceeds grant");
            let account_id = lease.account_id;
            // Overlaid, because a sweep can reclaim several leases of one
            // account and their credits accumulate.
            let balance = *balances.get(&account_id).unwrap_or_else(|| {
                &inner
                    .accounts
                    .get(&account_id)
                    .expect("lease account exists")
                    .balance
            });
            balances.insert(
                account_id,
                balance
                    .checked_add(credit)
                    .expect("reclaim credit overflow"),
            );
            reclaimed.push(ReclaimedLease {
                lease_id,
                account_id,
                reclaimed: credit,
            });
        }
        let batch = ReclaimBatch::try_new(reclaimed, limit)?;

        for entry in batch.reclaimed() {
            assert!(
                inner
                    .leases
                    .settle(entry.lease_id, Settled::Expired, entry.reclaimed),
                "reclaimable only yields active leases"
            );
        }
        for (account_id, balance) in balances {
            inner
                .accounts
                .get_mut(&account_id)
                .expect("lease account exists")
                .balance = balance;
        }
        // One line per sweep rather than one per batch: a drain calls this
        // until a batch comes back unsaturated, and that last call carries the
        // post-sweep numbers.
        if batch.reclaimed().len() < limit.get() {
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
        Ok(batch)
    }
}

/// Insert a snapshot under a guard the caller already holds, returning what
/// to push once the guard is released.
///
/// Splitting the locked work from the push is what lets a caller hold one
/// guard across its own checks and this insert. `push_to_subscribers` must not
/// run under the lock, and a subscriber must never observe a push for a
/// publication that is still mid-flight — so the value to push comes back out
/// instead of being sent from in here.
///
/// Generation-monotonic: a replayed or reordered publish carrying an older or
/// equal generation is a no-op, and returns `None` because there is nothing to
/// announce.
fn publish_locked(
    inner: &mut Inner,
    principal: Principal,
    snapshot: PublishableSnapshot,
) -> Option<PublishableSnapshot> {
    if let Some(existing) = inner.snapshots.get(&principal)
        && existing.generation() >= snapshot.generation
    {
        return None;
    }
    inner
        .snapshots
        .insert(principal, SnapshotRecord::Present(snapshot.clone()));
    Some(snapshot)
}

/// Plan the re-stamping of every live snapshot of `account` to `status`,
/// under a lock the caller already holds. Mutates nothing: the caller applies
/// the plan only once every fallible step has succeeded.
///
/// **Two-phase on purpose.** Every new generation is computed, and every
/// overflow surfaced, *before* a single record is touched. A loop that
/// mutated as it went would leave an account half-republished behind a `u64`
/// overflow — one ledger status, two different snapshot statuses — which is
/// precisely the divergence #51 exists to abolish, reintroduced in the
/// backend that serves as the executable reference.
///
/// Tombstones are skipped: `snapshot: None` is a revoked principal, and
/// republishing it would resurrect it (INVARIANTS.md #15). Note this backend
/// skips them because the tombstone has *lost* its account attribution, while
/// PostgreSQL skips them by `deleted = FALSE` with the JSON still present —
/// different mechanisms, identical behaviour, which is what the mirrored
/// tests pin.
///
/// Rows already at the target status are left alone, so a repeated call
/// converges and bumps no generation.
///
/// Returned sorted by principal so both backends emit pushes in the same
/// order and a mirrored test need not assert on incidental ordering.
fn plan_republish(
    inner: &Inner,
    account: AccountId,
    status: AccountStatus,
) -> Result<Vec<(Principal, PublishableSnapshot)>, SetStatusError> {
    let mut planned = Vec::new();
    for (principal, record) in &inner.snapshots {
        // Revoked principals are skipped: republishing one would resurrect it,
        // which INVARIANTS.md #15 forbids.
        let SnapshotRecord::Present(snapshot) = record else {
            continue;
        };
        if snapshot.account_id != account || snapshot.status == status {
            continue;
        }
        let generation = snapshot
            .generation
            .0
            .checked_add(1)
            .map(Generation)
            .ok_or_else(|| {
                SetStatusError::Storage(StoreError("snapshot generation overflow".into()))
            })?;
        // The restamped snapshot carries the new generation; the plan does not
        // carry a second copy of it (#54).
        planned.push((*principal, snapshot.restamped(status, generation)));
    }
    planned.sort_unstable_by_key(|(principal, _)| *principal);
    Ok(planned)
}

/// Apply a plan from [`plan_republish`]. Infallible by construction: every
/// value it writes was computed and checked before the first mutation, which
/// is what keeps the ledger and the snapshots from parting company when a
/// generation is about to overflow.
fn apply_republish(
    inner: &mut Inner,
    planned: Vec<(Principal, PublishableSnapshot)>,
) -> Vec<(Principal, PublishableSnapshot)> {
    let mut pushes = Vec::with_capacity(planned.len());
    for (principal, snapshot) in planned {
        inner
            .snapshots
            .insert(principal, SnapshotRecord::Present(snapshot.clone()));
        pushes.push((principal, snapshot));
    }
    pushes
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

    async fn set_account_status(
        &self,
        account: AccountId,
        status: AccountStatus,
    ) -> Result<StatusChange, SetStatusError> {
        // One lock for both records. The inherent `publish_snapshot` takes the
        // lock itself, so it cannot be reused here: the whole point is that no
        // observer sees the ledger moved and the snapshots not.
        let republished = {
            let mut inner = self.lock();
            // Phase 1, read-only: every refusal happens before anything moves.
            let record = inner
                .accounts
                .get(&account)
                .ok_or(SetStatusError::UnknownAccount)?;
            if record.status == AccountStatus::Closed && status != AccountStatus::Closed {
                return Err(SetStatusError::AccountClosed);
            }
            // Phase 2, still read-only: plan every snapshot write, so a
            // generation overflow surfaces here rather than after the ledger
            // has already moved. Writing the ledger first and failing here
            // would leave the account suspended with Active snapshots -- the
            // divergence INVARIANTS.md #22 forbids, in the very backend that
            // serves as its reference.
            let planned = plan_republish(&inner, account, status)?;
            // Phase 3: apply. Nothing below this line can fail.
            inner
                .accounts
                .get_mut(&account)
                .expect("the account was found under this same guard")
                .status = status;
            apply_republish(&mut inner, planned)
        };
        // Outside the guard: `push_to_subscribers` must not run under it, and
        // a subscriber must never observe a push for a transition that is
        // still mid-flight.
        if pushes_exceed_capacity(republished.len()) {
            tracing::warn!(
                %account,
                principals = republished.len(),
                capacity = PUSH_CHANNEL_CAPACITY,
                "status change emitted more pushes than the channel holds; subscribers will resync"
            );
        }
        let planned_pushes = republished;
        let republished = planned_pushes.len();
        for (principal, snapshot) in planned_pushes {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Present(snapshot),
            });
        }
        Ok(StatusChange {
            republished,
            // This backend holds validated snapshots rather than encoded ones,
            // so there is nothing here that can fail to decode.
            unreadable: 0,
        })
    }

    async fn publish_snapshot(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
    ) -> Result<(), PublishSnapshotError> {
        // One guard for the check and the write. Releasing it between them
        // left a window `set_account_status` could run through entirely: a
        // publish that passed the status check, a suspension that restamped
        // every snapshot then existing, and finally the insert of a principal
        // `plan_republish` never saw — because it did not exist yet. The
        // ledger said suspended, that principal's live snapshot said active,
        // and it kept being admitted until someone repeated the transition.
        // That is #51's defect reintroduced in the backend that serves as
        // INVARIANTS.md #22's reference. `PostgresStore` holds `FOR SHARE` on
        // the account row across the same pair, which `set_account_status`'s
        // `FOR UPDATE` serialises against.
        let published = {
            let mut inner = self.lock();
            // The ledger decides an account's status; a publish may carry it
            // but not change it, or the two records this trait just unified
            // could be pulled apart again one principal at a time (#51).
            // An account the ledger does not hold publishes unchanged: this
            // adds no account-existence requirement.
            if let Some(record) = inner.accounts.get(&snapshot.account_id)
                && record.status != snapshot.status
            {
                return Err(PublishSnapshotError::StatusMismatch {
                    ledger: record.status,
                    submitted: snapshot.status,
                });
            }
            publish_locked(&mut inner, principal, snapshot)
        };
        if let Some(snapshot) = published {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Present(snapshot),
            });
        }
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
            Some(SnapshotRecord::Present(snapshot)) => {
                SnapshotResolution::Present(snapshot.clone())
            }
            Some(SnapshotRecord::Revoked(generation)) => SnapshotResolution::Revoked {
                generation: *generation,
            },
            None => SnapshotResolution::Unknown,
        })
    }

    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush> {
        self.push.subscribe()
    }

    /// Tombstones included: a revoked principal is one an instance must keep
    /// tracking so it keeps *knowing* about the revocation. Dropping it from
    /// the catalogue would make it indistinguishable from a principal that
    /// never existed, which is the resurrection INVARIANTS.md #15 forbids.
    async fn principals(&self) -> Result<Option<Vec<Principal>>, StoreError> {
        Ok(Some(self.lock().snapshots.keys().copied().collect()))
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

        // Planned first, applied second. The overage branch can fail the whole
        // batch on an accounting overflow, and returning from the middle of an
        // applying loop left every earlier event of the batch committed while
        // the caller was told the batch failed (#57): `UsageWriter` reads a
        // `StoreError` as an outage and retries, the replay counts the applied
        // events as duplicates, and retry accounting then describes a batch
        // that partially succeeded as a total failure. `PostgresStore` runs
        // the batch in one transaction and rolls it back, so this is the shape
        // that makes the two backends agree (INVARIANTS.md #7).
        //
        // The plan carries overlays rather than reading `inner` twice, because
        // events in one batch see each other: two charges against the same
        // lease consume its capacity in order, and a request id repeated
        // inside a batch is a duplicate of the earlier one.
        let mut report = IngestReport::default();
        let mut accepted: Vec<UsageEvent> = Vec::new();
        let mut lease_used: HashMap<LeaseId, CostUnits> = HashMap::new();
        let mut usage_recorded: HashMap<AccountId, CostUnits> = HashMap::new();
        let mut overage_recorded: HashMap<AccountId, CostUnits> = HashMap::new();
        let mut settlement_loss: HashMap<AccountId, CostUnits> = HashMap::new();
        let mut planned: HashSet<tollgate_core::RequestId> = HashSet::new();

        for event in events {
            if inner.usage.contains_key(&event.request_id) || planned.contains(&event.request_id) {
                report.duplicate += 1;
                continue;
            }
            // Overage has no lease, so it has neither a capability to verify
            // nor lease capacity to fit inside. It is recorded against the
            // account directly, moving two columns at once: `usage_recorded`,
            // so it is billed, and `overage_recorded`, so it is funded. Moving
            // only the first would break conservation by exactly these units.
            //
            // Deliberately not conditioned on the account's current
            // enforcement mode. The ledger does not carry the mode the
            // request was admitted under, and an account switched back to
            // `Strict` between admission and flush would otherwise have this
            // charge discarded — fail-open on accounting, to protect a
            // fail-closed decision that was already made correctly. The work
            // happened; it gets billed.
            let UsageSource::Leased {
                lease_id,
                fencing_token,
            } = event.source
            else {
                let Some(record) = inner.accounts.get(&event.account_id) else {
                    report.rejected += 1;
                    continue;
                };
                let recorded = *usage_recorded
                    .get(&event.account_id)
                    .unwrap_or(&record.usage_recorded);
                let overage = *overage_recorded
                    .get(&event.account_id)
                    .unwrap_or(&record.overage_recorded);
                // Both terms must move or neither does, or the equation is
                // left open (INVARIANTS.md #11). A total that cannot be
                // represented is corruption of a monotonic column rather than
                // a problem with this event, so it is surfaced as a store
                // error and the whole batch fails — and because nothing has
                // been applied yet, the batch fails whole.
                let (Some(next_recorded), Some(next_overage)) = (
                    recorded.checked_add(event.units),
                    overage.checked_add(event.units),
                ) else {
                    return Err(StoreError(format!(
                        "overage accounting overflow for account {:#034x}: recorded usage {} \
                         and overage {} cannot absorb {}",
                        event.account_id.0, recorded, overage, event.units
                    )));
                };
                usage_recorded.insert(event.account_id, next_recorded);
                overage_recorded.insert(event.account_id, next_overage);
                planned.insert(event.request_id);
                accepted.push(*event);
                report.accepted += 1;
                continue;
            };
            // Capability check: the (lease, token, account) triple must name
            // a known lease. Then the conservation check: the event must fit in
            // `granted - used - credited`. For an active lease `credited` is
            // zero (plain capacity check). For a released lease the gap is
            // exactly the provisional settlement loss, so a straggler that
            // was committed before release converts loss into billed usage.
            // For an expired lease the reclaim credited the full remainder —
            // nothing fits, so stragglers stay rejected (they'd double-count).
            let Some(lease) = inner.leases.get(lease_id) else {
                report.rejected += 1;
                continue;
            };
            if lease.fencing_token != fencing_token || lease.account_id != event.account_id {
                report.rejected += 1;
                continue;
            }
            let was_settled = !lease.is_active();
            let used = *lease_used.get(&lease_id).unwrap_or(&lease.used);
            let capacity = used
                .checked_add(lease.credited())
                .and_then(|committed| lease.granted.checked_sub(committed));
            let fits = matches!(capacity, Some(cap) if event.units <= cap);
            if !fits {
                report.rejected += 1;
                continue;
            }
            let account_id = lease.account_id;
            let record = inner
                .accounts
                .get(&account_id)
                .expect("lease account exists");
            let recorded = *usage_recorded
                .get(&account_id)
                .unwrap_or(&record.usage_recorded);
            lease_used.insert(
                lease_id,
                used.checked_add(event.units).expect("fits within grant"),
            );
            usage_recorded.insert(
                account_id,
                recorded.checked_add(event.units).expect("usage overflow"),
            );
            if was_settled {
                // The units move from provisional loss to billed usage.
                let loss = *settlement_loss
                    .get(&account_id)
                    .unwrap_or(&record.settlement_loss);
                settlement_loss.insert(
                    account_id,
                    loss.checked_sub(event.units)
                        .expect("straggler fits within recorded loss"),
                );
            }
            planned.insert(event.request_id);
            accepted.push(*event);
            report.accepted += 1;
        }

        // Apply. Every value here was computed above, so nothing in this block
        // can fail and leave the ledger half-moved.
        for (lease_id, used) in lease_used {
            inner
                .leases
                .get_mut(lease_id)
                .expect("planned against a lease that exists")
                .used = used;
        }
        for (account_id, value) in usage_recorded {
            inner
                .accounts
                .get_mut(&account_id)
                .expect("planned against an account that exists")
                .usage_recorded = value;
        }
        for (account_id, value) in overage_recorded {
            inner
                .accounts
                .get_mut(&account_id)
                .expect("planned against an account that exists")
                .overage_recorded = value;
        }
        for (account_id, value) in settlement_loss {
            inner
                .accounts
                .get_mut(&account_id)
                .expect("planned against an account that exists")
                .settlement_loss = value;
        }
        for event in accepted {
            inner.usage.insert(event.request_id, event);
        }
        Ok(report)
    }
}

#[async_trait]
impl KeyDirectory for MemoryStore {
    async fn insert_key(&self, record: KeyRecord) -> Result<(), KeyError> {
        let mut inner = self.lock();
        if !inner.accounts.contains_key(&record.account_id) {
            return Err(KeyError::UnknownAccount);
        }
        // Never destructive, for the reason `create_account` is not: an
        // overwrite would retire a live credential without saying so, and the
        // digest it replaced is unrecoverable.
        if inner.keys.contains_key(&record.key_id) {
            return Err(KeyError::AlreadyExists);
        }
        // Two credentials cannot share a principal. That value is the identity
        // admission decides with, so a collision would make one account's
        // revocation withdraw another's credential. At 128 bits of HMAC output
        // it means secret reuse or corruption rather than chance, and the
        // stored backend enforces it with a UNIQUE index — this is the
        // reference implementation of the same rule.
        if inner
            .keys
            .values()
            .any(|stored| stored.record.principal == record.principal)
        {
            return Err(KeyError::AlreadyExists);
        }
        inner.keys.insert(
            record.key_id,
            StoredKey {
                record,
                revoked_at: None,
            },
        );
        Ok(())
    }

    async fn revoke_key(&self, key_id: KeyId, now: Timestamp) -> Result<Revocation, KeyError> {
        let mut inner = self.lock();
        let Some(stored) = inner.keys.get_mut(&key_id) else {
            return Err(KeyError::UnknownKey);
        };
        if stored.revoked_at.is_some() {
            return Ok(Revocation::AlreadyRetired);
        }
        stored.revoked_at = Some(now);
        Ok(Revocation::Retired)
    }

    async fn active_keys(&self, now: Timestamp) -> Result<Vec<KeyRecord>, StoreError> {
        let inner = self.lock();
        let mut active: Vec<KeyRecord> = inner
            .keys
            .values()
            .filter(|stored| stored.revoked_at.is_none())
            // Expiry is decided here rather than by each reader, so every
            // backend answers "active" the same way and a projection cannot
            // disagree with the ledger about which credentials are live.
            .filter(|stored| {
                stored
                    .record
                    .not_after
                    .is_none_or(|not_after| now < not_after)
            })
            .map(|stored| stored.record.clone())
            .collect();
        // Deterministic order: the projection is rebuilt from this, and a
        // backend whose output order wanders makes two instances' tables
        // differ in a way no test would reproduce.
        active.sort_unstable_by_key(|record| record.key_id);
        Ok(active)
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
            status: AccountStatus::Active,
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
