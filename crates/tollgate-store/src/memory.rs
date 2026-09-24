//! The in-memory reference backend.
//!
//! A single mutex over plain maps: writes happen at control-plane frequency,
//! so contention is irrelevant, and the simplicity makes the settlement rules
//! auditable. A real backend (Postgres) must reproduce exactly these rules —
//! the shared correctness suite in `tests/` runs against both.
//!
//! [`MemoryStore::conservation`] returns the per-account ledger checked by
//! [`Conservation::holds`], including overage funding and expired allowances.
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
//! in the private `leases` module, so reclaim and [`MemoryStore::conservation`] walk
//! the live population, not the historical one (#23). Memory still does grow,
//! so this backend suits development and demos but not soak or load testing;
//! [`MemoryStore::stored_records`] reports the numbers, and the server logs
//! them once per sweep.

use crate::{AccountView, AdminReceipt, AdminState};

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountStatus, BudgetSchedule, BudgetView, CapacityClass, CostUnits, FencingToken,
    Generation, KeyId, LeaseGrant, LeaseId, Principal, PublishableSnapshot, UsageEvent,
    UsageSource,
};

use crate::leases::{LeaseRecord, Leases, Settled};
pub use crate::traits::{AccountConfig, Conservation, StatusChange};
use crate::traits::{
    AdminStore, AllocateError, Allocation, BudgetError, CreateAccountError, GrantPolicy,
    GrantPolicyError, IngestError, IngestReport, KeyDirectory, KeyError, KeyRecord, KeySummary,
    LeaseAllocator, PUSH_CHANNEL_CAPACITY, PublishSnapshotError, ReclaimBatch, ReclaimedLease,
    Revocation, RolledAccount, RolloverBatch, SetStatusError, SnapshotPush, SnapshotResolution,
    SnapshotSource, StoreError, StoreHealth, UsageSink, pushes_exceed_capacity,
};

/// An account's balance, split by what expires and what does not (#97).
///
/// One number could not carry this. At a period boundary the allowance's
/// remainder is expired and manual credits are kept, and a single balance can
/// only either expire the credits with it or resurrect allowance units that
/// were already spent — there is no arithmetic on one counter that separates
/// "unspent allowance" from "unspent top-up" after the fact.
///
/// Spend order is allowance first. A credit bought or granted out of band
/// should outlive the monthly allowance sitting beside it, so the units with
/// an expiry date are the ones consumed first.
#[derive(Debug, Clone, Copy, Default)]
struct Balance {
    /// Unspent units from the current period's allowance. Expired whole at
    /// the next boundary under `Rollover::None`.
    allowance: CostUnits,
    /// Unspent units from manual deposits. Never expired by a rollover.
    topup: CostUnits,
}

impl Balance {
    /// What the account can spend, and what every reader outside this module
    /// means by "balance".
    fn total(self) -> CostUnits {
        self.allowance
            .checked_add(self.topup)
            .expect("a balance that was funded in halves fits the sum it came from")
    }

    /// Take `units`, allowance first, reporting the split so the lease can
    /// give each half back to the bucket it came from.
    ///
    /// Returning the split rather than crediting allowance-first on release is
    /// what stops a top-up being silently expired: a lease funded entirely
    /// from credits, released after a boundary, would otherwise return its
    /// units to the allowance bucket and have them expired at the next one.
    fn take(&mut self, units: CostUnits) -> Option<Drawn> {
        let from_allowance = self.allowance.min(units);
        let from_topup = units.checked_sub(from_allowance)?;
        self.allowance = self.allowance.checked_sub(from_allowance)?;
        self.topup = self.topup.checked_sub(from_topup)?;
        Some(Drawn {
            from_allowance,
            from_topup,
        })
    }
}

/// Which buckets a grant drew from, carried by the lease so settlement can
/// return each half to where it came from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Drawn {
    pub(crate) from_allowance: CostUnits,
    pub(crate) from_topup: CostUnits,
}

#[derive(Debug)]
struct AccountRecord {
    balance: Balance,
    deposited: CostUnits,
    /// The account's periodic allowance, if it has one. `None` keeps the
    /// manual-deposit behaviour the ledger has always had, and a rollover pass
    /// skips the account entirely — which is why this is an `Option` rather
    /// than a schedule with a zero allowance, a very different thing.
    schedule: Option<BudgetSchedule>,
    /// The first instant of the period this account is currently in, and the
    /// marker rollover is idempotent against: a boundary is crossed exactly
    /// once because the update that crosses it is conditional on this value
    /// still being the old one.
    period_start: Timestamp,
    /// Units funded but never spendable again, because the period that funded
    /// them closed. Monotonic.
    expired: CostUnits,
    /// Mirrors `tollgate_accounts.status`. An [`AccountStatus`] rather than a
    /// bool so `Closed` is representable and terminality can be checked here
    /// instead of inferred from snapshots (#51).
    status: AccountStatus,
    /// Mirrors `tollgate_accounts.capacity_class`. The account-owned fact a
    /// snapshot's `capacity_class` is a copy of, and the reason a publish
    /// carrying a different one is refused: two writers for one fact is the
    /// divergence #51 abolished for status (#99).
    capacity_class: CapacityClass,
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

impl AccountRecord {
    /// What this account could still spend, for a snapshot's budget view
    /// (#97).
    ///
    /// Balance *plus* the unspent remainder of every active lease, because
    /// units out on lease are still the account's — an instance holding a
    /// 500-unit lease has not lost those units, and a figure that excluded
    /// them would tell a customer their quota had halved the moment a lease
    /// was taken.
    ///
    /// Derived from the account row alone, with no walk over the leases. The
    /// conservation equation is what makes that possible: `balance + active
    /// grants` is `funded - consumed`, so what an account can still spend is
    /// everything it was funded with minus everything it has consumed or lost.
    /// A lease scan would be O(the account's leases) at every publication and
    /// would answer the same number.
    ///
    /// The `expect`s are the split `PostgresStore::conservation` documents:
    /// here the counters are maintained by one process under one lock, so an
    /// underflow is unrepresentable rather than corruption a caller could
    /// have caused. The PostgreSQL backend reads a database it does not
    /// exclusively own and reports the same condition as an error.
    fn budget_view(&self) -> BudgetView {
        let funded = self
            .deposited
            .checked_add(self.overage_recorded)
            .expect("an account cannot be funded past what it was funded with");
        let consumed = self
            .usage_recorded
            .checked_add(self.settlement_loss)
            .and_then(|spent| spent.checked_add(self.expired))
            .expect("consumption cannot exceed the funding it came from");
        BudgetView {
            balance_at_publish: funded
                .checked_sub(consumed)
                .expect("conservation keeps consumption within funding"),
            // The period the account is *in*, which is what it can spend
            // against. An account whose schedule was set but whose first
            // rollover has not run yet is still in its previous period, and
            // says so, until the next sweep tick moves it.
            period_end: self
                .schedule
                .map(|schedule| schedule.period.end_after(self.period_start)),
        }
    }
}

/// Whether the period that funded a lease had already closed by the time the
/// lease settles.
///
/// Both settlement and consolidation use this decision so the credited bucket
/// and the replacement grant's floor agree about which units are spendable.
fn allowance_lapsed(record: &AccountRecord, period_start: Timestamp) -> bool {
    period_start < record.period_start
}

/// Return a settled lease's unspent units to the account — expiring the
/// allowance half, if the period that funded it has closed (#97).
///
/// This is the whole of the "drain then expire" decision. An active lease at a
/// period boundary keeps serving to its own TTL, so there is no admission gap
/// and no clock read on the request path; the boundary shows up here instead,
/// when the lease finally settles. Unspent units funded by an allowance that
/// no longer exists cannot go back to a balance — that would resurrect an
/// expired allowance, and the account would carry units its schedule says it
/// should not have.
///
/// The split is charged in the order the account spends, allowance first, so
/// the top-up half is what survives a partly-spent lease. Only that half is
/// unconditional: a top-up never expires, boundary or not, which is what makes
/// "manual credits persist across rollover" true even for a lease that
/// straddles one.
///
/// Usage is untouched either way, which is what makes a straggler across the
/// boundary bill against the period the lease was granted in.
fn credit_settlement(
    record: &mut AccountRecord,
    funding: Drawn,
    period_start: Timestamp,
    unspent: CostUnits,
) {
    let to_topup = funding.from_topup.min(unspent);
    let to_allowance = unspent
        .checked_sub(to_topup)
        .expect("the top-up half never exceeds the grant it was drawn from");
    record.balance.topup = record
        .balance
        .topup
        .checked_add(to_topup)
        .expect("settlement credit overflow");
    // The only thing the boundary decides.
    let bucket = if allowance_lapsed(record, period_start) {
        &mut record.expired
    } else {
        &mut record.balance.allowance
    };
    *bucket = bucket
        .checked_add(to_allowance)
        .expect("settlement credit overflow");
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
    credential_activity: HashMap<KeyId, Timestamp>,
    /// Credential records, live and retired alike. A revocation sets
    /// `revoked_at` rather than removing the row: the tombstone is what makes
    /// "already retired" distinguishable from "never existed", and a removed
    /// row would let a replayed issuance resurrect the credential.
    keys: HashMap<KeyId, StoredKey>,
    /// Ordered unrevoked ids avoid scanning retired history on every page.
    unrevoked_keys: std::collections::BTreeSet<KeyId>,
    /// Retained through retirement, like the durable principal UNIQUE index.
    key_principals: HashMap<Principal, KeyId>,
    credential_revision: u64,
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
    /// Credentials with recorded attributable commitments, including retired keys.
    pub credential_activity: usize,
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

    /// The expiry a grant issued now would carry, clamping the request to the
    /// policy's `max_ttl`.
    ///
    /// Every fallible value is computed before the ledger moves: an overflow
    /// must not debit a balance without creating a lease, nor settle a lease
    /// without replacing it.
    fn grant_expiry(
        &self,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<Timestamp, AllocateError> {
        if ttl <= SignedDuration::ZERO {
            return Err(AllocateError::InvalidTtl);
        }
        let ttl = ttl.min(self.policy.max_ttl);
        now.checked_add(ttl)
            .map_err(|e| AllocateError::Storage(StoreError(format!("ttl overflow: {e}"))))
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
        self.create_account_audited(config)
            .map(|receipt| receipt.outcome)
    }

    fn create_account_audited(
        &self,
        config: AccountConfig,
    ) -> Result<AdminReceipt<()>, CreateAccountError> {
        let mut inner = self.lock();
        if inner.accounts.contains_key(&config.account_id) {
            return Err(CreateAccountError::AlreadyExists);
        }
        inner.accounts.insert(
            config.account_id,
            AccountRecord {
                // The opening balance is a top-up, not an allowance: it was
                // deposited by whoever created the account, and nothing has
                // scheduled it to expire. A schedule set later starts its
                // first period at the next rollover.
                balance: Balance {
                    allowance: CostUnits::ZERO,
                    topup: config.initial_balance,
                },
                deposited: config.initial_balance,
                schedule: None,
                period_start: Timestamp::UNIX_EPOCH,
                expired: CostUnits::ZERO,
                status: config.status,
                capacity_class: config.capacity_class,
                next_fence: 1,
                usage_recorded: CostUnits::ZERO,
                overage_recorded: CostUnits::ZERO,
                settlement_loss: CostUnits::ZERO,
            },
        );
        Ok(AdminReceipt::new(
            (),
            AdminState::Absent,
            AdminState::AccountCreated {
                initial_balance: config.initial_balance,
                status: config.status,
                capacity_class: config.capacity_class,
            },
        ))
    }

    /// Add balance to an existing account (top-up).
    pub fn deposit(&self, account: AccountId, units: CostUnits) -> Result<(), AllocateError> {
        self.deposit_audited(account, units)
            .map(|receipt| receipt.outcome)
    }

    fn deposit_audited(
        &self,
        account: AccountId,
        units: CostUnits,
    ) -> Result<AdminReceipt<()>, AllocateError> {
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
        // A manual deposit is a top-up: it survives a period boundary, which
        // is the documented default (#97). An allowance only ever arrives
        // through `roll_period`.
        let topup = record
            .balance
            .topup
            .checked_add(units)
            .ok_or_else(|| AllocateError::Storage(StoreError("balance overflow".into())))?;
        let deposited = record
            .deposited
            .checked_add(units)
            .ok_or_else(|| AllocateError::Storage(StoreError("deposit overflow".into())))?;
        let before = AdminState::Funding {
            topup: record.balance.topup,
            deposited: record.deposited,
        };
        record.balance.topup = topup;
        record.deposited = deposited;
        Ok(AdminReceipt::new(
            (),
            before,
            AdminState::Funding { topup, deposited },
        ))
    }

    /// Bind (or replace) a principal's compiled snapshot and push it to
    /// subscribers. Generation-monotonic: a replayed or reordered publish
    /// carrying an older (or equal) generation is a no-op — matching the
    /// Postgres backend, which enforces the same rule in its upsert (review
    /// finding #5's backend-divergence note).
    pub fn publish_snapshot(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
    ) -> Result<(), PublishSnapshotError> {
        let published = {
            let mut inner = self.lock();
            publish_locked(&mut inner, principal, snapshot)?
        };
        if let Some(snapshot) = published {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Present(snapshot),
            });
        }
        Ok(())
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
        self.remove_snapshot_audited(principal);
    }

    fn remove_snapshot_audited(&self, principal: Principal) -> AdminReceipt<()> {
        let receipt = {
            let mut inner = self.lock();
            let before = snapshot_audit(inner.snapshots.get(&principal));
            if let Some(SnapshotRecord::Present(snapshot)) = inner.snapshots.get(&principal) {
                let generation = snapshot.generation;
                inner
                    .snapshots
                    .insert(principal, SnapshotRecord::Revoked(generation));
            }
            AdminReceipt::new((), before, snapshot_audit(inner.snapshots.get(&principal)))
        };
        if receipt.before != receipt.after
            && let AdminState::Snapshot { generation, .. } = receipt.after
        {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Revoked { generation },
            });
        }
        receipt
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
            balance: record.balance.total(),
            active_lease_grants: active_grants,
            settled_usage: record
                .usage_recorded
                .checked_sub(active_used)
                .expect("active usage never exceeds recorded usage"),
            settlement_loss: record.settlement_loss,
            expired: record.expired,
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

    /// The event this store settled for `request_id`, if it settled one.
    ///
    /// The reference implementation keeps whole events — the idempotency map
    /// is keyed by request and holds the value — so this reads back what was
    /// actually billed rather than a projection of it. `PostgresStore` keeps
    /// only the columns it needs and has no equivalent, which is why a
    /// backend-parity assertion on a stored field reads that backend's column
    /// directly instead.
    ///
    /// Exists for inspection and tests, beside [`usage_recorded`]. It is not
    /// part of `UsageSink`: no request-path code reads settled events back.
    ///
    /// [`usage_recorded`]: MemoryStore::usage_recorded
    #[must_use]
    pub fn settled_event(&self, request_id: tollgate_core::RequestId) -> Option<UsageEvent> {
        self.lock().usage.get(&request_id).copied()
    }

    #[must_use]
    pub fn balance(&self, account: AccountId) -> CostUnits {
        self.lock()
            .accounts
            .get(&account)
            .map(|a| a.balance.total())
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
            credential_activity: inner.credential_activity.len(),
            usage_events: inner.usage.len(),
            leases: inner.leases.len(),
            active_leases: inner.leases.active_len(),
        }
    }
}

/// The part of a settled lease's `unspent` that returns to *spendable*
/// balance.
///
/// Not always all of it: the allowance half of a lease funded by a period that
/// has since closed expires instead of coming back (#97, and
/// [`credit_settlement`], which applies the same rule). A caller sizing a
/// grant against the credit it is about to make must ask this rather than
/// assume `unspent`.
fn spendable_credit(
    record: &AccountRecord,
    funding: Drawn,
    period_start: Timestamp,
    unspent: CostUnits,
) -> CostUnits {
    if allowance_lapsed(record, period_start) {
        funding.from_topup.min(unspent)
    } else {
        unspent
    }
}

/// A validated release, decided before anything moves.
///
/// The reference backend holds one mutex where the SQL backend holds a
/// transaction, so it has nothing to roll back: every way an operation can
/// refuse is established here, and applying is infallible. That is what makes
/// a consolidation all-or-nothing in both backends for the same reason rather
/// than by coincidence.
struct ReleasePlan {
    account_id: AccountId,
    funding: Drawn,
    period_start: Timestamp,
    unspent: CostUnits,
    loss: CostUnits,
}

/// A validated grant, decided before anything moves. See [`ReleasePlan`].
struct GrantPlan {
    granted: CostUnits,
    fencing_token: FencingToken,
    next_fence: u64,
    next_lease_id: u128,
}

fn plan_release(
    inner: &Inner,
    policy: &GrantPolicy,
    lease_id: LeaseId,
    fencing_token: FencingToken,
    unspent: CostUnits,
    now: Timestamp,
) -> Result<ReleasePlan, AllocateError> {
    let lease = inner
        .leases
        .get(lease_id)
        .ok_or(AllocateError::UnknownLease)?;
    if lease.fencing_token != fencing_token {
        return Err(AllocateError::Fenced);
    }
    // A lease that lapsed before the release arrived settles by expiry
    // reclaim instead; the late releaser is told, not silently absorbed.
    // Releases are accepted through the grace window: a holder shutting down
    // slowly may reach here after `expires_at` but before the sweep settles
    // the lease. Only a settled (or grace-exhausted) lease refuses.
    if !lease.is_active()
        || policy
            .reclaim_cutoff(now)
            .is_some_and(|cutoff| lease.expires_at <= cutoff)
    {
        return Err(AllocateError::LeaseNotActive);
    }
    // granted = used + unspent + loss; a claim that doesn't fit is a client
    // accounting bug. The loss is *provisional*: usage events for this lease
    // that were committed but not yet flushed at release time still fit in the
    // gap and convert loss back into billed usage when they arrive (see
    // `ingest`).
    let spent_plus_unspent = lease
        .used
        .checked_add(unspent)
        .ok_or(AllocateError::InvalidRelease)?;
    let loss = lease
        .granted
        .checked_sub(spent_plus_unspent)
        .ok_or(AllocateError::InvalidRelease)?;
    Ok(ReleasePlan {
        account_id: lease.account_id,
        funding: lease.funding,
        period_start: lease.period_start,
        unspent,
        loss,
    })
}

fn apply_release(inner: &mut Inner, lease_id: LeaseId, plan: &ReleasePlan) {
    assert!(
        inner
            .leases
            .settle(lease_id, Settled::Released, plan.unspent),
        "the plan validated this lease as active under this same lock"
    );
    let record = inner
        .accounts
        .get_mut(&plan.account_id)
        .expect("lease account exists");
    credit_settlement(record, plan.funding, plan.period_start, plan.unspent);
    record.settlement_loss = record
        .settlement_loss
        .checked_add(plan.loss)
        .expect("loss overflow");
}

/// Size one grant against `account`'s balance.
///
/// `incoming` is units this same operation is about to credit to this same
/// account — a consolidation's returned tail. It counts twice, and
/// deliberately: as part of the balance the policy sizes against, and as the
/// floor that answer may not fall below, so the exchange can grow a holding or
/// leave it alone but never shrink it (see [`LeaseAllocator::consolidate`]).
/// A plain acquire credits nothing and passes zero, leaving the policy's
/// answer exactly as it was.
fn plan_grant(
    inner: &Inner,
    policy: &GrantPolicy,
    account: AccountId,
    requested: CostUnits,
    incoming: CostUnits,
    needed: CostUnits,
    attest_refusal: bool,
) -> Result<GrantPlan, AllocateError> {
    let next_lease_id = inner
        .next_lease_id
        .checked_add(1)
        .ok_or_else(|| AllocateError::Storage(StoreError("lease id overflow".into())))?;
    let record = inner
        .accounts
        .get(&account)
        .ok_or(AllocateError::UnknownAccount)?;
    if record.status != AccountStatus::Active {
        return Err(AllocateError::AccountInactive);
    }
    let balance = record
        .balance
        .total()
        .checked_add(incoming)
        .ok_or_else(|| AllocateError::Storage(StoreError("account balance overflow".into())))?;
    // The floor and demand are applied to the policy's answer, never to the
    // balance test: an account with nothing left still refuses, and both are
    // capped by the balance `incoming` is part of, so they can only re-select
    // capacity the account demonstrably has.
    let granted = policy
        .consolidation_grant(requested, balance, incoming, needed)
        .ok_or_else(|| {
            // A refused consolidation's settlement never applies. Attest
            // only where that settlement would not itself have removed
            // funding, the same rule the SQL backend needs because its
            // transaction has already applied the settlement it rolls back.
            if requested.is_zero() || !attest_refusal {
                return AllocateError::InsufficientBalance;
            }
            funding_refusal(record.budget_view().shortfall())
        })?;
    let next_fence = record
        .next_fence
        .checked_add(1)
        .ok_or_else(|| AllocateError::Storage(StoreError("fencing token overflow".into())))?;
    Ok(GrantPlan {
        granted,
        fencing_token: FencingToken(record.next_fence),
        next_fence,
        next_lease_id,
    })
}

/// The refusal a ledger with nothing allocatable attests: exhaustion when no
/// funding remains, otherwise how much remains in other leases.
fn funding_refusal(evidence: tollgate_core::BalanceShortfall) -> AllocateError {
    match evidence.exhaustion() {
        Some(exhausted) => AllocateError::BalanceExhausted(exhausted),
        None => AllocateError::BalanceInsufficient(evidence),
    }
}

/// Read after every half of the exchange has applied: the evidence must
/// describe the ledger the grant committed into, settlement loss included.
fn allocation(inner: &Inner, grant: LeaseGrant) -> Allocation {
    let funding = inner
        .accounts
        .get(&grant.account_id)
        .expect("a granted account exists")
        .budget_view()
        .shortfall();
    Allocation {
        grant,
        funding: Some(funding),
    }
}

fn apply_grant(
    inner: &mut Inner,
    account: AccountId,
    plan: GrantPlan,
    expires_at: Timestamp,
) -> LeaseGrant {
    let record = inner
        .accounts
        .get_mut(&account)
        .expect("the plan validated this account under this same lock");
    // Allowance first, and the split travels with the lease so settlement
    // returns each half where it came from (#97).
    let drawn = record
        .balance
        .take(plan.granted)
        .expect("grant never exceeds the balance the plan sized it against");
    let period_start = record.period_start;
    record.next_fence = plan.next_fence;

    inner.next_lease_id = plan.next_lease_id;
    let lease_id = LeaseId(plan.next_lease_id);
    inner.leases.open(
        lease_id,
        LeaseRecord::opened(account, plan.fencing_token, plan.granted, expires_at)
            .funded_by(drawn, period_start),
    );
    LeaseGrant {
        lease_id,
        account_id: account,
        fencing_token: plan.fencing_token,
        units: plan.granted,
        expires_at,
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
    ) -> Result<Allocation, AllocateError> {
        let expires_at = self.grant_expiry(ttl, now)?;
        let mut inner = self.lock();
        let plan = plan_grant(
            &inner,
            &self.policy,
            account,
            requested,
            CostUnits::ZERO,
            CostUnits::ZERO,
            true,
        )?;
        let grant = apply_grant(&mut inner, account, plan, expires_at);
        Ok(allocation(&inner, grant))
    }

    async fn release(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        now: Timestamp,
    ) -> Result<(), AllocateError> {
        let mut inner = self.lock();
        let plan = plan_release(&inner, &self.policy, lease_id, fencing_token, unspent, now)?;
        apply_release(&mut inner, lease_id, &plan);
        Ok(())
    }

    async fn consolidate(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        requested: CostUnits,
        needed: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<Allocation, AllocateError> {
        let expires_at = self.grant_expiry(ttl, now)?;
        let mut inner = self.lock();
        // Both halves are planned before either is applied. The reference
        // backend has no rollback, so a grant that refuses after the release
        // had already moved units would settle a lease it cannot replace —
        // which is the one failure this operation exists to prevent, arriving
        // by a different door.
        let release = plan_release(&inner, &self.policy, lease_id, fencing_token, unspent, now)?;
        let account = release.account_id;
        let record = inner
            .accounts
            .get(&account)
            .ok_or(AllocateError::UnknownAccount)?;
        // Not `unspent`: the allowance half of a lease funded by a closed
        // period expires rather than returning, so the grant is sized against
        // what the credit will actually restore (#97).
        let restored = spendable_credit(record, release.funding, release.period_start, unspent);
        let preserves_funding = release.loss.is_zero() && restored == unspent;
        let grant = plan_grant(
            &inner,
            &self.policy,
            account,
            requested,
            restored,
            needed,
            preserves_funding,
        )?;

        apply_release(&mut inner, lease_id, &release);
        let grant = apply_grant(&mut inner, account, grant, expires_at);
        Ok(allocation(&inner, grant))
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
            .reclaimable(self.policy.reclaim_cutoff(now), limit.get());
        // Planned, validated, then applied. `ReclaimBatch::try_new` is the
        // last fallible step, and running it after a loop that had already
        // settled leases and credited balances would return `Err` over a
        // ledger that had moved. Unreachable while `reclaimable` respects the
        // limit, but the structure is the defect, and it is one refactor away
        // from being reachable (#57).
        let mut reclaimed = Vec::with_capacity(expired.len());
        // A holder that never released cannot prove any unit unspent: its
        // lease accepted commits until `usable_until`, and whatever it had
        // committed but not flushed died with it. So a sweep settles the lease
        // as a release claiming nothing would: no credit, and the remainder
        // recorded as provisional settlement loss (#136). Usage that arrives
        // later still fits in that gap and converts loss into billed usage
        // (see `ingest`), which is how a holder that outlived an outage is
        // billed rather than dropped.
        for &lease_id in &expired {
            let lease = inner.leases.get(lease_id).expect("just listed");
            let forfeited = lease
                .granted
                .checked_sub(lease.used)
                .expect("usage never exceeds grant");
            reclaimed.push(ReclaimedLease {
                lease_id,
                account_id: lease.account_id,
                forfeited,
            });
        }
        let batch = ReclaimBatch::try_new(reclaimed, limit)?;

        for entry in batch.reclaimed() {
            assert!(
                inner
                    .leases
                    .settle(entry.lease_id, Settled::Expired, CostUnits::ZERO),
                "reclaimable only yields active leases"
            );
            let record = inner
                .accounts
                .get_mut(&entry.account_id)
                .expect("lease account exists");
            record.settlement_loss = record
                .settlement_loss
                .checked_add(entry.forfeited)
                .expect("loss overflow");
        }
        // One line per sweep rather than one per batch: a drain calls this
        // until a batch comes back unsaturated, and that last call carries the
        // post-sweep numbers.
        if batch.reclaimed().len() < limit.get() {
            let held = StoredRecords {
                credential_activity: inner.credential_activity.len(),
                usage_events: inner.usage.len(),
                leases: inner.leases.len(),
                active_leases: inner.leases.active_len(),
            };
            tracing::debug!(
                credential_activity = held.credential_activity,
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
) -> Result<Option<PublishableSnapshot>, PublishSnapshotError> {
    if let Some(key_id) = snapshot.key_id
        && !inner.keys.get(&key_id).is_some_and(|stored| {
            stored.record.principal == principal && stored.record.account_id == snapshot.account_id
        })
    {
        return Err(PublishSnapshotError::CredentialMismatch { key_id });
    }
    if let Some(record) = inner.accounts.get(&snapshot.account_id) {
        if record.status != snapshot.status {
            return Err(PublishSnapshotError::StatusMismatch {
                ledger: record.status,
                submitted: snapshot.status,
            });
        }
        if record.capacity_class != snapshot.capacity_class {
            return Err(PublishSnapshotError::CapacityClassMismatch {
                ledger: record.capacity_class,
                submitted: snapshot.capacity_class,
            });
        }
    }
    if let Some(existing) = inner.snapshots.get(&principal)
        && existing.generation() >= snapshot.generation
    {
        return Ok(None);
    }
    // The store stamps the budget view; a publisher cannot supply one (#97).
    // Done here rather than at each caller so the two publication entry points
    // cannot drift, and under the same lock as the write so the number
    // published is the ledger as of that write. An account this store does not
    // hold publishes unstamped, which is the same "adds no account-existence
    // requirement" rule the status check follows.
    // Unconditional, including the `None` arm: a snapshot arrives here having
    // crossed a wire, where nothing stops a publisher putting a balance in the
    // JSON. Overwriting always is what makes the store the field's only
    // writer, rather than only usually.
    let view = inner
        .accounts
        .get(&snapshot.account_id)
        .map(AccountRecord::budget_view);
    let snapshot = snapshot.with_budget(view);
    inner
        .snapshots
        .insert(principal, SnapshotRecord::Present(snapshot.clone()));
    Ok(Some(snapshot))
}

/// Which account-owned fact a republication is carrying.
///
/// The two operator actions — a status change and a capacity-class change —
/// differ only in the field they compare and set. Everything that makes the
/// republication *safe* is identical: the two-phase overflow check, the
/// tombstone skip, the already-at-target skip, and the deterministic ordering.
/// Writing that twice is how the two would drift, and the half that drifted
/// would be the half nobody was looking at.
#[derive(Debug, Clone, Copy)]
enum Restamp {
    Status(AccountStatus),
    CapacityClass(CapacityClass),
}

impl Restamp {
    /// Whether this snapshot already carries the target, and so must not be
    /// rewritten — that is what makes a repeated call converge.
    fn already_applied(self, snapshot: &tollgate_core::AccountSnapshot) -> bool {
        match self {
            Restamp::Status(status) => snapshot.status == status,
            Restamp::CapacityClass(class) => snapshot.capacity_class == class,
        }
    }

    fn apply(self, snapshot: &PublishableSnapshot, generation: Generation) -> PublishableSnapshot {
        match self {
            Restamp::Status(status) => snapshot.restamped(status, generation),
            Restamp::CapacityClass(class) => snapshot.reclassified(class, generation),
        }
    }
}

/// Plan the re-stamping of every live snapshot of `account`, under a lock the
/// caller already holds. Mutates nothing: the caller applies
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
/// Rows already at the target are left alone, so a repeated call converges
/// and bumps no generation.
///
/// Returned sorted by principal so both backends emit pushes in the same
/// order and a mirrored test need not assert on incidental ordering.
fn plan_republish(
    inner: &Inner,
    account: AccountId,
    restamp: Restamp,
) -> Result<Vec<(Principal, PublishableSnapshot)>, SetStatusError> {
    let mut planned = Vec::new();
    for (principal, record) in &inner.snapshots {
        // Revoked principals are skipped: republishing one would resurrect it,
        // which INVARIANTS.md #15 forbids.
        let SnapshotRecord::Present(snapshot) = record else {
            continue;
        };
        if snapshot.account_id != account || restamp.already_applied(snapshot) {
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
        planned.push((*principal, restamp.apply(snapshot, generation)));
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
    async fn create_account(
        &self,
        config: AccountConfig,
    ) -> Result<crate::AdminReceipt<()>, CreateAccountError> {
        self.create_account_audited(config)
    }

    async fn deposit(
        &self,
        account: AccountId,
        units: CostUnits,
    ) -> Result<crate::AdminReceipt<()>, AllocateError> {
        self.deposit_audited(account, units)
    }

    async fn set_budget_schedule(
        &self,
        account: AccountId,
        schedule: Option<BudgetSchedule>,
    ) -> Result<crate::AdminReceipt<()>, BudgetError> {
        let mut inner = self.lock();
        let record = inner
            .accounts
            .get_mut(&account)
            .ok_or(BudgetError::UnknownAccount)?;
        // Read before write, under the one guard, so the receipt reports what
        // this call replaced rather than what a later reader happens to find.
        let before = AdminState::Budget {
            schedule: record.schedule,
        };
        record.schedule = schedule;
        Ok(crate::AdminReceipt::new(
            (),
            before,
            AdminState::Budget { schedule },
        ))
    }

    async fn roll_due_periods(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<RolloverBatch, StoreError> {
        let mut inner = self.lock();
        let mut rolled = Vec::new();
        // Choose *which* accounts this bounded batch rolls before rolling any,
        // oldest period first, exactly as `DUE_PERIODS_SQL` does with
        // `ORDER BY period_start_us LIMIT $3`.
        //
        // Iterating `accounts` directly and breaking at `limit` selected by
        // hash order, so with more accounts due than one batch can take, two
        // runs over identical state rolled different accounts — and a different
        // set again from the backend this one is the reference for (#100). The
        // drain loop means every due account is rolled eventually, so this was
        // not a ledger defect; it was an unbounded-in-principle wait for any
        // particular account, and a divergence no test could see.
        //
        // The account id breaks ties so the order is total. PostgreSQL leaves
        // equal `period_start_us` rows in an arbitrary order; being stricter
        // than the contract is safe, and being unpredictable is what this
        // avoids.
        #[allow(
            clippy::disallowed_methods,
            reason = "sorted below before the limit is applied, so the batch's membership is a function of the stored state"
        )]
        let mut due: Vec<(Timestamp, AccountId)> = inner
            .accounts
            .iter()
            .filter_map(|(account_id, record)| {
                let schedule = record.schedule?;
                (schedule.period.start_of(now) > record.period_start)
                    .then_some((record.period_start, *account_id))
            })
            .collect();
        due.sort_unstable();
        due.truncate(limit.get());

        for (_, account_id) in due {
            let account_id = &account_id;
            let record = inner
                .accounts
                .get_mut(account_id)
                .expect("the due set was taken from this map under the same lock");
            let Some(schedule) = record.schedule else {
                continue;
            };
            // The boundary test and the crossing happen under one lock, which
            // is what makes two passes racing a boundary produce one roll. The
            // reference backend gets that from the mutex; PostgreSQL gets it
            // from a row lock over the same comparison.
            let boundary = schedule.period.start_of(now);
            if boundary <= record.period_start {
                continue;
            }
            // One allowance, not one per missed boundary: an account left
            // unrolled for two months is entitled to what it has now, not to a
            // backlog.
            let expired = record.balance.allowance;
            let deposited = record
                .deposited
                .checked_add(schedule.allowance)
                .ok_or_else(|| StoreError(format!("deposit overflow for account {account_id}")))?;
            let total_expired = record
                .expired
                .checked_add(expired)
                .ok_or_else(|| StoreError(format!("expiry overflow for account {account_id}")))?;
            // Written only after every fallible step has succeeded: a rollover
            // that overflowed halfway would leave the account funded but not
            // credited, or credited twice at the next pass.
            record.deposited = deposited;
            record.expired = total_expired;
            record.balance.allowance = schedule.allowance;
            record.period_start = boundary;
            rolled.push(RolledAccount {
                account_id: *account_id,
                deposited: schedule.allowance,
                expired,
            });
        }
        RolloverBatch::try_new(rolled, limit)
    }

    async fn set_account_status(
        &self,
        account: AccountId,
        status: AccountStatus,
    ) -> Result<crate::AdminReceipt<StatusChange>, SetStatusError> {
        // One lock for both records. The inherent `publish_snapshot` takes the
        // lock itself, so it cannot be reused here: the whole point is that no
        // observer sees the ledger moved and the snapshots not.
        let (republished, before) = {
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
            let before = AdminState::Status {
                status: record.status,
            };
            let planned = plan_republish(&inner, account, Restamp::Status(status))?;
            // Phase 3: apply. Nothing below this line can fail.
            inner
                .accounts
                .get_mut(&account)
                .expect("the account was found under this same guard")
                .status = status;
            (apply_republish(&mut inner, planned), before)
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
        Ok(AdminReceipt::new(
            StatusChange {
                republished,
                // This backend holds validated snapshots rather than encoded ones,
                // so there is nothing here that can fail to decode.
                unreadable: 0,
            },
            before,
            AdminState::Status { status },
        ))
    }

    async fn set_capacity_class(
        &self,
        account: AccountId,
        class: CapacityClass,
    ) -> Result<crate::AdminReceipt<StatusChange>, SetStatusError> {
        // Structurally identical to `set_account_status`, deliberately: one
        // lock across both records, refusals before anything moves, the whole
        // snapshot plan computed before the ledger is touched. What differs is
        // one field, and that difference lives in `Restamp` rather than in a
        // second copy of this procedure.
        let (republished, before) = {
            let mut inner = self.lock();
            let record = inner
                .accounts
                .get(&account)
                .ok_or(SetStatusError::UnknownAccount)?;
            // A closed account is terminal. Reclassifying one is meaningless,
            // and refusing costs nothing that a caller wanted.
            if record.status == AccountStatus::Closed {
                return Err(SetStatusError::AccountClosed);
            }
            let before = AdminState::CapacityClass {
                capacity_class: record.capacity_class,
            };
            let planned = plan_republish(&inner, account, Restamp::CapacityClass(class))?;
            inner
                .accounts
                .get_mut(&account)
                .expect("the account was found under this same guard")
                .capacity_class = class;
            (apply_republish(&mut inner, planned), before)
        };
        if pushes_exceed_capacity(republished.len()) {
            tracing::warn!(
                %account,
                principals = republished.len(),
                capacity = PUSH_CHANNEL_CAPACITY,
                "capacity class change emitted more pushes than the channel holds; \
                 subscribers will resync"
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
        Ok(AdminReceipt::new(
            StatusChange {
                republished,
                unreadable: 0,
            },
            before,
            AdminState::CapacityClass {
                capacity_class: class,
            },
        ))
    }

    async fn publish_snapshot(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
    ) -> Result<crate::AdminReceipt<()>, PublishSnapshotError> {
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
        let (published, before, after) = {
            let mut inner = self.lock();
            let before = snapshot_audit(inner.snapshots.get(&principal));
            let published = publish_locked(&mut inner, principal, snapshot)?;
            (
                published,
                before,
                snapshot_audit(inner.snapshots.get(&principal)),
            )
        };
        if let Some(snapshot) = published {
            self.push_to_subscribers(SnapshotPush {
                principal,
                resolution: SnapshotResolution::Present(snapshot),
            });
        }
        Ok(AdminReceipt::new((), before, after))
    }

    async fn account_view(&self, account: AccountId) -> Result<Option<AccountView>, StoreError> {
        // One guard over status, schedule and the funding equation. Reading
        // them separately could straddle a rollover or a status change and
        // describe a state this account was never in.
        let inner = self.lock();
        let Some(record) = inner.accounts.get(&account) else {
            return Ok(None);
        };
        let mut active_grants = CostUnits::ZERO;
        let mut active_used = CostUnits::ZERO;
        for lease in inner.leases.active_of(account) {
            active_grants = active_grants
                .checked_add(lease.granted)
                .ok_or_else(|| StoreError(format!("grant sum overflow for account {account}")))?;
            active_used = active_used
                .checked_add(lease.used)
                .ok_or_else(|| StoreError(format!("used sum overflow for account {account}")))?;
        }
        Ok(Some(AccountView {
            account_id: account,
            status: record.status,
            capacity_class: record.capacity_class,
            schedule: record.schedule,
            period_start: record.period_start,
            conservation: Conservation {
                deposited: record.deposited,
                overage_recorded: record.overage_recorded,
                balance: record.balance.total(),
                active_lease_grants: active_grants,
                settled_usage: record
                    .usage_recorded
                    .checked_sub(active_used)
                    .ok_or_else(|| {
                        StoreError(format!("active usage exceeds recorded for {account}"))
                    })?,
                settlement_loss: record.settlement_loss,
                expired: record.expired,
            },
        }))
    }

    async fn remove_snapshot(
        &self,
        principal: Principal,
    ) -> Result<crate::AdminReceipt<()>, StoreError> {
        Ok(self.remove_snapshot_audited(principal))
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
        // Sorted, because the catalogue is an output and `snapshots` is a
        // `HashMap`: returning its iteration order would make this call's
        // result depend on the hash seed rather than on the stored state, and
        // differ run to run. `PostgresStore` already answers
        // `ORDER BY principal`, so the reference backend was the one diverging
        // (#100). The suite's assertion sorted before comparing, which is how
        // it stayed invisible.
        #[allow(
            clippy::disallowed_methods,
            reason = "sorted on the next line before it leaves the function, so the hash order never reaches the caller"
        )]
        let mut principals: Vec<Principal> = self.lock().snapshots.keys().copied().collect();
        principals.sort_unstable_by_key(|principal| principal.0);
        Ok(Some(principals))
    }
}

#[async_trait]
impl UsageSink for MemoryStore {
    async fn ingest(
        &self,
        events: &[UsageEvent],
        _now: Timestamp,
    ) -> Result<IngestReport, IngestError> {
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
                    // Refused, not unavailable: a monotonic column that cannot
                    // absorb these units will not absorb them on the next
                    // attempt either, so retrying this batch forever would
                    // wedge the writer behind an arithmetic fact (#61).
                    return Err(IngestError::Refused(StoreError(format!(
                        "overage accounting overflow for account {:#034x}: recorded usage {} \
                         and overage {} cannot absorb {}",
                        event.account_id.0, recorded, overage, event.units
                    ))));
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
                recorded.checked_add(event.units).ok_or_else(|| {
                    IngestError::Refused(StoreError(format!(
                        "usage accounting overflow for account {:#034x}: recorded usage {} \
                         cannot absorb {}",
                        account_id.0, recorded, event.units
                    )))
                })?,
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

        // Attribute only the canonical accepted set. Replays were classified
        // above before their payload was trusted, and may never change history.
        let mut activity = HashMap::<KeyId, Timestamp>::new();
        let mut unattributed = 0;
        for event in &accepted {
            if let Some(key_id) = event.key_id
                && inner
                    .keys
                    .get(&key_id)
                    .is_some_and(|key| key.record.account_id == event.account_id)
            {
                let at = crate::clock::timestamp_from_micros(event.occurred_at.as_microsecond())
                    .expect("truncating a valid Timestamp to microseconds stays representable");
                activity
                    .entry(key_id)
                    .and_modify(|old| *old = (*old).max(at))
                    .or_insert(at);
            } else {
                unattributed += 1;
            }
        }
        report.unattributed = Some(unattributed);

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
        for (key_id, at) in activity {
            inner
                .credential_activity
                .entry(key_id)
                .and_modify(|old| *old = (*old).max(at))
                .or_insert(at);
        }
        Ok(report)
    }
}

#[async_trait]
impl KeyDirectory for MemoryStore {
    async fn credential_activity(
        &self,
        keys: &[KeyId],
    ) -> Result<Vec<crate::CredentialActivity>, StoreError> {
        let inner = self.lock();
        Ok(keys
            .iter()
            .map(|&key_id| crate::CredentialActivity {
                key_id,
                state: if !inner.keys.contains_key(&key_id) {
                    crate::CredentialActivityState::Unknown
                } else if let Some(&last_committed_at) = inner.credential_activity.get(&key_id) {
                    crate::CredentialActivityState::Committed { last_committed_at }
                } else {
                    crate::CredentialActivityState::Unobserved
                },
            })
            .collect())
    }

    async fn insert_key(&self, record: KeyRecord) -> Result<(), KeyError> {
        let mut inner = self.lock();
        insert_key_locked(&mut inner, record).map(|receipt| receipt.outcome)
    }

    async fn revoke_key(&self, key_id: KeyId, now: Timestamp) -> Result<Revocation, KeyError> {
        self.revoke_key_audited(key_id, now)
            .await
            .map(|receipt| receipt.outcome)
    }

    async fn revoke_key_audited(
        &self,
        key_id: KeyId,
        now: Timestamp,
    ) -> Result<crate::AdminReceipt<Revocation>, KeyError> {
        let mut inner = self.lock();
        let Some(stored) = inner.keys.get(&key_id) else {
            return Err(KeyError::UnknownKey);
        };
        let account_id = stored.record.account_id;
        let before = AdminState::Credential {
            account_id,
            key_id,
            revoked: stored.revoked_at.is_some(),
        };
        if stored.revoked_at.is_some() {
            return Ok(crate::AdminReceipt::new(
                Revocation::AlreadyRetired,
                before,
                before,
            ));
        }
        let revision = next_credential_revision(inner.credential_revision)?;
        inner
            .keys
            .get_mut(&key_id)
            .expect("key exists under the same guard")
            .revoked_at = Some(now);
        inner.unrevoked_keys.remove(&key_id);
        inner.credential_revision = revision;
        Ok(crate::AdminReceipt::new(
            Revocation::Retired,
            before,
            AdminState::Credential {
                account_id,
                key_id,
                revoked: true,
            },
        ))
    }

    async fn active_keys(&self, now: Timestamp) -> Result<Vec<KeyRecord>, StoreError> {
        let inner = self.lock();
        let active: Vec<KeyRecord> = inner
            .unrevoked_keys
            .iter()
            .map(|id| &inner.keys[id])
            // Expiry is decided here rather than by each reader, so every
            // backend answers "active" the same way and a projection cannot
            // disagree with the ledger about which credentials are live.
            // Compare the full Timestamp: truncating either side can change
            // an exclusive expiry boundary or the proof sent to a verifier.
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
        Ok(active)
    }

    async fn account_keys(
        &self,
        account: AccountId,
        after: Option<KeyId>,
        limit: NonZeroUsize,
    ) -> Result<Vec<KeySummary>, StoreError> {
        crate::validate_key_page_limit(limit)?;
        let inner = self.lock();
        // Revoked credentials are included, so `unrevoked_keys` is not the
        // index to read here; `keys` is a `HashMap`, so the order is imposed
        // rather than inherited. Sorting before truncating is what makes the
        // page a function of the stored state instead of the hash seed — the
        // same rule `principals` follows.
        #[allow(
            clippy::disallowed_methods,
            reason = "sorted below before the page is truncated, so the hash order never reaches the caller"
        )]
        let mut summaries: Vec<KeySummary> = inner
            .keys
            .values()
            .filter(|stored| stored.record.account_id == account)
            .filter(|stored| after.is_none_or(|cursor| stored.record.key_id > cursor))
            .map(|stored| KeySummary {
                key_id: stored.record.key_id,
                not_after: stored.record.not_after,
                revoked_at: stored.revoked_at,
            })
            .collect();
        summaries.sort_unstable_by_key(|summary| summary.key_id);
        summaries.truncate(limit.get());
        Ok(summaries)
    }

    async fn insert_key_within(
        &self,
        record: KeyRecord,
        max_active: NonZeroUsize,
        now: Timestamp,
    ) -> Result<(), KeyError> {
        self.insert_key_within_audited(record, max_active, now)
            .await
            .map(|receipt| receipt.outcome)
    }

    async fn insert_key_within_audited(
        &self,
        record: KeyRecord,
        max_active: NonZeroUsize,
        now: Timestamp,
    ) -> Result<crate::AdminReceipt<()>, KeyError> {
        // One guard across the count and the write. That is the whole point of
        // the method: releasing it between them would let two issuers each see
        // room for one more and both take it.
        let mut inner = self.lock();
        // Identity is decided *before* the bound, and the order is load-bearing.
        // A caller that lost the response resends the same `key_id`; by then
        // its own successful write may have filled the bound, and answering
        // `ActiveKeyLimit` would tell it to retire a credential when what
        // actually happened is that its first call worked. `AlreadyExists` is
        // both the true answer and the one that makes the retry safe (#121).
        if !inner.accounts.contains_key(&record.account_id) {
            return Err(KeyError::UnknownAccount);
        }
        if inner.keys.contains_key(&record.key_id)
            || inner.key_principals.contains_key(&record.principal)
        {
            return Err(KeyError::AlreadyExists);
        }
        #[allow(
            clippy::disallowed_methods,
            reason = "counts live credentials; a count does not depend on the order they are counted in"
        )]
        let live = inner
            .keys
            .values()
            .filter(|stored| stored.record.account_id == record.account_id)
            .filter(|stored| {
                KeySummary {
                    key_id: stored.record.key_id,
                    not_after: stored.record.not_after,
                    revoked_at: stored.revoked_at,
                }
                .is_live(now)
            })
            .count();
        if live >= max_active.get() {
            return Err(KeyError::ActiveKeyLimit { limit: max_active });
        }
        insert_key_locked(&mut inner, record)
    }
}

/// The credential write both [`KeyDirectory::insert_key`] and
/// [`KeyDirectory::insert_key_within`] perform, with the guard already held.
///
/// Shared rather than copied so the bounded path cannot drift from the
/// unbounded one on what it validates — and taken by `&mut Inner` so it
/// *cannot* acquire a lock, which is what makes the caller's guard span the
/// count and the write.
fn insert_key_locked(
    inner: &mut Inner,
    record: KeyRecord,
) -> Result<crate::AdminReceipt<()>, KeyError> {
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
    if inner.key_principals.contains_key(&record.principal) {
        return Err(KeyError::AlreadyExists);
    }
    let revision = next_credential_revision(inner.credential_revision)?;
    inner.key_principals.insert(record.principal, record.key_id);
    inner.unrevoked_keys.insert(record.key_id);
    let after = AdminState::Credential {
        account_id: record.account_id,
        key_id: record.key_id,
        revoked: false,
    };
    inner.keys.insert(
        record.key_id,
        StoredKey {
            record,
            revoked_at: None,
        },
    );
    inner.credential_revision = revision;
    Ok(crate::AdminReceipt::new((), AdminState::Absent, after))
}

fn next_credential_revision(revision: u64) -> Result<u64, KeyError> {
    revision
        .checked_add(1)
        .filter(|next| *next <= crate::MAX_KEY_REVISION)
        .ok_or_else(|| KeyError::Storage(StoreError("credential revision exhausted".into())))
}

#[async_trait]
impl crate::KeySource for MemoryStore {
    async fn active_keys_page(
        &self,
        now: Timestamp,
        after: Option<KeyId>,
        limit: NonZeroUsize,
    ) -> Result<crate::KeyPage, StoreError> {
        use std::ops::Bound::{Excluded, Unbounded};
        crate::validate_key_page_limit(limit)?;
        let inner = self.lock();
        let lower = after.map_or(Unbounded, Excluded);
        let mut records: Vec<crate::CredentialRecord> = inner
            .unrevoked_keys
            .range((lower, Unbounded))
            .map(|id| &inner.keys[id].record)
            .filter(|record| record.not_after.is_none_or(|end| now < end))
            .take(limit.get() + 1)
            .cloned()
            .map(Into::into)
            .collect();
        let next_after = if records.len() > limit.get() {
            records.pop();
            records.last().map(|key| key.key_id)
        } else {
            None
        };
        crate::KeyPage::try_new(
            inner.credential_revision,
            now,
            after,
            limit,
            records,
            next_after,
        )
    }
}

fn snapshot_audit(record: Option<&SnapshotRecord>) -> AdminState {
    match record {
        None => AdminState::Absent,
        Some(SnapshotRecord::Present(snapshot)) => AdminState::Snapshot {
            generation: snapshot.generation,
            revoked: false,
        },
        Some(SnapshotRecord::Revoked(generation)) => AdminState::Snapshot {
            generation: *generation,
            revoked: true,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    #[tokio::test]
    async fn credential_revision_overflow_preserves_records_and_both_indexes() {
        use crate::KeySource;
        let store = store_with(100);
        let record = |id: u128| {
            let mut digest = [0; 32];
            digest[..16].copy_from_slice(&id.to_be_bytes());
            KeyRecord {
                key_id: KeyId(id),
                account_id: ACCOUNT,
                principal: Principal(id),
                digest,
                not_after: None,
            }
        };
        store.insert_key(record(1)).await.unwrap();
        store.inner.lock().unwrap().credential_revision = crate::MAX_KEY_REVISION - 1;
        store.insert_key(record(2)).await.unwrap();
        assert!(matches!(
            store.insert_key(record(3)).await,
            Err(KeyError::Storage(_))
        ));
        assert!(matches!(
            store.revoke_key(KeyId(1), t(100)).await,
            Err(KeyError::Storage(_))
        ));
        let page = store
            .active_keys_page(t(100), None, crate::DEFAULT_KEY_PAGE_LIMIT)
            .await
            .unwrap();
        assert_eq!(page.revision(), crate::MAX_KEY_REVISION);
        assert_eq!(
            page.records().iter().map(|k| k.key_id).collect::<Vec<_>>(),
            vec![KeyId(1), KeyId(2)]
        );
        store.inner.lock().unwrap().credential_revision = 2;
        store.insert_key(record(3)).await.unwrap(); // failure left no duplicate-principal index entry
    }

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
            capacity_class: CapacityClass::Assured,
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
                .expect("funded")
                .grant;
            store
                .release(lease.lease_id, lease.fencing_token, lease.units, t(1))
                .await
                .expect("active");
        }
        let due = store
            .acquire(ACCOUNT, CostUnits(1), TTL, t(0))
            .await
            .expect("funded")
            .grant;

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
                    .expect("funded")
                    .grant;
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
