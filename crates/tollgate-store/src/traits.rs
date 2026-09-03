//! The storage traits and their shared vocabulary.

use std::num::NonZeroUsize;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountStatus, CostUnits, FencingToken, Generation, KeyId, LeaseGrant, LeaseId,
    Principal, PublishableSnapshot, UsageEvent,
};

/// Backend failure unrelated to domain rules (connection lost, transaction
/// aborted). Callers treat it as retryable-with-backoff; it must never be
/// conflated with a domain refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "store error: {}", self.0)
    }
}

impl std::error::Error for StoreError {}

/// Domain refusals from the allocator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllocateError {
    UnknownAccount,
    /// The account exists but is not in a state that may spend.
    AccountInactive,
    /// Nothing left to lease. Distinct from `AccountInactive`: the client
    /// should keep polling, because usage settlement or a top-up can restore
    /// balance.
    InsufficientBalance,
    /// A lease must have a strictly positive lifetime.
    InvalidTtl,
    UnknownLease,
    /// The fencing token does not match the lease record named by `lease_id`.
    /// Token ordering across different active leases is irrelevant
    /// (INVARIANTS.md #4).
    Fenced,
    /// The lease exists but is no longer active (already released, expired,
    /// or reclaimed).
    LeaseNotActive,
    /// A release claimed more unspent units than the lease can still hold
    /// (`unspent + recorded usage > granted`) — a client accounting bug,
    /// surfaced rather than absorbed.
    InvalidRelease,
    Storage(StoreError),
}

impl AllocateError {
    /// Stable metric labels, in [`index`](AllocateError::index) order.
    ///
    /// Variant names, not [`Display`](std::fmt::Display) output: `Storage`
    /// wraps a backend message that varies per failure, so labelling by
    /// rendered text would mint a fresh time series per connection error.
    pub const NAMES: [&'static str; Self::COUNT] = [
        "unknown_account",
        "account_inactive",
        "insufficient_balance",
        "invalid_ttl",
        "unknown_lease",
        "fenced",
        "lease_not_active",
        "invalid_release",
        "storage",
    ];

    /// How many distinct refusals exist — the width of a per-reason tally.
    pub const COUNT: usize = 9;

    /// This refusal's dense slot, for direct-indexed per-reason counters.
    ///
    /// `Storage` carries data, so there is no discriminant to cast; the
    /// mapping is written out and the match is exhaustive, so a new variant
    /// fails to compile until it is given a slot rather than silently landing
    /// in another's bucket. Mirrors `DenyReason::index`.
    #[must_use]
    pub const fn index(&self) -> usize {
        match self {
            AllocateError::UnknownAccount => 0,
            AllocateError::AccountInactive => 1,
            AllocateError::InsufficientBalance => 2,
            AllocateError::InvalidTtl => 3,
            AllocateError::UnknownLease => 4,
            AllocateError::Fenced => 5,
            AllocateError::LeaseNotActive => 6,
            AllocateError::InvalidRelease => 7,
            AllocateError::Storage(_) => 8,
        }
    }

    /// This refusal's stable metric label.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        Self::NAMES[self.index()]
    }
}

impl std::fmt::Display for AllocateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocateError::UnknownAccount => f.write_str("unknown account"),
            AllocateError::AccountInactive => f.write_str("account inactive"),
            AllocateError::InsufficientBalance => f.write_str("insufficient balance"),
            AllocateError::InvalidTtl => f.write_str("lease TTL must be positive"),
            AllocateError::UnknownLease => f.write_str("unknown lease"),
            AllocateError::Fenced => f.write_str("fencing token mismatch"),
            AllocateError::LeaseNotActive => f.write_str("lease not active"),
            AllocateError::InvalidRelease => f.write_str("invalid release"),
            AllocateError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for AllocateError {}

impl From<StoreError> for AllocateError {
    fn from(error: StoreError) -> Self {
        AllocateError::Storage(error)
    }
}

/// Admin-side inputs when creating an account.
#[derive(Debug, Clone, Copy)]
pub struct AccountConfig {
    pub account_id: AccountId,
    pub initial_balance: CostUnits,
    /// The account's administrative status at birth. Anything but
    /// [`AccountStatus::Active`] refuses leases while keeping the ledger, and
    /// `Closed` is terminal from creation onwards (INVARIANTS.md #22).
    ///
    /// An [`AccountStatus`] rather than a bool so creation and
    /// [`AdminStore::set_account_status`] speak one vocabulary about one
    /// column; the bool could not express `Closed`, which is what let
    /// terminality be a convention instead of a check (#51).
    pub status: AccountStatus,
}

/// Per-account conservation view for reconciliation.
///
/// Read the equation as a funding statement: the left side is everything the
/// account was ever funded with, the right side is where those units now sit.
/// The two sources of funding are money in ([`deposited`](Self::deposited))
/// and credit extended ([`overage_recorded`](Self::overage_recorded)); the
/// three resting places are unspent balance, capacity currently out on lease,
/// and units already consumed or written off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Conservation {
    pub deposited: CostUnits,
    /// Unfunded units billed under [`EnforcementMode::Elastic`]: spend no
    /// deposit paid for and no lease debited.
    ///
    /// A *funding* term, on the left of the equation beside `deposited`, not
    /// a bucket on the right. Overage usage also lands in `settled_usage`, so
    /// without a matching term on the left the equation would fail by exactly
    /// the overage — which is the whole reason this field exists rather than
    /// the ledger simply recording the usage and saying nothing else.
    ///
    /// [`EnforcementMode::Elastic`]: tollgate_core::EnforcementMode::Elastic
    pub overage_recorded: CostUnits,
    pub balance: CostUnits,
    pub active_lease_grants: CostUnits,
    /// Usage billed against leases that have settled (released or expired),
    /// plus all overage usage — which belongs to no lease and is therefore
    /// settled the moment it is recorded. Usage on active leases is inside
    /// `active_lease_grants`.
    pub settled_usage: CostUnits,
    pub settlement_loss: CostUnits,
}

impl Conservation {
    /// `deposited + overage == balance + active grants + settled usage + loss`,
    /// exactly.
    ///
    /// Both sides accumulate with checked arithmetic and an overflow answers
    /// `false`, never a wrap or a panic: this function exists to *detect*
    /// corrupt ledger state, so arithmetic that could not represent the state
    /// must report a violation rather than quietly produce a total that
    /// happens to match (INVARIANTS.md #11).
    #[must_use]
    pub fn holds(&self) -> bool {
        let Some(funded) = self.deposited.checked_add(self.overage_recorded) else {
            return false;
        };
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
        sum == funded
    }
}

/// How an allocator sizes grants as an account's balance shrinks.
///
/// Deep balances grant the full request; near exhaustion the grant is capped
/// at `balance / shrink_divisor` (floored at `min_grant`, and never above the
/// remaining balance). This is the design-review answer to the quota-edge
/// problem: N instances can no longer strand a small balance behind one
/// holder's oversized lease.
#[derive(Debug, Clone, Copy)]
pub struct GrantPolicy {
    pub shrink_divisor: u64,
    pub min_grant: CostUnits,
    /// Hard cap on any single lease's TTL; requests beyond it are clamped.
    pub max_ttl: SignedDuration,
    /// How long past a lease's `expires_at` the allocator waits before
    /// reclaiming its unspent units. Holders stop spending at
    /// `expires_at - safety margin` (their side of the protocol), so work
    /// committed inside the usability window has `margin + grace` to be
    /// flushed and billed before settlement could reject it. Releases are
    /// also accepted through the grace window (review finding #1).
    pub reclaim_grace: SignedDuration,
}

/// Invalid allocator policy. Duration signs are part of the lease safety
/// protocol, so invalid values are rejected at backend construction rather
/// than normalized into a potentially unsafe policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrantPolicyError(pub &'static str);

impl std::fmt::Display for GrantPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for GrantPolicyError {}

impl Default for GrantPolicy {
    fn default() -> Self {
        GrantPolicy {
            shrink_divisor: 2,
            min_grant: CostUnits(1),
            max_ttl: SignedDuration::from_secs(300),
            reclaim_grace: SignedDuration::from_secs(30),
        }
    }
}

impl GrantPolicy {
    pub fn validate(&self) -> Result<(), GrantPolicyError> {
        if self.shrink_divisor == 0 {
            return Err(GrantPolicyError("shrink_divisor must be positive"));
        }
        if self.min_grant.is_zero() {
            return Err(GrantPolicyError("min_grant must be positive"));
        }
        if self.max_ttl <= SignedDuration::ZERO {
            return Err(GrantPolicyError("max_ttl must be positive"));
        }
        if self.reclaim_grace < SignedDuration::ZERO {
            return Err(GrantPolicyError("reclaim_grace must not be negative"));
        }
        Ok(())
    }

    /// The units a request for `requested` receives from `balance`, or `None`
    /// when the balance cannot fund any grant.
    #[must_use]
    pub fn grant(&self, requested: CostUnits, balance: CostUnits) -> Option<CostUnits> {
        // Constructors reject these policy/request states, but `grant` is a
        // public pure helper too. Keep direct use fail-closed instead of
        // panicking on a zero divisor or manufacturing a unit for a zero
        // request.
        if requested.is_zero()
            || balance.is_zero()
            || self.shrink_divisor == 0
            || self.min_grant.is_zero()
        {
            return None;
        }
        let cap = (balance.get() / self.shrink_divisor).max(self.min_grant.get());
        Some(CostUnits(
            requested.get().min(cap).min(balance.get()).max(1),
        ))
    }
}

/// One lease settled by an expiry sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "wire", derive(serde::Serialize, serde::Deserialize))]
pub struct ReclaimedLease {
    pub lease_id: LeaseId,
    pub account_id: AccountId,
    /// Units credited back to the account: `granted - recorded usage`.
    pub reclaimed: CostUnits,
}

/// The production-sized upper bound for one expiry-reclaim transaction.
///
/// The limit bounds locks, row materialization, and SQL parameters per
/// transaction; it does not cap a legitimate backlog because the maintenance
/// task drains saturated batches until it reaches a partial one.
pub const DEFAULT_RECLAIM_BATCH_LIMIT: NonZeroUsize =
    NonZeroUsize::new(256).expect("the reclaim batch limit is nonzero");

/// Verified evidence returned by one bounded expiry-reclaim transaction.
///
/// The fields are private so `saturated` cannot disagree with the requested
/// limit. Callers may therefore use it to decide whether another batch is
/// required without re-deriving the backend's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReclaimBatch {
    reclaimed: Vec<ReclaimedLease>,
    saturated: bool,
}

impl ReclaimBatch {
    /// Build a batch and derive its saturation evidence from `limit`.
    pub fn try_new(
        reclaimed: Vec<ReclaimedLease>,
        limit: NonZeroUsize,
    ) -> Result<Self, StoreError> {
        if reclaimed.len() > limit.get() {
            return Err(StoreError(format!(
                "reclaim backend returned {} leases for a batch limit of {}",
                reclaimed.len(),
                limit
            )));
        }
        Ok(ReclaimBatch {
            saturated: reclaimed.len() == limit.get(),
            reclaimed,
        })
    }

    #[must_use]
    pub fn reclaimed(&self) -> &[ReclaimedLease] {
        &self.reclaimed
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.reclaimed.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.reclaimed.is_empty()
    }

    #[must_use]
    pub fn is_saturated(&self) -> bool {
        self.saturated
    }

    #[must_use]
    pub fn into_reclaimed(self) -> Vec<ReclaimedLease> {
        self.reclaimed
    }
}

/// Atomic lease allocation against the account balance — the amortization
/// point: one `acquire` funds thousands of local reservations.
#[async_trait]
pub trait LeaseAllocator: Send + Sync {
    /// Atomically debit a grant from the account. The granted size follows
    /// the backend's [`GrantPolicy`] and may be smaller than `requested`;
    /// the fencing token comes from a strictly increasing per-account
    /// sequence. It remains a capability for this lease only; allocating a
    /// newer token does not invalidate another active lease.
    async fn acquire(
        &self,
        account: AccountId,
        requested: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<LeaseGrant, AllocateError>;

    /// Graceful return: require the stored `(lease_id, fencing_token)` pair,
    /// credit `unspent` back, and close the lease. Callers should flush usage
    /// first when possible; events arriving after release are accepted only
    /// when they fit its provisional settlement loss.
    async fn release(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        now: Timestamp,
    ) -> Result<(), AllocateError>;

    /// Settle at most `limit` active leases whose TTL (plus the policy's
    /// reclaim grace) has lapsed, crediting `granted - recorded usage` back
    /// to each account (INVARIANTS.md #9). One call is one bounded atomic
    /// transaction; [`ReclaimBatch::is_saturated`] is verified evidence that
    /// the caller should immediately run another batch. It must be safe to
    /// run concurrently with everything else.
    async fn reclaim_expired_batch(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<ReclaimBatch, StoreError>;

    /// Settle every currently expired lease through bounded transactions.
    ///
    /// This preserves the original full-drain caller API. If a later batch
    /// fails, earlier batches are already committed, so the returned error
    /// explicitly reports that partial progress rather than presenting the
    /// operation as all-or-nothing.
    async fn reclaim_expired(&self, now: Timestamp) -> Result<Vec<ReclaimedLease>, StoreError> {
        let mut reclaimed: Vec<ReclaimedLease> = Vec::new();
        loop {
            let batch = match self
                .reclaim_expired_batch(now, DEFAULT_RECLAIM_BATCH_LIMIT)
                .await
            {
                Ok(batch) => batch,
                Err(error) if reclaimed.is_empty() => return Err(error),
                Err(error) => {
                    let units: u128 = reclaimed
                        .iter()
                        .map(|lease| u128::from(lease.reclaimed.get()))
                        .sum();
                    return Err(StoreError(format!(
                        "reclaim drain failed after {} leases totaling {units} units were committed: {error}",
                        reclaimed.len()
                    )));
                }
            };
            let saturated = batch.is_saturated();
            reclaimed.extend(batch.into_reclaimed());
            if !saturated {
                return Ok(reclaimed);
            }
            tokio::task::yield_now().await;
        }
    }
}

/// Authoritative state returned by a snapshot pull or push.
#[derive(Debug, Clone)]
pub enum SnapshotResolution {
    /// A compiled snapshot is currently authoritative.
    Present(PublishableSnapshot),
    /// The principal existed but was revoked at this generation. Sources must
    /// retain this watermark so a delayed older positive cannot resurrect it.
    Revoked { generation: Generation },
    /// The source has never observed this principal.
    Unknown,
}

/// Slots in a backend's snapshot push channel.
///
/// Shared so the two backends cannot drift, and named rather than inlined
/// because two things must agree on it: the channel, and the warning that
/// fires when one operation would out-run it. Each slot retains an
/// `Arc<AccountSnapshot>`, so this is also a bound on how much snapshot memory
/// one slow subscriber can pin.
pub const PUSH_CHANNEL_CAPACITY: usize = 256;

/// Whether pushing `principals` updates at once will out-run the push channel.
///
/// A status change republishes every live snapshot of an account (#51), so a
/// wide account can exceed the channel in one operation. Past this point every
/// subscriber lags and resyncs its whole tracked set — correct, and bounded by
/// the client's `max_concurrent_fetches`, but expensive enough that an
/// operator should not have to infer it from a latency graph.
///
/// Strictly greater: a batch that exactly fills the channel is delivered, so
/// warning at equality would cry wolf on the largest successful case. Pure, so
/// the boundary is pinned by a test rather than by whichever backend is being
/// read.
#[must_use]
pub fn pushes_exceed_capacity(principals: usize) -> bool {
    principals > PUSH_CHANNEL_CAPACITY
}

/// One pushed snapshot update.
#[derive(Debug, Clone)]
pub struct SnapshotPush {
    pub principal: Principal,
    pub resolution: SnapshotResolution,
}

/// Where compiled snapshots come from.
#[async_trait]
pub trait SnapshotSource: Send + Sync {
    /// Fetch the authoritative state for a principal. Revocation is distinct
    /// from never-known so pull, lag recovery, and restart preserve the
    /// generation watermark required for anti-resurrection semantics.
    async fn snapshot(&self, principal: Principal) -> Result<SnapshotResolution, StoreError>;

    /// Subscribe to pushes. A lagging receiver may miss updates; the
    /// contract is that a fresh `snapshot()` fetch after a lag error
    /// observes at least the newest generation.
    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush>;

    /// Every principal this source knows, including revoked ones — a
    /// tombstone is still a principal an instance must track, so that it
    /// knows the revocation (INVARIANTS.md #15).
    ///
    /// For instances that serve any customer rather than a configured slice
    /// (#48). Pushes alone cannot answer this: they carry deltas from the
    /// moment of subscribing, so a cold instance has no way to learn the set
    /// that already exists.
    ///
    /// `Ok(None)` means this source cannot enumerate, and the manager stays
    /// on its configured set — exactly today's behaviour. `Err` means
    /// enumeration *failed* and is retried. The two are deliberately
    /// distinct: collapsing them would let a broken source look like a
    /// limited one, and an instance would quietly serve a stale set forever.
    ///
    /// Defaulted so a source that has no catalogue — a test double, an
    /// embedder's own adapter — is unaffected.
    async fn principals(&self) -> Result<Option<Vec<Principal>>, StoreError> {
        Ok(None)
    }
}

/// Liveness of the backing store, for readiness probes: a server must not
/// report ready while its source of truth is unreachable (review finding
/// #11).
#[async_trait]
pub trait StoreHealth: Send + Sync {
    async fn ping(&self) -> Result<(), StoreError>;
}

/// Refusals from account creation (review finding #7): creation is never
/// destructive and never silently idempotent — recreating an existing
/// account is a surfaced error in every backend, because an overwrite would
/// reset balances/fencing under live leases and a silent no-op would hide
/// operator mistakes. Resetting an account is a deliberate, separate
/// workflow, not a create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateAccountError {
    AlreadyExists,
    Storage(StoreError),
}

impl std::fmt::Display for CreateAccountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CreateAccountError::AlreadyExists => f.write_str("account already exists"),
            CreateAccountError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for CreateAccountError {}

/// What a status transition actually did.
///
/// The blast radius of the operation, returned rather than logged, because an
/// operator suspending an account has no other way to learn it: the ledger
/// half is one row, but the snapshot half is however many credentials that
/// account has, and 204 says nothing.
///
/// `republished == 0` is the interesting value. It means the account had no
/// live snapshots to change — either it has no credentials yet, or the ones it
/// has are all revoked, or a status change was repeated and everything was
/// already at the target. All three are worth knowing at the moment of the
/// call rather than from a later denial.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatusChange {
    /// Live snapshots republished with the new status, at `generation + 1`.
    /// Excludes tombstones, which are never republished, and snapshots already
    /// carrying the target status, which are not rewritten.
    pub republished: usize,
    /// Rows that changed durably but could not be decoded well enough to push.
    ///
    /// Always zero in `MemoryStore`, which holds validated snapshots rather
    /// than encoded ones. In a stored backend a row can be undecodable — it
    /// already was before the transition touched it, and the request path
    /// already refuses it — and the transition deliberately does not fail
    /// whole over one corrupt credential. But it is not silently absorbed
    /// either: those principals did not get a push, so they will not converge
    /// until their next refresh.
    pub unreadable: usize,
}

/// Refusals from an account-status transition (#51).
///
/// Deliberately not an [`AllocateError`]: that enum's `NAMES`/`COUNT`/`index`
/// are the width of `LeaseCounters`' per-reason tally, and a status refusal
/// can never come out of `acquire`, so widening it would export a slot that
/// is permanently zero in every deployment. [`CreateAccountError`] is the
/// existing precedent for this shape — domain refusals plus `Storage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetStatusError {
    /// No such account. Never a silent no-op, and the same answer whichever
    /// status was asked for.
    UnknownAccount,
    /// [`AccountStatus::Closed`] is terminal: an account enters it from any
    /// status and leaves it never. The refusal changes nothing — not the
    /// ledger, not one snapshot, not one generation.
    AccountClosed,
    Storage(StoreError),
}

impl std::fmt::Display for SetStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetStatusError::UnknownAccount => f.write_str("unknown account"),
            SetStatusError::AccountClosed => f.write_str("account is closed"),
            SetStatusError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SetStatusError {}

impl From<StoreError> for SetStatusError {
    fn from(error: StoreError) -> Self {
        SetStatusError::Storage(error)
    }
}

/// Refusals from publishing a snapshot (#51).
///
/// `publish_snapshot` used to return a bare [`StoreError`], which left it free
/// to write a status contradicting the ledger and recreate the divergence
/// [`AdminStore::set_account_status`] exists to abolish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishSnapshotError {
    /// The snapshot's status disagrees with the account ledger. An account's
    /// status is changed through [`AdminStore::set_account_status`], which
    /// republishes; a publish may carry the current status but may not change
    /// it.
    StatusMismatch {
        ledger: AccountStatus,
        submitted: AccountStatus,
    },
    Storage(StoreError),
}

impl std::fmt::Display for PublishSnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublishSnapshotError::StatusMismatch { ledger, submitted } => write!(
                f,
                "snapshot status {} contradicts account status {}",
                submitted.as_str(),
                ledger.as_str()
            ),
            PublishSnapshotError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PublishSnapshotError {}

impl From<StoreError> for PublishSnapshotError {
    fn from(error: StoreError) -> Self {
        PublishSnapshotError::Storage(error)
    }
}

/// Administrative writes: the control plane's mutation surface. Kept apart
/// from the data-plane traits so a read-only replica can implement those
/// without this.
#[async_trait]
pub trait AdminStore: Send + Sync {
    async fn create_account(&self, config: AccountConfig) -> Result<(), CreateAccountError>;
    async fn deposit(&self, account: AccountId, units: CostUnits) -> Result<(), AllocateError>;
    /// Set an existing account's administrative status, in one transaction:
    /// the ledger's status, and a republication of every *live* snapshot of
    /// that account carrying the new status at `generation + 1`.
    ///
    /// This is the whole operator action. Before #51 the ledger flag and the
    /// published `AccountStatus` were two records with two propagation paths
    /// and nothing checking them against each other, so "deactivate" returned
    /// success while the request path kept admitting.
    ///
    /// Rules, all enforced here rather than by caller discipline:
    /// - A missing account is [`SetStatusError::UnknownAccount`], never a
    ///   silent no-op, whichever status was asked for.
    /// - [`AccountStatus::Closed`] is terminal
    ///   ([`SetStatusError::AccountClosed`]); `Closed` → `Closed` is a no-op.
    /// - Revoked principals are never republished: resurrecting a tombstone
    ///   is what INVARIANTS.md #15 forbids, and revocation stays a separate
    ///   per-credential mechanism.
    /// - Snapshots already at the target status are not rewritten, so a
    ///   repeat converges and bumps no generation.
    /// - Outstanding leases are **not** reclaimed. Lease acquisition refuses
    ///   at once, but admission stops only when the new snapshot installs —
    ///   one `SnapshotManager` refresh interval, and already-debited units
    ///   settle at release or TTL reclaim (#9).
    async fn set_account_status(
        &self,
        account: AccountId,
        status: AccountStatus,
    ) -> Result<StatusChange, SetStatusError>;
    /// Publish a principal's compiled snapshot.
    ///
    /// Refused with [`PublishSnapshotError::StatusMismatch`] when the
    /// snapshot's status contradicts the account ledger, so the two records
    /// [`set_account_status`](AdminStore::set_account_status) unifies cannot
    /// be pulled apart again one principal at a time. A snapshot whose
    /// account the ledger does not hold publishes unchanged, as before.
    async fn publish_snapshot(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
    ) -> Result<(), PublishSnapshotError>;
    async fn remove_snapshot(&self, principal: Principal) -> Result<(), StoreError>;
}

/// Outcome of one ingest batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "wire", derive(serde::Serialize, serde::Deserialize))]
pub struct IngestReport {
    /// Newly recorded events.
    pub accepted: u64,
    /// Events whose `request_id` was already recorded (idempotent replay —
    /// INVARIANTS.md #7).
    pub duplicate: u64,
    /// Events refused: unknown lease, lease-capability mismatch, or no
    /// remaining accounting capacity. These are bounded billing loss,
    /// visible to reconciliation.
    pub rejected: u64,
}

/// The billing ledger's write side.
#[async_trait]
pub trait UsageSink: Send + Sync {
    /// Record a batch. Idempotent on `request_id`; every event must match its
    /// stored `(lease_id, account_id, fencing_token)` capability before lease
    /// state and accounting capacity are checked. Partial acceptance is
    /// normal — the report says what happened.
    async fn ingest(
        &self,
        events: &[UsageEvent],
        now: Timestamp,
    ) -> Result<IngestReport, IngestError>;
}

/// One credential's durable record.
///
/// The `digest` is opaque here on purpose. The HMAC secret that produced it
/// lives with the verifier (`tollgate-auth`) and never reaches a store, so a
/// backend holds material that verifies nothing on its own — the property
/// `HmacRegistry` is built around, preserved across the persistence boundary.
/// A store that could compute a digest would be a store whose compromise is
/// sufficient to mint credentials.
///
/// `principal` is the digest's own truncation, so it is derived rather than
/// assigned: the request path is keyed by it, and per-credential revocation
/// is `install_revoked` for exactly this value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRecord {
    pub key_id: KeyId,
    pub account_id: AccountId,
    /// The principal this credential authenticates as: the leading 128 bits
    /// of `digest`.
    pub principal: Principal,
    /// HMAC-SHA256 of the secret under the verifier's server secret.
    pub digest: [u8; 32],
    /// When the credential stops being valid of its own accord, independent
    /// of revocation. Surfaced to the request path through
    /// `Verified::reusable_until`, so a session cache cannot outlive it.
    pub not_after: Option<Timestamp>,
}

/// What a revocation actually did.
///
/// Returned rather than inferred, for the reason [`StatusChange`] is: an
/// operator retiring a suspicious credential needs to know whether they
/// retired anything. "Already revoked" and "no such key" are different
/// answers to the same request and only one of them is a mistake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Revocation {
    /// The credential was live and is now retired.
    Retired,
    /// The credential was already retired; nothing changed.
    AlreadyRetired,
}

/// The largest usage batch any sink accepts in one call, in events.
///
/// A batch cap has to admit the largest batch the system can legitimately
/// produce, not a comfortable one: the shipped example writes 256 per flush,
/// and this leaves a factor of sixteen for an embedder that batches harder to
/// cut round-trips. Beyond it the answer is more flushes, not a bigger body —
/// an ingest is idempotent, so splitting costs a round trip and risks nothing.
///
/// `UsageWriterConfig::validate` refuses a `max_batch` above this, so the
/// misconfiguration is a startup error rather than a permanently-rejected
/// batch discovered in production (#61).
pub const MAX_INGEST_BATCH: usize = 4_096;

/// Why an ingest attempt failed, and whether replaying it unchanged could
/// ever succeed.
///
/// The distinction exists because the writer's correct response to the two is
/// opposite. An unreachable sink is a *duration*: retrying the same batch is
/// the designed behaviour, and giving up would lose billable events over a
/// blip. A refused batch is a *fact about the batch*: retrying it unchanged
/// gets the same answer forever, and every event queued behind it waits for a
/// recovery that cannot come — a permanent, deterministic error laundered
/// into an unbounded billing and availability outage (#61).
///
/// [`From<StoreError>`] yields [`Unavailable`](Self::Unavailable), so a
/// backend that does not classify keeps the retry-forever behaviour it had.
/// Terminality is asserted, never assumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestError {
    /// The sink could not be reached, or could not answer in time. The batch
    /// is unchanged and will be retried.
    Unavailable(StoreError),
    /// The sink refused this batch and will refuse it again unchanged: a body
    /// over the endpoint's limit, an event it cannot decode, a contract it
    /// does not implement. Retrying cannot help.
    Refused(StoreError),
}

impl IngestError {
    /// Whether replaying this batch unchanged could ever succeed.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, IngestError::Unavailable(_))
    }
}

impl From<StoreError> for IngestError {
    fn from(error: StoreError) -> Self {
        IngestError::Unavailable(error)
    }
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestError::Unavailable(e) => write!(f, "{e}"),
            IngestError::Refused(e) => write!(f, "refused: {e}"),
        }
    }
}

impl std::error::Error for IngestError {}

/// Refusals from credential lifecycle operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyError {
    /// No such credential. Never a silent no-op — an operator revoking a key
    /// that does not exist has either the wrong id or a false belief about
    /// what is live, and both are worth surfacing.
    UnknownKey,
    /// The credential's account does not exist, so nothing could authenticate
    /// as it. Refused at issuance rather than producing a key that verifies
    /// and is then denied by every admission.
    UnknownAccount,
    /// This `key_id` is already recorded. Issuance is never destructive, for
    /// the reason account creation is not ([`CreateAccountError`]): an
    /// overwrite would silently retire a live credential.
    AlreadyExists,
    Storage(StoreError),
}

impl std::fmt::Display for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyError::UnknownKey => f.write_str("no such credential"),
            KeyError::UnknownAccount => f.write_str("no such account"),
            KeyError::AlreadyExists => f.write_str("credential already exists"),
            KeyError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for KeyError {}

/// Durable credential lifecycle: the half of key management that outlives a
/// process and is shared by a fleet.
///
/// **Why this is a store trait rather than registry state.** Every other
/// control-plane fact here — accounts, balances, leases, statuses, snapshots —
/// is transactional in a backend with a local read projection, and credentials
/// are the same class of fact. An in-memory-only registry would lose issued
/// keys on restart, keep verifying a credential another instance revoked, and
/// make a fleet-wide active-key limit unenforceable, because no instance sees
/// the fleet.
///
/// **Durability before disclosure.** [`insert_key`](Self::insert_key) must
/// commit before its caller returns the secret to anyone. A crash between the
/// two hands out a credential the server has never heard of, which no later
/// reconciliation can repair: the digest is unrecoverable from the record,
/// which is the point of storing digests.
///
/// The verifier's projection is rebuilt from [`active_keys`](Self::active_keys),
/// so a backend decides what "active" means once, here, rather than in each
/// reader.
#[async_trait]
pub trait KeyDirectory: Send + Sync {
    /// Record a minted credential. The caller has already generated the
    /// secret and computed its digest; this stores what remains.
    async fn insert_key(&self, record: KeyRecord) -> Result<(), KeyError>;

    /// Retire one credential, reporting whether it was live.
    ///
    /// Revocation is durable and terminal: a retired credential is never
    /// resurrected, for the same reason a snapshot tombstone is not
    /// (INVARIANTS.md #15).
    async fn revoke_key(&self, key_id: KeyId, now: Timestamp) -> Result<Revocation, KeyError>;

    /// Every credential valid at `now`: not revoked, and not past its
    /// `not_after`. This is the projection's source of truth.
    async fn active_keys(&self, now: Timestamp) -> Result<Vec<KeyRecord>, StoreError>;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Clone, Copy)]
    enum ReclaimScript {
        FailFirst,
        FullBatchThenFail,
    }

    struct ScriptedReclaimer {
        script: ReclaimScript,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl LeaseAllocator for ScriptedReclaimer {
        async fn acquire(
            &self,
            _account: AccountId,
            _requested: CostUnits,
            _ttl: SignedDuration,
            _now: Timestamp,
        ) -> Result<LeaseGrant, AllocateError> {
            unreachable!("the full-drain tests only reclaim")
        }

        async fn release(
            &self,
            _lease_id: LeaseId,
            _fencing_token: FencingToken,
            _unspent: CostUnits,
            _now: Timestamp,
        ) -> Result<(), AllocateError> {
            unreachable!("the full-drain tests only reclaim")
        }

        async fn reclaim_expired_batch(
            &self,
            _now: Timestamp,
            limit: NonZeroUsize,
        ) -> Result<ReclaimBatch, StoreError> {
            let call = self.calls.fetch_add(1, Ordering::AcqRel);
            if matches!(self.script, ReclaimScript::FailFirst) || call > 0 {
                return Err(StoreError("scripted reclaim failure".into()));
            }
            let reclaimed = (0..limit.get())
                .map(|id| ReclaimedLease {
                    lease_id: LeaseId(u128::try_from(id).unwrap()),
                    account_id: AccountId(1),
                    reclaimed: CostUnits(1),
                })
                .collect();
            ReclaimBatch::try_new(reclaimed, limit)
        }
    }

    /// Every variant, once. Sized by `COUNT`, so adding a refusal without
    /// widening this array fails to compile.
    fn all() -> [AllocateError; AllocateError::COUNT] {
        [
            AllocateError::UnknownAccount,
            AllocateError::AccountInactive,
            AllocateError::InsufficientBalance,
            AllocateError::InvalidTtl,
            AllocateError::UnknownLease,
            AllocateError::Fenced,
            AllocateError::LeaseNotActive,
            AllocateError::InvalidRelease,
            AllocateError::Storage(StoreError("connection reset".into())),
        ]
    }

    /// The indices must be a permutation of `0..COUNT`: two refusals sharing
    /// a slot would silently merge their tallies, and a slot no refusal maps
    /// to would export a counter that can never move.
    #[test]
    fn indices_cover_every_slot_exactly_once() {
        let mut seen = [false; AllocateError::COUNT];
        for error in all() {
            let index = error.index();
            assert!(index < AllocateError::COUNT, "{error} indexes out of range");
            assert!(!seen[index], "{error} shares slot {index}");
            seen[index] = true;
        }
        assert!(seen.iter().all(|hit| *hit), "every slot must be claimed");
    }

    /// Labels are read by whatever scrapes the counters, so they are a
    /// contract: distinct, and free of the backend text `Display` carries.
    /// `Storage` wraps an arbitrary message, so labelling by rendered text
    /// would mint a fresh time series per connection failure.
    #[test]
    fn labels_are_distinct_and_free_of_backend_text() {
        for (position, error) in all().iter().enumerate() {
            assert_eq!(error.name(), AllocateError::NAMES[position]);
        }
        let storage = AllocateError::Storage(StoreError("connection reset".into()));
        assert_eq!(storage.name(), "storage");
        assert!(
            !storage.name().contains("connection"),
            "the label must not carry the backend's message"
        );
        let mut names = AllocateError::NAMES;
        names.sort_unstable();
        names.iter().reduce(|previous, next| {
            assert_ne!(previous, next, "duplicate label {next}");
            next
        });
    }

    /// The payload must not affect the slot, or one refusal would scatter
    /// across slots as its message varied.
    #[test]
    fn payload_does_not_affect_the_slot() {
        assert_eq!(
            AllocateError::Storage(StoreError("a".into())).index(),
            AllocateError::Storage(StoreError("b".into())).index()
        );
    }

    #[test]
    fn conservation_requires_an_exact_equation_without_overflow() {
        let balanced = Conservation {
            deposited: CostUnits(10),
            overage_recorded: CostUnits::ZERO,
            balance: CostUnits(1),
            active_lease_grants: CostUnits(2),
            settled_usage: CostUnits(3),
            settlement_loss: CostUnits(4),
        };
        assert!(balanced.holds());

        let drifted = Conservation {
            deposited: CostUnits(11),
            ..balanced
        };
        assert!(!drifted.holds());

        let overflowing = Conservation {
            deposited: CostUnits(u64::MAX),
            overage_recorded: CostUnits::ZERO,
            balance: CostUnits(u64::MAX),
            active_lease_grants: CostUnits(1),
            settled_usage: CostUnits::ZERO,
            settlement_loss: CostUnits::ZERO,
        };
        assert!(!overflowing.holds());
    }

    /// Overage funds the left side, so usage it paid for closes the equation
    /// rather than breaking it — and the same numbers without the funding term
    /// must *not* balance, or the field would be decorative.
    #[test]
    fn overage_funds_the_usage_it_bills() {
        let elastic = Conservation {
            deposited: CostUnits(10),
            overage_recorded: CostUnits(5),
            balance: CostUnits(1),
            active_lease_grants: CostUnits(2),
            settled_usage: CostUnits(8),
            settlement_loss: CostUnits(4),
        };
        assert!(elastic.holds());
        assert!(
            !Conservation {
                overage_recorded: CostUnits::ZERO,
                ..elastic
            }
            .holds(),
            "the same ledger without the funding term must fail by exactly the overage"
        );
    }

    /// The left side is checked too. Overflowing the funding sum answers
    /// `false` rather than wrapping to a total that might coincidentally match
    /// the right side (INVARIANTS.md #11).
    #[test]
    fn overflowing_the_funding_sum_is_a_violation_not_a_wrap() {
        let overflowing = Conservation {
            deposited: CostUnits(u64::MAX),
            overage_recorded: CostUnits(1),
            balance: CostUnits::ZERO,
            active_lease_grants: CostUnits::ZERO,
            settled_usage: CostUnits::ZERO,
            settlement_loss: CostUnits::ZERO,
        };
        assert!(!overflowing.holds());
    }

    #[test]
    fn reclaim_batch_reports_an_empty_result() {
        let batch = ReclaimBatch::try_new(Vec::new(), NonZeroUsize::new(2).unwrap()).unwrap();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
        assert!(!batch.is_saturated());
        assert!(batch.reclaimed().is_empty());
    }

    #[tokio::test]
    async fn full_drain_preserves_an_initial_batch_error() {
        let allocator = ScriptedReclaimer {
            script: ReclaimScript::FailFirst,
            calls: AtomicUsize::new(0),
        };
        let expected = StoreError("scripted reclaim failure".into());
        assert_eq!(
            allocator.reclaim_expired(Timestamp::MIN).await,
            Err(expected)
        );
        assert_eq!(allocator.calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn full_drain_reports_progress_before_a_later_batch_error() {
        let allocator = ScriptedReclaimer {
            script: ReclaimScript::FullBatchThenFail,
            calls: AtomicUsize::new(0),
        };
        let original = StoreError("scripted reclaim failure".into());
        let error = allocator.reclaim_expired(Timestamp::MIN).await.unwrap_err();
        assert_ne!(error, original);
        assert!(error.0.contains("256 leases"));
        assert!(error.0.contains("256 units"));
        assert_eq!(allocator.calls.load(Ordering::Acquire), 2);
    }

    /// The push-capacity boundary, pinned where both backends read it.
    ///
    /// Strictly greater is the whole content of the rule: a batch that exactly
    /// fills the channel is delivered, so warning at equality would fire on the
    /// largest successful case and train an operator to ignore it. Mutation
    /// testing found this untested — the comparison could be flipped to `<`,
    /// `<=` or `>=` and every scenario stayed green, because nothing observed
    /// the warning at all (#51).
    #[test]
    fn the_push_capacity_warning_fires_only_above_the_channel() {
        assert!(
            !pushes_exceed_capacity(0),
            "an empty batch is not a capacity problem"
        );
        assert!(!pushes_exceed_capacity(PUSH_CHANNEL_CAPACITY - 1));
        assert!(
            !pushes_exceed_capacity(PUSH_CHANNEL_CAPACITY),
            "a batch that exactly fills the channel is still delivered"
        );
        assert!(
            pushes_exceed_capacity(PUSH_CHANNEL_CAPACITY + 1),
            "one more than the channel holds is what makes a subscriber lag"
        );
    }
}
