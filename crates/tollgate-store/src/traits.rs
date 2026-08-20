//! The storage traits and their shared vocabulary.

use std::sync::Arc;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountSnapshot, CostUnits, FencingToken, Generation, LeaseGrant, LeaseId,
    Principal, UsageEvent,
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
    /// The fencing token does not match the lease — a stale or partitioned
    /// holder (INVARIANTS.md #4).
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

/// Atomic lease allocation against the account balance — the amortization
/// point: one `acquire` funds thousands of local reservations.
#[async_trait]
pub trait LeaseAllocator: Send + Sync {
    /// Atomically debit a grant from the account. The granted size follows
    /// the backend's [`GrantPolicy`] and may be smaller than `requested`;
    /// the fencing token is strictly monotonic per account.
    async fn acquire(
        &self,
        account: AccountId,
        requested: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<LeaseGrant, AllocateError>;

    /// Graceful return: credit `unspent` back and close the lease. The
    /// holder must flush usage for this lease *before* releasing — events
    /// arriving for a settled lease are rejected.
    async fn release(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        now: Timestamp,
    ) -> Result<(), AllocateError>;

    /// Settle every active lease whose TTL (plus the policy's reclaim grace)
    /// has lapsed, crediting `granted - recorded usage` back to each account
    /// (INVARIANTS.md #9). Backends run this from a maintenance task; it
    /// must be safe to run concurrently with everything else.
    async fn reclaim_expired(&self, now: Timestamp) -> Result<Vec<ReclaimedLease>, StoreError>;
}

/// One pushed snapshot update.
#[derive(Debug, Clone)]
pub struct SnapshotPush {
    pub principal: Principal,
    /// `Some` publishes a snapshot; `None` is a revocation tombstone.
    pub snapshot: Option<Arc<AccountSnapshot>>,
    /// Source-side generation watermark. Revocations retain the generation
    /// they removed so delayed positive pushes cannot resurrect them.
    pub generation: Option<Generation>,
}

/// Where compiled snapshots come from.
#[async_trait]
pub trait SnapshotSource: Send + Sync {
    /// Fetch the current snapshot for a principal; `None` is a confirmed
    /// unknown (candidate for the admission layer's negative cache).
    async fn snapshot(
        &self,
        principal: Principal,
    ) -> Result<Option<Arc<AccountSnapshot>>, StoreError>;

    /// Subscribe to pushes. A lagging receiver may miss updates; the
    /// contract is that a fresh `snapshot()` fetch after a lag error
    /// observes at least the newest generation.
    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush>;
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

/// Administrative writes: the control plane's mutation surface. Kept apart
/// from the data-plane traits so a read-only replica can implement those
/// without this.
#[async_trait]
pub trait AdminStore: Send + Sync {
    async fn create_account(
        &self,
        config: crate::memory::AccountConfig,
    ) -> Result<(), CreateAccountError>;
    async fn deposit(&self, account: AccountId, units: CostUnits) -> Result<(), AllocateError>;
    async fn set_active(&self, account: AccountId, active: bool) -> Result<(), AllocateError>;
    async fn publish_snapshot(
        &self,
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
    ) -> Result<(), StoreError>;
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
    /// Events refused: unknown lease, fencing mismatch, or settled lease.
    /// These are bounded billing loss, visible to reconciliation.
    pub rejected: u64,
}

/// The billing ledger's write side.
#[async_trait]
pub trait UsageSink: Send + Sync {
    /// Record a batch. Idempotent on `request_id`; fencing-checked per event.
    /// Partial acceptance is normal — the report says what happened.
    async fn ingest(
        &self,
        events: &[UsageEvent],
        now: Timestamp,
    ) -> Result<IngestReport, StoreError>;
}
