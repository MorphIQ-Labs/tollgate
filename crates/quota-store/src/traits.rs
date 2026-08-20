//! The storage traits and their shared vocabulary.

use std::sync::Arc;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use quota_core::{
    AccountId, AccountSnapshot, CostUnits, FencingToken, LeaseGrant, LeaseId, Principal, UsageEvent,
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
}

impl Default for GrantPolicy {
    fn default() -> Self {
        GrantPolicy {
            shrink_divisor: 2,
            min_grant: CostUnits(1),
            max_ttl: SignedDuration::from_secs(300),
        }
    }
}

impl GrantPolicy {
    /// The units a request for `requested` receives from `balance`, or `None`
    /// when the balance cannot fund any grant.
    #[must_use]
    pub fn grant(&self, requested: CostUnits, balance: CostUnits) -> Option<CostUnits> {
        if balance.is_zero() {
            return None;
        }
        let cap = (balance.get() / self.shrink_divisor.max(1)).max(self.min_grant.get());
        Some(CostUnits(
            requested.get().min(cap).min(balance.get()).max(1),
        ))
    }
}

/// One lease settled by an expiry sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    /// Settle every active lease whose TTL has lapsed, crediting
    /// `granted - recorded usage` back to each account (INVARIANTS.md #9).
    /// Backends run this from a maintenance task; it must be safe to run
    /// concurrently with everything else.
    async fn reclaim_expired(&self, now: Timestamp) -> Result<Vec<ReclaimedLease>, StoreError>;
}

/// One pushed snapshot update.
#[derive(Debug, Clone)]
pub struct SnapshotPush {
    pub principal: Principal,
    pub snapshot: Arc<AccountSnapshot>,
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

/// Outcome of one ingest batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
