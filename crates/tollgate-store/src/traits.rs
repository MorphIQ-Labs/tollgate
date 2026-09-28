//! The storage traits and their shared vocabulary.

use std::num::NonZeroUsize;

use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::broadcast;

use tollgate_core::{
    AccountId, AccountStatus, BudgetSchedule, CapacityClass, CostUnits, FencingToken, Generation,
    KeyId, LeaseGrant, LeaseId, Principal, PublishableSnapshot, UsageEvent,
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
    /// No such account.
    UnknownAccount,
    /// The account exists but is not in a state that may spend.
    AccountInactive,
    /// Nothing left to lease. Distinct from `AccountInactive`: the client
    /// should keep polling, because usage settlement or a top-up can restore
    /// balance.
    InsufficientBalance,
    /// The ledger confirms no funding remains, including outstanding leases.
    BalanceExhausted(tollgate_core::BalanceExhaustion),
    /// No grant is possible, and the ledger confirms how much funding remains
    /// outside this instance's reach — all of it held in other leases.
    /// `remaining` is never zero; zero is [`Self::BalanceExhausted`].
    /// `InsufficientBalance` stays the refusal that carries no attestation.
    BalanceInsufficient(tollgate_core::BalanceShortfall),
    /// A lease must specify one unambiguous, strictly positive lifetime.
    InvalidTtl,
    /// No lease record has this `lease_id`.
    UnknownLease,
    /// The fencing token does not match the lease record named by `lease_id`.
    /// Token ordering across different active leases is irrelevant
    /// (INVARIANTS.md GL-4).
    Fenced,
    /// The lease exists but is no longer active (already released, expired,
    /// or reclaimed).
    LeaseNotActive,
    /// A release claimed more unspent units than the lease can still hold
    /// (`unspent + recorded usage > granted`) — a client accounting bug,
    /// surfaced rather than absorbed.
    InvalidRelease,
    /// A deposit exceeds the backend's unit domain or would overflow its
    /// top-up balance or lifetime deposited total. Nothing changes; do not retry.
    BalanceOverflow,
    /// A backend failure unrelated to domain rules; see [`StoreError`].
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
        "balance_exhausted",
        "balance_insufficient",
        "balance_overflow",
    ];

    /// How many distinct refusals exist — the width of a per-reason tally.
    pub const COUNT: usize = 12;

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
            AllocateError::BalanceExhausted(_) => 9,
            AllocateError::BalanceInsufficient(_) => 10,
            AllocateError::BalanceOverflow => 11,
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
            AllocateError::BalanceExhausted(_) => f.write_str("account balance exhausted"),
            AllocateError::BalanceInsufficient(evidence) => write!(
                f,
                "insufficient balance ({} units remain, all held in leases)",
                evidence.remaining
            ),
            AllocateError::InvalidTtl => {
                f.write_str("lease TTL must specify one positive duration")
            }
            AllocateError::UnknownLease => f.write_str("unknown lease"),
            AllocateError::Fenced => f.write_str("fencing token mismatch"),
            AllocateError::LeaseNotActive => f.write_str("lease not active"),
            AllocateError::InvalidRelease => f.write_str("invalid release"),
            AllocateError::BalanceOverflow => {
                f.write_str("deposit exceeds account funding capacity")
            }
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
    /// The new account's identifier.
    pub account_id: AccountId,
    /// Opening balance. Deposited as a top-up, so it counts toward
    /// [`Conservation::deposited`] and never expires at a period boundary.
    pub initial_balance: CostUnits,
    /// The account's administrative status at birth. Anything but
    /// [`AccountStatus::Active`] refuses leases while keeping the ledger, and
    /// `Closed` is terminal from creation onwards (INVARIANTS.md GL-22).
    ///
    /// An [`AccountStatus`] rather than a bool so creation and
    /// [`AdminStore::set_account_status`] speak one vocabulary about one
    /// column; the bool could not express `Closed`, which is what let
    /// terminality be a convention instead of a check (GL-51).
    pub status: AccountStatus,
    /// The account's execution-capacity class at birth (GL-99).
    ///
    /// Present at creation for the reason `status` is: creation and
    /// [`AdminStore::set_capacity_class`] speak one vocabulary about one
    /// column, so there is no window in which an account exists without a
    /// class and no second place that decides the default.
    pub capacity_class: CapacityClass,
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
    /// Every unit ever deposited: the opening balance, top-ups, and periodic
    /// allowances. Monotonic.
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
    /// Spendable units not out on lease: allowance plus top-up.
    pub balance: CostUnits,
    /// Units granted to leases that have not settled, including usage already
    /// recorded against them.
    pub active_lease_grants: CostUnits,
    /// Usage billed against leases that have settled (released or expired),
    /// plus all overage usage — which belongs to no lease and is therefore
    /// settled the moment it is recorded. Usage on active leases is inside
    /// `active_lease_grants`.
    pub settled_usage: CostUnits,
    /// Units granted to leases that settled without usage accounting for them:
    /// unclaimed at release or forfeited at expiry reclaim. Usage for such a lease
    /// that arrives later moves units from here into `settled_usage`.
    pub settlement_loss: CostUnits,
    /// Units that were funded but will never be spent, because the period
    /// that funded them ended (GL-97).
    ///
    /// A resting place on the right of the equation, beside `settlement_loss`
    /// and for the same reason: both are units the account was funded with
    /// that no longer sit in a balance, on a lease, or in billed usage.
    /// Without it, an allowance that resets each month would make the equation
    /// fail by exactly the unspent remainder — the ledger reporting corruption
    /// every time a budget did the one thing it exists to do.
    ///
    /// Monotonic, like `deposited` and `overage_recorded`: expiry is a fact
    /// about a period that has closed, and closing a period is not reversible.
    pub expired: CostUnits,
}

impl Conservation {
    /// `deposited + overage == balance + active grants + settled usage + loss
    /// + expired`, exactly.
    ///
    /// Both sides accumulate with checked arithmetic and an overflow answers
    /// `false`, never a wrap or a panic: this function exists to *detect*
    /// corrupt ledger state, so arithmetic that could not represent the state
    /// must report a violation rather than quietly produce a total that
    /// happens to match (INVARIANTS.md GL-11).
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
            self.expired,
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
/// holder's oversized lease. A consolidation may exceed the cap only to fund
/// a quote the holder already refused ([`Self::consolidation_grant`]), which
/// is demand rather than hoarding.
#[derive(Debug, Clone, Copy)]
pub struct GrantPolicy {
    /// Divisor applied to the balance to cap a grant: the cap is
    /// `balance / shrink_divisor`, floored at `min_grant`. `1` caps at the whole
    /// balance. Must be positive.
    pub shrink_divisor: u64,
    /// Floor for the shrink cap, so a shrinking balance still grants leases of a
    /// useful size; a grant never exceeds the balance. Must be positive.
    pub min_grant: CostUnits,
    /// Hard cap on any single lease's TTL; requests beyond it are clamped.
    pub max_ttl: SignedDuration,
    /// How long past a lease's `expires_at` the allocator waits before
    /// reclaiming its unspent units. Holders stop spending at
    /// `expires_at - safety margin` (their side of the protocol), so work
    /// committed inside the usability window has `margin + grace` to be
    /// flushed and billed before settlement could reject it. Releases are
    /// also accepted through the grace window (review finding GL-1).
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
    /// Greatest expiry whose full grace window has elapsed at `now`.
    /// A validated policy has nonnegative grace; subtraction underflow means
    /// no representable expiry is due. In particular, a deadline beyond
    /// Timestamp::MAX is never shortened to that last representable instant.
    pub fn reclaim_cutoff(&self, now: Timestamp) -> Option<Timestamp> {
        now.checked_sub(self.reclaim_grace).ok()
    }

    /// Check that the policy is safe to allocate with: `shrink_divisor`,
    /// `min_grant` and `max_ttl` positive, and `reclaim_grace` nonnegative. Backends
    /// call this at construction.
    ///
    /// # Errors
    ///
    /// A [`GrantPolicyError`] naming the first invalid field.
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

    /// The units a consolidation re-grants, or `None` exactly when
    /// [`Self::grant`] refuses.
    ///
    /// `balance` already includes the credit the exchange restores, and
    /// `floor` is that credit: the ordinary answer may not shrink the holding
    /// (GL-109). `needed` is the largest quote the returned lease refused, and
    /// the answer grows to it when `balance` can fund it (GL-131). The shrink
    /// cap stops one holder hoarding a small balance ahead of demand; a quote
    /// the holder already failed to fund is demand, and the request that
    /// proved it spends it. A quote `balance` cannot fund grows nothing,
    /// because no grant would serve it. A plain acquire is `floor` and
    /// `needed` both zero, which is [`Self::grant`] unchanged.
    #[must_use]
    pub fn consolidation_grant(
        &self,
        requested: CostUnits,
        balance: CostUnits,
        floor: CostUnits,
        needed: CostUnits,
    ) -> Option<CostUnits> {
        let sized = self.grant(requested, balance)?.max(floor.min(balance));
        Some(if needed <= balance {
            sized.max(needed)
        } else {
            sized
        })
    }
}

/// One lease settled by an expiry sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "wire", derive(serde::Serialize, serde::Deserialize))]
pub struct ReclaimedLease {
    /// The lease the sweep settled.
    pub lease_id: LeaseId,
    /// The account the lease was drawn from.
    pub account_id: AccountId,
    /// Units recorded as provisional settlement loss: `granted - recorded
    /// usage` at the sweep. Nothing is credited back, because a holder that
    /// never released cannot prove any unit unspent (GL-136); usage for the
    /// lease that arrives later converts loss into billed usage.
    pub forfeited: CostUnits,
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

    /// The leases this batch settled.
    #[must_use]
    pub fn reclaimed(&self) -> &[ReclaimedLease] {
        &self.reclaimed
    }

    /// How many leases this batch settled.
    #[must_use]
    pub fn len(&self) -> usize {
        self.reclaimed.len()
    }

    /// Whether this batch settled nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.reclaimed.is_empty()
    }

    /// Whether the batch reached its limit, so more expired leases may remain and
    /// the caller should run another batch.
    #[must_use]
    pub fn is_saturated(&self) -> bool {
        self.saturated
    }

    /// Consume the batch, returning the leases it settled.
    #[must_use]
    pub fn into_reclaimed(self) -> Vec<ReclaimedLease> {
        self.reclaimed
    }
}

/// A grant, and the ledger's remaining funding as of the transaction that
/// made it.
///
/// `funding` is read from the committed ledger after the grant and any
/// consolidation settlement, so it counts the new lease's units. It is an
/// upper bound on what the account can still spend (see
/// [`tollgate_core::BalanceShortfall`]), carried apart from the grant because
/// the grant is a capability and this is evidence about the account. `None`
/// means the answering allocator attested nothing, as an older server does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "wire", derive(serde::Serialize, serde::Deserialize))]
pub struct Allocation {
    /// The lease capability. Its fields are flattened into the wire object.
    #[cfg_attr(feature = "wire", serde(flatten))]
    pub grant: LeaseGrant,
    /// The account's remaining funding after this grant, or `None` when the
    /// allocator attested nothing. Omitted from the wire when `None`.
    #[cfg_attr(
        feature = "wire",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub funding: Option<tollgate_core::BalanceShortfall>,
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
    ) -> Result<Allocation, AllocateError>;

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

    /// Atomically return an active lease's `unspent` units and re-grant
    /// against the restored balance: [`release`](Self::release) followed by
    /// [`acquire`](Self::acquire), in one transaction, for the same account
    /// the lease names.
    ///
    /// This exists because the holder cannot compose it from the two calls.
    /// A holder whose grant is too small for the work it is being offered is
    /// holding exactly the units the next grant needs, and separating the
    /// return from the request loses them twice over: the
    /// [`GrantPolicy`] re-sizes against a balance the returned units have
    /// already rejoined, so a `shrink_divisor` above one can hand back
    /// *less* than was returned (49 units returned into a balance of 58
    /// re-grants 29 under the default policy), and in the gap between the two
    /// calls another instance can take them. Neither is recoverable by the
    /// holder, which is why the exchange belongs to the component that owns
    /// both the policy and the transaction (INVARIANTS.md GL-1, GL-6).
    ///
    /// **The grant is never smaller than the credit actually restored.**
    /// Allowance funded by a closed period expires at settlement; only its
    /// surviving top-up credit supplies a floor. Otherwise all `unspent`
    /// units return. The result is the larger of this floor and the ordinary
    /// policy grant, so it can exceed `requested` when preserving a larger
    /// holding. A zero request or a balance unable to fund any grant refuses
    /// the whole exchange.
    ///
    /// **The grant grows to `needed` when the restored balance can fund it.**
    /// `needed` is the largest quote the returned lease refused, zero when
    /// none. The shrink cap exists so one holder cannot hoard a small balance
    /// ahead of demand; a refused quote is demand already proven, so under a
    /// `shrink_divisor` above one it is what lets a single holder reach a
    /// quote above `balance / shrink_divisor` at all. A `needed` the balance
    /// cannot fund changes nothing. Sizing is
    /// [`GrantPolicy::consolidation_grant`] in every backend.
    ///
    /// The transaction applies both halves or neither. A domain refusal
    /// (`InsufficientBalance`, `BalanceExhausted`, `BalanceInsufficient`,
    /// `UnknownAccount`, `AccountInactive`, or `InvalidTtl`) leaves the
    /// original lease unchanged.
    /// `InvalidRelease` also leaves it unchanged but reports an
    /// accounting-integrity fault.
    /// `UnknownLease`, `Fenced`, and `LeaseNotActive` provide no authority to
    /// resume spending from the old lease.
    ///
    /// **`Storage`, timeouts, and cancellation have an ambiguous outcome.**
    /// The transaction may have committed before its reply was lost, including
    /// during an HTTP response or transaction-commit failure. A holder must
    /// keep the old lease out of service and attempt its release; reinstating
    /// it could spend credited units twice. The unanswered replacement grant
    /// cannot be recovered through the old capability, must be reported as
    /// uncertain, and remains bounded by TTL reclaim.
    #[allow(
        clippy::too_many_arguments,
        reason = "one transactional exchange: the release half's capability and credit, the \
                  grant half's size and demand, and the shared lifetime and clock"
    )]
    async fn consolidate(
        &self,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        unspent: CostUnits,
        requested: CostUnits,
        needed: CostUnits,
        ttl: SignedDuration,
        now: Timestamp,
    ) -> Result<Allocation, AllocateError>;

    /// Settle at most `limit` active leases whose TTL (plus the policy's
    /// reclaim grace) has lapsed and whose holder never released them.
    ///
    /// Nothing is credited back. Each lease's `granted - recorded usage` is
    /// recorded as provisional settlement loss, exactly as a release claiming
    /// nothing unspent would record it, because a holder that never released
    /// cannot prove any unit unspent: it may have committed work it never
    /// flushed (INVARIANTS.md GL-9, GL-136). Usage for the lease that arrives
    /// later fits in that loss and converts it into billed usage.
    ///
    /// One call is one bounded atomic
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
        drain_reclaim_expired(self, now).await
    }
}

/// The drain loop [`LeaseAllocator::reclaim_expired`] performs, as a free
/// function so that an override can reuse it instead of re-deriving it.
///
/// Rust has no `super` for a trait default, so a wrapper that overrides
/// `reclaim_expired` cannot call the body it overrides. Without this it must
/// choose between forwarding to an inner allocator — which silently discards
/// the wrapper's own `reclaim_expired_batch` override, and so discards any
/// failure that override injects — and copying this loop, which is how two
/// copies drift apart. Calling this keeps one body and re-dispatches every
/// batch through `allocator`, whatever `allocator` is (GL-83).
///
/// `#[doc(hidden)]` marks it cross-crate-visible for that purpose rather than
/// part of the documented surface, as [`Reservation::reserve_at_locality`] is
/// in `tollgate-core`.
///
/// [`Reservation::reserve_at_locality`]: https://docs.rs/tollgate-core
#[doc(hidden)]
pub async fn drain_reclaim_expired<A>(
    allocator: &A,
    now: Timestamp,
) -> Result<Vec<ReclaimedLease>, StoreError>
where
    A: LeaseAllocator + ?Sized,
{
    let mut reclaimed: Vec<ReclaimedLease> = Vec::new();
    loop {
        let batch = match allocator
            .reclaim_expired_batch(now, DEFAULT_RECLAIM_BATCH_LIMIT)
            .await
        {
            Ok(batch) => batch,
            Err(error) if reclaimed.is_empty() => return Err(error),
            Err(error) => {
                let units: u128 = reclaimed
                    .iter()
                    .map(|lease| u128::from(lease.forfeited.get()))
                    .sum();
                return Err(StoreError(format!(
                    "reclaim drain failed after {} leases forfeiting {units} units were committed: {error}",
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

/// Authoritative state returned by a snapshot pull or push.
#[derive(Debug, Clone)]
pub enum SnapshotResolution {
    /// A compiled snapshot is currently authoritative.
    Present(PublishableSnapshot),
    /// The principal existed but was revoked at this generation. Sources must
    /// retain this watermark so a delayed older positive cannot resurrect it.
    Revoked {
        /// The generation the tombstone was published at. A positive snapshot
        /// at or below it cannot resurrect the principal (INVARIANTS.md 15).
        generation: Generation,
    },
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
/// A status change republishes every live snapshot of an account (GL-51), so a
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
    /// The principal whose authoritative state changed.
    pub principal: Principal,
    /// Its new authoritative state.
    pub resolution: SnapshotResolution,
}

/// Where compiled snapshots come from.
#[async_trait]
pub trait SnapshotSource: Send + Sync {
    /// Fetch the authoritative state for a principal. Revocation is distinct
    /// from never-known so pull, lag recovery, and restart preserve the
    /// generation watermark required for anti-resurrection semantics.
    /// Reads used to reconstruct reclaimed local history must be linearizable
    /// against durable publications/tombstones. Start a new source operation;
    /// an earlier cached response or lagging replica cannot establish that
    /// principal's forgotten generation floor. Return an error if this
    /// authority is unavailable. MemoryStore and primary PostgresStore reads
    /// supply this ordering; HTTP deployments must preserve it end to end.
    async fn snapshot(&self, principal: Principal) -> Result<SnapshotResolution, StoreError>;

    /// Subscribe to pushes. A lagging receiver may miss updates; the
    /// contract is that a fresh `snapshot()` fetch after a lag error
    /// observes at least the newest generation.
    fn subscribe(&self) -> broadcast::Receiver<SnapshotPush>;

    /// Every principal this source knows, including revoked ones — a
    /// tombstone is still a principal an instance must track, so that it
    /// knows the revocation (INVARIANTS.md GL-15).
    ///
    /// For instances that serve any customer rather than a configured slice
    /// (GL-48). Pushes alone cannot answer this: they carry deltas from the
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
/// GL-11).
#[async_trait]
pub trait StoreHealth: Send + Sync {
    /// Succeed only when the backing store can currently answer.
    /// `MemoryStore` always succeeds; `PostgresStore` runs a trivial query.
    ///
    /// # Errors
    ///
    /// A [`StoreError`] when the store cannot be reached.
    async fn ping(&self) -> Result<(), StoreError>;
}

/// Refusals from account creation (review finding GL-7): creation is never
/// destructive and never silently idempotent — recreating an existing
/// account is a surfaced error in every backend, because an overwrite would
/// reset balances/fencing under live leases and a silent no-op would hide
/// operator mistakes. Resetting an account is a deliberate, separate
/// workflow, not a create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateAccountError {
    /// An account with this id already exists. It is left untouched.
    AlreadyExists,
    /// A backend failure unrelated to domain rules; see [`StoreError`].
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

/// One account moved across a period boundary by a rollover pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RolledAccount {
    /// The account whose period boundary was crossed.
    pub account_id: AccountId,
    /// The new period's allowance, deposited by this pass.
    pub deposited: CostUnits,
    /// Unspent allowance from the period that just closed. Manual top-ups are
    /// never included: they persist across a boundary (GL-97).
    pub expired: CostUnits,
}

/// The production-sized upper bound for one rollover transaction.
///
/// Every scheduled account comes due at the same instant — that is what a
/// calendar boundary means — so this is not a cap on a rare backlog but the
/// normal shape of the first pass after midnight on the 1st. It bounds locks,
/// row materialization, and transaction size per statement; the sweep drains
/// saturated batches until it reaches a partial one, exactly as the expiry
/// reclaim does.
pub const DEFAULT_ROLLOVER_BATCH_LIMIT: NonZeroUsize =
    NonZeroUsize::new(256).expect("the rollover batch limit is nonzero");

/// Verified evidence returned by one bounded rollover transaction.
///
/// Private fields, so `saturated` cannot disagree with the requested limit —
/// the same contract [`ReclaimBatch`] carries, and for the same reason: the
/// caller decides whether to ask for another batch from this, without
/// re-deriving the backend's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloverBatch {
    rolled: Vec<RolledAccount>,
    saturated: bool,
}

impl RolloverBatch {
    /// Build a batch and derive its saturation evidence from `limit`.
    pub fn try_new(rolled: Vec<RolledAccount>, limit: NonZeroUsize) -> Result<Self, StoreError> {
        if rolled.len() > limit.get() {
            return Err(StoreError(format!(
                "rollover backend returned {} accounts for a batch limit of {}",
                rolled.len(),
                limit
            )));
        }
        Ok(RolloverBatch {
            saturated: rolled.len() == limit.get(),
            rolled,
        })
    }

    /// The accounts this batch rolled.
    #[must_use]
    pub fn rolled(&self) -> &[RolledAccount] {
        &self.rolled
    }

    /// How many accounts this batch rolled.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rolled.len()
    }

    /// Whether this batch rolled nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rolled.is_empty()
    }

    /// Whether the batch reached its limit, so more due accounts may remain and
    /// the caller should run another batch.
    #[must_use]
    pub fn is_saturated(&self) -> bool {
        self.saturated
    }
}

/// Refusals from setting or rolling a budget schedule (GL-97).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetError {
    /// No such account. Never a silent no-op: an operator setting a schedule
    /// on a mistyped id has to learn it now rather than at the next boundary.
    UnknownAccount,
    /// A backend failure unrelated to domain rules; see [`StoreError`].
    Storage(StoreError),
}

impl std::fmt::Display for BudgetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BudgetError::UnknownAccount => f.write_str("no such account"),
            BudgetError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BudgetError {}

impl From<StoreError> for BudgetError {
    fn from(error: StoreError) -> Self {
        BudgetError::Storage(error)
    }
}

/// Refusals from an account-status transition (GL-51).
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
    /// A backend failure unrelated to domain rules; see [`StoreError`].
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

/// Refusals from publishing a snapshot (GL-51).
///
/// `publish_snapshot` used to return a bare [`StoreError`], which left it free
/// to write a status contradicting the ledger and recreate the divergence
/// [`AdminStore::set_account_status`] exists to abolish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishSnapshotError {
    /// The stated credential is missing or belongs to another principal/account.
    CredentialMismatch {
        /// The credential the snapshot states.
        key_id: KeyId,
    },
    /// The snapshot's status disagrees with the account ledger. An account's
    /// status is changed through [`AdminStore::set_account_status`], which
    /// republishes; a publish may carry the current status but may not change
    /// it.
    StatusMismatch {
        /// The status the account ledger holds.
        ledger: AccountStatus,
        /// The status the snapshot carried.
        submitted: AccountStatus,
    },
    /// The snapshot's execution-capacity class disagrees with the account
    /// ledger (GL-99). The class is an account-owned fact changed through
    /// [`AdminStore::set_capacity_class`], which republishes; a publish may
    /// carry the current class but may not change it. Two writers for one
    /// fact is the divergence the status guard above already exists to
    /// abolish, and a second field must not reintroduce it.
    CapacityClassMismatch {
        /// The class the account ledger holds.
        ledger: CapacityClass,
        /// The class the snapshot carried.
        submitted: CapacityClass,
    },
    /// A backend failure unrelated to domain rules; see [`StoreError`].
    Storage(StoreError),
}

impl std::fmt::Display for PublishSnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublishSnapshotError::CredentialMismatch { key_id } => {
                write!(
                    f,
                    "credential {key_id} does not bind the published principal and account"
                )
            }
            PublishSnapshotError::StatusMismatch { ledger, submitted } => write!(
                f,
                "snapshot status {} contradicts account status {}",
                submitted.as_str(),
                ledger.as_str()
            ),
            PublishSnapshotError::CapacityClassMismatch { ledger, submitted } => write!(
                f,
                "snapshot capacity class {} contradicts account capacity class {}",
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

/// One account's administrative state, for an operator read (GL-121).
///
/// Assembled from types that already exist rather than a parallel vocabulary,
/// so the HTTP surface reports the same terms the ledger reasons in and a
/// reader can hold a dashboard next to a conservation check.
///
/// **Funding is not billing, and the shape says so.** A falling `balance` does
/// not mean units were billed: it also falls when they go out on a lease that
/// has not settled, and it falls when a budget period closes and takes its
/// unspent allowance with it. Those are three different facts, and
/// [`Conservation`] keeps them apart — `active_lease_grants` is capacity
/// currently out, `settled_usage` is what was actually consumed,
/// `settlement_loss` and `expired` are what will never be. A surface that
/// reported only a balance would let a customer read depletion as spend, which
/// is exactly what GL-121 asks not to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountView {
    /// The account read.
    pub account_id: AccountId,
    /// Administrative status, from the ledger.
    pub status: AccountStatus,
    /// Execution-capacity class, from the ledger.
    pub capacity_class: CapacityClass,
    /// The periodic allowance, if this account has one. `None` is "no
    /// schedule, the balance does not expire" — not "unknown".
    pub schedule: Option<BudgetSchedule>,
    /// First instant of the period currently in force. Meaningful only
    /// alongside a `schedule`; it is the marker rollover is idempotent
    /// against.
    pub period_start: Timestamp,
    /// Every term of the funding equation, so a caller can distinguish
    /// remaining funding from outstanding grants from settled usage.
    pub conservation: Conservation,
}

/// Administrative writes: the control plane's mutation surface. Kept apart
/// from the data-plane traits so a read-only replica can implement those
/// without this.
///
/// HTTP-facing mutations return [`crate::AdminReceipt`] captured at the same
/// serialization point as the write. Its `outcome` contains the operation's
/// result; before/after describe the fields owned by that operation. A separate
/// read before or after the transaction is not a valid receipt under concurrency.
/// No-op receipts have equal states; errors carry no confirmed transition.
/// Direct callers attach their own actor and audit delivery policy.
#[async_trait]
pub trait AdminStore: Send + Sync {
    /// Create an account from `config`, with its opening balance deposited as a
    /// top-up and its fencing sequence starting at one.
    ///
    /// Never destructive and never silently idempotent (INVARIANTS.md 14): an
    /// existing account is refused with [`CreateAccountError::AlreadyExists`] and
    /// left untouched, including its balance, ledger totals, fencing sequence and
    /// leases. The receipt's `before` is [`AdminState::Absent`](crate::AdminState::Absent).
    async fn create_account(
        &self,
        config: AccountConfig,
    ) -> Result<crate::AdminReceipt<()>, CreateAccountError>;
    /// Add `units` to an existing account as a top-up, which survives period
    /// boundaries, raising its balance and its `deposited` total together.
    ///
    /// A missing account is [`AllocateError::UnknownAccount`]. An overflow of
    /// either counter, or units outside the backend's numeric domain, is
    /// [`AllocateError::BalanceOverflow`] and moves neither. Account
    /// status is not checked. The receipt carries
    /// [`AdminState::Funding`](crate::AdminState::Funding) before and after.
    async fn deposit(
        &self,
        account: AccountId,
        units: CostUnits,
    ) -> Result<crate::AdminReceipt<()>, AllocateError>;
    /// Set an existing account's administrative status, in one transaction:
    /// the ledger's status, and a republication of every *live* snapshot of
    /// that account carrying the new status at `generation + 1`.
    ///
    /// This is the whole operator action. Before GL-51 the ledger flag and the
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
    ///   is what INVARIANTS.md GL-15 forbids, and revocation stays a separate
    ///   per-credential mechanism.
    /// - Snapshots already at the target status are not rewritten, so a
    ///   repeat converges and bumps no generation.
    /// - Outstanding leases are **not** reclaimed. Lease acquisition refuses
    ///   at once, but admission stops only when the new snapshot installs —
    ///   one `SnapshotManager` refresh interval, and already-debited units
    ///   settle at release or TTL reclaim (GL-9).
    async fn set_account_status(
        &self,
        account: AccountId,
        status: AccountStatus,
    ) -> Result<crate::AdminReceipt<StatusChange>, SetStatusError>;

    /// Set an existing account's execution-capacity class, in one
    /// transaction: the ledger's class, and a republication of every *live*
    /// snapshot of that account carrying the new class at `generation + 1`
    /// (GL-99).
    ///
    /// The same operator action, and the same ownership argument, as
    /// [`set_account_status`](Self::set_account_status): the class is one
    /// fact with one writer. A control plane that published it per credential
    /// instead would recreate exactly the divergence GL-51 abolished — some of
    /// an account's principals assured and some best-effort, with nothing
    /// checking them against each other, and a request's treatment depending
    /// on which credential it arrived with.
    ///
    /// Rules, all enforced here rather than by caller discipline:
    /// - A missing account is [`SetStatusError::UnknownAccount`].
    /// - A closed account is [`SetStatusError::AccountClosed`]. Reclassifying
    ///   a terminally closed account is meaningless and the refusal changes
    ///   nothing, exactly as it does for a status change.
    /// - Revoked principals are never republished (INVARIANTS.md GL-15).
    /// - Snapshots already at the target class are not rewritten, so a repeat
    ///   converges and bumps no generation.
    /// - Nothing about funding changes. The class decides whether an instance
    ///   starts an already-funded request, so quota, leases, and outstanding
    ///   usage are untouched — an account reclassified mid-flight keeps every
    ///   charge it has already committed.
    ///
    /// Reuses [`SetStatusError`] rather than declaring a near-identical twin:
    /// the two refusals are the same two conditions about the same ledger row,
    /// and a second enum would be two vocabularies for one answer.
    async fn set_capacity_class(
        &self,
        account: AccountId,
        class: CapacityClass,
    ) -> Result<crate::AdminReceipt<StatusChange>, SetStatusError>;

    /// Give an account a periodic allowance, or take it away.
    ///
    /// Setting a schedule does not deposit anything: the first allowance
    /// arrives at the first [`roll_due_periods`](Self::roll_due_periods) pass after the
    /// schedule exists. Depositing here would make "set a schedule" and "give
    /// this account units now" the same operation, and an operator correcting
    /// a mistyped allowance would fund the account twice.
    ///
    /// `None` removes the schedule and leaves the balance alone — including
    /// any unspent allowance, which simply stops expiring. Removing a schedule
    /// is not a way to claw units back.
    /// Set or clear an account's periodic allowance.
    ///
    /// Returns a receipt rather than `()` so the change can be audited like
    /// every other administrative mutation: an operator surface has to be able
    /// to report what a call actually committed, and a bare `Ok` cannot say
    /// whether a schedule was introduced, replaced, or was already what the
    /// caller asked for (GL-121). A repeat returns equal before/after states,
    /// which is how the convention expresses an idempotent no-op.
    async fn set_budget_schedule(
        &self,
        account: AccountId,
        schedule: Option<BudgetSchedule>,
    ) -> Result<crate::AdminReceipt<()>, BudgetError>;

    /// Cross the period boundary for up to `limit` accounts that are past it:
    /// expire each closed period's unspent allowance and deposit the next one,
    /// one transaction per batch.
    ///
    /// **Idempotency is this method's job, not its caller's.** The pass runs
    /// on every control-plane replica, so two of them will race a boundary;
    /// the backend crosses it under a row lock, and the second caller then
    /// reads the period the first one wrote and skips the account. A caller
    /// that read each period first and then rolled would produce two deposits
    /// under exactly the race this exists to survive.
    ///
    /// Safe to call at any cadence: before a boundary it selects nothing, and
    /// after one the first caller wins. It never rolls an account more than
    /// one period at a time — an account left unrolled for two months lands in
    /// the current period with one allowance, because an allowance is what the
    /// account is entitled to now, not a backlog to be paid out.
    ///
    /// Bounded, and saturating means there is more: every scheduled account
    /// comes due at the same instant, so a caller must drain saturated batches
    /// until one comes back partial. Unscheduled accounts are never selected.
    async fn roll_due_periods(
        &self,
        now: Timestamp,
        limit: NonZeroUsize,
    ) -> Result<RolloverBatch, StoreError>;
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
    ) -> Result<crate::AdminReceipt<()>, PublishSnapshotError>;
    /// Withdraw a principal's snapshot by tombstoning it at its current
    /// generation, and push the revocation to subscribers. The tombstone is
    /// durable, so no positive snapshot at or below that generation can resurrect
    /// the principal (INVARIANTS.md 15).
    ///
    /// A principal with no snapshot, or one already tombstoned, is left unchanged
    /// and the receipt's states are equal.
    async fn remove_snapshot(
        &self,
        principal: Principal,
    ) -> Result<crate::AdminReceipt<()>, StoreError>;

    /// One account's administrative state, or `None` if no such account (GL-121).
    ///
    /// The read an operator surface needs and the traits did not have. Both
    /// backends already expose `conservation` as an inherent method, but with
    /// different signatures — one synchronous returning an `Option`, one
    /// asynchronous returning a `Result` — so nothing generic over a backend
    /// could read an account at all.
    ///
    /// A single call rather than several, because the terms have to agree with
    /// each other: status, schedule and the funding equation read separately
    /// can straddle a rollover or a suspension and describe a state the account
    /// was never in. A backend answers this from one consistent read.
    async fn account_view(&self, account: AccountId) -> Result<Option<AccountView>, StoreError>;
}

/// Outcome of one ingest batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "wire", derive(serde::Serialize, serde::Deserialize))]
pub struct IngestReport {
    /// Newly recorded events.
    pub accepted: u64,
    /// Events whose `request_id` was already recorded (idempotent replay —
    /// INVARIANTS.md GL-7).
    pub duplicate: u64,
    /// Events refused: unknown lease, lease-capability mismatch, or no
    /// remaining accounting capacity, or units outside the backend's storage
    /// domain. These are bounded billing loss,
    /// visible to reconciliation.
    pub rejected: u64,
    /// Newly accepted events with absent, unknown or different-account key
    /// attribution. Duplicates never supply fresh activity evidence. `None`
    /// means this sink does not report attribution (including older servers),
    /// not that every event was attributed.
    #[cfg_attr(feature = "wire", serde(default))]
    pub unattributed: Option<u64>,
}

impl IngestReport {
    /// A complete acknowledgement partitions this batch exactly once. Validate
    /// before releasing queued evidence, including replies from custom sinks.
    pub fn validate(&self, submitted: usize) -> Result<(), StoreError> {
        let total = self
            .accepted
            .checked_add(self.duplicate)
            .and_then(|n| n.checked_add(self.rejected));
        if !total.is_some_and(|n| u64::try_from(submitted) == Ok(n))
            || self.unattributed.is_some_and(|n| n > self.accepted)
        {
            return Err(StoreError(
                "invalid usage acknowledgement cardinality".into(),
            ));
        }
        Ok(())
    }
}

/// The billing ledger's write side.
#[async_trait]
pub trait UsageSink: Send + Sync {
    /// Record a batch. Idempotent on `request_id`; every event must match its
    /// stored `(lease_id, account_id, fencing_token)` capability before lease
    /// state and accounting capacity are checked. Partial acceptance is
    /// normal — the report says what happened.
    ///
    /// Duplicates are classified before inspecting their payload. A new event
    /// outside the backend's unit domain is rejected individually; it is not
    /// remembered as accepted and cannot poison otherwise valid neighbors.
    /// MemoryStore supports `u64` units; PostgreSQL supports nonnegative
    /// `BIGINT` units (`0..=i64::MAX`). A representable event that overflows an
    /// accumulated accounting total refuses the entire batch atomically.
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
#[derive(Clone, PartialEq, Eq)]
pub struct KeyRecord {
    /// The credential's non-secret identifier, chosen by its issuer.
    pub key_id: KeyId,
    /// The account the credential authenticates for.
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

impl std::fmt::Debug for KeyRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyRecord")
            .field("key_id", &self.key_id)
            .field("account_id", &self.account_id)
            .field("principal", &self.principal)
            .field("not_after", &self.not_after)
            .finish_non_exhaustive()
    }
}

/// One requested key's activity. No observation is not proof of non-use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialActivity {
    /// The requested key.
    pub key_id: KeyId,
    /// What the store has recorded for it.
    pub state: CredentialActivityState,
}

/// A credential's recorded commitment activity (INVARIANTS.md 35).
/// It is never authentication or authorization evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialActivityState {
    /// The directory holds no credential with this id.
    Unknown,
    /// The credential exists, but no accepted, attributable usage has been
    /// recorded for it. Not proof that it was never used.
    Unobserved,
    /// Maximum accepted, attributable execution-start time, at microsecond
    /// precision. Never an authorization or independent server-clock fact.
    Committed {
        /// The latest recorded execution-start time.
        last_committed_at: Timestamp,
    },
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
/// batch discovered in production (GL-61).
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
/// into an unbounded billing and availability outage (GL-61).
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
    ///
    /// This is also the retry answer. A caller that supplies the `key_id` and
    /// loses the response resends the same one and is told the credential
    /// exists — which is the truth, and which discloses no secret. That is why
    /// issuance must never become an upsert (GL-121).
    AlreadyExists,
    /// The account already holds `limit` live credentials, so issuing another
    /// would exceed the bound the caller supplied.
    ///
    /// "Live" excludes revoked keys and keys whose `not_after` has passed: a
    /// bound that counted expired credentials would strand an account behind
    /// keys nobody can authenticate with.
    ActiveKeyLimit {
        /// The bound the caller supplied.
        limit: NonZeroUsize,
    },
    /// A backend failure unrelated to domain rules; see [`StoreError`].
    Storage(StoreError),
}

impl std::fmt::Display for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyError::UnknownKey => f.write_str("no such credential"),
            KeyError::UnknownAccount => f.write_str("no such account"),
            KeyError::AlreadyExists => f.write_str("credential already exists"),
            KeyError::ActiveKeyLimit { limit } => {
                write!(f, "account already holds {limit} live credentials")
            }
            KeyError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for KeyError {}

/// Refusals from binding or withdrawing a snapshot by the credential it was
/// issued as (GL-143).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeySnapshotError {
    /// No such credential, or it belongs to another account. One answer for
    /// both, as revocation gives: a foreign `key_id` discloses nothing.
    UnknownCredential,
    /// The credential was revoked. Revocation is terminal (INVARIANTS.md
    /// GL-27), so it is never granted positive authorization again; withdrawal
    /// remains allowed.
    Retired {
        /// The revoked credential.
        key_id: KeyId,
    },
    /// Publication refused as [`AdminStore::publish_snapshot`] would refuse it.
    Publish(PublishSnapshotError),
    /// A backend failure unrelated to domain rules; see [`StoreError`].
    Storage(StoreError),
}

impl std::fmt::Display for KeySnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeySnapshotError::UnknownCredential => f.write_str("no such credential"),
            KeySnapshotError::Retired { key_id } => {
                write!(f, "credential {key_id} is revoked")
            }
            KeySnapshotError::Publish(e) => write!(f, "{e}"),
            KeySnapshotError::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for KeySnapshotError {}

impl From<PublishSnapshotError> for KeySnapshotError {
    fn from(error: PublishSnapshotError) -> Self {
        Self::Publish(error)
    }
}

impl From<StoreError> for KeySnapshotError {
    fn from(error: StoreError) -> Self {
        Self::Storage(error)
    }
}

impl From<StoreError> for KeyError {
    fn from(error: StoreError) -> Self {
        Self::Storage(error)
    }
}

/// One credential as an *administrator* sees it (GL-121).
///
/// Deliberately not a [`KeyRecord`]. A record carries `digest` — the HMAC the
/// verifier compares against — and `principal`, documented there as "the
/// leading 128 bits of `digest`". Both are digest material, and an
/// account-scoped listing is reachable by an application backend and from
/// there a browser, so neither may appear in it. `key_id` is the non-secret
/// handle: what the caller chose, what revocation names, what an audit shows.
///
/// Expiry and revocation are surfaced separately. A credential that lapsed on
/// its own is a different operational fact from one an operator withdrew, and
/// collapsing both into "inactive" loses the distinction exactly where someone
/// is deciding whether to issue a replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeySummary {
    /// The credential's non-secret identifier.
    pub key_id: KeyId,
    /// When the credential stops being valid of its own accord, if ever.
    pub not_after: Option<Timestamp>,
    /// When an operator withdrew it, if they did. Terminal.
    pub revoked_at: Option<Timestamp>,
}

impl KeySummary {
    /// Whether this credential can still authenticate at `now`.
    ///
    /// The predicate the issuance bound counts with, written once so a listing
    /// and a limit cannot disagree about what "live" means.
    #[must_use]
    pub fn is_live(&self, now: Timestamp) -> bool {
        self.revoked_at.is_none() && self.not_after.is_none_or(|until| now < until)
    }
}

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
pub trait KeyDirectory: crate::KeySource {
    /// Inspect requested keys in input order, including repeated IDs and
    /// retired keys. Every input has one explicit result or the read fails.
    /// This operator read uses O(keys.len()) output memory; backends bound
    /// individual queries internally. Multiple chunks need not share an instant.
    async fn credential_activity(
        &self,
        keys: &[KeyId],
    ) -> Result<Vec<CredentialActivity>, StoreError>;

    /// Record a minted credential. The caller has already generated the
    /// secret and computed its digest; this stores what remains.
    async fn insert_key(&self, record: KeyRecord) -> Result<(), KeyError>;

    /// Retire one credential, reporting whether it was live.
    ///
    /// Revocation is durable and terminal: a retired credential is never
    /// resurrected, for the same reason a snapshot tombstone is not
    /// (INVARIANTS.md GL-15).
    async fn revoke_key(&self, key_id: KeyId, now: Timestamp) -> Result<Revocation, KeyError>;

    /// Unbounded operator read of every credential valid at `now`. Serving
    /// instances use `KeySource` pages and an owned, bounded drain instead.
    /// Retained for existing direct-store lifecycle tooling; no hidden page cap.
    async fn active_keys(&self, now: Timestamp) -> Result<Vec<KeyRecord>, StoreError>;

    /// One account's credentials, ordered by `key_id`, for an operator
    /// listing (GL-121).
    ///
    /// Distinct from [`active_keys`](Self::active_keys), which is the
    /// fleet-wide, digest-bearing projection an *instance* pulls: this is
    /// account-scoped, bounded, and carries no digest material, because it
    /// answers a different question for a different caller.
    ///
    /// Paginate with `after` — the greatest `key_id` already seen, exclusive.
    /// Revoked and expired credentials are included, because an administrator
    /// deciding whether to issue a replacement needs to see what became of the
    /// last one; [`KeySummary::is_live`] separates them.
    async fn account_keys(
        &self,
        account: AccountId,
        after: Option<KeyId>,
        limit: NonZeroUsize,
    ) -> Result<Vec<KeySummary>, StoreError>;

    /// Record a credential only if the account holds fewer than `max_active`
    /// live ones, counting and inserting indivisibly (GL-121).
    ///
    /// The bound is supplied per call rather than stored: what counts as a
    /// reasonable number of credentials belongs to the application's plan, not
    /// to Tollgate, and a value in the request is one the caller can change
    /// without a migration.
    ///
    /// **Why this is not [`active_keys`](Self::active_keys) then
    /// [`insert_key`](Self::insert_key).** Two issuers racing that pair both
    /// read `max_active - 1`, both insert, and the account ends up over the
    /// bound with no error raised anywhere — the more replicas, the likelier.
    /// A backend must make the count and the insert one indivisible step:
    /// `MemoryStore` holds a single lock across both, and `PostgresStore`
    /// takes the account row `FOR UPDATE` first — the row
    /// `set_account_status` already serialises against.
    ///
    /// Live excludes revoked credentials, and those whose `not_after` has
    /// passed at `now`, so an account cannot be stranded behind keys that can
    /// no longer authenticate.
    async fn insert_key_within(
        &self,
        record: KeyRecord,
        max_active: NonZeroUsize,
        now: Timestamp,
    ) -> Result<(), KeyError>;

    /// Bounded issuance with lifecycle evidence captured under the mutation lock.
    /// HTTP administrators must use this receipt rather than synthesize history.
    async fn insert_key_within_audited(
        &self,
        record: KeyRecord,
        max_active: NonZeroUsize,
        now: Timestamp,
    ) -> Result<crate::AdminReceipt<()>, KeyError>;

    /// Retire a credential and capture its actual owner, key and predecessor
    /// under the mutation lock. A repeated revocation returns equal states.
    async fn revoke_key_audited(
        &self,
        key_id: KeyId,
        now: Timestamp,
    ) -> Result<crate::AdminReceipt<Revocation>, KeyError>;

    /// Publish `snapshot` for the principal of `account`'s credential `key`,
    /// resolved inside the store (GL-143).
    ///
    /// An operator holds `(account, key)`; the principal is digest material
    /// and never leaves the server. Resolution, the retirement check and the
    /// publication are one indivisible step, so a concurrent revocation
    /// either precedes it — and the publish is refused as
    /// [`KeySnapshotError::Retired`] — or follows it.
    ///
    /// `snapshot` must state `key_id == Some(key)`; anything else is
    /// [`PublishSnapshotError::CredentialMismatch`]. Every other rule is
    /// [`AdminStore::publish_snapshot`]'s, including the generation no-op.
    async fn publish_key_snapshot(
        &self,
        account: AccountId,
        key: KeyId,
        snapshot: PublishableSnapshot,
    ) -> Result<crate::AdminReceipt<()>, KeySnapshotError>;

    /// Withdraw the snapshot of `account`'s credential `key`, tombstoning it
    /// as [`AdminStore::remove_snapshot`] does. Allowed for a revoked
    /// credential: revocation does not withdraw its snapshot, and withdrawal
    /// is the safe direction.
    async fn remove_key_snapshot(
        &self,
        account: AccountId,
        key: KeyId,
    ) -> Result<crate::AdminReceipt<()>, KeySnapshotError>;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::KeySummary;
    use jiff::Timestamp;
    use tollgate_core::KeyId;

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_second(seconds).expect("a test instant")
    }

    /// `not_after` is exclusive, and the instant itself is the whole question.
    ///
    /// Both backends filter with `now < not_after` — `memory.rs` in three
    /// places, and four SQL predicates written `not_after > $now`. `is_live`
    /// is the summary of exactly those queries, so `<=` here would not merely
    /// be off by an instant: a listing would report a credential live for the
    /// one instant at which every query that selects credentials has already
    /// dropped it, and the issuance bound counts with this predicate.
    #[test]
    fn a_credential_is_dead_at_its_expiry_instant_not_after_it() {
        let expiring = |not_after| KeySummary {
            key_id: KeyId(1),
            not_after: Some(not_after),
            revoked_at: None,
        };
        assert!(
            expiring(at(100)).is_live(at(99)),
            "live up to the instant before"
        );
        assert!(
            !expiring(at(100)).is_live(at(100)),
            "dead *at* the boundary: expiry is exclusive, as both backends filter it"
        );
        assert!(!expiring(at(100)).is_live(at(101)), "and dead after it");
    }

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
        ) -> Result<Allocation, AllocateError> {
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

        async fn consolidate(
            &self,
            _lease_id: LeaseId,
            _fencing_token: FencingToken,
            _unspent: CostUnits,
            _requested: CostUnits,
            _needed: CostUnits,
            _ttl: SignedDuration,
            _now: Timestamp,
        ) -> Result<Allocation, AllocateError> {
            unreachable!("the reclaim drain never consolidates")
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
                    forfeited: CostUnits(1),
                })
                .collect();
            ReclaimBatch::try_new(reclaimed, limit)
        }
    }

    /// GL-131: under the default divisor of 2, a 60-unit balance re-granted as
    /// 30 forever, however often a 51-unit quote was refused.
    #[test]
    fn consolidation_grows_only_to_a_fundable_needed_quote() {
        let policy = GrantPolicy::default();
        let size = |requested, balance, floor, needed| {
            policy.consolidation_grant(
                CostUnits(requested),
                CostUnits(balance),
                CostUnits(floor),
                CostUnits(needed),
            )
        };
        assert_eq!(
            size(1_000, 60, 30, 0),
            Some(CostUnits(30)),
            "the GL-109 floor"
        );
        assert_eq!(
            size(1_000, 60, 30, 51),
            Some(CostUnits(51)),
            "proven demand"
        );
        assert_eq!(
            size(1_000, 60, 30, 60),
            Some(CostUnits(60)),
            "all of it, inclusive"
        );
        assert_eq!(
            size(1_000, 60, 30, 61),
            Some(CostUnits(30)),
            "an unfundable quote grows nothing"
        );
        assert_eq!(
            size(1_000, 60, 40, 35),
            Some(CostUnits(40)),
            "demand never shrinks the floor"
        );
        assert_eq!(
            size(10, 60, 0, 51),
            Some(CostUnits(51)),
            "past a small target"
        );
        assert_eq!(
            size(1_000, 0, 0, 51),
            None,
            "an empty balance still refuses"
        );
        assert_eq!(size(0, 60, 0, 51), None, "a zero request still refuses");
        for (requested, balance) in [(1_000, 60), (7, 60), (1_000, 1)] {
            assert_eq!(
                size(requested, balance, 0, 0),
                policy.grant(CostUnits(requested), CostUnits(balance)),
                "a plain acquire is the ordinary policy"
            );
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
            AllocateError::BalanceExhausted(tollgate_core::BalanceExhaustion { period_end: None }),
            AllocateError::BalanceInsufficient(tollgate_core::BalanceShortfall {
                remaining: CostUnits(1),
                period_end: None,
            }),
            AllocateError::BalanceOverflow,
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
            expired: CostUnits::ZERO,
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
            expired: CostUnits::ZERO,
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
            expired: CostUnits::ZERO,
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
    /// the right side (INVARIANTS.md GL-11).
    #[test]
    fn overflowing_the_funding_sum_is_a_violation_not_a_wrap() {
        let overflowing = Conservation {
            deposited: CostUnits(u64::MAX),
            overage_recorded: CostUnits(1),
            balance: CostUnits::ZERO,
            active_lease_grants: CostUnits::ZERO,
            settled_usage: CostUnits::ZERO,
            settlement_loss: CostUnits::ZERO,
            expired: CostUnits::ZERO,
        };
        assert!(!overflowing.holds());
    }

    /// An allowance that expired at a period boundary left the balance without
    /// being spent, so the equation only closes if `expired` is on the right
    /// side — and the same ledger without the term must fail by exactly the
    /// units that expired, or the field would be decorative (GL-97).
    #[test]
    fn expiry_accounts_for_an_allowance_that_was_never_spent() {
        let rolled = Conservation {
            deposited: CostUnits(10),
            overage_recorded: CostUnits::ZERO,
            balance: CostUnits(1),
            active_lease_grants: CostUnits(2),
            settled_usage: CostUnits(3),
            settlement_loss: CostUnits::ZERO,
            expired: CostUnits(4),
        };
        assert!(rolled.holds());
        assert!(
            !Conservation {
                expired: CostUnits::ZERO,
                ..rolled
            }
            .holds(),
            "the same ledger without the expiry term must fail by exactly the expired units"
        );
    }

    /// A rollover pass reports what it did, and its caller decides whether to
    /// ask for another batch from `is_saturated` alone. Every accessor is
    /// asserted directly: a `len` that always answered 1, or an `is_empty`
    /// stuck either way, would send the server's drain loop into an endless
    /// round of empty batches or stop it one batch short of the boundary it
    /// was crossing.
    #[test]
    fn rollover_batch_reports_what_it_rolled() {
        let limit = NonZeroUsize::new(2).unwrap();
        let rolled = |account| RolledAccount {
            account_id: AccountId(account),
            deposited: CostUnits(100),
            expired: CostUnits::ZERO,
        };

        let empty = RolloverBatch::try_new(Vec::new(), limit).unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert!(!empty.is_saturated());
        assert!(empty.rolled().is_empty());

        let partial = RolloverBatch::try_new(vec![rolled(1)], limit).unwrap();
        assert!(!partial.is_empty());
        assert_eq!(partial.len(), 1);
        assert!(
            !partial.is_saturated(),
            "a partial batch is what ends the drain"
        );
        assert_eq!(partial.rolled(), &[rolled(1)]);

        let full = RolloverBatch::try_new(vec![rolled(1), rolled(2)], limit).unwrap();
        assert_eq!(full.len(), 2);
        assert!(
            full.is_saturated(),
            "a batch at the limit means there may be more"
        );
    }

    /// Saturation is derived from the limit the caller asked for, so a backend
    /// returning more than it was allowed is corruption to refuse rather than
    /// a batch to trust — the same contract `ReclaimBatch::try_new` carries.
    #[test]
    fn a_rollover_batch_beyond_its_limit_is_refused() {
        let rolled = |account| RolledAccount {
            account_id: AccountId(account),
            deposited: CostUnits(100),
            expired: CostUnits::ZERO,
        };
        assert!(
            RolloverBatch::try_new(vec![rolled(1), rolled(2)], NonZeroUsize::new(1).unwrap())
                .is_err()
        );
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
    /// the warning at all (GL-51).
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
