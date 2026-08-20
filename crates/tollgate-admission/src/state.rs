//! Per-account admission state and the snapshot-map abstraction.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use jiff::Timestamp;

use tollgate_core::{AccountSnapshot, LocalLease, ResolvedLimits};

pub use tollgate_core::Principal;

/// The slot a background refill task installs leases into. Shared between
/// the request path (load) and the refill plane (store); per account, and
/// shared by every principal of that account.
///
/// A `None` slot is the cold-start / lost-lease state and denies
/// (`LeaseUnavailable`), keeping INVARIANTS.md #5 and #10 honest.
#[derive(Debug, Default)]
pub struct LeaseSlot(ArcSwapOption<LocalLease>);

impl LeaseSlot {
    #[must_use]
    pub fn empty() -> Arc<Self> {
        Arc::new(LeaseSlot(ArcSwapOption::const_empty()))
    }

    /// Install a fresh lease. The old lease (if any) is dropped here — never
    /// mid-request, since in-flight reservations hold their own `Arc`.
    pub fn install(&self, lease: Arc<LocalLease>) {
        self.0.store(Some(lease));
    }

    /// Drop the current lease (server told us we are fenced out, or shutdown
    /// returned it). Subsequent requests deny until a new lease arrives.
    pub fn clear(&self) {
        self.0.store(None);
    }

    #[must_use]
    pub fn load(&self) -> Option<Arc<LocalLease>> {
        self.0.load_full()
    }
}

pub(crate) type AccountRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Everything the request path needs for one principal, resolved to a single
/// `Arc`: the compiled snapshot, the account's weighted rate limiter, and the
/// account's lease slot.
#[derive(Debug)]
pub struct AccountAdmissionState {
    pub snapshot: Arc<AccountSnapshot>,
    pub(crate) limiter: Arc<AccountRateLimiter>,
    pub lease: Arc<LeaseSlot>,
}

impl AccountAdmissionState {
    /// Compile the runtime state for a snapshot. `previous` is the state this
    /// one replaces, if any: when the rate parameters are unchanged the old
    /// limiter (and its consumed-token state) is carried over, so republishing
    /// a snapshot does not hand the account a fresh burst allowance.
    #[must_use]
    pub fn new(
        snapshot: Arc<AccountSnapshot>,
        lease: Arc<LeaseSlot>,
        previous: Option<&AccountAdmissionState>,
    ) -> Arc<Self> {
        let limiter = match previous {
            Some(prev) if rate_params(&prev.snapshot.limits) == rate_params(&snapshot.limits) => {
                Arc::clone(&prev.limiter)
            }
            // Changed limits (or first install): swap in a new limiter — the
            // `Quota` inside a governor limiter is immutable by design.
            _ => Arc::new(build_limiter(&snapshot.limits)),
        };
        Arc::new(AccountAdmissionState {
            snapshot,
            limiter,
            lease,
        })
    }
}

fn rate_params(limits: &ResolvedLimits) -> (u64, u64) {
    (limits.rate_units_per_second, limits.rate_burst_units)
}

fn build_limiter(limits: &ResolvedLimits) -> AccountRateLimiter {
    // governor buckets are u32-denominated. Rates/bursts beyond u32::MAX are
    // clamped rather than wrapped; a per-account rate of 4.29e9 units/sec is
    // beyond any current schedule by orders of magnitude.
    let rate = u32::try_from(limits.rate_units_per_second)
        .unwrap_or(u32::MAX)
        .max(1);
    let burst = u32::try_from(limits.rate_burst_units)
        .unwrap_or(u32::MAX)
        .max(1);
    let quota = Quota::per_second(rate.try_into().expect("nonzero by max(1)"))
        .allow_burst(burst.try_into().expect("nonzero by max(1)"));
    RateLimiter::direct(quota)
}

/// One resolved lookup result.
#[derive(Debug, Clone)]
pub enum MapEntry {
    /// A compiled account is installed.
    Present(Arc<AccountAdmissionState>),
    /// The control plane recently confirmed this principal unknown; deny
    /// without consulting anything else until `until` passes. This is the
    /// negative cache that stops manufactured misses from becoming work.
    NegativeUntil(Timestamp),
}

/// The pluggable snapshot map. Implementations must make `get` lock-free (or
/// as close as their backing store allows) and safe for concurrent `install`.
///
/// Generation monotonicity is part of the contract: installing a snapshot
/// older than the one present must be a no-op, so replayed or reordered
/// control-plane pushes can never roll an account back.
pub trait SnapshotMap: Send + Sync {
    fn get(&self, principal: &Principal) -> Option<MapEntry>;

    /// Install (or refresh) the state for a principal, respecting generation
    /// monotonicity. `lease` is the account's slot, shared across the
    /// account's principals by the caller.
    fn install(&self, principal: Principal, snapshot: Arc<AccountSnapshot>, lease: Arc<LeaseSlot>);

    /// Record a confirmed-unknown principal until `until`.
    fn install_negative(&self, principal: Principal, until: Timestamp);

    /// Remove a principal outright (key revoked).
    fn remove(&self, principal: &Principal);
}
