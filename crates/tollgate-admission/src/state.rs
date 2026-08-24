//! Per-account admission state and the snapshot-map abstraction.

use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use jiff::Timestamp;

use tollgate_core::{AccountSnapshot, Generation, LocalLease, ResolvedLimits};

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

    /// Install `lease`, returning the previously installed lease if one was
    /// present. The refill plane uses this to retain every superseded grant
    /// until it can be released safely.
    pub fn replace(&self, lease: Arc<LocalLease>) -> Option<Arc<LocalLease>> {
        self.0.swap(Some(lease))
    }

    /// Drop the current lease after the control plane invalidates local lease
    /// state or shutdown returns it. Subsequent requests deny until a new
    /// lease arrives.
    pub fn clear(&self) {
        self.0.store(None);
    }

    /// Remove and return the current lease in one atomic operation.
    pub fn take(&self) -> Option<Arc<LocalLease>> {
        self.0.swap(None)
    }

    #[must_use]
    pub fn load(&self) -> Option<Arc<LocalLease>> {
        self.0.load_full()
    }
}

type AccountRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Stable per-account indirection shared by every principal. Governor quotas
/// are immutable, so a policy update swaps the inner limiter while all
/// existing principal states keep pointing at this same object.
#[derive(Debug)]
pub(crate) struct AccountLimiter {
    current: ArcSwap<AccountRateLimiter>,
    config: std::sync::Mutex<(Generation, (u64, u64))>,
}

impl AccountLimiter {
    fn new(generation: Generation, limits: &ResolvedLimits) -> Self {
        Self {
            current: ArcSwap::from_pointee(build_limiter(limits)),
            config: std::sync::Mutex::new((generation, rate_params(limits))),
        }
    }

    fn update(&self, generation: Generation, limits: &ResolvedLimits) {
        let params = rate_params(limits);
        let mut installed = self.config.lock().expect("limiter config poisoned");
        if generation <= installed.0 {
            return;
        }
        if params != installed.1 {
            self.current.store(Arc::new(build_limiter(limits)));
        }
        *installed = (generation, params);
    }

    pub(crate) fn check_n(
        &self,
        n: std::num::NonZeroU32,
    ) -> Result<
        Result<(), governor::NotUntil<governor::clock::QuantaInstant>>,
        governor::InsufficientCapacity,
    > {
        self.current.load().check_n(n)
    }

    #[cfg(test)]
    pub(crate) fn current(&self) -> Arc<AccountRateLimiter> {
        self.current.load_full()
    }
}

/// Everything the request path needs for one principal, resolved to a single
/// `Arc`: the compiled snapshot, the account's weighted rate limiter, and the
/// account's lease slot.
#[derive(Debug)]
pub struct AccountAdmissionState {
    pub snapshot: Arc<AccountSnapshot>,
    pub(crate) limiter: Arc<AccountLimiter>,
    pub lease: Arc<LeaseSlot>,
}

impl AccountAdmissionState {
    /// Compile the runtime state for a snapshot. The limiter comes from the
    /// map's per-account registry, never per principal — see
    /// [`AccountLimiters`].
    #[must_use]
    pub(crate) fn new(
        snapshot: Arc<AccountSnapshot>,
        lease: Arc<LeaseSlot>,
        limiter: Arc<AccountLimiter>,
    ) -> Arc<Self> {
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

/// Per-account limiter registry owned by each snapshot map.
///
/// The advertised limits are *account* limits: every principal (API key) of
/// an account must draw from one bucket, or N keys would multiply the
/// account's allowance N-fold (review finding #4). Reinstalling snapshots
/// with unchanged rate parameters keeps the existing limiter — and its
/// consumed-token state — while a genuine limit change swaps in a fresh
/// limiter for the whole account (governor's `Quota` is immutable by
/// design).
///
/// Scope note: this registry is per admission-engine instance, so the limit
/// is enforced *per service instance*, not aggregated across a fleet —
/// consistent with every other local mechanism here (leases aggregate spend
/// globally; rate limits do not). Entries live as long as the map: bounded
/// by account count.
#[derive(Default)]
pub(crate) struct AccountLimiters {
    inner: std::sync::Mutex<Registry>,
}

#[derive(Default)]
struct Registry {
    by_account:
        std::collections::HashMap<tollgate_core::AccountId, std::sync::Weak<AccountLimiter>>,
    /// Size at the last sweep, so dead entries are reclaimed in proportion to
    /// how many have accumulated rather than on every single lookup.
    swept_at: usize,
    /// Entries walked across every sweep so far.
    ///
    /// The amortised bound is the whole point of #8, and it is a claim about
    /// total *work*, not sweep count: many tiny sweeps of a registry that
    /// keeps emptying are cheap, while one sweep per install over a registry
    /// full of live accounts is the quadratic. Only the entries-walked total
    /// distinguishes them.
    #[cfg(test)]
    swept_entries: usize,
}

/// Below this many accounts a sweep is too cheap to be worth deferring, and
/// deferring it would let a tiny registry hold dead entries indefinitely.
const SWEEP_FLOOR: usize = 8;

impl Registry {
    /// Reclaim dead entries, but only once the registry has grown enough since
    /// the last sweep to be worth walking.
    ///
    /// Sweeping on every lookup made a bulk install O(N·A): every one of N
    /// entries walked all A accounts (#8). Since the sweep reclaims memory and
    /// nothing else — a dead `Weak` upgrades to `None`, which the lookup below
    /// already handles by building a fresh limiter — it can be deferred freely.
    /// Doubling keeps dead entries within a constant factor of live ones and
    /// makes the amortised cost per lookup O(1).
    fn sweep_if_overgrown(&mut self) {
        // Doubling is what makes the amortisation work: each sweep costs the
        // registry's size, and the next one cannot come until that size has
        // doubled, so the total walked across N installs stays proportional to
        // N. The floor keeps a small registry from being walked repeatedly on
        // the way up from empty.
        let threshold = self.swept_at.saturating_mul(2).max(SWEEP_FLOOR);
        if self.by_account.len() <= threshold {
            return;
        }
        #[cfg(test)]
        {
            self.swept_entries += self.by_account.len();
        }
        self.by_account
            .retain(|_, limiter| limiter.strong_count() > 0);
        self.swept_at = self.by_account.len();
    }

    fn resolve(
        &mut self,
        account: tollgate_core::AccountId,
        generation: Generation,
        limits: &ResolvedLimits,
    ) -> Arc<AccountLimiter> {
        if let Some(limiter) = self
            .by_account
            .get(&account)
            .and_then(std::sync::Weak::upgrade)
        {
            limiter.update(generation, limits);
            return limiter;
        }
        let limiter = Arc::new(AccountLimiter::new(generation, limits));
        self.by_account.insert(account, Arc::downgrade(&limiter));
        limiter
    }
}

impl AccountLimiters {
    /// Fetch the account's shared limiter, building or swapping it when the
    /// snapshot's rate parameters differ from the installed ones. Called at
    /// control-plane frequency only.
    pub(crate) fn limiter_for(
        &self,
        account: tollgate_core::AccountId,
        generation: Generation,
        limits: &ResolvedLimits,
    ) -> Arc<AccountLimiter> {
        let mut inner = self.inner.lock().expect("limiter registry poisoned");
        inner.sweep_if_overgrown();
        inner.resolve(account, generation, limits)
    }

    /// Resolve a whole batch under one lock.
    ///
    /// The bulk paths used to take and release the registry mutex once per
    /// entry; a batch of N took it N times. `resolve` still runs per entry —
    /// two principals of one account can arrive in the same batch carrying
    /// different generations, and de-duplicating by account would silently
    /// drop one of them. What is shared is the lock and the sweep decision,
    /// not the update.
    pub(crate) fn limiters_for<T>(
        &self,
        batch: impl IntoIterator<Item = T>,
        mut key: impl FnMut(&T) -> (tollgate_core::AccountId, Generation),
        mut limits: impl FnMut(&T) -> ResolvedLimits,
    ) -> Vec<(T, Arc<AccountLimiter>)> {
        let mut inner = self.inner.lock().expect("limiter registry poisoned");
        inner.sweep_if_overgrown();
        batch
            .into_iter()
            .map(|item| {
                let (account, generation) = key(&item);
                let limiter = inner.resolve(account, generation, &limits(&item));
                (item, limiter)
            })
            .collect()
    }

    /// How many accounts the registry is holding, live or not yet reclaimed.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("limiter registry poisoned")
            .by_account
            .len()
    }

    /// Total entries walked across every sweep — the work the amortisation
    /// exists to bound.
    #[cfg(test)]
    pub(crate) fn swept_entries(&self) -> usize {
        self.inner
            .lock()
            .expect("limiter registry poisoned")
            .swept_entries
    }
}

fn build_limiter(limits: &ResolvedLimits) -> AccountRateLimiter {
    // governor buckets are u32-denominated. Rates/bursts beyond u32::MAX are
    // clamped rather than wrapped; a per-account rate of 4.29e9 units/sec is
    // beyond any current schedule by orders of magnitude.
    //
    // These clamps decide bucket *construction* only, never a verdict. A
    // schedule whose burst cannot hold a request is refused upstream in
    // `AdmissionEngine::admit`, comparing the quote against
    // `rate_burst_units` in full width — so `burst = 0` denies every priced
    // request as `UnpriceableUnderLimits` rather than silently behaving like
    // a burst of 1, and a burst above u32::MAX admits by the same comparison
    // it was configured with (#40). The `max(1)` below exists because
    // governor requires a nonzero quota, not to repair a configured value.
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
    /// without consulting anything else. `until` is control-plane metadata:
    /// the request path deliberately does not read a clock and denies both a
    /// live negative and a missing entry. Generation watermarks live outside
    /// this evictable entry so expiry cannot enable stale resurrection.
    NegativeUntil { until: Timestamp },
}

/// One control-plane mutation. Mixed positive and negative batches let a
/// copy-on-write map apply an entire refresh with one clone.
#[derive(Debug, Clone)]
pub enum SnapshotUpdate {
    Present {
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
        lease: Arc<LeaseSlot>,
    },
    /// A revocation the source published, which always carries its generation.
    Revoked {
        principal: Principal,
        until: Timestamp,
        generation: Generation,
    },
    /// An absent row, which carries no generation and asserts nothing about
    /// any (#53).
    Unknown {
        principal: Principal,
        until: Timestamp,
    },
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

    /// Record a revocation the source published at `generation`, denying until
    /// `until`.
    ///
    /// The generation is not optional: a revocation always carries one, and it
    /// is what refuses a replayed snapshot at or below it (INVARIANTS.md #15).
    fn install_revoked(&self, principal: Principal, until: Timestamp, generation: Generation);

    /// Record that the source has no row for this principal, denying until
    /// `until`.
    ///
    /// Deliberately takes no generation, because an absence has none to give —
    /// the 404 path cannot supply one, and inventing one from what this
    /// instance last saw is #53. It therefore leaves any existing watermark
    /// exactly as it was rather than raising or re-tagging it.
    fn install_unknown(&self, principal: Principal, until: Timestamp);

    /// Evict a principal outright. Revocations must use
    /// [`SnapshotMap::install_revoked`] so their generation watermark survives
    /// reordered control-plane messages.
    ///
    /// Eviction keeps the watermark, but keeping it no longer means the
    /// principal cannot be reinstalled at the same generation: since #53 a
    /// watermark left by a *positive* refuses only strictly older snapshots, so
    /// re-fetching the evicted generation repairs the entry. That is
    /// deliberate — it is what lets a bounded map recover from capacity
    /// pressure. A watermark left by a *revocation* still refuses its own
    /// generation, evicted or not.
    fn remove(&self, principal: &Principal);

    /// Install a batch in one logical write. The default loops over
    /// [`install`](SnapshotMap::install); copy-on-write implementations
    /// override it to pay their clone cost once per batch instead of once
    /// per entry (review finding #9 — loading N principals individually is
    /// O(N²) on a whole-map-clone structure).
    fn install_many(&self, entries: Vec<(Principal, Arc<AccountSnapshot>, Arc<LeaseSlot>)>) {
        self.apply_many(
            entries
                .into_iter()
                .map(|(principal, snapshot, lease)| SnapshotUpdate::Present {
                    principal,
                    snapshot,
                    lease,
                })
                .collect(),
        );
    }

    fn apply_many(&self, updates: Vec<SnapshotUpdate>) {
        for update in updates {
            match update {
                SnapshotUpdate::Present {
                    principal,
                    snapshot,
                    lease,
                } => self.install(principal, snapshot, lease),
                SnapshotUpdate::Revoked {
                    principal,
                    until,
                    generation,
                } => self.install_revoked(principal, until, generation),
                SnapshotUpdate::Unknown { principal, until } => {
                    self.install_unknown(principal, until);
                }
            }
        }
    }

    /// Apply a control-plane batch at an explicit time. Implementations with
    /// expiry maintenance can combine the sweep and batch in one write;
    /// implementations without time-based maintenance use the default.
    fn apply_many_at(&self, updates: Vec<SnapshotUpdate>, _now: Timestamp) {
        self.apply_many(updates);
    }
}

// A shared map is still a map: lets an `AdmissionEngine<Arc<M>>` and a
// background snapshot manager hold the same map instance.
impl<T: SnapshotMap + ?Sized> SnapshotMap for Arc<T> {
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        (**self).get(principal)
    }

    fn install(&self, principal: Principal, snapshot: Arc<AccountSnapshot>, lease: Arc<LeaseSlot>) {
        (**self).install(principal, snapshot, lease);
    }

    fn install_revoked(&self, principal: Principal, until: Timestamp, generation: Generation) {
        (**self).install_revoked(principal, until, generation);
    }

    fn install_unknown(&self, principal: Principal, until: Timestamp) {
        (**self).install_unknown(principal, until);
    }

    fn remove(&self, principal: &Principal) {
        (**self).remove(principal);
    }

    fn install_many(&self, entries: Vec<(Principal, Arc<AccountSnapshot>, Arc<LeaseSlot>)>) {
        (**self).install_many(entries);
    }

    fn apply_many(&self, updates: Vec<SnapshotUpdate>) {
        (**self).apply_many(updates);
    }

    fn apply_many_at(&self, updates: Vec<SnapshotUpdate>, now: Timestamp) {
        (**self).apply_many_at(updates, now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tollgate_core::{AccountId, CostUnits, FencingToken, LeaseGrant, LeaseId};

    fn lease(units: u64) -> Arc<LocalLease> {
        Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id: LeaseId(1),
                account_id: AccountId(1),
                fencing_token: FencingToken(1),
                units: CostUnits(units),
                expires_at: Timestamp::from_second(10_000).unwrap(),
            },
            CostUnits::ZERO,
        ))
    }

    /// `state.rs` carried no tests at all, and the slot's whole job is to say
    /// whether this instance may spend. `clear` in particular could be
    /// replaced by a no-op with the entire suite still green (#43): the refill
    /// plane happens to use `take` everywhere, so the documented invalidation
    /// path — "subsequent requests deny until a new lease arrives" — had no
    /// witness at all.
    #[test]
    fn a_cleared_slot_stops_the_instance_spending() {
        let slot = LeaseSlot::empty();
        assert!(slot.load().is_none(), "a cold slot denies");

        slot.install(lease(100));
        assert!(slot.load().is_some());

        slot.clear();
        assert!(
            slot.load().is_none(),
            "an instance with invalidated lease state must hold no lease"
        );
    }

    /// `replace` hands back the superseded grant so the refill plane can
    /// release it; `take` removes and returns in one step. Losing either
    /// return value strands units until TTL reclaim.
    #[test]
    fn superseding_a_lease_hands_back_the_old_one() {
        let slot = LeaseSlot::empty();
        assert!(
            slot.replace(lease(100)).is_none(),
            "nothing was installed, so there is nothing to give back"
        );

        let superseded = slot.replace(lease(200)).expect("the first lease");
        assert_eq!(superseded.remaining(), CostUnits(100));
        assert_eq!(
            slot.load().expect("the second lease").remaining(),
            CostUnits(200)
        );

        let taken = slot.take().expect("the second lease");
        assert_eq!(taken.remaining(), CostUnits(200));
        assert!(slot.load().is_none(), "take leaves the slot empty");
        assert!(slot.take().is_none(), "and taking again yields nothing");
    }
}
