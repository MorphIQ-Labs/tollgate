//! Per-account admission state and the snapshot-map abstraction.

use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use jiff::Timestamp;

use tollgate_core::{
    AccountId, AccountOverage, AccountSnapshot, CostUnits, Generation, LocalLease, LocalSharding,
    Locality, PublishableSnapshot, ResolvedLimits,
};

pub use tollgate_core::Principal;

/// The slot a background refill task installs leases into, and the account's
/// overage counter. Shared between the request path (load) and the refill
/// plane (store); per account, and shared by every principal of that account.
///
/// A `None` lease is the cold-start / lost-lease state. Under
/// [`EnforcementMode::Strict`] it denies (`LeaseUnavailable`), keeping
/// INVARIANTS.md #5 and #10 honest; under `Elastic` it is one of the states
/// the overage counter answers for.
///
/// **The overage counter lives here, and that is a decision worth stating.**
/// It has to outlive individual leases — the account is elastic precisely when
/// it has no usable lease — so it cannot hang off `LocalLease`. It also has to
/// outlive snapshot installs, which rules out `AccountAdmissionState`, rebuilt
/// on every publish. And it must not live beside the rate-limiter registry,
/// whose entries are `Weak` and swept: that is safe only because a rebuilt
/// limiter costs a full bucket, whereas a rebuilt overage counter silently
/// resets a spend cap. This slot is held by a strong `Arc` in the client's
/// `SlotRegistry`, created on first use and never reclaimed, which is exactly
/// the lifetime a spend cap needs.
///
/// **The counter stays unsharded while the lease views shard**, and that is
/// also deliberate. Sharding the lease splits a *grant* N ways, which is
/// sound because the shards' remainders sum to the grant. A spend cap is not
/// divisible the same way: N per-locality caps summing to the configured cap
/// would refuse an elastic request on a saturated core while headroom sat
/// unreachable on another, and one cap of `N × cap` would silently raise it.
/// The counter is therefore one atomic per account, contended only on the
/// path that by definition has no usable lease to spend from.
///
/// [`EnforcementMode::Strict`]: tollgate_core::EnforcementMode::Strict
#[derive(Debug)]
pub struct LeaseSlot {
    current: LeaseSlotCurrent,
    sharding: LocalSharding,
    overage: Arc<AccountOverage>,
}

#[derive(Debug)]
enum LeaseSlotCurrent {
    Single(ArcSwapOption<LocalLease>),
    /// One published view per locality, plus the mutex that keeps a
    /// multi-view publication indivisible *between mutators*.
    ///
    /// A single-view slot publishes in one swap, so two mutators can only
    /// order themselves. N swaps cannot: without this lock a `clear` racing a
    /// `replace` would interleave and leave some localities empty and others
    /// holding the fresh lease — a mixed state the single-view slot cannot
    /// reach, and one that would let a locality keep spending after a
    /// revocation. Publication is control-plane work; `load_at` never takes
    /// the lock, so the request path is unaffected.
    Sharded {
        publish: std::sync::Mutex<()>,
        views: Box<[ArcSwapOption<LocalLease>]>,
    },
}

impl LeaseSlotCurrent {
    fn sharded(shards: usize) -> Self {
        Self::Sharded {
            publish: std::sync::Mutex::new(()),
            views: (0..shards)
                .map(|_| ArcSwapOption::const_empty())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }
}

impl LeaseSlot {
    /// An empty single-view slot for `account`, with its overage counter at
    /// zero.
    ///
    /// Takes the account id because the overage counter is the billing
    /// identity for spend that has no lease to borrow one from: an overage
    /// usage event names this account and nothing else.
    #[must_use]
    pub fn for_account(account: AccountId) -> Arc<Self> {
        Self::with_sharding(account, LocalSharding::SINGLE)
    }

    /// Create an empty slot for `account` whose refill manager will install
    /// leases with the selected instance-local layout.
    #[must_use]
    pub fn with_sharding(account: AccountId, sharding: LocalSharding) -> Arc<Self> {
        let current = if sharding == LocalSharding::SINGLE {
            LeaseSlotCurrent::Single(ArcSwapOption::const_empty())
        } else {
            LeaseSlotCurrent::sharded(sharding.get())
        };
        Arc::new(Self {
            current,
            sharding,
            overage: Arc::new(AccountOverage::new(account)),
        })
    }

    #[must_use]
    pub fn sharding(&self) -> LocalSharding {
        self.sharding
    }

    /// The account's unfunded-spend counter, shared by every principal of the
    /// account, by every lease that passes through this slot, and by every
    /// locality this slot publishes to.
    #[must_use]
    pub fn overage(&self) -> &Arc<AccountOverage> {
        &self.overage
    }

    /// Install a fresh lease. The old lease (if any) is dropped here — never
    /// mid-request, since in-flight reservations hold their own `Arc`.
    pub fn install(&self, lease: Arc<LocalLease>) {
        let _ = self.publish(Some(lease));
    }

    /// Install `lease`, returning the previously installed lease if one was
    /// present. The refill plane uses this to retain every superseded grant
    /// until it can be released safely.
    pub fn replace(&self, lease: Arc<LocalLease>) -> Option<Arc<LocalLease>> {
        self.publish(Some(lease))
    }

    /// Drop the current lease after the control plane invalidates local lease
    /// state or shutdown returns it. Subsequent requests deny until a new
    /// lease arrives.
    ///
    /// The overage counter is deliberately untouched: losing a lease is not a
    /// funding event, and clearing spend here would hand an elastic account a
    /// fresh cap on every control-plane hiccup.
    pub fn clear(&self) {
        let _ = self.publish(None);
    }

    /// Remove every published local view and return one handle to their
    /// shared lease state. A concurrent request may already have loaded a
    /// view, exactly as it could have loaded the single-view slot before its
    /// swap; quiescence detection accounts for that handle.
    pub fn take(&self) -> Option<Arc<LocalLease>> {
        self.publish(None)
    }

    /// Publish `next` to every local view, returning one handle to whatever
    /// the slot held before.
    ///
    /// Mutators are serialized, so a slot never ends a publication holding a
    /// mixture of two leases: readers see the outgoing or the incoming lease
    /// per locality while one publication is in flight, exactly as a
    /// single-view slot's readers straddle its one swap, and every locality
    /// has converged on the incoming lease by the time this returns.
    ///
    /// A sharded slot publishes an independently reference-counted handle to
    /// each view rather than sharing one: that is what keeps a routine
    /// request's `Arc` traffic off the other localities' cache lines, and
    /// quiescence still sees through every alias to the shared inner state.
    fn publish(&self, next: Option<Arc<LocalLease>>) -> Option<Arc<LocalLease>> {
        match &self.current {
            LeaseSlotCurrent::Single(current) => current.swap(next),
            LeaseSlotCurrent::Sharded { publish, views } => {
                let _publishing = publish.lock().expect("lease slot publication poisoned");
                let mut replaced = None;
                for view in views {
                    let view_lease = next.as_ref().map(|lease| Arc::new((**lease).clone()));
                    let old = view.swap(view_lease);
                    if replaced.is_none() {
                        replaced = old;
                    }
                }
                replaced
            }
        }
    }

    /// Every published view, in locality order. Publication is only
    /// meaningfully indivisible if a test can look at all of them at once.
    #[cfg(test)]
    fn published_views(&self) -> Vec<Option<Arc<LocalLease>>> {
        match &self.current {
            LeaseSlotCurrent::Single(current) => vec![current.load_full()],
            LeaseSlotCurrent::Sharded { views, .. } => views
                .iter()
                .map(arc_swap::ArcSwapOption::load_full)
                .collect(),
        }
    }

    #[must_use]
    pub fn load(&self) -> Option<Arc<LocalLease>> {
        self.load_at(Locality::current())
    }

    #[must_use]
    pub(crate) fn load_at(&self, locality: Locality) -> Option<Arc<LocalLease>> {
        match &self.current {
            LeaseSlotCurrent::Single(current) => current.load_full(),
            LeaseSlotCurrent::Sharded { views, .. } => {
                views[locality.index(self.sharding)].load_full()
            }
        }
    }
}

type AccountRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

#[repr(align(128))]
#[derive(Debug)]
pub(crate) struct RateShard(AccountRateLimiter);

#[derive(Debug)]
pub(crate) enum AccountRateState {
    Single(AccountRateLimiter),
    Sharded(Box<[RateShard]>),
}

impl AccountRateState {
    fn check_n_at(
        &self,
        n: std::num::NonZeroU32,
        locality: Locality,
    ) -> Result<
        Result<(), governor::NotUntil<governor::clock::QuantaInstant>>,
        governor::InsufficientCapacity,
    > {
        match self {
            Self::Single(limiter) => limiter.check_n(n),
            Self::Sharded(shards) => {
                let sharding = LocalSharding::new(
                    std::num::NonZeroUsize::new(shards.len())
                        .expect("a sharded rate state is non-empty"),
                );
                let first = locality.index(sharding);
                let mut denied = None;
                let mut insufficient = None;
                for offset in 0..shards.len() {
                    let index = (first + offset) % shards.len();
                    match shards[index].0.check_n(n) {
                        Ok(Ok(())) => return Ok(Ok(())),
                        // The caller's own shard's denial, kept by
                        // construction: it is the bucket a retry from this
                        // locality will meet. `DenyReason::RateLimited`
                        // carries no retry hint today, so which denial
                        // travels is not observable — which is exactly why
                        // the choice should be the defensible one now rather
                        // than whichever shard happened to be scanned last.
                        Ok(Err(denial)) => {
                            denied.get_or_insert(denial);
                        }
                        // A shard too small to ever hold `n` says nothing
                        // about its siblings — the remainder of a partition
                        // leaves them up to one unit larger. Keep looking,
                        // and report this only if no shard can take the
                        // request now or later (INVARIANTS.md #5).
                        Err(error) => insufficient = Some(error),
                    }
                }
                match denied {
                    Some(denial) => Ok(Err(denial)),
                    None => Err(insufficient.expect("a sharded rate state is non-empty")),
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn shard_count(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::Sharded(shards) => shards.len(),
        }
    }
}

/// Stable per-account indirection shared by every principal. Governor quotas
/// are immutable, so a policy update swaps the inner limiter while all
/// existing principal states keep pointing at this same object.
#[derive(Debug)]
pub(crate) struct AccountLimiter {
    current: ArcSwap<AccountRateState>,
    config: std::sync::Mutex<LimiterConfig>,
}

#[derive(Debug)]
struct LimiterConfig {
    /// Highest generation whose limits are installed below.
    generation: Generation,
    installed: RateParams,
    /// Finest split every principal of this account can still spend in.
    ///
    /// The bucket is account-wide but the largest quote is a property of one
    /// principal's cost table, so the split is only safe at the *most*
    /// constraining principal's ceiling. Sizing it from whichever snapshot
    /// installed last would leave a principal with a heavier table unable to
    /// spend its largest quote in any shard — a permanent
    /// `UnpriceableUnderLimits` for a request the account's burst can hold.
    /// Tightening therefore ignores generation ordering: an older snapshot's
    /// evidence is still evidence about a principal that is admitting
    /// requests now.
    ///
    /// It never widens again. Doing so would need per-principal evidence with
    /// its own eviction story for churned credentials, and the whole cost of
    /// holding the floor is a coarser split — less cache isolation for an
    /// account that once carried a heavier table, never a wrong admission
    /// decision. A limiter is dropped with its last principal, so the floor
    /// does not outlive the account's presence in the map.
    shard_ceiling: usize,
}

impl AccountLimiter {
    fn new(
        generation: Generation,
        limits: &ResolvedLimits,
        maximum_quote: Option<CostUnits>,
        sharding: LocalSharding,
    ) -> Self {
        let shard_ceiling = shard_ceiling(limits, maximum_quote);
        let params = rate_params(limits, sharding, shard_ceiling);
        Self {
            current: ArcSwap::from_pointee(build_rate_state(params)),
            config: std::sync::Mutex::new(LimiterConfig {
                generation,
                installed: params,
                shard_ceiling,
            }),
        }
    }

    fn update(
        &self,
        generation: Generation,
        limits: &ResolvedLimits,
        maximum_quote: Option<CostUnits>,
        sharding: LocalSharding,
    ) {
        let mut config = self.config.lock().expect("limiter config poisoned");
        config.shard_ceiling = config
            .shard_ceiling
            .min(shard_ceiling(limits, maximum_quote));
        // Strictly newer, so the first snapshot installed at a generation
        // owns its limits. Two principals of one account carry the same
        // account limits by design; if a misconfiguration makes them differ,
        // a stable answer beats one that oscillates with install order.
        let params = if generation > config.generation {
            config.generation = generation;
            rate_params(limits, sharding, config.shard_ceiling)
        } else {
            // Too old to install its limits, but its ceiling still applies:
            // re-split the installed rate and burst, keeping their totals.
            RateParams {
                shards: shard_count(config.installed.rate, sharding, config.shard_ceiling),
                ..config.installed
            }
        };
        if params != config.installed {
            self.current.store(Arc::new(build_rate_state(params)));
            config.installed = params;
        }
    }

    #[cfg(test)]
    pub(crate) fn check_n(
        &self,
        n: std::num::NonZeroU32,
    ) -> Result<
        Result<(), governor::NotUntil<governor::clock::QuantaInstant>>,
        governor::InsufficientCapacity,
    > {
        self.check_n_at(n, Locality::current())
    }

    pub(crate) fn check_n_at(
        &self,
        n: std::num::NonZeroU32,
        locality: Locality,
    ) -> Result<
        Result<(), governor::NotUntil<governor::clock::QuantaInstant>>,
        governor::InsufficientCapacity,
    > {
        self.current.load().check_n_at(n, locality)
    }

    #[cfg(test)]
    pub(crate) fn current(&self) -> Arc<AccountRateState> {
        self.current.load_full()
    }
}

/// Everything the request path needs for one principal, resolved to a single
/// `Arc`: the compiled snapshot, the account's weighted rate limiter, and the
/// account's lease slot.
#[derive(Debug)]
#[repr(align(128))]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RateParams {
    rate: u32,
    burst: u32,
    shards: usize,
}

fn rate_params(limits: &ResolvedLimits, sharding: LocalSharding, ceiling: usize) -> RateParams {
    let rate = narrow(limits.rate_units_per_second);
    RateParams {
        rate,
        burst: narrow(limits.rate_burst_units),
        shards: shard_count(rate, sharding, ceiling),
    }
}

/// Governor counts in `u32`; a limit beyond that is clamped, never wrapped,
/// and a zero limit would make an empty quota.
fn narrow(limit: u64) -> u32 {
    u32::try_from(limit).unwrap_or(u32::MAX).max(1)
}

fn shard_count(rate: u32, sharding: LocalSharding, ceiling: usize) -> usize {
    sharding
        .get()
        .min(usize::try_from(rate).unwrap_or(usize::MAX))
        .min(ceiling)
        .max(1)
}

/// How finely this snapshot's burst may be split while every shard still
/// admits the largest quote the snapshot can produce.
///
/// A split of `n` gives the smallest shard `floor(burst / n)` units, so
/// `n <= burst / maximum_quote` is exactly the condition that keeps even that
/// shard able to hold one maximum quote. Without a publication proof there is
/// no bound on the quote, and the only defensible split is none at all.
fn shard_ceiling(limits: &ResolvedLimits, maximum_quote: Option<CostUnits>) -> usize {
    let Some(maximum_quote) = maximum_quote else {
        return 1;
    };
    let capacity = narrow(limits.rate_burst_units) / narrow(maximum_quote.get());
    usize::try_from(capacity).unwrap_or(usize::MAX).max(1)
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
pub(crate) struct AccountLimiters {
    inner: std::sync::Mutex<Registry>,
    sharding: LocalSharding,
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
        maximum_quote: Option<CostUnits>,
        sharding: LocalSharding,
    ) -> Arc<AccountLimiter> {
        if let Some(limiter) = self
            .by_account
            .get(&account)
            .and_then(std::sync::Weak::upgrade)
        {
            limiter.update(generation, limits, maximum_quote, sharding);
            return limiter;
        }
        let limiter = Arc::new(AccountLimiter::new(
            generation,
            limits,
            maximum_quote,
            sharding,
        ));
        self.by_account.insert(account, Arc::downgrade(&limiter));
        limiter
    }
}

impl AccountLimiters {
    pub(crate) fn new(sharding: LocalSharding) -> Self {
        Self {
            inner: std::sync::Mutex::new(Registry::default()),
            sharding,
        }
    }

    /// Fetch the account's shared limiter, building or swapping it when the
    /// snapshot's rate parameters differ from the installed ones. Called at
    /// control-plane frequency only.
    pub(crate) fn limiter_for(
        &self,
        account: tollgate_core::AccountId,
        generation: Generation,
        limits: &ResolvedLimits,
        maximum_quote: Option<CostUnits>,
    ) -> Arc<AccountLimiter> {
        let mut inner = self.inner.lock().expect("limiter registry poisoned");
        inner.sweep_if_overgrown();
        inner.resolve(account, generation, limits, maximum_quote, self.sharding)
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
        mut maximum_quote: impl FnMut(&T) -> Option<CostUnits>,
    ) -> Vec<(T, Arc<AccountLimiter>)> {
        let mut inner = self.inner.lock().expect("limiter registry poisoned");
        inner.sweep_if_overgrown();
        batch
            .into_iter()
            .map(|item| {
                let (account, generation) = key(&item);
                let limiter = inner.resolve(
                    account,
                    generation,
                    &limits(&item),
                    maximum_quote(&item),
                    self.sharding,
                );
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

impl Default for AccountLimiters {
    fn default() -> Self {
        Self::new(LocalSharding::SINGLE)
    }
}

fn build_limiter(rate: u32, burst: u32) -> AccountRateLimiter {
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
    let quota = Quota::per_second(rate.try_into().expect("nonzero by max(1)"))
        .allow_burst(burst.try_into().expect("nonzero by max(1)"));
    RateLimiter::direct(quota)
}

fn build_rate_state(params: RateParams) -> AccountRateState {
    if params.shards == 1 {
        return AccountRateState::Single(build_limiter(params.rate, params.burst));
    }
    let shards = (0..params.shards)
        .map(|index| {
            let rate = u32::try_from(partition(u64::from(params.rate), params.shards, index))
                .expect("a partition of u32 fits u32");
            let burst = u32::try_from(partition(u64::from(params.burst), params.shards, index))
                .expect("a partition of u32 fits u32");
            RateShard(build_limiter(rate, burst))
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    AccountRateState::Sharded(shards)
}

fn partition(total: u64, count: usize, index: usize) -> u64 {
    let count = u64::try_from(count).expect("shard count fits u64");
    let index = u64::try_from(index).expect("shard index fits u64");
    total / count + u64::from(index < total % count)
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

/// A control-plane mutation whose positive snapshots carry their validated
/// publication evidence through to the admission map. The separate type
/// keeps the existing raw installation API available for defensive tests and
/// embedders while letting trusted sources avoid discarding and re-deriving
/// the proof at the boundary.
#[derive(Debug, Clone)]
pub enum PublishableSnapshotUpdate {
    Present {
        principal: Principal,
        snapshot: PublishableSnapshot,
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

    /// Lookup using locality already resolved by the admission engine, so a
    /// request reads its affinity once rather than at each stage.
    ///
    /// An implementation that owns per-locality state must select it from
    /// this argument: resolving locality again during retrieval ties the
    /// choice to whichever thread happened to be running, which for a cache
    /// that clones on write is the thread that installed the entry. The
    /// compatibility default delegates to [`Self::get`], which is correct
    /// precisely because such a map has one state per principal to return.
    fn get_at(&self, principal: &Principal, _locality: Locality) -> Option<MapEntry> {
        self.get(principal)
    }

    /// Instance-local sharding selected for the map and every hot-path state
    /// object it owns. The default preserves the historical single-counter
    /// layout.
    fn local_sharding(&self) -> LocalSharding {
        LocalSharding::SINGLE
    }

    /// Install (or refresh) the state for a principal, respecting generation
    /// monotonicity. `lease` is the account's slot, shared across the
    /// account's principals by the caller.
    fn install(&self, principal: Principal, snapshot: Arc<AccountSnapshot>, lease: Arc<LeaseSlot>);

    /// Install a publication-validated snapshot without discarding its
    /// maximum-quote proof. Implementations that shard the rate limiter
    /// override this method; the compatibility default installs one bucket.
    fn install_publishable(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
        lease: Arc<LeaseSlot>,
    ) {
        self.install(principal, snapshot.into_inner(), lease);
    }

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

    fn apply_publishable_many(&self, updates: Vec<PublishableSnapshotUpdate>) {
        for update in updates {
            match update {
                PublishableSnapshotUpdate::Present {
                    principal,
                    snapshot,
                    lease,
                } => self.install_publishable(principal, snapshot, lease),
                PublishableSnapshotUpdate::Revoked {
                    principal,
                    until,
                    generation,
                } => self.install_revoked(principal, until, generation),
                PublishableSnapshotUpdate::Unknown { principal, until } => {
                    self.install_unknown(principal, until)
                }
            }
        }
    }

    fn apply_publishable_many_at(&self, updates: Vec<PublishableSnapshotUpdate>, _now: Timestamp) {
        self.apply_publishable_many(updates);
    }
}

// A shared map is still a map: lets an `AdmissionEngine<Arc<M>>` and a
// background snapshot manager hold the same map instance.
impl<T: SnapshotMap + ?Sized> SnapshotMap for Arc<T> {
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        (**self).get(principal)
    }

    fn get_at(&self, principal: &Principal, locality: Locality) -> Option<MapEntry> {
        (**self).get_at(principal, locality)
    }

    fn local_sharding(&self) -> LocalSharding {
        (**self).local_sharding()
    }

    fn install(&self, principal: Principal, snapshot: Arc<AccountSnapshot>, lease: Arc<LeaseSlot>) {
        (**self).install(principal, snapshot, lease);
    }

    fn install_publishable(
        &self,
        principal: Principal,
        snapshot: PublishableSnapshot,
        lease: Arc<LeaseSlot>,
    ) {
        (**self).install_publishable(principal, snapshot, lease);
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

    fn apply_publishable_many(&self, updates: Vec<PublishableSnapshotUpdate>) {
        (**self).apply_publishable_many(updates);
    }

    fn apply_publishable_many_at(&self, updates: Vec<PublishableSnapshotUpdate>, now: Timestamp) {
        (**self).apply_publishable_many_at(updates, now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::{NonZeroU32, NonZeroUsize};
    use tollgate_core::{AccountId, CostUnits, FencingToken, LeaseGrant, LeaseId};

    fn lease(units: u64) -> Arc<LocalLease> {
        identified_lease(LeaseId(1), units)
    }

    fn identified_lease(lease_id: LeaseId, units: u64) -> Arc<LocalLease> {
        Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id,
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
        let slot = LeaseSlot::for_account(AccountId(1));
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
        let slot = LeaseSlot::for_account(AccountId(1));
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

    /// A sharded slot publishes N views, so two mutators that interleave
    /// could leave half the localities empty and half holding the fresh lease
    /// — a state one swap cannot reach, and one that would let a locality
    /// keep spending after a revocation cleared the slot.
    #[test]
    fn racing_mutators_never_leave_a_slot_holding_two_answers() {
        let sharding = LocalSharding::new(NonZeroUsize::new(8).unwrap());
        for round in 0..500 {
            let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
            slot.install(identified_lease(LeaseId(1), 100));

            let replacer = {
                let slot = Arc::clone(&slot);
                std::thread::spawn(move || slot.replace(identified_lease(LeaseId(2), 100)))
            };
            let clearer = {
                let slot = Arc::clone(&slot);
                std::thread::spawn(move || slot.clear())
            };
            replacer.join().unwrap();
            clearer.join().unwrap();

            let published: Vec<_> = slot
                .published_views()
                .into_iter()
                .map(|view| view.map(|lease| lease.grant().lease_id))
                .collect();
            assert!(
                published.iter().all(|view| *view == published[0]),
                "round {round}: localities disagree about the slot: {published:?}"
            );
        }
    }

    #[test]
    fn sharded_slot_keeps_release_parked_while_any_local_view_is_held() {
        let sharding = LocalSharding::new(NonZeroUsize::new(4).unwrap());
        let slot = LeaseSlot::with_sharding(AccountId(1), sharding);
        slot.install(lease(100));
        let sibling = match &slot.current {
            LeaseSlotCurrent::Sharded { views, .. } => views[1].load_full().unwrap(),
            LeaseSlotCurrent::Single(_) => unreachable!(),
        };

        let parked = slot.take().unwrap();
        assert!(!parked.is_only_local_view());
        drop(sibling);
        assert!(parked.is_only_local_view());
    }

    #[test]
    fn rate_shards_partition_one_account_burst_without_multiplying_it() {
        assert_eq!(align_of::<RateShard>(), 128);
        let limits = ResolvedLimits {
            max_items_per_request: 1,
            rate_units_per_second: 8,
            rate_burst_units: 800,
        };
        let limiter = AccountLimiter::new(
            Generation(1),
            &limits,
            Some(CostUnits(100)),
            LocalSharding::new(NonZeroUsize::new(8).unwrap()),
        );
        assert_eq!(limiter.current.load().shard_count(), 8);

        let request = NonZeroU32::new(100).unwrap();
        for _ in 0..8 {
            assert_eq!(limiter.check_n(request), Ok(Ok(())));
        }
        assert!(matches!(limiter.check_n(request), Ok(Err(_))));
    }

    #[test]
    fn rate_partition_preserves_quotient_and_remainder_exactly() {
        let shares: Vec<_> = (0..4).map(|index| partition(10, 4, index)).collect();
        assert_eq!(shares, [3, 3, 2, 2]);
        assert_eq!(shares.into_iter().sum::<u64>(), 10);

        let unequal_quotient_and_remainder: Vec<_> =
            (0..4).map(|index| partition(11, 4, index)).collect();
        assert_eq!(unequal_quotient_and_remainder, [3, 3, 3, 2]);
        assert_eq!(unequal_quotient_and_remainder.into_iter().sum::<u64>(), 11);
    }

    #[test]
    fn maximum_quote_limits_shards_to_buckets_that_can_admit_it() {
        let limits = ResolvedLimits {
            max_items_per_request: 1,
            rate_units_per_second: 8,
            rate_burst_units: 500,
        };
        let eight = LocalSharding::new(NonZeroUsize::new(8).unwrap());

        let ceiling = shard_ceiling(&limits, Some(CostUnits(300)));
        assert_eq!(ceiling, 1);
        assert_eq!(rate_params(&limits, eight, ceiling).shards, 1);

        // The bound is exact, not conservative: five shards of a 500-unit
        // burst are each 100 units, which is precisely one maximum quote.
        let ceiling = shard_ceiling(&limits, Some(CostUnits(100)));
        assert_eq!(ceiling, 5);
        assert_eq!(rate_params(&limits, eight, ceiling).shards, 5);
    }

    /// The limiter is shared by every principal of the account, but the
    /// largest quote belongs to one principal's cost table. A split sized
    /// from whichever snapshot happened to install first left a principal
    /// with a heavier table unable to spend its largest quote in any shard —
    /// a permanent `UnpriceableUnderLimits` for a request the account's whole
    /// burst can hold, and one that was admitted before the split existed.
    #[test]
    fn a_heavier_principal_resplits_the_account_bucket_it_shares() {
        let limits = ResolvedLimits {
            max_items_per_request: 1,
            rate_units_per_second: 800,
            rate_burst_units: 800,
        };
        let eight = LocalSharding::new(NonZeroUsize::new(8).unwrap());

        // A light principal installs first: eight buckets of 100 units.
        let limiter = AccountLimiter::new(Generation(7), &limits, Some(CostUnits(10)), eight);
        assert_eq!(limiter.current().shard_count(), 8);

        // A second principal of the same account, at the same generation,
        // prices a 500-unit request. Its quote fits the account's burst, so
        // publication accepted it and admission must too.
        limiter.update(Generation(7), &limits, Some(CostUnits(500)), eight);
        assert_eq!(limiter.current().shard_count(), 1);
        assert_eq!(
            limiter.check_n(NonZeroU32::new(500).unwrap()),
            Ok(Ok(())),
            "a quote within the account's burst must be admissible"
        );
    }

    /// A raw install carries no publication proof, so nothing bounds its
    /// quotes; the account it shares must fall back to one bucket even if a
    /// publishable sibling already split it.
    #[test]
    fn an_unproven_principal_collapses_the_account_split() {
        let limits = ResolvedLimits {
            max_items_per_request: 1,
            rate_units_per_second: 800,
            rate_burst_units: 800,
        };
        let eight = LocalSharding::new(NonZeroUsize::new(8).unwrap());
        let limiter = AccountLimiter::new(Generation(1), &limits, Some(CostUnits(10)), eight);
        assert_eq!(limiter.current().shard_count(), 8);

        limiter.update(Generation(2), &limits, None, eight);
        assert_eq!(limiter.current().shard_count(), 1);
    }

    /// Only a *strictly* newer generation installs its limits, so the first
    /// snapshot at a generation owns them. Accepting an equal generation
    /// would make the account's bucket depend on which principal was
    /// published last.
    #[test]
    fn an_equal_generation_does_not_reinstall_the_account_limits() {
        let limits = ResolvedLimits {
            max_items_per_request: 1,
            rate_units_per_second: 800,
            rate_burst_units: 800,
        };
        let limiter = AccountLimiter::new(
            Generation(5),
            &limits,
            Some(CostUnits(10)),
            LocalSharding::SINGLE,
        );

        let widened = ResolvedLimits {
            rate_burst_units: 8_000,
            ..limits
        };
        limiter.update(
            Generation(5),
            &widened,
            Some(CostUnits(10)),
            LocalSharding::SINGLE,
        );

        assert_eq!(
            limiter.config.lock().unwrap().installed.burst,
            800,
            "a same-generation snapshot cannot rewrite the installed limits"
        );
    }

    /// The ceiling is account-wide safety evidence, not a limit value, so a
    /// snapshot too old to install its rate and burst still narrows the
    /// split: that principal is admitting requests now.
    #[test]
    fn a_stale_snapshot_still_narrows_the_split_it_cannot_widen() {
        let limits = ResolvedLimits {
            max_items_per_request: 1,
            rate_units_per_second: 800,
            rate_burst_units: 800,
        };
        let eight = LocalSharding::new(NonZeroUsize::new(8).unwrap());
        let limiter = AccountLimiter::new(Generation(9), &limits, Some(CostUnits(10)), eight);

        // Ten times the burst, but a quote so heavy that only two shards of
        // it could hold one: stale limits, binding evidence.
        let stale = ResolvedLimits {
            rate_burst_units: 8_000,
            ..limits
        };
        limiter.update(Generation(4), &stale, Some(CostUnits(4_000)), eight);

        let config = limiter.config.lock().unwrap();
        assert_eq!(config.generation, Generation(9), "older limits stay out");
        assert_eq!(config.installed.burst, 800, "and so does the older burst");
        assert_eq!(config.installed.shards, 2, "but its ceiling still binds");
    }
}
