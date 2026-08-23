//! The two candidate snapshot-map implementations.
//!
//! Both satisfy [`SnapshotMap`]; the perf gate's `admission/snapshot_lookup`
//! benches decide which one a deployment should use. Writes are rare
//! (control-plane pushes), reads are every request — both structures are
//! chosen for that asymmetry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use jiff::Timestamp;

use tollgate_core::{AccountSnapshot, Generation};

use crate::generation_model::{accept_negative, accept_positive};
use crate::state::{
    AccountAdmissionState, AccountLimiters, LeaseSlot, MapEntry, Principal, SnapshotMap,
    SnapshotUpdate,
};

#[derive(Default)]
struct GenerationWatermarks {
    by_principal: HashMap<Principal, Generation>,
}

impl GenerationWatermarks {
    fn accept_positive(&mut self, principal: Principal, incoming: Generation) -> bool {
        let (next, accepted) =
            accept_positive(self.by_principal.get(&principal).copied(), incoming);
        if let Some(next) = next {
            self.by_principal.insert(principal, next);
        }
        accepted
    }

    fn accept_negative(&mut self, principal: Principal, incoming: Option<Generation>) -> bool {
        let (next, accepted) =
            accept_negative(self.by_principal.get(&principal).copied(), incoming);
        if let Some(next) = next {
            self.by_principal.insert(principal, next);
        }
        accepted
    }
}

enum PreparedUpdate {
    Present {
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
        lease: Arc<LeaseSlot>,
        limiter: Arc<crate::state::AccountLimiter>,
    },
    Negative {
        principal: Principal,
        until: Timestamp,
        generation: Option<Generation>,
    },
}

fn apply_prepared(
    map: &mut HashMap<Principal, MapEntry>,
    watermarks: &mut GenerationWatermarks,
    updates: &[PreparedUpdate],
) {
    for update in updates {
        match update {
            PreparedUpdate::Present {
                principal,
                snapshot,
                lease,
                limiter,
            } => {
                if watermarks.accept_positive(*principal, snapshot.generation) {
                    map.insert(
                        *principal,
                        MapEntry::Present(AccountAdmissionState::new(
                            Arc::clone(snapshot),
                            Arc::clone(lease),
                            Arc::clone(limiter),
                        )),
                    );
                }
            }
            PreparedUpdate::Negative {
                principal,
                until,
                generation,
            } => {
                if watermarks.accept_negative(*principal, *generation) {
                    map.insert(*principal, MapEntry::NegativeUntil { until: *until });
                }
            }
        }
    }
}

/// `moka`-backed bounded cache. Values are `Arc`-cheap by construction (the
/// review's caution about moka cloning values on retrieval).
pub struct MokaSnapshotMap {
    cache: moka::sync::Cache<Principal, MapEntry>,
    watermarks: Mutex<GenerationWatermarks>,
    limiters: AccountLimiters,
}

impl MokaSnapshotMap {
    #[must_use]
    pub fn new(max_capacity: u64) -> Self {
        MokaSnapshotMap {
            cache: moka::sync::Cache::new(max_capacity),
            watermarks: Mutex::new(GenerationWatermarks::default()),
            limiters: AccountLimiters::default(),
        }
    }
}

impl SnapshotMap for MokaSnapshotMap {
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        self.cache.get(principal)
    }

    fn install(&self, principal: Principal, snapshot: Arc<AccountSnapshot>, lease: Arc<LeaseSlot>) {
        let limiter =
            self.limiters
                .limiter_for(snapshot.account_id, snapshot.generation, &snapshot.limits);
        let mut watermarks = self.watermarks.lock().expect("watermarks poisoned");
        if watermarks.accept_positive(principal, snapshot.generation) {
            self.cache.insert(
                principal,
                MapEntry::Present(AccountAdmissionState::new(snapshot, lease, limiter)),
            );
        }
    }

    fn install_negative(&self, principal: Principal, until: Timestamp) {
        self.install_negative_at_generation(principal, until, None);
    }

    fn install_negative_at_generation(
        &self,
        principal: Principal,
        until: Timestamp,
        generation: Option<Generation>,
    ) {
        let mut watermarks = self.watermarks.lock().expect("watermarks poisoned");
        if watermarks.accept_negative(principal, generation) {
            self.cache
                .insert(principal, MapEntry::NegativeUntil { until });
        }
    }

    fn remove(&self, principal: &Principal) {
        let _watermarks = self.watermarks.lock().expect("watermarks poisoned");
        self.cache.invalidate(principal);
    }
}

/// Copy-on-write immutable map behind `arc-swap`. Reads are a load plus one
/// hash lookup; writes clone the whole map, which is acceptable because they
/// happen at control-plane frequency, not request frequency.
pub struct ArcSwapSnapshotMap {
    map: ArcSwap<HashMap<Principal, MapEntry>>,
    watermarks: Mutex<GenerationWatermarks>,
    limiters: AccountLimiters,
    max_negative_entries: usize,
}

impl ArcSwapSnapshotMap {
    pub const DEFAULT_MAX_NEGATIVE_ENTRIES: usize = 4_096;

    #[must_use]
    pub fn new() -> Self {
        Self::with_max_negative_entries(Self::DEFAULT_MAX_NEGATIVE_ENTRIES)
    }

    #[must_use]
    pub fn with_max_negative_entries(max_negative_entries: usize) -> Self {
        ArcSwapSnapshotMap {
            map: ArcSwap::default(),
            watermarks: Mutex::new(GenerationWatermarks::default()),
            limiters: AccountLimiters::default(),
            max_negative_entries,
        }
    }

    /// Resolve each update's limiter ahead of the map write.
    ///
    /// The positives are resolved as one batch under a single registry lock
    /// (#8): this used to take and release that mutex once per entry, so a
    /// bulk install of N principals took it N times — inside the very function
    /// that exists to make bulk installs cheap.
    fn prepare(&self, updates: Vec<SnapshotUpdate>) -> Vec<PreparedUpdate> {
        // Negatives need no limiter, so they skip the registry entirely and
        // keep their place by index rather than by being carried through it.
        let mut prepared: Vec<Option<PreparedUpdate>> = Vec::with_capacity(updates.len());
        let mut positives = Vec::new();
        for update in updates {
            match update {
                SnapshotUpdate::Present {
                    principal,
                    snapshot,
                    lease,
                } => {
                    prepared.push(None);
                    positives.push((prepared.len() - 1, principal, snapshot, lease));
                }
                SnapshotUpdate::Negative {
                    principal,
                    until,
                    generation,
                } => prepared.push(Some(PreparedUpdate::Negative {
                    principal,
                    until,
                    generation,
                })),
            }
        }

        let resolved = self.limiters.limiters_for(
            positives,
            |(_, _, snapshot, _)| (snapshot.account_id, snapshot.generation),
            |(_, _, snapshot, _)| snapshot.limits,
        );
        for ((slot, principal, snapshot, lease), limiter) in resolved {
            prepared[slot] = Some(PreparedUpdate::Present {
                principal,
                snapshot,
                lease,
                limiter,
            });
        }

        prepared
            .into_iter()
            .map(|update| update.expect("every slot is filled by exactly one branch above"))
            .collect()
    }

    /// Writers serialize only against other control-plane writers. Request
    /// reads remain one ArcSwap load and one hash lookup.
    fn write(&self, updates: &[PreparedUpdate], now: Option<Timestamp>) {
        let mut watermarks = self.watermarks.lock().expect("watermarks poisoned");
        let current = self.map.load_full();
        let mut next = HashMap::clone(&current);
        if let Some(now) = now {
            next.retain(
                |_, entry| !matches!(entry, MapEntry::NegativeUntil { until } if *until <= now),
            );
        }
        apply_prepared(&mut next, &mut watermarks, updates);
        trim_negatives(&mut next, self.max_negative_entries);
        self.map.store(Arc::new(next));
    }
}

impl Default for ArcSwapSnapshotMap {
    fn default() -> Self {
        Self::new()
    }
}

fn trim_negatives(map: &mut HashMap<Principal, MapEntry>, max_negative_entries: usize) {
    let mut negatives: Vec<_> = map
        .iter()
        .filter_map(|(principal, entry)| match entry {
            MapEntry::NegativeUntil { until } => Some((*until, *principal)),
            MapEntry::Present(_) => None,
        })
        .collect();
    let remove = negatives.len().saturating_sub(max_negative_entries);
    if remove == 0 {
        return;
    }
    negatives.sort_unstable();
    for (_, principal) in negatives.into_iter().take(remove) {
        map.remove(&principal);
    }
}

impl SnapshotMap for ArcSwapSnapshotMap {
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        self.map.load().get(principal).cloned()
    }

    fn install(&self, principal: Principal, snapshot: Arc<AccountSnapshot>, lease: Arc<LeaseSlot>) {
        let updates = self.prepare(vec![SnapshotUpdate::Present {
            principal,
            snapshot,
            lease,
        }]);
        self.write(&updates, None);
    }

    fn install_negative(&self, principal: Principal, until: Timestamp) {
        self.install_negative_at_generation(principal, until, None);
    }

    fn install_negative_at_generation(
        &self,
        principal: Principal,
        until: Timestamp,
        generation: Option<Generation>,
    ) {
        let updates = self.prepare(vec![SnapshotUpdate::Negative {
            principal,
            until,
            generation,
        }]);
        self.write(&updates, None);
    }

    fn remove(&self, principal: &Principal) {
        let _watermarks = self.watermarks.lock().expect("watermarks poisoned");
        let current = self.map.load_full();
        let mut next = HashMap::clone(&current);
        next.remove(principal);
        self.map.store(Arc::new(next));
    }

    /// One map clone for the whole batch — the point of the override: bulk
    /// loading N principals costs O(N), not O(N²).
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
        let prepared = self.prepare(updates);
        self.write(&prepared, None);
    }

    fn apply_many_at(&self, updates: Vec<SnapshotUpdate>, now: Timestamp) {
        let prepared = self.prepare(updates);
        self.write(&prepared, Some(now));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tollgate_core::{
        AccountId, AccountStatus, CostTable, CostUnits, PermissionBits, ResolvedLimits,
    };

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    fn snapshot(generation: u64) -> Arc<AccountSnapshot> {
        Arc::new(AccountSnapshot {
            account_id: AccountId(1),
            key_id: None,
            generation: Generation(generation),
            status: AccountStatus::Active,
            valid_until: t(10_000),
            permissions: PermissionBits::ALL,
            limits: ResolvedLimits {
                max_items_per_request: 100,
                rate_units_per_second: 1_000,
                rate_burst_units: 5_000,
            },
            cost_table: Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        })
    }

    fn generation_of(map: &impl SnapshotMap, p: &Principal) -> Option<u64> {
        match map.get(p)? {
            MapEntry::Present(state) => Some(state.snapshot.generation.0),
            MapEntry::NegativeUntil { .. } => None,
        }
    }

    fn exercises_map(map: impl SnapshotMap) {
        let p = Principal(9);
        assert!(map.get(&p).is_none());

        // Install, then attempt a rollback to an older generation: no-op.
        map.install(p, snapshot(5), LeaseSlot::empty());
        assert_eq!(generation_of(&map, &p), Some(5));
        map.install(p, snapshot(3), LeaseSlot::empty());
        assert_eq!(generation_of(&map, &p), Some(5));
        // Same generation is also a no-op (idempotent replay).
        map.install(p, snapshot(5), LeaseSlot::empty());
        assert_eq!(generation_of(&map, &p), Some(5));
        // Newer generation replaces.
        map.install(p, snapshot(6), LeaseSlot::empty());
        assert_eq!(generation_of(&map, &p), Some(6));

        // Negative entries replace and are replaced by real snapshots.
        map.install_negative(p, t(100));
        assert!(matches!(map.get(&p), Some(MapEntry::NegativeUntil { .. })));
        // The negative retained generation 6: a delayed generation-1 push
        // cannot resurrect the revoked principal.
        map.install(p, snapshot(1), LeaseSlot::empty());
        assert!(matches!(map.get(&p), Some(MapEntry::NegativeUntil { .. })));
        map.install(p, snapshot(7), LeaseSlot::empty());
        assert_eq!(generation_of(&map, &p), Some(7));

        // The symmetric reorder is safe too: an old removal cannot revoke a
        // snapshot that has already advanced beyond it.
        map.install_negative_at_generation(p, t(200), Some(Generation(6)));
        assert_eq!(generation_of(&map, &p), Some(7));
        map.install_negative_at_generation(p, t(200), Some(Generation(7)));
        assert!(matches!(map.get(&p), Some(MapEntry::NegativeUntil { .. })));

        // Removing the request-visible entry must not remove the generation
        // watermark. Otherwise cache eviction could resurrect stale state.
        map.remove(&p);
        assert!(map.get(&p).is_none());
        map.install(p, snapshot(7), LeaseSlot::empty());
        assert!(map.get(&p).is_none());
        map.install(p, snapshot(8), LeaseSlot::empty());
        assert_eq!(generation_of(&map, &p), Some(8));
    }

    #[test]
    fn install_many_respects_generations_in_one_write() {
        let map = ArcSwapSnapshotMap::new();
        map.install(Principal(1), snapshot(5), LeaseSlot::empty());
        map.install_many(vec![
            (Principal(1), snapshot(3), LeaseSlot::empty()), // rollback: discarded
            (Principal(2), snapshot(1), LeaseSlot::empty()),
            (Principal(3), snapshot(1), LeaseSlot::empty()),
        ]);
        assert_eq!(generation_of(&map, &Principal(1)), Some(5));
        assert_eq!(generation_of(&map, &Principal(2)), Some(1));
        assert_eq!(generation_of(&map, &Principal(3)), Some(1));
    }

    #[test]
    fn moka_map_contract() {
        exercises_map(MokaSnapshotMap::new(1_000));
    }

    #[test]
    fn arc_swap_map_contract() {
        exercises_map(ArcSwapSnapshotMap::new());
    }

    #[test]
    fn apply_many_at_default_and_arc_delegate_apply_the_batch() {
        // Moka uses SnapshotMap's default apply_many_at; wrapping it in Arc
        // also exercises the delegating implementation used by managers.
        let map = Arc::new(MokaSnapshotMap::new(10));
        map.apply_many_at(
            vec![SnapshotUpdate::Present {
                principal: Principal(1),
                snapshot: snapshot(1),
                lease: LeaseSlot::empty(),
            }],
            t(10),
        );
        assert_eq!(generation_of(&map, &Principal(1)), Some(1));
    }

    #[test]
    fn arc_swap_negative_cache_is_bounded_and_evicts_oldest_deadline_first() {
        let map = ArcSwapSnapshotMap::with_max_negative_entries(2);
        map.install(Principal(1), snapshot(1), LeaseSlot::empty());
        map.install_negative(Principal(2), t(30));
        map.install_negative(Principal(3), t(10));
        map.install_negative(Principal(4), t(20));

        assert_eq!(generation_of(&map, &Principal(1)), Some(1));
        assert!(map.get(&Principal(3)).is_none());
        assert!(matches!(
            map.get(&Principal(2)),
            Some(MapEntry::NegativeUntil { until }) if until == t(30)
        ));
        assert!(matches!(
            map.get(&Principal(4)),
            Some(MapEntry::NegativeUntil { until }) if until == t(20)
        ));
    }

    #[test]
    fn arc_swap_control_write_drops_expired_negatives() {
        let map = ArcSwapSnapshotMap::with_max_negative_entries(10);
        map.install_negative(Principal(1), t(10));
        map.install_negative(Principal(2), t(20));

        map.apply_many_at(Vec::new(), t(10));

        assert!(map.get(&Principal(1)).is_none());
        assert!(matches!(
            map.get(&Principal(2)),
            Some(MapEntry::NegativeUntil { until }) if until == t(20)
        ));
    }

    #[test]
    fn negative_eviction_preserves_generation_monotonicity() {
        let map = ArcSwapSnapshotMap::with_max_negative_entries(0);
        let principal = Principal(1);
        map.install(principal, snapshot(5), LeaseSlot::empty());
        map.install_negative_at_generation(principal, t(10), Some(Generation(5)));
        assert!(map.get(&principal).is_none(), "zero-cap cache must evict");

        map.install(principal, snapshot(4), LeaseSlot::empty());
        map.install(principal, snapshot(5), LeaseSlot::empty());
        assert!(
            map.get(&principal).is_none(),
            "eviction must not admit a stale or replayed snapshot"
        );
        map.install(principal, snapshot(6), LeaseSlot::empty());
        assert_eq!(generation_of(&map, &principal), Some(6));
    }

    #[test]
    fn many_unknowns_leave_only_the_configured_number_visible() {
        const CAP: usize = 17;
        let map = ArcSwapSnapshotMap::with_max_negative_entries(CAP);
        for raw in 0..1_000 {
            map.install_negative(Principal(raw), t(i64::try_from(raw).unwrap() + 1));
        }
        let visible = (0..1_000)
            .filter(|raw| map.get(&Principal(*raw)).is_some())
            .count();
        assert_eq!(visible, CAP);
    }

    /// A snapshot for a named account, so a test can build the many-account
    /// workload the single-account `snapshot()` fixture cannot express.
    fn snapshot_for(account: u128, generation: u64) -> Arc<AccountSnapshot> {
        Arc::new(AccountSnapshot {
            account_id: AccountId(account),
            ..(*snapshot(generation)).clone()
        })
    }

    /// Issue #8 moved the dead-entry sweep off the per-lookup path, so the
    /// registry no longer reclaims on every call. It must still reclaim: an
    /// unbounded registry would defeat the bounded snapshot cache it sits
    /// beside. Nothing asserted this before, which is what made deferring the
    /// sweep a live question rather than an obvious win.
    #[test]
    fn the_limiter_registry_stays_bounded_across_account_churn() {
        let map = ArcSwapSnapshotMap::new();
        // A *distinct* account each time, installed and then evicted, so every
        // limiter's strong reference dies and its registry entry becomes
        // reclaimable. Reusing account ids would prove nothing: `insert`
        // replaces in place, so the registry would stay small even if the
        // sweep never ran at all.
        for account in 0..1_000u128 {
            let principal = Principal(account);
            map.install(principal, snapshot_for(account, 1), LeaseSlot::empty());
            map.remove(&principal);
        }

        let held = map.limiters.len();
        assert!(
            held <= 64,
            "1,000 dead accounts left {held} registry entries; the sweep is \
             not reclaiming, and the registry grows without bound"
        );
    }

    /// The quadratic #8 describes needs *live* accounts: a registry that keeps
    /// emptying is cheap to walk however often you do it. With a thousand
    /// accounts all still present, sweeping per install would walk on the
    /// order of a thousand entries a thousand times.
    ///
    /// Total entries walked is therefore the measure, not sweep count — this
    /// is the property the benchmark demonstrates, pinned as a test so a
    /// future change cannot quietly put the walk back on the install path.
    #[test]
    fn sweeping_costs_work_proportional_to_installs_not_to_their_square() {
        let map = ArcSwapSnapshotMap::new();
        // Kept alive: no `remove`, so every account stays in the registry and
        // each sweep has the full set to walk.
        for account in 0..1_000u128 {
            map.install(
                Principal(account),
                snapshot_for(account, 1),
                LeaseSlot::empty(),
            );
        }

        let walked = map.limiters.swept_entries();
        assert!(
            walked <= 4_000,
            "1,000 installs walked {walked} registry entries; sweeping is \
             back on the per-install path, which is O(N·A) again"
        );
        assert!(
            walked > 0,
            "no entry was ever walked, so either nothing sweeps or the \
             measurement is broken — both make the bound above vacuous"
        );
    }

    /// The sweep is deferred, not skipped: entries for accounts still present
    /// in the map must survive it, or a live account would lose its shared
    /// bucket and every principal would get its own.
    #[test]
    fn sweeping_never_reclaims_a_live_account() {
        let map = ArcSwapSnapshotMap::new();
        map.install(Principal(0), snapshot_for(7, 1), LeaseSlot::empty());
        let before = match map.get(&Principal(0)).unwrap() {
            MapEntry::Present(state) => state.limiter.clone(),
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };

        // Enough churn to force several sweeps past the growth watermark.
        for account in 100..300u128 {
            let principal = Principal(account);
            map.install(principal, snapshot_for(account, 1), LeaseSlot::empty());
            map.remove(&principal);
        }

        map.install(Principal(1), snapshot_for(7, 1), LeaseSlot::empty());
        let after = match map.get(&Principal(1)).unwrap() {
            MapEntry::Present(state) => state.limiter.clone(),
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };
        assert!(
            Arc::ptr_eq(&before, &after),
            "account 7 was still in the map; its limiter must have survived \
             the sweeps, or its principals stop sharing a bucket"
        );
    }

    /// Batching the registry lookup must not batch the *update*: two
    /// principals of one account can arrive in one write carrying different
    /// generations, and the newer must win whichever order they appear in.
    #[test]
    fn one_batch_with_two_generations_keeps_the_newer() {
        for reversed in [false, true] {
            let map = ArcSwapSnapshotMap::new();
            // A larger burst, so which generation won is observable: the
            // rate alone is not, without waiting for the bucket to refill.
            let faster = Arc::new(AccountSnapshot {
                limits: ResolvedLimits {
                    max_items_per_request: 100,
                    rate_units_per_second: 1_000,
                    rate_burst_units: 9_000,
                },
                ..(*snapshot(5)).clone()
            });
            let mut updates = vec![
                SnapshotUpdate::Present {
                    principal: Principal(1),
                    snapshot: snapshot(3),
                    lease: LeaseSlot::empty(),
                },
                SnapshotUpdate::Present {
                    principal: Principal(2),
                    snapshot: faster,
                    lease: LeaseSlot::empty(),
                },
            ];
            if reversed {
                updates.reverse();
            }
            map.apply_many(updates);

            let state = match map.get(&Principal(1)).unwrap() {
                MapEntry::Present(state) => state,
                MapEntry::NegativeUntil { .. } => unreachable!(),
            };
            assert_eq!(
                state.snapshot.generation,
                Generation(3),
                "each principal keeps its own snapshot"
            );
            // Both principals share one limiter, and generation 5's limits are
            // what it must be carrying: the older update cannot roll it back.
            let other = match map.get(&Principal(2)).unwrap() {
                MapEntry::Present(state) => state,
                MapEntry::NegativeUntil { .. } => unreachable!(),
            };
            assert!(Arc::ptr_eq(&state.limiter, &other.limiter));
            assert!(
                state
                    .limiter
                    .check_n(std::num::NonZeroU32::new(6_000).unwrap())
                    .is_ok(),
                "generation 5's larger burst must have been applied \
                 (reversed order: {reversed})"
            );
        }
    }

    /// Review finding #4: the advertised limit is an *account* limit — every
    /// principal of an account shares one limiter, so extra API keys cannot
    /// multiply the allowance.
    #[test]
    fn principals_of_one_account_share_the_limiter() {
        let map = ArcSwapSnapshotMap::new();
        map.install(Principal(1), snapshot(1), LeaseSlot::empty());
        map.install(Principal(2), snapshot(1), LeaseSlot::empty());
        let (a, b) = match (
            map.get(&Principal(1)).unwrap(),
            map.get(&Principal(2)).unwrap(),
        ) {
            (MapEntry::Present(a), MapEntry::Present(b)) => (a, b),
            _ => unreachable!(),
        };
        assert!(
            Arc::ptr_eq(&a.limiter, &b.limiter),
            "two principals of one account must draw from one bucket"
        );
    }

    /// Rate-limiter state carries over when limits are unchanged and resets
    /// when they change (the governor immutable-Quota caveat from the design
    /// review).
    #[test]
    fn limiter_survives_same_limit_reinstall() {
        let map = ArcSwapSnapshotMap::new();
        let p = Principal(1);
        map.install(p, snapshot(1), LeaseSlot::empty());
        let first = match map.get(&p).unwrap() {
            MapEntry::Present(s) => s,
            _ => unreachable!(),
        };
        map.install(p, snapshot(2), LeaseSlot::empty());
        let second = match map.get(&p).unwrap() {
            MapEntry::Present(s) => s,
            _ => unreachable!(),
        };
        assert!(Arc::ptr_eq(&first.limiter, &second.limiter));
        let before_update = second.limiter.current();

        let mut changed = (*snapshot(3)).clone();
        changed.limits.rate_units_per_second = 2_000;
        map.install(p, Arc::new(changed), LeaseSlot::empty());
        let third = match map.get(&p).unwrap() {
            MapEntry::Present(s) => s,
            _ => unreachable!(),
        };
        assert!(Arc::ptr_eq(&second.limiter, &third.limiter));
        assert!(!Arc::ptr_eq(&before_update, &third.limiter.current()));
    }

    #[test]
    fn limit_change_updates_every_principal_of_the_account() {
        let map = ArcSwapSnapshotMap::new();
        map.install(Principal(1), snapshot(1), LeaseSlot::empty());
        map.install(Principal(2), snapshot(1), LeaseSlot::empty());
        let before = match map.get(&Principal(2)).unwrap() {
            MapEntry::Present(state) => state.limiter.current(),
            _ => unreachable!(),
        };

        let mut changed = (*snapshot(2)).clone();
        changed.limits.rate_units_per_second = 2_000;
        map.install(Principal(1), Arc::new(changed), LeaseSlot::empty());

        let (a, b) = match (
            map.get(&Principal(1)).unwrap(),
            map.get(&Principal(2)).unwrap(),
        ) {
            (MapEntry::Present(a), MapEntry::Present(b)) => (a, b),
            _ => unreachable!(),
        };
        assert!(Arc::ptr_eq(&a.limiter, &b.limiter));
        assert!(!Arc::ptr_eq(&before, &b.limiter.current()));
    }

    #[test]
    fn stale_snapshot_cannot_roll_back_shared_limiter() {
        let map = ArcSwapSnapshotMap::new();
        let mut current = (*snapshot(5)).clone();
        current.limits.rate_units_per_second = 2_000;
        map.install(Principal(1), Arc::new(current), LeaseSlot::empty());
        let installed = match map.get(&Principal(1)).unwrap() {
            MapEntry::Present(state) => state.limiter.current(),
            _ => unreachable!(),
        };

        map.install(Principal(1), snapshot(3), LeaseSlot::empty());
        let after_stale = match map.get(&Principal(1)).unwrap() {
            MapEntry::Present(state) => state.limiter.current(),
            _ => unreachable!(),
        };
        assert!(Arc::ptr_eq(&installed, &after_stale));
    }
}
