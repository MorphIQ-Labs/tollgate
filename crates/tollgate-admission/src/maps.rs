//! The two candidate snapshot-map implementations.
//!
//! Both satisfy [`SnapshotMap`]; the perf gate's `admission/snapshot_lookup`
//! benches decide which one a deployment should use. Writes are rare
//! (control-plane pushes), reads are every request — both structures are
//! chosen for that asymmetry.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use jiff::Timestamp;

use tollgate_core::{AccountSnapshot, Generation};

use crate::state::{
    AccountAdmissionState, AccountLimiters, LeaseSlot, MapEntry, Principal, SnapshotMap,
};

fn installed_generation(entry: &MapEntry) -> Option<Generation> {
    match entry {
        MapEntry::Present(state) => Some(state.snapshot.generation),
        MapEntry::NegativeUntil(_) => None,
    }
}

/// Should `new` replace `existing`? Present entries only ever move forward by
/// generation; a negative entry is always replaceable by a real snapshot,
/// and a negative may replace a present entry only via `install_negative`
/// (which models explicit revocation-to-unknown, not reordering).
fn supersedes(existing: Option<&MapEntry>, new_generation: Generation) -> bool {
    match existing.and_then(installed_generation) {
        Some(current) => new_generation > current,
        None => true,
    }
}

/// `moka`-backed bounded cache. Values are `Arc`-cheap by construction (the
/// review's caution about moka cloning values on retrieval).
pub struct MokaSnapshotMap {
    cache: moka::sync::Cache<Principal, MapEntry>,
    limiters: AccountLimiters,
}

impl MokaSnapshotMap {
    #[must_use]
    pub fn new(max_capacity: u64) -> Self {
        MokaSnapshotMap {
            cache: moka::sync::Cache::new(max_capacity),
            limiters: AccountLimiters::default(),
        }
    }
}

impl SnapshotMap for MokaSnapshotMap {
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        self.cache.get(principal)
    }

    fn install(&self, principal: Principal, snapshot: Arc<AccountSnapshot>, lease: Arc<LeaseSlot>) {
        let generation = snapshot.generation;
        // The account-shared limiter, fetched outside the per-key closure.
        let limiter = self
            .limiters
            .limiter_for(snapshot.account_id, &snapshot.limits);
        // and_compute_with gives us an atomic read-modify-write per key, which
        // is what makes generation monotonicity hold under concurrent pushes.
        self.cache.entry(principal).and_compute_with(|existing| {
            let existing = existing.map(|e| e.into_value());
            if supersedes(existing.as_ref(), generation) {
                moka::ops::compute::Op::Put(MapEntry::Present(AccountAdmissionState::new(
                    Arc::clone(&snapshot),
                    Arc::clone(&lease),
                    Arc::clone(&limiter),
                )))
            } else {
                moka::ops::compute::Op::Nop
            }
        });
    }

    fn install_negative(&self, principal: Principal, until: Timestamp) {
        self.cache.insert(principal, MapEntry::NegativeUntil(until));
    }

    fn remove(&self, principal: &Principal) {
        self.cache.invalidate(principal);
    }
}

/// Copy-on-write immutable map behind `arc-swap`. Reads are a load plus one
/// hash lookup; writes clone the whole map, which is acceptable because they
/// happen at control-plane frequency, not request frequency.
#[derive(Default)]
pub struct ArcSwapSnapshotMap {
    map: ArcSwap<HashMap<Principal, MapEntry>>,
    limiters: AccountLimiters,
}

impl ArcSwapSnapshotMap {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read-copy-update loop shared by all mutations.
    fn rcu(&self, mutate: impl Fn(&mut HashMap<Principal, MapEntry>)) {
        self.map.rcu(|current| {
            let mut next = HashMap::clone(current);
            mutate(&mut next);
            next
        });
    }
}

impl SnapshotMap for ArcSwapSnapshotMap {
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        self.map.load().get(principal).cloned()
    }

    fn install(&self, principal: Principal, snapshot: Arc<AccountSnapshot>, lease: Arc<LeaseSlot>) {
        let limiter = self
            .limiters
            .limiter_for(snapshot.account_id, &snapshot.limits);
        self.rcu(|map| {
            if supersedes(map.get(&principal), snapshot.generation) {
                map.insert(
                    principal,
                    MapEntry::Present(AccountAdmissionState::new(
                        Arc::clone(&snapshot),
                        Arc::clone(&lease),
                        Arc::clone(&limiter),
                    )),
                );
            }
        });
    }

    fn install_negative(&self, principal: Principal, until: Timestamp) {
        self.rcu(|map| {
            map.insert(principal, MapEntry::NegativeUntil(until));
        });
    }

    fn remove(&self, principal: &Principal) {
        self.rcu(|map| {
            map.remove(principal);
        });
    }

    /// One map clone for the whole batch — the point of the override: bulk
    /// loading N principals costs O(N), not O(N²).
    fn install_many(&self, entries: Vec<(Principal, Arc<AccountSnapshot>, Arc<LeaseSlot>)>) {
        let prepared: Vec<_> = entries
            .into_iter()
            .map(|(principal, snapshot, lease)| {
                let limiter = self
                    .limiters
                    .limiter_for(snapshot.account_id, &snapshot.limits);
                (principal, snapshot, lease, limiter)
            })
            .collect();
        self.rcu(|map| {
            for (principal, snapshot, lease, limiter) in &prepared {
                if supersedes(map.get(principal), snapshot.generation) {
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
        });
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
            MapEntry::NegativeUntil(_) => None,
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
        assert!(matches!(map.get(&p), Some(MapEntry::NegativeUntil(_))));
        map.install(p, snapshot(1), LeaseSlot::empty());
        assert_eq!(generation_of(&map, &p), Some(1));

        map.remove(&p);
        assert!(map.get(&p).is_none());
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

        let mut changed = (*snapshot(3)).clone();
        changed.limits.rate_units_per_second = 2_000;
        map.install(p, Arc::new(changed), LeaseSlot::empty());
        let third = match map.get(&p).unwrap() {
            MapEntry::Present(s) => s,
            _ => unreachable!(),
        };
        assert!(!Arc::ptr_eq(&second.limiter, &third.limiter));
    }
}
