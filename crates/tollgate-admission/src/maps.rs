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
    SnapshotUpdate,
};

fn installed_generation(entry: &MapEntry) -> Option<Generation> {
    match entry {
        MapEntry::Present(state) => Some(state.snapshot.generation),
        MapEntry::NegativeUntil { generation, .. } => *generation,
    }
}

/// Should `new` replace `existing`? Present entries only ever move forward by
/// generation. Versioned negative entries are tombstones: only a strictly
/// newer positive snapshot may replace them.
fn supersedes(existing: Option<&MapEntry>, new_generation: Generation) -> bool {
    match existing.and_then(installed_generation) {
        Some(current) => new_generation > current,
        None => true,
    }
}

fn tombstone(
    existing: Option<&MapEntry>,
    until: Timestamp,
    generation: Option<Generation>,
) -> MapEntry {
    let installed = existing.and_then(installed_generation);
    // A delayed removal must not revoke a snapshot that is known to be
    // newer. Equal is intentional: removing generation N creates the
    // generation-N tombstone that only N+1 may supersede.
    if matches!((installed, generation), (Some(current), Some(incoming)) if incoming < current) {
        return existing
            .cloned()
            .expect("installed generation came from an entry");
    }
    MapEntry::NegativeUntil {
        until,
        generation: match (installed, generation) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        },
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
        let limiter =
            self.limiters
                .limiter_for(snapshot.account_id, snapshot.generation, &snapshot.limits);
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
        self.install_negative_at_generation(principal, until, None);
    }

    fn install_negative_at_generation(
        &self,
        principal: Principal,
        until: Timestamp,
        generation: Option<Generation>,
    ) {
        self.cache.entry(principal).and_compute_with(|existing| {
            let existing = existing.map(|e| e.into_value());
            moka::ops::compute::Op::Put(tombstone(existing.as_ref(), until, generation))
        });
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
        let limiter =
            self.limiters
                .limiter_for(snapshot.account_id, snapshot.generation, &snapshot.limits);
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
        self.install_negative_at_generation(principal, until, None);
    }

    fn install_negative_at_generation(
        &self,
        principal: Principal,
        until: Timestamp,
        generation: Option<Generation>,
    ) {
        self.rcu(|map| {
            let negative = tombstone(map.get(&principal), until, generation);
            map.insert(principal, negative);
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
                let limiter = self.limiters.limiter_for(
                    snapshot.account_id,
                    snapshot.generation,
                    &snapshot.limits,
                );
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

    fn apply_many(&self, updates: Vec<SnapshotUpdate>) {
        enum Prepared {
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
        let prepared: Vec<_> = updates
            .into_iter()
            .map(|update| match update {
                SnapshotUpdate::Present {
                    principal,
                    snapshot,
                    lease,
                } => {
                    let limiter = self.limiters.limiter_for(
                        snapshot.account_id,
                        snapshot.generation,
                        &snapshot.limits,
                    );
                    Prepared::Present {
                        principal,
                        snapshot,
                        lease,
                        limiter,
                    }
                }
                SnapshotUpdate::Negative {
                    principal,
                    until,
                    generation,
                } => Prepared::Negative {
                    principal,
                    until,
                    generation,
                },
            })
            .collect();
        self.rcu(|map| {
            for update in &prepared {
                match update {
                    Prepared::Present {
                        principal,
                        snapshot,
                        lease,
                        limiter,
                    } => {
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
                    Prepared::Negative {
                        principal,
                        until,
                        generation,
                    } => {
                        let negative = tombstone(map.get(principal), *until, *generation);
                        map.insert(*principal, negative);
                    }
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
