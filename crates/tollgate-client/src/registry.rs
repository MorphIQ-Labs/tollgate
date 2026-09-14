//! Off-path account membership and stable lease slots.
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use jiff::Timestamp;
use tokio::sync::watch;
use tollgate_admission::{LeaseSlot, MapEntry};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, EnforcementMode, LocalSharding, Principal,
};

// Every diagnostic sum has at most usize::MAX u64 contributions. The
// u128 bound is proved in AccountLifecycle.catalogue_total_fits; make the
// cardinality assumption a compile-time restriction on supported targets.
const _: () = assert!(usize::BITS <= 64);

/// Instance-wide diagnostic estimates. None means no contributing policy or
/// lease. Checked u128 sums cover every supported in-memory catalogue.
/// No admission reads this view.
#[derive(Debug, Clone, Default)]
pub struct RuntimeFundingReport {
    pub total_lease_remaining: Option<u128>,
    pub total_overage_spent: u128,
    pub total_overage_cap: Option<u128>,
    pub earliest_lease_usable_until: Option<Timestamp>,
}

/// Shared account slots. Slots retain irreversible overage spend for the
/// process lifetime; membership, timers, and managers have separate lifetimes.
pub struct SlotRegistry {
    inner: Mutex<Registry>,
    sharding: LocalSharding,
    changed: Option<watch::Sender<()>>,
}

#[derive(Default)]
struct Registry {
    slots: HashMap<AccountId, Arc<LeaseSlot>>,
    principals: HashMap<Principal, Arc<AccountSnapshot>>,
    members: HashMap<AccountId, HashMap<Principal, Arc<AccountSnapshot>>>,
    deadlines: BTreeSet<(Timestamp, Principal)>,
    dirty: BTreeSet<AccountId>,
    tracked: HashSet<Principal>,
    resolutions: HashMap<Principal, Timestamp>,
}

#[derive(Clone)]
pub(crate) struct AccountBinding {
    pub account: AccountId,
    pub slot: Arc<LeaseSlot>,
    pub snapshots: Vec<Arc<AccountSnapshot>>,
}

impl AccountBinding {
    pub fn eligible(&self, now: Timestamp) -> bool {
        self.snapshots
            .iter()
            .any(|snapshot| snapshot.status == AccountStatus::Active && now < snapshot.valid_until)
    }

    pub fn fundable(&self, now: Timestamp) -> bool {
        self.snapshots.iter().any(|snapshot| {
            snapshot.status == AccountStatus::Active
                && now < snapshot.valid_until
                && quota_usable(&self.slot, snapshot.enforcement_mode, now)
        })
    }
}

impl Default for SlotRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Registry::default()),
            sharding: LocalSharding::SINGLE,
            changed: None,
        }
    }
}

impl SlotRegistry {
    pub(crate) fn track(&self, principal: Principal) {
        if self.observes() {
            self.inner
                .lock()
                .expect("slot registry poisoned")
                .tracked
                .insert(principal);
        }
    }

    pub(crate) fn retain(&self, principals: &HashSet<Principal>) {
        if self.observes() {
            let mut inner = self.inner.lock().expect("slot registry poisoned");
            inner.tracked.clone_from(principals);
            // The predicate is pure and total -- every untracked resolution
            // goes, whatever order they are visited in.
            #[allow(
                clippy::disallowed_methods,
                reason = "pure, total predicate: the visit order cannot change which entries survive"
            )]
            inner
                .resolutions
                .retain(|principal, _| principals.contains(principal));
        }
    }

    pub(crate) fn resolution_counts(&self, now: Timestamp) -> (usize, usize) {
        let inner = self.inner.lock().expect("slot registry poisoned");
        #[allow(
            clippy::disallowed_methods,
            reason = "counts unresolved principals; a count does not depend on the order they are counted in"
        )]
        let unresolved = inner
            .tracked
            .iter()
            .filter(|principal| {
                inner
                    .resolutions
                    .get(principal)
                    .is_none_or(|until| now >= *until)
            })
            .count();
        (inner.tracked.len(), unresolved)
    }

    pub(crate) fn funding(&self, now: Timestamp) -> RuntimeFundingReport {
        let inner = self.inner.lock().expect("slot registry poisoned");
        let mut report = RuntimeFundingReport::default();
        #[allow(
            clippy::disallowed_methods,
            reason = "sums overage across slots with checked addition; the total does not depend on the order the addends arrive in"
        )]
        for slot in inner.slots.values() {
            report.total_overage_spent = report
                .total_overage_spent
                .checked_add(u128::from(slot.overage().spent().get()))
                .expect("at most usize::MAX u64 contributions fit u128");
            if let Some(lease) = slot.load() {
                let sum = report
                    .total_lease_remaining
                    .unwrap_or(0)
                    .checked_add(u128::from(lease.remaining().get()))
                    .expect("at most usize::MAX u64 contributions fit u128");
                report.total_lease_remaining = Some(sum);
            }
        }
        #[allow(
            clippy::disallowed_methods,
            reason = "reduces each account's members to `any(..)` and `max(..)`; neither depends on the order they are visited in"
        )]
        for (account, members) in &inner.members {
            if members
                .values()
                .any(|s| s.status == AccountStatus::Active && now < s.valid_until)
                && let Some(lease) = inner.slots[account].load()
            {
                let until = lease.usable_until();
                report.earliest_lease_usable_until = Some(
                    report
                        .earliest_lease_usable_until
                        .map_or(until, |old| old.min(until)),
                );
            }
            let cap = members
                .values()
                .filter(|s| s.status == AccountStatus::Active && now < s.valid_until)
                .filter_map(|s| s.enforcement_mode.overage_cap())
                .max();
            if let Some(cap) = cap {
                let sum = report
                    .total_overage_cap
                    .unwrap_or(0)
                    .checked_add(u128::from(cap.get()))
                    .expect("at most usize::MAX u64 contributions fit u128");
                report.total_overage_cap = Some(sum);
            }
        }
        report
    }
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[must_use]
    pub fn with_sharding(sharding: LocalSharding) -> Arc<Self> {
        Arc::new(Self {
            sharding,
            ..Self::default()
        })
    }

    pub(crate) fn observed(sharding: LocalSharding) -> (Arc<Self>, watch::Receiver<()>) {
        let (changed, receiver) = watch::channel(());
        (
            Arc::new(Self {
                sharding,
                changed: Some(changed),
                ..Self::default()
            }),
            receiver,
        )
    }

    /// Create or retrieve an account's stable slot. This is a control-plane
    /// primitive; it creates no membership and never starts a manager.
    #[must_use]
    pub fn slot(&self, account: AccountId) -> Arc<LeaseSlot> {
        Arc::clone(
            self.inner
                .lock()
                .expect("slot registry poisoned")
                .slots
                .entry(account)
                .or_insert_with(|| LeaseSlot::with_sharding(account, self.sharding)),
        )
    }

    #[must_use]
    pub fn sharding(&self) -> LocalSharding {
        self.sharding
    }

    pub(crate) fn observes(&self) -> bool {
        self.changed.is_some()
    }

    /// Publish the actual map result, never an offered snapshot that the map
    /// may have rejected. The snapshot manager serializes publication and this
    /// observation; the runtime does not expose its map's write surface.
    pub(crate) fn observe_many(
        &self,
        entries: impl IntoIterator<Item = (Principal, Option<MapEntry>)>,
    ) {
        let mut inner = self.inner.lock().expect("slot registry poisoned");
        for (principal, entry) in entries {
            let until = match &entry {
                Some(MapEntry::Present(state)) => Some(state.snapshot.valid_until),
                Some(MapEntry::NegativeUntil { until }) => Some(*until),
                None => None,
            };
            if let Some(until) = until {
                inner.resolutions.insert(principal, until);
            } else {
                inner.resolutions.remove(&principal);
            }
            if let Some(previous) = inner.principals.remove(&principal) {
                inner.deadlines.remove(&(previous.valid_until, principal));
                let account = previous.account_id;
                if let Some(members) = inner.members.get_mut(&account) {
                    members.remove(&principal);
                    if members.is_empty() {
                        inner.members.remove(&account);
                    }
                }
                inner.dirty.insert(account);
            }
            if let Some(MapEntry::Present(state)) = entry {
                let snapshot = Arc::clone(&state.snapshot);
                let account = snapshot.account_id;
                inner.deadlines.insert((snapshot.valid_until, principal));
                inner.principals.insert(principal, Arc::clone(&snapshot));
                inner
                    .members
                    .entry(account)
                    .or_default()
                    .insert(principal, snapshot);
                inner.dirty.insert(account);
            }
        }
        drop(inner);
        self.wake();
    }

    fn wake(&self) {
        if let Some(changed) = &self.changed {
            changed.send_replace(());
        }
    }

    pub(crate) fn drain_changes(&self, now: Timestamp) -> Vec<AccountBinding> {
        let mut inner = self.inner.lock().expect("slot registry poisoned");
        while let Some(&(at, principal)) = inner.deadlines.first() {
            if at > now {
                break;
            }
            inner.deadlines.pop_first();
            if let Some(snapshot) = inner.principals.get(&principal) {
                let account = snapshot.account_id;
                inner.dirty.insert(account);
            }
        }
        let dirty = std::mem::take(&mut inner.dirty);
        dirty
            .into_iter()
            .map(|account| inner.binding(account))
            .collect()
    }

    pub(crate) fn next_expiry(&self) -> Option<Timestamp> {
        self.inner
            .lock()
            .expect("slot registry poisoned")
            .deadlines
            .first()
            .map(|(at, _)| *at)
    }

    /// Both callers are order-independent: `readiness` counts eligible and
    /// unfundable accounts, and `account_reports` re-keys into a `BTreeMap`.
    #[allow(
        clippy::disallowed_methods,
        reason = "callers count or re-key into a BTreeMap; the key order never reaches an output"
    )]
    pub(crate) fn bindings(&self) -> Vec<AccountBinding> {
        let inner = self.inner.lock().expect("slot registry poisoned");
        inner
            .members
            .keys()
            .map(|&account| inner.binding(account))
            .collect()
    }

    pub(crate) fn retained_slots(&self) -> usize {
        self.inner
            .lock()
            .expect("slot registry poisoned")
            .slots
            .len()
    }
}

impl Registry {
    /// The snapshot list is only ever reduced with `any(..)`, by
    /// `AccountBinding::eligible` and `fundable`, so its order is not an output.
    #[allow(
        clippy::disallowed_methods,
        reason = "the snapshots are only reduced with any(..); their order never reaches an output"
    )]
    fn binding(&self, account: AccountId) -> AccountBinding {
        AccountBinding {
            account,
            slot: Arc::clone(
                self.slots
                    .get(&account)
                    .expect("published membership has a slot"),
            ),
            snapshots: self
                .members
                .get(&account)
                .map(|members| members.values().cloned().collect())
                .unwrap_or_default(),
        }
    }
}

fn quota_usable(slot: &LeaseSlot, mode: EnforcementMode, now: Timestamp) -> bool {
    let lease_usable = slot
        .load()
        .is_some_and(|lease| now < lease.usable_until() && !lease.remaining().is_zero());
    lease_usable
        || mode
            .overage_cap()
            .is_some_and(|cap| !slot.overage().headroom(cap).is_zero())
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_methods,
        reason = "unit tests that build an arbitrary `now` the assertions are relative to; \
                  no assertion here depends on what the clock actually said"
    )]
    use super::*;
    use jiff::SignedDuration;
    use tollgate_core::{
        CommitFunding, CostUnits, EnforcementMode, FencingToken, LeaseGrant, LeaseId, LocalLease,
        Reservation,
    };
    fn lease_expiring_at(expires_at: Timestamp, units: u64) -> Arc<LocalLease> {
        Arc::new(LocalLease::new(
            LeaseGrant {
                lease_id: LeaseId(1),
                account_id: AccountId(1),
                fencing_token: FencingToken(1),
                units: CostUnits(units),
                expires_at,
            },
            CostUnits(0),
        ))
    }

    #[test]
    fn configured_sharding_is_preserved_by_stable_account_slots() {
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        let registry = SlotRegistry::with_sharding(sharding);
        let slot = registry.slot(AccountId(1));
        assert!(!registry.observes());
        assert_eq!(registry.retained_slots(), 1);
        assert_eq!(registry.sharding(), sharding);
        assert_eq!(slot.sharding(), sharding);
        assert!(Arc::ptr_eq(&slot, &registry.slot(AccountId(1))));
        assert!(!Arc::ptr_eq(&slot, &registry.slot(AccountId(2))));
        assert_eq!(registry.retained_slots(), 2);
    }

    #[tokio::test]
    async fn membership_wakes_once_per_publication_and_expires_at_its_exact_deadline() {
        use tollgate_admission::{ArcSwapSnapshotMap, SnapshotMap};
        use tollgate_core::{CostTable, Generation, PermissionBits, ResolvedLimits};
        let (registry, mut changed) = SlotRegistry::observed(LocalSharding::SINGLE);
        let map = ArcSwapSnapshotMap::default();
        let now = Timestamp::from_second(100).unwrap();
        let until = Timestamp::from_second(101).unwrap();
        let principal = Principal(11);
        let snapshot = Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(1),
                AccountStatus::Active,
                until,
                PermissionBits::bit(0),
                ResolvedLimits::new(100),
                Arc::new(CostTable::builder(CostUnits(0), CostUnits(0)).build()),
            )
            .build(),
        );
        map.install(principal, snapshot, registry.slot(AccountId(1)))
            .unwrap();
        registry.observe_many([(principal, map.get(&principal))]);
        assert!(changed.has_changed().unwrap());
        changed.borrow_and_update();
        assert_eq!(registry.drain_changes(now).len(), 1);
        assert!(registry.drain_changes(now).is_empty());
        assert_eq!(registry.next_expiry(), Some(until));
        let expired = registry.drain_changes(until);
        assert_eq!(expired.len(), 1);
        assert!(!expired[0].eligible(until));
        assert_eq!(registry.next_expiry(), None);
        assert!(registry.drain_changes(until).is_empty());
        registry.observe_many([(principal, None)]);
        assert!(changed.has_changed().unwrap());
        assert!(registry.bindings().is_empty());
        assert_eq!(registry.retained_slots(), 1);
    }

    #[test]
    fn readiness_closes_the_lease_window_exactly_when_debits_do() {
        let now = Timestamp::now();
        let slot = LeaseSlot::for_account(AccountId(1));
        assert!(
            !quota_usable(&slot, EnforcementMode::Strict, now),
            "an empty slot funds nothing"
        );

        let lease = lease_expiring_at(now, 100);
        drop(slot.replace(Arc::clone(&lease)));
        assert!(
            lease.try_debit(CostUnits(1), now).is_err(),
            "the request path denies at the boundary",
        );
        assert!(
            !quota_usable(&slot, EnforcementMode::Strict, now),
            "so readiness must not still be advertising at it",
        );

        drop(slot.replace(lease_expiring_at(
            now.checked_add(SignedDuration::from_secs(60)).unwrap(),
            100,
        )));
        assert!(quota_usable(&slot, EnforcementMode::Strict, now));

        drop(slot.replace(lease_expiring_at(
            now.checked_add(SignedDuration::from_secs(60)).unwrap(),
            0,
        )));
        assert!(
            !quota_usable(&slot, EnforcementMode::Strict, now),
            "a live lease with nothing left funds nothing either",
        );
    }

    /// The mode's whole purpose, stated as a readiness property: an elastic
    /// account with headroom keeps its instance in rotation on exactly the
    /// states a strict one is withdrawn for, and leaves rotation when the
    /// headroom is gone (INVARIANTS.md #10).
    #[test]
    fn readiness_counts_overage_headroom_for_an_elastic_account() {
        let now = Timestamp::now();
        let elastic = EnforcementMode::Elastic {
            overage_cap: CostUnits(100),
        };
        let slot = LeaseSlot::for_account(AccountId(1));

        // No lease at all, and a live lease with nothing left: both deny under
        // `Strict`, and both are exactly what elastic mode serves through.
        assert!(!quota_usable(&slot, EnforcementMode::Strict, now));
        assert!(quota_usable(&slot, elastic, now));

        drop(slot.replace(lease_expiring_at(
            now.checked_add(SignedDuration::from_secs(60)).unwrap(),
            0,
        )));
        assert!(!quota_usable(&slot, EnforcementMode::Strict, now));
        assert!(quota_usable(&slot, elastic, now));

        // Spending the cap withdraws the instance, because at that point it
        // really cannot admit anything.
        let overage =
            Reservation::reserve_overage(slot.overage(), CostUnits(100), CostUnits(100)).unwrap();
        overage
            .commit_at_execution_start(now, CommitFunding::LeaseOnly)
            .unwrap();
        assert!(
            !quota_usable(&slot, elastic, now),
            "a spent cap is not admissible, and readiness must say so"
        );

        // A cap raised by a republish restores readiness with no other change.
        assert!(quota_usable(
            &slot,
            EnforcementMode::Elastic {
                overage_cap: CostUnits(200)
            },
            now
        ));
    }
}
