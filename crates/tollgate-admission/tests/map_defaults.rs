//! `SnapshotMap`'s default method bodies are the contract an embedder's own map
//! inherits without writing a line of it. Both maps in this crate override
//! every one of them, so nothing else in the suite executes these bodies.

use std::num::NonZeroUsize;
use std::sync::Arc;

use jiff::Timestamp;
use tollgate_admission::{
    AdmissionCounters, ArcSwapSnapshotMap, LeaseSlot, MapEntry, Principal, PublicationError,
    PublishableSnapshotUpdate, SnapshotMap, SnapshotUpdate,
};
use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CostTable, CostUnits, Generation, OpIndex,
    PermissionBits, ResolvedLimits,
};

#[derive(Clone, Copy)]
struct PriceOp;

impl OpIndex for PriceOp {
    fn index(&self) -> usize {
        0
    }
}

fn t(seconds: i64) -> Timestamp {
    Timestamp::from_second(1_755_600_000 + seconds).unwrap()
}

fn snapshot(generation: u64) -> Arc<AccountSnapshot> {
    Arc::new(
        AccountSnapshot::builder(
            AccountId(1),
            Generation(generation),
            AccountStatus::Active,
            t(86_400),
            PermissionBits::bit(0),
            ResolvedLimits::new(4_096),
            Arc::new(
                CostTable::builder(CostUnits(50), CostUnits(50))
                    .weight(&PriceOp, CostUnits(1))
                    .build(),
            ),
        )
        .build(),
    )
}

fn lease() -> Arc<LeaseSlot> {
    LeaseSlot::for_account(AccountId(1))
}

/// Implements exactly what `SnapshotMap` requires and nothing more, so every
/// call below runs the default body rather than an override.
struct DefaultsMap(ArcSwapSnapshotMap);

impl SnapshotMap for DefaultsMap {
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        self.0.get(principal)
    }

    fn counters(&self) -> &Arc<AdmissionCounters> {
        self.0.counters()
    }

    fn install(
        &self,
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
        lease: Arc<LeaseSlot>,
    ) -> Result<(), PublicationError> {
        self.0.install(principal, snapshot, lease)
    }

    fn install_revoked(
        &self,
        principal: Principal,
        until: Timestamp,
        generation: Generation,
    ) -> Result<(), PublicationError> {
        self.0.install_revoked(principal, until, generation)
    }

    fn install_unknown(
        &self,
        principal: Principal,
        until: Timestamp,
    ) -> Result<(), PublicationError> {
        self.0.install_unknown(principal, until)
    }

    fn remove(&self, principal: &Principal) {
        self.0.remove(principal);
    }
}

fn defaults_map() -> DefaultsMap {
    DefaultsMap(ArcSwapSnapshotMap::new())
}

#[test]
fn the_default_visibility_probe_answers_from_the_request_lookup() {
    let map = defaults_map();
    assert!(!map.contains_cached(&Principal(1)));
    map.install(Principal(1), snapshot(1), lease()).unwrap();
    assert!(map.contains_cached(&Principal(1)));
    map.remove(&Principal(1));
    assert!(!map.contains_cached(&Principal(1)));
}

#[test]
fn a_map_without_reclamation_never_demands_a_refresh_and_retains_everything() {
    let map = defaults_map();
    map.install(Principal(1), snapshot(1), lease()).unwrap();
    // Nothing is ever forgotten, so no push is ever superseded by a read.
    assert!(!map.needs_refresh(Principal(1)));
    assert!(!map.needs_refresh(Principal(2)));
    assert_eq!(map.generation_capacity(), NonZeroUsize::MAX);
    assert!(map.history_stats().is_none());
}

#[test]
fn the_default_batch_writes_dispatch_every_update_variant() {
    for at in [false, true] {
        let map = defaults_map();
        let updates = vec![
            SnapshotUpdate::Present {
                principal: Principal(1),
                snapshot: snapshot(1),
                lease: lease(),
            },
            SnapshotUpdate::Revoked {
                principal: Principal(2),
                until: t(60),
                generation: Generation(3),
            },
            SnapshotUpdate::Unknown {
                principal: Principal(3),
                until: t(60),
            },
        ];
        if at {
            map.apply_many_at(updates, t(0)).unwrap();
        } else {
            map.apply_many(updates).unwrap();
        }
        assert!(matches!(map.get(&Principal(1)), Some(MapEntry::Present(_))));
        assert!(matches!(
            map.get(&Principal(2)),
            Some(MapEntry::NegativeUntil { .. })
        ));
        assert!(matches!(
            map.get(&Principal(3)),
            Some(MapEntry::NegativeUntil { .. })
        ));
    }
}

#[test]
fn the_default_install_many_publishes_the_whole_batch() {
    let map = defaults_map();
    map.install_many(vec![
        (Principal(1), snapshot(1), lease()),
        (Principal(2), snapshot(1), lease()),
    ])
    .unwrap();
    assert!(matches!(map.get(&Principal(1)), Some(MapEntry::Present(_))));
    assert!(matches!(map.get(&Principal(2)), Some(MapEntry::Present(_))));
}

#[test]
fn an_unfenced_reservation_round_trip_publishes_its_reads() {
    let map = defaults_map();
    let batch = map
        .prepare_refreshes(&[Principal(1), Principal(2)])
        .unwrap();
    // A map that reclaims nothing evicts nothing and fences nothing.
    assert!(batch.evicted.is_empty());
    assert_eq!(batch.reads.len(), 2);
    let updates = batch
        .reads
        .into_iter()
        .map(|read| {
            let principal = read.principal();
            read.read(|| PublishableSnapshotUpdate::Present {
                principal,
                snapshot: tollgate_core::PublishableSnapshot::try_new(snapshot(4)).unwrap(),
                lease: lease(),
            })
        })
        .collect();
    map.apply_refreshed_many_at(updates, t(0)).unwrap();
    for principal in [Principal(1), Principal(2)] {
        let Some(MapEntry::Present(state)) = map.get(&principal) else {
            panic!("a refreshed read must reach the map through the default path")
        };
        assert_eq!(state.snapshot.generation, Generation(4));
    }
}
