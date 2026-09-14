//! The two candidate snapshot-map implementations.
//!
//! Both satisfy [`SnapshotMap`]; the perf gate's `admission/snapshot_lookup`
//! benches decide which one a deployment should use. Writes are rare
//! (control-plane pushes), reads are every request — both structures are
//! chosen for that asymmetry.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use jiff::Timestamp;

use tollgate_core::{
    AccountSnapshot, CostUnits, Generation, LocalSharding, Locality, PublishableSnapshot,
};

use crate::counters::AdmissionCounters;
use crate::history::GenerationHistory;
use crate::state::{
    AccountAdmissionState, AdmissionStateRegistry, LeaseSlot, MapEntry, Principal,
    PublishableSnapshotUpdate, ResolvedAdmissionState, SnapshotMap, SnapshotUpdate,
};
use crate::{PublicationError, RefreshBatch, Refreshed};

/// The hasher every `Principal`-keyed map in this crate uses.
///
/// Non-cryptographic on purpose (#9): SipHash's flooding resistance costs
/// 15–20 ns of every request-path lookup and defends against an attack
/// `Principal`'s own contract rules out. The argument is a property of how the
/// key is derived, not of this crate — see [`Principal`]'s documentation,
/// which is where an embedder who might violate it will be reading.
///
/// `fast` is the tier `hashbrown` picks for its own default, and `RandomState`
/// keeps a per-process seed at no per-lookup cost, since the seed lives in the
/// `BuildHasher` held by the map rather than being recomputed. That leaves the
/// unpredictability as free defence in depth.
type PrincipalHasher = foldhash::fast::RandomState;

type PrincipalMap = HashMap<Principal, StoredEntry, PrincipalHasher>;

/// A stored entry is cloned on every moka hit and on every copy-on-write
/// install, so each variant is one refcount bump: the shard array is shared
/// behind an `Arc`, never copied element by element.
#[derive(Clone)]
enum StoredEntry {
    Present(Arc<AccountAdmissionState>),
    ShardedPresent(Arc<[Arc<AccountAdmissionState>]>),
    NegativeUntil { until: Timestamp },
}

fn present_state(
    snapshot: Arc<AccountSnapshot>,
    counters: &Arc<AdmissionCounters>,
    lease: Arc<LeaseSlot>,
    runtime: ResolvedAdmissionState,
    sharding: LocalSharding,
) -> StoredEntry {
    if sharding == LocalSharding::SINGLE {
        return StoredEntry::Present(AccountAdmissionState::new(
            snapshot,
            Arc::clone(counters),
            lease,
            runtime.limiter,
            runtime.principal_gauge,
        ));
    }
    let states: Vec<_> = (0..sharding.get())
        .map(|_| {
            AccountAdmissionState::new(
                Arc::new((*snapshot).clone()),
                Arc::clone(counters),
                Arc::clone(&lease),
                Arc::clone(&runtime.limiter),
                Arc::clone(&runtime.principal_gauge),
            )
        })
        .collect();
    StoredEntry::ShardedPresent(states.into())
}

/// Resolve an owned entry, which is what a cache that hands back a clone
/// gives us. Moving the single-state `Arc` out matters: re-cloning it and
/// dropping the temporary would put two refcount operations on the shared
/// state back onto every request-path lookup.
fn request_entry_from(entry: StoredEntry, sharding: LocalSharding, locality: Locality) -> MapEntry {
    match entry {
        StoredEntry::Present(state) => MapEntry::Present(state),
        StoredEntry::ShardedPresent(states) => {
            MapEntry::Present(Arc::clone(&states[locality.index(sharding)]))
        }
        StoredEntry::NegativeUntil { until } => MapEntry::NegativeUntil { until },
    }
}

/// The same resolution for a borrowed entry, which is what a map read under a
/// guard gives us. Kept separate from [`request_entry_from`] rather than
/// cloning into it: the clone is the very thing that function exists to avoid.
fn request_entry_at(entry: &StoredEntry, sharding: LocalSharding, locality: Locality) -> MapEntry {
    match entry {
        StoredEntry::Present(state) => MapEntry::Present(Arc::clone(state)),
        StoredEntry::ShardedPresent(states) => {
            MapEntry::Present(Arc::clone(&states[locality.index(sharding)]))
        }
        StoredEntry::NegativeUntil { until } => MapEntry::NegativeUntil { until: *until },
    }
}

enum PreparedUpdate {
    Present {
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
        lease: Arc<LeaseSlot>,
        runtime: ResolvedAdmissionState,
    },
    Revoked {
        principal: Principal,
        until: Timestamp,
    },
    Unknown {
        principal: Principal,
        until: Timestamp,
    },
}

enum InstallUpdate {
    Present {
        principal: Principal,
        snapshot: Arc<AccountSnapshot>,
        maximum_quote: Option<CostUnits>,
        lease: Arc<LeaseSlot>,
    },
    Revoked {
        principal: Principal,
        until: Timestamp,
        generation: Generation,
    },
    Unknown {
        principal: Principal,
        until: Timestamp,
    },
}

fn apply_prepared(
    map: &mut PrincipalMap,
    updates: &[PreparedUpdate],
    sharding: LocalSharding,
    counters: &Arc<AdmissionCounters>,
) {
    for update in updates {
        match update {
            PreparedUpdate::Present {
                principal,
                snapshot,
                lease,
                runtime,
            } => {
                map.insert(
                    *principal,
                    present_state(
                        Arc::clone(snapshot),
                        counters,
                        Arc::clone(lease),
                        runtime.clone(),
                        sharding,
                    ),
                );
            }
            PreparedUpdate::Revoked { principal, until } => {
                map.insert(*principal, StoredEntry::NegativeUntil { until: *until });
            }
            // No watermark call: an absence neither consults nor changes it.
            // There is deliberately no `accept_unknown` here to mirror the
            // other two arms -- a call that always returns true only looks
            // like a decision, and the mutation gate says so by surviving its
            // removal.
            PreparedUpdate::Unknown { principal, until } => {
                map.insert(*principal, StoredEntry::NegativeUntil { until: *until });
            }
        }
    }
}

/// Decide every generation transition before resolving runtime state.
///
/// The returned updates are exactly the final accepted mutation for each
/// principal in this atomic batch. Earlier accepted entries overwritten later
/// in the same batch never become visible, so they must not become account
/// policy authorities either.
fn accept_updates(
    map: &PrincipalMap,
    watermarks: &mut GenerationHistory,
    updates: impl IntoIterator<Item = InstallUpdate>,
) -> Vec<InstallUpdate> {
    let mut visible = HashMap::with_hasher(PrincipalHasher::default());
    let mut accepted: Vec<Option<InstallUpdate>> = Vec::new();
    let mut last = HashMap::with_hasher(PrincipalHasher::default());

    for update in updates {
        let principal = match &update {
            InstallUpdate::Present { principal, .. }
            | InstallUpdate::Revoked { principal, .. }
            | InstallUpdate::Unknown { principal, .. } => *principal,
        };
        let was_visible = visible.get(&principal).copied().unwrap_or_else(|| {
            matches!(
                map.get(&principal),
                Some(StoredEntry::Present(_) | StoredEntry::ShardedPresent(_))
            )
        });
        let is_accepted = match &update {
            InstallUpdate::Present { snapshot, .. } => {
                watermarks.accept_positive(principal, snapshot.generation, was_visible)
            }
            InstallUpdate::Revoked { generation, .. } => {
                watermarks.accept_revoked(principal, *generation)
            }
            InstallUpdate::Unknown { .. } => true,
        };
        if !is_accepted {
            continue;
        }

        let becomes_visible = matches!(update, InstallUpdate::Present { .. });
        visible.insert(principal, becomes_visible);
        if let Some(previous) = last.insert(principal, accepted.len()) {
            accepted[previous] = None;
        }
        accepted.push(Some(update));
    }

    accepted.into_iter().flatten().collect()
}

fn raw_updates(updates: Vec<SnapshotUpdate>) -> Vec<InstallUpdate> {
    updates
        .into_iter()
        .map(|update| match update {
            SnapshotUpdate::Present {
                principal,
                snapshot,
                lease,
            } => InstallUpdate::Present {
                principal,
                snapshot,
                maximum_quote: None,
                lease,
            },
            SnapshotUpdate::Revoked {
                principal,
                until,
                generation,
            } => InstallUpdate::Revoked {
                principal,
                until,
                generation,
            },
            SnapshotUpdate::Unknown { principal, until } => {
                InstallUpdate::Unknown { principal, until }
            }
        })
        .collect()
}
fn raw_publishable_updates(updates: Vec<PublishableSnapshotUpdate>) -> Vec<InstallUpdate> {
    updates
        .into_iter()
        .map(|update| match update {
            PublishableSnapshotUpdate::Present {
                principal,
                snapshot,
                lease,
            } => InstallUpdate::Present {
                principal,
                maximum_quote: snapshot.maximum_quote(),
                snapshot: snapshot.into_inner(),
                lease,
            },
            PublishableSnapshotUpdate::Revoked {
                principal,
                until,
                generation,
            } => InstallUpdate::Revoked {
                principal,
                until,
                generation,
            },
            PublishableSnapshotUpdate::Unknown { principal, until } => {
                InstallUpdate::Unknown { principal, until }
            }
        })
        .collect()
}

/// Resolve only accepted, request-visible positives as one registry batch.
/// Generation rejection has already happened while the watermark lock is
/// held, so no rejected snapshot can mutate account policy.
fn prepare_accepted(
    states: &AdmissionStateRegistry,
    updates: impl IntoIterator<Item = InstallUpdate>,
) -> Vec<PreparedUpdate> {
    // Negatives need no limiter, so they skip the registry entirely and
    // keep their place by index rather than by being carried through it.
    let updates = updates.into_iter();
    let mut prepared: Vec<Option<PreparedUpdate>> = Vec::with_capacity(updates.size_hint().0);
    let mut positives = Vec::new();
    for update in updates {
        match update {
            InstallUpdate::Present {
                principal,
                snapshot,
                maximum_quote,
                lease,
            } => {
                prepared.push(None);
                positives.push((
                    prepared.len() - 1,
                    principal,
                    snapshot,
                    maximum_quote,
                    lease,
                ));
            }
            InstallUpdate::Revoked {
                principal,
                until,
                generation: _,
            } => prepared.push(Some(PreparedUpdate::Revoked { principal, until })),
            InstallUpdate::Unknown { principal, until } => {
                prepared.push(Some(PreparedUpdate::Unknown { principal, until }));
            }
        }
    }

    let resolved = states.states_for(
        positives,
        |(_, principal, snapshot, _, _)| (*principal, snapshot.account_id, snapshot.generation),
        |(_, _, snapshot, _, _)| snapshot.limits,
        |(_, _, _, maximum_quote, _)| *maximum_quote,
    );
    for ((slot, principal, snapshot, _, lease), runtime) in resolved {
        prepared[slot] = Some(PreparedUpdate::Present {
            principal,
            snapshot,
            lease,
            runtime,
        });
    }

    prepared
        .into_iter()
        .map(|update| update.expect("every slot is filled by exactly one branch above"))
        .collect()
}

enum InstallBatch {
    Push(Vec<InstallUpdate>),
    Refreshed(Vec<Refreshed<PublishableSnapshotUpdate>>),
}

impl InstallBatch {
    fn prepare(
        self,
        history: &mut GenerationHistory,
    ) -> Result<Vec<InstallUpdate>, PublicationError> {
        match self {
            Self::Push(updates) => {
                history.check_pushes(updates.iter().filter_map(|update| match update {
                    InstallUpdate::Present { principal, .. }
                    | InstallUpdate::Revoked { principal, .. } => Some(*principal),
                    InstallUpdate::Unknown { .. } => None,
                }))?;
                Ok(updates)
            }
            Self::Refreshed(updates) => {
                for update in &updates {
                    history.check_refresh(update, update.value().principal())?;
                }
                for update in &updates {
                    history.complete_refresh(
                        update.principal(),
                        !matches!(update.value(), PublishableSnapshotUpdate::Unknown { .. }),
                    );
                }
                Ok(raw_publishable_updates(
                    updates.into_iter().map(Refreshed::into_value).collect(),
                ))
            }
        }
    }
}

// Both map backends route every publication variant through the same guarded
// batch boundary; adding an overload cannot accidentally bypass the fence.
macro_rules! publication_methods {
    () => {
        fn history_stats(&self) -> Option<crate::SnapshotHistoryStats> {
            Some(self.watermarks.lock().expect("watermarks poisoned").stats())
        }
        fn generation_capacity(&self) -> std::num::NonZeroUsize {
            self.watermarks
                .lock()
                .expect("watermarks poisoned")
                .capacity()
        }
        fn needs_refresh(&self, principal: Principal) -> bool {
            self.watermarks
                .lock()
                .expect("watermarks poisoned")
                .needs_refresh(principal)
        }
        fn install(
            &self,
            principal: Principal,
            snapshot: Arc<AccountSnapshot>,
            lease: Arc<LeaseSlot>,
        ) -> Result<(), PublicationError> {
            self.apply_many(vec![SnapshotUpdate::Present {
                principal,
                snapshot,
                lease,
            }])
        }
        fn install_publishable(
            &self,
            principal: Principal,
            snapshot: PublishableSnapshot,
            lease: Arc<LeaseSlot>,
        ) -> Result<(), PublicationError> {
            self.apply_publishable_many(vec![PublishableSnapshotUpdate::Present {
                principal,
                snapshot,
                lease,
            }])
        }
        fn install_revoked(
            &self,
            principal: Principal,
            until: Timestamp,
            generation: Generation,
        ) -> Result<(), PublicationError> {
            self.apply_many(vec![SnapshotUpdate::Revoked {
                principal,
                until,
                generation,
            }])
        }
        fn install_unknown(
            &self,
            principal: Principal,
            until: Timestamp,
        ) -> Result<(), PublicationError> {
            self.apply_many(vec![SnapshotUpdate::Unknown { principal, until }])
        }
        fn apply_many(&self, updates: Vec<SnapshotUpdate>) -> Result<(), PublicationError> {
            self.write(InstallBatch::Push(raw_updates(updates)), None)
        }
        fn apply_many_at(
            &self,
            updates: Vec<SnapshotUpdate>,
            now: Timestamp,
        ) -> Result<(), PublicationError> {
            self.write(InstallBatch::Push(raw_updates(updates)), Some(now))
        }
        fn apply_publishable_many(
            &self,
            updates: Vec<PublishableSnapshotUpdate>,
        ) -> Result<(), PublicationError> {
            self.write(InstallBatch::Push(raw_publishable_updates(updates)), None)
        }
        fn apply_publishable_many_at(
            &self,
            updates: Vec<PublishableSnapshotUpdate>,
            now: Timestamp,
        ) -> Result<(), PublicationError> {
            self.write(
                InstallBatch::Push(raw_publishable_updates(updates)),
                Some(now),
            )
        }
        fn apply_refreshed_many_at(
            &self,
            updates: Vec<Refreshed<PublishableSnapshotUpdate>>,
            now: Timestamp,
        ) -> Result<(), PublicationError> {
            self.write(InstallBatch::Refreshed(updates), Some(now))
        }
    };
}

/// `moka`-backed bounded cache. Values are `Arc`-cheap by construction (the
/// review's caution about moka cloning values on retrieval) — and moka clones
/// on its *write* path too, so a stored value must not resolve anything
/// request-local in `Clone`: it would be resolved against the installing
/// thread before any request saw it. Locality is applied in `get_at`.
///
/// **This map does maintenance on the request path, and that is the price of
/// being bounded.** A `get` records a read op; when the read log reaches
/// `READ_LOG_FLUSH_POINT` (64) or a 300 ms monotonic deadline passes, moka's
/// housekeeper drains it — behind a `try_lock` on a `parking_lot::Mutex`,
/// updating the TinyLFU frequency sketch, and evicting when the cache is at
/// capacity (moka 0.12.16). The `try_lock` never blocks: a request that finds
/// the housekeeper busy skips the drain and returns. So this is bounded,
/// amortized, non-blocking work on the hot path rather than a stall — but it
/// is not nothing, and the crate's budget counts it rather than claiming the
/// request path takes no lock at all.
///
/// Two consequences worth knowing before choosing this map over
/// [`ArcSwapSnapshotMap`], whose reads take no lock and evict nothing:
///
/// * Below half of `max_capacity`, moka does not enable the frequency sketch
///   at all, so a cache that is never filled pays neither the sketch nor
///   eviction. The `admission/snapshot_lookup_moka` bench row measures that
///   under-filled state; `admission/snapshot_lookup_moka_at_capacity`
///   measures a full one. Both run; the second is reserved in the threshold
///   manifest rather than gated by it, because its bound has to be recorded
///   on the controlled host before it is a contract.
/// * A bounded map can evict an entry the control plane still believes is
///   published. That is not a defect — it is what `SnapshotMap::needs_refresh`
///   and the generation history below exist to repair.
pub struct MokaSnapshotMap {
    cache: moka::sync::Cache<Principal, StoredEntry, PrincipalHasher>,
    watermarks: Mutex<GenerationHistory>,
    states: AdmissionStateRegistry,
    sharding: LocalSharding,
    counters: Arc<AdmissionCounters>,
}

impl MokaSnapshotMap {
    #[must_use]
    pub fn new(max_capacity: u64) -> Self {
        Self::with_sharding(max_capacity, LocalSharding::SINGLE)
    }

    #[must_use]
    pub fn with_sharding(max_capacity: u64, sharding: LocalSharding) -> Self {
        let history = NonZeroUsize::new(usize::try_from(max_capacity).unwrap_or(usize::MAX).max(1))
            .expect("capacity is positive");
        Self::with_capacities(max_capacity, sharding, history)
    }

    /// Configure visible and retained-history capacities independently.
    #[must_use]
    pub fn with_capacities(
        max_capacity: u64,
        sharding: LocalSharding,
        history: NonZeroUsize,
    ) -> Self {
        MokaSnapshotMap {
            // Same hasher as the arc-swap map, and for the same reason. It
            // also keeps the two `snapshot_lookup` benches comparing like with
            // like: the ~3x gap between them is the evidence for which map a
            // deployment should pick, and it would stop measuring TinyLFU
            // bookkeeping the moment one side hashed differently.
            cache: moka::sync::Cache::builder()
                .max_capacity(max_capacity)
                .build_with_hasher(PrincipalHasher::default()),
            watermarks: Mutex::new(GenerationHistory::new(history)),
            states: AdmissionStateRegistry::new(sharding),
            sharding,
            counters: Arc::new(AdmissionCounters::with_sharding(sharding)),
        }
    }
    fn write(&self, batch: InstallBatch, _now: Option<Timestamp>) -> Result<(), PublicationError> {
        let mut history = self.watermarks.lock().expect("watermarks poisoned");
        let updates = batch.prepare(&mut history)?;
        let mut visible = PrincipalMap::default();
        for update in &updates {
            let principal = match update {
                InstallUpdate::Present { principal, .. }
                | InstallUpdate::Revoked { principal, .. }
                | InstallUpdate::Unknown { principal, .. } => *principal,
            };
            if let Some(entry) = self.cache.get(&principal) {
                visible.insert(principal, entry);
            }
        }
        let accepted = accept_updates(&visible, &mut history, updates);
        let prepared = prepare_accepted(&self.states, accepted);
        // Only final accepted mutations are inserted; a refused update must
        // neither refresh cache frequency nor become account policy authority.
        let mut changed = PrincipalMap::default();
        apply_prepared(&mut changed, &prepared, self.sharding, &self.counters);
        for (principal, entry) in changed {
            self.cache.insert(principal, entry);
        }
        Ok(())
    }
}

impl SnapshotMap for MokaSnapshotMap {
    fn contains_cached(&self, principal: &Principal) -> bool {
        self.cache.contains_key(principal)
    }
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        self.get_at(principal, Locality::current())
    }

    fn get_at(&self, principal: &Principal, locality: Locality) -> Option<MapEntry> {
        self.cache
            .get(principal)
            .map(|entry| request_entry_from(entry, self.sharding, locality))
    }

    fn local_sharding(&self) -> LocalSharding {
        self.sharding
    }

    fn counters(&self) -> &Arc<AdmissionCounters> {
        &self.counters
    }

    publication_methods!();

    fn prepare_refreshes(
        &self,
        principals: &[Principal],
    ) -> Result<RefreshBatch, PublicationError> {
        let mut history = self.watermarks.lock().expect("watermarks poisoned");
        let batch = history.prepare(principals)?;
        for principal in &batch.evicted {
            self.cache.invalidate(principal);
        }
        Ok(batch)
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
    map: ArcSwap<PrincipalMap>,
    watermarks: Mutex<GenerationHistory>,
    states: AdmissionStateRegistry,
    max_negative_entries: usize,
    sharding: LocalSharding,
    counters: Arc<AdmissionCounters>,
}

impl ArcSwapSnapshotMap {
    pub const DEFAULT_MAX_NEGATIVE_ENTRIES: usize = 4_096;
    /// Covers the configured simultaneously served set, including refreshes.
    pub const DEFAULT_GENERATION_CAPACITY: NonZeroUsize =
        NonZeroUsize::new(65_536).expect("positive constant");

    #[must_use]
    pub fn new() -> Self {
        Self::with_sharding(LocalSharding::SINGLE)
    }

    #[must_use]
    pub fn with_sharding(sharding: LocalSharding) -> Self {
        Self::with_sharding_and_max_negative_entries(sharding, Self::DEFAULT_MAX_NEGATIVE_ENTRIES)
    }

    #[must_use]
    pub fn with_max_negative_entries(max_negative_entries: usize) -> Self {
        Self::with_sharding_and_max_negative_entries(LocalSharding::SINGLE, max_negative_entries)
    }

    #[must_use]
    pub fn with_sharding_and_max_negative_entries(
        sharding: LocalSharding,
        max_negative_entries: usize,
    ) -> Self {
        Self::with_capacities(
            sharding,
            max_negative_entries,
            Self::DEFAULT_GENERATION_CAPACITY,
        )
    }

    #[must_use]
    pub fn with_capacities(
        sharding: LocalSharding,
        max_negative_entries: usize,
        history: NonZeroUsize,
    ) -> Self {
        ArcSwapSnapshotMap {
            map: ArcSwap::default(),
            watermarks: Mutex::new(GenerationHistory::new(history)),
            states: AdmissionStateRegistry::new(sharding),
            max_negative_entries,
            sharding,
            counters: Arc::new(AdmissionCounters::with_sharding(sharding)),
        }
    }

    /// Writers serialize only against other control-plane writers. Request
    /// reads remain one ArcSwap load and one hash lookup.
    fn write(&self, batch: InstallBatch, now: Option<Timestamp>) -> Result<(), PublicationError> {
        let mut watermarks = self.watermarks.lock().expect("watermarks poisoned");
        let updates = batch.prepare(&mut watermarks)?;
        let current = self.map.load_full();
        let mut next = PrincipalMap::clone(&current);
        if let Some(now) = now {
            next.retain(
                |_, entry| !matches!(entry, StoredEntry::NegativeUntil { until } if *until <= now),
            );
        }
        let accepted = accept_updates(&next, &mut watermarks, updates);
        let prepared = prepare_accepted(&self.states, accepted);
        apply_prepared(&mut next, &prepared, self.sharding, &self.counters);
        trim_negatives(&mut next, self.max_negative_entries);
        self.map.store(Arc::new(next));
        Ok(())
    }
}

impl Default for ArcSwapSnapshotMap {
    fn default() -> Self {
        Self::new()
    }
}

fn trim_negatives(map: &mut PrincipalMap, max_negative_entries: usize) {
    let mut negatives: Vec<_> = map
        .iter()
        .filter_map(|(principal, entry)| match entry {
            StoredEntry::NegativeUntil { until } => Some((*until, *principal)),
            StoredEntry::Present(_) | StoredEntry::ShardedPresent(_) => None,
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
    fn contains_cached(&self, principal: &Principal) -> bool {
        self.map.load().contains_key(principal)
    }
    fn get(&self, principal: &Principal) -> Option<MapEntry> {
        self.get_at(principal, Locality::current())
    }

    fn get_at(&self, principal: &Principal, locality: Locality) -> Option<MapEntry> {
        self.map
            .load()
            .get(principal)
            .map(|entry| request_entry_at(entry, self.sharding, locality))
    }

    fn local_sharding(&self) -> LocalSharding {
        self.sharding
    }

    fn counters(&self) -> &Arc<AdmissionCounters> {
        &self.counters
    }

    publication_methods!();

    fn prepare_refreshes(
        &self,
        principals: &[Principal],
    ) -> Result<RefreshBatch, PublicationError> {
        let mut history = self.watermarks.lock().expect("watermarks poisoned");
        let batch = history.prepare(principals)?;
        if !batch.evicted.is_empty() {
            let mut next = PrincipalMap::clone(&self.map.load_full());
            for principal in &batch.evicted {
                next.remove(principal);
            }
            self.map.store(Arc::new(next));
        }
        Ok(batch)
    }

    fn remove(&self, principal: &Principal) {
        self.remove_many(std::slice::from_ref(principal));
    }

    fn remove_many(&self, principals: &[Principal]) {
        if principals.is_empty() {
            return;
        }
        let _watermarks = self.watermarks.lock().expect("watermarks poisoned");
        let current = self.map.load_full();
        let mut next = PrincipalMap::clone(&current);
        for principal in principals {
            next.remove(principal);
        }
        self.map.store(Arc::new(next));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::BuildHasher;
    use std::num::{NonZeroU32, NonZeroUsize};

    use tollgate_core::{
        AccountId, AccountStatus, CostTable, CostUnits, DenyReason, OpIndex, PermissionBits,
        ResolvedLimits,
    };

    struct PricedOp;

    impl OpIndex for PricedOp {
        fn index(&self) -> usize {
            0
        }
    }

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    fn hash_of<S: BuildHasher>(hasher: &S, principal: Principal) -> u64 {
        hasher.hash_one(principal)
    }

    /// Dropping SipHash is only sound if what replaces it still *spreads*, and
    /// the two halves of a hash are consumed differently: hashbrown takes the
    /// bucket index from the low bits and the control byte from the top seven.
    /// A hasher can look fine on one and be degenerate on the other.
    ///
    /// This is what rules out the identity fold #9 floats as an alternative.
    /// Truncated-HMAC principals carry entropy everywhere, so identity would
    /// pass on them — but sequential principals, which this crate's own tests
    /// and any integer-id embedder produce, would leave the top seven bits
    /// constant and collapse every control byte onto one value. Both input
    /// shapes are therefore checked, and the top bits are checked separately
    /// rather than trusted to follow from the low ones.
    #[test]
    fn principal_hashing_stays_spread_for_sequential_and_random_keys() {
        const KEYS: usize = 4_096;
        const BUCKETS: usize = 256;
        // Independent nonzero constants make the corpus repeatable without
        // manufacturing correlated weak points in foldhash's two seed layers.
        const PER_HASHER_SEEDS: [u64; 4] = [
            0x4D59_5DF4_D0F3_3173,
            0xD7A0_0D5E_A50D_0C75,
            0x8B8B_8B8B_8B8B_8B8B,
            0x6A09_E667_F3BC_C909,
        ];
        static SHARED_SEEDS: [foldhash::SharedSeed; 4] = [
            foldhash::SharedSeed::from_u64(0xBB67_AE85_84CA_A73B),
            foldhash::SharedSeed::from_u64(0x3C6E_F372_FE94_F82B),
            foldhash::SharedSeed::from_u64(0xA54F_F53A_5F1D_36F1),
            foldhash::SharedSeed::from_u64(0x510E_527F_ADE6_82D1),
        ];
        // Uniform over 256 buckets is 16 per bucket. This deliberately wide
        // regression bound distinguishes a spread hash from a collapse
        // without treating fast foldhash as a statistical-quality hash.
        const MAX_PER_BUCKET: usize = 64;

        // Keep the behavioral witness deterministic while mechanically tying
        // the production alias to foldhash's randomized fast state. Production
        // maps must retain their per-instance unpredictability; only this test
        // substitutes explicit seeds.
        let _: PrincipalHasher = foldhash::fast::RandomState::default();

        // A xorshift stand-in for truncated-HMAC principals: uniformly spread
        // bits, generated without a dependency or a clock read.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut random = std::iter::repeat_with(move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            u128::from(state) << 64 | u128::from(state.rotate_left(31))
        });

        let input_shapes = [
            (
                "sequential",
                (0..KEYS as u128).map(Principal).collect::<Vec<_>>(),
            ),
            (
                "hmac-like",
                (0..KEYS)
                    .map(|_| Principal(random.next().unwrap()))
                    .collect(),
            ),
        ];

        for (shared_seed_index, shared_seed) in SHARED_SEEDS.iter().enumerate() {
            for per_hasher_seed in PER_HASHER_SEEDS {
                let hasher =
                    foldhash::fast::SeedableRandomState::with_seed(per_hasher_seed, shared_seed);

                for (shape, principals) in &input_shapes {
                    let mut by_low = vec![0usize; BUCKETS];
                    let mut by_top = vec![0usize; 128];
                    for principal in principals {
                        let hash = hash_of(&hasher, *principal);
                        by_low[(hash as usize) % BUCKETS] += 1;
                        by_top[(hash >> 57) as usize] += 1;
                    }

                    let worst_low = by_low.iter().copied().max().unwrap_or(0);
                    assert!(
                        worst_low <= MAX_PER_BUCKET,
                        "{shape}, shared seed {shared_seed_index}, per-hasher seed \
                         {per_hasher_seed}: {worst_low} of {KEYS} keys landed in one of \
                         {BUCKETS} bucket-index slots; the hash does not spread its low bits",
                    );
                    let worst_top = by_top.iter().copied().max().unwrap_or(0);
                    assert!(
                        worst_top <= MAX_PER_BUCKET,
                        "{shape}, shared seed {shared_seed_index}, per-hasher seed \
                         {per_hasher_seed}: {worst_top} of {KEYS} keys share one control byte; \
                         the hash does not spread its top bits, so probing degrades however \
                         well the bucket index looks",
                    );
                    assert!(
                        by_top.iter().filter(|count| **count > 0).count() > 64,
                        "{shape}, shared seed {shared_seed_index}, per-hasher seed \
                         {per_hasher_seed}: the top seven bits took too few distinct values",
                    );
                }
            }
        }
    }

    fn snapshot(generation: u64) -> Arc<AccountSnapshot> {
        Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(generation),
                AccountStatus::Active,
                t(10_000),
                PermissionBits::ALL,
                ResolvedLimits::new(100).with_weighted_rate(1_000, 5_000),
                Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
            )
            .build(),
        )
    }

    #[derive(Default)]
    struct DefaultMethodsMap {
        installs: std::sync::atomic::AtomicUsize,
        negatives: std::sync::atomic::AtomicUsize,
        counters: Arc<AdmissionCounters>,
    }

    impl SnapshotMap for DefaultMethodsMap {
        fn get(&self, _principal: &Principal) -> Option<MapEntry> {
            Some(MapEntry::NegativeUntil { until: t(1) })
        }

        fn counters(&self) -> &Arc<AdmissionCounters> {
            &self.counters
        }

        fn install(
            &self,
            _principal: Principal,
            _snapshot: Arc<AccountSnapshot>,
            _lease: Arc<LeaseSlot>,
        ) -> Result<(), PublicationError> {
            self.installs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        fn install_revoked(
            &self,
            _principal: Principal,
            _until: Timestamp,
            _generation: Generation,
        ) -> Result<(), PublicationError> {
            self.negatives
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        fn install_unknown(
            &self,
            _principal: Principal,
            _until: Timestamp,
        ) -> Result<(), PublicationError> {
            self.negatives
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        fn remove(&self, _principal: &Principal) {}
    }

    #[test]
    fn snapshot_map_compatibility_defaults_preserve_single_shard_behavior() {
        let map = DefaultMethodsMap::default();
        assert!(matches!(
            map.get_at(&Principal(1), Locality::current()),
            Some(MapEntry::NegativeUntil { .. })
        ));
        assert_eq!(map.local_sharding(), LocalSharding::SINGLE);

        let publishable = PublishableSnapshot::try_new(snapshot(1)).unwrap();
        map.install_publishable(
            Principal(1),
            publishable.clone(),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        map.apply_publishable_many(vec![
            PublishableSnapshotUpdate::Present {
                principal: Principal(2),
                snapshot: publishable.clone(),
                lease: LeaseSlot::for_account(AccountId(1)),
            },
            PublishableSnapshotUpdate::Unknown {
                principal: Principal(3),
                until: t(2),
            },
        ])
        .unwrap();
        map.apply_publishable_many_at(
            vec![PublishableSnapshotUpdate::Present {
                principal: Principal(4),
                snapshot: publishable,
                lease: LeaseSlot::for_account(AccountId(1)),
            }],
            t(3),
        )
        .unwrap();
        assert_eq!(map.installs.load(std::sync::atomic::Ordering::Relaxed), 3);
        assert_eq!(map.negatives.load(std::sync::atomic::Ordering::Relaxed), 1);
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
        map.install(p, snapshot(5), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert_eq!(generation_of(&map, &p), Some(5));
        map.install(p, snapshot(3), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert_eq!(generation_of(&map, &p), Some(5));
        // Same generation is also a no-op (idempotent replay) -- and it must
        // be observed as *the same entry*, not merely the same generation.
        // Since #53 the equal-generation rule turns on `visible`, and a
        // regression that dropped it would still leave generation 5 here while
        // rebuilding the entry, which on this map is a clone of the whole map
        // per republish. Identity is what makes that detectable.
        let before = match map.get(&p) {
            Some(MapEntry::Present(state)) => state,
            other => panic!("expected a present entry, got {other:?}"),
        };
        map.install(p, snapshot(5), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert_eq!(generation_of(&map, &p), Some(5));
        let after = match map.get(&p) {
            Some(MapEntry::Present(state)) => state,
            other => panic!("expected a present entry, got {other:?}"),
        };
        assert!(
            Arc::ptr_eq(&before, &after),
            "a duplicate publish of a visible snapshot must not reinstall it"
        );
        // The accessor must hand back the *same* counters the states it
        // installs record through. Stage two tallies against `state.counters`
        // while the observability surface reads `map.counters()`; two
        // instances would leave an engine reporting zeros while work flowed
        // (INVARIANTS.md #20). Asserted behaviourally, so a fresh default
        // standing in for the accessor cannot satisfy it.
        let slot = DenyReason::UnknownPrincipal.index();
        let before = map.counters().snapshot().denials[slot];
        after.counters.record_deny_at(
            &DenyReason::UnknownPrincipal,
            tollgate_core::Locality::current(),
        );
        assert_eq!(
            map.counters().snapshot().denials[slot],
            before + 1,
            "the map's counters must be the instance its installed states record through"
        );

        // Newer generation replaces.
        map.install(p, snapshot(6), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert_eq!(generation_of(&map, &p), Some(6));

        // An absent row denies, and a *delayed older* push still cannot roll
        // the account back behind what this instance last saw.
        //
        // This used to assert that a generation-1 push "cannot resurrect the
        // revoked principal" after an unversioned negative — but nothing had
        // revoked it. The assertion passed on the positive's own watermark
        // being treated as a tombstone, which is exactly the conflation #53
        // removed. Ordering is the real property here; revocation is asserted
        // below, against an actual revocation.
        map.install_unknown(p, t(100)).unwrap();
        assert!(matches!(map.get(&p), Some(MapEntry::NegativeUntil { .. })));
        map.install(p, snapshot(1), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert!(matches!(map.get(&p), Some(MapEntry::NegativeUntil { .. })));

        // And the principal returns at the generation it already had: an
        // absence is not a statement that generation 6 is dead (#53).
        map.install(p, snapshot(6), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert_eq!(generation_of(&map, &p), Some(6));

        map.install(p, snapshot(7), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert_eq!(generation_of(&map, &p), Some(7));

        // The symmetric reorder is safe too: an old revocation cannot revoke a
        // snapshot that has already advanced beyond it.
        map.install_revoked(p, t(200), Generation(6)).unwrap();
        assert_eq!(generation_of(&map, &p), Some(7));
        map.install_revoked(p, t(200), Generation(7)).unwrap();
        assert!(matches!(map.get(&p), Some(MapEntry::NegativeUntil { .. })));

        // A real revocation *does* refuse its own generation back -- the half
        // that must not loosen (INVARIANTS.md #15).
        map.install(p, snapshot(7), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert!(
            matches!(map.get(&p), Some(MapEntry::NegativeUntil { .. })),
            "a replay at the tombstone's generation stays dead"
        );

        // Removing the request-visible entry must not remove the generation
        // watermark. Otherwise cache eviction could resurrect stale state.
        // The watermark here is a *revocation* at 7, so 7 stays refused.
        map.remove(&p);
        assert!(map.get(&p).is_none());
        map.install(p, snapshot(7), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert!(map.get(&p).is_none());
        map.install(p, snapshot(8), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert_eq!(generation_of(&map, &p), Some(8));

        // The other half of `remove`, which nothing pinned before #53 and
        // which the change above alters: with the watermark left by a
        // *positive* (8, from the install just above), re-installing the
        // evicted generation repairs the entry. That is what lets a bounded
        // map recover from capacity pressure instead of denying a principal
        // until someone publishes a higher generation.
        map.remove(&p);
        assert!(map.get(&p).is_none());
        map.install(p, snapshot(8), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert_eq!(
            generation_of(&map, &p),
            Some(8),
            "an evicted entry is repaired by re-fetching the generation it had"
        );
        // Ordering still holds across the eviction: older is still refused.
        map.install(p, snapshot(7), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert_eq!(generation_of(&map, &p), Some(8));
    }

    #[test]
    fn install_many_respects_generations_in_one_write() {
        let map = ArcSwapSnapshotMap::new();
        map.install(
            Principal(1),
            snapshot(5),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        map.install_many(vec![
            (
                Principal(1),
                snapshot(3),
                LeaseSlot::for_account(AccountId(1)),
            ), // rollback: discarded
            (
                Principal(2),
                snapshot(1),
                LeaseSlot::for_account(AccountId(1)),
            ),
            (
                Principal(3),
                snapshot(1),
                LeaseSlot::for_account(AccountId(1)),
            ),
        ])
        .unwrap();
        assert_eq!(generation_of(&map, &Principal(1)), Some(5));
        assert_eq!(generation_of(&map, &Principal(2)), Some(1));
        assert_eq!(generation_of(&map, &Principal(3)), Some(1));
    }

    #[test]
    fn moka_map_contract() {
        exercises_map(MokaSnapshotMap::new(1_000));
    }

    #[test]
    fn moka_publishable_install_preserves_configured_sharding() {
        let sharding = LocalSharding::new(NonZeroUsize::new(2).unwrap());
        let map = MokaSnapshotMap::with_sharding(10, sharding);
        map.install_publishable(
            Principal(1),
            PublishableSnapshot::try_new(snapshot(1)).unwrap(),
            LeaseSlot::with_sharding(AccountId(1), sharding),
        )
        .unwrap();

        assert_eq!(map.local_sharding(), sharding);
        assert!(matches!(map.get(&Principal(1)), Some(MapEntry::Present(_))));
    }

    /// Sharding the moka map is only real if lookups actually reach
    /// different states. They did not: moka clones a value on its *write*
    /// path, so an entry that resolved locality in `Clone` was already
    /// resolved — to the installing thread's shard — before any request saw
    /// it, and every worker on every locality then shared that one state.
    /// The entry now stays a shard array until `get_at` indexes it.
    #[test]
    fn moka_lookup_at_a_locality_selects_that_locality_own_state() {
        let sharding = LocalSharding::new(NonZeroUsize::new(2).unwrap());
        let map = Arc::new(MokaSnapshotMap::with_sharding(10, sharding));
        map.install_publishable(
            Principal(1),
            PublishableSnapshot::try_new(snapshot(1)).unwrap(),
            LeaseSlot::with_sharding(AccountId(1), sharding),
        )
        .unwrap();

        let seen: Vec<_> = (0..8)
            .map(|_| {
                let map = Arc::clone(&map);
                std::thread::spawn(move || {
                    let locality = Locality::current();
                    let first = present(map.get_at(&Principal(1), locality));
                    let second = present(map.get_at(&Principal(1), locality));
                    assert!(
                        Arc::ptr_eq(&first, &second),
                        "one thread's locality selects one state"
                    );
                    (locality.index(sharding), Arc::as_ptr(&first) as usize)
                })
                .join()
                .unwrap()
            })
            .collect();

        for (shard, state) in &seen {
            for (other_shard, other_state) in &seen {
                assert_eq!(
                    shard == other_shard,
                    state == other_state,
                    "each shard has exactly one state, and no two share one"
                );
            }
        }
    }

    fn present(entry: Option<MapEntry>) -> Arc<AccountAdmissionState> {
        match entry {
            Some(MapEntry::Present(state)) => state,
            _ => panic!("the principal is installed"),
        }
    }

    #[test]
    fn arc_swap_map_contract() {
        exercises_map(ArcSwapSnapshotMap::new());
    }

    /// `impl SnapshotMap for Arc<T>` is a `SnapshotMap` in its own right, and
    /// every method on it has to reach the inner map: this is the impl
    /// `SnapshotManager` writes the request path's map through. Running the
    /// whole contract through the delegation is what gives the single-principal
    /// writes a witness — mutation testing showed `install_revoked` and
    /// `install_unknown` could both be replaced by no-ops with the suite green,
    /// because the bulk test below reaches the inner map's own `apply_many` and
    /// never these two. Driving the contract, rather than adding one test per
    /// method as each is noticed, is what covers the ones nobody has renamed
    /// yet.
    #[test]
    fn arc_delegation_map_contract() {
        exercises_map(Arc::new(ArcSwapSnapshotMap::new()));
    }

    #[test]
    fn publishable_install_carries_maximum_quote_into_rate_sharding() {
        assert_eq!(align_of::<AccountAdmissionState>(), 128);
        assert_eq!(align_of::<AccountSnapshot>(), 128);
        let sharding = LocalSharding::new(NonZeroUsize::new(8).unwrap());
        let map = ArcSwapSnapshotMap::with_sharding(sharding);
        let mut priced = (*snapshot(1)).clone();
        priced.limits = ResolvedLimits::new(1).with_weighted_rate(8, 800);
        priced.cost_table = Arc::new(
            CostTable::builder(CostUnits(100), CostUnits(100))
                .weight(&PricedOp, CostUnits::ZERO)
                .build(),
        );
        let priced = Arc::new(priced);
        let publishable = PublishableSnapshot::try_new(priced).unwrap();
        map.install_publishable(
            Principal(1),
            publishable.clone(),
            LeaseSlot::with_sharding(AccountId(1), sharding),
        )
        .unwrap();

        match map.map.load().get(&Principal(1)).unwrap() {
            StoredEntry::ShardedPresent(states) => {
                assert_eq!(states.len(), 8);
                assert!(!Arc::ptr_eq(&states[0], &states[1]));
            }
            StoredEntry::Present(_) | StoredEntry::NegativeUntil { .. } => {
                panic!("sharded publication must install locality-owned states")
            }
        }

        let state = match map.get(&Principal(1)).unwrap() {
            MapEntry::Present(state) => state,
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };
        assert_eq!(
            state
                .limiter
                .current()
                .rate()
                .weighted()
                .unwrap()
                .shard_count(),
            8
        );
        assert_eq!(map.local_sharding(), sharding);
        assert_eq!(state.lease.sharding(), sharding);

        map.apply_publishable_many(vec![PublishableSnapshotUpdate::Present {
            principal: Principal(2),
            snapshot: publishable.clone(),
            lease: LeaseSlot::with_sharding(AccountId(1), sharding),
        }])
        .unwrap();
        map.apply_publishable_many_at(
            vec![PublishableSnapshotUpdate::Present {
                principal: Principal(3),
                snapshot: publishable,
                lease: LeaseSlot::with_sharding(AccountId(1), sharding),
            }],
            t(1),
        )
        .unwrap();
        assert_eq!(generation_of(&map, &Principal(2)), Some(1));
        assert_eq!(generation_of(&map, &Principal(3)), Some(1));
    }

    /// The control plane asks whether an entry is still *visible*, which is not
    /// the same question `get` answers for a request: a probe must not count as
    /// a request for cache-frequency purposes, and both maps therefore answer
    /// it from their own key index rather than through a lookup.
    #[test]
    fn the_visibility_probe_tracks_installation_and_eviction_in_every_map() {
        let moka: Arc<dyn SnapshotMap> = Arc::new(MokaSnapshotMap::new(8));
        let arc_swap: Arc<dyn SnapshotMap> = Arc::new(ArcSwapSnapshotMap::new());
        for map in [moka, arc_swap] {
            assert!(!map.contains_cached(&Principal(1)));
            map.install(
                Principal(1),
                snapshot(1),
                LeaseSlot::for_account(AccountId(1)),
            )
            .unwrap();
            assert!(map.contains_cached(&Principal(1)));
            assert!(!map.contains_cached(&Principal(2)));
            map.remove(&Principal(1));
            assert!(!map.contains_cached(&Principal(1)));
        }
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
                lease: LeaseSlot::for_account(AccountId(1)),
            }],
            t(10),
        )
        .unwrap();
        assert_eq!(generation_of(&map, &Principal(1)), Some(1));
    }

    /// The other two bulk methods on the `Arc` delegation had no witness, and
    /// both could be replaced by no-ops with the whole suite green (#43).
    ///
    /// That is not a cosmetic gap. `impl<T: SnapshotMap + ?Sized> SnapshotMap
    /// for Arc<T>` is what `SnapshotManager`'s own `Arc<dyn SnapshotMap>`
    /// dispatches through, so a silent no-op there means a refresh pass that
    /// reports success and installs nothing — every principal ageing out to
    /// `SnapshotExpired` with no error anywhere.
    #[test]
    fn the_arc_delegation_forwards_every_bulk_write() {
        let sharding = LocalSharding::new(NonZeroUsize::new(2).unwrap());
        let map: Arc<dyn SnapshotMap> = Arc::new(ArcSwapSnapshotMap::with_sharding(sharding));

        map.install_publishable(
            Principal(0),
            PublishableSnapshot::try_new(snapshot(1)).unwrap(),
            LeaseSlot::with_sharding(AccountId(1), sharding),
        )
        .unwrap();
        assert_eq!(map.local_sharding(), sharding);
        assert!(matches!(
            map.get_at(&Principal(0), Locality::current()),
            Some(MapEntry::Present(_))
        ));

        map.install_many(vec![
            (
                Principal(1),
                snapshot(1),
                LeaseSlot::for_account(AccountId(1)),
            ),
            (
                Principal(2),
                snapshot(1),
                LeaseSlot::for_account(AccountId(1)),
            ),
        ])
        .unwrap();
        assert_eq!(generation_of(&map, &Principal(1)), Some(1));
        assert_eq!(generation_of(&map, &Principal(2)), Some(1));

        map.apply_many(vec![
            SnapshotUpdate::Present {
                principal: Principal(1),
                snapshot: snapshot(2),
                lease: LeaseSlot::for_account(AccountId(1)),
            },
            SnapshotUpdate::Revoked {
                principal: Principal(2),
                until: t(100),
                generation: Generation(2),
            },
        ])
        .unwrap();
        assert_eq!(generation_of(&map, &Principal(1)), Some(2));
        assert!(matches!(
            map.get(&Principal(2)),
            Some(MapEntry::NegativeUntil { .. })
        ));

        map.apply_publishable_many(vec![PublishableSnapshotUpdate::Present {
            principal: Principal(3),
            snapshot: PublishableSnapshot::try_new(snapshot(1)).unwrap(),
            lease: LeaseSlot::for_account(AccountId(1)),
        }])
        .unwrap();
        assert_eq!(generation_of(&map, &Principal(3)), Some(1));
        map.apply_publishable_many_at(
            vec![PublishableSnapshotUpdate::Present {
                principal: Principal(3),
                snapshot: PublishableSnapshot::try_new(snapshot(2)).unwrap(),
                lease: LeaseSlot::for_account(AccountId(1)),
            }],
            t(10),
        )
        .unwrap();
        assert_eq!(generation_of(&map, &Principal(3)), Some(2));

        map.remove(&Principal(1));
        assert!(map.get(&Principal(1)).is_none());
    }

    #[test]
    fn bulk_removal_withdraws_every_entry_and_preserves_revocation_watermarks() {
        exercises_bulk_removal(Arc::new(ArcSwapSnapshotMap::new()));
        exercises_bulk_removal(Arc::new(MokaSnapshotMap::new(100)));
    }

    fn exercises_bulk_removal(map: impl SnapshotMap) {
        for id in 1..=3 {
            map.install(
                Principal(id),
                snapshot(1),
                LeaseSlot::for_account(AccountId(1)),
            )
            .unwrap();
        }
        map.install_revoked(Principal(2), t(100), Generation(2))
            .unwrap();
        map.remove_many(&[Principal(1), Principal(2), Principal(999)]);
        assert!(map.get(&Principal(1)).is_none());
        assert!(map.get(&Principal(2)).is_none());
        assert_eq!(generation_of(&map, &Principal(3)), Some(1));
        map.install(
            Principal(2),
            snapshot(2),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        assert!(
            map.get(&Principal(2)).is_none(),
            "removal must preserve the tombstone watermark"
        );
        map.install(
            Principal(2),
            snapshot(3),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        assert_eq!(generation_of(&map, &Principal(2)), Some(3));
        map.remove_many(&[]);
        assert_eq!(generation_of(&map, &Principal(3)), Some(1));
    }

    #[test]
    fn arc_swap_negative_cache_is_bounded_and_evicts_oldest_deadline_first() {
        let map = ArcSwapSnapshotMap::with_max_negative_entries(2);
        map.install(
            Principal(1),
            snapshot(1),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        map.install_unknown(Principal(2), t(30)).unwrap();
        map.install_unknown(Principal(3), t(10)).unwrap();
        map.install_unknown(Principal(4), t(20)).unwrap();

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
        map.install_unknown(Principal(1), t(10)).unwrap();
        map.install_unknown(Principal(2), t(20)).unwrap();

        map.apply_many_at(Vec::new(), t(10)).unwrap();

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
        map.install(principal, snapshot(5), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        map.install_revoked(principal, t(10), Generation(5))
            .unwrap();
        assert!(map.get(&principal).is_none(), "zero-cap cache must evict");

        map.install(principal, snapshot(4), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        map.install(principal, snapshot(5), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert!(
            map.get(&principal).is_none(),
            "eviction must not admit a stale or replayed snapshot"
        );
        map.install(principal, snapshot(6), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        assert_eq!(generation_of(&map, &principal), Some(6));
    }

    #[test]
    fn many_unknowns_leave_only_the_configured_number_visible() {
        const CAP: usize = 17;
        let map = ArcSwapSnapshotMap::with_max_negative_entries(CAP);
        for raw in 0..1_000 {
            map.install_unknown(Principal(raw), t(i64::try_from(raw).unwrap() + 1))
                .unwrap();
        }
        let visible = (0..1_000)
            .filter(|raw| map.get(&Principal(*raw)).is_some())
            .count();
        assert_eq!(visible, CAP);
    }

    /// A snapshot for a named account, so a test can build the many-account
    /// workload the single-account `snapshot()` fixture cannot express.
    fn snapshot_for(account: u128, generation: u64) -> Arc<AccountSnapshot> {
        let mut snapshot = (*snapshot(generation)).clone();
        snapshot.account_id = AccountId(account);
        Arc::new(snapshot)
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
            map.install(
                principal,
                snapshot_for(account, 1),
                LeaseSlot::for_account(AccountId(1)),
            )
            .unwrap();
            map.remove(&principal);
        }

        let held = map.states.len();
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
                LeaseSlot::for_account(AccountId(1)),
            )
            .unwrap();
        }

        let walked = map.states.swept_entries();
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
        map.install(
            Principal(0),
            snapshot_for(7, 1),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        let before = match map.get(&Principal(0)).unwrap() {
            MapEntry::Present(state) => state.limiter.clone(),
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };

        // Enough churn to force several sweeps past the growth watermark.
        for account in 100..300u128 {
            let principal = Principal(account);
            map.install(
                principal,
                snapshot_for(account, 1),
                LeaseSlot::for_account(AccountId(1)),
            )
            .unwrap();
            map.remove(&principal);
        }

        map.install(
            Principal(1),
            snapshot_for(7, 1),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
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
            let mut faster = (*snapshot(5)).clone();
            faster.limits = ResolvedLimits::new(100).with_weighted_rate(1_000, 9_000);
            let faster = Arc::new(faster);
            let mut updates = vec![
                SnapshotUpdate::Present {
                    principal: Principal(1),
                    snapshot: snapshot(3),
                    lease: LeaseSlot::for_account(AccountId(1)),
                },
                SnapshotUpdate::Present {
                    principal: Principal(2),
                    snapshot: faster,
                    lease: LeaseSlot::for_account(AccountId(1)),
                },
            ];
            if reversed {
                updates.reverse();
            }
            map.apply_many(updates).unwrap();

            let state = match map.get(&Principal(1)).unwrap() {
                MapEntry::Present(state) => state,
                MapEntry::NegativeUntil { .. } => unreachable!(),
            };
            assert_eq!(
                state.snapshot.generation,
                Generation(3),
                "each principal keeps its own snapshot"
            );
            // Both principals share one stable account object. Every request
            // loads its current authority, so generation 5 is the one answer
            // whichever order the batch used.
            let other = match map.get(&Principal(2)).unwrap() {
                MapEntry::Present(state) => state,
                MapEntry::NegativeUntil { .. } => unreachable!(),
            };
            assert!(Arc::ptr_eq(&state.limiter, &other.limiter));
            let current = state.limiter.current();
            assert!(
                current
                    .rate()
                    .weighted()
                    .unwrap()
                    .check_n_at(
                        std::num::NonZeroU32::new(6_000).unwrap(),
                        Locality::current(),
                    )
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
        map.install(
            Principal(1),
            snapshot(1),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        map.install(
            Principal(2),
            snapshot(1),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
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
        map.install(p, snapshot(1), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        let first = match map.get(&p).unwrap() {
            MapEntry::Present(s) => s,
            _ => unreachable!(),
        };
        map.install(p, snapshot(2), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        let second = match map.get(&p).unwrap() {
            MapEntry::Present(s) => s,
            _ => unreachable!(),
        };
        assert!(Arc::ptr_eq(&first.limiter, &second.limiter));
        let before_update = second.limiter.current();

        let mut changed = (*snapshot(3)).clone();
        changed.limits = ResolvedLimits::new(100).with_weighted_rate(2_000, 5_000);
        map.install(p, Arc::new(changed), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        let third = match map.get(&p).unwrap() {
            MapEntry::Present(s) => s,
            _ => unreachable!(),
        };
        assert!(Arc::ptr_eq(&second.limiter, &third.limiter));
        assert!(!Arc::ptr_eq(&before_update, &third.limiter.current()));
    }

    #[test]
    fn limit_change_is_one_account_authority_for_every_principal() {
        let map = ArcSwapSnapshotMap::new();
        map.install(
            Principal(1),
            snapshot(1),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        map.install(
            Principal(2),
            snapshot(1),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        let before = match map.get(&Principal(2)).unwrap() {
            MapEntry::Present(state) => state.limiter.current(),
            _ => unreachable!(),
        };

        let mut changed = (*snapshot(2)).clone();
        changed.limits = ResolvedLimits::new(100).with_weighted_rate(2_000, 5_000);
        map.install(
            Principal(1),
            Arc::new(changed),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();

        let (a, b) = match (
            map.get(&Principal(1)).unwrap(),
            map.get(&Principal(2)).unwrap(),
        ) {
            (MapEntry::Present(a), MapEntry::Present(b)) => (a, b),
            _ => unreachable!(),
        };
        assert!(Arc::ptr_eq(&a.limiter, &b.limiter));
        let current = a.limiter.current();
        assert!(!Arc::ptr_eq(&before, &current));
        assert!(Arc::ptr_eq(&current, &b.limiter.current()));
    }

    #[test]
    fn an_in_flight_principal_gauge_survives_removal_and_reinstall() {
        let map = ArcSwapSnapshotMap::new();
        let principal = Principal(1);
        let mut first = (*snapshot(1)).clone();
        first.limits = first
            .limits
            .with_concurrency(
                NonZeroU32::new(2).unwrap(),
                Some(NonZeroU32::new(1).unwrap()),
            )
            .unwrap();
        map.install(
            principal,
            Arc::new(first),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        let first = match map.get(&principal).unwrap() {
            MapEntry::Present(state) => state,
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };
        let first_account = first.limiter.current();
        let held = AccountAdmissionState::acquire_concurrency(
            Arc::clone(&first),
            first_account.max_concurrent_requests(),
            Locality::current(),
        )
        .unwrap();

        map.remove(&principal);
        let mut replacement = (*snapshot(2)).clone();
        replacement.limits = first.snapshot.limits;
        map.install(
            principal,
            Arc::new(replacement),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        let replacement = match map.get(&principal).unwrap() {
            MapEntry::Present(state) => state,
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };
        assert!(Arc::ptr_eq(
            &first.principal_gauge,
            &replacement.principal_gauge
        ));
        assert_eq!(
            AccountAdmissionState::acquire_concurrency(
                Arc::clone(&replacement),
                replacement.limiter.current().max_concurrent_requests(),
                Locality::current(),
            )
            .unwrap_err()
            .0,
            DenyReason::ConcurrencyLimited
        );

        drop(held);
        let replacement_account = replacement.limiter.current();
        drop(
            AccountAdmissionState::acquire_concurrency(
                replacement,
                replacement_account.max_concurrent_requests(),
                Locality::current(),
            )
            .unwrap(),
        );
    }

    #[test]
    fn stale_snapshot_cannot_roll_back_shared_limiter() {
        let map = ArcSwapSnapshotMap::new();
        let mut current = (*snapshot(5)).clone();
        current.limits = ResolvedLimits::new(100).with_weighted_rate(2_000, 5_000);
        map.install(
            Principal(1),
            Arc::new(current),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        let installed = match map.get(&Principal(1)).unwrap() {
            MapEntry::Present(state) => state.limiter.current(),
            _ => unreachable!(),
        };

        map.install(
            Principal(1),
            snapshot(3),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        let after_stale = match map.get(&Principal(1)).unwrap() {
            MapEntry::Present(state) => state.limiter.current(),
            _ => unreachable!(),
        };
        assert!(Arc::ptr_eq(&installed, &after_stale));
    }

    fn rejected_positive_cannot_change_account_policy(map: impl SnapshotMap) {
        let sibling = Principal(2);
        let revoked = Principal(1);
        map.install(sibling, snapshot(5), LeaseSlot::for_account(AccountId(1)))
            .unwrap();
        let before = match map.get(&sibling).unwrap() {
            MapEntry::Present(state) => state.limiter.current(),
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };

        map.install_revoked(revoked, t(100), Generation(10))
            .unwrap();
        let mut replay = (*snapshot(9)).clone();
        replay.limits = ResolvedLimits::new(100)
            .with_weighted_rate_compatibility_fallback(7, 11)
            .with_request_rate(NonZeroU32::new(3).unwrap(), NonZeroU32::new(4).unwrap());
        map.install(
            revoked,
            Arc::new(replay),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();

        assert!(matches!(
            map.get(&revoked),
            Some(MapEntry::NegativeUntil { .. })
        ));
        let after = match map.get(&sibling).unwrap() {
            MapEntry::Present(state) => state.limiter.current(),
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };
        assert!(
            Arc::ptr_eq(&before, &after),
            "a replay rejected by the principal watermark cannot publish account policy"
        );
    }

    #[test]
    fn moka_rejects_replays_before_resolving_account_state() {
        rejected_positive_cannot_change_account_policy(MokaSnapshotMap::new(16));
    }

    #[test]
    fn arc_swap_rejects_replays_before_resolving_account_state() {
        rejected_positive_cannot_change_account_policy(ArcSwapSnapshotMap::new());
    }

    #[test]
    fn an_overwritten_batch_positive_never_becomes_account_authority() {
        let map = ArcSwapSnapshotMap::new();
        map.install(
            Principal(2),
            snapshot(5),
            LeaseSlot::for_account(AccountId(1)),
        )
        .unwrap();
        let before = match map.get(&Principal(2)).unwrap() {
            MapEntry::Present(state) => state.limiter.current(),
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };

        let mut transient = (*snapshot(9)).clone();
        transient.limits = ResolvedLimits::new(100)
            .with_weighted_rate_compatibility_fallback(7, 11)
            .with_request_rate(NonZeroU32::new(3).unwrap(), NonZeroU32::new(4).unwrap());
        map.apply_many(vec![
            SnapshotUpdate::Present {
                principal: Principal(1),
                snapshot: Arc::new(transient),
                lease: LeaseSlot::for_account(AccountId(1)),
            },
            SnapshotUpdate::Revoked {
                principal: Principal(1),
                until: t(100),
                generation: Generation(10),
            },
        ])
        .unwrap();

        assert!(matches!(
            map.get(&Principal(1)),
            Some(MapEntry::NegativeUntil { .. })
        ));
        let after = match map.get(&Principal(2)).unwrap() {
            MapEntry::Present(state) => state.limiter.current(),
            MapEntry::NegativeUntil { .. } => unreachable!(),
        };
        assert!(Arc::ptr_eq(&before, &after));
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;
    use tollgate_core::{AccountId, AccountStatus, CostTable, PermissionBits, ResolvedLimits};

    fn t(seconds: i64) -> Timestamp {
        Timestamp::from_second(seconds).unwrap()
    }
    fn snapshot(generation: u64) -> Arc<AccountSnapshot> {
        Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(generation),
                AccountStatus::Active,
                Timestamp::MAX,
                PermissionBits::ALL,
                ResolvedLimits::new(100),
                Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
            )
            .build(),
        )
    }
    fn positive(principal: Principal, generation: u64) -> PublishableSnapshotUpdate {
        PublishableSnapshotUpdate::Present {
            principal,
            snapshot: PublishableSnapshot::try_new(snapshot(generation)).unwrap(),
            lease: LeaseSlot::for_account(AccountId(1)),
        }
    }
    fn maps(capacity: usize) -> [Arc<dyn SnapshotMap>; 2] {
        let capacity = NonZeroUsize::new(capacity).unwrap();
        [
            Arc::new(ArcSwapSnapshotMap::with_capacities(
                LocalSharding::SINGLE,
                2,
                capacity,
            )),
            Arc::new(MokaSnapshotMap::with_capacities(
                128,
                LocalSharding::SINGLE,
                capacity,
            )),
        ]
    }
    fn fresh(map: &dyn SnapshotMap, update: PublishableSnapshotUpdate) {
        let read = map
            .prepare_refreshes(&[update.principal()])
            .unwrap()
            .reads
            .pop()
            .unwrap();
        map.apply_refreshed_many_at(vec![read.read(|| update)], t(0))
            .unwrap();
    }
    fn present(map: &dyn SnapshotMap, principal: Principal) -> u64 {
        match map.get(&principal) {
            Some(MapEntry::Present(state)) => state.snapshot.generation.0,
            other => panic!("expected a positive, got {other:?}"),
        }
    }

    #[test]
    fn generation_churn_is_bounded_in_both_maps() {
        for map in maps(7) {
            for n in 0..1000 {
                let principal = Principal(n);
                let update = if n % 2 == 0 {
                    positive(principal, 1)
                } else {
                    PublishableSnapshotUpdate::Revoked {
                        principal,
                        generation: Generation(2),
                        until: t(1),
                    }
                };
                fresh(&*map, update);
                let stats = map.history_stats().unwrap();
                assert_eq!(stats.capacity, 7);
                assert_eq!(stats.retained, ((n + 1) as usize).min(7));
                if n >= 7 {
                    assert!(map.get(&Principal(n - 7)).is_none());
                }
            }
            assert!(map.needs_refresh(Principal(0)));
            assert_eq!(
                map.install(
                    Principal(0),
                    snapshot(1),
                    LeaseSlot::for_account(AccountId(1))
                ),
                Err(PublicationError::RefreshRequired(Principal(0)))
            );
        }
    }

    #[test]
    fn reclaimed_history_requires_a_fresh_authority_before_replay() {
        for map in maps(1) {
            map.install_revoked(Principal(1), t(1), Generation(9))
                .unwrap();
            // The replay stays valid forever, so no tombstone TTL can make it safe.
            fresh(&*map, positive(Principal(2), 1));
            assert!(map.get(&Principal(1)).is_none());
            assert_eq!(
                map.install(
                    Principal(1),
                    snapshot(9),
                    LeaseSlot::for_account(AccountId(1))
                ),
                Err(PublicationError::RefreshRequired(Principal(1)))
            );
            fresh(
                &*map,
                PublishableSnapshotUpdate::Revoked {
                    principal: Principal(1),
                    until: t(10),
                    generation: Generation(9),
                },
            );
            map.remove(&Principal(1));
            map.install(
                Principal(1),
                snapshot(9),
                LeaseSlot::for_account(AccountId(1)),
            )
            .unwrap();
            assert!(map.get(&Principal(1)).is_none());
            fresh(&*map, positive(Principal(1), 10));
            assert_eq!(present(&*map, Principal(1)), 10);
            map.install_revoked(Principal(1), t(20), Generation(9))
                .unwrap();
            assert_eq!(present(&*map, Principal(1)), 10);
        }
    }

    #[test]
    fn a_recreated_principal_rejects_the_old_in_flight_response() {
        for map in maps(1) {
            let old = map
                .prepare_refreshes(&[Principal(1)])
                .unwrap()
                .reads
                .pop()
                .unwrap()
                .read(|| positive(Principal(1), 1));
            fresh(&*map, positive(Principal(2), 1));
            fresh(
                &*map,
                PublishableSnapshotUpdate::Revoked {
                    principal: Principal(1),
                    until: t(10),
                    generation: Generation(9),
                },
            );
            assert_eq!(
                map.apply_refreshed_many_at(vec![old], t(0)),
                Err(PublicationError::Superseded(Principal(1)))
            );
            assert!(matches!(
                map.get(&Principal(1)),
                Some(MapEntry::NegativeUntil { .. })
            ));
        }
    }

    #[test]
    fn an_unknown_revalidation_cannot_reopen_forgotten_history() {
        for map in maps(1) {
            map.install_revoked(Principal(1), t(1), Generation(9))
                .unwrap();
            fresh(&*map, positive(Principal(2), 1));
            fresh(
                &*map,
                PublishableSnapshotUpdate::Unknown {
                    principal: Principal(1),
                    until: t(10),
                },
            );
            assert!(map.needs_refresh(Principal(1)));
            assert_eq!(
                map.install(
                    Principal(1),
                    snapshot(9),
                    LeaseSlot::for_account(AccountId(1))
                ),
                Err(PublicationError::RefreshRequired(Principal(1)))
            );
            fresh(&*map, positive(Principal(1), 10));
            assert_eq!(present(&*map, Principal(1)), 10);
        }
    }

    #[test]
    fn refresh_fences_are_bound_to_the_map_principal_and_whole_batch() {
        for map in maps(2) {
            fresh(&*map, positive(Principal(1), 9));
            let foreign = ArcSwapSnapshotMap::new()
                .prepare_refreshes(&[Principal(1)])
                .unwrap()
                .reads
                .pop()
                .unwrap()
                .read(|| positive(Principal(1), 10));
            assert_eq!(
                map.apply_refreshed_many_at(vec![foreign], t(0)),
                Err(PublicationError::Superseded(Principal(1)))
            );
            let swapped = map
                .prepare_refreshes(&[Principal(1)])
                .unwrap()
                .reads
                .pop()
                .unwrap()
                .read(|| positive(Principal(2), 10));
            assert_eq!(
                map.apply_refreshed_many_at(vec![swapped], t(0)),
                Err(PublicationError::Superseded(Principal(2)))
            );
            let mut reads = map
                .prepare_refreshes(&[Principal(1), Principal(2)])
                .unwrap()
                .reads
                .into_iter();
            let valid = reads.next().unwrap().read(|| positive(Principal(1), 10));
            let invalid = reads.next().unwrap().read(|| positive(Principal(2), 10));
            // Reclaim principal 2 while explicitly protecting principal 1.
            map.prepare_refreshes(&[Principal(1), Principal(3)])
                .unwrap();
            assert_eq!(
                map.apply_refreshed_many_at(vec![valid, invalid], t(0)),
                Err(PublicationError::Superseded(Principal(2)))
            );
            assert_eq!(present(&*map, Principal(1)), 9);
            assert!(map.get(&Principal(2)).is_none());
            assert!(matches!(
                map.prepare_refreshes(&[Principal(1), Principal(2), Principal(3)]),
                Err(PublicationError::BatchExceedsCapacity {
                    requested: 3,
                    capacity: 2
                })
            ));
            assert_eq!(present(&*map, Principal(1)), 9);
        }
    }

    #[test]
    fn a_source_read_cannot_overwrite_a_concurrent_revocation() {
        for map in maps(1) {
            fresh(&*map, positive(Principal(1), 1));
            let read = map
                .prepare_refreshes(&[Principal(1)])
                .unwrap()
                .reads
                .pop()
                .unwrap();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (resume_tx, resume_rx) = std::sync::mpsc::channel();
            let writer = Arc::clone(&map);
            let task = std::thread::spawn(move || {
                let response = read.read(|| {
                    let response = positive(Principal(1), 1);
                    started_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                    response
                });
                writer.apply_refreshed_many_at(vec![response], t(0))
            });
            started_rx.recv().unwrap();
            map.install_revoked(Principal(1), t(10), Generation(9))
                .unwrap();
            resume_tx.send(()).unwrap();
            task.join().unwrap().unwrap();
            assert!(matches!(
                map.get(&Principal(1)),
                Some(MapEntry::NegativeUntil { .. })
            ));
        }
    }

    #[test]
    fn a_full_push_batch_preserves_existing_entries_and_account_policy() {
        for map in maps(1) {
            map.install(
                Principal(1),
                snapshot(9),
                LeaseSlot::for_account(AccountId(1)),
            )
            .unwrap();
            assert_eq!(
                map.apply_publishable_many(vec![
                    positive(Principal(1), 10),
                    positive(Principal(2), 20)
                ]),
                Err(PublicationError::RefreshRequired(Principal(2)))
            );
            assert_eq!(present(&*map, Principal(1)), 9);
            assert!(map.get(&Principal(2)).is_none());
        }
    }
}
