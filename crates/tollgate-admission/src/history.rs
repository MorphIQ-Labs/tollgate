//! Bounded, control-plane generation history and authoritative-read fences.
//!
//! A visible eviction preserves history. Reclaiming history is different: it
//! invalidates the visible entry and every outstanding read of that incarnation.
//! An authoritative read started afterwards is required before pushes may use
//! the slot again. Source reads must be linearizable against durable tombstones;
//! an eventually consistent replica is not a revalidation authority.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tollgate_core::{Generation, Principal};

use crate::generation_model::{Watermark, accept_positive, accept_revoked};

/// A publication could not safely replace the current cache state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationError {
    /// History is absent or still being reconstructed. Start an authoritative
    /// refresh; retrying the push cannot establish the missing generation floor.
    RefreshRequired(Principal),
    /// History was reclaimed after this read started, or its proof belongs to
    /// another principal/map. Discard the response and start a new read.
    Superseded(Principal),
    /// One atomic refresh cannot retain more distinct principals than its budget.
    BatchExceedsCapacity { requested: usize, capacity: usize },
    /// Read identities never wrap. Replace the map and revalidate from the source.
    IdentityExhausted,
}

impl std::fmt::Display for PublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RefreshRequired(principal) => {
                write!(f, "principal {principal} requires an authoritative refresh")
            }
            Self::Superseded(principal) => {
                write!(f, "refresh for principal {principal} was superseded")
            }
            Self::BatchExceedsCapacity {
                requested,
                capacity,
            } => write!(
                f,
                "refresh contains {requested} principals; history capacity is {capacity}"
            ),
            Self::IdentityExhausted => f.write_str("snapshot refresh identity space exhausted"),
        }
    }
}

impl std::error::Error for PublicationError {}

#[derive(Debug)]
struct Fence {
    owner: Option<Arc<()>>,
    incarnation: u64,
    principal: Principal,
}

/// A reserved slot whose source read has not yet started.
///
/// Call [`Self::fetch`] with a new authoritative source operation, not a cached
/// response or an already running future. The method invokes the operation only
/// after reservation and carries its fence through cancellation and completion.
#[derive(Debug)]
pub struct SnapshotRefresh(Fence);

impl SnapshotRefresh {
    pub(crate) fn unfenced(principal: Principal) -> Self {
        Self(Fence {
            owner: None,
            incarnation: 0,
            principal,
        })
    }

    #[must_use]
    pub fn principal(&self) -> Principal {
        self.0.principal
    }

    pub async fn fetch<T, F: Future<Output = T>>(self, read: impl FnOnce() -> F) -> Refreshed<T> {
        let value = read().await;
        Refreshed {
            fence: self.0,
            value,
        }
    }

    /// Synchronous counterpart for an authoritative in-memory source.
    pub fn read<T>(self, read: impl FnOnce() -> T) -> Refreshed<T> {
        let value = read();
        Refreshed {
            fence: self.0,
            value,
        }
    }
}

/// An authoritative read and the retained incarnation it started against.
/// No public constructor can attach a new fence to an old result.
#[derive(Debug)]
pub struct Refreshed<T> {
    fence: Fence,
    value: T,
}

impl<T> Refreshed<T> {
    pub(crate) fn value(&self) -> &T {
        &self.value
    }
    #[must_use]
    pub fn principal(&self) -> Principal {
        self.fence.principal
    }

    /// Translate a source response into its cache update, preserving the fence.
    /// Returning `None` discards a failed or unnecessary response.
    pub fn filter_map<U>(self, convert: impl FnOnce(T) -> Option<U>) -> Option<Refreshed<U>> {
        convert(self.value).map(|value| Refreshed {
            fence: self.fence,
            value,
        })
    }

    pub(crate) fn into_value(self) -> T {
        self.value
    }
}

/// Reservation changes are reported even if the ensuing source reads fail.
#[derive(Debug)]
pub struct RefreshBatch {
    pub reads: Vec<SnapshotRefresh>,
    /// These principals are already absent from the request-visible map. Drop
    /// their resolution/deadline bookkeeping, preserving account lease slots.
    pub evicted: Vec<Principal>,
}

/// Current control-plane retention occupancy, including pending source reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotHistoryStats {
    pub capacity: usize,
    pub retained: usize,
}

struct Entry {
    incarnation: u64,
    watermark: Option<Watermark>,
    initialized: bool,
    invalidated: bool,
}

pub(crate) struct GenerationHistory {
    by_principal: HashMap<Principal, Entry, foldhash::fast::RandomState>,
    oldest: BTreeMap<u64, Principal>,
    capacity: NonZeroUsize,
    owner: Arc<()>,
    next: u64,
    reclaimed: bool,
}

impl GenerationHistory {
    pub(crate) fn stats(&self) -> SnapshotHistoryStats {
        SnapshotHistoryStats {
            capacity: self.capacity.get(),
            retained: self.by_principal.len(),
        }
    }
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            by_principal: HashMap::default(),
            oldest: BTreeMap::new(),
            capacity,
            owner: Arc::new(()),
            next: 0,
            reclaimed: false,
        }
    }

    pub(crate) fn capacity(&self) -> NonZeroUsize {
        self.capacity
    }

    pub(crate) fn needs_refresh(&self, principal: Principal) -> bool {
        match self.by_principal.get(&principal) {
            Some(entry) => !entry.initialized,
            None => self.reclaimed || self.by_principal.len() == self.capacity.get(),
        }
    }

    /// Validate an entire push batch before mutating history or runtime state.
    pub(crate) fn check_pushes(
        &mut self,
        principals: impl Iterator<Item = Principal>,
    ) -> Result<(), PublicationError> {
        let mut new = HashSet::new();
        for principal in principals {
            if self.needs_refresh(principal) {
                if let Some(entry) = self.by_principal.get_mut(&principal) {
                    // A refused push may be newer than an already running
                    // reconstruction. Only a read started after this refusal
                    // may reopen the principal.
                    entry.invalidated = true;
                }
                return Err(PublicationError::RefreshRequired(principal));
            }
            if !self.by_principal.contains_key(&principal)
                && new.insert(principal)
                && new.len() > self.capacity.get() - self.by_principal.len()
            {
                return Err(PublicationError::RefreshRequired(principal));
            }
        }
        self.check_identities(new.len())
    }

    fn check_identities(&self, count: usize) -> Result<(), PublicationError> {
        let count = u64::try_from(count).map_err(|_| PublicationError::IdentityExhausted)?;
        self.next
            .checked_add(count)
            .ok_or(PublicationError::IdentityExhausted)?;
        Ok(())
    }

    fn insert(&mut self, principal: Principal, initialized: bool) {
        // Every caller preflights the entire insertion set under the same lock.
        self.next = self
            .next
            .checked_add(1)
            .expect("preflight reserved these identities");
        self.oldest.insert(self.next, principal);
        self.by_principal.insert(
            principal,
            Entry {
                incarnation: self.next,
                watermark: None,
                initialized,
                invalidated: false,
            },
        );
    }

    pub(crate) fn prepare(
        &mut self,
        principals: &[Principal],
    ) -> Result<RefreshBatch, PublicationError> {
        let wanted: HashSet<_> = principals.iter().copied().collect();
        if wanted.len() > self.capacity.get() {
            return Err(PublicationError::BatchExceedsCapacity {
                requested: wanted.len(),
                capacity: self.capacity.get(),
            });
        }
        #[allow(
            clippy::disallowed_methods,
            reason = "counts matches; a count does not depend on the order they are counted in"
        )]
        let added = wanted
            .iter()
            .filter(|principal| !self.by_principal.contains_key(principal))
            .count();
        #[allow(
            clippy::disallowed_methods,
            reason = "counts matches; a count does not depend on the order they are counted in"
        )]
        let invalidated = wanted
            .iter()
            .filter(|principal| {
                self.by_principal
                    .get(principal)
                    .is_some_and(|entry| entry.invalidated)
            })
            .count();
        self.check_identities(added + invalidated)?;
        let remove = added.saturating_sub(self.capacity.get() - self.by_principal.len());
        let evicted: Vec<_> = self
            .oldest
            .values()
            .filter(|principal| !wanted.contains(principal))
            .take(remove)
            .copied()
            .collect();
        for principal in &evicted {
            let entry = self
                .by_principal
                .remove(principal)
                .expect("eviction index owns a retained entry");
            self.oldest.remove(&entry.incarnation);
        }
        self.reclaimed |= !evicted.is_empty();
        let mut reads = Vec::with_capacity(principals.len());
        for &principal in principals {
            if !self.by_principal.contains_key(&principal) {
                self.insert(principal, false);
            }
            if self.by_principal[&principal].invalidated {
                let entry = self
                    .by_principal
                    .get_mut(&principal)
                    .expect("retained above");
                self.oldest.remove(&entry.incarnation);
                self.next = self
                    .next
                    .checked_add(1)
                    .expect("preflight reserved replacement identities");
                entry.incarnation = self.next;
                entry.invalidated = false;
                self.oldest.insert(self.next, principal);
            }
            reads.push(SnapshotRefresh(Fence {
                owner: Some(Arc::clone(&self.owner)),
                incarnation: self.by_principal[&principal].incarnation,
                principal,
            }));
        }
        Ok(RefreshBatch { reads, evicted })
    }

    pub(crate) fn check_refresh<T>(
        &self,
        read: &Refreshed<T>,
        principal: Principal,
    ) -> Result<(), PublicationError> {
        let same_owner = read
            .fence
            .owner
            .as_ref()
            .is_some_and(|owner| Arc::ptr_eq(owner, &self.owner));
        if !same_owner
            || principal != read.fence.principal
            || !self.by_principal.get(&principal).is_some_and(|entry| {
                entry.incarnation == read.fence.incarnation && !entry.invalidated
            })
        {
            return Err(PublicationError::Superseded(principal));
        }
        Ok(())
    }

    pub(crate) fn complete_refresh(&mut self, principal: Principal, has_generation: bool) {
        let entry = self
            .by_principal
            .get_mut(&principal)
            .expect("validated refresh retains its entry");
        // Unknown is a denial, not evidence of a generation floor. A source
        // gap after reclamation must not admit a delayed positive push.
        entry.initialized |= has_generation;
    }

    pub(crate) fn accept_positive(
        &mut self,
        principal: Principal,
        incoming: Generation,
        visible: bool,
    ) -> bool {
        if !self.by_principal.contains_key(&principal) {
            self.insert(principal, true);
        }
        let entry = self
            .by_principal
            .get_mut(&principal)
            .expect("entry was inserted above");
        let (next, accepted) = accept_positive(entry.watermark, incoming, visible);
        entry.watermark = next;
        accepted
    }

    pub(crate) fn accept_revoked(&mut self, principal: Principal, incoming: Generation) -> bool {
        if !self.by_principal.contains_key(&principal) {
            self.insert(principal, true);
        }
        let entry = self
            .by_principal
            .get_mut(&principal)
            .expect("entry was inserted above");
        let (next, accepted) = accept_revoked(entry.watermark, incoming);
        entry.watermark = next;
        accepted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn history(capacity: usize) -> GenerationHistory {
        GenerationHistory::new(NonZeroUsize::new(capacity).unwrap())
    }

    #[test]
    fn identity_exhaustion_preserves_the_last_good_history() {
        let mut history = history(1);
        history.check_pushes([Principal(1)].into_iter()).unwrap();
        history.accept_revoked(Principal(1), Generation(7));
        history.next = u64::MAX;
        assert!(matches!(
            history.prepare(&[Principal(2)]),
            Err(PublicationError::IdentityExhausted)
        ));
        assert_eq!(history.stats().retained, 1);
        assert_eq!(
            history.by_principal[&Principal(1)].watermark,
            Some(Watermark::Revoked(Generation(7)))
        );
        assert_eq!(
            history.check_pushes([Principal(2)].into_iter()),
            Err(PublicationError::RefreshRequired(Principal(2)))
        );
        // Refreshing an existing, non-invalidated entry consumes no identity.
        assert!(history.prepare(&[Principal(1)]).is_ok());
        history
            .by_principal
            .get_mut(&Principal(1))
            .unwrap()
            .invalidated = true;
        assert!(matches!(
            history.prepare(&[Principal(1)]),
            Err(PublicationError::IdentityExhausted)
        ));
        assert!(history.by_principal[&Principal(1)].invalidated);
    }

    #[test]
    fn a_refused_push_invalidates_an_older_reconstruction() {
        let mut history = history(1);
        let old = history
            .prepare(&[Principal(1)])
            .unwrap()
            .reads
            .pop()
            .unwrap()
            .read(|| ());
        assert_eq!(
            history.check_pushes([Principal(1)].into_iter()),
            Err(PublicationError::RefreshRequired(Principal(1)))
        );
        assert_eq!(
            history.check_refresh(&old, Principal(1)),
            Err(PublicationError::Superseded(Principal(1)))
        );
        let new = history
            .prepare(&[Principal(1)])
            .unwrap()
            .reads
            .pop()
            .unwrap()
            .read(|| ());
        assert!(history.check_refresh(&new, Principal(1)).is_ok());
        assert!(history.check_refresh(&old, Principal(1)).is_err());
    }

    #[test]
    fn an_unseen_principal_needs_authority_once_history_is_full_or_has_reclaimed() {
        // Room to spare and nothing reclaimed: the watermark of an unseen
        // principal cannot have been forgotten, so a push may proceed.
        let mut spare = history(2);
        spare.prepare(&[Principal(1)]).unwrap();
        assert!(!spare.needs_refresh(Principal(9)));

        // Full, with nothing ever reclaimed: the next principal has no slot
        // and its history would be absent rather than merely unwritten.
        let mut full = history(1);
        full.prepare(&[Principal(1)]).unwrap();
        assert!(full.needs_refresh(Principal(9)));

        // Reclaimed once, and no longer full: reclamation alone still demands
        // an authoritative read, because a forgotten watermark never returns.
        let mut reclaimed = history(2);
        reclaimed.prepare(&[Principal(1), Principal(2)]).unwrap();
        reclaimed.prepare(&[Principal(3)]).unwrap();
        reclaimed.accept_positive(Principal(4), Generation(1), true);
        assert_ne!(reclaimed.stats().retained, reclaimed.capacity().get());
        assert!(reclaimed.needs_refresh(Principal(9)));
    }

    #[test]
    fn a_push_batch_may_not_exceed_the_room_history_has_left() {
        let mut history = history(2);
        history.prepare(&[Principal(1)]).unwrap();
        history.complete_refresh(Principal(1), true);
        // One slot is left. A two-principal batch would need two, and the
        // whole batch is refused rather than partly admitted.
        assert_eq!(
            history.check_pushes([Principal(2), Principal(3)].into_iter()),
            Err(PublicationError::RefreshRequired(Principal(3)))
        );
        assert!(history.check_pushes([Principal(2)].into_iter()).is_ok());
    }

    proptest! {
        #[test]
        fn churn_bounds_both_history_and_its_index(
            capacity in 1usize..32,
            principals in prop::collection::vec(0u128..128, 1..500),
        ) {
            let mut history = history(capacity);
            for principal in principals.into_iter().map(Principal) {
                let mut batch = history.prepare(&[principal]).unwrap();
                let observed = batch.reads.pop().unwrap().read(|| ());
                prop_assert!(history.check_refresh(&observed, principal).is_ok());
                history.complete_refresh(principal, true);
                history.accept_revoked(principal, Generation(100));
                prop_assert!(history.by_principal.len() <= capacity);
                prop_assert_eq!(history.oldest.len(), history.by_principal.len());
                for (identity, retained) in &history.oldest {
                    prop_assert_eq!(history.by_principal[retained].incarnation, *identity);
                }
                for evicted in batch.evicted {
                    prop_assert!(!history.by_principal.contains_key(&evicted));
                    prop_assert!(history.needs_refresh(evicted));
                }
            }
        }
    }
}
