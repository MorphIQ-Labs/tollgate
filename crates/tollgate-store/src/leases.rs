//! The memory backend's lease table and its active-lease index, held together
//! because they must agree.
//!
//! The index exists so the reclaim sweep and `conservation` cost what the live
//! population costs rather than what the process has ever done (#23). A
//! derived structure like that is normally maintained by whoever mutates the
//! table, and this one must not be: the three transition points are spread
//! across `acquire`, `release` and the sweep, and the drift is silent and
//! severe. A lease left `Active` in the table but missing from the index is
//! never reclaimed *and* stops being counted by `conservation` — so the ledger
//! checker goes blind to the leak it caused.
//!
//! Hence this module. [`LeaseRecord::state`] and [`LeaseRecord::credited`] are
//! private to it, the only constructor produces an active lease, and
//! [`Leases::settle`] is the only way out of that state. No call site in
//! `memory.rs` can retire a lease and forget the index, because none of them
//! can retire a lease at all.

use std::collections::{BTreeSet, HashMap};

use jiff::{SignedDuration, Timestamp};

use tollgate_core::{AccountId, CostUnits, FencingToken, LeaseId};

/// How a lease left the active set. There is no `Active` variant: an active
/// lease is one that has not been settled, and [`Leases::settle`] is the only
/// thing that can settle it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Settled {
    Released,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseState {
    Active,
    Settled(Settled),
}

#[derive(Debug)]
pub(crate) struct LeaseRecord {
    pub(crate) account_id: AccountId,
    pub(crate) fencing_token: FencingToken,
    pub(crate) granted: CostUnits,
    /// Usage recorded against this lease so far. Mutable from outside:
    /// `ingest` legitimately moves it on a lease in any state.
    pub(crate) used: CostUnits,
    pub(crate) expires_at: Timestamp,
    /// Units credited back to the account at settlement (release `unspent`, or
    /// the full remainder at expiry reclaim). Zero while active, and settable
    /// only through [`Leases::settle`], so it cannot drift from the state it
    /// describes.
    credited: CostUnits,
    state: LeaseState,
}

impl LeaseRecord {
    /// The only constructor, and it always yields an active lease. Reaching
    /// the table means going through [`Leases::open`], which indexes it.
    pub(crate) fn opened(
        account_id: AccountId,
        fencing_token: FencingToken,
        granted: CostUnits,
        expires_at: Timestamp,
    ) -> Self {
        LeaseRecord {
            account_id,
            fencing_token,
            granted,
            used: CostUnits::ZERO,
            expires_at,
            credited: CostUnits::ZERO,
            state: LeaseState::Active,
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state == LeaseState::Active
    }

    pub(crate) fn credited(&self) -> CostUnits {
        self.credited
    }
}

#[derive(Default)]
pub(crate) struct Leases {
    records: HashMap<LeaseId, LeaseRecord>,
    /// Active leases ordered by expiry, so the sweep can stop at the first one
    /// that is not due instead of filtering the whole table — the shape #22
    /// used for the snapshot manager's deadline indexes.
    active_by_expiry: BTreeSet<(Timestamp, LeaseId)>,
    /// Lease records examined by [`Leases::reclaimable`] and
    /// [`Leases::active_of`] since construction.
    ///
    /// The bound #23 claims is about *work*, and only a count of records
    /// actually looked at can distinguish "the sweep walks the live set" from
    /// "the sweep walks everything and the live set happens to be small".
    #[cfg(test)]
    examined: std::cell::Cell<usize>,
}

impl Leases {
    /// Record a newly acquired lease, active and indexed.
    pub(crate) fn open(&mut self, lease_id: LeaseId, record: LeaseRecord) {
        self.active_by_expiry.insert((record.expires_at, lease_id));
        self.records.insert(lease_id, record);
    }

    pub(crate) fn get(&self, lease_id: LeaseId) -> Option<&LeaseRecord> {
        self.records.get(&lease_id)
    }

    /// Mutable access for the fields a settled lease may still move — `used`,
    /// which `ingest` credits when a straggler arrives. `state` and `credited`
    /// are not reachable this way.
    pub(crate) fn get_mut(&mut self, lease_id: LeaseId) -> Option<&mut LeaseRecord> {
        self.records.get_mut(&lease_id)
    }

    /// Settle an active lease: leave the index, take the settled state, and
    /// record what went back to the account — one transition, so the three
    /// cannot disagree.
    ///
    /// Returns `false` if the lease is unknown or already settled, which is
    /// how the caller distinguishes a real transition from a replay.
    pub(crate) fn settle(
        &mut self,
        lease_id: LeaseId,
        settled: Settled,
        credited: CostUnits,
    ) -> bool {
        let Some(record) = self.records.get_mut(&lease_id) else {
            return false;
        };
        if record.state != LeaseState::Active {
            return false;
        }
        self.active_by_expiry.remove(&(record.expires_at, lease_id));
        record.state = LeaseState::Settled(settled);
        record.credited = credited;
        true
    }

    /// Active leases whose grace window has closed by `now`, oldest first, at
    /// most `limit` of them.
    ///
    /// The index is ordered by expiry and `reclaim_grace` is a constant, so
    /// the due-ness predicate is monotone along it and the walk can stop at
    /// the first lease that is not due. Testing the predicate rather than
    /// computing `now - grace` keeps the existing saturating `checked_add` and
    /// leaves no subtraction to underflow.
    pub(crate) fn reclaimable(
        &self,
        now: Timestamp,
        grace: SignedDuration,
        limit: usize,
    ) -> Vec<LeaseId> {
        self.active_by_expiry
            .iter()
            .inspect(|_| self.mark_examined())
            .take_while(|(expires_at, _)| {
                let reclaim_at = expires_at.checked_add(grace).unwrap_or(Timestamp::MAX);
                now >= reclaim_at
            })
            .take(limit)
            .map(|(_, lease_id)| *lease_id)
            .collect()
    }

    /// The account's active leases. Walks the index rather than the table, so
    /// the cost is the live population — but it still yields the *records*:
    /// `conservation` recomputes its sums from them rather than reading a
    /// maintained total, because a ledger checker that trusts a number the
    /// ledger's own writers maintain cannot catch those writers being wrong.
    pub(crate) fn active_of(&self, account: AccountId) -> impl Iterator<Item = &LeaseRecord> {
        self.active_by_expiry.iter().filter_map(move |(_, id)| {
            self.mark_examined();
            let record = self.records.get(id)?;
            (record.account_id == account).then_some(record)
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn active_len(&self) -> usize {
        self.active_by_expiry.len()
    }

    #[cfg(test)]
    fn mark_examined(&self) {
        self.examined.set(self.examined.get() + 1);
    }

    #[cfg(not(test))]
    #[expect(
        clippy::unused_self,
        reason = "the counter this stands in for exists only under cfg(test)"
    )]
    fn mark_examined(&self) {}

    #[cfg(test)]
    pub(crate) fn examined(&self) -> usize {
        self.examined.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCOUNT: AccountId = AccountId(1);
    const GRACE: SignedDuration = SignedDuration::from_secs(30);

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    fn open_at(leases: &mut Leases, id: u128, expires_at: i64) -> LeaseId {
        let lease_id = LeaseId(id);
        leases.open(
            lease_id,
            LeaseRecord::opened(ACCOUNT, FencingToken(1), CostUnits(100), t(expires_at)),
        );
        lease_id
    }

    /// The reason this module exists: settling must leave the index, or the
    /// lease is invisible to the sweep that should reclaim it *and* to the
    /// conservation check that would have noticed.
    #[test]
    fn settling_removes_a_lease_from_the_index_but_keeps_its_record() {
        let mut leases = Leases::default();
        let lease_id = open_at(&mut leases, 1, 60);
        assert_eq!(leases.active_len(), 1);
        assert_eq!(leases.reclaimable(t(90), GRACE, 10), vec![lease_id]);

        assert!(leases.settle(lease_id, Settled::Released, CostUnits(40)));
        assert_eq!(leases.active_len(), 0);
        assert!(
            leases.reclaimable(t(90), GRACE, 10).is_empty(),
            "a released lease must never be reclaimed as well"
        );
        assert_eq!(
            leases.active_of(ACCOUNT).count(),
            0,
            "and must stop counting towards active grants"
        );

        // The record survives: a straggling usage event still has to be
        // fenced and rejected against it.
        let record = leases.get(lease_id).expect("record retained");
        assert!(!record.is_active());
        assert_eq!(record.credited(), CostUnits(40));
        assert_eq!(leases.len(), 1);
    }

    /// Settlement is one transition, so a replayed release cannot credit the
    /// account twice — the caller is told instead.
    #[test]
    fn a_lease_settles_once() {
        let mut leases = Leases::default();
        let lease_id = open_at(&mut leases, 1, 60);
        assert!(leases.settle(lease_id, Settled::Released, CostUnits(40)));
        assert!(!leases.settle(lease_id, Settled::Expired, CostUnits(100)));
        assert_eq!(
            leases.get(lease_id).unwrap().credited(),
            CostUnits(40),
            "the second settlement must not overwrite the first's credit"
        );
        assert!(!leases.settle(LeaseId(99), Settled::Released, CostUnits(0)));
    }

    /// The walk stops at the first lease that is not due, which is only sound
    /// because the index is ordered by expiry.
    #[test]
    fn reclaimable_yields_due_leases_oldest_first_and_stops_there() {
        let mut leases = Leases::default();
        let early = open_at(&mut leases, 1, 10);
        let middle = open_at(&mut leases, 2, 20);
        let late = open_at(&mut leases, 3, 500);

        assert_eq!(
            leases.reclaimable(t(55), GRACE, 10),
            vec![early, middle],
            "due at expiry + grace, and `late` is not"
        );
        assert_eq!(
            leases.reclaimable(t(55), GRACE, 1),
            vec![early],
            "the limit truncates from the oldest end"
        );
        assert!(leases.reclaimable(t(39), GRACE, 10).is_empty());

        let before = leases.examined();
        assert!(leases.reclaimable(t(0), GRACE, 10).is_empty());
        assert_eq!(
            leases.examined() - before,
            1,
            "nothing is due, so the walk must stop after the first lease \
             rather than filtering the whole index"
        );
        assert_eq!(leases.active_len(), 3, "listing settles nothing");
        let _ = late;
    }

    #[test]
    fn active_of_selects_by_account() {
        const OTHER: AccountId = AccountId(2);
        let mut leases = Leases::default();
        open_at(&mut leases, 1, 60);
        leases.open(
            LeaseId(2),
            LeaseRecord::opened(OTHER, FencingToken(1), CostUnits(7), t(60)),
        );

        assert_eq!(leases.active_of(ACCOUNT).count(), 1);
        let other: Vec<_> = leases.active_of(OTHER).map(|lease| lease.granted).collect();
        assert_eq!(other, vec![CostUnits(7)]);
    }
}
