//! Property tests for the hot-path invariants (INVARIANTS.md #1–#3, #11).

use std::sync::Arc;

use jiff::Timestamp;
use proptest::prelude::*;

use tollgate_core::{
    AccountId, CancelOutcome, CostTable, CostUnits, FencingToken, LeaseGrant, LeaseId, LocalLease,
    OpIndex, QuoteError, Reservation,
};

struct Op(usize);
impl OpIndex for Op {
    fn index(&self) -> usize {
        self.0
    }
}

fn t(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

fn lease(units: u64) -> Arc<LocalLease> {
    Arc::new(LocalLease::new(
        LeaseGrant {
            lease_id: LeaseId(1),
            account_id: AccountId(1),
            fencing_token: FencingToken(1),
            units: CostUnits(units),
            expires_at: t(i64::from(u16::MAX)),
        },
        CostUnits::ZERO,
    ))
}

/// What a request path may do with a reservation once opened.
#[derive(Debug, Clone, Copy)]
enum Action {
    Commit,
    Cancel,
    Drop,
    CommitThenCancel,
    CancelThenCommit,
}

proptest! {
    /// Quotes match exact u128 arithmetic or refuse with Overflow — never a
    /// wrapped value (INVARIANTS.md #11).
    #[test]
    fn quote_never_wraps(
        fixed in any::<u64>(),
        minimum in any::<u64>(),
        weight in any::<u64>(),
        items in any::<u64>(),
    ) {
        let table = CostTable::builder(CostUnits(fixed), CostUnits(minimum))
            .weight(&Op(0), CostUnits(weight))
            .build();
        let exact = u128::from(fixed) + u128::from(weight) * u128::from(items);
        match table.quote(&Op(0), items) {
            Ok(quote) => {
                prop_assert!(exact <= u128::from(u64::MAX));
                let expected = (exact as u64).max(minimum);
                prop_assert_eq!(quote.total, CostUnits(expected));
            }
            Err(QuoteError::Overflow) => prop_assert!(exact > u128::from(u64::MAX)),
            Err(other) => prop_assert!(false, "unexpected quote error: {:?}", other),
        }
    }

    /// Units are conserved across any sequence of reservations and outcomes:
    /// `initial == remaining + committed` once every reservation is resolved,
    /// and committed spend never exceeds the lease (INVARIANTS.md #1, #2).
    #[test]
    fn lease_units_are_conserved(
        capacity in 0u64..10_000,
        requests in proptest::collection::vec(
            (1u64..200, prop_oneof![
                Just(Action::Commit),
                Just(Action::Cancel),
                Just(Action::Drop),
                Just(Action::CommitThenCancel),
                Just(Action::CancelThenCommit),
            ]),
            0..64,
        ),
    ) {
        let l = lease(capacity);
        let mut committed_total: u64 = 0;
        for (units, action) in requests {
            let Ok(r) = Reservation::reserve(&l, CostUnits(units), t(0)) else {
                continue; // exhausted: denied, nothing debited
            };
            let committed = match action {
                Action::Commit => r.commit_at_execution_start(t(0)).is_ok(),
                Action::Cancel => {
                    prop_assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
                    false
                }
                Action::Drop => { drop(r); false }
                Action::CommitThenCancel => {
                    r.commit_at_execution_start(t(0)).unwrap();
                    prop_assert_eq!(
                        r.cancel(),
                        CancelOutcome::AlreadyCommitted { units: CostUnits(units) }
                    );
                    true
                }
                Action::CancelThenCommit => {
                    prop_assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
                    prop_assert!(r.commit_at_execution_start(t(0)).is_err());
                    false
                }
            };
            if committed {
                committed_total += units;
            }
        }
        prop_assert!(committed_total <= capacity);
        prop_assert_eq!(l.remaining(), CostUnits(capacity - committed_total));
    }
}

/// Concurrent spend across "instances" of work sharing one lease: total
/// committed never exceeds the lease and the counter balances exactly
/// (threaded companion to the sequential conservation property).
#[test]
fn concurrent_commit_conservation() {
    let capacity = 5_000u64;
    let l = lease(capacity);
    let mut handles = Vec::new();
    for worker in 0..8 {
        let l = Arc::clone(&l);
        handles.push(std::thread::spawn(move || {
            let mut committed = 0u64;
            for i in 0..500 {
                let units = 1 + ((worker + i) % 7) as u64;
                let Ok(r) = Reservation::reserve(&l, CostUnits(units), t(0)) else {
                    continue;
                };
                // Odd iterations cancel (zero charge), even ones commit.
                if i % 2 == 0 {
                    r.commit_at_execution_start(t(0)).unwrap();
                    committed += units;
                } else {
                    assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
                }
            }
            committed
        }));
    }
    let committed_total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(committed_total <= capacity);
    assert_eq!(l.remaining(), CostUnits(capacity - committed_total));
}
