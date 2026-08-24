//! Property tests for the hot-path invariants (INVARIANTS.md #1–#3, #11).

use std::sync::Arc;

use jiff::Timestamp;
use proptest::prelude::*;

use tollgate_core::{
    AccountId, AccountSnapshot, AccountStatus, CancelOutcome, CostTable, CostUnits, FencingToken,
    Generation, LeaseGrant, LeaseId, LocalLease, OpIndex, PermissionBits, PublishableSnapshot,
    QuoteError, Reservation, ResolvedLimits, SnapshotValidationError,
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
    /// The complete u128 domain has one fixed-width textual representation;
    /// parsing never narrows through a JavaScript-sized integer
    /// (INVARIANTS.md #21).
    #[test]
    fn identifier_text_round_trips_the_full_u128_domain(value in any::<u128>()) {
        let id = AccountId(value);
        let text = id.to_string();
        prop_assert_eq!(text.len(), 32);
        prop_assert!(text.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        prop_assert_eq!(text.parse::<AccountId>(), Ok(id));
    }

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

    /// Snapshot publication agrees with an independent exact-arithmetic
    /// oracle for the worst registered operation at the configured batch cap
    /// (INVARIANTS.md #16).
    #[test]
    fn snapshot_publication_matches_u128_worst_case_oracle(
        fixed in any::<u64>(),
        minimum in any::<u64>(),
        weights in proptest::collection::vec(proptest::option::of(any::<u64>()), 0..16),
        max_items in any::<u64>(),
        burst in any::<u64>(),
    ) {
        let mut builder = CostTable::builder(CostUnits(fixed), CostUnits(minimum));
        for (index, weight) in weights.iter().enumerate() {
            if let Some(weight) = weight {
                builder = builder.weight(&Op(index), CostUnits(*weight));
            }
        }
        let snapshot = Arc::new(AccountSnapshot {
            account_id: AccountId(1),
            key_id: None,
            generation: Generation(1),
            status: AccountStatus::Active,
            valid_until: t(10_000),
            permissions: PermissionBits::ALL,
            limits: ResolvedLimits {
                max_items_per_request: max_items,
                rate_units_per_second: 1,
                rate_burst_units: burst,
            },
            cost_table: Arc::new(builder.build()),
        });

        let Some(max_weight) = weights.iter().flatten().max().copied() else {
            prop_assert!(PublishableSnapshot::try_new(snapshot).is_ok());
            return Ok(());
        };
        let exact = u128::from(fixed) + u128::from(max_weight) * u128::from(max_items);
        let result = PublishableSnapshot::try_new(snapshot);

        if exact > u128::from(u64::MAX) {
            let is_overflow = matches!(
                result,
                Err(SnapshotValidationError::QuoteOverflow { .. })
            );
            prop_assert!(is_overflow);
        } else if (exact as u64).max(minimum) > burst {
            let exceeds_burst = matches!(
                result,
                Err(SnapshotValidationError::QuoteExceedsBurst { .. })
            );
            prop_assert!(exceeds_burst);
        } else {
            prop_assert!(result.is_ok());
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
