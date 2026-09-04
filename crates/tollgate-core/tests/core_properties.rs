//! Property tests for the hot-path invariants (INVARIANTS.md #1–#3, #11).

use std::num::NonZeroUsize;
use std::sync::Arc;

use jiff::Timestamp;
use proptest::prelude::*;

use tollgate_core::{
    AccountId, AccountOverage, AccountSnapshot, AccountStatus, CancelOutcome, CommitFunding,
    CostTable, CostUnits, DenyReason, FencingToken, Generation, LeaseGrant, LeaseId, LocalLease,
    LocalSharding, OpIndex, PermissionBits, PolicyRevision, PublishableSnapshot, QuoteError,
    RequestId, Reservation, ResolvedLimits, Retry, SnapshotValidationError, UsageSource,
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

fn sharded_lease(units: u64, shards: usize) -> Arc<LocalLease> {
    Arc::new(LocalLease::with_sharding(
        LeaseGrant {
            lease_id: LeaseId(1),
            account_id: AccountId(1),
            fencing_token: FencingToken(1),
            units: CostUnits(units),
            expires_at: t(i64::from(u16::MAX)),
        },
        CostUnits::ZERO,
        jiff::SignedDuration::ZERO,
        LocalSharding::new(NonZeroUsize::new(shards).unwrap()),
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
    /// The lease's window lapses before execution start, under `Strict`.
    LapseStrict,
    /// The same lapse under `Elastic`, which may settle against overage.
    LapseElastic,
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

    /// The same contract at the other width, over the full 256-bit domain.
    ///
    /// A consumer compares these for equality to select its own metadata, so
    /// a byte that survived as a different byte — or an ordering the text
    /// transposed — would silently name a different policy while still
    /// looking like a valid revision (INVARIANTS.md #21).
    #[test]
    fn revision_text_round_trips_the_full_256_bit_domain(bytes in any::<[u8; 32]>()) {
        let revision = PolicyRevision(bytes);
        let text = revision.to_string();
        prop_assert_eq!(text.len(), 64);
        prop_assert!(text.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        prop_assert_eq!(text.parse::<PolicyRevision>(), Ok(revision));
        prop_assert_eq!(revision.as_bytes(), &bytes);
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
        burst in 1u64..=u64::from(u32::MAX),
    ) {
        let mut builder = CostTable::builder(CostUnits(fixed), CostUnits(minimum));
        for (index, weight) in weights.iter().enumerate() {
            if let Some(weight) = weight {
                builder = builder.weight(&Op(index), CostUnits(*weight));
            }
        }
        let snapshot = Arc::new(AccountSnapshot::builder(
            AccountId(1),
            Generation(1),
            AccountStatus::Active,
            t(10_000),
            PermissionBits::ALL,
            ResolvedLimits::new(max_items).with_weighted_rate(1, burst),
            Arc::new(builder.build()),
        ).build());

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
        shards in 1usize..16,
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
        let l = sharded_lease(capacity, shards);
        let mut committed_total: u64 = 0;
        for (units, action) in requests {
            let Ok(r) = Reservation::reserve(&l, CostUnits(units), t(0)) else {
                continue; // exhausted: denied, nothing debited
            };
            let committed = match action {
                Action::Commit => r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly).is_ok(),
                Action::Cancel => {
                    prop_assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
                    false
                }
                Action::Drop => { drop(r); false }
                Action::CommitThenCancel => {
                    r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly).unwrap();
                    prop_assert_eq!(
                        r.cancel(),
                        CancelOutcome::AlreadyCommitted { units: CostUnits(units) }
                    );
                    true
                }
                Action::CancelThenCommit => {
                    prop_assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
                    prop_assert!(r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly).is_err());
                    false
                }
                // Lapse belongs to `funding_terms_are_conserved_across_lapse_and_fallback`,
                // which owns the commit-time funding transition; this property
                // does not generate them.
                Action::LapseStrict | Action::LapseElastic => unreachable!(
                    "this property generates no lapse actions"
                ),
            };
            if committed {
                committed_total += units;
            }
        }
        prop_assert!(committed_total <= capacity);
        prop_assert_eq!(l.remaining(), CostUnits(capacity - committed_total));
    }

    /// Every resolved reservation reports **exactly one** funding term, across
    /// any mix of ordinary commits, cancels, drops, and commit-time elastic
    /// fallbacks. A committed request moves its units into the lease's spend
    /// or into overage — never both, never neither — and a released one moves
    /// nothing (INVARIANTS.md #1, #2, #3).
    ///
    /// This is the double-charge exclusion as a property. The lapse actions
    /// exercise the one transition that changes a reservation's funding source
    /// after admission, which is exactly where a second funding term could
    /// appear.
    #[test]
    fn funding_terms_are_conserved_across_lapse_and_fallback(
        capacity in 0u64..10_000,
        shards in 1usize..16,
        cap in 0u64..10_000,
        requests in proptest::collection::vec(
            (1u64..200, prop_oneof![
                Just(Action::Commit),
                Just(Action::Cancel),
                Just(Action::Drop),
                Just(Action::CommitThenCancel),
                Just(Action::CancelThenCommit),
                Just(Action::LapseElastic),
                Just(Action::LapseStrict),
            ]),
            0..64,
        ),
    ) {
        let l = sharded_lease(capacity, shards);
        let overage = Arc::new(AccountOverage::new(AccountId(1)));
        let cap = CostUnits(cap);
        // Past the lease's `expires_at`, so every lapse action really lapses.
        let lapsed = t(i64::from(u16::MAX) + 1);
        let mut lease_committed: u64 = 0;
        let mut overage_committed: u64 = 0;

        for (units, action) in requests {
            let Ok(r) = Reservation::reserve(&l, CostUnits(units), t(0)) else {
                continue; // exhausted: denied, nothing debited
            };
            let elastic = CommitFunding::OverageFallback { overage: &overage, cap };

            match action {
                Action::Commit => {
                    if r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly).is_ok() {
                        lease_committed += units;
                        let source = r.usage_event(RequestId(1), t(0), PolicyRevision::UNSTATED).unwrap().source;
                        prop_assert!(
                            matches!(source, UsageSource::Leased { .. }),
                            "an unlapsed commit bills against its lease, got {:?}",
                            source
                        );
                    }
                }
                Action::Cancel => {
                    prop_assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
                }
                Action::Drop => drop(r),
                Action::CommitThenCancel => {
                    r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly).unwrap();
                    prop_assert_eq!(
                        r.cancel(),
                        CancelOutcome::AlreadyCommitted { units: CostUnits(units) }
                    );
                    lease_committed += units;
                }
                Action::CancelThenCommit => {
                    prop_assert_eq!(r.cancel(), CancelOutcome::ZeroCharged);
                    prop_assert!(r.commit_at_execution_start(lapsed, elastic).is_err());
                }
                // Strict: a lapse is always a release for zero, whatever the
                // overage counter looks like.
                Action::LapseStrict => {
                    prop_assert!(
                        r.commit_at_execution_start(lapsed, CommitFunding::LeaseOnly).is_err()
                    );
                    prop_assert!(r.usage_event(RequestId(1), lapsed, PolicyRevision::UNSTATED).is_none());
                }
                // Elastic: the charge either moves wholly to overage or is
                // released wholly; the lease receipt never funds it.
                Action::LapseElastic => {
                    if r.commit_at_execution_start(lapsed, elastic).is_ok() {
                        overage_committed += units;
                        prop_assert_eq!(
                            r.usage_event(RequestId(1), lapsed, PolicyRevision::UNSTATED).unwrap().source,
                            UsageSource::Overage,
                            "a fallback must never bill against its lapsed lease"
                        );
                    } else {
                        prop_assert!(r.usage_event(RequestId(1), lapsed, PolicyRevision::UNSTATED).is_none());
                    }
                }
            }
        }

        // The lease funded exactly its own commits: every lapse, refusal, and
        // cancellation returned its units.
        prop_assert!(lease_committed <= capacity);
        prop_assert_eq!(l.remaining(), CostUnits(capacity - lease_committed));
        // Overage funded exactly the fallbacks that won, and stayed inside
        // its cap.
        prop_assert_eq!(overage.spent(), CostUnits(overage_committed));
        prop_assert!(overage.spent() <= cap);
    }

    /// In every stable overage state, occupancy determines whether the local
    /// reason is refundable or committed saturation. Both remain transient at
    /// the admission boundary because an ordinary lease refill can fund the
    /// unchanged request.
    #[test]
    fn overage_retry_class_matches_stable_occupancy(
        cap in 0u64..10_000,
        occupied in 0u64..10_000,
        want in 0u64..10_000,
        commit in any::<bool>(),
    ) {
        prop_assume!(occupied <= cap);
        let overage = Arc::new(AccountOverage::new(AccountId(1)));
        let held = Reservation::reserve_overage(
            &overage,
            CostUnits(occupied),
            CostUnits(cap),
        )
        .unwrap();
        if commit {
            held.commit_at_execution_start(t(0), CommitFunding::LeaseOnly).unwrap();
        }

        match Reservation::reserve_overage(&overage, CostUnits(want), CostUnits(cap)) {
            Ok(reservation) => {
                prop_assert!(occupied.checked_add(want).is_some_and(|total| total <= cap));
                drop(reservation);
            }
            Err(reason) if !commit && want <= cap => {
                prop_assert!(matches!(
                    reason,
                    DenyReason::OverageCapTemporarilyExhausted { .. }
                ), "unexpected pending refusal: {:?}", reason);
                prop_assert_eq!(reason.retry(), Retry::Transient);
            }
            Err(reason) => {
                prop_assert!(
                    matches!(reason, DenyReason::OverageCapExhausted { .. }),
                    "unexpected committed refusal: {:?}",
                    reason
                );
                prop_assert_eq!(reason.retry(), Retry::Transient);
            }
        }
    }
}

/// Concurrent spend across "instances" of work sharing one lease: total
/// committed never exceeds the lease and the counter balances exactly
/// (threaded companion to the sequential conservation property).
#[test]
fn concurrent_commit_conservation() {
    let capacity = 5_000u64;
    let l = sharded_lease(capacity, 8);
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
                    r.commit_at_execution_start(t(0), CommitFunding::LeaseOnly)
                        .unwrap();
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
