use jiff::{SignedDuration, Timestamp};
use proptest::prelude::*;
use tollgate_store::GrantPolicy;

// The oracle uses exact i128 nanoseconds. The full Timestamp domain plus
// SignedDuration::MAX is far below i128::MAX; it does not call Jiff arithmetic.
proptest! {
    #[test]
    fn reclaim_cutoff_matches_an_independent_nanosecond_oracle(
        now in Timestamp::MIN.as_nanosecond()..=Timestamp::MAX.as_nanosecond(),
        expiry in Timestamp::MIN.as_nanosecond()..=Timestamp::MAX.as_nanosecond(),
        grace_secs in prop_oneof![3 => 0..=1_000_000_000_000_i64, 1 => 0..=i64::MAX],
        grace_nanos in 0..1_000_000_000_i32,
    ) {
        let timestamp = |n: i128| Timestamp::new((n / 1_000_000_000) as i64,
            (n % 1_000_000_000) as i32).unwrap();
        let grace = SignedDuration::new(grace_secs, grace_nanos);
        let policy = GrantPolicy { reclaim_grace: grace, ..GrantPolicy::default() };
        let cutoff = policy.reclaim_cutoff(timestamp(now));
        let exact_cutoff = now - (i128::from(grace_secs) * 1_000_000_000 + i128::from(grace_nanos));
        prop_assert_eq!(cutoff.map(Timestamp::as_nanosecond),
            (exact_cutoff >= Timestamp::MIN.as_nanosecond()).then_some(exact_cutoff));
        prop_assert_eq!(cutoff.is_some_and(|c| timestamp(expiry) <= c), expiry <= exact_cutoff);
    }
}

#[test]
fn reclaim_cutoff_preserves_single_nanosecond_boundaries() {
    for now in [Timestamp::MIN, Timestamp::UNIX_EPOCH, Timestamp::MAX] {
        let policy = GrantPolicy {
            reclaim_grace: SignedDuration::ZERO,
            ..GrantPolicy::default()
        };
        assert_eq!(policy.reclaim_cutoff(now), Some(now));
        let policy = GrantPolicy {
            reclaim_grace: SignedDuration::from_nanos(1),
            ..policy
        };
        let cutoff = policy.reclaim_cutoff(now);
        if now == Timestamp::MIN {
            assert_eq!(cutoff, None);
        } else {
            assert_eq!(cutoff.unwrap().as_nanosecond(), now.as_nanosecond() - 1);
        }
    }
}
