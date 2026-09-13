//! Exact lease instants as an indexable PostgreSQL integer pair.
use jiff::Timestamp;
use tollgate_store::StoreError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LeaseInstant {
    pub micros: i64,
    pub submicro_nanos: i16,
}

impl From<Timestamp> for LeaseInstant {
    fn from(timestamp: Timestamp) -> Self {
        let nanos = timestamp.as_nanosecond();
        Self {
            // Timestamp's complete domain fits i64 microseconds. Euclidean
            // division keeps the remainder nonnegative before the epoch too.
            micros: nanos.div_euclid(1_000) as i64,
            submicro_nanos: nanos.rem_euclid(1_000) as i16,
        }
    }
}

impl LeaseInstant {
    pub fn timestamp(self) -> Result<Timestamp, StoreError> {
        if !(0..1_000).contains(&self.submicro_nanos) {
            return Err(StoreError(
                "invalid stored lease nanosecond remainder".into(),
            ));
        }
        // The seconds constructor includes Timestamp::MAX's final fraction.
        Timestamp::new(
            self.micros.div_euclid(1_000_000),
            (self.micros.rem_euclid(1_000_000) * 1_000 + i64::from(self.submicro_nanos)) as i32,
        )
        .map_err(|_| StoreError("stored lease expiry exceeds the timestamp domain".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn lease_instants_preserve_epoch_edges_and_the_full_timestamp_domain() {
        for timestamp in [
            Timestamp::MIN,
            Timestamp::MAX,
            Timestamp::UNIX_EPOCH,
            Timestamp::new(0, -1).unwrap(),
            Timestamp::new(0, 1).unwrap(),
            Timestamp::new(-1, -999_999_999).unwrap(),
        ] {
            assert_eq!(
                LeaseInstant::from(timestamp).timestamp().unwrap(),
                timestamp
            );
        }
        for invalid in [
            LeaseInstant {
                micros: 0,
                submicro_nanos: -1,
            },
            LeaseInstant {
                micros: 0,
                submicro_nanos: 1_000,
            },
            LeaseInstant {
                micros: i64::MIN,
                submicro_nanos: 0,
            },
            LeaseInstant {
                micros: i64::MAX,
                submicro_nanos: 999,
            },
        ] {
            assert!(invalid.timestamp().is_err());
        }
    }

    proptest! {
        #[test]
        fn durable_pairs_round_trip_and_preserve_order(
            a in Timestamp::MIN.as_nanosecond()..=Timestamp::MAX.as_nanosecond(),
            b in Timestamp::MIN.as_nanosecond()..=Timestamp::MAX.as_nanosecond(),
        ) {
            let timestamp = |n: i128| Timestamp::new((n / 1_000_000_000) as i64,
                (n % 1_000_000_000) as i32).unwrap();
            let a_pair = LeaseInstant::from(timestamp(a));
            let b_pair = LeaseInstant::from(timestamp(b));
            prop_assert_eq!(a_pair.timestamp().unwrap().as_nanosecond(), a);
            prop_assert_eq!(b_pair.timestamp().unwrap().as_nanosecond(), b);
            prop_assert_eq!(a_pair.cmp(&b_pair), a.cmp(&b));
        }
    }
}
