//! Request-local affinity for opt-in instance-local sharding.
//!
//! The value is deliberately an affinity hint, not a CPU identity. Tokio
//! tasks may move, operating-system threads may migrate between cores, and
//! stable core identifiers are not portable. Assigning each participating OS
//! thread one process-local number gives worker-thread workloads the property
//! that matters here: their routine writes land on different cache lines.

use std::cell::Cell;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Number of instance-local shards used by hot-path state.
///
/// [`SINGLE`](Self::SINGLE) preserves the original layout and behavior.
/// Larger values are an explicit deployment choice for accounts that
/// genuinely saturate several worker threads; they trade per-account memory
/// for less cache-line sharing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalSharding {
    shards: NonZeroUsize,
    /// How to reduce a locality onto these shards, decided once here rather
    /// than at each lookup (#111).
    ///
    /// `shards - 1` when the count is a power of two, so the reduction is a
    /// mask; [`NOT_A_MASK`](Self::NOT_A_MASK) otherwise, so it is a modulo.
    /// A sentinel rather than an `Option` because this value is copied on
    /// every sharded lookup: the option is sixteen bytes and pushes the whole
    /// struct past what fits in registers, and `usize::MAX` cannot collide
    /// with a real mask, which would need `2^64` shards. The shard count is
    /// fixed for the process lifetime, which makes this a stored fact and not
    /// a test to re-run per request — and the test was not free: written as
    /// `if shards.is_power_of_two()` inside `Locality::index`, LLVM speculated
    /// both arms and emitted the 64-bit division *unconditionally*, selecting
    /// between it and the mask afterwards. Every sharded lookup in the
    /// workspace paid for a division it discarded.
    mask: usize,
}

impl LocalSharding {
    pub const SINGLE: Self = Self::new(NonZeroUsize::MIN);

    /// No mask exists for this shard count, so the reduction is a modulo.
    ///
    /// Unreachable as a real mask: `shards - 1` equals `usize::MAX` only for
    /// `2^64` shards, which no allocation can hold.
    const NOT_A_MASK: usize = usize::MAX;

    #[must_use]
    pub const fn new(shards: NonZeroUsize) -> Self {
        Self {
            shards,
            mask: if shards.get().is_power_of_two() {
                shards.get() - 1
            } else {
                Self::NOT_A_MASK
            },
        }
    }

    /// Match the host's advertised parallelism. This is a control-plane
    /// helper; it is never called from the request path.
    #[must_use]
    pub fn available_parallelism() -> Self {
        Self::new(std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN))
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.shards.get()
    }
}

impl Default for LocalSharding {
    fn default() -> Self {
        Self::SINGLE
    }
}

static NEXT_LOCALITY: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static LOCALITY: Cell<usize> = Cell::new(NEXT_LOCALITY.fetch_add(1, Ordering::Relaxed));
}

/// Opaque process-local affinity assigned once to each participating thread.
///
/// It carries no authorization or accounting meaning. Components reduce it
/// modulo their effective shard count, so one lookup can consistently select
/// the lease, rate-limit, and observability shards for a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Locality(usize);

impl Locality {
    #[inline]
    #[must_use]
    pub fn current() -> Self {
        LOCALITY.with(|locality| Self(locality.get()))
    }

    #[inline]
    #[must_use]
    pub fn index(self, sharding: LocalSharding) -> usize {
        // The choice was made when the sharding was built; this reads it.
        if sharding.mask == LocalSharding::NOT_A_MASK {
            self.0 % sharding.shards
        } else {
            self.0 & sharding.mask
        }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(value: usize) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_sharding_always_selects_the_only_shard() {
        assert_eq!(Locality::current().index(LocalSharding::SINGLE), 0);
    }

    #[test]
    fn one_thread_keeps_one_affinity() {
        assert_eq!(Locality::current(), Locality::current());
    }

    #[test]
    fn host_parallelism_helper_preserves_the_advertised_count() {
        let expected = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        assert_eq!(LocalSharding::available_parallelism().get(), expected.get());
    }

    #[test]
    fn power_of_two_and_arbitrary_counts_select_the_same_modulo_index() {
        let locality = Locality(13);
        assert_eq!(
            locality.index(LocalSharding::new(NonZeroUsize::new(8).unwrap())),
            5
        );
        assert_eq!(
            locality.index(LocalSharding::new(NonZeroUsize::new(5).unwrap())),
            3
        );
    }

    /// The stored reduction computes the same index the modulo always did,
    /// for every shard count and on both arms (#111).
    ///
    /// The mask is an optimisation of `% shards`, so the definition it has to
    /// agree with is `% shards` — stated here against the arithmetic rather
    /// than against a table of expected answers, which would only pin the
    /// examples someone thought to write down.
    #[test]
    fn a_masked_reduction_agrees_with_the_modulo_it_replaces() {
        for shards in 1..=64usize {
            let sharding = LocalSharding::new(NonZeroUsize::new(shards).unwrap());
            assert_eq!(sharding.get(), shards, "the count itself must not move");
            for value in [0usize, 1, 7, 13, 64, 255, 4_096, usize::MAX - 1, usize::MAX] {
                assert_eq!(
                    Locality(value).index(sharding),
                    value % shards,
                    "{value} on {shards} shards"
                );
                assert!(Locality(value).index(sharding) < shards);
            }
        }
    }

    /// Both arms exist and each is taken by the counts it is for.
    ///
    /// Without this the masked path could quietly become the only one — or
    /// stop being taken at all — while every index above still agreed, since
    /// the two arms are defined to produce the same answer.
    #[test]
    fn a_power_of_two_count_masks_and_any_other_divides() {
        for shards in [1usize, 2, 4, 8, 16, 1_024] {
            let sharding = LocalSharding::new(NonZeroUsize::new(shards).unwrap());
            assert_eq!(
                sharding.mask,
                shards - 1,
                "{shards} is a power of two and must reduce by mask"
            );
            assert_ne!(sharding.mask, LocalSharding::NOT_A_MASK);
        }
        for shards in [3usize, 5, 6, 7, 10, 100] {
            let sharding = LocalSharding::new(NonZeroUsize::new(shards).unwrap());
            assert_eq!(
                sharding.mask,
                LocalSharding::NOT_A_MASK,
                "{shards} is not a power of two and must reduce by modulo"
            );
        }
    }

    /// The sentinel cannot collide with a real mask.
    ///
    /// `shards - 1` reaches `usize::MAX` only at `2^64` shards, which
    /// `NonZeroUsize` can express and no allocation can hold — so the encoding
    /// is safe by arithmetic, and this says so rather than leaving it to the
    /// comment.
    #[test]
    fn the_modulo_sentinel_is_not_a_reachable_mask() {
        // The largest power of two a `usize` can hold, and the largest shard
        // count anything could allocate for.
        let largest = NonZeroUsize::new(1usize << (usize::BITS - 1)).unwrap();
        let sharding = LocalSharding::new(largest);
        assert_ne!(sharding.mask, LocalSharding::NOT_A_MASK);
        assert_eq!(sharding.mask, largest.get() - 1);

        // And `SINGLE` masks with zero, which is what makes every locality
        // select shard zero without a division.
        assert_eq!(LocalSharding::SINGLE.mask, 0);
        assert_eq!(LocalSharding::SINGLE.get(), 1);
    }

    /// Equality still means "the same sharding", now that a derived field
    /// rides along with the count.
    #[test]
    fn shardings_compare_by_the_count_they_were_built_from() {
        let four = LocalSharding::new(NonZeroUsize::new(4).unwrap());
        assert_eq!(four, LocalSharding::new(NonZeroUsize::new(4).unwrap()));
        assert_ne!(four, LocalSharding::new(NonZeroUsize::new(5).unwrap()));
        assert_eq!(LocalSharding::SINGLE, LocalSharding::default());
    }
}
