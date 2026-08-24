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
pub struct LocalSharding(NonZeroUsize);

impl LocalSharding {
    pub const SINGLE: Self = Self(NonZeroUsize::MIN);

    #[must_use]
    pub const fn new(shards: NonZeroUsize) -> Self {
        Self(shards)
    }

    /// Match the host's advertised parallelism. This is a control-plane
    /// helper; it is never called from the request path.
    #[must_use]
    pub fn available_parallelism() -> Self {
        Self(std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN))
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
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
        let shards = sharding.get();
        if shards.is_power_of_two() {
            self.0 & (shards - 1)
        } else {
            self.0 % shards
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
}
