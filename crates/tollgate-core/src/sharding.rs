//! Request-local affinity for opt-in instance-local sharding.
//!
//! The value is deliberately an affinity hint, not a CPU identity. Tokio
//! tasks may move, operating-system threads may migrate between cores, and
//! stable core identifiers are not portable. Assigning each participating OS
//! thread one process-local number gives worker-thread workloads the property
//! that matters here: a thread's routine writes land on the *same* cache
//! lines every time.
//!
//! Whether they land on lines *no other thread writes* is a separate claim,
//! and this module does not make it (GL-124). Affinities come from one
//! process-global counter shared by every component and every thread that
//! reaches one, and [`Locality::index`] reduces them onto each component's own
//! shard count — so two threads whose numbers are congruent modulo that count
//! share every sharded structure they touch. The counter is never recycled, so
//! a thread that took a number and exited keeps pushing the live ones apart.
//!
//! Distinctness therefore holds while the affinities handed out do not
//! outnumber the shards, which is a property of the deployment rather than one
//! this module can enforce: it does not choose how many threads serve requests,
//! and cannot know which of them still exist. What it can do is report, and
//! [`LocalSharding::occupancy`] is that report.

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
    /// than at each lookup (GL-111).
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

    /// How this layout is holding up against the affinities handed out.
    ///
    /// Metrics only, like [`CapacityOccupancy`]: no decision reads it, and it
    /// is a control-plane call rather than a request-path one.
    ///
    /// [`CapacityOccupancy`]: https://docs.rs/tollgate-admission
    #[must_use]
    pub fn occupancy(self) -> ShardOccupancy {
        ShardOccupancy {
            shards: self.get(),
            affinities_assigned: Locality::assigned(),
        }
    }
}

/// What an instance's shard layout is actually carrying.
///
/// Counts only. There is no per-shard breakdown and no thread identity,
/// because neither is knowable: affinities are never recycled, so the process
/// can say how many it handed out but not which threads still hold them.
///
/// That is enough to answer the question an operator has. Affinities come from
/// one `fetch_add`, so `affinities_assigned` of `n` means exactly the values
/// `0..n` were handed out, and reducing those onto `shards` is arithmetic
/// rather than estimation — see [`crowded_shards`](Self::crowded_shards).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardOccupancy {
    /// The effective shard count this instance runs.
    pub shards: usize,
    /// How many affinities the *process* has handed out, across every
    /// component and every thread that ever reached one — not how many threads
    /// are alive, and not how many serve requests.
    pub affinities_assigned: usize,
}

impl ShardOccupancy {
    /// How many shards carry more than one affinity.
    ///
    /// The values handed out are `0..affinities_assigned`, so shard `i` carries
    /// every `j` below that bound with `j % shards == i`. Each shard therefore
    /// carries either `n / shards` or one more than that, and the ones carrying
    /// more are the first `n % shards`. Counting the shards left above one
    /// collapses to the expression below, which is why this is exact and not a
    /// sample.
    #[must_use]
    pub fn crowded_shards(self) -> usize {
        self.shards
            .min(self.affinities_assigned.saturating_sub(self.shards))
    }

    /// Whether any shard carries more than one affinity.
    ///
    /// True means this instance has handed out more affinities than it has
    /// shards, so some threads provably share sharded state — the contention
    /// the layout was enabled to remove, looking exactly like ordinary load.
    /// It does not mean two *live request-serving* threads collided: an
    /// affinity a departed thread took still counts, because it still displaces
    /// the ones that came after it.
    #[must_use]
    pub fn is_crowded(self) -> bool {
        self.affinities_assigned > self.shards
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
    /// The affinity a control-plane read uses.
    ///
    /// Reading published state is not request work and must not spend a
    /// number: every affinity the control plane takes displaces a
    /// request-serving thread onto a shard one of its peers already holds, and
    /// `SnapshotManager` observing its own publications is exactly how that
    /// happened (GL-124).
    ///
    /// It aliases shard zero under every layout, deliberately. A reader that
    /// wants the generation, the validity bound or the presence of an entry
    /// gets the same answer from any shard, so there is nothing to choose
    /// between them — and a constant cannot drift the way "whichever number
    /// this thread happens to hold" does.
    pub const OBSERVER: Self = Self(0);

    #[inline]
    #[must_use]
    pub fn current() -> Self {
        LOCALITY.with(|locality| Self(locality.get()))
    }

    /// How many affinities this process has handed out.
    ///
    /// Control plane only — no policy decision reads it, and it is relaxed
    /// because it answers "roughly how crowded is this instance", never
    /// "which shard is this request on".
    #[must_use]
    pub fn assigned() -> usize {
        NEXT_LOCALITY.load(Ordering::Relaxed)
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
#[path = "../tests/support/isolated.rs"]
mod isolated;

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
    /// for every shard count and on both arms (GL-111).
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

    /// Counted against the residues the counter actually produces, not against
    /// a table of expected answers.
    ///
    /// `crowded_shards` is a closed form for "how many shards receive more
    /// than one of `0..n`", so the thing it has to agree with is that tally —
    /// written out here the slow way for every shard count and every load up
    /// to three times it, including the boundaries on either side of `n ==
    /// shards` where the answer turns over.
    #[test]
    fn crowded_shards_counts_the_residues_the_counter_hands_out() {
        for shards in 1..=16usize {
            let sharding = LocalSharding::new(NonZeroUsize::new(shards).unwrap());
            for assigned in 0..=(3 * shards) {
                let occupancy = ShardOccupancy {
                    shards,
                    affinities_assigned: assigned,
                };
                let mut carried = vec![0usize; shards];
                for affinity in 0..assigned {
                    carried[Locality(affinity).index(sharding)] += 1;
                }
                let expected = carried.iter().filter(|held| **held > 1).count();
                assert_eq!(
                    occupancy.crowded_shards(),
                    expected,
                    "{assigned} affinities on {shards} shard(s) crowd {expected} of them"
                );
                assert_eq!(
                    occupancy.is_crowded(),
                    expected > 0,
                    "{assigned} on {shards}: crowding must agree with the count"
                );
                assert!(occupancy.crowded_shards() <= shards);
            }
        }
    }

    /// The case the layout is bought for, stated on its own so a regression
    /// that crowded *every* instance could not hide inside the sweep above.
    #[test]
    fn a_layout_with_room_reports_every_affinity_distinct() {
        for shards in 1..=16usize {
            let sharding = LocalSharding::new(NonZeroUsize::new(shards).unwrap());
            for assigned in 0..=shards {
                let occupancy = ShardOccupancy {
                    shards,
                    affinities_assigned: assigned,
                };
                assert!(
                    !occupancy.is_crowded(),
                    "{assigned} affinities fit {shards} shard(s)"
                );
                assert_eq!(occupancy.crowded_shards(), 0);
            }
            // And one past the count is the first crowding, whatever the size.
            let over = ShardOccupancy {
                shards,
                affinities_assigned: shards + 1,
            };
            assert!(over.is_crowded());
            assert_eq!(over.crowded_shards(), 1);
            assert_eq!(sharding.get(), shards);
        }
    }

    /// Reading the report is not itself a claim on an affinity.
    ///
    /// Isolated from other tests' claims, and read on an untouched thread:
    /// even one accidental first-use claim must fail this witness.
    #[test]
    fn occupancy_reports_the_counter_without_consuming_from_it() {
        if isolated::rerun_in_child() {
            return;
        }
        let sharding = LocalSharding::new(NonZeroUsize::new(4).unwrap());
        assert_eq!(Locality::assigned(), 0);
        for expected in 0..=5 {
            // The reporting thread never claims an affinity, even after
            // other threads have advanced the live counter past crowding.
            for _ in 0..2 {
                let now = sharding.occupancy();
                assert_eq!(now.shards, 4);
                assert_eq!(now.affinities_assigned, expected, "the counter is live");
                assert_eq!(Locality::assigned(), expected, "reporting spends nothing");
            }
            std::thread::spawn(Locality::current).join().unwrap();
        }
    }

    /// The observer affinity is a constant, spends nothing, and lands on the
    /// same shard under every layout.
    #[test]
    fn the_observer_affinity_costs_nothing_and_never_moves() {
        if isolated::rerun_in_child() {
            return;
        }
        let before = Locality::assigned();
        assert_eq!(before, 0, "the thread has never claimed an affinity");
        for shards in 1..=16usize {
            let sharding = LocalSharding::new(NonZeroUsize::new(shards).unwrap());
            for _ in 0..64 {
                assert_eq!(Locality::OBSERVER.index(sharding), 0, "{shards} shards");
            }
        }
        assert_eq!(Locality::assigned(), before, "observing spends nothing");
    }
}
