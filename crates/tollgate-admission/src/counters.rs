//! What the request path is allowed to say about itself.
//!
//! A denial returns a [`DenyReason`] to its caller and is then gone: an
//! instance serving no traffic and one refusing every request look identical
//! from outside. The control plane closes that gap with structured events,
//! but this plane cannot — INVARIANTS.md #5 forbids I/O, locks and clock
//! reads on the request path, and a logging call is all three. What remains
//! affordable is a counter: no allocation, no formatting, and only the fixed
//! single-vs-sharded layout branch selected at engine construction.
//!
//! [`DenyReason`] is a closed enum, so the tally is a fixed array indexed by
//! [`DenyReason::index`] — never a map, never a string key. That is what
//! bounds both the cost (a direct index) and the cardinality
//! ([`DenyReason::COUNT`] series, whatever the traffic).
//!
//! Scope is per engine instance, like the limiter registry in
//! [`crate::state`]: these count what *this* process admitted and refused, and
//! a fleet view is the scrape's job to aggregate.

use std::sync::atomic::{AtomicU64, Ordering};

use tollgate_core::{CostUnits, DenyReason, LocalSharding, Locality};

/// One counter on its own cache line.
///
/// Without the padding the whole array shares a handful of lines, so eight
/// cores counting eight *different* reasons would serialise on the same line
/// for no reason at all — the classic false-sharing tax. Apple Silicon uses
/// 128-byte cache lines; over-aligning on 64-byte-line x86-64 is harmless.
#[repr(align(128))]
#[derive(Debug)]
struct Padded(AtomicU64);

impl Padded {
    /// A constructor rather than a `const ZERO` item: a constant holding an
    /// atomic is *copied* at each use, so incrementing through it would bump
    /// a temporary and discard the result. A function leaves no such constant
    /// to misuse, which is why clippy's `declare_interior_mutable_const` is
    /// satisfied here by construction rather than by an allow.
    const fn zero() -> Self {
        Padded(AtomicU64::new(0))
    }

    #[inline]
    fn bump(&self, by: u64) {
        // Relaxed throughout: nothing is published through these counters, so
        // no other memory needs to become visible with them. Atomicity still
        // holds — concurrent increments never lose an update — and dropping
        // the fences is what keeps the increment near-free on the hot path.
        // Matches the `Unaccounted` tally in tollgate-client.
        self.0.fetch_add(by, Ordering::Relaxed);
    }

    #[inline]
    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Per-instance admission tallies, written from the request path and read by
/// whatever exports them.
///
/// Counters are monotonic and wrap at [`u64::MAX`]. Unlike cost and lease
/// arithmetic — which is checked, because a wrapped charge is a wrong bill
/// (INVARIANTS.md #11) — a wrapped monitoring counter is not a correctness
/// event, and rate-of-change is how counters are read anyway. At a billion
/// admissions a second the `admitted` counter would need roughly 584 years to
/// reach the wrap; adding a branch on the request path to guard against it
/// would cost more than it could ever save.
#[derive(Debug)]
pub struct AdmissionCounters {
    admitted: Padded,
    units_admitted: Padded,
    admitted_overage: Padded,
    units_admitted_overage: Padded,
    denials: [Padded; DenyReason::COUNT],
    shards: Option<Box<[CounterShard]>>,
}

/// One locality's complete tally. Different localities never share a cache
/// line; counters within one locality are deliberately compact because the
/// same request updates them.
#[repr(align(128))]
#[derive(Debug)]
struct CounterShard {
    admitted: AtomicU64,
    units_admitted: AtomicU64,
    denials: [AtomicU64; DenyReason::COUNT],
}

impl CounterShard {
    fn zero() -> Self {
        Self {
            admitted: AtomicU64::new(0),
            units_admitted: AtomicU64::new(0),
            denials: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl AdmissionCounters {
    #[must_use]
    pub const fn new() -> Self {
        AdmissionCounters {
            admitted: Padded::zero(),
            units_admitted: Padded::zero(),
            admitted_overage: Padded::zero(),
            units_admitted_overage: Padded::zero(),
            denials: [const { Padded::zero() }; DenyReason::COUNT],
            shards: None,
        }
    }

    /// Create counters partitioned by the same sticky locality used by lease
    /// and rate state. One shard preserves the inline historical layout.
    #[must_use]
    pub fn with_sharding(sharding: LocalSharding) -> Self {
        if sharding == LocalSharding::SINGLE {
            return Self::new();
        }
        Self {
            admitted: Padded::zero(),
            units_admitted: Padded::zero(),
            admitted_overage: Padded::zero(),
            units_admitted_overage: Padded::zero(),
            denials: [const { Padded::zero() }; DenyReason::COUNT],
            shards: Some(
                (0..sharding.get())
                    .map(|_| CounterShard::zero())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
        }
    }

    /// Record an admitted request and the units it was quoted.
    #[inline]
    pub fn record_admit(&self, units: CostUnits) {
        self.record_admit_at(units, Locality::current());
    }

    #[inline]
    pub(crate) fn record_admit_at(&self, units: CostUnits, locality: Locality) {
        if let Some(shards) = &self.shards {
            let shard = &shards[locality.index(LocalSharding::new(
                std::num::NonZeroUsize::new(shards.len()).expect("sharded counters are non-empty"),
            ))];
            shard.admitted.fetch_add(1, Ordering::Relaxed);
            shard
                .units_admitted
                .fetch_add(units.get(), Ordering::Relaxed);
            return;
        }
        self.admitted.bump(1);
        self.units_admitted.bump(units.get());
    }

    /// Record an admission that no lease funded.
    ///
    /// A *subset* of `admitted`, not a sibling of it: the request is counted
    /// in both, so `admitted` remains the total and needs no reader to add two
    /// numbers to get it. `admitted_overage` answers a different question —
    /// how much of that total the account is being trusted for — and it is the
    /// number an operator watches climb before an invoice does.
    ///
    /// Deliberately not a `DenyReason`-style dense outcome table. There is one
    /// admit outcome plus one qualifier, and building the mirror of
    /// [`DenyReason::index`] for two counters would add the machinery without
    /// the forcing function that justifies it there.
    ///
    /// The qualifier pair stays unsharded while `admitted` shards. Sharding
    /// buys nothing here: this path runs only when the account had no lease
    /// able to fund the quote, and it already serializes on the overage
    /// counter's own compare-exchange one step earlier. Two more relaxed
    /// bumps on that path add no contention class that the cap has not
    /// already imposed.
    #[inline]
    pub fn record_admit_overage(&self, units: CostUnits) {
        self.record_admit_overage_at(units, Locality::current());
    }

    #[inline]
    pub(crate) fn record_admit_overage_at(&self, units: CostUnits, locality: Locality) {
        self.record_admit_at(units, locality);
        self.admitted_overage.bump(1);
        self.units_admitted_overage.bump(units.get());
    }

    /// Record a refusal against its reason's slot.
    ///
    /// Public because not every refusal originates in
    /// [`AdmissionEngine::admit`](crate::AdmissionEngine::admit):
    /// `AccountingBackpressure` is decided by the embedder *before* admission
    /// (INVARIANTS.md #8 sheds on a full usage queue), so a service that
    /// sheds there records it here. A slot that could only ever read zero
    /// because nothing can reach it would be worse than absent — it would
    /// read as "this never happens".
    #[inline]
    pub fn record_deny(&self, reason: &DenyReason) {
        self.record_deny_at(reason, Locality::current());
    }

    #[inline]
    pub(crate) fn record_deny_at(&self, reason: &DenyReason, locality: Locality) {
        if let Some(shards) = &self.shards {
            let shard = &shards[locality.index(LocalSharding::new(
                std::num::NonZeroUsize::new(shards.len()).expect("sharded counters are non-empty"),
            ))];
            shard.denials[reason.index()].fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.denials[reason.index()].bump(1);
    }

    /// Read every counter.
    ///
    /// Lock-free, so the result is not a single instant: counters read later
    /// may include increments that landed after the earlier ones were read.
    /// The alternative is a lock on the request path, which INVARIANTS.md #5
    /// forbids outright — and skew between counters read microseconds apart
    /// does not survive the scrape interval that consumes them.
    #[must_use]
    /// Units quoted by every admitted request on this instance, including the
    /// overage ones (`record_admit_overage` counts through `record_admit`).
    ///
    /// A focused read, because its one caller wants this number and not the
    /// twenty-odd in [`snapshot`](Self::snapshot): the balance estimate
    /// subtracts it from what the ledger last reported, and building a whole
    /// `CountersSnapshot` to reach one field would walk every denial slot as
    /// well. Wrapping-summed across shards for the same reason `snapshot` is —
    /// see the type docs on why a monitoring counter does not use checked
    /// arithmetic.
    pub fn units_admitted(&self) -> u64 {
        match &self.shards {
            Some(shards) => shards.iter().fold(0u64, |total, shard| {
                total.wrapping_add(shard.units_admitted.load(Ordering::Relaxed))
            }),
            None => self.units_admitted.get(),
        }
    }

    pub fn snapshot(&self) -> CountersSnapshot {
        if let Some(shards) = &self.shards {
            let mut snapshot = CountersSnapshot {
                admitted: 0,
                units_admitted: 0,
                // Not summed from the shards: the overage qualifier is one
                // inline pair under every layout, because the path that bumps
                // it is the one with no lease to shard.
                admitted_overage: self.admitted_overage.get(),
                units_admitted_overage: self.units_admitted_overage.get(),
                denials: [0; DenyReason::COUNT],
            };
            for shard in shards {
                snapshot.admitted = snapshot
                    .admitted
                    .wrapping_add(shard.admitted.load(Ordering::Relaxed));
                snapshot.units_admitted = snapshot
                    .units_admitted
                    .wrapping_add(shard.units_admitted.load(Ordering::Relaxed));
                for (total, counter) in snapshot.denials.iter_mut().zip(&shard.denials) {
                    *total = total.wrapping_add(counter.load(Ordering::Relaxed));
                }
            }
            return snapshot;
        }
        CountersSnapshot {
            admitted: self.admitted.get(),
            units_admitted: self.units_admitted.get(),
            admitted_overage: self.admitted_overage.get(),
            units_admitted_overage: self.units_admitted_overage.get(),
            denials: std::array::from_fn(|slot| self.denials[slot].get()),
        }
    }
}

impl Default for AdmissionCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// A read of [`AdmissionCounters`]: plain values, safe to serialise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CountersSnapshot {
    /// Requests admitted.
    pub admitted: u64,
    /// Units *quoted* by admitted requests.
    ///
    /// Not units billed. Admission opens a pending debit that a cancellation
    /// releases and a commit charges, and usage events — not this counter —
    /// are billing truth. Reading this as revenue would over-count every
    /// request that was admitted and then cancelled before execution.
    pub units_admitted: u64,
    /// Admitted requests that no lease funded, under
    /// [`EnforcementMode::Elastic`]. Included in `admitted`, never instead of
    /// it.
    ///
    /// [`EnforcementMode::Elastic`]: tollgate_core::EnforcementMode::Elastic
    pub admitted_overage: u64,
    /// Units quoted by those requests — unfunded credit this instance has
    /// extended, before any commit or cancellation. The billed figure is
    /// `Conservation::overage_recorded`, which lags this one and is smaller by
    /// whatever was cancelled before execution.
    pub units_admitted_overage: u64,
    /// Refusals per [`DenyReason::index`] slot.
    pub denials: [u64; DenyReason::COUNT],
}

impl CountersSnapshot {
    /// Refusals paired with their stable labels, in slot order.
    pub fn denials_by_name(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        DenyReason::NAMES
            .iter()
            .copied()
            .zip(self.denials.iter().copied())
    }

    /// Every refusal, whatever the reason.
    #[must_use]
    pub fn denied(&self) -> u64 {
        self.denials.iter().sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The padding is the whole point of the type: if it ever stops applying,
    /// counters for unrelated reasons start contending on one cache line and
    /// the cost shows up only under concurrency, where it is hardest to
    /// attribute.
    #[test]
    fn each_counter_occupies_its_own_cache_line() {
        assert_eq!(align_of::<Padded>(), 128);
        assert_eq!(size_of::<Padded>(), 128);
        let counters = AdmissionCounters::new();
        let first = std::ptr::from_ref(&counters.denials[0]).addr();
        let second = std::ptr::from_ref(&counters.denials[1]).addr();
        assert_eq!(second - first, 128, "adjacent slots must not share a line");
    }

    #[test]
    fn configured_sharding_selects_the_matching_counter_layout() {
        assert!(
            AdmissionCounters::with_sharding(LocalSharding::SINGLE)
                .shards
                .is_none()
        );
        let sharding = LocalSharding::new(std::num::NonZeroUsize::new(8).unwrap());
        assert_eq!(
            AdmissionCounters::with_sharding(sharding)
                .shards
                .as_ref()
                .map(|shards| shards.len()),
            Some(8)
        );
    }

    /// Each reason must land in its own slot and leave the rest alone.
    #[test]
    fn every_reason_tallies_separately() {
        let counters = AdmissionCounters::new();
        counters.record_deny(&DenyReason::RateLimited);
        counters.record_deny(&DenyReason::RateLimited);
        counters.record_deny(&DenyReason::LeaseExhausted {
            remaining: CostUnits(4),
        });

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.denied(), 3);
        let named: Vec<_> = snapshot
            .denials_by_name()
            .filter(|(_, count)| *count > 0)
            .collect();
        assert_eq!(named, vec![("rate_limited", 2), ("lease_exhausted", 1)]);
        assert_eq!(snapshot.admitted, 0, "a denial is not an admission");
    }

    /// Admissions accumulate units; denials must add none, since a refusal
    /// charges zero (INVARIANTS.md #5).
    #[test]
    fn admissions_accumulate_units_and_denials_do_not() {
        let counters = AdmissionCounters::new();
        counters.record_admit(CostUnits(51));
        counters.record_admit(CostUnits(64));
        counters.record_deny(&DenyReason::UnknownPrincipal);

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.admitted, 2);
        assert_eq!(snapshot.units_admitted, 115);
        assert_eq!(snapshot.denied(), 1);
    }

    /// The overage qualifier is a *subset* of `admitted`, so one call has to
    /// move both pairs. Recording it as a sibling instead would make
    /// `admitted` stop being the total, and every dashboard that reads it
    /// would quietly under-count elastic traffic.
    ///
    /// This also covers the public wrapper itself. `record_admit`,
    /// `record_deny`, and `record_admit_overage` are three
    /// `Locality::current()` wrappers over their `_at` forms; the first two
    /// are exercised by the tests above, and this one was not once the engine
    /// started calling `record_admit_overage_at` directly. A wrapper with no
    /// caller and no test is a wrapper that can be a no-op, which is what the
    /// mutation gate reported (!96).
    #[test]
    fn an_overage_admission_counts_in_both_the_total_and_the_qualifier() {
        let counters = AdmissionCounters::new();
        counters.record_admit(CostUnits(51));
        counters.record_admit_overage(CostUnits(64));

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.admitted, 2, "the qualifier is not a sibling");
        assert_eq!(snapshot.units_admitted, 115);
        assert_eq!(snapshot.admitted_overage, 1);
        assert_eq!(snapshot.units_admitted_overage, 64);
    }

    /// The qualifier stays unsharded while `admitted` shards, so a sharded
    /// snapshot has to read the two pairs from different places: `admitted`
    /// summed across shards, the qualifier from the inline counters. Reading
    /// the qualifier out of the shards — the obvious symmetry — would report
    /// zero overage on every sharded instance.
    #[test]
    fn a_sharded_snapshot_still_reports_the_unsharded_overage_qualifier() {
        let counters = AdmissionCounters::with_sharding(LocalSharding::new(
            std::num::NonZeroUsize::new(8).unwrap(),
        ));
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| counters.record_admit_overage(CostUnits(10)));
            }
        });

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.admitted, 4, "sharded totals still sum");
        assert_eq!(snapshot.units_admitted, 40);
        assert_eq!(
            snapshot.admitted_overage, 4,
            "the qualifier survives a layout that does not shard it"
        );
        assert_eq!(snapshot.units_admitted_overage, 40);
    }

    /// Concurrent increments must not lose updates — the one guarantee
    /// `Relaxed` still owes us.
    #[test]
    fn concurrent_increments_are_not_lost() {
        let counters = AdmissionCounters::with_sharding(LocalSharding::new(
            std::num::NonZeroUsize::new(8).unwrap(),
        ));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1_000 {
                        counters.record_admit(CostUnits(2));
                        counters.record_deny(&DenyReason::RateLimited);
                    }
                });
            }
        });

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.admitted, 8_000);
        assert_eq!(snapshot.units_admitted, 16_000);
        assert_eq!(snapshot.denials[DenyReason::RateLimited.index()], 8_000);
    }
}
