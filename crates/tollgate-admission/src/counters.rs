//! What the request path is allowed to say about itself.
//!
//! A denial returns a [`DenyReason`] to its caller and is then gone: an
//! instance serving no traffic and one refusing every request look identical
//! from outside. The control plane closes that gap with structured events,
//! but this plane cannot — INVARIANTS.md #5 forbids I/O, locks and clock
//! reads on the request path, and a logging call is all three. What remains
//! affordable is a counter: no allocation, no formatting, no branch beyond
//! the one the pipeline already took.
//!
//! [`DenyReason`] is a closed enum, so the tally is a fixed array indexed by
//! [`DenyReason::index`] — never a map, never a string key. That is what
//! bounds both the cost (a direct index) and the cardinality (fourteen series,
//! whatever the traffic).
//!
//! Scope is per engine instance, like the limiter registry in
//! [`crate::state`]: these count what *this* process admitted and refused, and
//! a fleet view is the scrape's job to aggregate.

use std::sync::atomic::{AtomicU64, Ordering};

use tollgate_core::{CostUnits, DenyReason};

/// One counter on its own cache line.
///
/// Without the padding the whole array shares a handful of lines, so eight
/// cores counting eight *different* reasons would serialise on the same line
/// for no reason at all — the classic false-sharing tax. 64 bytes is the line
/// size on both targets that matter here (x86-64 and aarch64); being wrong
/// about it costs a little memory and never correctness.
#[repr(align(64))]
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
    denials: [Padded; DenyReason::COUNT],
}

impl AdmissionCounters {
    #[must_use]
    pub const fn new() -> Self {
        AdmissionCounters {
            admitted: Padded::zero(),
            units_admitted: Padded::zero(),
            denials: [const { Padded::zero() }; DenyReason::COUNT],
        }
    }

    /// Record an admitted request and the units it was quoted.
    #[inline]
    pub fn record_admit(&self, units: CostUnits) {
        self.admitted.bump(1);
        self.units_admitted.bump(units.get());
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
    pub fn snapshot(&self) -> CountersSnapshot {
        CountersSnapshot {
            admitted: self.admitted.get(),
            units_admitted: self.units_admitted.get(),
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
        assert_eq!(align_of::<Padded>(), 64);
        assert_eq!(size_of::<Padded>(), 64);
        let counters = AdmissionCounters::new();
        let first = std::ptr::from_ref(&counters.denials[0]).addr();
        let second = std::ptr::from_ref(&counters.denials[1]).addr();
        assert_eq!(second - first, 64, "adjacent slots must not share a line");
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

    /// Concurrent increments must not lose updates — the one guarantee
    /// `Relaxed` still owes us.
    #[test]
    fn concurrent_increments_are_not_lost() {
        let counters = AdmissionCounters::new();
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
