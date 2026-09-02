//! Direct index-addressed cost table, compiled once at startup.
//!
//! Consumers define an operation enum, implement [`OpIndex`] for it, and build
//! a [`CostTable`] from their pricing schedule during service startup. From
//! then on a quote is two array loads and checked integer arithmetic — no
//! search, no strings, no hashing.

use core::fmt;

use crate::units::CostUnits;

/// Maps a consumer operation onto a dense table index.
///
/// Implementations must be a pure function of `self` and return stable, small
/// indices (typically `enum as usize`). The table is sized to the largest
/// index registered at build time; quoting an unregistered index denies.
pub trait OpIndex {
    fn index(&self) -> usize;
}

impl<O: OpIndex + ?Sized> OpIndex for &O {
    #[inline]
    fn index(&self) -> usize {
        (**self).index()
    }
}

/// One quoted request: the committed price if execution starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CostQuote {
    /// Total units to reserve: `max(fixed + per_item * items, minimum)`.
    pub total: CostUnits,
    /// The fixed component actually applied (charged once per request).
    pub fixed: CostUnits,
    /// The variable component (`per_item * items`).
    pub variable: CostUnits,
}

/// Why a quote was refused. Refusals happen before any reservation, so they
/// always charge zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum QuoteError {
    /// The caller supplied no nonzero work.
    EmptyWorkload,
    /// The operation's index was never registered in this table.
    UnknownOperation { index: usize },
    /// `fixed + per_item * items` exceeded `u64` (INVARIANTS.md #11).
    Overflow,
}

impl fmt::Display for QuoteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuoteError::EmptyWorkload => f.write_str("workload is empty"),
            QuoteError::UnknownOperation { index } => {
                write!(f, "operation index {index} is not in the cost table")
            }
            QuoteError::Overflow => f.write_str("cost quote overflowed"),
        }
    }
}

/// Immutable, densely indexed pricing schedule.
///
/// `weights[op.index()]` is the per-item cost; `None` marks an index inside
/// the table's bounds that was never registered. The struct is built once and
/// shared (`Arc<CostTable>`) for the life of a policy generation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CostTable {
    fixed_request: CostUnits,
    minimum_charge: CostUnits,
    weights: Box<[Option<CostUnits>]>,
}

impl CostTable {
    #[must_use]
    pub fn builder(fixed_request: CostUnits, minimum_charge: CostUnits) -> CostTableBuilder {
        CostTableBuilder {
            fixed_request,
            minimum_charge,
            weights: Vec::new(),
        }
    }

    /// Quote `items` items of `op` as the homogeneous one-entry workload.
    #[inline]
    pub fn quote(&self, op: &impl OpIndex, items: u64) -> Result<CostQuote, QuoteError> {
        self.quote_workload(&[(op, items)]).map(|(quote, _)| quote)
    }

    /// Quote caller-owned class aggregates, applying the fixed and minimum
    /// terms exactly once. Pure, allocation-free, and O(entries).
    #[inline]
    pub fn quote_workload<O: OpIndex>(
        &self,
        workload: &[(O, u64)],
    ) -> Result<(CostQuote, u64), QuoteError> {
        let mut items = 0_u64;
        let mut variable = CostUnits::ZERO;
        for (op, count) in workload {
            if *count == 0 {
                continue;
            }
            items = items.checked_add(*count).ok_or(QuoteError::Overflow)?;
            let index = op.index();
            let per_item = match self.weights.get(index) {
                Some(Some(weight)) => *weight,
                _ => return Err(QuoteError::UnknownOperation { index }),
            };
            let entry = per_item.checked_mul(*count).ok_or(QuoteError::Overflow)?;
            variable = variable.checked_add(entry).ok_or(QuoteError::Overflow)?;
        }
        if items == 0 {
            return Err(QuoteError::EmptyWorkload);
        }
        let subtotal = self
            .fixed_request
            .checked_add(variable)
            .ok_or(QuoteError::Overflow)?;
        Ok((
            CostQuote {
                total: subtotal.max(self.minimum_charge),
                fixed: self.fixed_request,
                variable,
            },
            items,
        ))
    }

    /// Quote one already-resolved weight. Keeping the arithmetic here makes
    /// publication-time validation and request-time quoting share the exact
    /// checked formula rather than maintaining parallel implementations.
    #[inline]
    pub(crate) fn quote_weight(
        &self,
        per_item: CostUnits,
        items: u64,
    ) -> Result<CostQuote, QuoteError> {
        let variable = per_item.checked_mul(items).ok_or(QuoteError::Overflow)?;
        let subtotal = self
            .fixed_request
            .checked_add(variable)
            .ok_or(QuoteError::Overflow)?;
        Ok(CostQuote {
            total: subtotal.max(self.minimum_charge),
            fixed: self.fixed_request,
            variable,
        })
    }

    /// The largest registered per-item weight and its operation index.
    ///
    /// This O(n) scan is used only while validating a compiled snapshot for
    /// publication. Request-time lookup remains direct-indexed and O(1).
    pub(crate) fn maximum_weight(&self) -> Option<(usize, CostUnits)> {
        let mut maximum = None;
        for (index, weight) in self.weights.iter().enumerate() {
            let Some(weight) = *weight else {
                continue;
            };
            if maximum.is_none_or(|(_, current)| weight > current) {
                maximum = Some((index, weight));
            }
        }
        maximum
    }

    #[must_use]
    pub fn fixed_request(&self) -> CostUnits {
        self.fixed_request
    }

    #[must_use]
    pub fn minimum_charge(&self) -> CostUnits {
        self.minimum_charge
    }
}

/// Startup-time builder; the only path to a [`CostTable`].
#[derive(Debug, Clone)]
pub struct CostTableBuilder {
    fixed_request: CostUnits,
    minimum_charge: CostUnits,
    weights: Vec<Option<CostUnits>>,
}

impl CostTableBuilder {
    /// Register the per-item weight for one operation. Registering the same
    /// index twice keeps the last value; that is a configuration authoring
    /// concern, not a runtime one.
    #[must_use]
    pub fn weight(mut self, op: &impl OpIndex, per_item: CostUnits) -> Self {
        let index = op.index();
        if index >= self.weights.len() {
            self.weights.resize(index + 1, None);
        }
        self.weights[index] = Some(per_item);
        self
    }

    #[must_use]
    pub fn build(self) -> CostTable {
        CostTable {
            fixed_request: self.fixed_request,
            minimum_charge: self.minimum_charge,
            weights: self.weights.into_boxed_slice(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    enum Op {
        Price,
        Greeks,
        Unpriced,
    }

    impl OpIndex for Op {
        fn index(&self) -> usize {
            *self as usize
        }
    }

    fn table() -> CostTable {
        CostTable::builder(CostUnits(50), CostUnits(50))
            .weight(&Op::Price, CostUnits(1))
            .weight(&Op::Greeks, CostUnits(5))
            .build()
    }

    #[test]
    fn quote_is_fixed_plus_weighted_items() {
        let q = table().quote(&Op::Greeks, 10).unwrap();
        assert_eq!(q.total, CostUnits(100));
        assert_eq!(q.fixed, CostUnits(50));
        assert_eq!(q.variable, CostUnits(50));
    }

    #[test]
    fn minimum_charge_applies() {
        let t = CostTable::builder(CostUnits(0), CostUnits(25))
            .weight(&Op::Price, CostUnits(1))
            .build();
        assert_eq!(t.quote(&Op::Price, 3).unwrap().total, CostUnits(25));
    }

    #[test]
    fn workload_applies_fixed_once_and_sums_repeated_classes() {
        let quote = table()
            .quote_workload(&[(Op::Price, 2), (Op::Greeks, 3), (Op::Price, 4)])
            .unwrap();
        assert_eq!(quote.1, 9);
        assert_eq!(quote.0.fixed, CostUnits(50));
        assert_eq!(quote.0.variable, CostUnits(21));
        assert_eq!(quote.0.total, CostUnits(71));
    }

    #[test]
    fn empty_or_all_zero_workload_is_refused() {
        assert_eq!(
            table().quote_workload::<Op>(&[]),
            Err(QuoteError::EmptyWorkload)
        );
        assert_eq!(
            table().quote_workload(&[(Op::Price, 0), (Op::Greeks, 0)]),
            Err(QuoteError::EmptyWorkload)
        );
        assert_eq!(table().quote(&Op::Price, 0), Err(QuoteError::EmptyWorkload));
    }

    #[test]
    fn workload_checks_the_item_sum_and_variable_sum() {
        assert_eq!(
            table().quote_workload(&[(Op::Price, u64::MAX), (Op::Price, 1)]),
            Err(QuoteError::Overflow)
        );
        let overflowing = CostTable::builder(CostUnits::ZERO, CostUnits::ZERO)
            .weight(&Op::Price, CostUnits(u64::MAX))
            .weight(&Op::Greeks, CostUnits(1))
            .build();
        assert_eq!(
            overflowing.quote_workload(&[(Op::Price, 1), (Op::Greeks, 1)]),
            Err(QuoteError::Overflow)
        );
    }

    #[test]
    fn unregistered_operation_denies() {
        assert_eq!(
            table().quote(&Op::Unpriced, 1),
            Err(QuoteError::UnknownOperation { index: 2 })
        );
    }

    #[test]
    fn overflow_denies_instead_of_wrapping() {
        let t = CostTable::builder(CostUnits(1), CostUnits(0))
            .weight(&Op::Price, CostUnits(u64::MAX))
            .build();
        assert_eq!(t.quote(&Op::Price, 2), Err(QuoteError::Overflow));
        assert_eq!(t.quote(&Op::Price, u64::MAX), Err(QuoteError::Overflow));
    }

    /// The two accessors are how an embedder reads a compiled schedule back —
    /// to display pricing, or to reconcile a charge — and nothing asserted
    /// they report the schedule that is actually applied. Both mutated to
    /// `CostUnits(0)` without a single failure (#43).
    ///
    /// So this asserts agreement rather than the stored values alone: what
    /// `fixed_request()` reports is the fixed component a quote charges, and
    /// what `minimum_charge()` reports is the floor a quote is raised to.
    #[test]
    fn the_accessors_report_the_schedule_a_quote_applies() {
        let table = CostTable::builder(CostUnits(50), CostUnits(80))
            .weight(&Op::Price, CostUnits(1))
            .build();
        assert_eq!(table.fixed_request(), CostUnits(50));
        assert_eq!(table.minimum_charge(), CostUnits(80));

        // Above the floor: the quote's fixed component is exactly what
        // `fixed_request()` advertises.
        let priced = table.quote(&Op::Price, 100).unwrap();
        assert_eq!(priced.fixed, table.fixed_request());
        assert_eq!(priced.total, CostUnits(150));

        // Below the floor: the total is exactly what `minimum_charge()`
        // advertises, so an embedder quoting from the accessor and the engine
        // charging from the table cannot disagree.
        let floored = table.quote(&Op::Price, 1).unwrap();
        assert_eq!(floored.total, table.minimum_charge());
    }
}
