//! Direct index-addressed cost table, compiled once at startup.
//!
//! Consumers define an operation enum, implement [`OpIndex`] for it, and build
//! a [`CostTable`] from their pricing schedule during service startup. From
//! then on a quote is two array loads and checked integer arithmetic — no
//! search, no strings, no hashing.

use core::fmt;

use crate::{snapshot::PermissionBits, units::CostUnits};

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
    /// Work permissions, parallel to `weights` and index-addressed the same way.
    ///
    /// Held in canonical form: trailing `NONE` entries are trimmed at build
    /// time and an entirely-`NONE` array is empty. A missing index therefore
    /// means `PermissionBits::NONE`, which is what makes a table decoded from
    /// JSON written before this field existed compare equal to the same table
    /// built today — a stored snapshot must not start denying work because the
    /// binary that reads it learned a new field.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "<[PermissionBits]>::is_empty")
    )]
    permissions: Box<[PermissionBits]>,
}

impl CostTable {
    #[must_use]
    pub fn builder(fixed_request: CostUnits, minimum_charge: CostUnits) -> CostTableBuilder {
        CostTableBuilder {
            fixed_request,
            minimum_charge,
            weights: Vec::new(),
            permissions: Vec::new(),
        }
    }

    /// Quote `items` items of `op` as the homogeneous one-entry workload.
    ///
    /// Answers the one-class case by direct index, which is what the module
    /// header promises: two array loads and checked integer arithmetic.
    ///
    /// #91 replaced that with a delegation to [`CostTable::quote_workload`],
    /// and `cost_table/quote` went 1.70 → 2.25 ns (+32%). Nothing caught it:
    /// the row's recorded baseline had been taken one commit earlier in the
    /// same issue and was never re-recorded, so the gate compared the new code
    /// against a number the new code no longer described (#114). Embedders
    /// pricing a single class call this; the admission engine folds its own
    /// workload and needs the permission bits only the fold returns, so it
    /// keeps [`CostTable::quote_workload`] and is unaffected either way.
    ///
    /// The arithmetic stays shared — `CostTable::weight_at` and
    /// `CostTable::quote_weight` are the same two steps the fold takes — and
    /// `quote_agrees_with_the_one_entry_workload` holds the two paths to the
    /// same answer rather than trusting that argument.
    #[inline]
    pub fn quote(&self, op: &impl OpIndex, items: u64) -> Result<CostQuote, QuoteError> {
        if items == 0 {
            return Err(QuoteError::EmptyWorkload);
        }
        self.quote_weight(self.weight_at(op.index())?, items)
    }

    /// Quote caller-owned class aggregates, applying the fixed and minimum
    /// terms exactly once. Pure, allocation-free, and O(entries).
    #[inline]
    pub fn quote_workload<O: OpIndex>(
        &self,
        workload: &[(O, u64)],
    ) -> Result<(CostQuote, u64, PermissionBits), QuoteError> {
        let mut items = 0_u64;
        let mut variable = CostUnits::ZERO;
        let mut required = PermissionBits::NONE;
        for (op, count) in workload {
            if *count == 0 {
                continue;
            }
            items = items.checked_add(*count).ok_or(QuoteError::Overflow)?;
            let index = op.index();
            let per_item = self.weight_at(index)?;
            let entry = per_item.checked_mul(*count).ok_or(QuoteError::Overflow)?;
            variable = variable.checked_add(entry).ok_or(QuoteError::Overflow)?;
            required = required.union(self.required_at(index));
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
            required,
        ))
    }

    /// The per-item weight of a class, or `UnknownOperation` for a class the
    /// table does not price. Shared so the one-class path and the workload
    /// fold cannot disagree about which indices exist.
    #[inline]
    fn weight_at(&self, index: usize) -> Result<CostUnits, QuoteError> {
        match self.weights.get(index) {
            Some(Some(weight)) => Ok(*weight),
            _ => Err(QuoteError::UnknownOperation { index }),
        }
    }

    /// Work permissions for a class, or `NONE` past the canonical array's end.
    #[inline]
    fn required_at(&self, index: usize) -> PermissionBits {
        self.permissions
            .get(index)
            .copied()
            .unwrap_or(PermissionBits::NONE)
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
    permissions: Vec<PermissionBits>,
}

impl CostTableBuilder {
    /// Register the per-item weight for one operation. Registering the same
    /// index twice keeps the last value; that is a configuration authoring
    /// concern, not a runtime one.
    #[must_use]
    pub fn weight(self, op: &impl OpIndex, per_item: CostUnits) -> Self {
        self.class(op, per_item, PermissionBits::NONE)
    }

    /// Register a class: what it costs per item and what it requires.
    ///
    /// One growth path with [`Self::weight`], which is this call with no
    /// requirement. Registering the same index twice keeps the last pair; that
    /// is a configuration authoring concern, not a runtime one.
    #[must_use]
    pub fn class(
        mut self,
        op: &impl OpIndex,
        per_item: CostUnits,
        required: PermissionBits,
    ) -> Self {
        let index = op.index();
        if index >= self.weights.len() {
            self.weights.resize(index + 1, None);
        }
        if index >= self.permissions.len() {
            self.permissions.resize(index + 1, PermissionBits::NONE);
        }
        self.weights[index] = Some(per_item);
        self.permissions[index] = required;
        self
    }

    #[must_use]
    pub fn build(mut self) -> CostTable {
        // Canonical form: trailing `NONE` carries no information, and leaving
        // it in would make a table built with `.weight` unequal to the same
        // table decoded from JSON that predates the field.
        while self.permissions.last() == Some(&PermissionBits::NONE) {
            self.permissions.pop();
        }
        CostTable {
            fixed_request: self.fixed_request,
            minimum_charge: self.minimum_charge,
            weights: self.weights.into_boxed_slice(),
            permissions: self.permissions.into_boxed_slice(),
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

    // The one-class path and the workload fold answer identically, including
    // which error they answer with.
    //
    // `quote` takes the direct index rather than folding a one-element
    // workload, because delegating cost it 32% (#114). That is only safe while
    // the two cannot diverge, and "they share `weight_at` and `quote_weight`"
    // is an argument, not a check — this is the check.
    proptest::proptest! {
        #[test]
        fn quote_agrees_with_the_one_entry_workload(
            class in 0_usize..3,
            items in 0_u64..=u64::MAX,
        ) {
            let op = [Op::Price, Op::Greeks, Op::Unpriced][class];
            let table = table();
            let folded = table
                .quote_workload(&[(op, items)])
                .map(|(quote, _, _)| quote);
            proptest::prop_assert_eq!(table.quote(&op, items), folded);
        }
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

    /// A repeated class is summed, and the fixed term still applies once.
    ///
    /// The alternative — rejecting a duplicate — was available and not taken.
    /// Summing means a caller that groups its own workload and a caller that
    /// does not are charged identically, so grouping stays an optimisation
    /// rather than a correctness obligation the caller can get wrong.
    #[test]
    fn a_repeated_class_is_summed_not_quoted_twice() {
        let table = table();

        let (split, split_items, _) = table
            .quote_workload(&[(Op::Price, 2), (Op::Price, 3)])
            .unwrap();
        let (grouped, grouped_items, _) = table.quote_workload(&[(Op::Price, 5)]).unwrap();

        assert_eq!(split, grouped, "a repeated class changed the quote");
        assert_eq!(split_items, grouped_items);
        // The load-bearing half: the fixed term is a property of the request,
        // so it cannot arrive once per entry.
        assert_eq!(split.fixed, table.fixed_request());
    }

    /// The fold reports the union of what its classes require.
    #[test]
    fn work_permissions_are_the_union_of_the_classes_quoted() {
        let price = PermissionBits::bit(1);
        let greeks = PermissionBits::bit(2);
        let table = CostTable::builder(CostUnits(50), CostUnits(50))
            .class(&Op::Price, CostUnits(1), price)
            .class(&Op::Greeks, CostUnits(5), greeks)
            .build();

        let (_, _, one) = table.quote_workload(&[(Op::Price, 1)]).unwrap();
        assert_eq!(one, price);

        let (_, _, both) = table
            .quote_workload(&[(Op::Price, 1), (Op::Greeks, 1)])
            .unwrap();
        assert_eq!(both, price.union(greeks));

        // A zero count is not work, so it cannot contribute a requirement —
        // otherwise a caller could be denied for a class it did not ask for.
        let (_, _, skipped) = table
            .quote_workload(&[(Op::Price, 1), (Op::Greeks, 0)])
            .unwrap();
        assert_eq!(skipped, price);
    }

    /// A class registered without a requirement requires nothing.
    #[test]
    fn weight_registers_a_class_that_requires_nothing() {
        let table = table();
        let (_, _, required) = table.quote_workload(&[(Op::Price, 1)]).unwrap();
        assert_eq!(required, PermissionBits::NONE);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn legacy_cost_table_round_trips_canonically() {
        // A table stored before work permissions existed has no such field.
        let legacy = r#"{"fixed_request":50,"minimum_charge":50,"weights":[1,5]}"#;
        let decoded: CostTable = serde_json::from_str(legacy).expect("legacy table decodes");

        // It must equal the same table built today, or a control plane that has
        // not been redeployed would start publishing tables that compare
        // unequal to what instances already hold.
        assert_eq!(decoded, table());

        // And it must serialize back without inventing the field, so a
        // round trip through a newer binary does not rewrite stored bytes.
        let reserialized = serde_json::to_string(&decoded).expect("table serializes");
        assert_eq!(reserialized, legacy);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn an_all_none_permission_array_is_not_serialized() {
        // `.weight` sets NONE, so a table built entirely from it must be
        // byte-identical to the legacy form; trailing NONE is trimmed at build.
        let built = table();
        let rendered = serde_json::to_string(&built).expect("table serializes");
        assert!(
            !rendered.contains("permissions"),
            "an all-NONE array was serialized: {rendered}"
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_table_with_permissions_round_trips() {
        let table = CostTable::builder(CostUnits(50), CostUnits(50))
            .class(&Op::Price, CostUnits(1), PermissionBits::bit(1))
            .class(&Op::Greeks, CostUnits(5), PermissionBits::bit(2))
            .build();
        let rendered = serde_json::to_string(&table).expect("table serializes");
        let decoded: CostTable = serde_json::from_str(&rendered).expect("table decodes");
        assert_eq!(decoded, table);
    }
}
