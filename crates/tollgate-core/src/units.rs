//! Generic cost units with checked arithmetic.

use core::fmt;

/// A quantity of abstract cost units — the generic stand-in for a consumer's
/// compute-unit vocabulary (FerroRisk's FCUs, another product's credits).
///
/// All arithmetic is checked: overflow returns `None` and callers must treat
/// it as a denial (INVARIANTS.md #11). There are deliberately no `Add`/`Mul`
/// operator impls, because operators hide the overflow decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct CostUnits(pub u64);

impl CostUnits {
    pub const ZERO: CostUnits = CostUnits(0);

    #[inline]
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[inline]
    #[must_use]
    pub const fn checked_add(self, other: CostUnits) -> Option<CostUnits> {
        match self.0.checked_add(other.0) {
            Some(v) => Some(CostUnits(v)),
            None => None,
        }
    }

    #[inline]
    #[must_use]
    pub const fn checked_sub(self, other: CostUnits) -> Option<CostUnits> {
        match self.0.checked_sub(other.0) {
            Some(v) => Some(CostUnits(v)),
            None => None,
        }
    }

    #[inline]
    #[must_use]
    pub const fn checked_mul(self, count: u64) -> Option<CostUnits> {
        match self.0.checked_mul(count) {
            Some(v) => Some(CostUnits(v)),
            None => None,
        }
    }

    #[inline]
    #[must_use]
    pub const fn max(self, other: CostUnits) -> CostUnits {
        if self.0 >= other.0 { self } else { other }
    }

    #[inline]
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for CostUnits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for CostUnits {
    fn from(v: u64) -> Self {
        CostUnits(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_add_overflow_is_none() {
        assert_eq!(CostUnits(u64::MAX).checked_add(CostUnits(1)), None);
        assert_eq!(CostUnits(1).checked_add(CostUnits(2)), Some(CostUnits(3)));
    }

    #[test]
    fn checked_sub_underflow_is_none() {
        assert_eq!(CostUnits(0).checked_sub(CostUnits(1)), None);
        assert_eq!(
            CostUnits(5).checked_sub(CostUnits(5)),
            Some(CostUnits::ZERO)
        );
    }

    #[test]
    fn checked_mul_overflow_is_none() {
        assert_eq!(CostUnits(u64::MAX).checked_mul(2), None);
        assert_eq!(CostUnits(3).checked_mul(4), Some(CostUnits(12)));
    }
}
