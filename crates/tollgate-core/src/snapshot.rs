//! Compiled account snapshots: the immutable, admission-ready form of an
//! account's policy.
//!
//! A snapshot is what a control plane compiles *from* its source of truth
//! (database rows, policy records, plan bindings) and pushes *to* service
//! instances. By the time it reaches this type there is nothing left to
//! resolve: bitsets, integers, and a compiled cost table. Strings, JSON, and
//! joins belong to the control plane.

use std::sync::Arc;

use jiff::Timestamp;

use crate::cost_table::CostTable;
use crate::deny::DenyReason;
use crate::ids::{AccountId, Generation, KeyId};

/// Administrative state of the account at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AccountStatus {
    Active,
    /// Temporarily disabled; denies but may return.
    Suspended,
    /// Terminally disabled.
    Closed,
}

/// Up to 64 permission slots, compiled from whatever entitlement vocabulary
/// the consumer uses. Admission is a superset test — one AND and one compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct PermissionBits(pub u64);

impl PermissionBits {
    pub const NONE: PermissionBits = PermissionBits(0);
    pub const ALL: PermissionBits = PermissionBits(u64::MAX);

    /// The single permission at `bit` (0..64).
    #[inline]
    #[must_use]
    pub const fn bit(bit: u32) -> PermissionBits {
        PermissionBits(1u64 << bit)
    }

    #[inline]
    #[must_use]
    pub const fn union(self, other: PermissionBits) -> PermissionBits {
        PermissionBits(self.0 | other.0)
    }

    /// True when every bit in `required` is granted here.
    #[inline]
    #[must_use]
    pub const fn contains_all(self, required: PermissionBits) -> bool {
        self.0 & required.0 == required.0
    }
}

/// Resolved integer limits. These parameterize the admission layer's local
/// rate limiting and request shaping; the snapshot only carries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResolvedLimits {
    /// Largest admissible item count in one request (batch cap).
    pub max_items_per_request: u64,
    /// Sustained refill rate of the account's local token bucket, in cost
    /// units per second.
    pub rate_units_per_second: u64,
    /// Burst capacity of that bucket, in cost units.
    pub rate_burst_units: u64,
}

/// One account credential's compiled, immutable admission state.
///
/// Shared as `Arc<AccountSnapshot>`; replaced whole (never mutated) when the
/// control plane publishes a newer [`Generation`].
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AccountSnapshot {
    pub account_id: AccountId,
    /// The credential this snapshot was compiled for, when key-scoped.
    pub key_id: Option<KeyId>,
    /// Monotonic snapshot version; see [`Generation`].
    pub generation: Generation,
    pub status: AccountStatus,
    /// Hard staleness bound: past this instant the snapshot denies
    /// (INVARIANTS.md #5) until the control plane delivers a successor.
    pub valid_until: Timestamp,
    pub permissions: PermissionBits,
    pub limits: ResolvedLimits,
    pub cost_table: Arc<CostTable>,
}

impl AccountSnapshot {
    /// The principal-level admission test: status, staleness, permissions.
    /// Pure and O(1); rate and quota checks come after, in the admission
    /// pipeline, because they consume state.
    #[inline]
    pub fn admit(&self, now: Timestamp, required: PermissionBits) -> Result<(), DenyReason> {
        match self.status {
            AccountStatus::Active => {}
            AccountStatus::Suspended => return Err(DenyReason::AccountSuspended),
            AccountStatus::Closed => return Err(DenyReason::AccountClosed),
        }
        if now >= self.valid_until {
            return Err(DenyReason::SnapshotExpired);
        }
        if !self.permissions.contains_all(required) {
            return Err(DenyReason::MissingPermission);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::units::CostUnits;

    fn snapshot(status: AccountStatus, valid_until: Timestamp) -> AccountSnapshot {
        AccountSnapshot {
            account_id: AccountId(1),
            key_id: Some(KeyId(2)),
            generation: Generation(1),
            status,
            valid_until,
            permissions: PermissionBits::bit(0).union(PermissionBits::bit(3)),
            limits: ResolvedLimits {
                max_items_per_request: 1024,
                rate_units_per_second: 10_000,
                rate_burst_units: 50_000,
            },
            cost_table: Arc::new(CostTable::builder(CostUnits(50), CostUnits(50)).build()),
        }
    }

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    #[test]
    fn active_valid_and_permitted_admits() {
        let s = snapshot(AccountStatus::Active, t(1_000));
        assert_eq!(s.admit(t(999), PermissionBits::bit(0)), Ok(()));
    }

    #[test]
    fn suspended_and_closed_deny() {
        let s = snapshot(AccountStatus::Suspended, t(1_000));
        assert_eq!(
            s.admit(t(0), PermissionBits::NONE),
            Err(DenyReason::AccountSuspended)
        );
        let s = snapshot(AccountStatus::Closed, t(1_000));
        assert_eq!(
            s.admit(t(0), PermissionBits::NONE),
            Err(DenyReason::AccountClosed)
        );
    }

    #[test]
    fn expiry_boundary_is_exclusive_of_valid_until() {
        let s = snapshot(AccountStatus::Active, t(1_000));
        assert_eq!(
            s.admit(t(1_000), PermissionBits::NONE),
            Err(DenyReason::SnapshotExpired)
        );
        assert_eq!(
            s.admit(t(1_001), PermissionBits::NONE),
            Err(DenyReason::SnapshotExpired)
        );
    }

    #[test]
    fn missing_permission_denies() {
        let s = snapshot(AccountStatus::Active, t(1_000));
        assert_eq!(
            s.admit(t(0), PermissionBits::bit(1)),
            Err(DenyReason::MissingPermission)
        );
        // Superset of granted bits is required, not intersection.
        assert_eq!(
            s.admit(t(0), PermissionBits::bit(0).union(PermissionBits::bit(1))),
            Err(DenyReason::MissingPermission)
        );
    }
}
