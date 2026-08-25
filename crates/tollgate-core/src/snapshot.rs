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
use crate::units::CostUnits;

/// Administrative state of the account at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AccountStatus {
    Active,
    /// Temporarily disabled; denies but may return.
    Suspended,
    /// Terminally disabled. One-way: an account enters `Closed` from any
    /// status and leaves it never (INVARIANTS.md #22).
    Closed,
}

impl AccountStatus {
    /// The one spelling of each status: serde's, the ledger column's, and the
    /// operator-facing one.
    ///
    /// Three places compare these strings — the `tollgate_accounts.status`
    /// `CHECK` constraint, the JSONB predicate that decides which snapshots a
    /// status change rewrites, and the admin wire DTO. Spelling them
    /// separately is how they drift, so they all read from here, and
    /// `account_status_text_matches_its_serde_spelling` pins this against
    /// serde. Exhaustive by construction: a new variant fails to compile
    /// until it has a spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            AccountStatus::Active => "Active",
            AccountStatus::Suspended => "Suspended",
            AccountStatus::Closed => "Closed",
        }
    }
}

/// What an account does when its local lease cannot fund a quote.
///
/// This is the one axis of the fail-closed rule that is per-account rather than
/// absolute. Unknown principals, stale snapshots, cost overflow and accounting
/// backpressure deny under every mode; only *lease cannot fund this request*
/// is negotiable, because only that condition says something about funding
/// rather than about validity (INVARIANTS.md #1, #5).
///
/// **The cap is per service instance.** The counter it bounds is a local
/// atomic, like every other local mechanism here, so a fleet of `N` instances
/// can extend up to `N * overage_cap` units of credit for one account before
/// any of them refuses. That multiplication is a property of the design, not
/// an oversight: aggregating it would require the synchronous coordination the
/// request path exists to avoid. Size the cap against the fleet, not against
/// one process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EnforcementMode {
    /// Deny with zero charge when the lease cannot fund the quote. The
    /// behaviour every account had before elastic mode existed, and the
    /// default a snapshot decodes to when it carries no mode at all.
    #[default]
    Strict,
    /// Admit past the lease and record the spend as overage, up to
    /// `overage_cap` units **per service instance**.
    ///
    /// Overage is unfunded spend: units consumed that no deposit paid for and
    /// no lease debited. It is billed like any other usage — the resulting
    /// event carries no lease capability, and the ledger records it as a
    /// second funding term so per-account conservation still closes exactly.
    Elastic { overage_cap: CostUnits },
}

impl EnforcementMode {
    /// The one spelling of each mode's *tag*: serde's, the ledger column's,
    /// and the operator-facing one.
    ///
    /// Deliberately the tag alone, never the cap. This string labels metrics
    /// and log lines, and interpolating a per-account number into a label is
    /// how a bounded label set becomes an unbounded one — the same rule
    /// `payload_does_not_affect_the_slot` pins for [`DenyReason`]. Exhaustive
    /// by construction: a new variant fails to compile until it has a
    /// spelling, and `enforcement_mode_text_matches_its_serde_tag` pins these
    /// against serde.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            EnforcementMode::Strict => "Strict",
            EnforcementMode::Elastic { .. } => "Elastic",
        }
    }

    /// The overage allowance, or `None` under [`EnforcementMode::Strict`].
    #[must_use]
    pub const fn overage_cap(self) -> Option<CostUnits> {
        match self {
            EnforcementMode::Strict => None,
            EnforcementMode::Elastic { overage_cap } => Some(overage_cap),
        }
    }
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
#[repr(align(128))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AccountSnapshot {
    pub account_id: AccountId,
    /// The credential this snapshot was compiled for, when key-scoped.
    pub key_id: Option<KeyId>,
    /// Monotonic snapshot version; see [`Generation`].
    pub generation: Generation,
    pub status: AccountStatus,
    /// What this account does when its lease cannot fund a quote. Lands in
    /// the struct's existing tail padding beside `status`, and is read on the
    /// same cache line the request path already touches for it.
    ///
    /// Defaults on the wire as well as in storage, so a control plane that
    /// predates elastic mode keeps publishing successfully instead of getting
    /// a 422 for a field it has never heard of. The default is
    /// [`EnforcementMode::Strict`]: an unstated mode enforces, it does not
    /// extend credit.
    #[cfg_attr(feature = "serde", serde(default))]
    pub enforcement_mode: EnforcementMode,
    /// Hard staleness bound: past this instant the snapshot denies
    /// (INVARIANTS.md #5) until the control plane delivers a successor.
    pub valid_until: Timestamp,
    pub permissions: PermissionBits,
    pub limits: ResolvedLimits,
    pub cost_table: Arc<CostTable>,
}

/// Why a compiled snapshot cannot be published.
///
/// The validation domain is the full `u64` configuration space. Arithmetic
/// is checked in the same `CostTable` implementation the request path uses;
/// overflow is a refusal, never a wrapped low quote (INVARIANTS #11/#16).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotValidationError {
    /// The most expensive registered operation overflows at the batch cap.
    QuoteOverflow {
        operation_index: usize,
        max_items: u64,
    },
    /// A request admitted by the batch cap can never fit in the whole burst.
    QuoteExceedsBurst {
        operation_index: usize,
        max_quote: CostUnits,
        burst_units: CostUnits,
    },
    /// An elastic account's overage cap cannot fund even one worst-case
    /// request, so the mode would admit nothing it was configured to admit.
    ///
    /// Refused rather than silently tolerated, for the same reason
    /// [`QuoteExceedsBurst`](Self::QuoteExceedsBurst) is: a configuration
    /// value is an operator contract, and a cap that reads as "extend credit"
    /// while behaving as `Strict` is the silent-semantics failure the
    /// repository guidelines forbid.
    OverageCapBelowMaxQuote {
        operation_index: usize,
        max_quote: CostUnits,
        overage_cap: CostUnits,
    },
}

impl std::fmt::Display for SnapshotValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotValidationError::QuoteOverflow {
                operation_index,
                max_items,
            } => write!(
                f,
                "operation {operation_index} cost overflows at the batch cap of {max_items} items"
            ),
            SnapshotValidationError::QuoteExceedsBurst {
                operation_index,
                max_quote,
                burst_units,
            } => write!(
                f,
                "operation {operation_index} can quote {max_quote} units, exceeding the burst of {burst_units} units"
            ),
            SnapshotValidationError::OverageCapBelowMaxQuote {
                operation_index,
                max_quote,
                overage_cap,
            } => write!(
                f,
                "operation {operation_index} can quote {max_quote} units, exceeding the overage cap of {overage_cap} units"
            ),
        }
    }
}

impl std::error::Error for SnapshotValidationError {}

/// Evidence that an account snapshot satisfies publication-time invariants.
///
/// The raw [`AccountSnapshot`] intentionally remains constructible: the
/// request path keeps `UnpriceableUnderLimits` as a defensive runtime
/// backstop. Store publication and source boundaries exchange this proof so
/// an invalid snapshot cannot reach them by caller convention alone.
#[derive(Debug, Clone)]
pub struct PublishableSnapshot {
    snapshot: Arc<AccountSnapshot>,
    maximum_quote: Option<CostUnits>,
}

impl PublishableSnapshot {
    pub fn try_new(snapshot: Arc<AccountSnapshot>) -> Result<Self, SnapshotValidationError> {
        let Some((operation_index, maximum_weight)) = snapshot.cost_table.maximum_weight() else {
            // With no registered operation the table cannot produce a quote,
            // so no request can witness a quote/burst inconsistency.
            return Ok(PublishableSnapshot {
                snapshot,
                maximum_quote: None,
            });
        };
        let max_quote = snapshot
            .cost_table
            .quote_weight(maximum_weight, snapshot.limits.max_items_per_request)
            .map_err(|_| SnapshotValidationError::QuoteOverflow {
                operation_index,
                max_items: snapshot.limits.max_items_per_request,
            })?
            .total;
        let burst_units = CostUnits(snapshot.limits.rate_burst_units);
        if max_quote > burst_units {
            return Err(SnapshotValidationError::QuoteExceedsBurst {
                operation_index,
                max_quote,
                burst_units,
            });
        }
        if let Some(overage_cap) = snapshot.enforcement_mode.overage_cap()
            && max_quote > overage_cap
        {
            return Err(SnapshotValidationError::OverageCapBelowMaxQuote {
                operation_index,
                max_quote,
                overage_cap,
            });
        }
        Ok(PublishableSnapshot {
            snapshot,
            maximum_quote: Some(max_quote),
        })
    }

    #[must_use]
    pub fn as_snapshot(&self) -> &AccountSnapshot {
        &self.snapshot
    }

    /// The largest quote publication proved the configured request shape can
    /// produce. Control-plane consumers use this evidence directly instead
    /// of repeating the cost-table scan at the next trust boundary.
    #[must_use]
    pub fn maximum_quote(&self) -> Option<CostUnits> {
        self.maximum_quote
    }

    #[must_use]
    pub fn into_inner(self) -> Arc<AccountSnapshot> {
        self.snapshot
    }

    /// Re-stamp status and generation, carrying the publication proof over.
    ///
    /// Sound without revalidating, and the reason is worth keeping next to the
    /// code rather than at the call site: [`try_new`](Self::try_new) checks
    /// `cost_table`'s worst quote at `limits.max_items_per_request` against
    /// two ceilings — `limits.rate_burst_units`, and an elastic account's
    /// `overage_cap`. Neither `status` nor `generation` is one of the five
    /// fields those checks read, so a snapshot that was publishable stays
    /// publishable under any value of either.
    ///
    /// Note what this method therefore must not grow: re-stamping the
    /// enforcement mode would change a value validation *does* depend on, and
    /// would need a fallible signature. An account-wide mode change goes
    /// through publication, not through here.
    ///
    /// This is what an account-wide status change needs (#22): the ledger and
    /// every live snapshot move together, and re-deriving a proof that cannot
    /// have changed would only invite an `expect` at each call site.
    #[must_use]
    pub fn restamped(&self, status: AccountStatus, generation: Generation) -> Self {
        let mut snapshot = AccountSnapshot::clone(&self.snapshot);
        snapshot.status = status;
        snapshot.generation = generation;
        PublishableSnapshot {
            snapshot: Arc::new(snapshot),
            maximum_quote: self.maximum_quote,
        }
    }
}

impl std::ops::Deref for PublishableSnapshot {
    type Target = AccountSnapshot;

    fn deref(&self) -> &Self::Target {
        self.as_snapshot()
    }
}

impl AsRef<AccountSnapshot> for PublishableSnapshot {
    fn as_ref(&self) -> &AccountSnapshot {
        self.as_snapshot()
    }
}

impl TryFrom<Arc<AccountSnapshot>> for PublishableSnapshot {
    type Error = SnapshotValidationError;

    fn try_from(snapshot: Arc<AccountSnapshot>) -> Result<Self, Self::Error> {
        PublishableSnapshot::try_new(snapshot)
    }
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

    /// The ledger column, the JSONB predicate and the wire DTO all compare
    /// these strings, so `as_str` and serde must agree exactly. If they ever
    /// diverge, a status change silently rewrites the wrong set of snapshots
    /// — the failure this whole mechanism exists to prevent (#51).
    #[test]
    #[cfg(feature = "serde")]
    fn account_status_text_matches_its_serde_spelling() {
        for status in [
            AccountStatus::Active,
            AccountStatus::Suspended,
            AccountStatus::Closed,
        ] {
            assert_eq!(
                serde_json::to_value(status).expect("a unit variant serializes"),
                serde_json::Value::String(status.as_str().to_owned()),
                "{status:?} disagrees with its serde spelling"
            );
        }
    }

    /// The same rule `AccountStatus` follows, for the same reason: the mode's
    /// tag is compared as text by the ledger column, the JSONB predicate that
    /// selects which snapshots a mode change rewrites, and the admin wire DTO.
    #[test]
    #[cfg(feature = "serde")]
    fn enforcement_mode_text_matches_its_serde_tag() {
        for mode in [
            EnforcementMode::Strict,
            EnforcementMode::Elastic {
                overage_cap: CostUnits(1_000),
            },
        ] {
            let value = serde_json::to_value(mode).expect("a mode serializes");
            let tag = match &value {
                serde_json::Value::String(tag) => tag.clone(),
                serde_json::Value::Object(map) => {
                    map.keys().next().expect("one variant key").clone()
                }
                other => panic!("unexpected encoding {other}"),
            };
            assert_eq!(tag, mode.as_str(), "{mode:?} disagrees with its serde tag");
        }
    }

    /// The cap must never reach a label. A per-account number in a metric
    /// label turns a fixed enum into unbounded cardinality — the rule
    /// `DenyReason`'s `payload_does_not_affect_the_slot` pins one layer down.
    #[test]
    fn the_cap_does_not_affect_the_mode_label() {
        assert_eq!(
            EnforcementMode::Elastic {
                overage_cap: CostUnits(1)
            }
            .as_str(),
            EnforcementMode::Elastic {
                overage_cap: CostUnits(u64::MAX)
            }
            .as_str()
        );
    }

    /// An unstated mode enforces. This is the value every pre-existing stored
    /// row and every older control plane decodes to, so it is the one that
    /// must not extend credit.
    #[test]
    fn the_default_mode_is_strict() {
        assert_eq!(EnforcementMode::default(), EnforcementMode::Strict);
        assert_eq!(EnforcementMode::Strict.overage_cap(), None);
    }

    /// A cap that cannot fund one worst-case request would read as "extend
    /// credit" and behave as `Strict`. Refused at publication, where the
    /// burst check already refuses its analogue.
    #[test]
    fn a_cap_below_the_worst_quote_is_unpublishable() {
        // Worst quote is 10 fixed + 7 x 10 items = 80, equal to the burst, so
        // only the overage cap can be what refuses these.
        let elastic = |overage_cap: u64| {
            let mut snapshot = AccountSnapshot::clone(&priced_snapshot(10, 1, &[(0, 7)], 10, 80));
            snapshot.enforcement_mode = EnforcementMode::Elastic {
                overage_cap: CostUnits(overage_cap),
            };
            PublishableSnapshot::try_new(Arc::new(snapshot))
        };

        assert!(
            elastic(80).is_ok(),
            "a cap that funds exactly one worst case is publishable"
        );
        assert_eq!(
            elastic(79).unwrap_err(),
            SnapshotValidationError::OverageCapBelowMaxQuote {
                operation_index: 0,
                max_quote: CostUnits(80),
                overage_cap: CostUnits(79),
            }
        );
    }

    /// The mode participates in validation, unlike status and generation, so
    /// `restamped` must not be able to change it — otherwise it would carry a
    /// proof forward over a value that proof depended on.
    #[test]
    fn restamping_cannot_change_the_enforcement_mode() {
        let mut snapshot = snapshot(AccountStatus::Active, t(1_000));
        snapshot.enforcement_mode = EnforcementMode::Elastic {
            overage_cap: CostUnits(5_000),
        };
        let publishable = PublishableSnapshot::try_new(Arc::new(snapshot)).unwrap();
        let restamped = publishable.restamped(AccountStatus::Suspended, Generation(9));
        assert_eq!(
            restamped.as_snapshot().enforcement_mode,
            EnforcementMode::Elastic {
                overage_cap: CostUnits(5_000)
            }
        );
    }

    /// Re-stamping carries the publication proof because neither field it
    /// touches participates in validation. Pinned against a snapshot whose
    /// margin is exact: if `restamped` ever rebuilt the proof from scratch
    /// this would still pass, but if it ever *altered* limits or cost table
    /// it would not.
    #[test]
    fn restamping_preserves_everything_validation_depends_on() {
        let original = PublishableSnapshot::try_new(Arc::new(snapshot(
            AccountStatus::Active,
            Timestamp::from_second(1_000).unwrap(),
        )))
        .expect("the fixture is publishable");

        let restamped = original.restamped(AccountStatus::Suspended, Generation(9));

        assert_eq!(restamped.status, AccountStatus::Suspended);
        assert_eq!(restamped.generation, Generation(9));
        assert_eq!(restamped.account_id, original.account_id);
        assert_eq!(restamped.key_id, original.key_id);
        assert_eq!(restamped.valid_until, original.valid_until);
        assert_eq!(restamped.permissions, original.permissions);
        assert_eq!(
            restamped.limits.max_items_per_request,
            original.limits.max_items_per_request
        );
        assert_eq!(
            restamped.limits.rate_burst_units,
            original.limits.rate_burst_units
        );
        assert!(
            Arc::ptr_eq(&restamped.cost_table, &original.cost_table),
            "the cost table is shared, not rebuilt"
        );
        // The proof still holds when re-derived, which is the claim the doc
        // comment makes.
        PublishableSnapshot::try_new(restamped.into_inner())
            .expect("status and generation do not affect publishability");
    }

    fn snapshot(status: AccountStatus, valid_until: Timestamp) -> AccountSnapshot {
        AccountSnapshot {
            account_id: AccountId(1),
            key_id: Some(KeyId(2)),
            generation: Generation(1),
            status,
            enforcement_mode: EnforcementMode::Strict,
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

    fn priced_snapshot(
        fixed: u64,
        minimum: u64,
        weights: &[(usize, u64)],
        max_items: u64,
        burst: u64,
    ) -> Arc<AccountSnapshot> {
        struct Op(usize);
        impl crate::cost_table::OpIndex for Op {
            fn index(&self) -> usize {
                self.0
            }
        }

        let mut builder = CostTable::builder(CostUnits(fixed), CostUnits(minimum));
        for (index, weight) in weights {
            builder = builder.weight(&Op(*index), CostUnits(*weight));
        }
        Arc::new(AccountSnapshot {
            limits: ResolvedLimits {
                max_items_per_request: max_items,
                rate_units_per_second: 1_000,
                rate_burst_units: burst,
            },
            cost_table: Arc::new(builder.build()),
            ..snapshot(AccountStatus::Active, t(1_000))
        })
    }

    #[test]
    fn publication_uses_the_largest_registered_weight() {
        let snapshot = priced_snapshot(10, 1, &[(0, 2), (3, 7), (5, 4)], 10, 79);
        assert_eq!(
            PublishableSnapshot::try_new(snapshot).unwrap_err(),
            SnapshotValidationError::QuoteExceedsBurst {
                operation_index: 3,
                max_quote: CostUnits(80),
                burst_units: CostUnits(79),
            }
        );
    }

    #[test]
    fn publication_reports_first_operation_when_maximum_weights_tie() {
        assert_eq!(
            PublishableSnapshot::try_new(priced_snapshot(0, 0, &[(2, 7), (5, 7)], 10, 69))
                .unwrap_err(),
            SnapshotValidationError::QuoteExceedsBurst {
                operation_index: 2,
                max_quote: CostUnits(70),
                burst_units: CostUnits(69),
            }
        );
    }

    #[test]
    fn publication_accepts_a_worst_case_quote_equal_to_the_burst() {
        let publishable =
            PublishableSnapshot::try_new(priced_snapshot(10, 1, &[(0, 7)], 10, 80)).unwrap();
        assert_eq!(
            publishable.maximum_quote(),
            Some(CostUnits(80)),
            "the proof passed to the next trust boundary is the quote validated here"
        );
    }

    #[test]
    fn publication_rejects_a_minimum_above_the_burst() {
        assert!(matches!(
            PublishableSnapshot::try_new(priced_snapshot(0, 50, &[(0, 0)], 64, 49)),
            Err(SnapshotValidationError::QuoteExceedsBurst {
                max_quote: CostUnits(50),
                ..
            })
        ));
    }

    #[test]
    fn publication_rejects_worst_case_quote_overflow() {
        assert_eq!(
            PublishableSnapshot::try_new(priced_snapshot(1, 0, &[(0, u64::MAX)], 2, u64::MAX,))
                .unwrap_err(),
            SnapshotValidationError::QuoteOverflow {
                operation_index: 0,
                max_items: 2,
            }
        );
    }

    #[test]
    fn publication_allows_a_table_with_no_registered_operations() {
        PublishableSnapshot::try_new(priced_snapshot(100, 100, &[], 64, 0)).unwrap();
    }
}
