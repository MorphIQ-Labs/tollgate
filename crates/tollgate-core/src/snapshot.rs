//! Compiled account snapshots: the immutable, admission-ready form of an
//! account's policy.
//!
//! A snapshot is what a control plane compiles *from* its source of truth
//! (database rows, policy records, plan bindings) and pushes *to* service
//! instances. By the time it reaches this type there is nothing left to
//! resolve: bitsets, integers, and a compiled cost table. Strings, JSON, and
//! joins belong to the control plane.

use std::num::NonZeroU32;
use std::sync::Arc;

use jiff::Timestamp;

use crate::budget::BudgetView;
use crate::cost_table::CostTable;
use crate::deny::DenyReason;
use crate::ids::{AccountId, Generation, KeyId, PolicyRevision};
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

/// Which execution-capacity class an account's work belongs to (#99).
///
/// A separate axis from [`EnforcementMode`], and the two must not be
/// conflated. Enforcement mode answers whether an account can *fund* a
/// request; this answers whether an instance should *start* an already-valid
/// request with the compute capacity it has right now. An assured account may
/// be strict, and a best-effort account still spends quota and emits ordinary
/// usage whenever it does execute.
///
/// The vocabulary is deliberately semantic rather than commercial. Tollgate
/// has no `Paid`/`Free` and no open-ended priority integer: the product maps
/// its plans onto these two, and which customers are assured is a decision
/// that stays outside an enforcement substrate.
///
/// # Assured is the default, and that is a compatibility choice
///
/// An account that states no class gets the availability every account has
/// today. The unsafe direction would be defaulting to `BestEffort`, which
/// would silently make existing traffic sheddable the moment a capacity gate
/// was enabled — the same reasoning that makes [`AccountStatus`] required at
/// construction and [`EnforcementMode`] default to `Strict`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CapacityClass {
    /// Work that may use the whole instance, including the reserve kept for
    /// it. The default.
    #[default]
    Assured,
    /// Work that may use only capacity not reserved for assured traffic, and
    /// is shed first under load.
    BestEffort,
}

impl CapacityClass {
    /// The one spelling of each class: serde's, the ledger column's, and the
    /// operator-facing one.
    ///
    /// The same three-consumer argument [`AccountStatus::as_str`] makes — the
    /// `tollgate_accounts.capacity_class` `CHECK` constraint, the JSONB
    /// predicate deciding which snapshots a class change rewrites, and the
    /// admin wire DTO all compare these strings, so they all read from here.
    /// It is also the metric label: capacity counters distinguish the classes
    /// by these two tags and nothing else, which is what keeps their
    /// cardinality bounded (#99).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            CapacityClass::Assured => "Assured",
            CapacityClass::BestEffort => "BestEffort",
        }
    }

    /// Whether work of this class may draw on the assured reserve.
    ///
    /// Stated once, here, rather than re-derived as `== Assured` at each pool
    /// decision: the reserve's whole purpose is that exactly one class reaches
    /// it, and a second spelling of that rule is how the two drift.
    #[must_use]
    pub const fn may_use_assured_reserve(self) -> bool {
        matches!(self, CapacityClass::Assured)
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

/// The cost-weighted token bucket carried by a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightedRateLimit {
    units_per_second: u64,
    burst_units: u64,
}

impl WeightedRateLimit {
    #[must_use]
    pub const fn units_per_second(self) -> u64 {
        self.units_per_second
    }

    #[must_use]
    pub const fn burst_units(self) -> u64 {
        self.burst_units
    }
}

/// The request-count token bucket carried by a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestRateLimit {
    requests_per_second: NonZeroU32,
    burst_requests: NonZeroU32,
}

impl RequestRateLimit {
    #[must_use]
    pub const fn requests_per_second(self) -> NonZeroU32 {
        self.requests_per_second
    }

    #[must_use]
    pub const fn burst_requests(self) -> NonZeroU32 {
        self.burst_requests
    }
}

/// The account-wide rate dimensions carried inside a principal snapshot.
///
/// Admission compiles these values into shared mutable governor buckets. A
/// separate value type lets that compiler bind the exact policy represented
/// by a bucket back into every principal state that pins it, while leaving
/// principal-local shaping and concurrency dimensions untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountRatePolicy {
    weighted_rate: Option<WeightedRateLimit>,
    legacy_weighted_rate: WeightedRateLimit,
    request_rate: Option<RequestRateLimit>,
}

impl AccountRatePolicy {
    #[must_use]
    pub const fn weighted_rate(self) -> Option<WeightedRateLimit> {
        self.weighted_rate
    }

    #[must_use]
    pub const fn legacy_weighted_rate(self) -> WeightedRateLimit {
        self.legacy_weighted_rate
    }

    #[must_use]
    pub const fn request_rate(self) -> Option<RequestRateLimit> {
        self.request_rate
    }
}

/// Why resolved limits could not be constructed from a wire representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedLimitsError {
    RequestRatePairIncomplete,
    PrincipalConcurrencyWithoutAccount,
    PrincipalConcurrencyExceedsAccount {
        principal: NonZeroU32,
        account: NonZeroU32,
    },
}

impl std::fmt::Display for ResolvedLimitsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolvedLimitsError::RequestRatePairIncomplete => {
                f.write_str("request_rate_per_second and request_burst must be supplied together")
            }
            ResolvedLimitsError::PrincipalConcurrencyWithoutAccount => {
                f.write_str("principal_max_concurrent_requests requires max_concurrent_requests")
            }
            ResolvedLimitsError::PrincipalConcurrencyExceedsAccount { principal, account } => {
                write!(
                    f,
                    "principal concurrency ceiling {principal} exceeds account ceiling {account}"
                )
            }
        }
    }
}

impl std::error::Error for ResolvedLimitsError {}

/// Resolved integer limits. These parameterize the admission layer's local
/// rate limiting and request shaping; the snapshot only carries them.
///
/// Fields are private and construction is incremental so adding another
/// optional policy dimension does not break every consumer's struct literal.
/// The two concurrency limits can only be installed together in a valid
/// narrowing relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(try_from = "WireResolvedLimits", into = "WireResolvedLimits")
)]
#[non_exhaustive]
pub struct ResolvedLimits {
    max_items_per_request: u64,
    weighted_rate: Option<WeightedRateLimit>,
    legacy_weighted_rate: WeightedRateLimit,
    request_rate: Option<RequestRateLimit>,
    max_concurrent_requests: Option<NonZeroU32>,
    principal_max_concurrent_requests: Option<NonZeroU32>,
}

impl ResolvedLimits {
    const COMPATIBILITY_FALLBACK: WeightedRateLimit = WeightedRateLimit {
        units_per_second: u32::MAX as u64,
        burst_units: u32::MAX as u64,
    };

    /// Start with only request shaping enabled. The carried legacy weighted
    /// pair is deliberately finite and valid for governor so an older reader
    /// remains safe during a reader-first rollout.
    #[must_use]
    pub const fn new(max_items_per_request: u64) -> Self {
        Self {
            max_items_per_request,
            weighted_rate: None,
            legacy_weighted_rate: Self::COMPATIBILITY_FALLBACK,
            request_rate: None,
            max_concurrent_requests: None,
            principal_max_concurrent_requests: None,
        }
    }

    #[must_use]
    pub const fn with_weighted_rate(mut self, units_per_second: u64, burst_units: u64) -> Self {
        let rate = WeightedRateLimit {
            units_per_second,
            burst_units,
        };
        self.weighted_rate = Some(rate);
        self.legacy_weighted_rate = rate;
        self
    }

    /// Select the values an older reader will enforce while the weighted
    /// bucket is disabled for readers that understand the new wire flag.
    #[must_use]
    pub const fn with_weighted_rate_compatibility_fallback(
        mut self,
        units_per_second: u64,
        burst_units: u64,
    ) -> Self {
        self.weighted_rate = None;
        self.legacy_weighted_rate = WeightedRateLimit {
            units_per_second,
            burst_units,
        };
        self
    }

    #[must_use]
    pub const fn with_request_rate(
        mut self,
        requests_per_second: NonZeroU32,
        burst_requests: NonZeroU32,
    ) -> Self {
        self.request_rate = Some(RequestRateLimit {
            requests_per_second,
            burst_requests,
        });
        self
    }

    pub fn with_concurrency(
        mut self,
        max_concurrent_requests: NonZeroU32,
        principal_max_concurrent_requests: Option<NonZeroU32>,
    ) -> Result<Self, ResolvedLimitsError> {
        if let Some(principal) = principal_max_concurrent_requests
            && principal > max_concurrent_requests
        {
            return Err(ResolvedLimitsError::PrincipalConcurrencyExceedsAccount {
                principal,
                account: max_concurrent_requests,
            });
        }
        self.max_concurrent_requests = Some(max_concurrent_requests);
        self.principal_max_concurrent_requests = principal_max_concurrent_requests;
        Ok(self)
    }

    #[must_use]
    pub const fn max_items_per_request(self) -> u64 {
        self.max_items_per_request
    }

    #[must_use]
    pub const fn weighted_rate(self) -> Option<WeightedRateLimit> {
        self.weighted_rate
    }

    /// The pair understood by pre-#91 readers. During the reader-first phase
    /// they continue enforcing it even when the new flag disables the bucket.
    #[must_use]
    pub const fn legacy_weighted_rate(self) -> WeightedRateLimit {
        self.legacy_weighted_rate
    }

    #[must_use]
    pub const fn request_rate(self) -> Option<RequestRateLimit> {
        self.request_rate
    }

    /// Extract the account-wide rate policy independently of request shaping
    /// and concurrency limits.
    #[must_use]
    pub const fn account_rate_policy(self) -> AccountRatePolicy {
        AccountRatePolicy {
            weighted_rate: self.weighted_rate,
            legacy_weighted_rate: self.legacy_weighted_rate,
            request_rate: self.request_rate,
        }
    }

    #[must_use]
    pub const fn max_concurrent_requests(self) -> Option<NonZeroU32> {
        self.max_concurrent_requests
    }

    #[must_use]
    pub const fn principal_max_concurrent_requests(self) -> Option<NonZeroU32> {
        self.principal_max_concurrent_requests
    }
}

#[cfg(feature = "serde")]
#[derive(serde::Serialize, serde::Deserialize)]
struct WireResolvedLimits {
    max_items_per_request: u64,
    rate_units_per_second: u64,
    rate_burst_units: u64,
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    weighted_rate_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_rate_per_second: Option<NonZeroU32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_burst: Option<NonZeroU32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_concurrent_requests: Option<NonZeroU32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    principal_max_concurrent_requests: Option<NonZeroU32>,
}

#[cfg(feature = "serde")]
const fn default_true() -> bool {
    true
}

#[cfg(feature = "serde")]
const fn is_true(value: &bool) -> bool {
    *value
}

#[cfg(feature = "serde")]
impl TryFrom<WireResolvedLimits> for ResolvedLimits {
    type Error = ResolvedLimitsError;

    fn try_from(wire: WireResolvedLimits) -> Result<Self, Self::Error> {
        let legacy_weighted_rate = WeightedRateLimit {
            units_per_second: wire.rate_units_per_second,
            burst_units: wire.rate_burst_units,
        };
        let mut limits = Self {
            max_items_per_request: wire.max_items_per_request,
            weighted_rate: wire.weighted_rate_enabled.then_some(legacy_weighted_rate),
            legacy_weighted_rate,
            request_rate: None,
            max_concurrent_requests: None,
            principal_max_concurrent_requests: None,
        };
        limits = match (wire.request_rate_per_second, wire.request_burst) {
            (None, None) => limits,
            (Some(requests_per_second), Some(burst_requests)) => {
                limits.with_request_rate(requests_per_second, burst_requests)
            }
            _ => return Err(ResolvedLimitsError::RequestRatePairIncomplete),
        };
        match wire.max_concurrent_requests {
            Some(account) => {
                limits.with_concurrency(account, wire.principal_max_concurrent_requests)
            }
            None if wire.principal_max_concurrent_requests.is_some() => {
                Err(ResolvedLimitsError::PrincipalConcurrencyWithoutAccount)
            }
            None => Ok(limits),
        }
    }
}

#[cfg(feature = "serde")]
impl From<ResolvedLimits> for WireResolvedLimits {
    fn from(limits: ResolvedLimits) -> Self {
        Self {
            max_items_per_request: limits.max_items_per_request,
            rate_units_per_second: limits.legacy_weighted_rate.units_per_second,
            rate_burst_units: limits.legacy_weighted_rate.burst_units,
            weighted_rate_enabled: limits.weighted_rate.is_some(),
            request_rate_per_second: limits.request_rate.map(|rate| rate.requests_per_second),
            request_burst: limits.request_rate.map(|rate| rate.burst_requests),
            max_concurrent_requests: limits.max_concurrent_requests,
            principal_max_concurrent_requests: limits.principal_max_concurrent_requests,
        }
    }
}

/// One account credential's compiled, immutable admission state.
///
/// Shared as `Arc<AccountSnapshot>`; replaced whole (never mutated) when the
/// control plane publishes a newer [`Generation`].
/// # Layout
///
/// `#[repr(C, align(128))]`, and both halves are load-bearing. The alignment
/// keeps one snapshot off its neighbours' cache lines. `repr(C)` is what makes
/// the declaration order below a *contract* rather than whatever this build
/// chose: without it the compiler reorders freely, and an offset assertion
/// would pin an accident. The fields the request path reads to admit — status,
/// permissions, validity, enforcement mode — are declared first so they share
/// the first cache line, and the cold ones follow. Pinned by
/// `stage_one_fields_share_the_first_cache_line`.
#[derive(Debug, Clone)]
#[repr(C, align(128))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct AccountSnapshot {
    pub status: AccountStatus,
    /// Which execution-capacity class this account's work belongs to (#99).
    ///
    /// Declared beside `status` because the capacity gate reads it on the same
    /// cache line admission already touches, and because it costs nothing to
    /// do so: `AccountStatus` is a one-byte enum and `permissions` is
    /// `u64`-aligned, so a second one-byte enum lands in padding that already
    /// existed.
    ///
    /// Defaults on the wire as well as in storage, the precedent
    /// `enforcement_mode` set. An absent class decodes to
    /// [`CapacityClass::Assured`] — the availability every account has today,
    /// and the only safe direction: defaulting to `BestEffort` would make
    /// existing traffic sheddable the moment a gate was enabled.
    #[cfg_attr(feature = "serde", serde(default))]
    pub capacity_class: CapacityClass,
    pub permissions: PermissionBits,
    /// Hard staleness bound: past this instant the snapshot denies
    /// (INVARIANTS.md #5) until the control plane delivers a successor.
    pub valid_until: Timestamp,
    /// What this account does when its lease cannot fund a quote. Declared
    /// among the stage-one fields because funding reads it on the same cache
    /// line the request path already touches for `status`.
    ///
    /// Defaults on the wire as well as in storage, so a control plane that
    /// predates elastic mode keeps publishing successfully instead of getting
    /// a 422 for a field it has never heard of. The default is
    /// [`EnforcementMode::Strict`]: an unstated mode enforces, it does not
    /// extend credit.
    #[cfg_attr(feature = "serde", serde(default))]
    pub enforcement_mode: EnforcementMode,
    /// Monotonic snapshot version; see [`Generation`].
    pub generation: Generation,
    pub cost_table: Arc<CostTable>,
    pub account_id: AccountId,
    /// The credential this snapshot was compiled for, when key-scoped.
    pub key_id: Option<KeyId>,
    pub limits: ResolvedLimits,
    /// What the control plane last said about the account's budget (#97), or
    /// `None` when it said nothing — an older control plane, or a publication
    /// that did not go through a store.
    ///
    /// `None` is deliberately not "zero remaining": an instance that reported
    /// a fabricated zero because its publisher predated the field would deny
    /// nothing but would tell every caller they were out of quota. Readers
    /// return `None` rather than a number they cannot stand behind.
    ///
    /// Defaults on the wire as well as in storage, the precedent
    /// `enforcement_mode` set, so a control plane that predates this keeps
    /// publishing successfully instead of getting a 422 for a field it has
    /// never heard of.
    ///
    /// There is no builder setter: the store stamps this at publication and is
    /// its only writer. See [`PublishableSnapshot::with_budget`].
    ///
    /// Free, spatially: it lands in the tail padding `#[repr(align(128))]`
    /// already reserved, so `AccountSnapshot` remains 256 bytes and the
    /// request path touches no line it did not already touch.
    #[cfg_attr(feature = "serde", serde(default))]
    pub budget: Option<BudgetView>,
    /// The consuming application's identity for the product policy compiled
    /// into this snapshot (#94). See [`PolicyRevision`].
    ///
    /// Tollgate carries it and never reads it, so it is declared last, well
    /// clear of the cache line admission touches. Distinct from `generation`:
    /// that orders publications, this names the inputs one was compiled from.
    ///
    /// Defaults on the wire as well as in storage, the precedent
    /// `enforcement_mode` and `budget` set. An absent revision decodes to
    /// [`PolicyRevision::UNSTATED`] rather than failing, which is safe here in
    /// a way it would not be for an enforcement field: nothing in Tollgate
    /// reads this, so an unstated revision cannot change an outcome.
    #[cfg_attr(feature = "serde", serde(default))]
    pub policy_revision: PolicyRevision,
}

/// Builder for the optional parts of an immutable account snapshot.
#[derive(Debug)]
pub struct AccountSnapshotBuilder {
    account_id: AccountId,
    key_id: Option<KeyId>,
    generation: Generation,
    status: AccountStatus,
    capacity_class: CapacityClass,
    enforcement_mode: EnforcementMode,
    valid_until: Timestamp,
    permissions: PermissionBits,
    limits: ResolvedLimits,
    cost_table: Arc<CostTable>,
    policy_revision: PolicyRevision,
}

impl AccountSnapshotBuilder {
    #[must_use]
    pub const fn key_id(mut self, key_id: KeyId) -> Self {
        self.key_id = Some(key_id);
        self
    }

    #[must_use]
    pub const fn enforcement_mode(mut self, enforcement_mode: EnforcementMode) -> Self {
        self.enforcement_mode = enforcement_mode;
        self
    }

    /// Set the account's execution-capacity class (#99).
    ///
    /// Optional, and omitting it leaves [`CapacityClass::Assured`]. The class
    /// is an account-owned fact: the control plane derives it from the ledger
    /// rather than each publisher choosing one, and a snapshot whose class
    /// contradicts its account is refused at publication.
    #[must_use]
    pub const fn capacity_class(mut self, capacity_class: CapacityClass) -> Self {
        self.capacity_class = capacity_class;
        self
    }

    /// Name the product policy this snapshot was compiled from (#94).
    ///
    /// Optional, and omitting it leaves [`PolicyRevision::UNSTATED`] — a
    /// publisher that has no revision to state says nothing rather than
    /// inventing one. Unlike `budget`, this has a setter: the control plane
    /// compiling the snapshot is the value's author, not the store.
    #[must_use]
    pub const fn policy_revision(mut self, policy_revision: PolicyRevision) -> Self {
        self.policy_revision = policy_revision;
        self
    }

    #[must_use]
    pub fn build(self) -> AccountSnapshot {
        AccountSnapshot {
            account_id: self.account_id,
            key_id: self.key_id,
            generation: self.generation,
            status: self.status,
            capacity_class: self.capacity_class,
            enforcement_mode: self.enforcement_mode,
            valid_until: self.valid_until,
            permissions: self.permissions,
            limits: self.limits,
            cost_table: self.cost_table,
            // Absent by construction. A publisher describes policy; the
            // account's balance has one authority and it is the store.
            budget: None,
            policy_revision: self.policy_revision,
        }
    }
}

/// Why a compiled snapshot cannot be published.
///
/// The validation domain is the full `u64` configuration space. Arithmetic
/// is checked in the same `CostTable` implementation the request path uses;
/// overflow is a refusal, never a wrapped low quote (INVARIANTS #11/#16).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotValidationError {
    /// The carried compatibility pair cannot be represented by governor.
    /// It is validated even when disabled because an older reader will still
    /// enforce these values during a mixed-version rollout.
    WeightedRateOutsideGovernorDomain {
        units_per_second: u64,
        burst_units: u64,
    },
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
            SnapshotValidationError::WeightedRateOutsideGovernorDomain {
                units_per_second,
                burst_units,
            } => write!(
                f,
                "weighted rate ({units_per_second}/s, burst {burst_units}) must fit governor's non-zero u32 domain"
            ),
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
        let legacy_rate = snapshot.limits.legacy_weighted_rate();
        if legacy_rate.units_per_second() == 0
            || legacy_rate.units_per_second() > u64::from(u32::MAX)
            || legacy_rate.burst_units() == 0
            || legacy_rate.burst_units() > u64::from(u32::MAX)
        {
            return Err(SnapshotValidationError::WeightedRateOutsideGovernorDomain {
                units_per_second: legacy_rate.units_per_second(),
                burst_units: legacy_rate.burst_units(),
            });
        }
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
            .quote_weight(maximum_weight, snapshot.limits.max_items_per_request())
            .map_err(|_| SnapshotValidationError::QuoteOverflow {
                operation_index,
                max_items: snapshot.limits.max_items_per_request(),
            })?
            .total;
        let burst_units = CostUnits(legacy_rate.burst_units());
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
    /// [`try_new`](Self::try_new) checks the carried weighted-rate pair's
    /// domain, then checks `cost_table`'s worst quote at the batch cap against
    /// the carried compatibility burst and an elastic account's
    /// `overage_cap`. Neither `status` nor `generation` participates in those
    /// checks, so a snapshot that was publishable stays publishable under any
    /// value of either.
    ///
    /// Note what this method therefore must not grow: re-stamping the
    /// enforcement mode would change a value validation *does* depend on, and
    /// would need a fallible signature. An account-wide mode change goes
    /// through publication, not through here.
    ///
    /// This is what an account-wide status change needs (#22): the ledger and
    /// every live snapshot move together, and re-deriving a proof that cannot
    /// have changed would only invite an `expect` at each call site.
    /// Attach the ledger's budget view, carrying the publication proof over.
    ///
    /// Sound without revalidating for the reason [`restamped`](Self::restamped)
    /// gives: [`try_new`](Self::try_new) checks the weighted-rate pair's
    /// domain and the cost table's worst quote against the burst and any
    /// overage cap. The budget view participates in none of them, so a
    /// snapshot that was publishable stays publishable under any value of it.
    ///
    /// This exists so that the store, and only the store, can write the field:
    /// [`AccountSnapshot`]'s builder has no setter for it, so no caller
    /// outside this crate can put a balance into a snapshot except by
    /// deserializing one — which is exactly why the argument is an `Option`
    /// and a publish calls this *unconditionally*. An account the ledger does
    /// not hold clears the field rather than leaving it, so a submitted value
    /// can never survive publication. One fact, one writer — the rule #51
    /// established for status, applied to a number that moves far faster.
    #[must_use]
    pub fn with_budget(&self, budget: Option<BudgetView>) -> Self {
        let mut snapshot = AccountSnapshot::clone(&self.snapshot);
        snapshot.budget = budget;
        PublishableSnapshot {
            snapshot: Arc::new(snapshot),
            maximum_quote: self.maximum_quote,
        }
    }

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

    /// The same publication proof at a new execution-capacity class and
    /// generation (#99).
    ///
    /// Infallible for the reason [`restamped`](Self::restamped) is, and the
    /// reason is worth stating rather than inferring from the signature:
    /// [`try_new`](Self::try_new) validates the weighted-rate domain and the
    /// cost table's worst quote against burst and any overage cap. A capacity
    /// class participates in none of that — it decides whether an instance
    /// *starts* already-funded work, not what that work costs — so the proof
    /// carries over unchanged. Re-stamping the enforcement mode would not,
    /// which is why there is no method for it.
    #[must_use]
    pub fn reclassified(&self, class: CapacityClass, generation: Generation) -> Self {
        let mut snapshot = AccountSnapshot::clone(&self.snapshot);
        snapshot.capacity_class = class;
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
    /// Begin construction with the fields that have no meaningful default.
    ///
    /// `status` is deliberately required: inferring `Active` for an omitted
    /// administrative state would turn incomplete control-plane data into an
    /// authorization grant instead of failing closed (INVARIANTS.md #5).
    #[must_use]
    pub fn builder(
        account_id: AccountId,
        generation: Generation,
        status: AccountStatus,
        valid_until: Timestamp,
        permissions: PermissionBits,
        limits: ResolvedLimits,
        cost_table: Arc<CostTable>,
    ) -> AccountSnapshotBuilder {
        AccountSnapshotBuilder {
            account_id,
            key_id: None,
            generation,
            status,
            capacity_class: CapacityClass::Assured,
            enforcement_mode: EnforcementMode::Strict,
            valid_until,
            permissions,
            limits,
            cost_table,
            policy_revision: PolicyRevision::UNSTATED,
        }
    }

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

    /// The class's spellings must agree with serde's for the reason the
    /// status's must: the SQL `CHECK`, the JSONB republish predicate, the
    /// admin wire DTO, and the metric label all compare these exact strings.
    #[cfg(feature = "serde")]
    #[test]
    fn capacity_class_text_matches_its_serde_spelling() {
        for class in [CapacityClass::Assured, CapacityClass::BestEffort] {
            assert_eq!(
                serde_json::to_value(class).expect("a unit variant serializes"),
                serde_json::Value::String(class.as_str().to_owned()),
                "{class:?} disagrees with its serde spelling"
            );
        }
    }

    /// An unstated class is `Assured`, and that direction is the whole
    /// compatibility argument: it preserves the availability every account
    /// has today. `BestEffort` would make existing traffic sheddable the
    /// moment a gate was enabled.
    #[test]
    fn the_default_class_is_assured() {
        assert_eq!(CapacityClass::default(), CapacityClass::Assured);
        assert!(CapacityClass::Assured.may_use_assured_reserve());
        assert!(!CapacityClass::BestEffort.may_use_assured_reserve());
    }

    /// A snapshot from a control plane that predates #99 carries no class, and
    /// must decode as `Assured` rather than failing — the reader-first half of
    /// the rollout.
    #[cfg(feature = "serde")]
    #[test]
    fn a_snapshot_without_a_capacity_class_key_decodes_as_assured() {
        let snapshot = AccountSnapshot::builder(
            AccountId(1),
            Generation(1),
            AccountStatus::Active,
            Timestamp::from_second(10_000).unwrap(),
            PermissionBits::ALL,
            ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
            Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        )
        .capacity_class(CapacityClass::BestEffort)
        .build();

        let mut value = serde_json::to_value(&snapshot).expect("a snapshot serializes");
        assert_eq!(
            value["capacity_class"],
            serde_json::Value::String("BestEffort".to_owned()),
            "a stated class is on the wire in its canonical spelling"
        );
        assert!(
            value
                .as_object_mut()
                .expect("a snapshot is a JSON object")
                .remove("capacity_class")
                .is_some()
        );

        let decoded: AccountSnapshot =
            serde_json::from_value(value).expect("an older payload still decodes");
        assert_eq!(decoded.capacity_class, CapacityClass::Assured);
    }

    /// A builder that states no class states `Assured` rather than inventing
    /// one, and the class survives a re-stamp of the other account-owned fact.
    #[test]
    fn a_class_survives_a_status_restamp() {
        let snapshot = AccountSnapshot::builder(
            AccountId(1),
            Generation(1),
            AccountStatus::Active,
            Timestamp::from_second(10_000).unwrap(),
            PermissionBits::ALL,
            ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
            Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        );
        assert_eq!(
            snapshot.build().capacity_class,
            CapacityClass::Assured,
            "an unstated class is assured"
        );

        let publishable = PublishableSnapshot::try_new(Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(1),
                AccountStatus::Active,
                Timestamp::from_second(10_000).unwrap(),
                PermissionBits::ALL,
                ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
                Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
            )
            .capacity_class(CapacityClass::BestEffort)
            .build(),
        ))
        .expect("the fixture publishes");

        let restamped = publishable.restamped(AccountStatus::Suspended, Generation(2));
        assert_eq!(
            restamped.as_snapshot().capacity_class,
            CapacityClass::BestEffort,
            "a status change must not silently reclassify the account"
        );

        // And the mirror: a class change leaves the status alone.
        let reclassified = publishable.reclassified(CapacityClass::Assured, Generation(3));
        assert_eq!(
            reclassified.as_snapshot().capacity_class,
            CapacityClass::Assured
        );
        assert_eq!(reclassified.as_snapshot().status, AccountStatus::Active);
        assert_eq!(reclassified.as_snapshot().generation, Generation(3));
        assert!(
            PublishableSnapshot::try_new(Arc::new(AccountSnapshot::clone(
                reclassified.as_snapshot()
            )))
            .is_ok(),
            "reclassifying preserves the publication proof"
        );
    }

    /// A payload from a control plane that predates periodic budgets. It must
    /// decode, and it must decode to "nothing was said" — a required field
    /// would deny every principal until the whole catalogue was republished,
    /// and a defaulted zero would tell every caller they were out of quota.
    #[test]
    #[cfg(feature = "serde")]
    fn a_snapshot_without_a_budget_key_decodes_as_no_budget() {
        let snapshot = AccountSnapshot::builder(
            AccountId(1),
            Generation(1),
            AccountStatus::Active,
            Timestamp::from_second(10_000).unwrap(),
            PermissionBits::ALL,
            ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
            Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        )
        .build();

        let mut value = serde_json::to_value(&snapshot).expect("a snapshot serializes");
        let removed = value
            .as_object_mut()
            .expect("a snapshot is a JSON object")
            .remove("budget");
        assert!(removed.is_some(), "the field is on the wire when present");

        let decoded: AccountSnapshot =
            serde_json::from_value(value).expect("an older payload still decodes");
        assert_eq!(decoded.budget, None);
    }

    /// A control plane that predates #94 publishes no revision, and its
    /// snapshots must keep working. The absent key decodes to "unstated"
    /// rather than to a decode error, which is the whole reader-first half of
    /// the rollout.
    #[test]
    #[cfg(feature = "serde")]
    fn a_snapshot_without_a_revision_key_decodes_as_unstated() {
        let snapshot = AccountSnapshot::builder(
            AccountId(1),
            Generation(1),
            AccountStatus::Active,
            Timestamp::from_second(10_000).unwrap(),
            PermissionBits::ALL,
            ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
            Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        )
        .policy_revision(PolicyRevision([0x5a; 32]))
        .build();

        let mut value = serde_json::to_value(&snapshot).expect("a snapshot serializes");
        assert_eq!(
            value["policy_revision"],
            serde_json::Value::String("5a".repeat(32)),
            "a stated revision is on the wire in canonical form"
        );
        let removed = value
            .as_object_mut()
            .expect("a snapshot is a JSON object")
            .remove("policy_revision");
        assert!(removed.is_some(), "the field is on the wire when present");

        let decoded: AccountSnapshot =
            serde_json::from_value(value).expect("an older payload still decodes");
        assert_eq!(decoded.policy_revision, PolicyRevision::UNSTATED);
        assert!(decoded.policy_revision.is_unstated());
    }

    /// Omitting the setter states nothing rather than inventing a value.
    #[test]
    fn a_builder_without_a_revision_states_none() {
        let snapshot = AccountSnapshot::builder(
            AccountId(1),
            Generation(1),
            AccountStatus::Active,
            Timestamp::from_second(10_000).unwrap(),
            PermissionBits::ALL,
            ResolvedLimits::new(64),
            Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
        )
        .build();
        assert_eq!(snapshot.policy_revision, PolicyRevision::UNSTATED);
    }

    /// The store overwrites this field on every publish, so the clearing arm
    /// has to work as well as the setting one — that is what stops a value
    /// arriving over the wire from surviving publication.
    #[test]
    fn with_budget_replaces_and_can_clear() {
        let publishable = PublishableSnapshot::try_new(Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(1),
                AccountStatus::Active,
                Timestamp::from_second(10_000).unwrap(),
                PermissionBits::ALL,
                ResolvedLimits::new(64).with_weighted_rate(1_000, 1_000),
                Arc::new(CostTable::builder(CostUnits(1), CostUnits(1)).build()),
            )
            .build(),
        ))
        .expect("the test snapshot is publishable");
        assert_eq!(publishable.budget, None, "a builder cannot set one");

        let view = BudgetView {
            balance_at_publish: CostUnits(500),
            period_end: None,
        };
        let stamped = publishable.with_budget(Some(view));
        assert_eq!(stamped.budget, Some(view));
        assert_eq!(
            stamped.maximum_quote(),
            publishable.maximum_quote(),
            "the publication proof carries over: validation does not read the budget"
        );
        assert_eq!(stamped.with_budget(None).budget, None);
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

    #[test]
    fn concurrency_construction_rejects_a_principal_ceiling_above_the_account() {
        let account = NonZeroU32::new(4).unwrap();
        assert!(
            ResolvedLimits::new(64)
                .with_concurrency(account, Some(account))
                .is_ok(),
            "a principal ceiling equal to the account ceiling is a valid narrowing"
        );
        let principal = NonZeroU32::new(5).unwrap();
        assert_eq!(
            ResolvedLimits::new(64)
                .with_concurrency(account, Some(principal))
                .unwrap_err(),
            ResolvedLimitsError::PrincipalConcurrencyExceedsAccount { principal, account }
        );
    }

    #[test]
    fn configured_dimensions_are_visible_through_the_public_accessors() {
        let requests_per_second = NonZeroU32::new(10).unwrap();
        let burst_requests = NonZeroU32::new(20).unwrap();
        let account = NonZeroU32::new(4).unwrap();
        let principal = NonZeroU32::new(2).unwrap();
        let limits = ResolvedLimits::new(64)
            .with_request_rate(requests_per_second, burst_requests)
            .with_concurrency(account, Some(principal))
            .unwrap();

        let request_rate = limits.request_rate().expect("request rate is configured");
        assert_eq!(request_rate.requests_per_second(), requests_per_second);
        assert_eq!(request_rate.burst_requests(), burst_requests);
        assert_eq!(limits.max_concurrent_requests(), Some(account));
        assert_eq!(limits.principal_max_concurrent_requests(), Some(principal));
    }

    #[test]
    fn account_rate_policy_extracts_exactly_the_rate_dimensions() {
        let account = ResolvedLimits::new(999)
            .with_weighted_rate(700, 800)
            .with_request_rate(NonZeroU32::new(9).unwrap(), NonZeroU32::new(10).unwrap());
        let principal = ResolvedLimits::new(64)
            .with_weighted_rate_compatibility_fallback(11, 12)
            .with_concurrency(
                NonZeroU32::new(4).unwrap(),
                Some(NonZeroU32::new(2).unwrap()),
            )
            .unwrap();

        let policy = account.account_rate_policy();
        assert_eq!(policy.weighted_rate(), account.weighted_rate());
        assert_eq!(
            policy.legacy_weighted_rate(),
            account.legacy_weighted_rate()
        );
        assert_eq!(policy.request_rate(), account.request_rate());

        assert_eq!(principal.max_items_per_request(), 64);
        assert_eq!(principal.weighted_rate(), None);
        assert_eq!(principal.legacy_weighted_rate().units_per_second(), 11);
        assert_eq!(principal.legacy_weighted_rate().burst_units(), 12);
        assert_eq!(principal.request_rate(), None);
        assert_eq!(principal.max_concurrent_requests(), NonZeroU32::new(4));
        assert_eq!(
            principal.principal_max_concurrent_requests(),
            NonZeroU32::new(2)
        );
    }

    #[test]
    #[cfg(feature = "serde")]
    fn pre_staged_limits_decode_as_weighted_and_round_trip_canonically() {
        let old = serde_json::json!({
            "max_items_per_request": 64,
            "rate_units_per_second": 1_000,
            "rate_burst_units": 2_000
        });
        let limits: ResolvedLimits = serde_json::from_value(old.clone()).unwrap();
        assert_eq!(
            limits.weighted_rate(),
            Some(WeightedRateLimit {
                units_per_second: 1_000,
                burst_units: 2_000,
            })
        );
        assert_eq!(serde_json::to_value(limits).unwrap(), old);
    }

    #[test]
    #[cfg(feature = "serde")]
    fn disabled_weighted_rate_preserves_the_legacy_fallback() {
        let wire = serde_json::json!({
            "max_items_per_request": 64,
            "rate_units_per_second": 800,
            "rate_burst_units": 1_600,
            "weighted_rate_enabled": false
        });
        let limits: ResolvedLimits = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(limits.weighted_rate(), None);
        assert_eq!(limits.legacy_weighted_rate().units_per_second(), 800);
        assert_eq!(limits.legacy_weighted_rate().burst_units(), 1_600);
        assert_eq!(serde_json::to_value(limits).unwrap(), wire);
    }

    #[test]
    fn disabled_weighted_rate_still_validates_the_rollback_burst() {
        let mut snapshot = (*priced_snapshot(50, 50, &[(0, 1)], 64, 114)).clone();
        snapshot.limits =
            ResolvedLimits::new(64).with_weighted_rate_compatibility_fallback(1_000, 113);
        assert_eq!(
            PublishableSnapshot::try_new(Arc::new(snapshot)).unwrap_err(),
            SnapshotValidationError::QuoteExceedsBurst {
                operation_index: 0,
                max_quote: CostUnits(114),
                burst_units: CostUnits(113),
            }
        );
    }

    #[test]
    #[cfg(feature = "serde")]
    fn wire_rejects_partial_rate_pairs_and_widening_principal_limits() {
        let partial = serde_json::json!({
            "max_items_per_request": 64,
            "rate_units_per_second": 1_000,
            "rate_burst_units": 2_000,
            "request_rate_per_second": 10
        });
        assert!(serde_json::from_value::<ResolvedLimits>(partial).is_err());

        let widening = serde_json::json!({
            "max_items_per_request": 64,
            "rate_units_per_second": 1_000,
            "rate_burst_units": 2_000,
            "max_concurrent_requests": 4,
            "principal_max_concurrent_requests": 5
        });
        assert!(serde_json::from_value::<ResolvedLimits>(widening).is_err());

        let orphaned_principal = serde_json::json!({
            "max_items_per_request": 64,
            "rate_units_per_second": 1_000,
            "rate_burst_units": 2_000,
            "principal_max_concurrent_requests": 4
        });
        assert!(serde_json::from_value::<ResolvedLimits>(orphaned_principal).is_err());

        let equal = serde_json::json!({
            "max_items_per_request": 64,
            "rate_units_per_second": 1_000,
            "rate_burst_units": 2_000,
            "max_concurrent_requests": 4,
            "principal_max_concurrent_requests": 4
        });
        assert!(serde_json::from_value::<ResolvedLimits>(equal).is_ok());
    }

    #[test]
    fn publication_rejects_weighted_values_outside_governors_domain() {
        for limits in [
            ResolvedLimits::new(64).with_weighted_rate(0, 1),
            ResolvedLimits::new(64).with_weighted_rate(1, 0),
            ResolvedLimits::new(64).with_weighted_rate(u64::from(u32::MAX) + 1, 1),
            ResolvedLimits::new(64).with_weighted_rate(1, u64::from(u32::MAX) + 1),
        ] {
            let mut snapshot = snapshot(AccountStatus::Active, t(1_000));
            snapshot.limits = limits;
            assert!(matches!(
                PublishableSnapshot::try_new(Arc::new(snapshot)),
                Err(SnapshotValidationError::WeightedRateOutsideGovernorDomain { .. })
            ));
        }
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
            restamped.limits.max_items_per_request(),
            original.limits.max_items_per_request()
        );
        assert_eq!(
            restamped.limits.legacy_weighted_rate().burst_units(),
            original.limits.legacy_weighted_rate().burst_units()
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
        AccountSnapshot::builder(
            AccountId(1),
            Generation(1),
            status,
            valid_until,
            PermissionBits::bit(0).union(PermissionBits::bit(3)),
            ResolvedLimits::new(1024).with_weighted_rate(10_000, 50_000),
            Arc::new(CostTable::builder(CostUnits(50), CostUnits(50)).build()),
        )
        .key_id(KeyId(2))
        .build()
    }

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_second(secs).unwrap()
    }

    /// The administrative state is part of the constructor's type, not a
    /// setter callers can forget. This signature witness fails to compile if
    /// the builder ever regains a fail-open status default.
    #[test]
    fn builder_requires_account_status_at_construction() {
        type Builder = fn(
            AccountId,
            Generation,
            AccountStatus,
            Timestamp,
            PermissionBits,
            ResolvedLimits,
            Arc<CostTable>,
        ) -> AccountSnapshotBuilder;

        let _: Builder = AccountSnapshot::builder;
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
        Arc::new(
            AccountSnapshot::builder(
                AccountId(1),
                Generation(1),
                AccountStatus::Active,
                t(1_000),
                PermissionBits::bit(0).union(PermissionBits::bit(3)),
                ResolvedLimits::new(max_items).with_weighted_rate(1_000, burst),
                Arc::new(builder.build()),
            )
            .key_id(KeyId(2))
            .build(),
        )
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
            PublishableSnapshot::try_new(priced_snapshot(
                1,
                0,
                &[(0, u64::MAX)],
                2,
                u64::from(u32::MAX),
            ))
            .unwrap_err(),
            SnapshotValidationError::QuoteOverflow {
                operation_index: 0,
                max_items: 2,
            }
        );
    }

    #[test]
    fn publication_allows_a_table_with_no_registered_operations() {
        PublishableSnapshot::try_new(priced_snapshot(100, 100, &[], 64, 1)).unwrap();
    }
}

#[cfg(test)]
mod layout {
    use super::*;
    use std::mem::{align_of, offset_of, size_of};

    /// One cache line, and the admission path reads it once.
    ///
    /// Before `#[repr(C)]` these four fields were wherever the compiler put
    /// them — measured at offsets 160, 192, 200 and 32, so admitting a request
    /// touched two lines to read four values. Declaration order is now a
    /// contract, and this is what makes it one: a later field inserted among
    /// them, or a reorder that looks harmless, fails here rather than showing
    /// up as an unexplained regression in `snapshot/admit`.
    #[test]
    fn stage_one_fields_share_the_first_cache_line() {
        const LINE: usize = 64;
        for (name, offset) in [
            ("status", offset_of!(AccountSnapshot, status)),
            ("permissions", offset_of!(AccountSnapshot, permissions)),
            ("valid_until", offset_of!(AccountSnapshot, valid_until)),
            (
                "enforcement_mode",
                offset_of!(AccountSnapshot, enforcement_mode),
            ),
        ] {
            assert!(
                offset < LINE,
                "{name} sits at offset {offset}, past the first {LINE}-byte line \
                 the request path reads"
            );
        }
    }

    /// The revision is carried, never read, so it must not displace anything
    /// admission touches. Asserting it is *past* the first line is the half
    /// that would catch a well-meaning reorder putting it up front.
    #[test]
    fn the_policy_revision_is_cold() {
        assert!(
            offset_of!(AccountSnapshot, policy_revision) >= 64,
            "the policy revision belongs outside the stage-one cache line"
        );
    }

    /// The alignment keeps one account's snapshot off another's cache lines,
    /// and the size claim in `budget`'s documentation is now checked rather
    /// than asserted in prose: `policy_revision` lands in padding the
    /// `align(128)` had already reserved, so the type did not grow.
    #[test]
    fn the_snapshot_stays_one_two_line_object() {
        assert_eq!(align_of::<AccountSnapshot>(), 128);
        assert_eq!(
            size_of::<AccountSnapshot>(),
            256,
            "a snapshot that outgrew its two lines costs every account an \
             extra line; justify the growth or move the new field"
        );
    }
}
