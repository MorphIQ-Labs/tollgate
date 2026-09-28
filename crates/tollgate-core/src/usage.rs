//! Usage events: the billing record.
//!
//! Leases *bound* spend; usage events *are* what gets billed. Reconciliation
//! compares the two ledgers and steady-state drift is zero (INVARIANTS.md,
//! ledger-roles note). Events are idempotent on `request_id`, so batched
//! writers may retry whole batches freely (INVARIANTS.md GL-7).

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use jiff::Timestamp;

use crate::ids::{AccountId, FencingToken, KeyId, LeaseId, PolicyRevision, RequestId};
use crate::units::CostUnits;

/// Pre-reserved capacity for exactly one usage event.
///
/// Implementations must record without fallible I/O: obtaining a slot is the
/// backpressure decision, while consuming it is the committed-charge path.
pub trait UsageSlot: Send + 'static {
    /// Record the charge into the capacity this slot reserved. Called from the
    /// committed guard's drop, so it must not block, allocate fallibly or
    /// fail.
    fn record(self, event: UsageEvent);
}

/// A [`UsageSlot`] that discards the event and counts it.
///
/// The reference implementation for an integration that has not reached usage
/// export yet, in the same spirit as `NoGate` for `CapacityGate`: it lets an
/// embedder adopt admission one stage at a time instead of taking the client
/// runtime and a background writer purely to satisfy a type.
///
/// It counts what it discarded, deliberately. A silently dropping slot makes
/// "usage is not wired up" indistinguishable from "no usage happened", and
/// those are very different operational stories — one of them means an
/// integration is admitting billable work and losing the record of it.
///
/// Not for production billing. Discarded events are not recoverable; use the
/// batching writer once usage must be durable.
///
/// Deliberately no `Default`. A zero-valued counter would make
/// `Default::default()` and `new()` the same value, leaving a mutant no test
/// can distinguish; an equivalent mutant is a design smell rather than a
/// coverage gap. `new` returning `Arc<Self>` also keeps clippy's
/// `new_without_default` inapplicable, so the two gates agree rather than
/// pulling opposite ways.
///
/// ```
/// # use tollgate_core::DiscardedUsage;
/// let discarded = DiscardedUsage::new();
/// // hand `discarded.slot()` to `admit`; assert on the count later
/// assert_eq!(discarded.count(), 0);
/// ```
#[derive(Debug)]
pub struct DiscardedUsage {
    count: AtomicU64,
}

impl DiscardedUsage {
    /// A shared counter, ready to hand out slots.
    ///
    /// Returns `Arc<Self>` because that is the only useful form: [`Self::slot`]
    /// needs one, and a bare `DiscardedUsage` can do nothing. Making the
    /// reachable shape the only constructible one removes a step an embedder
    /// would otherwise have to know to take.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicU64::new(0),
        })
    }

    /// A slot that discards one event into this counter.
    #[must_use]
    pub fn slot(self: &Arc<Self>) -> DiscardedUsageSlot {
        DiscardedUsageSlot(Arc::clone(self))
    }

    /// How many events have been discarded.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

/// One pre-reserved discard, obtained from [`DiscardedUsage::slot`].
#[derive(Debug)]
pub struct DiscardedUsageSlot(Arc<DiscardedUsage>);

impl UsageSlot for DiscardedUsageSlot {
    fn record(self, _event: UsageEvent) {
        self.0.count.fetch_add(1, Ordering::Relaxed);
    }
}

/// What funded the units in a [`UsageEvent`].
///
/// Two variants, because two things fund spend and they are validated by
/// opposite rules. A leased charge arrives with a capability the sink checks
/// before it touches the ledger; an overage charge has no capability to check,
/// because no lease existed to issue one.
///
/// Making this a sum type rather than a pair of `Option`s is deliberate. A
/// half-formed capability — a lease id with no token, or a token naming no
/// lease — is the shape a sink would have to defend against, and here it
/// cannot be written down. The storage schema mirrors the same constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UsageSource {
    /// Spent from a lease. The sink requires the stored
    /// `(lease_id, account_id, fencing_token)` triple to match before the
    /// event may change either ledger; token age relative to another active
    /// lease is irrelevant (INVARIANTS.md GL-4).
    Leased {
        /// The lease the units were debited from.
        lease_id: LeaseId,
        /// That lease's capability token.
        fencing_token: FencingToken,
    },
    /// Admitted under [`EnforcementMode::Elastic`] with no lease behind it.
    ///
    /// **It carries no lease id on purpose, and must never be given one.**
    /// Attributing unfunded spend to a real lease drives that lease's `used`
    /// past its `granted`, which makes reclaim's `granted - used` credit
    /// negative — a panic in the memory backend and a permanently failing
    /// transaction in Postgres, so the account's expired leases would never be
    /// reclaimed again. Overage stands outside lease accounting entirely and
    /// is funded by its own ledger term.
    ///
    /// [`EnforcementMode::Elastic`]: crate::snapshot::EnforcementMode::Elastic
    Overage,
}

impl UsageSource {
    /// The lease this charge was spent from, or `None` for overage.
    #[must_use]
    pub const fn lease_id(self) -> Option<LeaseId> {
        match self {
            UsageSource::Leased { lease_id, .. } => Some(lease_id),
            UsageSource::Overage => None,
        }
    }

    /// The referenced lease's capability token, or `None` for overage.
    #[must_use]
    pub const fn fencing_token(self) -> Option<FencingToken> {
        match self {
            UsageSource::Leased { fencing_token, .. } => Some(fencing_token),
            UsageSource::Overage => None,
        }
    }
}

/// One committed charge. Produced only from a committed
/// [`crate::reservation::Reservation`]; there is deliberately no public
/// constructor path for uncommitted work.
///
/// `#[non_exhaustive]` is what makes that sentence true outside this crate
/// rather than merely stated. The type had said "produced only from a
/// committed reservation" while remaining a plain struct literal any crate
/// could fill in, which is a convention rather than a boundary; a caller could
/// assemble an event for work that never committed and hand it to a sink.
/// Construction now goes through [`UsageEvent::new`], and the sealing has a
/// second benefit the workspace pays for once: a field added here no longer
/// breaks every literal in every downstream test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct UsageEvent {
    /// Idempotency key: a sink must treat a replayed `request_id` as the same
    /// charge, not a new one.
    ///
    /// The only field of this struct that is independent of how the units were
    /// funded, which is what lets overage replay under the same rule as any
    /// other event (INVARIANTS.md GL-7).
    pub request_id: RequestId,
    /// The account billed.
    pub account_id: AccountId,
    /// What funded the units, and the evidence the sink validates.
    pub source: UsageSource,
    /// The units charged: the quote reserved at admission.
    pub units: CostUnits,
    /// When the charge was committed, at execution start.
    pub occurred_at: Timestamp,
    /// The consuming application's policy identity, copied from the pinned
    /// snapshot that priced this request (GL-94).
    ///
    /// Carried so a billing record can be traced to the exact product policy
    /// that produced it. Tollgate never reads it, and an event from a
    /// publisher that stated no revision carries
    /// [`PolicyRevision::UNSTATED`].
    ///
    /// Defaults on the wire, so an ingest from a peer that predates the field
    /// decodes rather than failing — the same rule the snapshot's optional
    /// fields follow.
    #[cfg_attr(feature = "serde", serde(default))]
    pub policy_revision: PolicyRevision,
    /// Credential named by the pinned, key-scoped snapshot. An absent value
    /// preserves billing but supplies no credential activity. This metadata
    /// is never authorization evidence; the sink checks account ownership.
    #[cfg_attr(feature = "serde", serde(default))]
    pub key_id: Option<KeyId>,
}

impl UsageEvent {
    /// Build a committed charge.
    ///
    /// Called by [`Reservation::usage_event`], which is the only place that
    /// can prove the charge committed. It is public because tests, benchmarks,
    /// and store backends across the workspace need to construct events
    /// without a live reservation; what `#[non_exhaustive]` buys is that every
    /// such construction goes through one signature, so a new field reaches
    /// them as a compile error at one call each rather than as a silent
    /// default.
    ///
    /// [`Reservation::usage_event`]: crate::reservation::Reservation::usage_event
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        account_id: AccountId,
        source: UsageSource,
        units: CostUnits,
        occurred_at: Timestamp,
        policy_revision: PolicyRevision,
        key_id: Option<KeyId>,
    ) -> Self {
        Self {
            request_id,
            account_id,
            source,
            units,
            occurred_at,
            policy_revision,
            key_id,
        }
    }
}

#[cfg(all(test, feature = "serde"))]
mod revision_wire_tests {
    use super::*;

    fn event(revision: PolicyRevision) -> UsageEvent {
        UsageEvent::new(
            RequestId(9),
            AccountId(1),
            UsageSource::Overage,
            CostUnits(70),
            Timestamp::UNIX_EPOCH,
            revision,
            None,
        )
    }

    /// A peer that predates GL-94 sends no revision, and its events must ingest
    /// rather than fail. The absent key decodes to "unstated" — a value, not
    /// an error — which is what makes the additive rollout safe in both
    /// directions.
    #[test]
    fn an_event_without_a_revision_key_decodes_as_unstated() {
        let mut value =
            serde_json::to_value(event(PolicyRevision([0xc3; 32]))).expect("an event serializes");
        assert_eq!(
            value["policy_revision"],
            serde_json::Value::String("c3".repeat(32)),
            "a stated revision is on the wire in canonical form"
        );
        assert!(
            value
                .as_object_mut()
                .expect("an event is a JSON object")
                .remove("policy_revision")
                .is_some()
        );

        let decoded: UsageEvent =
            serde_json::from_value(value).expect("an older payload still decodes");
        assert_eq!(decoded.policy_revision, PolicyRevision::UNSTATED);
    }

    /// The revision survives the wire byte for byte. It is compared for
    /// equality by the consumer to select its own metadata, so a value that
    /// round-tripped to something else would silently name a different policy.
    #[test]
    fn a_revision_survives_the_event_round_trip_exactly() {
        let mut bytes = [0u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = (index as u8).wrapping_mul(7).wrapping_add(1);
        }
        let original = event(PolicyRevision(bytes));
        let encoded = serde_json::to_string(&original).expect("an event serializes");
        let decoded: UsageEvent = serde_json::from_str(&encoded).expect("it decodes");
        assert_eq!(decoded, original);
        assert_eq!(decoded.policy_revision.as_bytes(), &bytes);
    }
}

#[cfg(test)]
mod discarded_usage_tests {
    use super::*;

    fn event(request: u128) -> UsageEvent {
        UsageEvent::new(
            RequestId(request),
            AccountId(1),
            UsageSource::Overage,
            CostUnits(70),
            Timestamp::UNIX_EPOCH,
            PolicyRevision::UNSTATED,
            None,
        )
    }

    #[test]
    fn discarding_is_counted_not_silent() {
        let discarded = DiscardedUsage::new();
        assert_eq!(discarded.count(), 0);

        discarded.slot().record(event(1));
        discarded.slot().record(event(2));

        // The count is the whole point: without it, an integration that is
        // admitting billable work and losing every record of it looks exactly
        // like one that has served nothing.
        assert_eq!(discarded.count(), 2);
    }

    #[test]
    fn slots_are_independent_and_share_one_counter() {
        let discarded = DiscardedUsage::new();
        let first = discarded.slot();
        let second = discarded.slot();

        // A slot is pre-reserved capacity for exactly one event, so holding
        // two and consuming them in either order must total two.
        second.record(event(2));
        first.record(event(1));

        assert_eq!(discarded.count(), 2);
    }

    #[test]
    fn counts_across_threads() {
        let discarded = DiscardedUsage::new();
        std::thread::scope(|scope| {
            for index in 0..8u128 {
                let discarded = Arc::clone(&discarded);
                scope.spawn(move || discarded.slot().record(event(index)));
            }
        });
        assert_eq!(discarded.count(), 8);
    }
}
