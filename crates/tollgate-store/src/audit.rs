//! Evidence returned by administrative mutations, captured at their serialization
//! point. The HTTP boundary supplies identity and time; a second store read must
//! never be substituted for the state this operation actually replaced.

use tollgate_core::{AccountStatus, CapacityClass, CostUnits, Generation};

/// The fields one administrative operation owns, as an [`AdminReceipt`]
/// records them before and after (INVARIANTS.md 33).
///
/// Each variant is one operation's slice of state, not the whole account. With
/// the `wire` feature it serializes with a snake_case `kind` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "wire", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "wire", serde(tag = "kind", rename_all = "snake_case"))]
pub enum AdminState {
    /// No record existed: the `before` of an account or credential creation or of
    /// a first snapshot publication, and both states of a snapshot removal that
    /// found nothing.
    Absent,
    /// A newly created account, as its [`AccountConfig`](crate::AccountConfig) set it.
    AccountCreated {
        /// Opening balance, deposited as a top-up that does not expire at a period boundary.
        initial_balance: CostUnits,
        /// Administrative status at creation.
        status: AccountStatus,
        /// Execution-capacity class at creation.
        capacity_class: CapacityClass,
    },
    /// An account's funding counters around a deposit.
    Funding {
        /// The top-up part of the spendable balance: manually deposited units, which
        /// survive period boundaries. Excludes any periodic allowance.
        topup: CostUnits,
        /// Every unit ever deposited, the monotonic [`Conservation::deposited`](crate::Conservation::deposited) term.
        deposited: CostUnits,
    },
    /// An account's administrative status around a status change.
    Status {
        /// The status the ledger holds.
        status: AccountStatus,
    },
    /// An account's execution-capacity class around a class change.
    CapacityClass {
        /// The class the ledger holds.
        capacity_class: CapacityClass,
    },
    /// A publication's identity, not a duplicate of its entire policy graph.
    /// The principal is the audit target and generations are immutable.
    Snapshot {
        /// The generation of the stored snapshot or tombstone.
        generation: Generation,
        /// Whether the record is a revocation tombstone rather than a live snapshot.
        revoked: bool,
    },
    /// Durable credential identity and retirement state, without digest material.
    /// Expiry is independent of revocation; `revoked: false` does not imply live.
    Credential {
        /// The account that owns the credential.
        account_id: tollgate_core::AccountId,
        /// The credential's non-secret identifier.
        key_id: tollgate_core::KeyId,
        /// Whether the credential has been retired.
        revoked: bool,
    },
    /// An account's periodic allowance, whole. The schedule is three coupled
    /// values — allowance, period, rollover — and the row's own CHECK treats
    /// them as all-or-nothing, so auditing one without the others would record
    /// a state the ledger cannot hold. `None` is "no schedule", which is a
    /// different fact from an allowance of zero (GL-121).
    Budget {
        /// The schedule in force, or `None` when the account has none.
        schedule: Option<tollgate_core::BudgetSchedule>,
    },
}

/// A successful operation and the exact before/after states of the fields it
/// owns. An idempotent no-op returns equal states; a failed operation returns no
/// receipt and must never be logged as a confirmed mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminReceipt<T> {
    /// The operation's own result.
    pub outcome: T,
    /// The owned fields as the operation found them.
    pub before: AdminState,
    /// The owned fields as the operation left them; equal to `before` for an idempotent no-op.
    pub after: AdminState,
}

impl<T> AdminReceipt<T> {
    /// Assemble a receipt. `before` and `after` must be captured under the same
    /// lock or transaction as the write they describe.
    pub fn new(outcome: T, before: AdminState, after: AdminState) -> Self {
        Self {
            outcome,
            before,
            after,
        }
    }
}
