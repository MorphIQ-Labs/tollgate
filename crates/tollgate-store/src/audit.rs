//! Evidence returned by administrative mutations, captured at their serialization
//! point. The HTTP boundary supplies identity and time; a second store read must
//! never be substituted for the state this operation actually replaced.

use tollgate_core::{AccountStatus, CapacityClass, CostUnits, Generation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "wire", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "wire", serde(tag = "kind", rename_all = "snake_case"))]
pub enum AdminState {
    Absent,
    AccountCreated {
        initial_balance: CostUnits,
        status: AccountStatus,
        capacity_class: CapacityClass,
    },
    Funding {
        topup: CostUnits,
        deposited: CostUnits,
    },
    Status {
        status: AccountStatus,
    },
    CapacityClass {
        capacity_class: CapacityClass,
    },
    /// A publication's identity, not a duplicate of its entire policy graph.
    /// The principal is the audit target and generations are immutable.
    Snapshot {
        generation: Generation,
        revoked: bool,
    },
    /// Durable credential identity and retirement state, without digest material.
    /// Expiry is independent of revocation; `revoked: false` does not imply live.
    Credential {
        account_id: tollgate_core::AccountId,
        key_id: tollgate_core::KeyId,
        revoked: bool,
    },
    /// An account's periodic allowance, whole. The schedule is three coupled
    /// values — allowance, period, rollover — and the row's own CHECK treats
    /// them as all-or-nothing, so auditing one without the others would record
    /// a state the ledger cannot hold. `None` is "no schedule", which is a
    /// different fact from an allowance of zero (GL-121).
    Budget {
        schedule: Option<tollgate_core::BudgetSchedule>,
    },
}

/// A successful operation and the exact before/after states of the fields it
/// owns. An idempotent no-op returns equal states; a failed operation returns no
/// receipt and must never be logged as a confirmed mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminReceipt<T> {
    pub outcome: T,
    pub before: AdminState,
    pub after: AdminState,
}

impl<T> AdminReceipt<T> {
    pub fn new(outcome: T, before: AdminState, after: AdminState) -> Self {
        Self {
            outcome,
            before,
            after,
        }
    }
}
