/-!
Exact ledger and publication model for #128. All units are natural numbers.
The ledger lock and checked finite-width decoding must establish the equation
before using these results. Unreported usage remains in outstanding funding;
a zero allocatable balance alone cannot establish exhaustion.

The epoch models identity of the Arc token, not a wrapping integer in Rust.
Mutex serialization is a precondition. These results do not prove SQL isolation,
HTTP delivery, freshness of a remote answer, or machine atomic memory ordering.
-/
namespace Tollgate.BalanceExhaustion

theorem no_funding_remains (funded consumed balance outstanding : Nat)
    (ledger : funded = consumed + balance + outstanding)
    (empty : funded = consumed) : balance = 0 ∧ outstanding = 0 := by
  omega

theorem allocatable_zero_can_leave_funding :
    ∃ funded consumed balance outstanding : Nat,
      funded = consumed + balance + outstanding ∧ balance = 0 ∧ outstanding > 0 := by
  exact ⟨100, 0, 0, 100, by decide⟩

theorem uncommitted_consumption_is_not_exhaustion (funded consumed removed : Nat)
    (temporaryZero : funded = consumed + removed) (rollbackRestores : 0 < removed) :
    consumed < funded := by
  omega

structure State where
  epoch : Nat
  deadline : Option Int
  deriving DecidableEq

def invalidate (s : State) : State := ⟨s.epoch + 1, none⟩

def report (s : State) (attempt : Nat) (deadline : Int) : State :=
  if attempt = s.epoch then { s with deadline := some deadline } else s

def exhausted (s : State) (now : Int) : Prop :=
  ∃ deadline, s.deadline = some deadline ∧ now < deadline

theorem late_response_cannot_restore (s : State) (deadline : Int) :
    report (invalidate s) s.epoch deadline = invalidate s := by
  simp [report, invalidate]

theorem boundary_invalidates (s : State) (deadline now : Int)
    (held : s.deadline = some deadline) (past : deadline ≤ now) :
    ¬ exhausted s now := by
  simp [exhausted, held]
  omega

theorem flooring_cannot_extend (floor exact now : Int)
    (conservative : floor ≤ exact) (live : now < floor) : now < exact := by
  omega

end Tollgate.BalanceExhaustion
