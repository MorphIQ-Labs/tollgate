import Init.Omega

/-!
GL-105: one credential's projection of accepted canonical usage. Timestamps are
integer microseconds, including negative values. Attribution and acceptance
are inputs already classified by the backend. A repeated request ID never
provides fresh evidence, even when its payload changes.

This exact model assumes atomic transactions and correct identity/account
classification. It does not prove authentication, PostgreSQL execution, Rust
refinement, finite timestamp conversion, transport, or lossless delivery.
-/
namespace Tollgate.CredentialActivity

def merge (previous : Option Int) (instant : Int) : Option Int :=
  some (match previous with | none => instant | some old => max old instant)

theorem merge_idempotent (previous : Option Int) (instant : Int) :
    merge (merge previous instant) instant = merge previous instant := by
  cases previous <;> simp [merge, Int.max_assoc]

theorem merge_commutes (previous : Option Int) (a b : Int) :
    merge (merge previous a) b = merge (merge previous b) a := by
  cases previous <;> simp only [merge, Option.some.injEq] <;> omega

theorem merge_preserves_predecessor (old instant : Int) :
    old ≤ (merge (some old) instant).getD old := by
  simp only [merge, Option.getD_some]
  exact Int.le_max_left old instant

structure Event where
  request : Nat
  instant : Int
  units : Nat
  accepted : Bool
  attributed : Bool

structure State where
  seen : List Nat
  activity : Option Int
  billed : Nat
  revision : Nat

def step (s : State) (e : Event) : State :=
  if e.request ∈ s.seen then s
  else if e.accepted then
    { s with seen := e.request :: s.seen, billed := s.billed + e.units, activity := if e.attributed then merge s.activity e.instant else s.activity }
  else s

theorem duplicate_changes_nothing (s : State) (e : Event)
    (h : e.request ∈ s.seen) : step s e = s := by
  simp [step, h]

theorem altered_replay_keeps_the_first_event (s : State) (first replay : Event)
    (accepted : first.accepted = true) (same : replay.request = first.request) :
    step (step s first) replay = step s first := by
  by_cases h : first.request ∈ s.seen <;> simp [step, h, accepted, same]

theorem rejected_changes_nothing (s : State) (e : Event)
    (h : e.accepted = false) : step s e = s := by
  simp [step, h]

theorem activity_never_changes_the_revision (s : State) (e : Event) :
    (step s e).revision = s.revision := by
  by_cases h : e.request ∈ s.seen <;> cases ha : e.accepted <;> simp [step, h, ha]

theorem unattributed_work_still_bills (s : State) (e : Event)
    (fresh : e.request ∉ s.seen) (accepted : e.accepted = true)
    (unattributed : e.attributed = false) :
    (step s e).billed = s.billed + e.units ∧ (step s e).activity = s.activity := by
  simp [step, fresh, accepted, unattributed]

def transaction (s : State) (events : List Event) (succeeds : Bool) : State :=
  if succeeds then events.foldl step s else s

theorem failed_transaction_preserves_every_component (s : State) (events : List Event) :
    transaction s events false = s := by rfl

inductive Outcome | accepted (attributed : Bool) | duplicate | rejected

structure Report where
  accepted : Nat
  duplicate : Nat
  rejected : Nat
  unattributed : Nat

def tally (r : Report) (o : Outcome) : Report :=
  match o with
  | .accepted attributed => { r with accepted := r.accepted + 1, unattributed := r.unattributed + (if attributed then 0 else 1) }
  | .duplicate => { r with duplicate := r.duplicate + 1 }
  | .rejected => { r with rejected := r.rejected + 1 }

def valid (r : Report) (submitted : Nat) : Prop :=
  r.accepted + r.duplicate + r.rejected = submitted ∧ r.unattributed ≤ r.accepted

theorem acknowledgement_partitions_each_input (r : Report) (o : Outcome) (n : Nat)
    (h : valid r n) : valid (tally r o) (n + 1) := by
  cases o with
  | accepted attributed => cases attributed <;> simp_all [valid, tally] <;> omega
  | duplicate => simp_all [valid, tally] <;> omega
  | rejected => simp_all [valid, tally] <;> omega

end Tollgate.CredentialActivity
