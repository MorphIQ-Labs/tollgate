/-!
Exact ledger and publication model for #128 and #130. All units are natural
numbers. The ledger lock and checked finite-width decoding must establish the
equation before using these results. Unreported usage remains in outstanding
funding; a zero allocatable balance alone cannot establish exhaustion, and the
ledger's remaining funding is only an upper bound on what can still be spent.

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

/-! ### Shortfall (#130)

Ledger remaining is `funded - recorded`. Consumption not yet reported can only
lower what is truly left, so remaining is an upper bound, and a quote above it
cannot be funded. A quote at or below it proves nothing either way. -/

theorem ledger_remaining_bounds_true_remaining (funded recorded unreported : Nat)
    (consistent : recorded + unreported ≤ funded) :
    funded - (recorded + unreported) ≤ funded - recorded := by
  omega

theorem quote_above_evidence_is_unfundable (quote evidence truth : Nat)
    (bound : truth ≤ evidence) (above : evidence < quote) : truth < quote := by
  omega

theorem quote_within_evidence_proves_nothing :
    ∃ quote evidence truth : Nat, truth ≤ evidence ∧ quote ≤ evidence ∧ truth < quote := by
  exact ⟨5, 10, 1, by decide⟩

/-- Grant evidence is published through `report`'s epoch test: a grant whose
call overlapped an accepted funding change cannot restore older evidence. -/
theorem late_grant_evidence_cannot_restore (s : State) (deadline : Int) :
    report (invalidate s) s.epoch deadline = invalidate s :=
  late_response_cannot_restore s deadline

/-! ### Paired publication (seqlock)

The slot stores `(deadline, remaining)` in two words. A serialized writer makes
the sequence odd, writes both, and makes it even again; invalidation clears only
the deadline. A reader that sees the same even sequence before and after its
reads accepts. The model is sequentially consistent: it proves the protocol,
not the Rust memory-ordering argument (the release/acquire fences) that
realizes it. -/

structure Cell where
  seq : Nat
  deadline : Option Int
  remaining : Nat

inductive Step where
  | begin
  | write (deadline : Int) (remaining : Nat)
  | finish
  | clear

def Step.allowed (c : Cell) : Step → Prop
  | .begin => c.seq % 2 = 0
  | .write _ _ => c.seq % 2 = 1
  | .finish => c.seq % 2 = 1
  | .clear => True

def Step.apply (c : Cell) : Step → Cell
  | .begin => { c with seq := c.seq + 1 }
  | .write d r => { c with deadline := some d, remaining := r }
  | .finish => { c with seq := c.seq + 1 }
  | .clear => { c with deadline := none }

/-- A trace the serialized writer protocol permits. -/
def Run : Cell → List Step → Prop
  | _, [] => True
  | c, s :: rest => s.allowed c ∧ Run (s.apply c) rest

def final : Cell → List Step → Cell
  | c, [] => c
  | c, s :: rest => final (s.apply c) rest

theorem seq_monotone (c : Cell) (steps : List Step) : c.seq ≤ (final c steps).seq := by
  induction steps generalizing c with
  | nil => simp [final]
  | cons s rest ih =>
    have next := ih (s.apply c)
    cases s <;> simp only [final, Step.apply] at next ⊢ <;> omega

/-- Between two equal even sequence reads, only invalidation can have run. -/
theorem stable_window (c : Cell) (steps : List Step) (run : Run c steps)
    (even : c.seq % 2 = 0) (same : (final c steps).seq = c.seq) (n : Nat) :
    (final c (steps.take n)).remaining = c.remaining ∧
      ((final c (steps.take n)).deadline = c.deadline ∨
        (final c (steps.take n)).deadline = none) := by
  induction steps generalizing c n with
  | nil => simp [final]
  | cons s rest ih =>
    obtain ⟨allowed, rest_run⟩ := run
    cases s with
    | begin =>
      have grows := seq_monotone (Step.apply c .begin) rest
      simp only [final, Step.apply] at same grows
      omega
    | write d r =>
      simp only [Step.allowed] at allowed
      omega
    | finish =>
      simp only [Step.allowed] at allowed
      omega
    | clear =>
      cases n with
      | zero => simp [final]
      | succ n =>
        simp only [final, List.take_succ_cons] at same ⊢
        have inner := ih (Step.apply c .clear) rest_run (by simpa [Step.apply] using even)
          (by simpa [Step.apply] using same) n
        refine ⟨by simpa [Step.apply] using inner.1, Or.inr ?_⟩
        rcases inner.2 with h | h <;> simpa [Step.apply] using h

/-- An accepted read pairs a live deadline with the remaining of the same
publication, whatever prefixes the two loads observed. -/
theorem reader_pairs_one_publication (c : Cell) (steps : List Step) (run : Run c steps)
    (even : c.seq % 2 = 0) (same : (final c steps).seq = c.seq) (i j : Nat) (d : Int)
    (seen : (final c (steps.take i)).deadline = some d) :
    c.deadline = some d ∧ (final c (steps.take j)).remaining = c.remaining := by
  refine ⟨?_, (stable_window c steps run even same j).1⟩
  rcases (stable_window c steps run even same i).2 with h | h
  · rw [← h, seen]
  · rw [h] at seen
    cases seen

end Tollgate.BalanceExhaustion
