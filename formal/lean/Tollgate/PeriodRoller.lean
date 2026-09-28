import Init.Omega

/-!
The GL-107 driver's abstract lifecycle. A cutoff is chosen only at pass start;
batch calls are owned sequentially and a stop is terminal. Store effects,
wall-clock bounds and scheduler fairness are outside this model. The store's
rollover conservation proofs and Rust paused-time tests are separate evidence.
-/
namespace Tollgate.PeriodRoller

inductive Phase | waiting | draining | stopped
  deriving DecidableEq, Repr
inductive Event | start (now : Nat) | call | full | completed | failed | expired | stop
  deriving DecidableEq, Repr

structure State where
  phase : Phase
  cutoff : Nat
  pending : Bool
  healthy : Bool
  calls : Nat
  settled : Nat

def owned (s : State) : Nat := if s.pending then 1 else 0
def safe (s : State) : Prop := s.calls = s.settled + owned s

def finish (s : State) (phase : Phase) (healthy : Bool) : State :=
  { s with phase := phase, healthy := healthy, pending := false, settled := s.settled + owned s }

def step (s : State) (e : Event) : State :=
  if s.phase = .stopped then s else
  match e with
  | .stop => finish s .stopped false
  | .start now =>
    if s.phase = .waiting then { s with phase := .draining, cutoff := now } else s
  | .call =>
    if s.phase = .draining && !s.pending then
      { s with pending := true, calls := s.calls + 1 }
    else s
  | .full => if s.phase = .draining && s.pending then finish s .draining s.healthy else s
  | .completed => if s.phase = .draining && s.pending then finish s .waiting true else s
  | .failed | .expired => finish s .waiting false

theorem ownership_preserved (s : State) (e : Event) (h : safe s) : safe (step s e) := by
  cases s with
  | mk phase cutoff pending healthy calls settled =>
    cases phase <;> cases pending <;> cases healthy <;> cases e <;>
      simp_all [safe, step, finish, owned] <;> omega

theorem at_most_one_owned_call (s : State) (h : safe s) :
    s.settled ≤ s.calls ∧ s.calls ≤ s.settled + 1 := by
  cases hp : s.pending <;> simp_all [safe, owned] <;> omega

theorem cutoff_is_frozen_during_a_pass (s : State) (e : Event)
    (h : s.phase = .draining) : (step s e).cutoff = s.cutoff := by
  cases e <;> cases hp : s.pending <;> simp [step, h, hp, finish]

theorem stopped_is_terminal (s : State) (e : Event) (h : s.phase = .stopped) :
    step s e = s := by
  simp [step, h]

theorem stop_ends_ownership (s : State) (h : s.phase ≠ .stopped) :
    (step s .stop).pending = false ∧ (step s .stop).healthy = false := by
  simp [step, h, finish]

theorem incomplete_pass_is_not_healthy (s : State) (h : s.phase ≠ .stopped) :
    (step s .failed).healthy = false ∧ (step s .expired).healthy = false := by
  simp [step, h, finish]

/-- A batch call is issued only inside a pass and only when none is owned. -/
theorem call_issues_iff_draining_and_idle (s : State) (h : s.phase ≠ .stopped) :
    (step s .call).calls = s.calls + 1 ↔ (s.phase = .draining ∧ s.pending = false) := by
  cases hp : s.phase <;> cases hq : s.pending <;> simp_all [step]

/-- A full batch settles the owned call and continues the same pass, keeping
its health; with no owned call it changes nothing. -/
theorem full_continues_the_pass (s : State) (h : s.phase = .draining) (hp : s.pending = true) :
    (step s .full).phase = .draining ∧ (step s .full).pending = false ∧
    (step s .full).healthy = s.healthy := by
  simp [step, h, hp, finish]

theorem full_without_a_call_changes_nothing (s : State) (hp : s.pending = false) :
    step s .full = s := by
  unfold step; split <;> simp [hp]

/-- Completion ends the pass healthy, and only for the owned call of a pass. -/
theorem completed_ends_the_pass_healthy (s : State) (h : s.phase = .draining) (hp : s.pending = true) :
    (step s .completed).phase = .waiting ∧ (step s .completed).healthy = true := by
  simp [step, h, hp, finish]

theorem completed_outside_a_pass_changes_nothing (s : State) (h : s.phase ≠ .draining) :
    step s .completed = s := by
  unfold step; split <;> simp [h]

theorem full_outside_a_pass_changes_nothing (s : State) (h : s.phase ≠ .draining) :
    step s .full = s := by
  unfold step; split <;> simp [h]

def initial : State := ⟨.waiting, 0, false, false, 0, 0⟩

/-- Nothing is healthy before a pass has completed. -/
theorem initially_unhealthy_and_idle : initial.healthy = false ∧ initial.pending = false := by
  simp [initial]
def run : List Event → State
  | [] => initial
  | e :: es => step (run es) e

theorem every_trace_is_safe (events : List Event) : safe (run events) := by
  induction events with
  | nil => rfl
  | cons e es ih => exact ownership_preserved (run es) e ih

end Tollgate.PeriodRoller
