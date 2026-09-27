import Init.Omega

/-!
Abstract per-account supervisor ownership (GL-95). A retiring task is still
owned until its join; reactivation cannot create a second owner. Time and
backend outcomes are abstract inputs. The model proves transition safety,
not scheduler fairness or a Rust refinement. Paused-time integration tests
connect those assumptions to actual timers, task joins, and lease release.
-/
namespace Tollgate.AccountLifecycle

inductive Phase | dormant | running | lingering | retiring | backoff
  deriving DecidableEq, Repr
inductive Event | live | idle | elapsed | joined | died | stop
  deriving DecidableEq, Repr

structure State where
  phase : Phase
  desired : Bool
  stopping : Bool
  starts : Nat
  joins : Nat

def owned : Phase → Nat
  | .running | .lingering | .retiring => 1
  | _ => 0

def safe (s : State) : Prop := s.starts = s.joins + owned s.phase

def start (s : State) : State :=
  { s with phase := .running, starts := s.starts + 1 }

def step (s : State) : Event → State
  | .stop => { s with stopping := true, desired := false, phase := if owned s.phase = 1 then .retiring else .dormant }
  | .live =>
    if s.stopping then s else
    match s.phase with
    | .dormant => start { s with desired := true }
    | .lingering => { s with desired := true, phase := .running }
    | _ => { s with desired := true }
  | .idle => { s with desired := false, phase :=
      match s.phase with
      | .running => .lingering
      | .backoff => .dormant
      | other => other }
  | .elapsed =>
    match s.phase with
    | .lingering => { s with phase := .retiring }
    | .backoff => if s.desired && !s.stopping then start s else { s with phase := .dormant }
    | _ => s
  | .died =>
    match s.phase with
    | .running | .lingering => { s with phase := .retiring }
    | _ => s
  | .joined =>
    match s.phase with
    | .retiring => { s with joins := s.joins + 1, phase := if s.desired && !s.stopping then .backoff else .dormant }
    | _ => s

theorem ownership_preserved (s : State) (e : Event) (h : safe s) : safe (step s e) := by
  cases s with
  | mk phase desired stopping starts joins =>
    cases phase <;> cases desired <;> cases stopping <;> cases e <;>
      simp_all [safe, step, start, owned] <;> omega

theorem at_most_one_owner (s : State) (h : safe s) :
    s.joins ≤ s.starts ∧ s.starts ≤ s.joins + 1 := by
  cases hp : s.phase <;> simp_all [safe, owned] <;> omega

theorem reactivation_waits_for_join (s : State) (h : s.phase = .retiring) :
    (step s .live).starts = s.starts := by
  simp [step, h]
  split <;> rfl

theorem expired_linger_retires (s : State) (h : s.phase = .lingering) :
    (step s .elapsed).phase = .retiring := by
  simp [step, h]

theorem duplicate_join_is_inert (s : State) (h : s.phase ≠ .retiring) :
    step s .joined = s := by
  cases hp : s.phase <;> simp_all [step]

theorem stopping_never_starts (s : State) (e : Event) (h : s.stopping = true) :
    (step s e).starts = s.starts := by
  cases hp : s.phase <;> cases hd : s.desired <;> cases e <;> simp_all [step]

def initial : State := ⟨.dormant, false, false, 0, 0⟩
def run : List Event → State
  | [] => initial
  | e :: es => step (run es) e

theorem every_trace_is_safe (events : List Event) : safe (run events) := by
  induction events with
  | nil => rfl
  | cons e es ih => exact ownership_preserved (run es) e ih

/-- The worst possible in-memory sum on a target with at most 64-bit usize.
Every slot contributes at most one u64 value to each diagnostic sum. -/
theorem catalogue_total_fits (accounts units : Nat)
    (ha : accounts ≤ 2^64 - 1) (hu : units ≤ 2^64 - 1) :
    accounts * units < 2^128 := by
  calc
    accounts * units ≤ (2^64 - 1) * (2^64 - 1) := Nat.mul_le_mul ha hu
    _ < 2^128 := by decide

/-- Terminal inventory includes the capability inherited at manager start.
The implementation records unknown acquire outcomes separately; this equation
only concerns known grants with non-overflowed counters. -/
theorem parked_inventory (opening acquired released abandoned current parked : Nat)
    (h : opening + acquired = released + abandoned + current + parked) :
    opening + acquired - released - abandoned - current = parked := by
  omega

/-- A successful consolidation settles one known grant and acquires one.
Both counters advance, leaving the parked inventory unchanged. -/
theorem consolidation_preserves_parked_inventory
    (opening acquired released abandoned current parked : Nat)
    (h : opening + acquired = released + abandoned + current + parked) :
    opening + (acquired + 1) - (released + 1) - abandoned - current = parked := by
  omega

end Tollgate.AccountLifecycle
