/-!
Exact state model for the request-visible snapshot cache and its separate
generation watermark. This mirrors
`crates/tollgate-admission/src/generation_model.rs`.

Scope: unbounded natural-number generations and atomic control-plane
transitions. Rust `Generation` is `u64`; its comparisons are exact and these
transitions perform no arithmetic, so there is no finite-precision gap inside
the modeled operations. Scheduling, hashing, and memory ordering remain Rust
implementation obligations covered by tests and the lock-free map design.
-/

namespace Tollgate.SnapshotCache

inductive Visible where
  | absent
  | present (generation : Nat)
  | negative
  deriving DecidableEq, Repr

structure State where
  watermark : Option Nat
  visible : Visible
  deriving DecidableEq, Repr

def acceptsPositive : Option Nat → Nat → Bool
  | none, _ => true
  | some current, incoming => current < incoming

def installPositive (state : State) (incoming : Nat) : State :=
  if acceptsPositive state.watermark incoming then
    { watermark := some incoming, visible := .present incoming }
  else
    state

def installRevoked (state : State) (incoming : Nat) : State :=
  match state.watermark with
  | none => { watermark := some incoming, visible := .negative }
  | some current =>
      if incoming < current then state
      else { watermark := some incoming, visible := .negative }

def installUnknown (state : State) : State :=
  { state with visible := .negative }

def evictVisible (state : State) : State :=
  { state with visible := .absent }

theorem eviction_preserves_watermark (state : State) :
    (evictVisible state).watermark = state.watermark := by
  rfl

theorem unknown_preserves_watermark (state : State) :
    (installUnknown state).watermark = state.watermark := by
  rfl

theorem positive_at_or_below_watermark_is_rejected
    (current incoming : Nat) (visible : Visible) (h : incoming ≤ current) :
    installPositive { watermark := some current, visible := visible } incoming =
      { watermark := some current, visible := visible } := by
  simp [installPositive, acceptsPositive, Nat.not_lt.mpr h]

theorem revoked_watermark_is_max
    (current incoming : Nat) (visible : Visible) :
    (installRevoked { watermark := some current, visible := visible } incoming).watermark =
      some (max current incoming) := by
  simp only [installRevoked]
  split
  · rename_i h
    rw [Nat.max_eq_left (Nat.le_of_lt h)]
  · rename_i h
    rw [Nat.max_eq_right (Nat.le_of_not_gt h)]

theorem revoked_then_evicted_rejects_replay
    (current revoked replay : Nat) (visible : Visible)
    (replay_le_revoked : replay ≤ revoked) :
    installPositive
        (evictVisible
          (installRevoked { watermark := some current, visible := visible } revoked))
        replay =
      { watermark := some (max current revoked), visible := .absent } := by
  by_cases h : revoked < current
  · simp [installRevoked, h, evictVisible, installPositive, acceptsPositive,
      Nat.not_lt.mpr (Nat.le_trans replay_le_revoked (Nat.le_of_lt h)),
      Nat.max_eq_left (Nat.le_of_lt h)]
  · have current_le_revoked := Nat.le_of_not_gt h
    simp [installRevoked, h, evictVisible, installPositive, acceptsPositive,
      Nat.not_lt.mpr replay_le_revoked, Nat.max_eq_right current_le_revoked]

end Tollgate.SnapshotCache
