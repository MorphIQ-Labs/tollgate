/-!
Exact state model for the request-visible snapshot cache and its separate
generation watermark. This mirrors
`crates/tollgate-admission/src/generation_model.rs`.

Scope: unbounded natural-number generations and atomic control-plane
transitions. Rust `Generation` is `u64`; its comparisons are exact and these
transitions perform no arithmetic, so there is no finite-precision gap inside
the modeled operations. Scheduling, hashing, and memory ordering remain Rust
implementation obligations covered by tests and the lock-free map design.

The watermark records *why* it exists, and that is the content of issue GL-53. A
generation this instance merely observed is not the same fact as a generation
the source published a revocation at. Collapsing them made an absent row
inherit the generation of the positive it replaced and then refuse it back
forever -- a state this file previously *proved* unreachable-from, by deriving
that the principal could never return.

One field, matching `Option<Watermark>` in the Rust. An earlier cut of this
model split it into two, which broke the correspondence the file exists for:
with separate fields, a delayed older revocation could pass the revocation
check while a newer positive sat in the other field, and the model admitted a
transition `accept_revoked` refuses.
-/

namespace Tollgate.SnapshotCache

inductive Visible where
  | absent
  | present (generation : Nat)
  | negative
  deriving DecidableEq, Repr

def Visible.isPresent : Visible → Bool
  | .present _ => true
  | _ => false

/-- A principal's durable generation, and why it is durable. -/
inductive Watermark where
  /-- The source published a revocation at this generation. -/
  | revoked (generation : Nat)
  /-- The newest positive this instance installed. -/
  | positive (generation : Nat)
  deriving DecidableEq, Repr

def Watermark.generation : Watermark → Nat
  | .revoked g => g
  | .positive g => g

def Watermark.isRevoked : Watermark → Bool
  | .revoked _ => true
  | .positive _ => false

structure State where
  watermark : Option Watermark
  visible : Visible
  deriving DecidableEq, Repr

/-- Refuse anything strictly older. Refuse an *equal* generation only when it is
dead (a revocation) or already installed (visible); otherwise it is a
re-observation, which is what lets a principal return from an absence at the
generation it always had. -/
def acceptsPositive (state : State) (incoming : Nat) : Bool :=
  match state.watermark with
  | none => true
  | some current =>
      !(incoming < current.generation ||
        (incoming = current.generation && (current.isRevoked || state.visible.isPresent)))

def installPositive (state : State) (incoming : Nat) : State :=
  if acceptsPositive state incoming then
    { watermark := some (.positive incoming), visible := .present incoming }
  else
    state

/-- A revocation is refused when it is strictly older than whatever the
principal already carries -- a delayed tombstone cannot revoke a newer positive
snapshot (INVARIANTS.md GL-15). -/
def installRevoked (state : State) (incoming : Nat) : State :=
  match state.watermark with
  | none => { watermark := some (.revoked incoming), visible := .negative }
  | some current =>
      if incoming < current.generation then state
      else { watermark := some (.revoked incoming), visible := .negative }

/-- An absent row denies locally and says nothing about any generation, so it
leaves the watermark exactly as it found it. In particular it does not promote
an observation into a revocation: that promotion is GL-53. -/
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

/-- An absence never makes a generation dead. This is the property whose
absence was issue GL-53. -/
theorem unknown_never_creates_a_revocation (state : State) (generation : Nat)
    (h : state.watermark = some (.positive generation)) :
    (installUnknown state).watermark = some (.positive generation) := by
  simpa [installUnknown] using h

/-- The half that must not loosen: a positive at or below a published
revocation is refused, equality included (INVARIANTS.md GL-15). -/
theorem positive_at_or_below_revocation_is_rejected
    (current incoming : Nat) (visible : Visible) (h : incoming ≤ current) :
    installPositive { watermark := some (.revoked current), visible := visible } incoming =
      { watermark := some (.revoked current), visible := visible } := by
  rcases Nat.lt_or_ge incoming current with hlt | hge
  · simp [installPositive, acceptsPositive, Watermark.generation, hlt]
  · have : incoming = current := Nat.le_antisymm h hge
    simp [installPositive, acceptsPositive, Watermark.generation, Watermark.isRevoked, this]

/-- GL-15's other half, which the two-field model silently lost: a delayed older
revocation cannot revoke a newer positive. -/
theorem older_revocation_cannot_revoke_a_newer_positive
    (current incoming : Nat) (visible : Visible) (h : incoming < current) :
    installRevoked { watermark := some (.positive current), visible := visible } incoming =
      { watermark := some (.positive current), visible := visible } := by
  simp [installRevoked, Watermark.generation, h]

/-- The fix, stated: observe a generation, lose the row, see the same
generation again -- and it is admitted. Under the pre-GL-53 model this was false,
and its negation was derivable. -/
theorem unknown_then_same_generation_positive_is_accepted (incoming : Nat) :
    installPositive
        (installUnknown
          (installPositive { watermark := none, visible := .absent } incoming))
        incoming =
      { watermark := some (.positive incoming), visible := .present incoming } := by
  simp [installPositive, installUnknown, acceptsPositive, Watermark.generation,
    Watermark.isRevoked, Visible.isPresent]

/-- A duplicate publish of a *visible* snapshot stays a no-op, so relaxing the
equal-generation rule does not turn every republish into a fresh install. -/
theorem visible_same_generation_positive_is_a_no_op (incoming : Nat) :
    installPositive
        { watermark := some (.positive incoming), visible := .present incoming }
        incoming =
      { watermark := some (.positive incoming), visible := .present incoming } := by
  simp [installPositive, acceptsPositive, Watermark.generation, Watermark.isRevoked,
    Visible.isPresent]

theorem revoked_watermark_is_max
    (current incoming : Nat) (visible : Visible) :
    (installRevoked { watermark := some (.revoked current), visible := visible }
        incoming).watermark =
      some (.revoked (max current incoming)) := by
  simp only [installRevoked, Watermark.generation]
  split
  · rename_i h
    rw [Nat.max_eq_left (Nat.le_of_lt h)]
  · rename_i h
    rw [Nat.max_eq_right (Nat.le_of_not_gt h)]

/-- INVARIANTS.md GL-15's headline over *any* prior watermark, not just a
revocation: revoke, evict the visible entry, then replay a positive at or below
the revoked generation -- the principal stays dead.

The sibling below pins the exact resulting watermark, which is only expressible
when the prior one was itself a revocation (an older revocation is refused, and
then the watermark is whatever it already was). This one keeps the coverage: it
quantifies over every pre-state, which is what makes "unweakened" true rather
than merely claimed. -/
theorem revoked_then_evicted_rejects_replay_from_any_watermark
    (prior : Option Watermark) (revoked replay : Nat) (visible : Visible)
    (replay_le_revoked : replay ≤ revoked) :
    (installPositive
        (evictVisible
          (installRevoked { watermark := prior, visible := visible } revoked))
        replay).visible = .absent := by
  cases prior with
  | none =>
      rcases Nat.lt_or_ge replay revoked with h | h
      · simp [installRevoked, evictVisible, installPositive, acceptsPositive,
          Watermark.generation, h]
      · have heq : replay = revoked := Nat.le_antisymm replay_le_revoked h
        simp [installRevoked, evictVisible, installPositive, acceptsPositive,
          Watermark.generation, Watermark.isRevoked, heq]
  | some current =>
      by_cases h : revoked < current.generation
      · have hlt : replay < current.generation := Nat.lt_of_le_of_lt replay_le_revoked h
        simp [installRevoked, h, evictVisible, installPositive, acceptsPositive, hlt]
      · -- The revocation is accepted, so resolve that step first: `h` must
        -- discharge the `if` before the concrete constructor is reduced.
        have hstate :
            installRevoked { watermark := some current, visible := visible } revoked =
              { watermark := some (.revoked revoked), visible := .negative } := by
          simp [installRevoked, h]
        rw [hstate]
        rcases Nat.lt_or_ge replay revoked with hr | hr
        · simp [evictVisible, installPositive, acceptsPositive, Watermark.generation, hr]
        · have heq : replay = revoked := Nat.le_antisymm replay_le_revoked hr
          simp [evictVisible, installPositive, acceptsPositive, Watermark.generation,
            Watermark.isRevoked, heq]

/-- The headline composition of INVARIANTS.md GL-15, unweakened: revoke, evict the
visible entry, then replay a positive at or below the revoked generation -- the
principal stays dead. -/
theorem revoked_then_evicted_rejects_replay
    (current revoked replay : Nat) (visible : Visible)
    (replay_le_revoked : replay ≤ revoked) :
    installPositive
        (evictVisible
          (installRevoked { watermark := some (.revoked current), visible := visible } revoked))
        replay =
      { watermark := some (.revoked (max current revoked)), visible := .absent } := by
  have le_max : replay ≤ max current revoked :=
    Nat.le_trans replay_le_revoked (Nat.le_max_right current revoked)
  by_cases h : revoked < current
  · have hmax : max current revoked = current := Nat.max_eq_left (Nat.le_of_lt h)
    rcases Nat.lt_or_ge replay current with hlt | hge
    · simp [installRevoked, Watermark.generation, h, evictVisible, installPositive,
        acceptsPositive, hlt, hmax]
    · have heq : replay = current := Nat.le_antisymm (hmax ▸ le_max) hge
      simp [installRevoked, Watermark.generation, h, evictVisible, installPositive,
        acceptsPositive, Watermark.isRevoked, heq, hmax]
  · have hge : current ≤ revoked := Nat.le_of_not_gt h
    have hmax : max current revoked = revoked := Nat.max_eq_right hge
    rcases Nat.lt_or_ge replay revoked with hlt | hle
    · simp [installRevoked, Watermark.generation, h, evictVisible, installPositive,
        acceptsPositive, hlt, hmax]
    · have : replay = revoked := Nat.le_antisymm replay_le_revoked hle
      simp [installRevoked, Watermark.generation, h, evictVisible, installPositive,
        acceptsPositive, Watermark.isRevoked, this, hmax]

theorem isPresent_is_exact (v : Visible) : v.isPresent = true ↔ ∃ g, v = .present g := by
  cases v <;> simp [Visible.isPresent]

end Tollgate.SnapshotCache
