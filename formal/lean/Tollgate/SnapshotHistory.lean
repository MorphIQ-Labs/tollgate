import Init.Omega
import Tollgate.SnapshotCache

/-!
Bounded snapshot-history reconstruction (#67).

This model separates visible eviction (SnapshotCache) from reclaiming a retained
incarnation. Natural-number identities model checked Rust u64 allocation: Rust
refuses exhaustion before mutation; it never wraps. The capacity theorem checks
the exact count arithmetic, not a HashMap implementation. Rust property tests
check index correspondence, distinct batch membership and actual map occupancy.

The source assumption is explicit: a fresh authoritative read started after an
observed revocation returns a tombstone at least as new, a strictly newer positive,
or a non-authorizing absence/error. This requires durable source ordering and
linearizable reads. The cache cannot prove a remote source satisfies it.

Atomic transitions correspond to the map's control-plane writer lock. Request
lookup, memory ordering, concurrent source scheduling, and cancellation are Rust
implementation obligations, not consequences of this sequential model.
-/
namespace Tollgate.SnapshotHistory
open SnapshotCache

/-- The implementation removes exactly the deficit before adding new entries. -/
def retainedAfterReserve (used capacity added : Nat) : Nat :=
  used - (added - (capacity - used)) + added

theorem reservation_stays_within_capacity (used capacity added : Nat)
    (held : used ≤ capacity) (batch : added ≤ capacity) :
    retainedAfterReserve used capacity added ≤ capacity := by
  unfold retainedAfterReserve
  omega

theorem reservation_keeps_every_new_entry (used capacity added : Nat) :
    added ≤ retainedAfterReserve used capacity added := by
  unfold retainedAfterReserve
  omega

structure Fence where
  owner : Nat
  principal : Nat
  incarnation : Nat
  deriving DecidableEq

structure Slot where
  fence : Fence
  initialized : Bool
  invalidated : Bool
  cache : State
  deriving DecidableEq

def reserve (owner principal incarnation : Nat) : Slot :=
  { fence := ⟨owner, principal, incarnation⟩, initialized := false,
    invalidated := false, cache := { watermark := none, visible := .absent } }

def valid (slot : Slot) (fence : Fence) : Bool :=
  slot.fence == fence && !slot.invalidated

inductive Answer where
  | positive (generation : Nat)
  | revoked (generation : Nat)
  | unknown
  deriving DecidableEq

def finish (slot : Slot) (fence : Fence) (answer : Answer) : Slot :=
  if valid slot fence then
    match answer with
    | .positive g => { slot with initialized := true, cache := installPositive slot.cache g }
    | .revoked g => { slot with initialized := true, cache := installRevoked slot.cache g }
    | .unknown => { slot with cache := installUnknown slot.cache }
  else slot

/-- A push cannot reconstruct pending history. Refusal fences an older read. -/
def pushPositive (slot : Slot) (generation : Nat) : Slot :=
  if slot.initialized then { slot with cache := installPositive slot.cache generation }
  else { slot with invalidated := true }

theorem pending_push_does_not_publish (owner principal incarnation generation : Nat) :
    (pushPositive (reserve owner principal incarnation) generation).cache.visible = .absent := by
  simp [pushPositive, reserve]

theorem a_refused_push_invalidates_the_pending_read
    (owner principal incarnation generation : Nat) (answer : Answer) :
    finish (pushPositive (reserve owner principal incarnation) generation)
        ⟨owner, principal, incarnation⟩ answer =
      pushPositive (reserve owner principal incarnation) generation := by
  simp [finish, valid, pushPositive, reserve]

theorem a_recreated_incarnation_rejects_its_old_read
    (owner principal old fresh : Nat) (answer : Answer) (different : old ≠ fresh) :
    finish (reserve owner principal fresh) ⟨owner, principal, old⟩ answer =
      reserve owner principal fresh := by
  have unequal : (⟨owner, principal, fresh⟩ : Fence) ≠ ⟨owner, principal, old⟩ := by
    intro h
    have eq := congrArg Fence.incarnation h
    exact different eq.symm
  simp [finish, valid, reserve, unequal]

theorem foreign_map_cannot_publish (slot : Slot) (fence : Fence) (answer : Answer)
    (different : slot.fence.owner ≠ fence.owner) :
    finish slot fence answer = slot := by
  have unequal : slot.fence ≠ fence := by
    intro h
    exact different (congrArg Fence.owner h)
  simp [finish, valid, unequal]

theorem identity_allocation_never_reuses_an_old_identity
    (old next : Nat) (allocated : old ≤ next) : old ≠ next + 1 := by
  omega

def freshAfterRevocation (revocation : Nat) : Answer → Prop
  | .positive g => revocation < g
  | .revoked g => revocation ≤ g
  | .unknown => True

/-- Reconstructing from a fresh source answer and then replaying a positive at
or below the old revocation never installs that replay. A newer positive may
legitimately reopen the principal; an absence keeps its pending fence closed. -/
theorem revalidation_preserves_anti_resurrection
    (owner principal incarnation revoked replay : Nat) (answer : Answer)
    (source_fresh : freshAfterRevocation revoked answer) (old : replay ≤ revoked) :
    (pushPositive
      (finish (reserve owner principal incarnation) ⟨owner, principal, incarnation⟩ answer)
      replay).cache.visible ≠ .present replay := by
  cases answer with
  | unknown =>
      simp [finish, valid, reserve, pushPositive, installUnknown]
  | revoked generation =>
      have le : replay ≤ generation := Nat.le_trans old source_fresh
      have rejected := positive_at_or_below_revocation_is_rejected generation replay .negative le
      simp [finish, valid, reserve, installRevoked, pushPositive, rejected]
  | positive generation =>
      have lt : replay < generation := Nat.lt_of_le_of_lt old source_fresh
      have ne : generation ≠ replay := by omega
      simp [finish, valid, reserve, installPositive, acceptsPositive, pushPositive,
        Watermark.generation, lt, ne]

end Tollgate.SnapshotHistory
