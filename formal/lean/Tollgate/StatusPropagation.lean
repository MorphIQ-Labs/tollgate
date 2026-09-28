import Init.Omega

/-!
An account's status has one writer and one propagation path (INVARIANTS.md
22). The ledger's status and the status inside every live snapshot of that
account are written together by one operation, `set_account_status`, or not
at all. Snapshots already at the target are left alone, revoked principals
are never republished, a directly published snapshot that contradicts the
ledger is refused, and `closed` is terminal.

The model is a ledger status per account and a list of snapshots, each with
its principal, account, status, generation and whether it is a revocation
tombstone. It proves that the ledger and every live snapshot always agree;
that a status change republishes exactly the account's live snapshots not
already at the target, each at generation + 1, and touches nothing else, so
tombstones are never resurrected and no generation moves backward; that
repeating a change changes nothing; that a closed account never leaves
`closed`; and that publication contradicting the ledger is refused.

The operation is atomic by assumption, which the memory store gets from its
lock and PostgreSQL from one transaction. Snapshot distribution to instances
is `SnapshotCache`'s; the refresh-interval bound on "requests stop" is the
snapshot manager's.
-/
namespace Tollgate.StatusPropagation

inductive Status | active | suspended | closed
  deriving DecidableEq, Repr

structure Snap where
  principal : Nat
  account : Nat
  status : Status
  generation : Nat
  revoked : Bool
  deriving DecidableEq, Repr

structure Store where
  ledger : Nat → Status
  snaps : List Snap

/-- Republish one snapshot for a status change of `account` to `target`. -/
def restamp (account : Nat) (target : Status) (s : Snap) : Snap :=
  if s.account = account ∧ !s.revoked ∧ s.status ≠ target then
    { s with status := target, generation := s.generation + 1 }
  else s

/-- The one writer: ledger and live snapshots move together, or not at all. -/
def setStatus (st : Store) (account : Nat) (target : Status) : Option Store :=
  if st.ledger account = .closed ∧ target ≠ .closed then none
  else some { ledger := fun a => if a = account then target else st.ledger a,
              snaps := st.snaps.map (restamp account target) }

/-- Direct publication is refused when it contradicts the ledger. -/
def publish (st : Store) (s : Snap) : Option Store :=
  if !s.revoked ∧ s.status ≠ st.ledger s.account then none
  else some { st with snaps := s :: st.snaps }

/-- Every live snapshot agrees with its account's ledger status. -/
def Agrees (st : Store) : Prop :=
  ∀ s ∈ st.snaps, s.revoked = false → s.status = st.ledger s.account

theorem restamp_account (a : Nat) (t : Status) (s : Snap) : (restamp a t s).account = s.account := by
  unfold restamp; split <;> rfl

theorem restamp_revoked (a : Nat) (t : Status) (s : Snap) : (restamp a t s).revoked = s.revoked := by
  unfold restamp; split <;> rfl

theorem restamp_principal (a : Nat) (t : Status) (s : Snap) :
    (restamp a t s).principal = s.principal := by
  unfold restamp; split <;> rfl

/-! ## Ledger and snapshots never disagree -/

theorem setStatus_agrees (st st' : Store) (a : Nat) (t : Status) (h : Agrees st)
    (hs : setStatus st a t = some st') : Agrees st' := by
  unfold setStatus at hs
  split at hs
  · cases hs
  · cases hs
    intro s' hs' hlive
    simp only [List.mem_map] at hs'
    obtain ⟨s, hs, rfl⟩ := hs'
    rw [restamp_revoked] at hlive
    rw [restamp_account]
    by_cases ha : s.account = a
    · simp only [ha, if_true]
      unfold restamp
      by_cases hst : s.status = t
      · simp [ha, hst]
      · simp [ha, hlive, hst]
    · simp only [ha, if_false]
      unfold restamp
      simp [ha]
      exact h s hs hlive

theorem publish_agrees (st st' : Store) (s : Snap) (h : Agrees st)
    (hp : publish st s = some st') : Agrees st' := by
  unfold publish at hp
  split at hp
  · cases hp
  · rename_i hc
    cases hp
    intro x hx hlive
    simp at hx
    rcases hx with hx | hx
    · subst hx
      simp [hlive] at hc
      exact hc
    · exact h x hx hlive

/-- A snapshot contradicting the ledger is refused, not reconciled later. -/
theorem contradicting_publication_refused (st : Store) (s : Snap) (hlive : s.revoked = false)
    (hc : s.status ≠ st.ledger s.account) : publish st s = none := by
  simp [publish, hlive, hc]

/-! ## One propagation path -/

/-- A status change touches only the account's live snapshots that are not
already at the target, and bumps each of their generations by exactly one. -/
theorem setStatus_restamps_exactly (st st' : Store) (a : Nat) (t : Status)
    (hs : setStatus st a t = some st') :
    st'.snaps = st.snaps.map (restamp a t) ∧
    ∀ s, (s.account = a ∧ s.revoked = false ∧ s.status ≠ t →
            restamp a t s = { s with status := t, generation := s.generation + 1 }) ∧
         (¬ (s.account = a ∧ s.revoked = false ∧ s.status ≠ t) → restamp a t s = s) := by
  unfold setStatus at hs
  split at hs
  · cases hs
  · cases hs
    refine ⟨rfl, fun s => ⟨fun h => ?_, fun h => ?_⟩⟩
    · simp [restamp, h.1, h.2.1, h.2.2]
    · unfold restamp
      split
      · rename_i hc
        exact absurd ⟨hc.1, by simpa using hc.2.1, hc.2.2⟩ h
      · rfl

/-- Tombstones are never republished: a revoked snapshot is unchanged. -/
theorem revoked_never_republished (a : Nat) (t : Status) (s : Snap) (h : s.revoked = true) :
    restamp a t s = s := by
  simp [restamp, h]

/-- No generation moves backward. -/
theorem generation_monotone (a : Nat) (t : Status) (s : Snap) :
    s.generation ≤ (restamp a t s).generation := by
  unfold restamp; split <;> simp

/-- Repeating a status change converges: nothing is republished. -/
theorem repeat_changes_nothing (st st' : Store) (a : Nat) (h : Agrees st)
    (hs : setStatus st a (st.ledger a) = some st') : st'.snaps = st.snaps := by
  unfold setStatus at hs
  split at hs
  · cases hs
  · cases hs
    simp only
    conv => rhs; rw [← List.map_id st.snaps]
    apply List.map_congr_left
    intro s hs
    unfold restamp
    split
    · rename_i hc
      have := h s hs (by simpa using hc.2.1)
      rw [hc.1] at this
      exact absurd this hc.2.2
    · rfl

/-! ## Closed is terminal -/

theorem closed_is_terminal (st : Store) (a : Nat) (t : Status) (hc : st.ledger a = .closed)
    (ht : t ≠ .closed) : setStatus st a t = none := by
  simp [setStatus, hc, ht]

/-- No operation moves a closed account's ledger status. -/
theorem closed_stays_closed (st st' : Store) (a b : Nat) (t : Status) (hc : st.ledger a = .closed)
    (hs : setStatus st b t = some st') : st'.ledger a = .closed := by
  unfold setStatus at hs
  split at hs
  · cases hs
  · rename_i hn
    cases hs
    simp only
    split
    · rename_i e
      subst e
      by_cases htc : t = .closed
      · exact htc
      · exact absurd ⟨hc, htc⟩ hn
    · exact hc

end Tollgate.StatusPropagation
