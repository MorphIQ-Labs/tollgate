import Init.Omega

/-!
A request's charge lifecycle on one instance (INVARIANTS.md 2, 8 and 13).
Each request reserves a usage-queue slot before admission, debits the local
lease at admission, is charged at commit, and emits its billing event when
its committed guard drops; the event leaves the queue when the writer
delivers it. Cancelling before commit releases the slot and refunds the
debit.

The model is a list of requests, each in one phase, sharing a lease and a
queue of fixed capacity. It proves that the slots in use never exceed the
queue's capacity, and that a request finding the queue full is shed with no
charge (8); that nothing is charged before commit and cancellation refunds
exactly what admission debited, so the lease is conserved (2); and that a
committed request always holds its slot, so emitting its event needs no
capacity and cannot be refused, and the charge is fixed at commit and emitted
exactly once (13).

Phases are atomic transitions. The model does not cover the lanes that
partition the queue, the drain deadline, sharded lease counters
(`LeaseShards`), the elastic commit fallback (`CommitFallback`), or process
loss, which invariant 13 states as its boundary.
-/
namespace Tollgate.ChargeLifecycle

inductive Phase
  | idle
  | shed
  | reserved
  | pending (units : Nat)
  | committed (units : Nat)
  | emitted (units : Nat)
  | delivered (units : Nat)
  | released
  deriving DecidableEq, Repr

/-- Whether a phase occupies a usage-queue slot: from reservation until the
writer delivers the event, or the request is released. -/
def occupies : Phase → Nat
  | .reserved | .pending _ | .committed _ | .emitted _ => 1
  | _ => 0

/-- Units the phase holds out of the local lease. -/
def debit : Phase → Nat
  | .pending u | .committed u | .emitted u | .delivered u => u
  | _ => 0

/-- Units the phase is charged. -/
def charge : Phase → Nat
  | .committed u | .emitted u | .delivered u => u
  | _ => 0

def sumBy (f : Phase → Nat) : List Phase → Nat
  | [] => 0
  | p :: ps => f p + sumBy f ps

theorem sumBy_set (f : Phase → Nat) :
    ∀ (l : List Phase) (i : Nat) (q p : Phase), l[i]? = some q →
      sumBy f (l.set i p) + f q = sumBy f l + f p
  | [], _, _, _, h => by simp at h
  | a :: as, 0, q, p, h => by
    simp at h; subst h; simp [sumBy]; omega
  | a :: as, i + 1, q, p, h => by
    simp at h
    have := sumBy_set f as i q p h
    simp [sumBy]; omega

structure Sys where
  reqs : List Phase
  capacity : Nat
  lease : Nat
  grant : Nat

def slots (s : Sys) : Nat := sumBy occupies s.reqs
def debited (s : Sys) : Nat := sumBy debit s.reqs
def charged (s : Sys) : Nat := sumBy charge s.reqs

def update (s : Sys) (i : Nat) (p : Phase) : Sys := { s with reqs := s.reqs.set i p }

/-- Reserve a slot before reading the request, or shed when the queue is full. -/
def reserve (s : Sys) (i : Nat) : Option Sys :=
  match s.reqs[i]? with
  | some .idle => some (update s i (if slots s < s.capacity then .reserved else .shed))
  | _ => none

/-- Admission debits the lease; a lease that cannot fund it refuses, which
releases the slot and charges nothing. -/
def admitRequest (s : Sys) (i u : Nat) : Option Sys :=
  match s.reqs[i]? with
  | some .reserved =>
    if u ≤ s.lease then some { update s i (.pending u) with lease := s.lease - u }
    else some (update s i .released)
  | _ => none

/-- Cancelling before commit releases the slot and refunds the debit. -/
def cancel (s : Sys) (i : Nat) : Option Sys :=
  match s.reqs[i]? with
  | some .reserved => some (update s i .released)
  | some (.pending u) => some { update s i .released with lease := s.lease + u }
  | _ => none

/-- Commit at execution start: the charge is fixed. -/
def commit (s : Sys) (i : Nat) : Option Sys :=
  match s.reqs[i]? with
  | some (.pending u) => some (update s i (.committed u))
  | _ => none

/-- The committed guard drops: its event goes into the slot bound at
reservation. There is no capacity check to fail. -/
def emit (s : Sys) (i : Nat) : Option Sys :=
  match s.reqs[i]? with
  | some (.committed u) => some (update s i (.emitted u))
  | _ => none

/-- The writer delivers the event and frees its slot. -/
def deliver (s : Sys) (i : Nat) : Option Sys :=
  match s.reqs[i]? with
  | some (.emitted u) => some (update s i (.delivered u))
  | _ => none

/-- Slots never exceed capacity, and every unit is either in the lease or
held by exactly one request. -/
structure Wf (s : Sys) : Prop where
  bounded : slots s ≤ s.capacity
  conserved : s.lease + debited s = s.grant

theorem start_wf (n capacity grant : Nat) :
    Wf { reqs := List.replicate n .idle, capacity, lease := grant, grant } := by
  have h : ∀ f : Phase → Nat, f .idle = 0 → sumBy f (List.replicate n .idle) = 0 := by
    intro f hf
    induction n with
    | zero => rfl
    | succ k ih => simp [List.replicate, sumBy, hf, ih]
  exact ⟨by simp [slots, h occupies rfl], by simp [debited, h debit rfl]⟩

theorem charge_le_debit (p : Phase) : charge p ≤ debit p := by
  cases p <;> simp [charge, debit]

theorem charged_le_debited (l : List Phase) : sumBy charge l ≤ sumBy debit l := by
  induction l with
  | nil => simp [sumBy]
  | cons p ps ih => simp [sumBy]; have := charge_le_debit p; omega

/-! ## Backpressure (invariant 8) -/

theorem reserve_wf (s s' : Sys) (i : Nat) (h : Wf s) (hr : reserve s i = some s') : Wf s' := by
  unfold reserve at hr
  split at hr
  · rename_i hq
    cases hr
    have hb := h.bounded
    have hcons := h.conserved
    by_cases hc : slots s < s.capacity
    · rw [if_pos hc]
      have o := sumBy_set occupies s.reqs i .idle .reserved hq
      have d := sumBy_set debit s.reqs i .idle .reserved hq
      simp [occupies, debit] at o d
      simp only [slots, debited] at hb hcons hc
      exact ⟨by simp only [slots, update]; omega, by simp only [debited, update]; omega⟩
    · rw [if_neg hc]
      have o := sumBy_set occupies s.reqs i .idle .shed hq
      have d := sumBy_set debit s.reqs i .idle .shed hq
      simp [occupies, debit] at o d
      simp only [slots, debited] at hb hcons
      exact ⟨by simp only [slots, update]; omega, by simp only [debited, update]; omega⟩
  · cases hr

/-- A full queue sheds: the request is refused and nothing is charged. -/
theorem full_queue_sheds (s s' : Sys) (i : Nat) (hfull : s.capacity ≤ slots s)
    (hr : reserve s i = some s') : s'.reqs[i]? = some .shed ∧ charged s' = charged s := by
  unfold reserve at hr
  split at hr
  · rename_i hq
    cases hr
    have hc : ¬ slots s < s.capacity := by omega
    have hlen : i < s.reqs.length := by
      rcases Nat.lt_or_ge i s.reqs.length with h | h
      · exact h
      · rw [List.getElem?_eq_none h] at hq; cases hq
    have := sumBy_set charge s.reqs i .idle .shed hq
    refine ⟨by simp [update, hc, hlen], ?_⟩
    simp only [charged, update, hc, if_false]
    simp [charge] at this
    omega
  · cases hr

/-! ## Zero charge before execution (invariant 2) -/

/-- Only a committed request is charged. -/
theorem uncommitted_charges_nothing (p : Phase)
    (h : ∀ u, p ≠ .committed u ∧ p ≠ .emitted u ∧ p ≠ .delivered u) : charge p = 0 := by
  cases p with
  | committed u => exact absurd rfl (h u).1
  | emitted u => exact absurd rfl (h u).2.1
  | delivered u => exact absurd rfl (h u).2.2
  | _ => rfl

theorem admit_wf (s s' : Sys) (i u : Nat) (h : Wf s) (hr : admitRequest s i u = some s') : Wf s' := by
  unfold admitRequest at hr
  split at hr
  · rename_i hq
    have ho := fun p => sumBy_set occupies s.reqs i .reserved p hq
    have hd := fun p => sumBy_set debit s.reqs i .reserved p hq
    have hb := h.bounded
    have hc := h.conserved
    simp only [slots, debited] at hb hc
    split at hr
    · cases hr
      have o := ho (.pending u); have d := hd (.pending u)
      simp [occupies, debit] at o d
      constructor
      · simp only [slots, update]; omega
      · simp only [debited, update]; omega
    · cases hr
      have o := ho .released; have d := hd .released
      simp [occupies, debit] at o d
      constructor
      · simp only [slots, update]; omega
      · simp only [debited, update]; omega
  · cases hr

/-- Admission charges nothing: the debit is held, not billed. -/
theorem admit_charges_nothing (s s' : Sys) (i u : Nat) (hr : admitRequest s i u = some s') :
    charged s' = charged s := by
  unfold admitRequest at hr
  split at hr
  · rename_i hq
    have hc := fun p => sumBy_set charge s.reqs i .reserved p hq
    split at hr
    · cases hr; have := hc (.pending u); simp [charge] at this; simp only [charged, update]; omega
    · cases hr; have := hc .released; simp [charge] at this; simp only [charged, update]; omega
  · cases hr

theorem cancel_wf (s s' : Sys) (i : Nat) (h : Wf s) (hr : cancel s i = some s') : Wf s' := by
  unfold cancel at hr
  have hb := h.bounded
  have hc := h.conserved
  simp only [slots, debited] at hb hc
  split at hr
  · rename_i hq
    cases hr
    have o := sumBy_set occupies s.reqs i .reserved .released hq
    have d := sumBy_set debit s.reqs i .reserved .released hq
    simp [occupies, debit] at o d
    exact ⟨by simp only [slots, update]; omega, by simp only [debited, update]; omega⟩
  · rename_i u hq
    cases hr
    have o := sumBy_set occupies s.reqs i (.pending u) .released hq
    have d := sumBy_set debit s.reqs i (.pending u) .released hq
    simp [occupies, debit] at o d
    exact ⟨by simp only [slots, update]; omega, by simp only [debited, update]; omega⟩
  · cases hr

/-- Cancelling a pending request refunds exactly what admission debited and
charges nothing. -/
theorem cancel_refunds_exactly (s s' : Sys) (i u : Nat) (hq : s.reqs[i]? = some (.pending u))
    (hr : cancel s i = some s') : s'.lease = s.lease + u ∧ charged s' = charged s := by
  simp [cancel, hq] at hr
  subst hr
  have := sumBy_set charge s.reqs i (.pending u) .released hq
  simp [charge] at this
  exact ⟨rfl, by simp only [charged, update]; omega⟩

/-! ## A committed charge is always emitted (invariant 13) -/

theorem commit_wf (s s' : Sys) (i : Nat) (h : Wf s) (hr : commit s i = some s') : Wf s' := by
  unfold commit at hr
  have hb := h.bounded
  have hc := h.conserved
  simp only [slots, debited] at hb hc
  split at hr
  · rename_i u hq
    cases hr
    have o := sumBy_set occupies s.reqs i (.pending u) (.committed u) hq
    have d := sumBy_set debit s.reqs i (.pending u) (.committed u) hq
    simp [occupies, debit] at o d
    exact ⟨by simp only [slots, update]; omega, by simp only [debited, update]; omega⟩
  · cases hr

/-- Commit fixes the charge at the admitted units. -/
theorem commit_charges_admitted_units (s s' : Sys) (i u : Nat)
    (hq : s.reqs[i]? = some (.pending u)) (hr : commit s i = some s') :
    charged s' = charged s + u := by
  simp [commit, hq] at hr
  subst hr
  have := sumBy_set charge s.reqs i (.pending u) (.committed u) hq
  simp [charge] at this
  simp only [charged, update]; omega

/-- A committed request holds a slot, bound at reservation. -/
theorem committed_holds_its_slot (u : Nat) : occupies (.committed u) = 1 := rfl

/-- Emission cannot be refused: a committed request always emits, with no
capacity check, and the slots in use do not change. -/
theorem committed_always_emits (s : Sys) (i u : Nat) (hq : s.reqs[i]? = some (.committed u)) :
    ∃ s', emit s i = some s' ∧ s'.reqs[i]? = some (.emitted u) ∧ slots s' = slots s := by
  refine ⟨update s i (.emitted u), by simp [emit, hq], ?_, ?_⟩
  · have hlen : i < s.reqs.length := by
      rcases Nat.lt_or_ge i s.reqs.length with h | h
      · exact h
      · rw [List.getElem?_eq_none h] at hq; cases hq
    simp [update, hlen]
  · have := sumBy_set occupies s.reqs i (.committed u) (.emitted u) hq
    simp [occupies] at this
    simp only [slots, update]; omega

theorem emit_wf (s s' : Sys) (i : Nat) (h : Wf s) (hr : emit s i = some s') : Wf s' := by
  unfold emit at hr
  have hb := h.bounded
  have hc := h.conserved
  simp only [slots, debited] at hb hc
  split at hr
  · rename_i u hq
    cases hr
    have o := sumBy_set occupies s.reqs i (.committed u) (.emitted u) hq
    have d := sumBy_set debit s.reqs i (.committed u) (.emitted u) hq
    simp [occupies, debit] at o d
    exact ⟨by simp only [slots, update]; omega, by simp only [debited, update]; omega⟩
  · cases hr

/-- An event is emitted once: only a committed request emits, and emitting
leaves the charge unchanged. -/
theorem emitted_once (s : Sys) (i : Nat) (p : Phase) (hq : s.reqs[i]? = some p)
    (hp : ∀ u, p ≠ .committed u) : emit s i = none := by
  unfold emit
  split
  · rename_i u h; rw [hq] at h; cases h; exact absurd rfl (hp u)
  · rfl

theorem emit_keeps_charge (s s' : Sys) (i : Nat) (hr : emit s i = some s') :
    charged s' = charged s := by
  unfold emit at hr
  split at hr
  · rename_i u hq
    cases hr
    have := sumBy_set charge s.reqs i (.committed u) (.emitted u) hq
    simp [charge] at this
    simp only [charged, update]; omega
  · cases hr

theorem deliver_wf (s s' : Sys) (i : Nat) (h : Wf s) (hr : deliver s i = some s') : Wf s' := by
  unfold deliver at hr
  have hb := h.bounded
  have hc := h.conserved
  simp only [slots, debited] at hb hc
  split at hr
  · rename_i u hq
    cases hr
    have o := sumBy_set occupies s.reqs i (.emitted u) (.delivered u) hq
    have d := sumBy_set debit s.reqs i (.emitted u) (.delivered u) hq
    simp [occupies, debit] at o d
    exact ⟨by simp only [slots, update]; omega, by simp only [debited, update]; omega⟩
  · cases hr

theorem deliver_keeps_charge (s s' : Sys) (i : Nat) (hr : deliver s i = some s') :
    charged s' = charged s := by
  unfold deliver at hr
  split at hr
  · rename_i u hq
    cases hr
    have := sumBy_set charge s.reqs i (.emitted u) (.delivered u) hq
    simp [charge] at this
    simp only [charged, update]; omega
  · cases hr

/-- An instance never charges more than its lease granted. -/
theorem charged_within_grant (s : Sys) (h : Wf s) : charged s ≤ s.grant := by
  have := charged_le_debited s.reqs
  have := h.conserved
  simp only [charged, debited] at *
  omega

/-- An admission the lease can fund, including one that spends it exactly,
is funded: the request becomes pending and the lease drops by its units. -/
theorem a_fundable_admission_is_funded (s : Sys) (i u : Nat) (hq : s.reqs[i]? = some .reserved)
    (hu : u ≤ s.lease) :
    ∃ s', admitRequest s i u = some s' ∧ s'.lease = s.lease - u ∧ s'.reqs[i]? = some (.pending u) := by
  have hlen : i < s.reqs.length := by
    rcases Nat.lt_or_ge i s.reqs.length with h | h
    · exact h
    · rw [List.getElem?_eq_none h] at hq; cases hq
  refine ⟨{ update s i (.pending u) with lease := s.lease - u }, by simp [admitRequest, hq, hu], rfl, ?_⟩
  simp [update, hlen]

end Tollgate.ChargeLifecycle
