import Init.Omega

/-!
Lease capabilities (INVARIANTS.md 4). A lease is named by its ID and carries
the fencing token its account's sequence stamped on it at acquisition. Release
must present the stored `(lease, fence)` pair and ingest the stored
`(lease, account, fence)` triple; anything else is refused and changes
nothing.

The model is a store of leases keyed by ID, with a per-account fence counter
starting at one. It proves that fences are positive and unique within an
account and strictly increase with acquisition; that a newer lease leaves
every existing one untouched, so the sequence is an audit order and not a
validity epoch; that a mismatched capability is refused; that each operation
changes only the lease it names; that a settled lease is never active again;
and that billed plus returned units never exceed the grant, so no lease bills
past its capacity and no unit is both returned and billed.

Expiry reclaim is modeled as forfeiture: the remainder is recorded as loss,
nothing is returned, and a straggler committed before expiry may still bill
into it. Timing is `LeaseTiming`'s; ledger totals are `Conservation`'s. This
model assumes each operation is atomic and does not prove the Rust or SQL
refinement, which the store suites' capability tests witness.
-/
namespace Tollgate.LeaseFencing

inductive Status | active | released | reclaimed
  deriving DecidableEq, Repr

structure Lease where
  account : Nat
  fence : Nat
  granted : Nat
  /-- Units billed against this lease by accepted usage. -/
  used : Nat
  /-- Units returned to the account's balance when the lease settled. -/
  credited : Nat
  status : Status

structure Store where
  leases : Nat → Option Lease
  nextFence : Nat → Nat
  nextId : Nat

def empty : Store :=
  { leases := fun _ => none, nextFence := fun _ => 1, nextId := 0 }

def put (s : Store) (id : Nat) (l : Lease) : Store :=
  { s with leases := fun j => if j = id then some l else s.leases j }

/-- Grant a new lease: the next ID, stamped with the account's next fence. -/
def acquire (s : Store) (account granted : Nat) : Store :=
  { leases := fun j =>
      if j = s.nextId then
        some { account, fence := s.nextFence account, granted,
               used := 0, credited := 0, status := .active }
      else s.leases j,
    nextFence := fun a => if a = account then s.nextFence account + 1 else s.nextFence a,
    nextId := s.nextId + 1 }

/-- Graceful release returns `unspent`; the rest of the grant is provisional
loss, which usage committed before the release may still convert to billing. -/
def release (s : Store) (id fence unspent : Nat) : Option Store :=
  match s.leases id with
  | none => none
  | some l =>
    if l.fence = fence ∧ l.status = .active ∧ l.used + unspent ≤ l.granted then
      some (put s id { l with credited := unspent, status := .released })
    else none

/-- Expiry reclaim forfeits the remainder; nothing is returned. -/
def reclaim (s : Store) (id : Nat) : Option Store :=
  match s.leases id with
  | none => none
  | some l =>
    if l.status = .active then some (put s id { l with status := .reclaimed })
    else none

/-- Usage bills against a lease only through its exact capability, and only
within what the grant has neither billed nor returned. -/
def ingest (s : Store) (id account fence units : Nat) : Option Store :=
  match s.leases id with
  | none => none
  | some l =>
    if l.fence = fence ∧ l.account = account ∧ l.used + l.credited + units ≤ l.granted then
      some (put s id { l with used := l.used + units })
    else none

/-- The store's well-formedness: every lease within its grant, every fence
issued and positive, fences unique per account, and unissued IDs empty. -/
structure Wf (s : Store) : Prop where
  within : ∀ id l, s.leases id = some l → l.used + l.credited ≤ l.granted
  issued : ∀ id l, s.leases id = some l → 1 ≤ l.fence ∧ l.fence < s.nextFence l.account
  counter : ∀ a, 1 ≤ s.nextFence a
  unique : ∀ i j li lj, s.leases i = some li → s.leases j = some lj →
    li.account = lj.account → li.fence = lj.fence → i = j
  fresh : ∀ id, s.nextId ≤ id → s.leases id = none

theorem empty_wf : Wf empty := by
  constructor <;> intros <;> simp_all [empty]

/-! ## Acquisition -/

/-- The lease `acquire` grants. -/
def granted_lease (s : Store) (account granted : Nat) : Lease :=
  { account, fence := s.nextFence account, granted, used := 0, credited := 0, status := .active }

@[simp] theorem acquire_leases (s : Store) (account granted j : Nat) :
    (acquire s account granted).leases j =
      if j = s.nextId then some (granted_lease s account granted) else s.leases j := rfl

@[simp] theorem acquire_nextFence (s : Store) (account granted a : Nat) :
    (acquire s account granted).nextFence a =
      if a = account then s.nextFence account + 1 else s.nextFence a := rfl

@[simp] theorem acquire_nextId (s : Store) (account granted : Nat) :
    (acquire s account granted).nextId = s.nextId + 1 := rfl

theorem acquire_stamps_next_fence (s : Store) (account granted : Nat) :
    ∃ l, (acquire s account granted).leases s.nextId = some l ∧
      l.fence = s.nextFence account ∧ l.account = account ∧ l.status = .active :=
  ⟨granted_lease s account granted, by simp, rfl, rfl, rfl⟩

/-- The new fence is above every fence already issued to the account. -/
theorem acquire_fence_exceeds_existing (s : Store) (h : Wf s) (account id : Nat)
    (l : Lease) (hl : s.leases id = some l) (ha : l.account = account) :
    l.fence < s.nextFence account := by
  have := (h.issued id l hl).2
  rw [ha] at this
  exact this

/-- A newer lease is not a validity epoch: every existing lease is unchanged. -/
theorem acquire_preserves_existing (s : Store) (h : Wf s) (account granted id : Nat)
    (l : Lease) (hl : s.leases id = some l) :
    (acquire s account granted).leases id = some l := by
  have hid : id ≠ s.nextId := by
    intro e
    rw [e, h.fresh s.nextId (Nat.le_refl _)] at hl
    cases hl
  simp [hid, hl]

theorem acquire_wf (s : Store) (h : Wf s) (account granted : Nat) :
    Wf (acquire s account granted) := by
  constructor
  · intro id l hl
    rw [acquire_leases] at hl
    split at hl
    · cases hl; simp [granted_lease]
    · exact h.within id l hl
  · intro id l hl
    rw [acquire_leases] at hl
    rw [acquire_nextFence]
    split at hl
    · cases hl
      have := h.counter account
      simp [granted_lease]
      omega
    · have := h.issued id l hl
      split
      · rename_i ha; rw [ha] at this; omega
      · exact this
  · intro a
    have := h.counter a
    rw [acquire_nextFence]
    split <;> omega
  · intro i j li lj hi hj hacc hfence
    rw [acquire_leases] at hi hj
    split at hi <;> split at hj
    · rename_i h1 h2; rw [h1, h2]
    · cases hi
      have := (h.issued j lj hj).2
      simp only [granted_lease] at hacc hfence
      rw [← hacc] at this
      omega
    · cases hj
      have := (h.issued i li hi).2
      simp only [granted_lease] at hacc hfence
      rw [hacc] at this
      omega
    · exact h.unique i j li lj hi hj hacc hfence
  · intro id hid
    rw [acquire_nextId] at hid
    rw [acquire_leases, if_neg (by omega)]
    exact h.fresh id (by omega)

/-! ## Exact capabilities -/

theorem release_requires_exact_pair (s s' : Store) (id fence unspent : Nat)
    (hr : release s id fence unspent = some s') :
    ∃ l, s.leases id = some l ∧ l.fence = fence ∧ l.status = .active := by
  unfold release at hr
  split at hr
  · simp at hr
  · rename_i l hl
    split at hr
    · rename_i hc; exact ⟨l, hl, hc.1, hc.2.1⟩
    · simp at hr

theorem release_refuses_wrong_fence (s : Store) (id fence unspent : Nat) (l : Lease)
    (hl : s.leases id = some l) (hf : l.fence ≠ fence) :
    release s id fence unspent = none := by
  simp [release, hl, hf]

theorem ingest_requires_exact_triple (s s' : Store) (id account fence units : Nat)
    (hi : ingest s id account fence units = some s') :
    ∃ l, s.leases id = some l ∧ l.fence = fence ∧ l.account = account := by
  unfold ingest at hi
  split at hi
  · simp at hi
  · rename_i l hl
    split at hi
    · rename_i hc; exact ⟨l, hl, hc.1, hc.2.1⟩
    · simp at hi

theorem ingest_refuses_wrong_fence (s : Store) (id account fence units : Nat) (l : Lease)
    (hl : s.leases id = some l) (hf : l.fence ≠ fence) :
    ingest s id account fence units = none := by
  simp [ingest, hl, hf]

theorem ingest_refuses_wrong_account (s : Store) (id account fence units : Nat) (l : Lease)
    (hl : s.leases id = some l) (ha : l.account ≠ account) :
    ingest s id account fence units = none := by
  simp [ingest, hl, ha]

/-- In a well-formed store a capability names at most one lease: two leases of
one account never share a fence. -/
theorem capability_names_one_lease (s : Store) (h : Wf s) (i j : Nat) (li lj : Lease)
    (hi : s.leases i = some li) (hj : s.leases j = some lj)
    (hacc : li.account = lj.account) (hfence : li.fence = lj.fence) : i = j :=
  h.unique i j li lj hi hj hacc hfence

/-! ## Lease scope -/

theorem put_other (s : Store) (id j : Nat) (l : Lease) (hj : j ≠ id) :
    (put s id l).leases j = s.leases j := by
  simp [put, hj]

theorem release_scoped (s s' : Store) (id fence unspent j : Nat) (hj : j ≠ id)
    (hr : release s id fence unspent = some s') : s'.leases j = s.leases j := by
  unfold release at hr
  split at hr
  · simp at hr
  · split at hr
    · simp at hr; subst hr; exact put_other _ _ _ _ hj
    · simp at hr

theorem reclaim_scoped (s s' : Store) (id j : Nat) (hj : j ≠ id)
    (hr : reclaim s id = some s') : s'.leases j = s.leases j := by
  unfold reclaim at hr
  split at hr
  · simp at hr
  · split at hr
    · simp at hr; subst hr; exact put_other _ _ _ _ hj
    · simp at hr

/-- Accepted usage changes only the lease its capability names. -/
theorem ingest_scoped (s s' : Store) (id account fence units j : Nat) (hj : j ≠ id)
    (hi : ingest s id account fence units = some s') : s'.leases j = s.leases j := by
  unfold ingest at hi
  split at hi
  · simp at hi
  · split at hi
    · simp at hi; subst hi; exact put_other _ _ _ _ hj
    · simp at hi

/-! ## Settlement is final -/

theorem release_refuses_settled (s : Store) (id fence unspent : Nat) (l : Lease)
    (hl : s.leases id = some l) (hs : l.status ≠ .active) :
    release s id fence unspent = none := by
  simp [release, hl, hs]

theorem reclaim_refuses_settled (s : Store) (id : Nat) (l : Lease)
    (hl : s.leases id = some l) (hs : l.status ≠ .active) :
    reclaim s id = none := by
  simp [reclaim, hl, hs]

/-- A settled lease stays settled under every operation. -/
theorem settled_lease_never_revives (s : Store) (h : Wf s) (id : Nat) (l : Lease)
    (hl : s.leases id = some l) (hs : l.status ≠ .active) :
    (∀ account granted, (acquire s account granted).leases id = some l) ∧
    (∀ j fence unspent s', release s j fence unspent = some s' →
      ∃ l', s'.leases id = some l' ∧ l'.status ≠ .active) ∧
    (∀ j s', reclaim s j = some s' →
      ∃ l', s'.leases id = some l' ∧ l'.status ≠ .active) ∧
    (∀ j account fence units s', ingest s j account fence units = some s' →
      ∃ l', s'.leases id = some l' ∧ l'.status ≠ .active) := by
  refine ⟨fun account granted => acquire_preserves_existing s h account granted id l hl, ?_, ?_, ?_⟩
  · intro j fence unspent s' hr
    by_cases hj : id = j
    · subst hj; simp [release_refuses_settled s id fence unspent l hl hs] at hr
    · exact ⟨l, by rw [release_scoped s s' j fence unspent id hj hr]; exact hl, hs⟩
  · intro j s' hr
    by_cases hj : id = j
    · subst hj; simp [reclaim_refuses_settled s id l hl hs] at hr
    · exact ⟨l, by rw [reclaim_scoped s s' j id hj hr]; exact hl, hs⟩
  · intro j account fence units s' hi
    by_cases hj : id = j
    · subst hj
      simp only [ingest, hl] at hi
      split at hi
      · cases hi
        exact ⟨{ l with used := l.used + units }, by simp [put], hs⟩
      · cases hi
    · exact ⟨l, by rw [ingest_scoped s s' j account fence units id hj hi]; exact hl, hs⟩

/-! ## Capacity -/

/-- Replacing a lease with one that keeps its account and fence and stays
within its grant preserves well-formedness. Release, reclaim and ingest are
all such replacements. -/
theorem wf_put (s : Store) (h : Wf s) (id : Nat) (l l' : Lease)
    (hl : s.leases id = some l) (hacc : l'.account = l.account) (hfence : l'.fence = l.fence)
    (hwithin : l'.used + l'.credited ≤ l'.granted) : Wf (put s id l') := by
  constructor
  · intro j lj hj
    simp only [put] at hj
    split at hj
    · cases hj; exact hwithin
    · exact h.within j lj hj
  · intro j lj hj
    simp only [put] at hj
    split at hj
    · cases hj; rw [hacc, hfence]; exact h.issued id l hl
    · exact h.issued j lj hj
  · exact h.counter
  · intro i j li lj hi hj ha hf
    simp only [put] at hi hj
    split at hi <;> split at hj
    · rename_i h1 h2; rw [h1, h2]
    · rename_i h1 _
      cases hi
      rw [h1]
      exact h.unique id j l lj hl hj (hacc ▸ ha) (hfence ▸ hf)
    · rename_i _ h2
      cases hj
      rw [h2]
      exact h.unique i id li l hi hl (hacc ▸ ha) (hfence ▸ hf)
    · exact h.unique i j li lj hi hj ha hf
  · intro j hj
    simp only [put]
    split
    · rename_i e
      rw [e] at hj
      rw [h.fresh id hj] at hl
      cases hl
    · exact h.fresh j hj

theorem release_wf (s s' : Store) (h : Wf s) (id fence unspent : Nat)
    (hr : release s id fence unspent = some s') : Wf s' := by
  unfold release at hr
  split at hr
  · cases hr
  · rename_i l hl
    split at hr
    · rename_i hc
      cases hr
      exact wf_put s h id l _ hl rfl rfl (by simp; omega)
    · cases hr

theorem reclaim_wf (s s' : Store) (h : Wf s) (id : Nat)
    (hr : reclaim s id = some s') : Wf s' := by
  unfold reclaim at hr
  split at hr
  · cases hr
  · rename_i l hl
    split at hr
    · cases hr
      exact wf_put s h id l _ hl rfl rfl (h.within id l hl)
    · cases hr

theorem ingest_wf (s s' : Store) (h : Wf s) (id account fence units : Nat)
    (hi : ingest s id account fence units = some s') : Wf s' := by
  unfold ingest at hi
  split at hi
  · cases hi
  · rename_i l hl
    split at hi
    · rename_i hc
      cases hi
      exact wf_put s h id l _ hl rfl rfl (by simp; omega)
    · cases hi

/-- No lease ever bills past its grant, and no unit is both returned to the
balance and billed. -/
theorem billed_and_returned_within_grant (s : Store) (h : Wf s) (id : Nat) (l : Lease)
    (hl : s.leases id = some l) : l.used + l.credited ≤ l.granted :=
  h.within id l hl

/-- Usage that does not fit what the grant has left is refused. -/
theorem ingest_refuses_past_capacity (s : Store) (id account fence units : Nat) (l : Lease)
    (hl : s.leases id = some l) (hover : l.granted < l.used + l.credited + units) :
    ingest s id account fence units = none := by
  simp [ingest, hl]
  intros
  omega

/-- The exact capability is accepted: release of an active lease with a claim
that fits the grant, and usage that fits what is left, both succeed. -/
theorem release_accepts_the_exact_capability (s : Store) (id unspent : Nat) (l : Lease)
    (hl : s.leases id = some l) (ha : l.status = .active) (fits : l.used + unspent ≤ l.granted) :
    (release s id l.fence unspent).isSome = true := by
  simp [release, hl, ha, fits]

theorem ingest_accepts_the_exact_capability (s : Store) (id units : Nat) (l : Lease)
    (hl : s.leases id = some l) (fits : l.used + l.credited + units ≤ l.granted) :
    (ingest s id l.account l.fence units).isSome = true := by
  simp [ingest, hl, fits]

end Tollgate.LeaseFencing
