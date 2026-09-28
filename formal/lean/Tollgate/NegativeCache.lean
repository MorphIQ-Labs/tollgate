import Init.Omega

/-!
The request-visible negative cache (INVARIANTS.md 17). A principal the
source reported absent, or revoked, is remembered as a negative with a
deadline; at the deadline a targeted pull re-resolves it. The cache holds at
most a configured number of negatives and, when full, evicts the one with the
earliest deadline.

The model is a list of `(principal, deadline)` entries. It proves that the
cache never holds more than its bound; that eviction removes an entry whose
deadline is no later than any other's; that pruning at `now` keeps exactly
the unexpired entries; that a principal has at most one negative; that the
deadline recorded is `now` plus the TTL the *source's answer* selects, an
absent row taking the unknown TTL and a tombstone the revoked TTL, whatever
the instance remembered before; and that a present answer clears the
principal's negative. Together these bound how long any principal can stay
negative without a re-resolution.

Generation ordering (`SnapshotCache`) and history retention are separate;
this model does not cover scheduling of the targeted pull, retry backoff or
broadcast-lag recovery, which the snapshot-manager tests witness.
-/
namespace Tollgate.NegativeCache

structure Entry where
  principal : Nat
  deadline : Nat
  deriving DecidableEq, Repr

inductive Answer | absent | tombstone
  deriving DecidableEq, Repr

/-- The TTL is chosen by what the source answered. -/
def ttl (unknownTtl revokedTtl : Nat) : Answer → Nat
  | .absent => unknownTtl
  | .tombstone => revokedTtl

/-- The entry with the earliest deadline, first among equals. -/
def earliest : List Entry → Option Entry
  | [] => none
  | e :: rest =>
    match earliest rest with
    | none => some e
    | some m => if e.deadline ≤ m.deadline then some e else some m

/-- Remove the first occurrence of `x`. -/
def remove (x : Entry) : List Entry → List Entry
  | [] => []
  | e :: rest => if e = x then rest else e :: remove x rest

def evict (l : List Entry) : List Entry :=
  match earliest l with
  | none => l
  | some m => remove m l

def without (p : Nat) (l : List Entry) : List Entry := l.filter (fun e => e.principal ≠ p)

/-- Record a negative for `p` at `now`: replace any earlier one, evicting the
earliest deadline if the cache is full. A zero bound retains nothing. -/
def record (bound unknownTtl revokedTtl : Nat) (l : List Entry) (p now : Nat) (a : Answer) :
    List Entry :=
  let rest := without p l
  let rest := if bound ≤ rest.length then evict rest else rest
  if bound = 0 then rest else ⟨p, now + ttl unknownTtl revokedTtl a⟩ :: rest

/-- Control writes drop expired negatives. -/
def prune (now : Nat) (l : List Entry) : List Entry := l.filter (fun e => now < e.deadline)

/-- A present answer resolves the principal: its negative is gone. -/
def resolve (p : Nat) (l : List Entry) : List Entry := without p l

/-! ## Eviction takes the earliest deadline -/

theorem earliest_none : ∀ (l : List Entry), earliest l = none → l = []
  | [], _ => rfl
  | e :: rest, h => by
    simp only [earliest] at h
    split at h
    · cases h
    · split at h <;> cases h

theorem earliest_spec : ∀ (l : List Entry) (m : Entry), earliest l = some m →
    m ∈ l ∧ ∀ e ∈ l, m.deadline ≤ e.deadline
  | [], m, h => by simp [earliest] at h
  | e :: rest, m, h => by
    simp only [earliest] at h
    split at h
    · rename_i hr
      cases h
      refine ⟨by simp, ?_⟩
      intro x hx
      simp at hx
      rcases hx with hx | hx
      · subst hx; exact Nat.le_refl _
      · have := earliest_none rest hr
        subst this
        simp at hx
    · rename_i m' hr
      have ih := earliest_spec rest m' hr
      split at h
      · rename_i hle
        cases h
        refine ⟨by simp, ?_⟩
        intro x hx
        simp at hx
        rcases hx with hx | hx
        · subst hx; exact Nat.le_refl _
        · exact Nat.le_trans hle (ih.2 x hx)
      · rename_i hle
        cases h
        refine ⟨by simp [ih.1], ?_⟩
        intro x hx
        simp at hx
        rcases hx with hx | hx
        · subst hx; omega
        · exact ih.2 x hx

theorem earliest_some_of_ne_nil : ∀ (l : List Entry), l ≠ [] → ∃ m, earliest l = some m
  | [], h => absurd rfl h
  | e :: rest, _ => by
    simp only [earliest]
    split
    · exact ⟨e, rfl⟩
    · split
      · exact ⟨e, rfl⟩
      · rename_i m _ _; exact ⟨m, rfl⟩

theorem remove_length : ∀ (x : Entry) (l : List Entry), x ∈ l → (remove x l).length + 1 = l.length
  | _, [], h => by simp at h
  | x, e :: rest, h => by
    simp only [remove]
    split
    · simp
    · rename_i hne
      simp at h
      rcases h with h | h
      · exact absurd h.symm hne
      · simp [remove_length x rest h]

theorem remove_subset : ∀ (x : Entry) (l : List Entry) (y : Entry), y ∈ remove x l → y ∈ l
  | _, [], _, h => by simp [remove] at h
  | x, e :: rest, y, h => by
    simp only [remove] at h
    split at h
    · simp [h]
    · simp at h
      rcases h with h | h
      · simp [h]
      · simp [remove_subset x rest y h]

/-- A full cache evicts exactly one entry, and it is one whose deadline is no
later than any other's. -/
theorem evict_removes_earliest (l : List Entry) (hne : l ≠ []) :
    ∃ m, m ∈ l ∧ (∀ e ∈ l, m.deadline ≤ e.deadline) ∧
      evict l = remove m l ∧ (evict l).length + 1 = l.length := by
  obtain ⟨m, hm⟩ := earliest_some_of_ne_nil l hne
  have spec := earliest_spec l m hm
  refine ⟨m, spec.1, spec.2, by simp [evict, hm], ?_⟩
  simp only [evict, hm]
  exact remove_length m l spec.1

theorem evict_subset (l : List Entry) (y : Entry) (h : y ∈ evict l) : y ∈ l := by
  unfold evict at h
  split at h
  · exact h
  · exact remove_subset _ _ _ h

theorem evict_length_le (l : List Entry) : (evict l).length ≤ l.length := by
  by_cases h : l = []
  · subst h; simp [evict, earliest]
  · obtain ⟨_, _, _, _, hl⟩ := evict_removes_earliest l h
    omega

/-! ## The bound -/

theorem without_length_le (p : Nat) (l : List Entry) : (without p l).length ≤ l.length := by
  simp [without]
  exact List.length_filter_le _ _

/-- The cache never holds more than its bound. -/
theorem record_bounded (bound unknownTtl revokedTtl : Nat) (l : List Entry) (p now : Nat)
    (a : Answer) (h : l.length ≤ bound) :
    (record bound unknownTtl revokedTtl l p now a).length ≤ bound := by
  have hw := without_length_le p l
  simp only [record]
  by_cases hb : bound = 0
  · subst hb
    have hl : l = [] := List.eq_nil_of_length_eq_zero (by omega)
    subst hl
    simp [without, evict, earliest]
  · simp only [if_neg hb]
    by_cases hf : bound ≤ (without p l).length
    · simp only [if_pos hf, List.length_cons]
      have hne : without p l ≠ [] := by
        intro e; rw [e] at hf; simp at hf; omega
      obtain ⟨_, _, _, _, hl⟩ := evict_removes_earliest (without p l) hne
      omega
    · simp only [if_neg hf, List.length_cons]
      omega

/-- Pruning keeps exactly the unexpired negatives. -/
theorem prune_exact (now : Nat) (l : List Entry) (e : Entry) :
    e ∈ prune now l ↔ e ∈ l ∧ now < e.deadline := by
  simp [prune]

theorem prune_bounded (now bound : Nat) (l : List Entry) (h : l.length ≤ bound) :
    (prune now l).length ≤ bound := by
  have := List.length_filter_le (fun e => decide (now < e.deadline)) l
  simp only [prune]
  omega

/-! ## One negative per principal, with the answer's TTL -/

theorem without_excludes (p : Nat) (l : List Entry) (e : Entry) (h : e ∈ without p l) :
    e.principal ≠ p := by
  simp [without] at h
  exact h.2

/-- After recording, `p` has exactly the negative just written, due at `now`
plus the TTL its answer selects, and no other. -/
theorem record_sets_answer_deadline (bound unknownTtl revokedTtl : Nat) (l : List Entry)
    (p now : Nat) (a : Answer) (hb : bound ≠ 0) (e : Entry)
    (he : e ∈ record bound unknownTtl revokedTtl l p now a) (hp : e.principal = p) :
    e.deadline = now + ttl unknownTtl revokedTtl a := by
  simp only [record, if_neg hb] at he
  simp at he
  rcases he with he | he
  · rw [he]
  · have hin : e ∈ without p l := by
      split at he
      · exact evict_subset _ _ he
      · exact he
    exact absurd hp (without_excludes p l e hin)

theorem absent_takes_unknown_ttl (u r : Nat) : ttl u r .absent = u := rfl
theorem tombstone_takes_revoked_ttl (u r : Nat) : ttl u r .tombstone = r := rfl

/-- A present answer clears the principal's negative. -/
theorem resolve_clears (p : Nat) (l : List Entry) (e : Entry) (h : e ∈ resolve p l) :
    e.principal ≠ p :=
  without_excludes p l e h

end Tollgate.NegativeCache
