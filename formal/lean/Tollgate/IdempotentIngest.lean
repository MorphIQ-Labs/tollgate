import Init.Omega

/-!
Idempotent partial accounting (INVARIANTS.md 7). Usage arrives in batches of
events keyed by request ID. Each event is classified exactly once: a
*duplicate* if its request ID is already settled or appeared earlier in the
batch, otherwise *accepted* or *rejected* by the backend's checks. Only
accepted events change the ledger; a batch that fails as a whole changes
nothing.

Acceptance is an arbitrary decision `ok` over the ledger as planned so far,
standing in for the capability, capacity and unit-domain checks
(`LeaseFencing` and `Conservation` model those). Every theorem holds for any
such decision. The model proves that each input is classified once; that a
duplicate is recognized from its request ID alone, before its payload or `ok`
is consulted; that the ledger only grows by appending accepted events, so a
settled event is never replaced; that no request ID is ever billed twice,
however batches are replayed, split or interleaved; that the billed total
grows by exactly the accepted units; that a rejected event does not claim its
request ID; and that replaying a committed batch accepts nothing it already
accepted.

It assumes the batch applies atomically, which `PostgresStore` gets from one
transaction and `MemoryStore` from planning before applying. It does not
prove the Rust or SQL refinement; the mirrored store-suite tests witness it.
-/
namespace Tollgate.IdempotentIngest

structure Event where
  id : Nat
  units : Nat

inductive Class | accepted | duplicate | rejected
  deriving DecidableEq, Repr

/-- The settled events, oldest first: request ID and billed units. -/
abbrev Ledger := List (Nat × Nat)

/-- How many settled entries carry request ID `id`. -/
def count (id : Nat) (l : Ledger) : Nat := (l.filter (fun p => p.1 == id)).length

def total : Ledger → Nat
  | [] => 0
  | p :: rest => p.2 + total rest

/-- Classify one event against the ledger as planned so far. -/
def step (ok : Ledger → Event → Bool) (l : Ledger) (e : Event) : Ledger × Class :=
  if 0 < count e.id l then (l, .duplicate)
  else if ok l e then (l ++ [(e.id, e.units)], .accepted)
  else (l, .rejected)

/-- Plan a batch in order: events see the effects of earlier ones. -/
def plan (ok : Ledger → Event → Bool) (l : Ledger) : List Event → Ledger × List Class
  | [] => (l, [])
  | e :: es =>
    let r := step ok l e
    let rest := plan ok r.1 es
    (rest.1, r.2 :: rest.2)

/-- A batch commits its whole plan, or fails and leaves the ledger as it was. -/
def ingest (ok : Ledger → Event → Bool) (commits : Bool) (l : Ledger) (b : List Event) : Ledger :=
  if commits then (plan ok l b).1 else l

theorem count_append (id : Nat) (l : Ledger) (p : Nat × Nat) :
    count id (l ++ [p]) = count id l + (if p.1 == id then 1 else 0) := by
  simp only [count, List.filter_append, List.length_append]
  by_cases h : p.1 == id <;> simp [h, List.filter]

theorem total_append (l : Ledger) (p : Nat × Nat) : total (l ++ [p]) = total l + p.2 := by
  induction l with
  | nil => simp [total]
  | cons q rest ih => simp [total, ih]; omega

/-! ## Classification -/

/-- Every input is classified exactly once. -/
theorem classified_once (ok : Ledger → Event → Bool) (l : Ledger) (b : List Event) :
    (plan ok l b).2.length = b.length := by
  induction b generalizing l with
  | nil => rfl
  | cons e es ih => simp [plan, ih]

/-- A duplicate is recognized from its request ID alone: neither its payload
nor the acceptance decision is consulted, and nothing changes. -/
theorem duplicate_ignores_payload (ok ok' : Ledger → Event → Bool) (l : Ledger) (e : Event)
    (units : Nat) (h : 0 < count e.id l) :
    step ok l e = (l, .duplicate) ∧ step ok' l { e with units } = (l, .duplicate) := by
  simp [step, h]

/-- A rejected event leaves the ledger unchanged and does not claim its ID. -/
theorem rejected_claims_nothing (ok : Ledger → Event → Bool) (l : Ledger) (e : Event)
    (h : (step ok l e).2 = .rejected) : (step ok l e).1 = l ∧ count e.id l = 0 := by
  unfold step at h ⊢
  by_cases hd : 0 < count e.id l
  · simp [hd] at h
  · by_cases ha : ok l e = true
    · simp [hd, ha] at h
    · simp [hd, ha]; omega

/-! ## The ledger only grows, by accepted events -/

/-- The accepted events of a planned batch, as ledger entries, in order. -/
def acceptedEntries (b : List Event) (cs : List Class) : Ledger :=
  ((b.zip cs).filter (fun p => p.2 == .accepted)).map (fun p => (p.1.id, p.1.units))

/-- The planned ledger is the old one followed by exactly the accepted
events, in order. -/
theorem plan_ledger (ok : Ledger → Event → Bool) (l : Ledger) (b : List Event) :
    (plan ok l b).1 = l ++ acceptedEntries b (plan ok l b).2 := by
  induction b generalizing l with
  | nil => simp [plan, acceptedEntries]
  | cons e es ih =>
    by_cases hd : 0 < count e.id l
    · have hs : step ok l e = (l, .duplicate) := by simp [step, hd]
      simp only [plan, hs, acceptedEntries, List.zip_cons_cons, List.filter_cons]
      rw [ih l]; simp [acceptedEntries]
    · by_cases ha : ok l e = true
      · have hs : step ok l e = (l ++ [(e.id, e.units)], .accepted) := by simp [step, hd, ha]
        simp only [plan, hs, acceptedEntries, List.zip_cons_cons, List.filter_cons]
        rw [ih]; simp [acceptedEntries]
      · have hs : step ok l e = (l, .rejected) := by simp [step, hd, ha]
        simp only [plan, hs, acceptedEntries, List.zip_cons_cons, List.filter_cons]
        rw [ih l]; simp [acceptedEntries]

/-- Planning only appends: every settled entry survives unchanged, so a
duplicate can never replace the original event's identity or units. -/
theorem plan_extends (ok : Ledger → Event → Bool) (l : Ledger) (b : List Event) :
    ∃ added, (plan ok l b).1 = l ++ added :=
  ⟨_, plan_ledger ok l b⟩

theorem total_append_list (l m : Ledger) : total (l ++ m) = total l + total m := by
  induction l with
  | nil => simp [total]
  | cons q rest ih => simp [total, ih]; omega

/-- No request ID is billed twice: if every ID is settled at most once, it
stays that way after any batch. -/
theorem billed_at_most_once (ok : Ledger → Event → Bool) (l : Ledger) (b : List Event)
    (h : ∀ id, count id l ≤ 1) : ∀ id, count id (plan ok l b).1 ≤ 1 := by
  induction b generalizing l with
  | nil => simpa [plan] using h
  | cons e es ih =>
    apply ih
    intro id
    unfold step
    by_cases hd : 0 < count e.id l
    · simp [hd]; exact h id
    · by_cases ha : ok l e = true
      · simp only [hd, ha, if_false, if_true]
        rw [count_append]
        by_cases hid : e.id = id
        · subst hid; simp; omega
        · have : (e.id == id) = false := by simp [hid]
          simp [this]; exact h id
      · simp [hd, ha]; exact h id

/-- The billed total grows by exactly the units of the accepted events. -/
theorem billed_grows_by_accepted (ok : Ledger → Event → Bool) (l : Ledger) (b : List Event) :
    total (plan ok l b).1 = total l + total (acceptedEntries b (plan ok l b).2) := by
  rw [plan_ledger ok l b, total_append_list]

/-- A failed batch changes nothing. -/
theorem failed_batch_changes_nothing (ok : Ledger → Event → Bool) (l : Ledger)
    (b : List Event) : ingest ok false l b = l := by
  simp [ingest]

/-! ## Replay -/

/-- Once an ID is settled, every later event with that ID is a duplicate. -/
theorem settled_id_is_duplicate (ok : Ledger → Event → Bool) (l : Ledger) (e : Event)
    (h : 0 < count e.id l) (b : List Event) :
    ∃ added, (plan ok l b).1 = l ++ added ∧ step ok ((plan ok l b).1) e = ((plan ok l b).1, .duplicate) := by
  obtain ⟨added, hp⟩ := plan_extends ok l b
  refine ⟨added, hp, ?_⟩
  have : 0 < count e.id (plan ok l b).1 := by
    rw [hp]
    simp only [count, List.filter_append, List.length_append]
    simp only [count] at h
    omega
  simp [step, this]

/-- Replaying a committed batch bills nothing it already billed: every ID it
accepted the first time is a duplicate the second time. -/
theorem replay_accepts_nothing_twice (ok ok' : Ledger → Event → Bool) (l : Ledger)
    (b : List Event) (h : ∀ id, count id l ≤ 1) :
    ∀ id, count id (plan ok' (plan ok l b).1 b).1 ≤ 1 :=
  billed_at_most_once ok' _ b (billed_at_most_once ok l b h)

end Tollgate.IdempotentIngest
