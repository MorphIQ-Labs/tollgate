import Init.Omega

/-!
Ordering of an account's budget observations (INVARIANTS.md 28, GL-77).

An instance receives readings of one ledger out of order: pushes carry the
reading taken at publication, and fetches for an account's principals complete
in any order. It keeps one per account, replacing it only with a reading whose
two growing totals — funded and consumed — are both at least as large.

The model proves that the held totals never move backwards; that every later
reading of a ledger replaces any earlier one and no earlier reading with a
smaller total replaces a later one; that once the latest reading has been
offered it is held, whatever older readings arrive before or after it; and that
readings with equal totals report the same balance.

It assumes the ledger's totals only grow, which `Conservation` and the store
suites carry for the backends, and says nothing about how soon a reading
arrives: lag is bounded only by usage ingestion and refresh, which the
snapshot-manager tests witness. Unordered readings from older control planes
are outside the model.
-/
namespace Tollgate.BudgetObservation

structure Reading where
  funded : Nat
  consumed : Nat
  deriving DecidableEq, Repr

def balance (r : Reading) : Nat := r.funded - r.consumed

/-- `earlier` could have been read from the same ledger no later than `later`. -/
def noLaterThan (earlier later : Reading) : Prop :=
  earlier.funded ≤ later.funded ∧ earlier.consumed ≤ later.consumed

instance (a b : Reading) : Decidable (noLaterThan a b) :=
  inferInstanceAs (Decidable (_ ∧ _))

/-- The transition: keep the offered reading only if the held one is no later. -/
def observe (held offered : Reading) : Reading :=
  if noLaterThan held offered then offered else held

def observeAll (held : Reading) (offered : List Reading) : Reading :=
  offered.foldl observe held

theorem noLaterThan_refl (a : Reading) : noLaterThan a a :=
  ⟨Nat.le_refl _, Nat.le_refl _⟩

theorem noLaterThan_trans {a b c : Reading} (h : noLaterThan a b) (k : noLaterThan b c) :
    noLaterThan a c :=
  ⟨Nat.le_trans h.1 k.1, Nat.le_trans h.2 k.2⟩

/-- What is held never moves backwards. -/
theorem observe_monotone (held offered : Reading) :
    noLaterThan held (observe held offered) := by
  unfold observe
  split
  · assumption
  · exact noLaterThan_refl _

theorem observeAll_monotone (held : Reading) (offered : List Reading) :
    noLaterThan held (observeAll held offered) := by
  induction offered generalizing held with
  | nil => exact noLaterThan_refl _
  | cons r rest ih =>
    exact noLaterThan_trans (observe_monotone held r) (ih (observe held r))

/-- A later reading is accepted. -/
theorem later_replaces {held offered : Reading} (h : noLaterThan held offered) :
    observe held offered = offered := by
  simp [observe, h]

/-- A reading smaller in either total is refused. -/
theorem stale_refused {held offered : Reading}
    (h : offered.funded < held.funded ∨ offered.consumed < held.consumed) :
    observe held offered = held := by
  have : ¬ noLaterThan held offered := by
    intro k
    unfold noLaterThan at k
    omega
  simp [observe, this]

theorem observeAll_bounded (latest held : Reading) (offered : List Reading)
    (hh : noLaterThan held latest) (ho : ∀ r ∈ offered, noLaterThan r latest) :
    noLaterThan (observeAll held offered) latest := by
  induction offered generalizing held with
  | nil => exact hh
  | cons r rest ih =>
    refine ih (observe held r) ?_ (fun x hx => ho x (by simp [hx]))
    unfold observe
    split
    · exact ho r (by simp)
    · exact hh

theorem antisymm {a b : Reading} (h : noLaterThan a b) (k : noLaterThan b a) : a = b := by
  cases a
  cases b
  simp only [noLaterThan] at h k
  simp only [Reading.mk.injEq]
  omega

/-- Convergence: with no reading later than `latest`, once it has been offered
it is held, whatever the order of everything else. -/
theorem latest_reading_wins (latest held : Reading) (before after : List Reading)
    (hh : noLaterThan held latest) (hb : ∀ r ∈ before, noLaterThan r latest)
    (ha : ∀ r ∈ after, noLaterThan r latest) :
    observeAll held (before ++ latest :: after) = latest := by
  have h1 := observeAll_bounded latest held before hh hb
  have h2 : observeAll held (before ++ latest :: after) = observeAll latest after := by
    simp only [observeAll, List.foldl_append, List.foldl_cons]
    rw [show observe (List.foldl observe held before) latest = latest from later_replaces h1]
  rw [h2]
  exact antisymm (observeAll_bounded latest latest after (noLaterThan_refl _) ha)
    (observeAll_monotone latest after)

/-- Readings with equal totals report one balance. -/
theorem equal_totals_equal_balance (a b : Reading)
    (h : a.funded = b.funded) (k : a.consumed = b.consumed) : balance a = balance b := by
  simp [balance, h, k]

/-- Spend alone lowers the balance; funding alone raises it. -/
theorem spend_never_raises (a b : Reading) (h : a.funded = b.funded)
    (k : a.consumed ≤ b.consumed) : balance b ≤ balance a := by
  unfold balance
  omega

theorem balance_is_funded_less_consumed (r : Reading) (h : r.consumed ≤ r.funded) :
    balance r + r.consumed = r.funded := by
  unfold balance
  omega

end Tollgate.BudgetObservation
