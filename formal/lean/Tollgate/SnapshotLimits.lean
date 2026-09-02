/-!
Exact arithmetic model for snapshot batch-limit validation.

Scope: unbounded natural-number costs, weights, item counts, and bursts. The
model proves that validating the largest registered per-item weight at the
batch cap bounds every registered operation at every permitted item count, and
(#92) that the same worst case bounds a *heterogeneous* workload whose classes
each stay under that weight and whose item counts sum within the cap. This is
what lets publication keep validating one number — fixed plus the largest
registered weight times the cap — after quoting became a sum over classes.
Rust's checked `u64` arithmetic and the table scan that selects that weight
remain implementation obligations covered by focused and property tests.
-/

namespace Tollgate.SnapshotLimits

def quote (fixed minimum weight items : Nat) : Nat :=
  max (fixed + weight * items) minimum

theorem quote_bounded_by_worst_case
    (fixed minimum weight maxWeight items cap : Nat)
    (weight_le_max : weight ≤ maxWeight)
    (items_le_cap : items ≤ cap) :
    quote fixed minimum weight items ≤ quote fixed minimum maxWeight cap := by
  apply Nat.max_le.mpr
  constructor
  · exact Nat.le_trans
      (Nat.add_le_add_left (Nat.mul_le_mul weight_le_max items_le_cap) fixed)
      (Nat.le_max_left (fixed + maxWeight * cap) minimum)
  · exact Nat.le_max_right (fixed + maxWeight * cap) minimum

theorem validated_burst_bounds_every_quote
    (fixed minimum weight maxWeight items cap burst : Nat)
    (weight_le_max : weight ≤ maxWeight)
    (items_le_cap : items ≤ cap)
    (worst_case_fits : quote fixed minimum maxWeight cap ≤ burst) :
    quote fixed minimum weight items ≤ burst := by
  exact Nat.le_trans
    (quote_bounded_by_worst_case fixed minimum weight maxWeight items cap
      weight_le_max items_le_cap)
    worst_case_fits

/-- A workload is a list of `(per-item weight, item count)` class aggregates. -/
def variableCost : List (Nat × Nat) → Nat
  | [] => 0
  | entry :: rest => entry.1 * entry.2 + variableCost rest

def itemCount : List (Nat × Nat) → Nat
  | [] => 0
  | entry :: rest => entry.2 + itemCount rest

/-- The heterogeneous quote: the fixed term and the floor apply once. -/
def quoteWorkload (fixed minimum : Nat) (workload : List (Nat × Nat)) : Nat :=
  max (fixed + variableCost workload) minimum

/-- The variable term never exceeds the largest weight times the total items. -/
theorem variableCost_le_max_weight_items
    (maxWeight : Nat) (workload : List (Nat × Nat))
    (bounded : ∀ entry ∈ workload, entry.1 ≤ maxWeight) :
    variableCost workload ≤ maxWeight * itemCount workload := by
  induction workload with
  | nil => simp [variableCost, itemCount]
  | cons entry rest ih =>
    have head_le : entry.1 ≤ maxWeight := bounded entry List.mem_cons_self
    have tail_bounded : ∀ e ∈ rest, e.1 ≤ maxWeight := fun e he =>
      bounded e (List.mem_cons_of_mem entry he)
    have tail := ih tail_bounded
    have head : entry.1 * entry.2 ≤ maxWeight * entry.2 :=
      Nat.mul_le_mul head_le (Nat.le_refl entry.2)
    calc variableCost (entry :: rest)
        = entry.1 * entry.2 + variableCost rest := rfl
      _ ≤ maxWeight * entry.2 + maxWeight * itemCount rest := Nat.add_le_add head tail
      _ = maxWeight * (entry.2 + itemCount rest) := (Nat.left_distrib _ _ _).symm
      _ = maxWeight * itemCount (entry :: rest) := rfl

/-- One homogeneous quote is the single-class workload, so the two agree. -/
theorem quote_is_single_class_workload (fixed minimum weight items : Nat) :
    quote fixed minimum weight items = quoteWorkload fixed minimum [(weight, items)] := by
  simp [quote, quoteWorkload, variableCost]

/-- The published worst case still bounds a heterogeneous workload. -/
theorem workload_bounded_by_worst_case
    (fixed minimum maxWeight cap : Nat) (workload : List (Nat × Nat))
    (weights_le_max : ∀ entry ∈ workload, entry.1 ≤ maxWeight)
    (items_le_cap : itemCount workload ≤ cap) :
    quoteWorkload fixed minimum workload ≤ quote fixed minimum maxWeight cap := by
  have variable_le : variableCost workload ≤ maxWeight * cap :=
    Nat.le_trans
      (variableCost_le_max_weight_items maxWeight workload weights_le_max)
      (Nat.mul_le_mul (Nat.le_refl maxWeight) items_le_cap)
  apply Nat.max_le.mpr
  constructor
  · exact Nat.le_trans
      (Nat.add_le_add_left variable_le fixed)
      (Nat.le_max_left (fixed + maxWeight * cap) minimum)
  · exact Nat.le_max_right (fixed + maxWeight * cap) minimum

/-- Therefore a burst validated once admits every in-limit heterogeneous quote. -/
theorem validated_burst_bounds_every_workload
    (fixed minimum maxWeight cap burst : Nat) (workload : List (Nat × Nat))
    (weights_le_max : ∀ entry ∈ workload, entry.1 ≤ maxWeight)
    (items_le_cap : itemCount workload ≤ cap)
    (worst_case_fits : quote fixed minimum maxWeight cap ≤ burst) :
    quoteWorkload fixed minimum workload ≤ burst :=
  Nat.le_trans
    (workload_bounded_by_worst_case fixed minimum maxWeight cap workload
      weights_le_max items_le_cap)
    worst_case_fits

end Tollgate.SnapshotLimits
