/-!
Exact arithmetic model for snapshot batch-limit validation.

Scope: unbounded natural-number costs, weights, item counts, and bursts. The
model proves that validating the largest registered per-item weight at the
batch cap bounds every registered operation at every permitted item count.
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

end Tollgate.SnapshotLimits
