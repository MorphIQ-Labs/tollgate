import Init.Omega

/-!
Lease expiry precision (GL-117). Instants and grace are exact integer nanoseconds.
These theorems establish encoding order, cutoff safety and conservative legacy
migration. They do not prove SQL transaction isolation, driver encoding, Jiff's
finite domain or the migration implementation; Rust and PostgreSQL tests are
separate witnesses of those boundaries. The ledger transitions themselves are
unchanged and retain the Conservation model's assumptions.
-/
namespace Tollgate.LeaseTiming

theorem floor_pair_preserves_nanoseconds (n : Int) :
    1000 * (n / 1000) + n % 1000 = n := by omega

theorem floor_pair_has_canonical_remainder (n : Int) :
    0 ≤ n % 1000 ∧ n % 1000 < 1000 := by omega

theorem pair_order_is_instant_order (a b ar br : Int)
    (arLow : 0 ≤ ar) (arHigh : ar < 1000)
    (brLow : 0 ≤ br) (brHigh : br < 1000) :
    (a < b ∨ (a = b ∧ ar ≤ br)) ↔ 1000 * a + ar ≤ 1000 * b + br := by omega

theorem cutoff_is_the_full_grace_deadline (expiry grace now : Int) :
    expiry ≤ now - grace ↔ expiry + grace ≤ now := by omega

theorem underflow_has_no_due_expiry (minimum expiry grace now : Int)
    (represented : minimum ≤ expiry) (underflow : now - grace < minimum) :
    now < expiry + grace := by omega

theorem overflowing_deadline_is_not_timestamp_max (expiry grace maximum : Int)
    (overflow : maximum < expiry + grace) : ¬ expiry ≤ maximum - grace := by omega

theorem conservative_legacy_expiry_cannot_reclaim_early (expiry upper grace now : Int)
    (bounded : expiry ≤ upper) (due : upper ≤ now - grace) :
    expiry + grace ≤ now := by omega

-- Legacy microseconds truncated toward zero. State its three possible
-- preimage intervals explicitly, including the bucket straddling the epoch.
def legacyUpper (micros : Int) : Int :=
    if micros < 0 then 1000 * micros else 1000 * micros + 999

theorem legacy_negative_bound (micros expiry : Int) (negative : micros < 0)
    (low : 1000 * micros - 999 ≤ expiry) (high : expiry ≤ 1000 * micros) :
    expiry ≤ legacyUpper micros ∧ legacyUpper micros - expiry ≤ 999 := by
  simp [legacyUpper, negative]
  omega

theorem legacy_positive_bound (micros expiry : Int) (positive : 0 < micros)
    (low : 1000 * micros ≤ expiry) (high : expiry ≤ 1000 * micros + 999) :
    expiry ≤ legacyUpper micros ∧ legacyUpper micros - expiry ≤ 999 := by
  have nonnegative : ¬ micros < 0 := by omega
  simp [legacyUpper, nonnegative]
  omega

theorem legacy_zero_bound (expiry : Int) (low : -999 ≤ expiry) (high : expiry ≤ 999) :
    expiry ≤ legacyUpper 0 ∧ legacyUpper 0 - expiry ≤ 1998 := by
  simp [legacyUpper]
  omega

end Tollgate.LeaseTiming
