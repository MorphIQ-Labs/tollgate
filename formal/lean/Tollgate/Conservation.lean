/-!
Exact model of the per-account conservation equation and the transitions that
must preserve it. This mirrors `Conservation::holds` in
`crates/tollgate-store/src/traits.rs` and the ledger writes in both backends.

The equation, restated by issue #1:

    deposited + overage
      = allowanceBalance + topupBalance + activeGrants + settledUsage + loss + expired

Read it as a funding statement. The left side is everything the account was
ever funded with -- money in, and credit extended. The right side is where
those units now sit: unspent balance, capacity out on lease, units consumed,
units written off at settlement, and units a closed budget period took away.

The balance is two buckets, because a periodic allowance expires and a manual
top-up does not (#97). A single balance could only expire both or neither: the
whole point of `topupBalance` is that no transition here ever moves it to
`expired`.

Scope. Natural-number units and atomic transitions. The Rust is `u64` with
checked arithmetic in both directions, and an overflow there answers `false`
rather than wrapping, so the exact model and the implementation agree wherever
the implementation does not refuse outright; overflow behaviour itself is an
implementation obligation covered by tests, not by this file. Interleaving,
locking, and SQL are likewise out of scope: what is proved here is that the
equation closes under each transition, not that the code performs them
atomically.

One transition is deliberately the identity. Usage on an *active* lease raises
`usage_recorded` and the lease's `used` by the same amount, and `settledUsage`
is derived as their difference, so the view this file models does not move.
That derivation lives outside the model; `ingest_on_an_active_lease_is_inert`
states the consequence, and the two backends' own tests are what check the
derivation.
-/

namespace Tollgate.Conservation

/-- The quantities per-account reconciliation compares. -/
structure Ledger where
  /-- Units paid for. Moves on account creation, deposit, and each period's
  allowance. -/
  deposited : Nat
  /-- Units extended on credit under elastic enforcement (#1). The second
  funding term, and the reason the equation still closes when spend outruns
  what was paid for. -/
  overage : Nat
  /-- Unspent units from the current period's allowance. A rollover takes what
  is left of this and nothing else. -/
  allowanceBalance : Nat
  /-- Unspent units from manual deposits. No transition in this file moves it
  to `expired`; that is the whole content of "top-ups persist" (#97). -/
  topupBalance : Nat
  activeGrants : Nat
  settledUsage : Nat
  loss : Nat
  /-- Units funded by a period that has closed: neither spendable nor billable,
  and with nowhere else to rest. The sink term #97 adds. -/
  expired : Nat
  deriving DecidableEq, Repr

def Ledger.funded (l : Ledger) : Nat := l.deposited + l.overage

/-- What every reader outside the rollover path means by "balance". -/
def Ledger.balance (l : Ledger) : Nat := l.allowanceBalance + l.topupBalance

def Ledger.held (l : Ledger) : Nat :=
  l.balance + l.activeGrants + l.settledUsage + l.loss + l.expired

def Ledger.holds (l : Ledger) : Prop := l.funded = l.held

/-- A deposit funds the account and lands in its top-up bucket: it was bought
or granted out of band, and nothing has scheduled it to expire. -/
def deposit (l : Ledger) (units : Nat) : Ledger :=
  { l with deposited := l.deposited + units, topupBalance := l.topupBalance + units }

theorem deposit_preserves_conservation (l : Ledger) (units : Nat) (h : l.holds) :
    (deposit l units).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, Ledger.balance, deposit] at *
  omega

/-- Acquiring a lease moves units from the two balance buckets to outstanding
grants. The whole grant is debited at allocation, which is what makes central
allocation bound spend (INVARIANTS.md #1).

Spend order is allowance first, but the proof does not need it: the split is a
parameter here, and conservation closes for any split that sums to the grant.
What the order decides is which units survive a boundary, and that is
`settle`'s subject. -/
def acquire (l : Ledger) (fromAllowance fromTopup : Nat) : Ledger :=
  { l with
    allowanceBalance := l.allowanceBalance - fromAllowance,
    topupBalance := l.topupBalance - fromTopup,
    activeGrants := l.activeGrants + (fromAllowance + fromTopup) }

theorem acquire_preserves_conservation
    (l : Ledger) (fromAllowance fromTopup : Nat)
    (allowance_funded : fromAllowance ≤ l.allowanceBalance)
    (topup_funded : fromTopup ≤ l.topupBalance)
    (h : l.holds) :
    (acquire l fromAllowance fromTopup).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, Ledger.balance, acquire] at *
  omega

/-- Settling a lease -- released by its holder, or reclaimed by the expiry
sweep -- splits its grant four ways: what was billed, what returns to each
balance bucket, and the provisional settlement loss between them.

`samePeriod` is the whole of the "drain then expire" decision (#97). An active
lease at a period boundary keeps serving to its own TTL; the boundary is
applied here, when it settles. Only the allowance half is affected: the top-up
half returns to its bucket either way, which is what makes a manual credit
survive a boundary its lease straddled. -/
def settle (l : Ledger) (used toAllowance toTopup lost : Nat) (samePeriod : Bool) : Ledger :=
  { l with
    allowanceBalance := if samePeriod then l.allowanceBalance + toAllowance
                        else l.allowanceBalance,
    topupBalance := l.topupBalance + toTopup,
    activeGrants := l.activeGrants - (used + toAllowance + toTopup + lost),
    settledUsage := l.settledUsage + used,
    loss := l.loss + lost,
    expired := if samePeriod then l.expired else l.expired + toAllowance }

theorem settle_preserves_conservation
    (l : Ledger) (used toAllowance toTopup lost : Nat) (samePeriod : Bool)
    (outstanding : used + toAllowance + toTopup + lost ≤ l.activeGrants)
    (h : l.holds) :
    (settle l used toAllowance toTopup lost samePeriod).holds := by
  cases samePeriod <;>
    · simp only [Ledger.holds, Ledger.funded, Ledger.held, Ledger.balance, settle,
        Bool.false_eq_true, if_true, if_false] at *
      omega

/-- #136: the expiry sweep settles a lease its holder never released. Nothing
is credited, because no unit can be proven unspent: the whole unaccounted
remainder is recorded as provisional loss. Whatever the period, the balance is
untouched, so executed-but-unflushed work can never become spendable again. -/
def forfeit (l : Ledger) (used lost : Nat) (samePeriod : Bool) : Ledger :=
  settle l used 0 0 lost samePeriod

theorem forfeit_credits_nothing (l : Ledger) (used lost : Nat) (samePeriod : Bool) :
    (forfeit l used lost samePeriod).balance = l.balance ∧
      (forfeit l used lost samePeriod).expired = l.expired := by
  cases samePeriod <;> simp [forfeit, settle, Ledger.balance]

theorem forfeit_preserves_conservation (l : Ledger) (used lost : Nat) (samePeriod : Bool)
    (outstanding : used + lost ≤ l.activeGrants) (h : l.holds) :
    (forfeit l used lost samePeriod).holds := by
  unfold forfeit
  exact settle_preserves_conservation l used 0 0 lost samePeriod (by omega) h

/-- Usage for a settled lease that arrives after its settlement, a straggler,
moves units from provisional loss to billed usage. It can never exceed the
loss, which is what makes a forfeited lease billable without over-billing. -/
def straggle (l : Ledger) (units : Nat) : Ledger :=
  { l with loss := l.loss - units, settledUsage := l.settledUsage + units }

theorem straggler_moves_loss_to_usage_conserves (l : Ledger) (units : Nat)
    (fits : units ≤ l.loss) (h : l.holds) :
    (straggle l units).holds ∧ (straggle l units).balance = l.balance := by
  refine ⟨?_, rfl⟩
  unfold Ledger.holds Ledger.funded Ledger.held Ledger.balance at *
  simp only [straggle]
  omega

/-- Consolidation's floor is the spendable credit from settlement. Expired
allowance has no contribution, even when the replacement draws a new period's
allowance. These are natural-number bounds; backend tests cover SQL and u64. -/
def consolidationCredit (toAllowance toTopup : Nat) (samePeriod : Bool) : Nat :=
  toTopup + if samePeriod then toAllowance else 0

theorem settlement_restores_consolidation_credit
    (l : Ledger) (used toAllowance toTopup lost : Nat) (samePeriod : Bool) :
    (settle l used toAllowance toTopup lost samePeriod).balance =
      l.balance + consolidationCredit toAllowance toTopup samePeriod := by
  cases samePeriod <;> simp [settle, Ledger.balance, consolidationCredit] <;> omega

theorem consolidation_grant_respects_restored_balance
    (balance policyGrant toAllowance toTopup : Nat) (samePeriod : Bool)
    (policy_funded : policyGrant ≤ balance + consolidationCredit toAllowance toTopup samePeriod) :
    consolidationCredit toAllowance toTopup samePeriod ≤
        max policyGrant (consolidationCredit toAllowance toTopup samePeriod) ∧
      max policyGrant (consolidationCredit toAllowance toTopup samePeriod) ≤
        balance + consolidationCredit toAllowance toTopup samePeriod := by
  omega

/-- #131: the consolidation grant may grow to `needed`, the largest quote the
returned lease refused, only when the restored balance funds it. `available`
is the ledger balance plus the restored credit; `sized` is the ordinary answer,
the policy grant raised to the credit floor. -/
def consolidationGrant (available sized needed : Nat) : Nat :=
  if needed ≤ available then max sized needed else sized

theorem consolidation_growth_is_bounded (available sized floor needed : Nat)
    (sized_funded : sized ≤ available) (floor_kept : floor ≤ sized) :
    floor ≤ consolidationGrant available sized needed ∧
      consolidationGrant available sized needed ≤ available ∧
      (sized < consolidationGrant available sized needed →
        consolidationGrant available sized needed = needed) := by
  unfold consolidationGrant
  split <;> omega

theorem unfundable_demand_changes_nothing (available sized needed : Nat)
    (unfundable : available < needed) :
    consolidationGrant available sized needed = sized := by
  unfold consolidationGrant
  split <;> omega

theorem expired_allowance_cannot_enlarge_consolidation
    (policyGrant toAllowance toTopup : Nat) :
    max policyGrant (consolidationCredit toAllowance toTopup false) =
      max policyGrant toTopup := by
  simp [consolidationCredit]

/-- Releasing a lease inside its own period: the shape the ledger had before
budgets, kept under its own name because INVARIANTS.md #4 cites it. -/
def release (l : Ledger) (_granted used unspent lost : Nat) : Ledger :=
  settle l used unspent 0 lost true

theorem release_preserves_conservation
    (l : Ledger) (granted used unspent lost : Nat)
    (outstanding : granted ≤ l.activeGrants)
    (splits : used + unspent + lost = granted)
    (h : l.holds) :
    (release l granted used unspent lost).holds := by
  refine settle_preserves_conservation l used unspent 0 lost true ?_ h
  omega

/-- Reclaiming an expired lease credits the whole remainder back, so nothing is
left in provisional loss for it (INVARIANTS.md #9). -/
def reclaim (l : Ledger) (granted used : Nat) : Ledger :=
  settle l used (granted - used) 0 0 true

theorem reclaim_preserves_conservation
    (l : Ledger) (granted used : Nat)
    (spent : used ≤ granted)
    (outstanding : granted ≤ l.activeGrants)
    (h : l.holds) :
    (reclaim l granted used).holds := by
  refine settle_preserves_conservation l used (granted - used) 0 0 true ?_ h
  omega

/-- A lease funded entirely from manual credits, settled after its period
closed. Nothing expires: the units never had an expiry date, and a boundary
they merely happened to straddle must not give them one. -/
theorem a_top_up_funded_lease_expires_nothing
    (l : Ledger) (used toTopup lost : Nat) :
    (settle l used 0 toTopup lost false).expired = l.expired := by
  simp [settle]

/-- Crossing a period boundary: what is left of the allowance is expired, and
the new period's allowance is deposited. One allowance, whatever the boundary
count -- an account nobody rolled for two months is entitled to what its
schedule gives it now, not to a backlog. -/
def rollover (l : Ledger) (allowance : Nat) : Ledger :=
  { l with
    deposited := l.deposited + allowance,
    expired := l.expired + l.allowanceBalance,
    allowanceBalance := allowance }

theorem rollover_preserves_conservation (l : Ledger) (allowance : Nat) (h : l.holds) :
    (rollover l allowance).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, Ledger.balance, rollover] at *
  omega

/-- The product promise, and the reason the balance is split in two: a rollover
does not touch manual credits, however many boundaries pass. -/
theorem a_rollover_never_touches_a_top_up (l : Ledger) (allowance : Nat) :
    (rollover l allowance).topupBalance = l.topupBalance := rfl

/-- Rolling twice at one boundary is not idempotent in the ledger -- it
deposits twice and expires the fresh allowance. This is why crossing a boundary
is guarded by the stored period rather than by the caller: the store's row lock
is what makes this state unreachable, and nothing in the arithmetic would
notice it. -/
theorem a_second_rollover_at_one_boundary_would_double_the_deposit
    (l : Ledger) (allowance : Nat) :
    (rollover (rollover l allowance) allowance).deposited = l.deposited + allowance + allowance :=
  rfl

/-- Expiring an allowance without recording it. This is the shape the ledger
would have had if `expired` had been left out and the rollover simply reset the
allowance bucket -- the design this term exists to rule out. -/
def rolloverUnrecorded (l : Ledger) (allowance : Nat) : Ledger :=
  { l with deposited := l.deposited + allowance, allowanceBalance := allowance }

/-- The theorem that earns the column. On a ledger that held, an unrecorded
expiry always produces a violation, by exactly the units that expired -- so
reconciliation would report corruption on a correctly working ledger every time
an account's allowance reset with anything left in it. -/
theorem unrecorded_expiry_always_breaks_conservation
    (l : Ledger) (allowance : Nat) (leftover : 0 < l.allowanceBalance) (h : l.holds) :
    ¬ (rolloverUnrecorded l allowance).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, Ledger.balance, rolloverUnrecorded] at *
  omega

/-- And the equation is over by exactly the units that vanished, not merely
wrong. -/
theorem unrecorded_expiry_is_long_by_exactly_the_expired_units
    (l : Ledger) (allowance : Nat) (h : l.holds) :
    (rolloverUnrecorded l allowance).funded
      = (rolloverUnrecorded l allowance).held + l.allowanceBalance := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, Ledger.balance, rolloverUnrecorded] at *
  omega

/-- Usage on an active lease. See the note at the top: it raises
`usage_recorded` and the lease's `used` together, and `settledUsage` is their
difference, so the reconciliation view does not move. -/
def ingestOnActiveLease (l : Ledger) (_units : Nat) : Ledger := l

theorem ingest_on_an_active_lease_is_inert (l : Ledger) (units : Nat) (h : l.holds) :
    (ingestOnActiveLease l units).holds := h

/-- A straggler on a settled lease converts provisional loss into billed usage.
It is bounded by the loss its own release recorded, which is why the units
cannot be counted twice. -/
def ingestStraggler (l : Ledger) (units : Nat) : Ledger :=
  { l with settledUsage := l.settledUsage + units, loss := l.loss - units }

theorem straggler_preserves_conservation
    (l : Ledger) (units : Nat) (within_loss : units ≤ l.loss) (h : l.holds) :
    (ingestStraggler l units).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, Ledger.balance, ingestStraggler] at *
  omega

/-- Overage: the transition #1 adds. It belongs to no lease, so it settles the
moment it is recorded -- and it moves *two* columns, funding on the left and
billing on the right, in one transaction. -/
def ingestOverage (l : Ledger) (units : Nat) : Ledger :=
  { l with overage := l.overage + units, settledUsage := l.settledUsage + units }

theorem overage_preserves_conservation (l : Ledger) (units : Nat) (h : l.holds) :
    (ingestOverage l units).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, Ledger.balance, ingestOverage] at *
  omega

/-- Overage recorded as billing alone, without its funding term. This is the
shape the ledger would have had if `overage_recorded` had been left out and
overage usage simply added to `usage_recorded` -- the design this file exists
to rule out. -/
def ingestOverageUnfunded (l : Ledger) (units : Nat) : Ledger :=
  { l with settledUsage := l.settledUsage + units }

/-- The theorem that earns the column. Billing overage without funding it does
not merely risk a violation: on a ledger that held, it *always* produces one,
by exactly the overage. A reconciliation check would then report corruption on
a correctly working ledger, every time an elastic account spent. -/
theorem unfunded_overage_always_breaks_conservation
    (l : Ledger) (units : Nat) (nonzero : 0 < units) (h : l.holds) :
    ¬ (ingestOverageUnfunded l units).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, Ledger.balance, ingestOverageUnfunded] at *
  omega

/-- And the equation is off by exactly the overage, not merely off. -/
theorem unfunded_overage_is_short_by_exactly_the_overage
    (l : Ledger) (units : Nat) (h : l.holds) :
    (ingestOverageUnfunded l units).held = (ingestOverageUnfunded l units).funded + units := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, Ledger.balance, ingestOverageUnfunded] at *
  omega

/-- One instance's overage debit against its cap, mirroring
`AccountOverage::try_debit`: the cap comparison and the claim are one step, and
a total that would exceed the cap claims nothing. -/
def debit (spent cap units : Nat) : Option Nat :=
  if spent + units ≤ cap then some (spent + units) else none

/-- The cap bounds unfunded spend on one instance. Stated over an arbitrary
prior `spent`, so it holds at every step of any sequence of debits rather than
only from zero. -/
theorem an_accepted_debit_stays_within_the_cap
    (spent cap units next : Nat) (accepted : debit spent cap units = some next) :
    next ≤ cap := by
  unfold debit at accepted
  split at accepted
  · rename_i within
    injection accepted with claimed
    omega
  · exact absurd accepted (by simp)

/-- A refused debit claims nothing, so a refusal cannot advance the counter and
a caller cannot be charged for work it was denied (INVARIANTS.md #2). -/
theorem a_refused_debit_claims_nothing (spent cap units : Nat) (over : cap < spent + units) :
    debit spent cap units = none := by
  unfold debit
  split
  · omega
  · rfl

/-- The cap is a *per-instance* bound, and this is the shape of what a fleet
gets: `instances` counters, each independently bounded by `cap`, sum to at most
`instances * cap`. Stated because it is the property operators must size
against -- aggregating it would need the synchronous coordination the request
path exists to avoid. -/
theorem fleet_overage_is_bounded_by_instances_times_the_cap :
    ∀ (cap : Nat) (spents : List Nat),
      (∀ s ∈ spents, s ≤ cap) → spents.sum ≤ spents.length * cap := by
  intro cap spents
  induction spents with
  | nil => intro _; simp
  | cons head tail ih =>
      intro bounded
      have head_le : head ≤ cap := bounded head (by simp)
      have tail_le : tail.sum ≤ tail.length * cap :=
        ih (fun s hs => bounded s (by simp [hs]))
      simp only [List.sum_cons, List.length_cons, Nat.add_mul, Nat.one_mul]
      omega

end Tollgate.Conservation
