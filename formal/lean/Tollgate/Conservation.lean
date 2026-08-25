/-!
Exact model of the per-account conservation equation and the transitions that
must preserve it. This mirrors `Conservation::holds` in
`crates/tollgate-store/src/traits.rs` and the ledger writes in both backends.

The equation, restated by issue #1:

    deposited + overage = balance + activeGrants + settledUsage + loss

Read it as a funding statement. The left side is everything the account was
ever funded with -- money in, and credit extended. The right side is where
those units now sit: unspent balance, capacity out on lease, units consumed,
and units written off at settlement.

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

/-- The six quantities per-account reconciliation compares. -/
structure Ledger where
  /-- Units paid for. Moves only on account creation and deposit. -/
  deposited : Nat
  /-- Units extended on credit under elastic enforcement (#1). The second
  funding term, and the reason the equation still closes when spend outruns
  what was paid for. -/
  overage : Nat
  balance : Nat
  activeGrants : Nat
  settledUsage : Nat
  loss : Nat
  deriving DecidableEq, Repr

def Ledger.funded (l : Ledger) : Nat := l.deposited + l.overage

def Ledger.held (l : Ledger) : Nat :=
  l.balance + l.activeGrants + l.settledUsage + l.loss

def Ledger.holds (l : Ledger) : Prop := l.funded = l.held

/-- A deposit funds the account and lands in its balance. -/
def deposit (l : Ledger) (units : Nat) : Ledger :=
  { l with deposited := l.deposited + units, balance := l.balance + units }

theorem deposit_preserves_conservation (l : Ledger) (units : Nat) (h : l.holds) :
    (deposit l units).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, deposit] at *
  omega

/-- Acquiring a lease moves units from balance to outstanding grants. The whole
grant is debited at allocation, which is what makes central allocation bound
spend (INVARIANTS.md #1). -/
def acquire (l : Ledger) (granted : Nat) : Ledger :=
  { l with balance := l.balance - granted, activeGrants := l.activeGrants + granted }

theorem acquire_preserves_conservation
    (l : Ledger) (granted : Nat) (funded : granted ≤ l.balance) (h : l.holds) :
    (acquire l granted).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, acquire] at *
  omega

/-- Releasing a lease splits its grant three ways: what was billed, what comes
back to the balance, and the provisional settlement loss between them. -/
def release (l : Ledger) (granted used unspent lost : Nat) : Ledger :=
  { l with
    balance := l.balance + unspent,
    activeGrants := l.activeGrants - granted,
    settledUsage := l.settledUsage + used,
    loss := l.loss + lost }

theorem release_preserves_conservation
    (l : Ledger) (granted used unspent lost : Nat)
    (outstanding : granted ≤ l.activeGrants)
    (splits : used + unspent + lost = granted)
    (h : l.holds) :
    (release l granted used unspent lost).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, release] at *
  omega

/-- Reclaiming an expired lease credits the whole remainder back, so nothing is
left in provisional loss for it (INVARIANTS.md #9). -/
def reclaim (l : Ledger) (granted used : Nat) : Ledger :=
  { l with
    balance := l.balance + (granted - used),
    activeGrants := l.activeGrants - granted,
    settledUsage := l.settledUsage + used }

theorem reclaim_preserves_conservation
    (l : Ledger) (granted used : Nat)
    (spent : used ≤ granted)
    (outstanding : granted ≤ l.activeGrants)
    (h : l.holds) :
    (reclaim l granted used).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, reclaim] at *
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
  simp only [Ledger.holds, Ledger.funded, Ledger.held, ingestStraggler] at *
  omega

/-- Overage: the transition #1 adds. It belongs to no lease, so it settles the
moment it is recorded -- and it moves *two* columns, funding on the left and
billing on the right, in one transaction. -/
def ingestOverage (l : Ledger) (units : Nat) : Ledger :=
  { l with overage := l.overage + units, settledUsage := l.settledUsage + units }

theorem overage_preserves_conservation (l : Ledger) (units : Nat) (h : l.holds) :
    (ingestOverage l units).holds := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, ingestOverage] at *
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
  simp only [Ledger.holds, Ledger.funded, Ledger.held, ingestOverageUnfunded] at *
  omega

/-- And the equation is off by exactly the overage, not merely off. -/
theorem unfunded_overage_is_short_by_exactly_the_overage
    (l : Ledger) (units : Nat) (h : l.holds) :
    (ingestOverageUnfunded l units).held = (ingestOverageUnfunded l units).funded + units := by
  simp only [Ledger.holds, Ledger.funded, Ledger.held, ingestOverageUnfunded] at *
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
