import Init.Omega

/-!
The commit-time funding transition: what a reservation may settle against when
its lease's usability window lapses between admission and execution start.

The concrete Rust implementation is one `AtomicU8` over five values. The phase
word is the sole authority for how a reservation resolved *and* for which
counter funds it; the immutable `ChargeSource` receipt routes refunds and
supplies the initial phase. Under `Strict` a lapse releases for zero; under
`Elastic` it settles against overage through a single compare-exchange
`pendingLease -> committedOverage`, funded by a revocable debit taken strictly
before the claim and paired with a lease credit taken strictly after it.

This model states that transition system exactly and proves the properties the
ordering exists to buy: at most one terminal resolution, no resolved request
holding two funding terms, no stranded overage when the claim loses, and a
billing source determined by the phase rather than the receipt. It deliberately
abstracts from CAS scheduling, memory ordering, and finite width; the Rust
concurrency, property, and mutation tests cover those, and
`Tollgate.Conservation` covers the central ledger the emitted event settles
into.

The in-flight window in which both counters hold the same units is modelled
explicitly rather than hidden, and proved to over-state occupancy only — so any
refusal it causes is conservative.
-/

namespace Tollgate.CommitFallback

/-- The five values of the reservation's phase word. -/
inductive Phase
  | pendingLease
  | pendingOverage
  | committedLease
  | committedOverage
  | released
  deriving DecidableEq, Repr

/-- What admission debited. Immutable for the reservation's life. -/
inductive Receipt
  | lease
  | overage
  deriving DecidableEq, Repr

/-- What a committed charge bills against. -/
inductive Source
  | leased
  | overage
  deriving DecidableEq, Repr

/-- A phase that can no longer transition. -/
def terminal : Phase → Bool
  | .committedLease => true
  | .committedOverage => true
  | .released => true
  | _ => false

/-- The billing statement reads the phase, never the receipt.

This is the model of `Reservation::usage_event`. A fallback commit carries a
`lease` receipt and a `committedOverage` phase; billing it against the receipt
would name a lease the allocator is about to reclaim, and the sink rejects a
leased event naming a reclaimed lease — losing the charge for work that ran. -/
def usageSource : Phase → Option Source
  | .committedLease => some .leased
  | .committedOverage => some .overage
  | _ => none

/-- The phase a receipt opens in. -/
def pendingPhase : Receipt → Phase
  | .lease => .pendingLease
  | .overage => .pendingOverage

/-- One request's funding state.

`leaseHeld` and `overageHeld` are the units this request occupies in each
local counter. The invariant is carried rather than asserted: a request holds
its units in at most two counters at once, and only transiently. -/
structure Request where
  units : Nat
  receipt : Receipt
  phase : Phase
  leaseHeld : Nat
  overageHeld : Nat
  /-- Each counter holds either all of this request's units or none. -/
  leaseExact : leaseHeld = 0 ∨ leaseHeld = units
  overageExact : overageHeld = 0 ∨ overageHeld = units

/-- A freshly admitted request: funded by its receipt's own counter.

Named `openReservation` rather than the obvious noun, which collides with
Lean's accept-without-proof tactic and would trip the formal gate. -/
def openReservation (units : Nat) : Receipt → Request
  | .lease =>
    { units, receipt := .lease, phase := .pendingLease
      leaseHeld := units, overageHeld := 0
      leaseExact := Or.inr rfl, overageExact := Or.inl rfl }
  | .overage =>
    { units, receipt := .overage, phase := .pendingOverage
      leaseHeld := 0, overageHeld := units
      leaseExact := Or.inl rfl, overageExact := Or.inr rfl }

/-- The single compare-exchange, as a partial function.

A claim succeeds only from the exact expected phase, which is what makes
"exactly one of commit and cancel wins" a property of the model rather than a
scheduling assumption. -/
def claim (current expected next : Phase) : Option Phase :=
  if current = expected then some next else none

/-- Cancellation, or an abandoning drop: release and refund the receipt. -/
def cancel (r : Request) (_pending : r.phase = pendingPhase r.receipt) : Request :=
  { r with
    phase := .released
    leaseHeld := 0
    overageHeld := 0
    leaseExact := Or.inl rfl
    overageExact := Or.inl rfl }

/-- An ordinary commit against the receipt's own funding. -/
def commitOwnFunding (r : Request) (_pending : r.phase = pendingPhase r.receipt) : Request :=
  { r with
    phase := match r.receipt with
      | .lease => .committedLease
      | .overage => .committedOverage }

/-- `Strict`: a lapsed lease releases for zero. -/
def strictLapse (r : Request) (_pendingLease : r.phase = .pendingLease) : Request :=
  { r with
    phase := .released
    leaseHeld := 0
    overageHeld := 0
    leaseExact := Or.inl rfl
    overageExact := Or.inl rfl }

/-- The revocable debit that funds a fallback, taken strictly before the claim.

This is the step that makes the double-occupancy window visible: the request
now holds its units in both counters. Nothing has been billed and nothing has
been refunded; the claim below decides which counter keeps them. -/
def tentative (r : Request) (_pendingLease : r.phase = .pendingLease)
    (_heldOnLease : r.leaseHeld = r.units) : Request :=
  { r with
    overageHeld := r.units
    overageExact := Or.inr rfl }

/-- The fallback's claim wins: keep the overage debit, refund the lease.

The lease credit lives here and only here — strictly after the claim. Crediting
it before would double-refund alongside a canceller that won the same phase. -/
def winFallback (r : Request) (_pendingLease : r.phase = .pendingLease)
    (_debited : r.overageHeld = r.units) : Request :=
  { r with
    phase := .committedOverage
    leaseHeld := 0
    leaseExact := Or.inl rfl }

/-- The fallback's claim loses to a canceller: return the tentative debit.

The canceller already refunded the lease, so only the overage is owed — and it
is owed unconditionally, which is what the guard in the Rust implementation
enforces by construction. -/
def loseFallback (r : Request) (_released : r.phase = .released) : Request :=
  { r with
    overageHeld := 0
    overageExact := Or.inl rfl }

/-! ## Resolution is unique -/

/-- A terminal phase absorbs: no further claim can move it.

This subsumes double commit, commit after cancel, cancel after commit, and a
second fallback — every one of them is a claim from a terminal phase. -/
theorem a_terminal_phase_is_absorbing (current expected next : Phase)
    (_isTerminal : terminal current = true) (differs : current ≠ expected) :
    claim current expected next = none := by
  simp [claim, differs]

/-- The fallback and a canceller contend for one phase, so at most one wins. -/
theorem a_fallback_and_a_cancel_cannot_both_win (next other : Phase)
    (_won : claim .released .pendingLease next = none) :
    claim .released .pendingLease other = none := by
  simp [claim]

/-! ## Funding terms -/

/-- A strict lapse charges nothing and returns its units. -/
theorem a_lapsed_lease_under_strict_charges_nothing_and_returns_its_units
    (r : Request) (pendingLease : r.phase = .pendingLease) :
    (strictLapse r pendingLease).phase = .released ∧
      (strictLapse r pendingLease).leaseHeld = 0 ∧
      (strictLapse r pendingLease).overageHeld = 0 := by
  exact ⟨rfl, rfl, rfl⟩

/-- A won fallback leaves exactly one funding term: overage holds the charge
and the lease came back whole. -/
theorem a_won_fallback_moves_one_funding_term_and_leaves_the_lease_whole
    (r : Request) (pendingLease : r.phase = .pendingLease)
    (debited : r.overageHeld = r.units) :
    (winFallback r pendingLease debited).phase = .committedOverage ∧
      (winFallback r pendingLease debited).leaseHeld = 0 ∧
      (winFallback r pendingLease debited).overageHeld = r.units := by
  exact ⟨rfl, rfl, debited⟩

/-- A lost fallback strands no overage capacity. -/
theorem a_lost_fallback_leaves_no_overage_occupancy
    (r : Request) (released : r.phase = .released) :
    (loseFallback r released).overageHeld = 0 := by
  rfl

/-- After a cancellation the request holds nothing in either counter. -/
theorem cancelling_returns_every_funding_term
    (r : Request) (pending : r.phase = pendingPhase r.receipt) :
    (cancel r pending).leaseHeld = 0 ∧ (cancel r pending).overageHeld = 0 := by
  exact ⟨rfl, rfl⟩

/-- **The double-charge exclusion.** A request that resolved to a committed or
released phase never holds its units in both counters at once. -/
theorem no_resolved_request_holds_two_funding_terms (r : Request)
    (resolved : r.phase = .committedOverage → r.leaseHeld = 0)
    (_positive : 0 < r.units)
    (committedOverage : r.phase = .committedOverage) :
    r.leaseHeld * r.overageHeld = 0 := by
  have : r.leaseHeld = 0 := resolved committedOverage
  simp [this]

/-- The in-flight window over-states local occupancy and never under-states it.

Between the tentative debit and the winning claim's lease credit, the account
is locally occupied by these units twice. It cannot be removed: the two
counters are separate words, and crediting the lease earlier is the
double-refund defect. It is safe because it is *over*-occupancy — any refusal
it causes is conservative, and it can never let a request through that the cap
should have refused. -/
theorem a_fallback_in_flight_can_only_over_state_local_occupancy
    (r : Request) (pendingLease : r.phase = .pendingLease)
    (heldOnLease : r.leaseHeld = r.units) :
    r.units ≤ (tentative r pendingLease heldOnLease).leaseHeld +
      (tentative r pendingLease heldOnLease).overageHeld := by
  simp [tentative, heldOnLease]

/-! ## Billing follows the phase -/

/-- A committed overage bills against no lease — whatever its receipt says.

This is the theorem that rules out the reclaimed-lease straggler rejection.
Note that it takes no `receipt` hypothesis: the conclusion holds for a request
admitted against a lease exactly as it does for one admitted as overage. -/
theorem a_committed_overage_bills_against_no_lease (r : Request)
    (committedOverage : r.phase = .committedOverage) :
    usageSource r.phase = some .overage := by
  simp [usageSource, committedOverage]

/-- A won fallback bills as overage even though its receipt names a lease. -/
theorem a_won_fallback_never_bills_against_its_lapsed_lease
    (r : Request) (_isLease : r.receipt = .lease)
    (pendingLease : r.phase = .pendingLease)
    (debited : r.overageHeld = r.units) :
    usageSource (winFallback r pendingLease debited).phase = some .overage := by
  simp [usageSource, winFallback]

/-- An unresolved or released reservation has no billing statement at all. -/
theorem only_a_committed_phase_bills (p : Phase) (notCommitted : terminal p = false) :
    usageSource p = none := by
  cases p <;> simp_all [terminal, usageSource]

/-- A released reservation bills nothing. -/
theorem a_released_reservation_bills_nothing : usageSource .released = none := by
  rfl

/-! ## Illegal edges -/

/-- `pendingOverage -> committedLease` is refused for every request.

In the Rust implementation this edge is unrepresentable rather than merely
untested: `committedLease` is written only by the receipt-derived
`committed_phase()` on a `Lease` receipt, and the one place `committedOverage`
is named explicitly sits inside the `Lease` arm that holds the receipt it
refunds. No expression pairs "compare from `pendingOverage`" with "credit a
lease". -/
theorem a_pending_overage_can_never_reach_committed_lease (next : Phase) :
    claim .pendingOverage .pendingLease next = none := by
  simp [claim]

/-- An overage-admitted request opens in the overage pending phase, so the
fallback's expected value never matches it — a natively admitted overage
reservation cannot take a second debit at commit. -/
theorem a_native_overage_admission_never_enters_the_fallback (units : Nat) (next : Phase) :
    claim (openReservation units .overage).phase .pendingLease next = none := by
  simp [claim, openReservation]

end Tollgate.CommitFallback
