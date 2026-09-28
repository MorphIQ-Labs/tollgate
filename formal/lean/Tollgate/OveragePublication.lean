/-!
Exact model of the observer-visible overage occupancy and its commit
publication protocol (INVARIANTS.md GL-1 and GL-3).

`spent` includes pending and committed reservations. `committed` includes only
irrevocable reservations. The Rust implementation must update a reservation's
phase and this aggregate through separate atomic words, so `publications`
marks the interval in which those words do not yet form a stable snapshot.
Observers return `publicationInProgress` throughout that interval; only a
stable state may be called refundable or committed-saturated. The request path
has no central-ledger evidence, so this model deliberately makes no claim that
committed saturation requires new account funding rather than an ordinary
lease refill.

Scope. Natural-number units and atomic abstract transitions. Rust separately
establishes finite-width bounds, the single phase CAS, memory ordering, and the
lock-free marker implementation with focused and mutation tests. This model
does not claim to verify Rust scheduling or compile atomics into Lean.
-/

namespace Tollgate.OveragePublication

inductive OccupancyState where
  | publicationInProgress
  | refundable
  | committedSaturation
  deriving DecidableEq, Repr

structure State where
  spent : Nat
  committed : Nat
  publications : Nat
  deriving DecidableEq, Repr

def State.WellFormed (state : State) : Prop :=
  state.committed ≤ state.spent

def classify (state : State) (cap want : Nat) : OccupancyState :=
  if 0 < state.publications then
    .publicationInProgress
  else if state.committed + want ≤ cap then
    .refundable
  else
    .committedSaturation

def beginPublication (state : State) : State :=
  { state with publications := state.publications + 1 }

def finishCommit (state : State) (units : Nat) : State :=
  { state with
      committed := state.committed + units
      publications := state.publications - 1 }

def finishLostClaim (state : State) : State :=
  { state with publications := state.publications - 1 }

def cancelPending (state : State) (units : Nat) : State :=
  { state with spent := state.spent - units }

theorem begin_is_visible_before_the_claim
    (state : State) (cap want : Nat) :
    classify (beginPublication state) cap want = .publicationInProgress := by
  simp [classify, beginPublication]

theorem an_in_flight_publication_is_never_reported_refundable
    (state : State) (cap want : Nat) (active : 0 < state.publications) :
    classify state cap want ≠ .refundable := by
  simp [classify, active]

theorem stable_refundable_means_committed_occupancy_can_fit
    (state : State) (cap want : Nat)
    (stable : state.publications = 0)
    (classified : classify state cap want = .refundable) :
    state.committed + want ≤ cap := by
  simpa [classify, stable] using classified

theorem stable_committed_saturation_means_committed_occupancy_cannot_fit
    (state : State) (cap want : Nat)
    (stable : state.publications = 0)
    (classified : classify state cap want = .committedSaturation) :
    cap < state.committed + want := by
  simp [classify, stable] at classified
  omega

theorem a_winning_commit_preserves_occupancy_bounds
    (state : State) (units : Nat)
    (valid : state.WellFormed)
    (wasPending : units ≤ state.spent - state.committed) :
    (finishCommit state units).WellFormed := by
  simp only [State.WellFormed, finishCommit] at *
  omega

theorem a_lost_claim_preserves_occupancy_bounds
    (state : State) (valid : state.WellFormed) :
    (finishLostClaim state).WellFormed := by
  simpa [State.WellFormed, finishLostClaim] using valid

theorem cancelling_pending_units_preserves_occupancy_bounds
    (state : State) (units : Nat)
    (valid : state.WellFormed)
    (wasPending : units ≤ state.spent - state.committed) :
    (cancelPending state units).WellFormed := by
  simp only [State.WellFormed, cancelPending] at *
  omega

theorem refunding_all_pending_units_leaves_exactly_committed
    (state : State) (valid : state.WellFormed) :
    (cancelPending state (state.spent - state.committed)).spent = state.committed := by
  simp only [cancelPending]
  simp only [State.WellFormed] at valid
  omega

/-- A finished commit moves exactly its units to committed and ends its
publication. -/
theorem finish_commit_is_exact (state : State) (units : Nat) :
    (finishCommit state units).committed = state.committed + units ∧
    (finishCommit state units).publications = state.publications - 1 := by
  simp [finishCommit]

/-- A publication begun and then finished, either way, leaves the count where
it started. -/
theorem a_publication_round_trip_restores_the_count (state : State) (units : Nat) :
    (finishCommit (beginPublication state) units).publications = state.publications ∧
    (finishLostClaim (beginPublication state)).publications = state.publications := by
  simp [finishCommit, finishLostClaim, beginPublication]

end Tollgate.OveragePublication
