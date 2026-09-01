import Init.Omega

/-!
Exact natural-number model for Tollgate's per-instance concurrency gauges.

The Rust request path acquires a narrowed principal gauge first, then the
account gauge. If the account is full it immediately undoes the principal
increment. A successful RAII guard releases account then principal exactly
once. This model separates the temporary principal step from published
admission state, proves the undo restores the original state, and proves every
successful acquire/release transition preserves both ceilings.

Occupancy is recorded even when a ceiling is absent. `RuntimeState` models the
one-time handoff from locality-sharded unbounded tracking, through a draining
phase, to the central bounded counter. Draining seals the shards to new
permits but is not an outage: an acquisition with no ceiling of its own is
never refused, and one carrying the activated ceiling is admitted against that
ceiling counting the undrained shard residue. It proves activation cannot
promote before old shard permits drain, that a draining admission never
carries total occupancy past the ceiling, and that disable/re-enable retains
central occupancy. Naturals abstract the `AtomicU32` representation. Rust
unit, concurrency, allocation, property, transition, mutation, and performance
tests cover the atomic implementation and the RAII evidence.
-/

namespace Tollgate.ConcurrencyGauge

structure State where
  accountLimit : Nat
  principalLimit : Nat
  principalNarrowsAccount : principalLimit ≤ accountLimit
  accountInFlight : Nat
  principalInFlight : Nat
  accountBounded : accountInFlight ≤ accountLimit
  principalBounded : principalInFlight ≤ principalLimit
  principalIncluded : principalInFlight ≤ accountInFlight

structure PrincipalStep where
  before : State
  principalInFlight : Nat
  incremented : principalInFlight = before.principalInFlight + 1
  bounded : principalInFlight ≤ before.principalLimit

def acquirePrincipal
    (state : State)
    (room : state.principalInFlight < state.principalLimit) : PrincipalStep :=
  {
    before := state
    principalInFlight := state.principalInFlight + 1
    incremented := rfl
    bounded := by omega
  }

def undoPrincipal (step : PrincipalStep) : State := step.before

def acquireAccount
    (step : PrincipalStep)
    (room : step.before.accountInFlight < step.before.accountLimit) : State :=
  {
    accountLimit := step.before.accountLimit
    principalLimit := step.before.principalLimit
    principalNarrowsAccount := step.before.principalNarrowsAccount
    accountInFlight := step.before.accountInFlight + 1
    principalInFlight := step.principalInFlight
    accountBounded := by omega
    principalBounded := step.bounded
    principalIncluded := by
      have included := step.before.principalIncluded
      have incremented := step.incremented
      omega
  }

def release
    (state : State)
    (accountHeld : 0 < state.accountInFlight)
    (principalHeld : 0 < state.principalInFlight) : State :=
  {
    accountLimit := state.accountLimit
    principalLimit := state.principalLimit
    principalNarrowsAccount := state.principalNarrowsAccount
    accountInFlight := state.accountInFlight - 1
    principalInFlight := state.principalInFlight - 1
    accountBounded := by
      have bounded := state.accountBounded
      omega
    principalBounded := by
      have bounded := state.principalBounded
      omega
    principalIncluded := by
      have included := state.principalIncluded
      omega
  }

theorem account_refusal_undoes_principal (step : PrincipalStep) :
    undoPrincipal step = step.before := by
  rfl

theorem successful_acquire_never_exceeds_account
    (step : PrincipalStep)
    (room : step.before.accountInFlight < step.before.accountLimit) :
    (acquireAccount step room).accountInFlight ≤
      (acquireAccount step room).accountLimit := by
  exact (acquireAccount step room).accountBounded

theorem successful_acquire_never_exceeds_principal
    (step : PrincipalStep)
    (room : step.before.accountInFlight < step.before.accountLimit) :
    (acquireAccount step room).principalInFlight ≤
      (acquireAccount step room).principalLimit := by
  exact (acquireAccount step room).principalBounded

theorem principal_ceiling_cannot_bypass_account
    (step : PrincipalStep)
    (room : step.before.accountInFlight < step.before.accountLimit) :
    (acquireAccount step room).principalInFlight ≤
      (acquireAccount step room).accountInFlight := by
  exact (acquireAccount step room).principalIncluded

theorem release_restores_counts
    (state : State)
    (accountHeld : 0 < state.accountInFlight)
    (principalHeld : 0 < state.principalInFlight) :
    (release state accountHeld principalHeld).accountInFlight + 1 =
        state.accountInFlight ∧
      (release state accountHeld principalHeld).principalInFlight + 1 =
        state.principalInFlight := by
  constructor <;> simp [release] <;> omega

inductive PermitPhase where
  | held
  | released
  deriving DecidableEq

def tryRelease : PermitPhase → Option PermitPhase
  | .held => some .released
  | .released => none

theorem one_release_succeeds : tryRelease .held = some .released := by
  rfl

theorem a_second_release_is_refused :
    (tryRelease .held).bind tryRelease = none := by
  rfl

/- The public ownership protocol around the permit. Execution start transfers
the unique owner from `Admitted` to the execution guard; it is not a release.
Both cancellation before execution and finishing execution release the same
permit exactly once. -/
inductive OccupancyOwner where
  | admitted
  | executionGuard
  | released
  deriving DecidableEq

def ownsOccupancy : OccupancyOwner → Bool
  | .admitted | .executionGuard => true
  | .released => false

def beginExecution : OccupancyOwner → Option OccupancyOwner
  | .admitted => some .executionGuard
  | .executionGuard | .released => none

def finish : OccupancyOwner → Option OccupancyOwner
  | .admitted | .executionGuard => some .released
  | .released => none

theorem execution_start_transfers_without_releasing :
    beginExecution .admitted = some .executionGuard ∧
      ownsOccupancy .admitted = true ∧
      ownsOccupancy .executionGuard = true := by
  decide

theorem pending_cancel_releases_occupancy :
    finish .admitted = some .released ∧ ownsOccupancy .released = false := by
  decide

theorem execution_finish_releases_occupancy :
    finish .executionGuard = some .released ∧
      ownsOccupancy .released = false := by
  decide

theorem released_owner_cannot_release_twice : finish .released = none := by
  rfl

inductive TrackingPhase where
  | sharded
  | draining
  | central
  deriving DecidableEq

structure RuntimeState where
  phase : TrackingPhase
  limit : Option Nat
  shardedInFlight : Nat
  centralInFlight : Nat

def configure (state : RuntimeState) (limit : Option Nat) : RuntimeState :=
  match state.phase, limit with
  | .sharded, some _ =>
      { state with
        phase := if state.shardedInFlight = 0 then .central else .draining
        limit }
  | .draining, none => { state with phase := .sharded, limit }
  | _, _ => { state with limit }

def promoteIfDrained (state : RuntimeState) : RuntimeState :=
  if state.phase = .draining ∧ state.shardedInFlight = 0 then
    { state with phase := .central }
  else
    state

def hasRoom (inFlight : Nat) : Option Nat → Prop
  | none => True
  | some limit => inFlight < limit

/-- Total occupancy the gauge is accountable for in any phase. -/
def RuntimeState.inFlight (state : RuntimeState) : Nat :=
  state.shardedInFlight + state.centralInFlight

/-- A draining acquisition is central, so it must leave room for the shard
residue the activation has not yet reclaimed. Truncated subtraction makes
`centralInFlight < limit - shardedInFlight` and `inFlight < limit` the same
proposition, which is what the Rust headroom computation evaluates. -/
def mayAcquire (state : RuntimeState) : Prop :=
  match state.phase with
  | .sharded => state.limit = none
  | .draining => hasRoom state.inFlight state.limit
  | .central => hasRoom state.centralInFlight state.limit

theorem publication_preserves_occupancy
    (state : RuntimeState)
    (limit : Option Nat) :
    (configure state limit).shardedInFlight = state.shardedInFlight ∧
      (configure state limit).centralInFlight = state.centralInFlight := by
  rcases state with ⟨phase, currentLimit, shardedInFlight, centralInFlight⟩
  cases phase <;> cases limit <;> simp [configure]

theorem activation_with_existing_occupancy_enters_draining
    (state : RuntimeState)
    (limit : Nat)
    (sharded : state.phase = .sharded)
    (occupied : 0 < state.shardedInFlight) :
    (configure state (some limit)).phase = .draining := by
  simp [configure, sharded, Nat.ne_of_gt occupied]

/-- The defect the draining phase used to carry: it refused every acquisition,
including one whose own policy configured no ceiling at all, for as long as the
longest request already in flight kept a shard nonempty. -/
theorem draining_admits_an_unlimited_acquisition
    (state : RuntimeState)
    (draining : state.phase = .draining)
    (unlimited : state.limit = none) :
    mayAcquire state := by
  simp only [mayAcquire, draining, unlimited, hasRoom]

theorem draining_admits_below_the_activated_ceiling
    (state : RuntimeState)
    (limit : Nat)
    (draining : state.phase = .draining)
    (bounded : state.limit = some limit)
    (room : state.inFlight < limit) :
    mayAcquire state := by
  simp only [mayAcquire, draining, bounded, hasRoom]
  exact room

/-- Not admitting past the ceiling is what pays for admitting at all: the
undrained shard permits are live work and occupy the ceiling being activated. -/
theorem draining_acquire_never_exceeds_the_ceiling
    (state : RuntimeState)
    (limit : Nat)
    (draining : state.phase = .draining)
    (bounded : state.limit = some limit)
    (admits : mayAcquire state) :
    state.inFlight + 1 ≤ limit := by
  simp only [mayAcquire, draining, bounded, hasRoom] at admits
  omega

theorem draining_refuses_a_ceiling_the_residue_already_fills
    (state : RuntimeState)
    (limit : Nat)
    (draining : state.phase = .draining)
    (bounded : state.limit = some limit)
    (full : limit ≤ state.shardedInFlight) :
    ¬ mayAcquire state := by
  simp only [mayAcquire, draining, bounded, hasRoom, RuntimeState.inFlight]
  omega

theorem nonempty_shards_cannot_promote
    (state : RuntimeState)
    (draining : state.phase = .draining)
    (occupied : 0 < state.shardedInFlight) :
    (promoteIfDrained state).phase = .draining := by
  simp [promoteIfDrained, draining, Nat.ne_of_gt occupied]

theorem drained_shards_promote_to_central
    (state : RuntimeState)
    (draining : state.phase = .draining)
    (drained : state.shardedInFlight = 0) :
    (promoteIfDrained state).phase = .central := by
  simp [promoteIfDrained, draining, drained]

/-- The precondition for promotion lives inside `promoteIfDrained`, not in
its callers: off that precondition the call is the identity.

This is exactly what makes `ConcurrencyGauge::release_shard`'s `&&` a cost
short-circuit rather than part of the transition. The Rust guard calls
`promote_if_drained` when `decrement() == 0 && phase == DRAINING`; widening
it to `||` adds precisely two cases, and both fail this precondition:

* `decrement() != 0 && phase == DRAINING` — the releasing shard is still
  occupied, so `shardedInFlight ≠ 0`;
* `decrement() == 0 && phase != DRAINING` — the phase is not `draining`.

So every call the mutant adds leaves the state unchanged. No test can
separate the two spellings; the guard's job is to keep the ordinary release
path off an O(shards) scan, which the performance gates enforce.
`.cargo/mutants.toml` excludes that mutant citing this theorem. -/
theorem promote_outside_its_precondition_is_a_no_op
    (state : RuntimeState)
    (blocked : ¬(state.phase = .draining ∧ state.shardedInFlight = 0)) :
    promoteIfDrained state = state := by
  unfold promoteIfDrained
  exact if_neg blocked

theorem central_disable_reenable_preserves_occupancy
    (state : RuntimeState)
    (central : state.phase = .central)
    (limit : Nat) :
    (configure (configure state none) (some limit)).centralInFlight =
      state.centralInFlight := by
  simp [configure, central]

theorem central_reenable_observes_existing_work
    (state : RuntimeState)
    (central : state.phase = .central)
    (limit : Nat)
    (full : limit ≤ state.centralInFlight) :
    ¬ mayAcquire (configure state (some limit)) := by
  simp [configure, central, mayAcquire, hasRoom]
  omega

end Tollgate.ConcurrencyGauge
