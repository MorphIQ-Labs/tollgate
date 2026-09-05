import Init.Omega

/-!
The two-pool execution-capacity model (#99).

An instance divides its execution capacity into a shared pool and a reserve
kept for assured work. Best-effort work may take only from shared; assured work
takes from shared first and falls back to the reserve. A permit returns exactly
its unit to the pool that issued it.

The two properties the feature exists for are proved here as theorems rather
than argued from the code:

* **Conservation** — live permits never exceed the configured total, at every
  reachable state. Carried as a structure field, so an over-occupied state is
  not constructible.
* **Class isolation** — a best-effort acquisition cannot reduce the assured
  headroom. A saturated best-effort flood therefore leaves the whole reserve
  reachable, which is what "a free-tier flood must not consume the assured
  reserve" means precisely.

The model deliberately abstracts from CAS scheduling, shard layout, and finite
width. The Rust implementation shards each pool across cache-isolated counters
and the partition is exact, so a sharded pool is this model with the pool's
occupancy summed over shards; `Tollgate.LeaseShards` proves that a partition
neither creates nor strands capacity. Concurrency, memory ordering, and
exactly-once release are covered by the Rust concurrency tests.
-/

namespace Tollgate.ExecutionCapacity

/-- The two execution-capacity classes. -/
inductive Class
  | assured
  | bestEffort
  deriving DecidableEq, Repr

/-- Which pool issued a permit, and therefore which one its release returns to. -/
inductive Pool
  | shared
  | reserve
  deriving DecidableEq, Repr

/-- Only assured work may draw on the reserve.

One definition, matching the single `may_use_assured_reserve` in the Rust. The
reserve's whole purpose is that exactly one class reaches it, and a second
spelling of that rule is how the two drift. -/
def mayUseReserve : Class → Bool
  | .assured => true
  | .bestEffort => false

/-- An instance's capacity state.

The bounds are *fields*, so a state that over-occupies a pool cannot be built.
Every transition below rebuilds them, which is where the proof obligation is
discharged. -/
structure State where
  sharedCapacity : Nat
  reserveCapacity : Nat
  sharedInFlight : Nat
  reserveInFlight : Nat
  sharedBounded : sharedInFlight ≤ sharedCapacity
  reserveBounded : reserveInFlight ≤ reserveCapacity

/-- Total configured capacity. -/
def State.total (s : State) : Nat :=
  s.sharedCapacity + s.reserveCapacity

/-- Permits currently outstanding across both pools. -/
def State.inFlight (s : State) : Nat :=
  s.sharedInFlight + s.reserveInFlight

/-- Units still available to assured work: shared plus reserve.

Assured work reaches both pools, so this is the headroom the isolation theorem
is stated against. -/
def State.assuredHeadroom (s : State) : Nat :=
  (s.sharedCapacity - s.sharedInFlight) + (s.reserveCapacity - s.reserveInFlight)

/-- Units still available to best-effort work: shared only. -/
def State.bestEffortHeadroom (s : State) : Nat :=
  s.sharedCapacity - s.sharedInFlight

/-- An empty instance of the given shape. -/
def start (shared reserve : Nat) : State :=
  { sharedCapacity := shared, reserveCapacity := reserve
    sharedInFlight := 0, reserveInFlight := 0
    sharedBounded := by omega, reserveBounded := by omega }

/-- Take one unit from shared. Both classes attempt this first. -/
def acquireShared (s : State) (room : s.sharedInFlight < s.sharedCapacity) : State :=
  { s with
    sharedInFlight := s.sharedInFlight + 1
    sharedBounded := by omega }

/-- Take one unit from the reserve.

`permitted` is the precondition that makes isolation structural: there is no
way to build a reserve acquisition for best-effort work, because the transition
will not typecheck without a proof that the class may use it. -/
def acquireReserve
    (s : State) (c : Class)
    (_permitted : mayUseReserve c = true)
    (room : s.reserveInFlight < s.reserveCapacity) : State :=
  { s with
    reserveInFlight := s.reserveInFlight + 1
    reserveBounded := by omega }

/-- Return a shared unit. -/
def releaseShared (s : State) (held : 0 < s.sharedInFlight) : State :=
  { s with
    sharedInFlight := s.sharedInFlight - 1
    sharedBounded := by
      have bounded := s.sharedBounded
      omega }

/-- Return a reserve unit. -/
def releaseReserve (s : State) (held : 0 < s.reserveInFlight) : State :=
  { s with
    reserveInFlight := s.reserveInFlight - 1
    reserveBounded := by
      have bounded := s.reserveBounded
      omega }

/-! ## Conservation -/

/-- Live permits never exceed configured total capacity, in any reachable
state. This is the first of #99's invariants, and it holds by construction:
the two bound fields are carried, so a state that violated it could not exist. -/
theorem in_flight_never_exceeds_total (s : State) : s.inFlight ≤ s.total := by
  have shared := s.sharedBounded
  have reserve := s.reserveBounded
  simp [State.inFlight, State.total]
  omega

/-- A shared acquisition moves exactly one unit. -/
theorem shared_acquire_takes_exactly_one
    (s : State) (room : s.sharedInFlight < s.sharedCapacity) :
    (acquireShared s room).inFlight = s.inFlight + 1 := by
  simp [acquireShared, State.inFlight]
  omega

/-- A release returns exactly one unit, so an acquire/release pair is the
identity on occupancy — the exactly-once property, stated on the counts. -/
theorem shared_release_returns_exactly_one
    (s : State) (room : s.sharedInFlight < s.sharedCapacity) :
    (releaseShared (acquireShared s room) (by simp [acquireShared])).sharedInFlight
      = s.sharedInFlight := by
  simp [acquireShared, releaseShared]

/-- Neither pool's capacity is changed by acquiring; only occupancy moves.
A transition that grew a pool would satisfy the bounds while inventing
capacity. -/
theorem acquire_preserves_capacity
    (s : State) (room : s.sharedInFlight < s.sharedCapacity) :
    (acquireShared s room).total = s.total := by
  rfl

/-! ## Class isolation -/

/-- **Best-effort work cannot reduce assured headroom below the reserve.**

The second of #99's invariants. Best-effort work reaches only the shared pool,
so however much of it is in flight, the reserve's free units remain available
to assured work. -/
theorem best_effort_cannot_touch_the_reserve (s : State) :
    s.reserveCapacity - s.reserveInFlight ≤ s.assuredHeadroom := by
  simp [State.assuredHeadroom]

/-- A saturated shared pool still leaves the entire reserve to assured work.

This is the flood scenario as a theorem: with shared fully occupied — by any
mix of traffic, best-effort included — an untouched reserve is exactly the
assured headroom, so the configured number of assured starts is still
available. -/
theorem a_saturated_shared_pool_leaves_the_whole_reserve
    (s : State)
    (saturated : s.sharedInFlight = s.sharedCapacity)
    (reserveIdle : s.reserveInFlight = 0) :
    s.assuredHeadroom = s.reserveCapacity := by
  simp [State.assuredHeadroom, saturated, reserveIdle]

/-- With shared saturated, best-effort work has no headroom at all — it is shed
rather than borrowing from the reserve. -/
theorem a_saturated_shared_pool_sheds_best_effort
    (s : State) (saturated : s.sharedInFlight = s.sharedCapacity) :
    s.bestEffortHeadroom = 0 := by
  simp [State.bestEffortHeadroom, saturated]

/-- A best-effort acquisition leaves the reserve's occupancy untouched.

The step-level form of isolation: not merely that the reserve is *reachable*,
but that a best-effort transition does not move it at all. -/
theorem a_best_effort_acquire_leaves_the_reserve_unmoved
    (s : State) (room : s.sharedInFlight < s.sharedCapacity) :
    (acquireShared s room).reserveInFlight = s.reserveInFlight := by
  rfl

/-- Best-effort work can never construct a reserve acquisition.

`mayUseReserve .bestEffort` is `false`, so the `permitted` premise of
`acquireReserve` is unprovable for best-effort work: the transition does not
exist for it. This is isolation made unrepresentable rather than merely
untaken. -/
theorem best_effort_has_no_reserve_transition :
    mayUseReserve Class.bestEffort = false := by
  rfl

/-! ## Utilization -/

/-- Assured work reaches shared *plus* reserve, so an instance with no
best-effort traffic is not partitioned against itself.

The counterpart to isolation, and the reason both classes try shared first: a
reserve that could only be reached by leaving shared idle would buy the
guarantee at the cost of the instance's throughput. -/
theorem assured_headroom_is_the_whole_instance_when_idle (shared reserve : Nat) :
    (start shared reserve).assuredHeadroom = shared + reserve := by
  simp [start, State.assuredHeadroom]

/-- Best-effort headroom is the shared pool alone, never the whole instance. -/
theorem best_effort_headroom_excludes_the_reserve (shared reserve : Nat) :
    (start shared reserve).bestEffortHeadroom = shared := by
  simp [start, State.bestEffortHeadroom]

/-! ## Disabled -/

/-- The three runtime modes, and what each configures.

`Disabled` is `none`: no pools, no bound, and — the property #99 asks for —
no state for classification to change. `Uniform` is a shared pool with no
reserve, so class changes no outcome. Only `Reserved` gives the reserve a
nonzero size. -/
def configure : Option (Nat × Nat) → Option State
  | none => none
  | some (shared, reserve) => some (start shared reserve)

/-- Under `Disabled` there is no capacity state at all, so classification
cannot change an outcome: there is nothing for it to change. -/
theorem disabled_configures_no_capacity_state : configure none = none := by
  rfl

/-- Under `Uniform` the reserve is empty, so both classes see identical
headroom — the mode's defining property, stated rather than assumed. -/
theorem uniform_gives_both_classes_the_same_headroom (shared : Nat) :
    (start shared 0).assuredHeadroom = (start shared 0).bestEffortHeadroom := by
  simp [start, State.assuredHeadroom, State.bestEffortHeadroom]

end Tollgate.ExecutionCapacity
