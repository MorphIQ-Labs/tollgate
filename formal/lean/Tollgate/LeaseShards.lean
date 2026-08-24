import Init.Omega

/-!
Exact aggregate model for a sharded local lease.

The concrete Rust implementation distributes a grant across cache-isolated
atomic counters and keeps a fixed-size receipt for the exact aggregate debit.
Refunds may rebalance counters because every counter belongs to the same lease.
This model deliberately abstracts from CAS scheduling and the distribution
shape: `debitExact` is the trust boundary stating that the post-debit shard sum
plus the receipt equals the pre-debit shard sum. Under that condition it proves
conservation through reserve, cancel, and commit, and proves that aggregate
capacity never needs to be stranded merely because it is split across shards.

Natural-number arithmetic is exact. Rust property and concurrency tests cover
the finite-width partition, fixed receipt, rollback, and atomic implementation.
-/

namespace Tollgate.LeaseShards

structure State where
  grant : Nat
  balances : List Nat
  pending : Nat
  spent : Nat
  conserved : balances.sum + pending + spent = grant

def reserve
    (state : State)
    (units : Nat)
    (after : List Nat)
    (debitExact : after.sum + units = state.balances.sum) : State :=
  {
    grant := state.grant
    balances := after
    pending := state.pending + units
    spent := state.spent
    conserved := by
      have conserved := state.conserved
      omega
  }

def cancel
    (state : State)
    (units : Nat)
    (after : List Nat)
    (pendingEnough : units ≤ state.pending)
    (refundExact : after.sum = state.balances.sum + units) : State :=
  {
    grant := state.grant
    balances := after
    pending := state.pending - units
    spent := state.spent
    conserved := by
      have conserved := state.conserved
      omega
  }

def commit
    (state : State)
    (units : Nat)
    (pendingEnough : units ≤ state.pending) : State :=
  {
    grant := state.grant
    balances := state.balances
    pending := state.pending - units
    spent := state.spent + units
    conserved := by
      have conserved := state.conserved
      omega
  }

def refuse
    (state : State)
    (after : List Nat)
    (rollbackExact : after.sum = state.balances.sum) : State :=
  {
    grant := state.grant
    balances := after
    pending := state.pending
    spent := state.spent
    conserved := by
      have conserved := state.conserved
      omega
  }

theorem reserve_preserves_grant
    (state : State)
    (units : Nat)
    (after : List Nat)
    (debitExact : after.sum + units = state.balances.sum) :
    (reserve state units after debitExact).grant = state.grant := by
  rfl

theorem cancel_restores_aggregate
    (state : State)
    (units : Nat)
    (after : List Nat)
    (pendingEnough : units ≤ state.pending)
    (refundExact : after.sum = state.balances.sum + units) :
    (cancel state units after pendingEnough refundExact).balances.sum =
      state.balances.sum + units := by
  exact refundExact

theorem committed_spend_never_exceeds_grant (state : State) :
    state.spent ≤ state.grant := by
  have conserved := state.conserved
  omega

theorem aggregate_capacity_has_an_exact_debit
    (balances : List Nat)
    (units : Nat)
    (enough : units ≤ balances.sum) :
    ∃ after : List Nat, after.sum + units = balances.sum := by
  refine ⟨[balances.sum - units], ?_⟩
  simp
  omega

theorem failed_debit_preserves_aggregate
    (state : State)
    (after : List Nat)
    (rollbackExact : after.sum = state.balances.sum) :
    (refuse state after rollbackExact).balances.sum = state.balances.sum := by
  exact rollbackExact

end Tollgate.LeaseShards
