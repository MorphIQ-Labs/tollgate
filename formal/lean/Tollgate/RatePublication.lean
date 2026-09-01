/-!
Exact model of accepted account-policy publication (INVARIANTS.md #5, #15,
and #25).

`Policy` represents the complete account-wide rate and concurrency policy.
`Authorities` separates the mutable weighted and request-count buckets so a
publication can replace one without refilling the other. Authorization
acceptance is an explicit precondition: a rejected principal snapshot leaves
policy and both authorities unchanged. An accepted newer generation selects
its incoming policy, while a same/older generation retains the current
policy. Either accepted branch may replace a dimension whose enforced
parameters changed (including a safe shard-layout tightening).

The finite-width governor arithmetic and the ArcSwap implementation remain
Rust obligations. This model checks the stable state-machine properties that
must agree with them: rejection precedes mutation, generation chooses the
canonical account policy, and every principal request reads the same current
account authority rather than retaining a principal-local copy.
-/

namespace Tollgate.RatePublication

structure Authorities (Weighted Request : Type) where
  weighted : Option Weighted
  requests : Option Request

structure AccountState (Policy Weighted Request : Type) where
  generation : Nat
  policy : Policy
  authorities : Authorities Weighted Request

def selectDimension {Authority : Type}
    (current : Option Authority)
    (changed : Bool)
    (incoming : Option Authority) : Option Authority :=
  if changed then incoming else current

def publish {Policy Weighted Request : Type}
    (current : AccountState Policy Weighted Request)
    (accepted : Bool)
    (incomingGeneration : Nat)
    (incomingPolicy : Policy)
    (weightedChanged : Bool)
    (incomingWeighted : Option Weighted)
    (requestChanged : Bool)
    (incomingRequests : Option Request) : AccountState Policy Weighted Request :=
  if accepted then
    {
      generation := if current.generation < incomingGeneration then
        incomingGeneration
      else
        current.generation
      policy := if current.generation < incomingGeneration then
        incomingPolicy
      else
        current.policy
      authorities := {
        weighted := selectDimension
          current.authorities.weighted weightedChanged incomingWeighted
        requests := selectDimension
          current.authorities.requests requestChanged incomingRequests
      }
    }
  else
    current

structure RequestView (Policy Weighted Request : Type) where
  policy : Policy
  authorities : Authorities Weighted Request

/-- A principal selects an account, but cannot select or retain its own copy
of that account's policy or authority. -/
def beginRequest {Principal Policy Weighted Request : Type}
    (account : AccountState Policy Weighted Request)
    (_principal : Principal) : RequestView Policy Weighted Request :=
  { policy := account.policy, authorities := account.authorities }

theorem rejected_snapshot_cannot_mutate_account_state
    {Policy Weighted Request : Type}
    (current : AccountState Policy Weighted Request)
    (incomingGeneration : Nat)
    (incomingPolicy : Policy)
    (weightedChanged : Bool)
    (incomingWeighted : Option Weighted)
    (requestChanged : Bool)
    (incomingRequests : Option Request) :
    publish current false incomingGeneration incomingPolicy
      weightedChanged incomingWeighted requestChanged incomingRequests = current := by
  simp [publish]

theorem accepted_newer_generation_selects_incoming_policy
    {Policy Weighted Request : Type}
    (current : AccountState Policy Weighted Request)
    (incomingGeneration : Nat)
    (incomingPolicy : Policy)
    (weightedChanged : Bool)
    (incomingWeighted : Option Weighted)
    (requestChanged : Bool)
    (incomingRequests : Option Request)
    (newer : current.generation < incomingGeneration) :
    (publish current true incomingGeneration incomingPolicy
      weightedChanged incomingWeighted requestChanged incomingRequests).policy =
      incomingPolicy := by
  simp [publish, newer]

theorem accepted_same_or_older_generation_retains_current_policy
    {Policy Weighted Request : Type}
    (current : AccountState Policy Weighted Request)
    (incomingGeneration : Nat)
    (incomingPolicy : Policy)
    (weightedChanged : Bool)
    (incomingWeighted : Option Weighted)
    (requestChanged : Bool)
    (incomingRequests : Option Request)
    (notNewer : ¬ current.generation < incomingGeneration) :
    (publish current true incomingGeneration incomingPolicy
      weightedChanged incomingWeighted requestChanged incomingRequests).policy =
      current.policy := by
  simp [publish, notNewer]

theorem unchanged_weighted_authority_is_preserved
    {Policy Weighted Request : Type}
    (current : AccountState Policy Weighted Request)
    (incomingGeneration : Nat)
    (incomingPolicy : Policy)
    (incomingWeighted : Option Weighted)
    (requestChanged : Bool)
    (incomingRequests : Option Request) :
    (publish current true incomingGeneration incomingPolicy
      false incomingWeighted requestChanged incomingRequests).authorities.weighted =
        current.authorities.weighted := by
  simp [publish, selectDimension]

theorem unchanged_request_authority_is_preserved
    {Policy Weighted Request : Type}
    (current : AccountState Policy Weighted Request)
    (incomingGeneration : Nat)
    (incomingPolicy : Policy)
    (weightedChanged : Bool)
    (incomingWeighted : Option Weighted)
    (incomingRequests : Option Request) :
    (publish current true incomingGeneration incomingPolicy
      weightedChanged incomingWeighted false incomingRequests).authorities.requests =
        current.authorities.requests := by
  simp [publish, selectDimension]

theorem every_principal_reads_the_same_account_view
    {Principal Policy Weighted Request : Type}
    (account : AccountState Policy Weighted Request)
    (first second : Principal) :
    beginRequest account first = beginRequest account second := by
  rfl

end Tollgate.RatePublication
