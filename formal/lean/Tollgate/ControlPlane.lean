import Init.Omega

/-!
Control-plane authorization and administrative receipts (GL-98).
Credential authenticity is an input: these proofs do not establish TLS, RSA,
HMAC, clock accuracy, or a Rust refinement. A request reads one immutable
policy generation; ArcSwap publication supplies that implementation boundary.
Concurrent backend tests separately witness the transaction serialization
assumed by the receipt model. Arithmetic here is exact Nat, not SQL BIGINT.
-/
namespace Tollgate.ControlPlane

inductive Role | instance | operator | provisioner
  deriving DecidableEq, Repr

structure Identity where
  name : Nat
  role : Role
  deriving DecidableEq, Repr

def select (bearer certificate : Option Identity) : Option Identity :=
  match bearer, certificate with
  | none, none => none
  | some a, none | none, some a => some a
  | some a, some b => if a = b then some a else none

def allowed (bearer certificate : Option Identity) (required : Role) : Bool :=
  match select bearer certificate with
  | none => false
  | some identity => decide (identity.role = required)

theorem no_evidence_no_authority (required : Role) :
    allowed none none required = false := by rfl

theorem conflicting_identities_have_no_authority
    (a b : Identity) (different : a ≠ b) (required : Role) :
    allowed (some a) (some b) required = false := by
  simp [allowed, select, different]

theorem authorization_requires_matching_role
    (bearer certificate : Option Identity) (required : Role)
    (accepted : allowed bearer certificate required = true) :
    ∃ identity, select bearer certificate = some identity ∧ identity.role = required := by
  cases selected : select bearer certificate with
  | none => simp [allowed, selected] at accepted
  | some identity =>
    exact ⟨identity, rfl, by simpa [allowed, selected] using accepted⟩

theorem an_operator_cannot_fund_an_instance (name : Nat) :
    allowed (some ⟨name, .operator⟩) none .instance = false := by rfl

theorem an_instance_cannot_administer (name : Nat) :
    allowed (some ⟨name, .instance⟩) none .operator = false := by rfl

/-! Each route accepts a fixed role set (#39): the operator-only admin routes accept
`[operator]`, the shared ones `[operator, provisioner]`. A route belongs to
exactly one set, so disjointness is per route rather than per role pair. -/
def admitted (bearer certificate : Option Identity) (roles : List Role) : Bool :=
  match select bearer certificate with
  | none => false
  | some identity => roles.contains identity.role

def operatorOnly : List Role := [.operator]
def sharedAdmin : List Role := [.operator, .provisioner]

theorem no_evidence_is_never_admitted (roles : List Role) :
    admitted none none roles = false := by rfl

theorem admission_requires_a_listed_role
    (bearer certificate : Option Identity) (roles : List Role)
    (accepted : admitted bearer certificate roles = true) :
    ∃ identity, select bearer certificate = some identity ∧ identity.role ∈ roles := by
  cases selected : select bearer certificate with
  | none => simp [admitted, selected] at accepted
  | some identity =>
    exact ⟨identity, rfl, by simpa [admitted, selected] using accepted⟩

theorem a_provisioner_cannot_fund_or_publish_principals (name : Nat) :
    admitted (some ⟨name, .provisioner⟩) none operatorOnly = false := by rfl

theorem a_provisioner_reaches_the_shared_routes (name : Nat) :
    admitted (some ⟨name, .provisioner⟩) none sharedAdmin = true := by rfl

theorem an_instance_cannot_provision (name : Nat) :
    admitted (some ⟨name, .instance⟩) none sharedAdmin = false := by rfl

theorem an_operator_reaches_every_admin_route (name : Nat) :
    admitted (some ⟨name, .operator⟩) none operatorOnly = true ∧
    admitted (some ⟨name, .operator⟩) none sharedAdmin = true := ⟨rfl, rfl⟩

/-! Account provenance and the operator hold (#39). `origin` is written once
at creation; `setBy` is rewritten by every status write. Both are read inside
the serialization point of the write that depends on them — the backends'
row lock or mutex, witnessed by the mirrored store tests — which this model
takes as given by treating each transition as atomic. -/
inductive Authority | operator | provisioner
  deriving DecidableEq, Repr

inductive Status | active | suspended | closed
  deriving DecidableEq, Repr

structure Account where
  origin : Authority
  status : Status
  setBy : Authority
  deriving DecidableEq, Repr

inductive Refusal | notProvisioned | closed | operatorHold
  deriving DecidableEq, Repr

/-- The only account a provisioner can create: suspended, and its own. Balance
and capacity class are fixed by the backend and are not modelled here. -/
def createProvisioned : Account := ⟨.provisioner, .suspended, .provisioner⟩

/-- An operator's status write. Closed is terminal; every write, a repeat
included, records the operator as the author. -/
def operatorSetStatus (a : Account) (target : Status) : Except Refusal Account :=
  if a.status = .closed ∧ target ≠ .closed then .error .closed
  else .ok { a with status := target, setBy := .operator }

/-- The provisioner's only status transition. -/
def activate (a : Account) : Except Refusal Account :=
  if a.origin ≠ .provisioner then .error .notProvisioned
  else if a.status = .closed then .error .closed
  else if a.status = .active then .ok a
  else if a.setBy = .operator then .error .operatorHold
  else .ok { a with status := .active, setBy := .provisioner }

theorem a_provisioned_account_activates :
    activate createProvisioned = .ok ⟨.provisioner, .active, .provisioner⟩ := by rfl

theorem an_operators_account_is_out_of_reach (status : Status) (setBy : Authority) :
    activate ⟨.operator, status, setBy⟩ = .error .notProvisioned := by rfl

theorem activation_leaves_closed_terminal (setBy : Authority) :
    activate ⟨.provisioner, .closed, setBy⟩ = .error .closed := by rfl

theorem repeated_activation_keeps_the_author (setBy : Authority) :
    activate ⟨.provisioner, .active, setBy⟩ = .ok ⟨.provisioner, .active, setBy⟩ := by rfl

theorem an_operator_suspension_refuses_activation :
    activate ⟨.provisioner, .suspended, .operator⟩ = .error .operatorHold := by rfl

theorem an_operator_can_suspend_and_lift (origin : Authority) (setBy : Authority) :
    operatorSetStatus ⟨origin, .active, setBy⟩ .suspended =
      .ok ⟨origin, .suspended, .operator⟩ ∧
    operatorSetStatus ⟨origin, .suspended, setBy⟩ .active =
      .ok ⟨origin, .active, .operator⟩ := ⟨rfl, rfl⟩

theorem operator_closure_is_terminal (origin : Authority) (setBy : Authority) :
    operatorSetStatus ⟨origin, .closed, setBy⟩ .active = .error .closed ∧
    operatorSetStatus ⟨origin, .closed, setBy⟩ .closed =
      .ok ⟨origin, .closed, .operator⟩ := ⟨rfl, rfl⟩

/-- Whatever an account held, once an operator suspends it no provisioner
activation succeeds until an operator writes the status again. -/
theorem an_operator_suspension_holds (a t : Account)
    (suspended : operatorSetStatus a .suspended = .ok t) :
    ∃ r, activate t = .error r := by
  obtain ⟨origin, status, setBy⟩ := a
  cases status <;> cases origin <;>
    simp [operatorSetStatus] at suspended <;> subst suspended <;> exact ⟨_, rfl⟩

/-- A provisioner's activation can only produce an active account, and never
changes who created it. -/
theorem activation_only_activates (a t : Account) (activated : activate a = .ok t) :
    t.status = .active ∧ t.origin = a.origin := by
  obtain ⟨origin, status, setBy⟩ := a
  cases origin <;> cases status <;> cases setBy <;>
    simp [activate] at activated <;> subst activated <;> exact ⟨rfl, rfl⟩

structure Funding where
  topup : Nat
  deposited : Nat
  deriving DecidableEq, Repr

structure Receipt where
  before : Funding
  after : Funding
  deriving DecidableEq, Repr

def deposit (before : Funding) (units : Nat) : Receipt :=
  ⟨before, ⟨before.topup + units, before.deposited + units⟩⟩

theorem receipt_records_the_predecessor (before : Funding) (units : Nat) :
    (deposit before units).before = before := by rfl

theorem receipt_records_exact_funding (before : Funding) (units : Nat) :
    (deposit before units).after.topup = before.topup + units ∧
    (deposit before units).after.deposited = before.deposited + units := by
  exact ⟨rfl, rfl⟩

theorem serialized_receipts_join (before : Funding) (a b : Nat) :
    (deposit (deposit before a).after b).before = (deposit before a).after := by rfl

theorem deposits_conserve_the_funding_difference
    (before : Funding) (units : Nat) (bounded : before.topup ≤ before.deposited) :
    (deposit before units).after.deposited - (deposit before units).after.topup =
      before.deposited - before.topup := by
  simp only [deposit]
  omega

/-! Replacements model the complete budget schedule as a value. Serialization
is a precondition: PostgreSQL row locks and the memory mutex are witnessed
separately by the concurrent backend tests. -/
structure ReplacementReceipt (α : Type) where
  before : α
  after : α

def replaceValue {α : Type} (before after : α) : ReplacementReceipt α :=
  ⟨before, after⟩

theorem replacement_records_predecessor {α : Type} (before after : α) :
    (replaceValue before after).before = before := by rfl

theorem serialized_replacements_join {α : Type} (before a b : α) :
    (replaceValue (replaceValue before a).after b).before =
      (replaceValue before a).after := by rfl

theorem repeated_replacement_is_a_noop {α : Type} (value : α) :
    (replaceValue value value).before = (replaceValue value value).after := by rfl

structure CredentialState where
  account : Nat
  key : Nat
  revoked : Bool
  deriving DecidableEq, Repr

def retire (before : CredentialState) : ReplacementReceipt CredentialState :=
  replaceValue before { before with revoked := true }

theorem retirement_preserves_identity (before : CredentialState) :
    (retire before).after.account = before.account ∧
    (retire before).after.key = before.key := by exact ⟨rfl, rfl⟩

theorem retirement_records_predecessor (before : CredentialState) :
    (retire before).before = before := by rfl

theorem repeated_retirement_is_a_noop (before : CredentialState) :
    (retire (retire before).after).before = (retire (retire before).after).after := by rfl

theorem retirement_is_terminal (before : CredentialState) :
    (retire (retire before).after).after.revoked = true := by rfl

/-! Store-owned snapshot generations. The predecessor is the locked live or
revoked watermark, or zero for first publication; limit is u64::MAX in memory
and i64::MAX in PostgreSQL. No caller generation enters this transition.
Atomicity and agreement between the SQL column, push and receipt are backend
assumptions, witnessed separately by the mirrored integration tests. -/
def nextGeneration (previous limit : Nat) : Option Nat :=
  if previous < limit then some (previous + 1) else none

theorem allocated_generation_advances_exactly_once (previous limit next : Nat)
    (accepted : nextGeneration previous limit = some next) :
    next = previous + 1 ∧ previous < next ∧ next ≤ limit := by
  unfold nextGeneration at accepted
  split at accepted
  · simp only [Option.some.injEq] at accepted
    omega
  · cases accepted

theorem exhausted_generation_refuses (previous limit : Nat) (full : limit ≤ previous) :
    nextGeneration previous limit = none := by
  simp [nextGeneration, Nat.not_lt.mpr full]

theorem first_allocated_generation_is_one (limit : Nat) (positive : 0 < limit) :
    nextGeneration 0 limit = some 1 := by
  simp [nextGeneration, positive]

/-! Operator-approved provisioner policy (GH-43). Each field stands for its
complete typed Rust value, not a digest. Exact comparison and manifest parsing
are implementation assumptions; HTTP tests witness their field coverage and
pre-store refusal. One request uses one immutable authorization generation. -/
structure SnapshotPolicy where
  pricing : Nat
  limits : Nat
  permissions : Nat
  revision : Nat
  deriving DecidableEq, Repr

def approvePolicy (templates : List SnapshotPolicy) (candidate : SnapshotPolicy)
    (strict : Bool) : Option SnapshotPolicy :=
  if strict = true ∧ candidate ∈ templates then some candidate else none

theorem approved_policy_is_one_whole_template
    (templates : List SnapshotPolicy) (candidate published : SnapshotPolicy) (strict : Bool)
    (accepted : approvePolicy templates candidate strict = some published) :
    candidate = published ∧ strict = true ∧ candidate ∈ templates := by
  unfold approvePolicy at accepted
  split at accepted
  next h => exact ⟨Option.some.inj accepted, h.1, h.2⟩
  next h => cases accepted

theorem an_approved_strict_policy_can_publish
    (templates : List SnapshotPolicy) (candidate : SnapshotPolicy)
    (approved : candidate ∈ templates) :
    approvePolicy templates candidate true = some candidate := by
  simp [approvePolicy, approved]

theorem an_empty_template_set_grants_nothing (candidate : SnapshotPolicy) (strict : Bool) :
    approvePolicy [] candidate strict = none := by
  simp [approvePolicy]

theorem templates_never_authorize_elastic (templates : List SnapshotPolicy) (candidate : SnapshotPolicy) :
    approvePolicy templates candidate false = none := by
  simp [approvePolicy]

theorem removing_approval_refuses_future_publication
    (templates : List SnapshotPolicy) (candidate : SnapshotPolicy) (strict : Bool)
    (unapproved : candidate ∉ templates) :
    approvePolicy templates candidate strict = none := by
  simp [approvePolicy, unapproved]

end Tollgate.ControlPlane
