import Init.Omega

/-!
Control-plane authorization and administrative receipts (#98).
Credential authenticity is an input: these proofs do not establish TLS, RSA,
HMAC, clock accuracy, or a Rust refinement. A request reads one immutable
policy generation; ArcSwap publication supplies that implementation boundary.
Concurrent backend tests separately witness the transaction serialization
assumed by the receipt model. Arithmetic here is exact Nat, not SQL BIGINT.
-/
namespace Tollgate.ControlPlane

inductive Role | instance | operator
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

end Tollgate.ControlPlane
