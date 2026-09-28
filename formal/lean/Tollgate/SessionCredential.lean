import Init.Omega

/-!
The session-scoped credential cache (INVARIANTS.md 23). A session may reuse
the principal a verification produced only for a byte-identical credential
and only while that verification is still reusable at `now`. A missing or
different credential clears the session's proof before a replacement is
verified; failed and already-expired verifications are never cached. A hit
skips verification and nothing else: its answer is an identity, and
authorization is decided afterwards by admission against the current snapshot.

The verifier is an arbitrary function from credential bytes to an optional
`(principal, reusable_until)`, so every theorem holds for any scheme the
verifier seam admits (HMAC keys, tokens with expiry, certificates). The model
proves that every principal the cache returns is exactly what the verifier
answers for the presented bytes, and is still reusable at `now`; that a hit
requires the identical credential; that a failed or changed credential never
leaves the previous principal reusable; that an already-expired answer is
neither returned nor cached; and that sessions are isolated.

Byte comparison is modeled as equality; its constant-time implementation and
the wipe on drop are Rust obligations witnessed by the tollgate-auth tests.
-/
namespace Tollgate.SessionCredential

structure Entry where
  cred : Nat
  principal : Nat
  /-- `none` is an indefinite verification, withdrawn only by snapshot. -/
  reusableUntil : Option Nat
  deriving DecidableEq, Repr

/-- The verifier seam: credential bytes to principal and validity bound. -/
abbrev Verifier := Nat → Option (Nat × Option Nat)

def reusable (u : Option Nat) (now : Nat) : Bool :=
  match u with
  | none => true
  | some t => decide (now < t)

/-- Cached proofs by session. -/
abbrev Cache := Nat → Option Entry

def put (c : Cache) (session : Nat) (e : Option Entry) : Cache :=
  fun x => if x = session then e else c x

/-- Clear the session's proof, then verify afresh; cache only a reusable
answer. -/
def fresh (verify : Verifier) (c : Cache) (session cred now : Nat) : Cache × Option Nat :=
  let cleared := put c session none
  match verify cred with
  | none => (cleared, none)
  | some (p, u) =>
    if reusable u now then (put cleared session (some ⟨cred, p, u⟩), some p)
    else (cleared, none)

def authenticate (verify : Verifier) (c : Cache) (session cred now : Nat) : Cache × Option Nat :=
  match c session with
  | some e =>
    if e.cred = cred ∧ reusable e.reusableUntil now then (c, some e.principal)
    else fresh verify c session cred now
  | none => fresh verify c session cred now

/-- Every cached proof is what the verifier answers for its bytes. -/
def Sound (verify : Verifier) (c : Cache) : Prop :=
  ∀ session e, c session = some e → verify e.cred = some (e.principal, e.reusableUntil)

theorem put_same (c : Cache) (s : Nat) (e : Option Entry) : put c s e s = e := by simp [put]

theorem put_other (c : Cache) (s x : Nat) (e : Option Entry) (h : x ≠ s) : put c s e x = c x := by
  simp [put, h]

theorem fresh_sound (verify : Verifier) (c : Cache) (session cred now : Nat)
    (h : Sound verify c) : Sound verify (fresh verify c session cred now).1 := by
  intro x e hx
  unfold fresh at hx
  split at hx
  · simp only at hx
    by_cases hs : x = session
    · subst hs; simp [put] at hx
    · rw [put_other _ _ _ _ hs] at hx; exact h x e hx
  · rename_i p u hv
    split at hx
    · simp only at hx
      by_cases hs : x = session
      · subst hs; simp [put] at hx; subst hx; exact hv
      · rw [put_other _ _ _ _ hs, put_other _ _ _ _ hs] at hx; exact h x e hx
    · simp only at hx
      by_cases hs : x = session
      · subst hs; simp [put] at hx
      · rw [put_other _ _ _ _ hs] at hx; exact h x e hx

theorem authenticate_sound (verify : Verifier) (c : Cache) (session cred now : Nat)
    (h : Sound verify c) : Sound verify (authenticate verify c session cred now).1 := by
  unfold authenticate
  split
  · split
    · exact h
    · exact fresh_sound verify c session cred now h
  · exact fresh_sound verify c session cred now h

theorem fresh_answer (verify : Verifier) (c : Cache) (session cred now p : Nat)
    (h : (fresh verify c session cred now).2 = some p) :
    ∃ u, verify cred = some (p, u) ∧ reusable u now = true := by
  unfold fresh at h
  split at h
  · simp at h
  · rename_i p' u hv
    split at h
    · rename_i hr; simp at h; subst h; exact ⟨u, hv, hr⟩
    · simp at h

/-! ## Every answer is a current verification of the presented bytes -/

/-- Whatever the cache returns is exactly what the verifier answers for the
presented credential, and that answer is still reusable at `now`. -/
theorem answer_is_current_verification (verify : Verifier) (c : Cache) (session cred now p : Nat)
    (hs : Sound verify c) (h : (authenticate verify c session cred now).2 = some p) :
    ∃ u, verify cred = some (p, u) ∧ reusable u now = true := by
  unfold authenticate at h
  split at h
  · rename_i e he
    split at h
    · rename_i hc
      simp at h
      subst h
      obtain ⟨hcred, hr⟩ := hc
      exact ⟨e.reusableUntil, hcred ▸ hs session e he, hr⟩
    · exact fresh_answer verify c session cred now p h
  · exact fresh_answer verify c session cred now p h

/-- A hit, answering without verification, requires the identical credential
and a proof still reusable at `now`. -/
theorem hit_requires_identical_and_reusable (verify : Verifier) (c : Cache) (session cred now : Nat)
    (e : Entry) (he : c session = some e) (hmiss : e.cred ≠ cred ∨ reusable e.reusableUntil now = false) :
    authenticate verify c session cred now = fresh verify c session cred now := by
  unfold authenticate
  rw [he]
  simp only
  rw [if_neg]
  rintro ⟨h1, h2⟩
  rcases hmiss with h | h
  · exact h h1
  · rw [h] at h2; cases h2

/-! ## A failed or changed credential never leaves the old principal usable -/

/-- After a fresh verification the session holds either nothing or a proof
for the credential just presented, never the previous one. -/
theorem fresh_replaces_or_clears (verify : Verifier) (c : Cache) (session cred now : Nat) :
    (fresh verify c session cred now).1 session = none ∨
    ∃ e, (fresh verify c session cred now).1 session = some e ∧ e.cred = cred := by
  unfold fresh
  split
  · left; simp [put]
  · split
    · right; simp [put]
    · left; simp [put]

/-- A failed verification leaves the session with no proof at all. -/
theorem failed_verification_clears (verify : Verifier) (c : Cache) (session cred now : Nat)
    (hv : verify cred = none) : (fresh verify c session cred now).1 session = none ∧
      (fresh verify c session cred now).2 = none := by
  simp [fresh, hv, put]

/-- An answer that is already past its validity is neither returned nor
cached. -/
theorem expired_answer_refused (verify : Verifier) (c : Cache) (session cred now p t : Nat)
    (hv : verify cred = some (p, some t)) (ht : t ≤ now) :
    (fresh verify c session cred now).2 = none ∧ (fresh verify c session cred now).1 session = none := by
  have : reusable (some t) now = false := by simp [reusable]; omega
  simp [fresh, hv, this, put]

/-! ## Sessions are isolated -/

theorem fresh_other (verify : Verifier) (c : Cache) (session cred now other : Nat)
    (h : other ≠ session) : (fresh verify c session cred now).1 other = c other := by
  unfold fresh
  split
  · simp [put, h]
  · split <;> simp [put, h]

/-- Authenticating in one session never changes another session's proof. -/
theorem sessions_isolated (verify : Verifier) (c : Cache) (session cred now other : Nat)
    (h : other ≠ session) : (authenticate verify c session cred now).1 other = c other := by
  unfold authenticate
  split
  · split
    · rfl
    · exact fresh_other verify c session cred now other h
  · exact fresh_other verify c session cred now other h

end Tollgate.SessionCredential
