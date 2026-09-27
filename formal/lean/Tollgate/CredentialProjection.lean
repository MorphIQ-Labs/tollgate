import Init.Omega

/-!
Read-only credential projection (GL-108). Times and durations are exact integer
ticks, with positive maxAge. One publisher selects a complete validated source
table. Verification authenticity, coherent backend reads, atomic publication,
accurate clocks, and the caller checking evidence at its supplied time are
assumptions. Rust tests separately exercise timestamp overflow, transport
failure, cancellation, and the actual HMAC/session implementation.
-/
namespace Tollgate.CredentialProjection

def deadline (started maxAge : Int) (expiry : Option Int) : Int :=
  match expiry with
  | none => started + maxAge
  | some ending => min ending (started + maxAge)

theorem freshness_is_never_extended (started maxAge : Int) (expiry : Option Int) :
    deadline started maxAge expiry ≤ started + maxAge := by
  cases expiry with
  | none => simp [deadline]
  | some ending => exact Int.min_le_right ending (started + maxAge)

theorem source_expiry_is_never_extended (started maxAge ending : Int) :
    deadline started maxAge (some ending) ≤ ending := by
  exact Int.min_le_left ending (started + maxAge)

theorem accepted_evidence_obeys_both_bounds (started maxAge ending now : Int)
    (accepted : now < deadline started maxAge (some ending)) :
    now < ending ∧ now < started + maxAge := by
  have := freshness_is_never_extended started maxAge (some ending)
  have := source_expiry_is_never_extended started maxAge ending
  omega

theorem slow_fetches_cannot_create_a_new_validity_window
    (started maxAge completed : Int) (expiry : Option Int)
    (late : started + maxAge ≤ completed) :
    ¬ completed < deadline started maxAge expiry := by
  have := freshness_is_never_extended started maxAge expiry
  omega

theorem validated_timing_covers_two_fetches_and_the_pause
    (started maxAge timeout pause previousFetch nextFetch : Int)
    (configuration : pause + 2 * timeout < maxAge)
    (previousBound : previousFetch ≤ timeout) (nextBound : nextFetch ≤ timeout) :
    started + previousFetch + pause + nextFetch < started + maxAge := by omega

-- A published table maps a principal to evidence's exclusive validity deadline.
abbrev Table := Nat → Option Int

def refresh (previous : Table) (complete : Option Table) : Table :=
  match complete with
  | none => previous
  | some next => next

theorem failure_preserves_the_original_deadline (previous : Table) (key : Nat) :
    refresh previous none key = previous key := by rfl

theorem complete_replacement_withdraws_removed_keys
    (previous next : Table) (key : Nat) (absent : next key = none) :
    refresh previous (some next) key = none := by exact absent

theorem an_empty_success_withdraws_every_key (previous : Table) (key : Nat) :
    refresh previous (some (fun _ => none)) key = none := by rfl

-- A session retains its already-issued evidence; publishing a later table
-- does not renew it. This bound deliberately does not promise instant eviction.
theorem cached_evidence_expires_at_its_original_deadline (issued now : Int)
    (expired : issued ≤ now) : ¬ now < issued := by omega

-- Paging refinement: with a fixed strictly ordered catalogue, an exclusive
-- key cursor selects the unvisited suffix. take/drop models that split; Rust
-- backend tests establish the ordering/cursor interpretation. A revision
-- mismatch discards the entire candidate, so this theorem applies only to a
-- coherent drain. Transport and PostgreSQL snapshot isolation remain premises.
def drain (limit : Nat) : Nat → List Nat → Option (List Nat)
  | 0, [] => some []
  | 0, _ :: _ => none
  | fuel + 1, records =>
      if records.length ≤ limit then some records
      else (drain limit fuel (records.drop limit)).map (records.take limit ++ ·)

theorem completed_drain_is_exact (limit fuel : Nat) (records result : List Nat)
    (complete : drain limit fuel records = some result) : result = records := by
  induction fuel generalizing records result with
  | zero =>
      cases records with
      | nil => simpa [drain] using complete.symm
      | cons x xs => simp [drain] at complete
  | succ fuel ih =>
      simp only [drain] at complete
      split at complete
      · exact Option.some.inj complete.symm
      · cases pending : drain limit fuel (records.drop limit) with
        | none => simp [pending] at complete
        | some suffix =>
          have exactSuffix := ih (records.drop limit) suffix pending
          simp only [pending, Option.map_some, Option.some.injEq] at complete
          rw [← complete, exactSuffix, List.take_append_drop]

theorem sufficient_budget_completes (limit fuel : Nat) (positive : 0 < limit)
    (records : List Nat) (budget : records.length ≤ fuel) :
    drain limit fuel records = some records := by
  induction fuel generalizing records with
  | zero =>
      cases records with
      | nil => rfl
      | cons head tail => simp at budget
  | succ fuel ih =>
      simp only [drain]
      split
      · rfl
      · have remaining : (records.drop limit).length ≤ fuel := by
          simp only [List.length_drop]
          omega
        rw [ih (records.drop limit) remaining]
        simp [List.take_append_drop]

theorem completed_drain_misses_no_key (limit fuel : Nat) (records result : List Nat)
    (complete : drain limit fuel records = some result) (key : Nat) :
    key ∈ result ↔ key ∈ records := by
  rw [completed_drain_is_exact limit fuel records result complete]

theorem completed_drain_visits_each_key_once (limit fuel : Nat) (records result : List Nat)
    (unique : records.Nodup) (complete : drain limit fuel records = some result) :
    result.Nodup := by
  rw [completed_drain_is_exact limit fuel records result complete]
  exact unique

-- The source certifies that a committed revision contains these live keys;
-- both server and instance time narrow that set. No claim is made that a key
-- cannot be revoked in a later commit immediately before publication.
def narrowed (live : List Nat) (serverUnexpired instanceUnexpired : Nat → Bool) : List Nat :=
  live.filter (fun key => serverUnexpired key && instanceUnexpired key)

theorem installed_keys_have_source_membership_and_both_expiry_checks
    (live : List Nat) (server localClock : Nat → Bool) (key : Nat)
    (installed : key ∈ narrowed live server localClock) :
    key ∈ live ∧ server key = true ∧ localClock key = true := by
  simpa [narrowed, List.mem_filter, Bool.and_eq_true] using installed

def coherentRefresh (previous : Table) (firstRevision lastRevision : Nat)
    (candidate : Table) : Table :=
  if firstRevision = lastRevision then candidate else previous

theorem mixed_revisions_preserve_the_predecessor (previous candidate : Table)
    (first last : Nat) (different : first ≠ last) :
    coherentRefresh previous first last candidate = previous := by
  simp [coherentRefresh, different]

-- GL-118: legacy microseconds discarded precision toward zero. Credential
-- authority uses the earliest compatible instant, intersected with the
-- timestamp domain. SQL migration and finite Rust decoding are separate
-- implementation witnesses; these are exact integer nanoseconds.
def legacyExpiryLower (minimum micros : Int) : Int :=
  max minimum (if 0 < micros then 1000 * micros else 1000 * micros - 999)

theorem positive_legacy_expiry_is_never_extended (minimum micros expiry : Int)
    (positive : 0 < micros) (domain : minimum ≤ expiry)
    (low : 1000 * micros ≤ expiry) (high : expiry ≤ 1000 * micros + 999) :
    legacyExpiryLower minimum micros ≤ expiry ∧
      expiry - legacyExpiryLower minimum micros ≤ 999 := by
  simp [legacyExpiryLower, positive]
  omega

theorem negative_legacy_expiry_is_never_extended (minimum micros expiry : Int)
    (negative : micros < 0) (domain : minimum ≤ expiry)
    (low : 1000 * micros - 999 ≤ expiry) (high : expiry ≤ 1000 * micros) :
    legacyExpiryLower minimum micros ≤ expiry ∧
      expiry - legacyExpiryLower minimum micros ≤ 999 := by
  have nonpositive : ¬ 0 < micros := by omega
  simp [legacyExpiryLower, nonpositive]
  omega

theorem zero_legacy_expiry_is_never_extended (minimum expiry : Int)
    (domain : minimum ≤ expiry) (low : -999 ≤ expiry) (high : expiry ≤ 999) :
    legacyExpiryLower minimum 0 ≤ expiry ∧
      expiry - legacyExpiryLower minimum 0 ≤ 1998 := by
  simp [legacyExpiryLower]
  omega

theorem a_reset_session_cannot_outlive_conservative_source_expiry
    (started maxAge lower original now : Int) (conservative : lower ≤ original)
    (accepted : now < deadline started maxAge (some lower)) : now < original := by
  have := source_expiry_is_never_extended started maxAge lower
  omega

end Tollgate.CredentialProjection
