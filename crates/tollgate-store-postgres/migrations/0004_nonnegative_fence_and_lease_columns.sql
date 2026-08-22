-- Companion to 0003 (issue #45): fence counters and lease unit columns are
-- as unable to go negative as the account ledger. Fences are seeded at 1 and
-- only incremented; lease unit columns are debit-bounded. A negative value in
-- any of them is corruption to refuse at write time. Same locking note as
-- 0003: plain ADD CONSTRAINT is fine at these row counts.
ALTER TABLE tollgate_accounts
    ADD CONSTRAINT tollgate_accounts_next_fence_nonneg CHECK (next_fence >= 0);
ALTER TABLE tollgate_leases
    ADD CONSTRAINT tollgate_leases_fencing_token_nonneg CHECK (fencing_token >= 0),
    ADD CONSTRAINT tollgate_leases_granted_nonneg CHECK (granted >= 0),
    ADD CONSTRAINT tollgate_leases_used_nonneg CHECK (used >= 0),
    ADD CONSTRAINT tollgate_leases_credited_nonneg CHECK (credited >= 0);
