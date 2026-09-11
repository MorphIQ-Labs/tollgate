-- Separate from installing the guards: VALIDATE CONSTRAINT takes SHARE
-- UPDATE EXCLUSIVE, allowing ordinary row reads/writes during the scan.
-- Existing invalid data aborts this migration and startup, while 0015 stays
-- committed. Reconcile the reported rows from authoritative billing and lease
-- evidence, then retry startup; never erase migration history to bypass it.

ALTER TABLE tollgate_usage_events
    VALIDATE CONSTRAINT tollgate_usage_events_units_nonneg,
    VALIDATE CONSTRAINT tollgate_usage_events_fencing_token_positive;

ALTER TABLE tollgate_accounts
    VALIDATE CONSTRAINT tollgate_accounts_next_fence_positive;

ALTER TABLE tollgate_leases
    VALIDATE CONSTRAINT tollgate_leases_fencing_token_positive;
