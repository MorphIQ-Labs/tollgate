-- #64: enforce the unit domain on the billing event table, and the actual
-- nonzero domain on every persisted fence. Legitimate old writers already
-- emit nonnegative units and mint fences from counters seeded at one.
--
-- NOT VALID installs write guards without scanning the append-only usage
-- history under ACCESS EXCLUSIVE. Migration 0016 validates in a separate
-- transaction with the weaker validation lock. If old corruption prevents
-- validation, these guards remain installed and protect subsequent writes.
-- No value is clamped, deleted or silently repaired.
--
-- Rollout and recovery, including SQLx's older-catalogue startup refusal:
-- docs/USAGE_ACCOUNTING.md. Retain both migrations in any rollback binary.

ALTER TABLE tollgate_usage_events
    ADD CONSTRAINT tollgate_usage_events_units_nonneg
        CHECK (units >= 0) NOT VALID,
    ADD CONSTRAINT tollgate_usage_events_fencing_token_positive
        CHECK (fencing_token IS NULL OR fencing_token > 0) NOT VALID;

ALTER TABLE tollgate_accounts
    DROP CONSTRAINT tollgate_accounts_next_fence_nonneg,
    ADD CONSTRAINT tollgate_accounts_next_fence_positive
        CHECK (next_fence > 0) NOT VALID;

ALTER TABLE tollgate_leases
    DROP CONSTRAINT tollgate_leases_fencing_token_nonneg,
    ADD CONSTRAINT tollgate_leases_fencing_token_positive
        CHECK (fencing_token > 0) NOT VALID;
