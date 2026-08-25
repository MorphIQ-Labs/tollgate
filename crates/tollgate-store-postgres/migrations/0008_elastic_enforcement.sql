-- Elastic enforcement (#1): unfunded spend becomes a first-class ledger fact.
--
-- 1. `tollgate_accounts.overage_recorded`. The conservation equation gains a
--    second term on its *funding* side:
--
--        deposited + overage_recorded
--            = balance + active grants + settled usage + settlement loss
--
--    `deposited` had only ever moved on account creation and deposit, so it
--    could not express "units extended on credit". Recording overage usage
--    without this column would leave the equation open by exactly the overage
--    -- the reconciliation check would report corruption on a correctly
--    working ledger. DEFAULT 0 backfills every existing row with the only
--    value that can be true of an account that predates the feature, and the
--    non-negative CHECK follows 0003's stated policy for every unit column
--    (INVARIANTS.md #11).
--
-- 2. `tollgate_usage_events.lease_id` / `fencing_token` become nullable, and a
--    CHECK makes them all-or-nothing.
--
--    An overage event has no lease, and must not be given one. Attributing
--    unfunded spend to a real lease drives that lease's `used` past its
--    `granted`, and `reclaim_expired_batch` then computes a negative
--    `granted - used` credit: `to_units` refuses it, the sweep transaction
--    rolls back, and it rolls back again on every retry -- so that account's
--    expired leases would never be reclaimed again. A sentinel lease id would
--    do the same thing while also colliding with the classifier's namespace.
--
--    The CHECK is what keeps "no lease" from decaying into "half a lease": a
--    row with an id and no token, or a token naming no lease, would satisfy
--    neither the leased path's capability check nor the overage path's
--    absence of one. `UsageSource` makes the same state unrepresentable in
--    Rust; this is the storage half of that guarantee.
--
-- Both statements are plain ALTERs. `tollgate_usage_events` can be large, but
-- DROP NOT NULL is catalogue-only, and the ADD COLUMN ... DEFAULT 0 on
-- `tollgate_accounts` uses PostgreSQL 11+ non-rewriting defaults. The new
-- CHECKs are validated: no legitimate pre-existing row can fail them, because
-- every row written before this migration had both columns NOT NULL and no
-- account could have had a non-zero overage.
--
-- Compatibility: forward-only, and mixed-version rollout is safe in the
-- fail-closed direction only. A pre-#1 binary reading a row written after it
-- cannot decode `UsageSource::Overage` and will error rather than mis-bill;
-- it also cannot see `overage_recorded`, so its conservation check would
-- report a violation on an elastic account. Roll forward, and do not enable
-- elastic mode for an account until the whole fleet carries this migration.
--
-- Recovery: one transaction, so a failure leaves nothing behind. To undo,
-- provided no overage has been recorded (verify first -- dropping the column
-- with non-zero rows destroys billing evidence):
--     SELECT count(*) FROM tollgate_accounts WHERE overage_recorded <> 0;
--     SELECT count(*) FROM tollgate_usage_events WHERE lease_id IS NULL;
--     ALTER TABLE tollgate_usage_events
--         DROP CONSTRAINT tollgate_usage_events_lease_all_or_nothing,
--         ALTER COLUMN lease_id SET NOT NULL,
--         ALTER COLUMN fencing_token SET NOT NULL;
--     ALTER TABLE tollgate_accounts DROP COLUMN overage_recorded;
--     DELETE FROM _sqlx_migrations WHERE version = 8;

ALTER TABLE tollgate_accounts
    ADD COLUMN overage_recorded BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT tollgate_accounts_overage_recorded_nonneg
        CHECK (overage_recorded >= 0);

ALTER TABLE tollgate_usage_events
    ALTER COLUMN lease_id DROP NOT NULL,
    ALTER COLUMN fencing_token DROP NOT NULL,
    ADD CONSTRAINT tollgate_usage_events_lease_all_or_nothing
        CHECK ((lease_id IS NULL) = (fencing_token IS NULL));
