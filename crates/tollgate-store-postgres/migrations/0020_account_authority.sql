-- Account provenance for the provisioner role (#39): which kind of
-- control-plane authority created an account, and which one set its current
-- status.
--
-- A self-service account service holds a `provisioner` credential rather than
-- an operator one. It may administer only accounts a provisioner created, and
-- it may never undo a status an operator set -- otherwise a customer could
-- reverse an abuse suspension by retrying signup. Both rules are decided from
-- these two columns.
--
-- Column notes:
--
-- 1. `origin TEXT NOT NULL DEFAULT 'Operator'`. Written once by account
--    creation and never updated, so the server may read it before a separate
--    write without a race: nothing it checks can change underneath it.
--
-- 2. `status_set_by TEXT NOT NULL DEFAULT 'Operator'`. Rewritten by every
--    status write. It is read under the same `FOR UPDATE` row lock that
--    serializes status changes, which is what makes an operator hold
--    unraceable: a provisioner activation and an operator suspension of the
--    same account are ordered by that lock, and whichever lands second sees
--    the other's author.
--
-- 3. The defaults are `Operator`, and that is the safe direction rather than
--    the neutral one. Every account that predates this migration was created
--    with an operator credential, because no other kind existed; `Operator`
--    grants a provisioner nothing on those rows. `Provisioner` would hand a
--    newly deployed provisioner credential every existing account.
--
-- 4. Named strings and a CHECK, for the reasons `status` (0006) and
--    `capacity_class` (0012) use them: the Rust decoder refuses any value
--    outside the pair rather than defaulting, so corruption can never read as
--    provisioner authority.
--
-- 5. No index. Both columns are read only from an already-located row.
--
-- Locks and rewrite: `ADD COLUMN ... DEFAULT` uses PostgreSQL 11+
-- non-rewriting defaults; the ACCESS EXCLUSIVE lock covers the catalogue
-- update only, and every existing row takes the default, so the CHECKs cannot
-- fail.
--
-- Compatibility: additive and forward-only. An old writer omits both columns
-- and gets `Operator`, which is what an old writer is. A new writer against an
-- old schema fails loudly on an unknown column. Rollout order is schema, then
-- every server instance, then issuing provisioner credentials: a provisioner
-- credential is refused by any server binary that predates the role.
--
-- Recovery: reversible once no provisioner credential is configured.
--
--   ALTER TABLE tollgate_accounts
--       DROP CONSTRAINT IF EXISTS tollgate_accounts_origin_known,
--       DROP CONSTRAINT IF EXISTS tollgate_accounts_status_set_by_known,
--       DROP COLUMN IF EXISTS origin,
--       DROP COLUMN IF EXISTS status_set_by;
--   DELETE FROM _sqlx_migrations WHERE version = 20;
--
-- Dropping the columns forgets which accounts a provisioner created and which
-- suspensions an operator made; balances, statuses and usage are untouched.

ALTER TABLE tollgate_accounts
    ADD COLUMN origin TEXT NOT NULL DEFAULT 'Operator',
    ADD COLUMN status_set_by TEXT NOT NULL DEFAULT 'Operator',
    ADD CONSTRAINT tollgate_accounts_origin_known
        CHECK (origin IN ('Operator', 'Provisioner')),
    ADD CONSTRAINT tollgate_accounts_status_set_by_known
        CHECK (status_set_by IN ('Operator', 'Provisioner'));
