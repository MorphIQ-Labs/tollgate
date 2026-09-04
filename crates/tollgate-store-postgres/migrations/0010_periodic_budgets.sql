-- Periodic budgets (#97): an allowance that is replenished every period and
-- does not carry over, alongside manual deposits that do.
--
-- 1. The conservation equation gains a sixth term, on its *sink* side:
--
--        deposited + overage_recorded
--            = balance + active grants + settled usage + settlement loss
--              + expired
--
--    Units funded by a period that has closed are neither spendable nor
--    billable, and before this column there was nowhere for them to rest.
--    Expiring an allowance without recording it would leave the equation open
--    by exactly the expired units -- reconciliation would report corruption on
--    a correctly working ledger, the same failure `overage_recorded` was added
--    to prevent in 0008.
--
-- 2. `allowance_balance` splits the balance without splitting the column.
--
--    `balance` stays what it has always been: everything the account can
--    spend, and the only number `acquire` compares a request against.
--    `allowance_balance` is the portion of it that came from the current
--    period's allowance, so the top-up portion is `balance -
--    allowance_balance` and no reader outside the rollover path changes.
--    A single balance could not express this: at a boundary it could only
--    expire the manual credits along with the allowance, or resurrect
--    allowance units that were already spent. The CHECK that it never exceeds
--    `balance` is what keeps the derived top-up portion non-negative.
--
--    Spend order is allowance first -- units with an expiry date are consumed
--    before units without one -- which is why the lease has to remember the
--    split it drew.
--
-- 3. `tollgate_leases.from_allowance` and `period_start_us` carry the boundary
--    decision to settlement.
--
--    An active lease at a boundary keeps serving to its own TTL (there is no
--    admission gap and no clock read on the request path); the boundary is
--    applied when the lease finally settles, in release and in the reclaim
--    sweep. Both need to know which half of the grant was allowance and which
--    period funded it. Without `from_allowance`, a lease funded entirely from
--    manual credits would return its units to the allowance bucket and have
--    them expired at the next boundary -- silently deleting units that never
--    had an expiry date.
--
--    Usage is untouched by either, which is what makes a usage event arriving
--    after the boundary bill against the period its lease was granted in.
--
-- 4. `budget_allowance` / `budget_period` / `budget_rollover` store the
--    schedule, and `period_start_us` the period the account is currently in.
--
--    All three schedule columns are nullable and all-or-nothing: NULL means
--    "no schedule", which is not the same as an allowance of zero (a schedule
--    that expires everything every month). `num_nonnulls` is what stops the
--    difference decaying into half a schedule -- a period with no allowance
--    would satisfy neither branch of the rollover. The period and rollover are
--    TEXT for the reason `status` is: a stored name survives a variant being
--    added ahead of it, where an ordinal silently re-labels every existing row.
--
--    `period_start_us` is the marker the rollover is idempotent against. It is
--    NOT NULL with a 0 (epoch) default even for unscheduled accounts, so
--    "which period is this lease from" is always answerable and the column
--    never has to be read as a tri-state.
--
-- Backfill: `allowance_balance = 0`, `expired = 0`, `period_start_us = 0`, no
-- schedule. Every unit an account held before this migration was deposited
-- manually, so it is a top-up; the equation stays exact for every pre-existing
-- account, and the feature is inert until a schedule is set. Backfilling
-- `allowance_balance = balance` instead would expire every existing account's
-- entire balance at its first boundary.
--
-- 5. `tollgate_accounts_due_rollover` is what keeps the pass cheap.
--
--    The rollover sweep shares the server's reclaim tick, so it asks "is any
--    account past its boundary" every few seconds and gets nothing back on all
--    but one of those calls a month. Partial on the schedule columns, so the
--    index holds only scheduled accounts and unscheduled ones cost nothing to
--    skip; `period_start_us` leads it because that is the range predicate.
--
-- The ALTERs are plain. `tollgate_accounts` holds one row per
-- account and `tollgate_leases` one per live lease; ADD COLUMN ... DEFAULT
-- uses PostgreSQL 11+ non-rewriting defaults, and the new CHECKs are validated
-- against backfilled values that cannot fail them (0 >= 0, 0 <= balance,
-- 0 <= granted, num_nonnulls(NULL,NULL,NULL) = 0).
--
-- Compatibility: forward-only and additive, so mixed-version rollout is safe
-- in both directions *while no schedule is set*. A pre-#97 binary ignores
-- every new column: it can still acquire, release and reclaim, but it credits
-- settlements to `balance` without maintaining `allowance_balance`, which
-- leaves the allowance portion overstated. Apply this migration fleet-wide
-- before setting any account's schedule.
--
-- Recovery: one transaction, so a failure leaves nothing behind. To undo,
-- provided no account has rolled a period (verify first -- dropping `expired`
-- with non-zero rows destroys the evidence that reconciliation needs):
--     SELECT count(*) FROM tollgate_accounts WHERE expired <> 0;
--     SELECT count(*) FROM tollgate_accounts WHERE budget_allowance IS NOT NULL;
--     ALTER TABLE tollgate_leases
--         DROP CONSTRAINT tollgate_leases_from_allowance_within_grant,
--         DROP CONSTRAINT tollgate_leases_from_allowance_nonneg,
--         DROP COLUMN period_start_us,
--         DROP COLUMN from_allowance;
--     ALTER TABLE tollgate_accounts
--         DROP CONSTRAINT tollgate_accounts_budget_all_or_nothing,
--         DROP CONSTRAINT tollgate_accounts_budget_allowance_nonneg,
--         DROP CONSTRAINT tollgate_accounts_allowance_within_balance,
--         DROP CONSTRAINT tollgate_accounts_allowance_balance_nonneg,
--         DROP CONSTRAINT tollgate_accounts_expired_nonneg,
--         DROP COLUMN period_start_us,
--         DROP COLUMN budget_rollover,
--         DROP COLUMN budget_period,
--         DROP COLUMN budget_allowance,
--         DROP COLUMN expired,
--         DROP COLUMN allowance_balance;
--     DELETE FROM _sqlx_migrations WHERE version = 10;

ALTER TABLE tollgate_accounts
    ADD COLUMN allowance_balance BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN expired           BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN budget_allowance  BIGINT,
    ADD COLUMN budget_period     TEXT,
    ADD COLUMN budget_rollover   TEXT,
    ADD COLUMN period_start_us   BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT tollgate_accounts_allowance_balance_nonneg
        CHECK (allowance_balance >= 0),
    ADD CONSTRAINT tollgate_accounts_allowance_within_balance
        CHECK (allowance_balance <= balance),
    ADD CONSTRAINT tollgate_accounts_expired_nonneg
        CHECK (expired >= 0),
    ADD CONSTRAINT tollgate_accounts_budget_allowance_nonneg
        CHECK (budget_allowance IS NULL OR budget_allowance >= 0),
    ADD CONSTRAINT tollgate_accounts_budget_all_or_nothing
        CHECK (num_nonnulls(budget_allowance, budget_period, budget_rollover) IN (0, 3));

CREATE INDEX IF NOT EXISTS tollgate_accounts_due_rollover
    ON tollgate_accounts (budget_period, period_start_us)
    WHERE budget_allowance IS NOT NULL;

ALTER TABLE tollgate_leases
    ADD COLUMN from_allowance  BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN period_start_us BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT tollgate_leases_from_allowance_nonneg
        CHECK (from_allowance >= 0),
    ADD CONSTRAINT tollgate_leases_from_allowance_within_grant
        CHECK (from_allowance <= granted);
