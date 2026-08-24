-- Issue #51: "suspend this customer" had two unreconciled records.
-- `tollgate_accounts.active` gated lease acquisition; `AccountStatus` inside a
-- published snapshot gated admission. Nothing kept them equal, so an operator
-- could deactivate an account, get a success, and watch it keep serving.
--
-- Two schema changes, one for each half of the fix.
--
-- 1. The ledger becomes three-valued. `active BOOLEAN` cannot express
--    `Closed`, so "Closed is terminal" could only ever be a convention checked
--    against snapshots, in two hand-mirrored backends. As a status column the
--    rule is one comparison inside the store, under the row lock that performs
--    the transition. The CHECK spells the same three strings `AccountStatus`
--    does; `account_status_text_matches_its_serde_spelling` is what keeps the
--    Rust and SQL vocabularies from drifting apart.
--
-- 2. Snapshots become addressable by account. Unifying the two records means
--    "republish every snapshot of account X", and `tollgate_snapshots` is
--    keyed by principal with the account buried in JSONB under a *dual*
--    representation: `StoredId` serializes a JSON number for ids in the legacy
--    u64 range and a 32-hex string above it. `snapshot->>'account_id' = $1`
--    therefore matches one spelling and silently misses the other -- which
--    would be this issue's own defect, one layer down.
--
-- GENERATED ... STORED, not a column the Rust write path fills. That leaves
-- exactly one writer for the (snapshot, account_id) pair -- PostgreSQL,
-- evaluating a deterministic function of the row -- so the column cannot drift
-- from the JSONB it mirrors and no future writer can forget it. Reintroducing
-- a two-writer record is the defect class this migration exists to remove, and
-- not one to recreate one table over.
--
-- The number branch never casts to bigint. Ids in [2^63, 2^64) are legal and
-- serialize as JSON numbers that overflow it:
--     SELECT (('{"a": 9223372036854775808}'::jsonb)->>'a')::bigint;
--     ERROR:  value "9223372036854775808" is out of range for type bigint
-- jsonb keeps numbers as exact numeric, so the value is split into two
-- sub-2^32 halves that each fit, then zero-extended to the 16 bytes
-- `id_bytes()` writes. Anything else -- a boolean, a malformed string --
-- yields NULL rather than raising, so this can neither abort the migration nor
-- refuse a write. That set is exactly the rows `StoredSnapshot` already fails
-- to decode and the request path already refuses, so a NULL can never hide a
-- row that would be admitted.
--
-- Plain CREATE INDEX, unlike 0005. That migration indexes the unbounded lease
-- table, where a SHARE lock blocks lease acquisition and so the request path.
-- Here `ADD COLUMN ... STORED` already holds ACCESS EXCLUSIVE for a full
-- rewrite in this same transaction, so building inside that lock costs one
-- sort over a table already in cache, while CONCURRENTLY would buy nothing and
-- cost the `-- no-transaction` split plus 0005's INVALID-index recovery path.
-- Partial on `deleted = FALSE` for 0005's reason: it matches the republish
-- predicate exactly and stays proportional to *live* snapshots, while
-- tombstones are retained permanently by design (INVARIANTS.md #15).
--
-- Recovery: the whole file is one transaction, so a failure leaves nothing
-- behind and sqlx re-runs it. To undo it after a successful apply:
--     ALTER TABLE tollgate_snapshots DROP COLUMN account_id;
--     ALTER TABLE tollgate_accounts ADD COLUMN active BOOLEAN;
--     UPDATE tollgate_accounts SET active = (status = 'Active');
--     ALTER TABLE tollgate_accounts ALTER COLUMN active SET NOT NULL;
--     ALTER TABLE tollgate_accounts DROP COLUMN status;
--     DELETE FROM _sqlx_migrations WHERE version = 6;
-- Note that undoing the *schema* does not undo the *data*: a status change
-- advances generations, and INVARIANTS.md #15 forbids moving them back. An
-- accidental suspension is undone forward, by setting the status again.

ALTER TABLE tollgate_accounts ADD COLUMN status TEXT;
UPDATE tollgate_accounts SET status = CASE WHEN active THEN 'Active' ELSE 'Suspended' END;
ALTER TABLE tollgate_accounts
    ALTER COLUMN status SET NOT NULL,
    ADD CONSTRAINT tollgate_accounts_status_known
        CHECK (status IN ('Active', 'Suspended', 'Closed'));
ALTER TABLE tollgate_accounts DROP COLUMN active;

ALTER TABLE tollgate_snapshots
    ADD COLUMN account_id BYTEA GENERATED ALWAYS AS (
        CASE jsonb_typeof(snapshot -> 'account_id')
          WHEN 'string' THEN decode(snapshot ->> 'account_id', 'hex')
          WHEN 'number' THEN decode(
              lpad(to_hex(div((snapshot ->> 'account_id')::numeric, 4294967296)::int8), 24, '0')
           || lpad(to_hex(mod((snapshot ->> 'account_id')::numeric, 4294967296)::int8),  8, '0'),
              'hex')
        END
    ) STORED;

CREATE INDEX tollgate_snapshots_account
    ON tollgate_snapshots (account_id) WHERE deleted = FALSE;
