-- #108: bounded credential projection in key-id order, with a coherent revision.
-- Additive: old binaries keep using the same credential table. A statement
-- trigger advances the revision even for writes from those binaries, so a
-- mixed-version issuer cannot invisibly invalidate a paged read. Transition
-- tables avoid bumps for zero-row statements: duplicate issuance and repeated
-- retirement keep their existing results even at the maximum revision. One
-- bump per affected statement keeps bulk writes linear. Overflow aborts the
-- mutation transaction. Revisions identify stability, not issued-key counts.
--
-- Keep the account-id partial index from 0009: it remains useful for account
-- scoped inspection. This index supports the new global key-id range/order.
-- Expired but unrevoked entries can still require filtering; LIMIT bounds
-- output cardinality, not the number of candidate rows the planner examines.
--
-- This migration is transactional. Index creation briefly blocks credential
-- writes; schedule it for a maintenance window for a large existing catalogue.
-- Older readers/writers remain compatible. Application rollback can leave all
-- objects in place. Do not remove revision tracking while paged readers run.
-- Removing these additive objects later requires a forward recovery migration,
-- after all paged readers have stopped; never edit an applied migration.

CREATE TABLE tollgate_credential_revision (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0)
);
INSERT INTO tollgate_credential_revision (singleton) VALUES (TRUE);

CREATE FUNCTION tollgate_advance_credential_revision() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'TRUNCATE' THEN
        IF NOT EXISTS (SELECT 1 FROM changed_credentials) THEN
            RETURN NULL;
        END IF;
    END IF;
    UPDATE tollgate_credential_revision SET revision = revision + 1 WHERE singleton;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'credential revision row is missing';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER tollgate_credential_revision_inserted
AFTER INSERT ON tollgate_credential_keys
REFERENCING NEW TABLE AS changed_credentials
FOR EACH STATEMENT EXECUTE FUNCTION tollgate_advance_credential_revision();
CREATE TRIGGER tollgate_credential_revision_updated
AFTER UPDATE ON tollgate_credential_keys
REFERENCING NEW TABLE AS changed_credentials
FOR EACH STATEMENT EXECUTE FUNCTION tollgate_advance_credential_revision();
CREATE TRIGGER tollgate_credential_revision_deleted
AFTER DELETE ON tollgate_credential_keys
REFERENCING OLD TABLE AS changed_credentials
FOR EACH STATEMENT EXECUTE FUNCTION tollgate_advance_credential_revision();
CREATE TRIGGER tollgate_credential_revision_truncated
AFTER TRUNCATE ON tollgate_credential_keys
FOR EACH STATEMENT EXECUTE FUNCTION tollgate_advance_credential_revision();

CREATE INDEX tollgate_credential_keys_projection
    ON tollgate_credential_keys (key_id) WHERE revoked_at_us IS NULL;
