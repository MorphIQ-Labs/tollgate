-- Credential lifecycle (#104): the durable half of key management.
--
-- Stage one made the verifier's digest table a projection; this is the table
-- it projects. Until now credentials existed only in process memory, which
-- made them the one piece of control-plane state that did not survive a
-- restart and did not reach a second instance.
--
-- Column notes:
--
-- 1. `key_id BYTEA PRIMARY KEY` follows every other identifier here: 16-byte
--    big-endian, so a backend may use UUIDs without the core depending on a
--    uuid library. The primary key is what makes issuance non-destructive --
--    an INSERT of an existing id conflicts rather than silently retiring the
--    credential it would overwrite, whose digest is unrecoverable.
--
-- 2. `digest BYTEA NOT NULL` is HMAC-SHA256 output, exactly 32 bytes, and the
--    CHECK says so. This column is *not* sufficient to authenticate: the
--    server secret that produced it lives with the verifier and never reaches
--    a store, so a dump of this table verifies nothing and mints nothing.
--    That split is what HMAC is paid for, and it now holds across the
--    persistence boundary rather than only within one process.
--
-- 3. `principal BYTEA NOT NULL UNIQUE` is derived -- the digest's leading 128
--    bits -- rather than assigned. It is what the request path is keyed by,
--    and what a per-credential revocation tombstones. UNIQUE because two
--    credentials sharing a principal would be two accounts' keys colliding on
--    the identity admission decides with; at 128 bits of HMAC output that is
--    a corruption or a secret reuse, not an accident, and it fails the insert
--    rather than silently overwriting.
--
-- 4. `not_after_us` and `revoked_at_us` are nullable microsecond instants, the
--    same encoding as `expires_at_us` on leases. NULL means "no expiry of its
--    own" and "not retired" respectively, which are the common cases and the
--    ones a missing value should mean.
--
-- 5. The account reference is ON DELETE RESTRICT by omission: accounts are
--    never deleted here (creation is non-destructive and closure is a status),
--    so a credential cannot outlive its account or be orphaned by one.
--
-- Index: `active_keys` is the projection's source and runs on every refresh,
-- filtering revoked rows. A partial index on the live set keeps that read
-- proportional to live credentials rather than to every credential ever
-- issued, which is the number that grows forever.
--
-- Compatibility: additive and forward-only. No existing table is altered, so
-- a binary that predates this migration is unaffected -- it neither reads nor
-- writes these rows, and its conservation check does not consult them.
-- Mixed-version rollout is therefore safe in both directions, which is not
-- true of most migrations here and is worth stating explicitly: the feature is
-- inert until a deployment starts issuing keys.
--
-- Recovery: one transaction, so a failure leaves nothing behind. To undo,
-- provided no credential has been issued -- verify first, because dropping
-- the table destroys every digest and no digest can be recovered from
-- anything else:
--     SELECT count(*) FROM tollgate_credential_keys;
--     DROP TABLE tollgate_credential_keys;
--     DELETE FROM _sqlx_migrations WHERE version = 9;

CREATE TABLE IF NOT EXISTS tollgate_credential_keys (
    key_id        BYTEA PRIMARY KEY,
    account_id    BYTEA NOT NULL REFERENCES tollgate_accounts(account_id),
    principal     BYTEA NOT NULL UNIQUE,
    digest        BYTEA NOT NULL,
    not_after_us  BIGINT,
    revoked_at_us BIGINT,
    CONSTRAINT tollgate_credential_keys_digest_len
        CHECK (octet_length(digest) = 32),
    CONSTRAINT tollgate_credential_keys_principal_len
        CHECK (octet_length(principal) = 16)
);

CREATE INDEX IF NOT EXISTS tollgate_credential_keys_live
    ON tollgate_credential_keys (account_id)
    WHERE revoked_at_us IS NULL;
