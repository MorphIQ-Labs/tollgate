-- Exact credential expiry. The rename fences old expiry reads/writes even on
-- connections opened before migration. Old cached authority must be cleared
-- by the coordinated verifier/session rollout in CREDENTIAL_PROJECTION.md.
ALTER TABLE tollgate_credential_keys
    RENAME COLUMN not_after_us TO not_after_floor_us;
ALTER TABLE tollgate_credential_keys
    ADD COLUMN not_after_submicro_ns SMALLINT,
    ADD COLUMN not_after_is_lower_bound BOOLEAN NOT NULL DEFAULT FALSE;

-- Old microseconds truncated toward zero. Authority uses the EARLIEST instant
-- consistent with that value: positive buckets start at u*1000 ns, negative
-- and zero buckets at u*1000-999 ns. Clamp only that uncertainty bound to the
-- timestamp domain's minimum; reject out-of-domain history below. Concurrent
-- assignments use the original row values. No-expiry records stay exact NULL.
-- The existing statement trigger advances the credential revision atomically
-- when any finite rows change; overflow aborts the entire migration.
UPDATE tollgate_credential_keys SET
    not_after_submicro_ns = CASE
        WHEN not_after_floor_us <= 0 AND not_after_floor_us > -377705023201000000 THEN 1
        ELSE 0 END,
    not_after_floor_us = CASE
        WHEN not_after_floor_us <= 0 AND not_after_floor_us > -377705023201000000
        THEN not_after_floor_us - 1 ELSE not_after_floor_us END,
    not_after_is_lower_bound = TRUE
WHERE not_after_floor_us IS NOT NULL;

ALTER TABLE tollgate_credential_keys
    ALTER COLUMN not_after_is_lower_bound DROP DEFAULT,
    ADD CONSTRAINT tollgate_credential_keys_expiry_domain CHECK (
        (not_after_floor_us IS NULL AND not_after_submicro_ns IS NULL
            AND NOT not_after_is_lower_bound)
        OR
        (not_after_floor_us IS NOT NULL AND not_after_submicro_ns IS NOT NULL
            AND not_after_floor_us BETWEEN -377705023201000000 AND 253402207200999999
            AND not_after_submicro_ns BETWEEN 0 AND 999)
    );
