-- Reproduce GL-105's bounded lookup evidence with psql -X -v ON_ERROR_STOP=1
-- -f testing/credential_activity_plan.sql against a migrated test database.
-- Session-local tables copy the real indexes without revision triggers or
-- foreign keys. No durable data is changed. The predicate and requested-list
-- shapes match PostgresStore; these plans are measurements, not Rust proofs.
BEGIN;
CREATE TEMP TABLE tollgate_credential_keys
    (LIKE public.tollgate_credential_keys INCLUDING ALL) ON COMMIT DROP;
CREATE TEMP TABLE tollgate_credential_activity
    (LIKE public.tollgate_credential_activity INCLUDING ALL) ON COMMIT DROP;

INSERT INTO tollgate_credential_keys (key_id, account_id, principal, digest)
SELECT decode(lpad(to_hex(n),32,'0'),'hex'), decode(lpad('1',32,'0'),'hex'),
       decode(lpad(to_hex(n),32,'0'),'hex'), decode(lpad(to_hex(n),64,'0'),'hex')
FROM generate_series(1,100000) AS n;
INSERT INTO tollgate_credential_activity
SELECT key_id, 0 FROM tollgate_credential_keys;
ANALYZE tollgate_credential_keys;
ANALYZE tollgate_credential_activity;

-- 256 input events, two per credential. Attribution counts events before
-- aggregation even when no maximum needs updating.
SELECT array_agg(decode(lpad(to_hex(1+n%128),32,'0'),'hex'))::text AS keys,
       array_agg(decode(lpad('1',32,'0'),'hex'))::text AS accounts,
       array_agg(n::bigint)::text AS instants
FROM generate_series(1,256) AS n \gset activity_
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH matched AS MATERIALIZED (
    SELECT k.key_id, b.occurred_at_us
    FROM UNNEST(:'activity_keys'::bytea[], :'activity_accounts'::bytea[], :'activity_instants'::bigint[])
        AS b(key_id, account_id, occurred_at_us)
    JOIN LATERAL (
        SELECT key_id FROM tollgate_credential_keys
        WHERE key_id = b.key_id AND account_id = b.account_id LIMIT 1
    ) k ON true
), updated AS (
    INSERT INTO tollgate_credential_activity AS activity (key_id, last_committed_at_us)
    SELECT key_id, MAX(occurred_at_us) FROM matched GROUP BY key_id ORDER BY key_id
    ON CONFLICT (key_id) DO UPDATE
    SET last_committed_at_us = EXCLUDED.last_committed_at_us
    WHERE activity.last_committed_at_us < EXCLUDED.last_committed_at_us
    RETURNING key_id
) SELECT COUNT(*) FROM matched;

-- A complete operator chunk of 4,096 requested rows; duplicates deliberately
-- survive the join, including unknown and unobserved identities in API tests.
SELECT array_agg(decode(lpad(to_hex(n*17),32,'0'),'hex'))::text AS keys
FROM generate_series(1,4096) AS n \gset activity_
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT evidence.key_id IS NOT NULL, evidence.last_committed_at_us
FROM UNNEST(:'activity_keys'::bytea[])
    WITH ORDINALITY AS requested(key_id, ordinal)
LEFT JOIN LATERAL (
    SELECT k.key_id, a.last_committed_at_us
    FROM tollgate_credential_keys k
    LEFT JOIN tollgate_credential_activity a ON a.key_id = k.key_id
    WHERE k.key_id = requested.key_id LIMIT 1
) AS evidence ON true
ORDER BY requested.ordinal;
ROLLBACK;
