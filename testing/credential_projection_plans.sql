-- Local Docker fixture only; all data changes roll back on completion or disconnect.
-- psql must use ON_ERROR_STOP=1. Measures the same predicates, range and lookahead as KeySource.
BEGIN;
TRUNCATE tollgate_credential_keys;
INSERT INTO tollgate_credential_keys (key_id, account_id, principal, digest, revoked_at_us)
SELECT decode(lpad(to_hex(n),32,'0'),'hex'), a.account_id,
       decode(lpad(to_hex(n),32,'0'),'hex'),
       decode(lpad(to_hex(n),32,'0') || repeat('0',32),'hex'),
       CASE WHEN n <= 100000 THEN 0 ELSE NULL END
FROM generate_series(1,100256) n CROSS JOIN (SELECT account_id FROM tollgate_accounts LIMIT 1) a;
ANALYZE tollgate_credential_keys;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT key_id, account_id, principal, digest, not_after_us
FROM tollgate_credential_keys WHERE revoked_at_us IS NULL
AND (not_after_us IS NULL OR not_after_us > 100000000)
AND key_id >= decode(repeat('0',32),'hex') ORDER BY key_id LIMIT 65;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT key_id, account_id, principal, digest, not_after_us
FROM tollgate_credential_keys WHERE revoked_at_us IS NULL
AND (not_after_us IS NULL OR not_after_us > 100000000)
AND key_id > decode(lpad(to_hex(100128),32,'0'),'hex') ORDER BY key_id LIMIT 65;
ROLLBACK;
