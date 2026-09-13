-- Exact instants use floor microseconds and a 0..999 nanosecond remainder.
-- The rename is also a compatibility fence: old allocation, release and
-- reclaim statements fail, including on connections open before migration.
-- This transaction waits for their existing table locks before changing rows.
ALTER TABLE tollgate_leases RENAME COLUMN expires_at_us TO expires_at_floor_us;
ALTER TABLE tollgate_leases
    ADD COLUMN expires_at_submicro_ns SMALLINT,
    ADD COLUMN expiry_is_upper_bound BOOLEAN NOT NULL DEFAULT TRUE;

-- Legacy writers truncated toward zero. Their lost fraction is unknowable:
-- for nonnegative stored microseconds the latest possible expiry is +999 ns;
-- for negative ones it is the stored instant itself. Preserve that safe upper
-- bound and its provenance, without changing any accounting field. This can
-- delay settlement by at most 999 ns, except the zero-microsecond bucket
-- (-999..=999 ns), whose uncertainty spans 1998 ns; never accelerate it.
UPDATE tollgate_leases
SET expires_at_submicro_ns = CASE WHEN expires_at_floor_us >= 0 THEN 999 ELSE 0 END;

ALTER TABLE tollgate_leases
    ALTER COLUMN expires_at_submicro_ns SET NOT NULL,
    ALTER COLUMN expiry_is_upper_bound DROP DEFAULT,
    ADD CONSTRAINT tollgate_leases_expiry_domain CHECK (
        expires_at_floor_us BETWEEN -377705023201000000 AND 253402207200999999
        AND expires_at_submicro_ns BETWEEN 0 AND 999
    );

DROP INDEX tollgate_leases_expiry;
CREATE INDEX tollgate_leases_expiry
    ON tollgate_leases (expires_at_floor_us, expires_at_submicro_ns) WHERE state = 0;
