-- #105: activity derives only from canonical accepted usage, outside the
-- credential table so 0013's revision triggers never see traffic updates.
-- The source key_id deliberately has no FK: unknown attribution must not
-- discard an otherwise valid bill. The aggregate references retained keys.
--
-- Additive and transactional: migrate before servers, then enable producers.
-- Old inserts omit key_id and receive NULL; old binaries ignore the aggregate.
-- Roll back binaries while retaining these objects and their history. Any
-- later schema removal requires a forward migration after writers stop;
-- never edit an applied migration or erase activity during binary rollback.
ALTER TABLE tollgate_usage_events ADD COLUMN key_id BYTEA
    CHECK (key_id IS NULL OR octet_length(key_id) = 16);

CREATE TABLE tollgate_credential_activity (
    key_id BYTEA PRIMARY KEY REFERENCES tollgate_credential_keys(key_id),
    -- Timestamp::MIN/MAX, including the final second's fractional part.
    last_committed_at_us BIGINT NOT NULL
        CHECK (last_committed_at_us BETWEEN -377705023201000000 AND 253402207200999999)
);
