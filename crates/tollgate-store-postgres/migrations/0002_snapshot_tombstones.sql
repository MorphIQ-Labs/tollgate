-- Preserve the last generation when a principal is revoked. A later publish
-- must be strictly newer than the tombstone, preventing delayed control-plane
-- writes from resurrecting stale authorization.
ALTER TABLE tollgate_snapshots
    ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE;
