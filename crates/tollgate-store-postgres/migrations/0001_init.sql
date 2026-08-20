-- Initial tollgate schema. IF NOT EXISTS keeps this migration safe on
-- databases bootstrapped by the pre-migration embedded schema.

CREATE TABLE IF NOT EXISTS tollgate_accounts (
    account_id      BYTEA PRIMARY KEY,
    balance         BIGINT NOT NULL,
    deposited       BIGINT NOT NULL,
    active          BOOLEAN NOT NULL,
    next_fence      BIGINT NOT NULL,
    usage_recorded  BIGINT NOT NULL,
    settlement_loss BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS tollgate_leases (
    lease_id      BYTEA PRIMARY KEY,
    account_id    BYTEA NOT NULL REFERENCES tollgate_accounts(account_id),
    fencing_token BIGINT NOT NULL,
    granted       BIGINT NOT NULL,
    used          BIGINT NOT NULL,
    credited      BIGINT NOT NULL,
    expires_at_us BIGINT NOT NULL,
    state         SMALLINT NOT NULL
);
CREATE INDEX IF NOT EXISTS tollgate_leases_expiry
    ON tollgate_leases (expires_at_us) WHERE state = 0;
CREATE TABLE IF NOT EXISTS tollgate_usage_events (
    request_id    BYTEA PRIMARY KEY,
    account_id    BYTEA NOT NULL,
    lease_id      BYTEA NOT NULL,
    fencing_token BIGINT NOT NULL,
    units         BIGINT NOT NULL,
    occurred_at_us BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS tollgate_snapshots (
    principal  BYTEA PRIMARY KEY,
    generation BIGINT NOT NULL,
    snapshot   JSONB NOT NULL
);
