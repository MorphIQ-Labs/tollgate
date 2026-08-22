-- Unit-valued ledger columns can never legitimately go negative; a negative
-- value is corruption that must be refused at write time, not normalised at
-- read time (issue #15). Plain ADD CONSTRAINT validates existing rows under
-- ACCESS EXCLUSIVE; tollgate_accounts holds one row per account, so the scan
-- is trivial (NOT VALID + VALIDATE CONSTRAINT is the live-upgrade alternative
-- if that ever changes).
ALTER TABLE tollgate_accounts
    ADD CONSTRAINT tollgate_accounts_deposited_nonneg CHECK (deposited >= 0),
    ADD CONSTRAINT tollgate_accounts_balance_nonneg CHECK (balance >= 0),
    ADD CONSTRAINT tollgate_accounts_usage_recorded_nonneg CHECK (usage_recorded >= 0),
    ADD CONSTRAINT tollgate_accounts_settlement_loss_nonneg CHECK (settlement_loss >= 0);
