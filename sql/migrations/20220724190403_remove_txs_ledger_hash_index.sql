-- +goose Up
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_transaction_ledger_hsh;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE INDEX idx_transaction_ledger_hsh on transactions (ledger_hash);
-- +goose StatementEnd
