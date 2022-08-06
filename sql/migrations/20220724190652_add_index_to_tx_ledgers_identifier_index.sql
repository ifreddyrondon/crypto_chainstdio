-- +goose Up
-- +goose StatementBegin
CREATE INDEX idx_transaction_ledger_index on transactions (ledger_index);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_transaction_ledger_index;
-- +goose StatementEnd
