-- +goose Up
-- +goose StatementBegin
CREATE INDEX idx_ledger_identifier_index on ledgers (identifier_index);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX idx_ledger_identifier_index;
-- +goose StatementEnd
