-- +goose Up
-- +goose StatementBegin
ALTER TABLE transactions
    DROP COLUMN IF EXISTS metadata;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE transactions
    ADD COLUMN metadata jsonb;
-- +goose StatementEnd
