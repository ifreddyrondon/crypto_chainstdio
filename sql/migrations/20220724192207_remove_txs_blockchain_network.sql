-- +goose Up
-- +goose StatementBegin
ALTER TABLE transactions
    DROP COLUMN IF EXISTS blockchain,
    DROP COLUMN IF EXISTS network;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE transactions
    ADD COLUMN blockchain INTEGER,
    ADD COLUMN network INTEGER,;
-- +goose StatementEnd
