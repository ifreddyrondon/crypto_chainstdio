-- +goose Up
-- +goose StatementBegin
ALTER TABLE ledgers
    DROP COLUMN IF EXISTS id,
    DROP COLUMN IF EXISTS updated_at;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE ledgers
    ADD COLUMN id uuid NOT NULL DEFAULT uuid_generate_v4(),
    ADD COLUMN updated_at TIMESTAMP DEFAULT now();
-- +goose StatementEnd
