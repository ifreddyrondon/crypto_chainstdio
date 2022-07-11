-- +goose Up
-- +goose StatementBegin
ALTER TABLE transactions
    DROP COLUMN IF EXISTS id,
    DROP COLUMN IF EXISTS fee_amount,
    DROP COLUMN IF EXISTS fee_currency,
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS timestamp,
    DROP COLUMN IF EXISTS updated_at;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE transactions
    ADD COLUMN id uuid NOT NULL DEFAULT uuid_generate_v4(),
    ADD COLUMN fee_amount NUMERIC,
    ADD COLUMN fee_currency INTEGER,
    ADD COLUMN status INTEGER,
    ADD COLUMN timestamp TIMESTAMP,
    ADD COLUMN updated_at TIMESTAMP DEFAULT now();
-- +goose StatementEnd
