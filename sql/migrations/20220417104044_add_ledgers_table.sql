-- +goose Up
-- +goose StatementBegin
CREATE TABLE ledgers
(
    id                    uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    identifier_hash       VARCHAR(255)                NOT NULL UNIQUE,
    identifier_index      NUMERIC                     NOT NULL,
    previous_ledger_hash  VARCHAR(255)                NOT NULL,
    previous_ledger_index NUMERIC                     NOT NULL,
    timestamp             timestamp                   NOT NULL,
    created_at            TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
    updated_at            TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
    metadata              jsonb
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
-- +goose StatementEnd
