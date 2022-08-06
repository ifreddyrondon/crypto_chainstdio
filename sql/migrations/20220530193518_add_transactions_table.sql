-- +goose Up
-- +goose StatementBegin
CREATE TABLE transactions
(
    id               uuid                        NOT NULL DEFAULT uuid_generate_v4(),
    blockchain       INTEGER                     NOT NULL,
    network          INTEGER                     NOT NULL,
    identifier_hash  VARCHAR(255)                NOT NULL UNIQUE,
    identifier_index NUMERIC                     NOT NULL,
    ledger_hash      VARCHAR(255)                NOT NULL,
    ledger_index     NUMERIC                     NOT NULL,
    from_address     VARCHAR(255)                NOT NULL,
    to_address       VARCHAR(255)                NOT NULL,
    fee_amount       NUMERIC,
    fee_currency     INTEGER,
    status           INTEGER,
    metadata         JSONB,
    timestamp        TIMESTAMP,
    created_at       TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
    UNIQUE (identifier_hash, ledger_hash)
);

CREATE INDEX idx_transaction_ledger_hsh on transactions (ledger_hash);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE transactions;
-- +goose StatementEnd
