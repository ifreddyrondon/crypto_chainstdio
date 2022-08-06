-- +goose Up
-- +goose StatementBegin
CREATE TABLE ethereum_mainnet_sync
(
    ledger_index NUMERIC NOT NULL,
    address VARCHAR(255),
    UNIQUE (ledger_index, address)
);

CREATE INDEX idx_ethereum_mainnet_sync_ledger_index on ethereum_mainnet_sync (ledger_index);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE ethereum_mainnet_sync;
-- +goose StatementEnd
