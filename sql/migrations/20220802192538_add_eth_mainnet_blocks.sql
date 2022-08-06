-- +goose Up
-- +goose StatementBegin
CREATE TABLE ethereum_mainnet_blocks
(
    ledger_index NUMERIC PRIMARY KEY
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE ethereum_mainnet_blocks;
-- +goose StatementEnd
