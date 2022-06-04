package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type Transaction struct {
	pool       *pgxpool.Pool
	blockchain pkg.Blockchain
	network    pkg.Network
}

func NewTransaction(pool *pgxpool.Pool, b pkg.Blockchain, n pkg.Network) Transaction {
	return Transaction{
		pool:       pool,
		blockchain: b,
		network:    n,
	}
}

const saveTxQry = `
INSERT INTO transactions(blockchain, network,
                    identifier_hash, identifier_index,
                    ledger_hash, ledger_index,
                    from_address, to_address,
                    fee_amount, fee_currency,
                    status,
                    metadata,
                    timestamp)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
RETURNING id, created_at, updated_at;
`

func (t Transaction) BatchSave(ctx context.Context, txs ...pkg.Transaction) error {
	if len(txs) == 0 {
		return nil
	}
	batch := pgx.Batch{}
	for _, b := range txs {
		batch.Queue(saveTxQry,
			t.blockchain,
			t.network,
			b.Identifier.Hash,
			b.Identifier.Index,
			b.Ledger.Hash,
			b.Ledger.Index,
			b.From.Hash,
			b.To.Hash,
			nil, nil,
			nil,
			nil,
			nil,
		)
	}
	batchRes := t.pool.SendBatch(ctx, &batch)
	if _, err := batchRes.Exec(); err != nil {
		return fmt.Errorf("error inserting transactions: %w", err)
	}
	return nil
}
