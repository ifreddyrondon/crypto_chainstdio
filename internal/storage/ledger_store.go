package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

var ErrNotFoundLedgers = errors.New("not found ledgers")

var (
	ledgerTableID         = pgx.Identifier{"ledgers"}
	ledgerTableInsertCols = []string{
		"blockchain",
		"network",
		"identifier_hash",
		"identifier_index",
		"previous_ledger_hash",
		"previous_ledger_index",
		"orphaned",
		"timestamp",
		"metadata",
	}
	transactionsTableID = pgx.Identifier{"transactions"}
	txsTableInsertCols  = []string{
		"blockchain",
		"network",
		"identifier_hash",
		"identifier_index",
		"ledger_hash",
		"ledger_index",
		"from_address",
		"to_address",
		"fee_amount",
		"fee_currency",
		"status",
		"metadata",
		"timestamp",
	}
)

type Ledger struct {
	pool       *pgxpool.Pool
	blockchain pkg.Blockchain
	network    pkg.Network
}

func NewLedger(pool *pgxpool.Pool, b pkg.Blockchain, n pkg.Network) Ledger {
	return Ledger{
		pool:       pool,
		blockchain: b,
		network:    n,
	}
}

const getLatestLedgerQry = `
SELECT id,
       identifier_hash,
       identifier_index,
       previous_ledger_hash,
       previous_ledger_index,
       orphaned,
       timestamp,
       metadata,
       created_at,
       updated_at
FROM ledgers
WHERE blockchain = $1
  AND network = $2
ORDER BY identifier_index DESC
LIMIT 1;
`

func (s Ledger) Latest(ctx context.Context) (pkg.Ledger, error) {
	row := s.pool.QueryRow(ctx, getLatestLedgerQry, s.blockchain, s.network)
	ledger := pkg.Ledger{
		Blockchain: s.blockchain,
		Network:    s.network,
	}
	if err := row.Scan(
		&ledger.ID,
		&ledger.Identifier.Hash,
		&ledger.Identifier.Index,
		&ledger.PreviousLedger.Hash,
		&ledger.PreviousLedger.Index,
		&ledger.Orphaned,
		&ledger.Timestamp,
		&ledger.CreatedAt,
		&ledger.LastUpdatedAt,
		&ledger.Metadata,
	); err != nil {
		if errors.As(err, &pgx.ErrNoRows) {
			return ledger, ErrNotFoundLedgers
		}
		return ledger, fmt.Errorf("error getting latest ledger: %w", err)
	}
	return ledger, nil
}

func (s Ledger) Save(ctx context.Context, l ...pkg.Ledger) (int, error) {
	if len(l) == 0 {
		return 0, nil
	}
	var txs []pkg.Transaction
	ledgerSrcFn := pgx.CopyFromSlice(len(l), func(i int) ([]interface{}, error) {
		txs = append(txs, l[i].Transactions...)
		return []interface{}{
			s.blockchain,
			s.network,
			l[i].Identifier.Hash,
			l[i].Identifier.Index,
			l[i].PreviousLedger.Hash,
			l[i].PreviousLedger.Index,
			l[i].Orphaned,
			l[i].Timestamp,
			l[i].Metadata,
		}, nil
	})
	if _, err := s.pool.CopyFrom(ctx, ledgerTableID, ledgerTableInsertCols, ledgerSrcFn); err != nil {
		return 0, errors.Wrap(err, "error bulk saving ledgers")
	}

	if len(txs) == 0 {
		return 0, nil
	}
	cpSrcFn := pgx.CopyFromSlice(len(txs), func(i int) ([]interface{}, error) {
		return []interface{}{
			s.blockchain,
			s.network,
			txs[i].Identifier.Hash,
			txs[i].Identifier.Index,
			txs[i].Ledger.Hash,
			txs[i].Ledger.Index,
			txs[i].From.Hash,
			txs[i].To.Hash,
			nil, nil,
			nil,
			nil,
			nil,
		}, nil
	})
	if _, err := s.pool.CopyFrom(ctx, transactionsTableID, txsTableInsertCols, cpSrcFn); err != nil {
		return 0, errors.Wrap(err, "error bulk saving transactions")
	}
	return len(txs), nil
}
