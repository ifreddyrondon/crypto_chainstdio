package storage

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

var (
	ErrNotFoundLedgers        = errors.New("not found ledgers")
	errMissingLedgerTableName = errors.New("missing ledgers table name")
	errMissingTxsTableName    = errors.New("missing transactions table name")

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
		"created_at",
	}
	txsTableInsertCols = []string{
		"blockchain",
		"network",
		"identifier_hash",
		"identifier_index",
		"ledger_hash",
		"ledger_index",
		"from_address",
		"to_address",
		"metadata",
		"created_at",
	}
)

type Ledger struct {
	pool       *pgxpool.Pool
	blockchain pkg.Blockchain
	network    pkg.Network

	upsertTxQry     string
	ledgerTableName string
	txsTableName    string
}

func NewLedger(pool *pgxpool.Pool, b pkg.Blockchain, n pkg.Network, ledgerTableName, txsTableName string) (*Ledger, error) {
	if ledgerTableName == "" {
		return nil, errMissingLedgerTableName
	}
	if txsTableName == "" {
		return nil, errMissingTxsTableName
	}
	return &Ledger{
		pool:            pool,
		blockchain:      b,
		network:         n,
		upsertTxQry:     buildTransactionUpsertQry(txsTableName),
		ledgerTableName: ledgerTableName,
		txsTableName:    txsTableName,
	}, nil
}

func buildTransactionUpsertQry(txsTableName string) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("INSERT INTO %s SELECT ", txsTableName))
	for i, col := range txsTableInsertCols {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col)
	}
	sb.WriteString(" FROM _temp_transactions ON CONFLICT ON CONSTRAINT transactions_identifier_hash_key DO NOTHING;")
	return sb.String()
}

const getLatestLedgerQry = `
SELECT identifier_hash,
       identifier_index,
       previous_ledger_hash,
       previous_ledger_index,
       orphaned,
       timestamp,
       metadata,
       created_at
FROM %s
WHERE blockchain = $1
  AND network = $2
ORDER BY identifier_index DESC
LIMIT 1;
`

func (s Ledger) Latest(ctx context.Context) (pkg.Ledger, error) {
	qry := fmt.Sprintf(getLatestLedgerQry, pq.QuoteIdentifier(s.ledgerTableName))
	row := s.pool.QueryRow(ctx, qry, s.blockchain, s.network)
	ledger := pkg.Ledger{
		Blockchain: s.blockchain,
		Network:    s.network,
	}
	if err := row.Scan(
		&ledger.Identifier.Hash,
		&ledger.Identifier.Index,
		&ledger.PreviousLedger.Hash,
		&ledger.PreviousLedger.Index,
		&ledger.Orphaned,
		&ledger.Timestamp,
		&ledger.Metadata,
		&ledger.CreatedAt,
	); err != nil {
		if errors.As(err, &pgx.ErrNoRows) {
			return ledger, ErrNotFoundLedgers
		}
		return ledger, fmt.Errorf("error getting latest ledger: %w", err)
	}
	return ledger, nil
}

const missingQry = `
SELECT generate_series(0, (SELECT MAX(identifier_index) FROM %s)) AS missing
EXCEPT
SELECT identifier_index FROM %s ORDER BY missing ASC;`

// Missing collects the missing ledgers stored from the first to the latest by index stored.
func (s Ledger) Missing(ctx context.Context) ([]pkg.Identifier, error) {
	qry := fmt.Sprintf(missingQry, pq.QuoteIdentifier(s.ledgerTableName), pq.QuoteIdentifier(s.ledgerTableName))
	rows, err := s.pool.Query(ctx, qry)
	if err != nil {
		return nil, errors.Wrap(err, "error running query to get missing ledgers")
	}
	var ids []pkg.Identifier
	for rows.Next() {
		var id pkg.Identifier
		if err := rows.Scan(&id.Index); err != nil {
			return nil, errors.Wrap(err, "error scanning results from query to get missing ledgers")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

const tempTableQry = `CREATE TEMPORARY TABLE _temp_transactions (LIKE transactions INCLUDING ALL) ON COMMIT DROP`

// Save stores ledgers and transactions.
// It's safe to call this method when there are missing transactions of a ledgers because
// it validates the existence of previous transactions for a given ledger and
// only store the missing ones. It's recommended to use when there are missing ledgers.
func (s Ledger) Save(ctx context.Context, l ...pkg.Ledger) (int, error) {
	if len(l) == 0 {
		return 0, nil
	}
	var err error
	pgTX, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "error creating transaction")
	}
	defer func() {
		if err := pgTX.Rollback(ctx); err != nil {
			log.Print(err)
		}
	}()
	var txs []pkg.Transaction
	ledgerSrcFn := pgx.CopyFromSlice(len(l), func(i int) ([]interface{}, error) {
		txs = append(txs, l[i].Transactions...)
		now := time.Now().UTC()
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
			now,
		}, nil
	})
	if _, err = pgTX.CopyFrom(ctx, pgx.Identifier{s.ledgerTableName}, ledgerTableInsertCols, ledgerSrcFn); err != nil {
		return 0, errors.Wrap(err, "error bulk saving ledgers")
	}
	if len(txs) == 0 {
		if err = pgTX.Commit(ctx); err != nil {
			return 0, errors.Wrap(err, "error committing ledgers")
		}
		return 0, nil
	}
	if _, err = pgTX.Exec(ctx, tempTableQry); err != nil {
		return 0, errors.Wrap(err, "error creating _temp_transactions table")
	}
	cpyTxsFn := pgx.CopyFromSlice(len(txs), func(i int) ([]interface{}, error) {
		now := time.Now().UTC()
		return []interface{}{
			s.blockchain,
			s.network,
			txs[i].Identifier.Hash,
			txs[i].Identifier.Index,
			txs[i].Ledger.Hash,
			txs[i].Ledger.Index,
			txs[i].From.Hash,
			txs[i].To.Hash,
			nil,
			now,
		}, nil
	})
	if _, err = pgTX.CopyFrom(ctx, pgx.Identifier{"_temp_transactions"}, txsTableInsertCols, cpyTxsFn); err != nil {
		return 0, errors.Wrap(err, "error bulk saving transactions in _temp_transactions table")
	}
	if _, err = pgTX.Exec(ctx, s.upsertTxQry); err != nil {
		return 0, errors.Wrap(err, "error upsetting transactions")
	}
	if err = pgTX.Commit(ctx); err != nil {
		return 0, errors.Wrap(err, "error committing transaction")
	}
	return len(txs), nil
}

func (s Ledger) SaveWithoutTransactionsChecks(ctx context.Context, l ...pkg.Ledger) (int, error) {
	if len(l) == 0 {
		return 0, nil
	}
	var err error
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "error creating transaction")
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Print(err)
		}
	}()

	var txs []pkg.Transaction
	ledgerSrcFn := pgx.CopyFromSlice(len(l), func(i int) ([]interface{}, error) {
		txs = append(txs, l[i].Transactions...)
		now := time.Now().UTC()
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
			now,
		}, nil
	})
	if _, err = tx.CopyFrom(ctx, pgx.Identifier{s.ledgerTableName}, ledgerTableInsertCols, ledgerSrcFn); err != nil {
		return 0, errors.Wrap(err, "error bulk saving ledgers")
	}
	if len(txs) == 0 {
		return 0, nil
	}
	cpyTxsFn := pgx.CopyFromSlice(len(txs), func(i int) ([]interface{}, error) {
		now := time.Now().UTC()
		return []interface{}{
			s.blockchain,
			s.network,
			txs[i].Identifier.Hash,
			txs[i].Identifier.Index,
			txs[i].Ledger.Hash,
			txs[i].Ledger.Index,
			txs[i].From.Hash,
			txs[i].To.Hash,
			nil,
			now,
		}, nil
	})
	if _, err = tx.CopyFrom(ctx, pgx.Identifier{s.txsTableName}, txsTableInsertCols, cpyTxsFn); err != nil {
		return 0, errors.Wrapf(err, "error bulk saving transactions in %s table", s.txsTableName)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, errors.Wrap(err, "error committing transaction")
	}
	return len(txs), nil
}
