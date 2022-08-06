package storage

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

var (
	ErrNotFoundLedgers = errors.New("not found ledgers")

	syncInsertCols = []string{
		"ledger_index",
		"address",
	}
)

type SyncTX struct {
	pool             *pgxpool.Pool
	upsertTxQry      string
	syncTableName    string
	tmpSyncTableName string
}

func NewSyncTX(pool *pgxpool.Pool, b pkg.Blockchain, n pkg.Network) (*SyncTX, error) {
	syncTableName := strings.ToLower(fmt.Sprintf("%v_%v_sync", b.String(), n.String()))
	tmpSyncTableName := fmt.Sprintf("tmp_%v", syncTableName)
	return &SyncTX{
		pool:             pool,
		syncTableName:    syncTableName,
		tmpSyncTableName: tmpSyncTableName,
		upsertTxQry:      buildTransactionUpsertQry(syncTableName, tmpSyncTableName),
	}, nil
}

func buildTransactionUpsertQry(tableName, tempTableName string) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("INSERT INTO %s SELECT ", tableName))
	for i, col := range syncInsertCols {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col)
	}
	sb.WriteString(fmt.Sprintf(" FROM %s ", tempTableName))
	sb.WriteString("ON CONFLICT (ledger_index, address) DO NOTHING;")
	return sb.String()
}

const getHighestLedgerIndexQry = `
SELECT ledger_index
FROM %s
ORDER BY ledger_index DESC
LIMIT 1;
`

func (s SyncTX) Highest(ctx context.Context) (uint64, error) {
	qry := fmt.Sprintf(getHighestLedgerIndexQry, pq.QuoteIdentifier(s.syncTableName))
	row := s.pool.QueryRow(ctx, qry)
	var highest uint64
	if err := row.Scan(&highest); err != nil {
		if errors.As(err, &pgx.ErrNoRows) {
			return highest, ErrNotFoundLedgers
		}
		return highest, fmt.Errorf("error getting highest ledger index: %w", err)
	}
	return highest, nil
}

const missingQry = `
SELECT i
FROM generate_series(0, (SELECT ledger_index
                         FROM %s
                         ORDER BY ledger_index DESC
                         LIMIT 1)
         ) as t(i)
WHERE NOT exists(
        SELECT 1
        FROM %s
        WHERE ledger_index = t.i);`

// Missing collects the missing ledgers indexes from 0 to highest.
func (s SyncTX) Missing(ctx context.Context) ([]pkg.Identifier, error) {
	qry := fmt.Sprintf(missingQry, pq.QuoteIdentifier(s.syncTableName), pq.QuoteIdentifier(s.syncTableName))
	rows, err := s.pool.Query(ctx, qry)
	if err != nil {
		return nil, errors.Wrap(err, "error running query to get missing ledger indexes")
	}
	var ids []pkg.Identifier
	for rows.Next() {
		var idx uint64
		if err := rows.Scan(&idx); err != nil {
			return nil, errors.Wrap(err, "error scanning results from query to get missing ledger indexes")
		}
		ids = append(ids, pkg.Identifier{Index: idx})
	}
	return ids, nil
}

const tempTableQry = "CREATE TEMPORARY TABLE %s (LIKE %s INCLUDING ALL) ON COMMIT DROP"

func (s SyncTX) Save(ctx context.Context, checkTx bool, l []pkg.Ledger) (int, error) {
	if len(l) == 0 {
		return 0, nil
	}
	var err error
	pgTX, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "error creating transaction")
	}
	defer func() {
		if err := pgTX.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			log.Print(err)
		}
	}()
	var txs []pkg.SyncTX
	for i := 0; i < len(l); i++ {
		ledgerID := pkg.Identifier{Index: l[i].Identifier.Index}
		if len(l[i].Transactions) == 0 {
			txs = append(txs, pkg.SyncTX{Ledger: ledgerID})
			continue
		}
		addresses := make(map[string]bool)
		for j := 0; j < len(l[i].Transactions); j++ {
			if _, ok := addresses[l[i].Transactions[j].From]; !ok {
				addresses[l[i].Transactions[j].From] = true
			}
			if _, ok := addresses[l[i].Transactions[j].To]; !ok {
				addresses[l[i].Transactions[j].To] = true
			}
		}
		for k := range addresses {
			txs = append(txs, pkg.SyncTX{
				Ledger:  ledgerID,
				Address: k,
			})
		}
	}

	cpyTxsFn := pgx.CopyFromSlice(len(txs), func(i int) ([]interface{}, error) {
		return []interface{}{
			txs[i].Ledger.Index,
			txs[i].Address,
		}, nil
	})

	if checkTx {
		if _, err = pgTX.Exec(ctx, fmt.Sprintf(tempTableQry, s.tmpSyncTableName, s.syncTableName)); err != nil {
			return 0, errors.Wrapf(err, "error creating %s table", s.tmpSyncTableName)
		}
		if _, err = pgTX.CopyFrom(ctx, pgx.Identifier{s.tmpSyncTableName}, syncInsertCols, cpyTxsFn); err != nil {
			return 0, errors.Wrapf(err, "error bulk saving SyncTX in %s table", s.tmpSyncTableName)
		}
		if _, err = pgTX.Exec(ctx, s.upsertTxQry); err != nil {
			return 0, errors.Wrap(err, "error upsetting SyncTX")
		}
	} else {
		if _, err = pgTX.CopyFrom(ctx, pgx.Identifier{s.syncTableName}, syncInsertCols, cpyTxsFn); err != nil {
			return 0, errors.Wrapf(err, "error bulk saving SyncTX in %s table", s.syncTableName)
		}
	}
	if err = pgTX.Commit(ctx); err != nil {
		return 0, errors.Wrap(err, "error committing transaction")
	}
	return len(txs), nil
}
