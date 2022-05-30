package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

var ErrNotFoundLedgers = errors.New("not found ledgers")

type Ledger struct {
	pool   *pgxpool.Pool
	params Params
}

type Params struct {
	Blockchain pkg.Blockchain
	Network    pkg.Network
}

func NewLedger(pool *pgxpool.Pool, p Params) Ledger {
	return Ledger{
		pool:   pool,
		params: p,
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
	row := s.pool.QueryRow(ctx, getLatestLedgerQry, s.params.Blockchain, s.params.Network)
	ledger := pkg.Ledger{
		Blockchain: s.params.Blockchain,
		Network:    s.params.Network,
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

const saveLedgerQry = `
INSERT INTO ledgers(blockchain, network,
                    identifier_hash, identifier_index,
                    previous_ledger_hash, previous_ledger_index,
                    orphaned, timestamp, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id, created_at, updated_at;
`

func (s Ledger) Save(ctx context.Context, l pkg.Ledger) (pkg.Ledger, error) {
	row := s.pool.QueryRow(
		ctx, saveLedgerQry,
		s.params.Blockchain,
		s.params.Network,
		l.Identifier.Hash,
		l.Identifier.Index,
		l.PreviousLedger.Hash,
		l.PreviousLedger.Index,
		l.Orphaned,
		l.Timestamp,
		l.Metadata,
	)

	var id string
	var createdAt time.Time
	var lastUpdatedAt time.Time
	if err := row.Scan(&id, &createdAt, &lastUpdatedAt); err != nil {
		return l, fmt.Errorf("error inserting ledger: %w", err)
	}
	l.ID = id
	l.CreatedAt = createdAt
	l.LastUpdatedAt = lastUpdatedAt
	return l, nil
}
