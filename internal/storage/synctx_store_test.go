package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/stretchr/testify/assert"
)

func setup(ctx context.Context) (*pgxpool.Pool, error) {
	dbName, ok := os.LookupEnv("POSTGRES_DB")
	if !ok {
		return nil, errors.New("missing DB name")
	}
	dbUser, ok := os.LookupEnv("POSTGRES_USER")
	if !ok {
		return nil, errors.New("missing DB user")
	}
	dbPassword, ok := os.LookupEnv("POSTGRES_PASSWORD")
	if !ok {
		return nil, errors.New("missing DB password")
	}
	dbHost, ok := os.LookupEnv("POSTGRES_HOST")
	if !ok {
		return nil, errors.New("missing DB host")
	}
	dbURI := fmt.Sprintf("postgres://%s:%s@%s/%s", dbUser, dbPassword, dbHost, dbName)
	config, err := pgxpool.ParseConfig(dbURI)
	if err != nil {
		return nil, fmt.Errorf("parsing DB URI %s. err: %w", dbURI, err)
	}
	config.MaxConns = 5
	dbpool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("error connecting to postgres pool: %w", err)
	}
	return dbpool, nil
}

const (
	qryT1 = `SELECT generate_series(0, (SELECT MAX(identifier_index) FROM ledgers)) AS genid
EXCEPT
SELECT identifier_index
FROM ledgers
ORDER BY genid ASC;`

	qryT2 = `SELECT s.i AS missing_cmd
FROM (SELECT generate_series(0, (SELECT MAX(identifier_index) FROM ledgers))) s(i)
WHERE NOT EXISTS(SELECT 1 FROM ledgers WHERE identifier_index = s.i);`

	qryT3 = `SELECT s.i AS missing_cmd
FROM (SELECT generate_series(0, (SELECT MAX(identifier_index) FROM ledgers))) s(i)
         LEFT OUTER JOIN ledgers ON (ledgers.identifier_index = s.i)
WHERE ledgers.identifier_index IS NULL;`
)

func BenchmarkQueryT1Performance(b *testing.B) {
	ctx := context.Background()
	dbpool, err := setup(ctx)
	assert.Nil(b, err)

	for i := 0; i < b.N; i++ {
		row := dbpool.QueryRow(ctx, qryT1)
		var amount int64
		assert.Nil(b, row.Scan(&amount))
	}
}

func BenchmarkQueryT2Performance(b *testing.B) {
	ctx := context.Background()
	dbpool, err := setup(ctx)
	assert.Nil(b, err)

	for i := 0; i < b.N; i++ {
		row := dbpool.QueryRow(ctx, qryT2)
		var amount int64
		assert.Nil(b, row.Scan(&amount))
	}
}

func BenchmarkQueryT3Performance(b *testing.B) {
	ctx := context.Background()
	dbpool, err := setup(ctx)
	assert.Nil(b, err)

	for i := 0; i < b.N; i++ {
		row := dbpool.QueryRow(ctx, qryT3)
		var amount int64
		assert.Nil(b, row.Scan(&amount))
	}
}
