package main

import (
	"context"
	"fmt"
	"os"

	"github.com/caarlos0/env/v6"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/ifreddyrondon/crypto_chainstdio/internal/storage"
	"github.com/ifreddyrondon/crypto_chainstdio/internal/syncronizer"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg/blockchain"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg/manager"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error running worker: %s\n", err)
		os.Exit(1)
	}
}

type conf struct {
	// DB
	DBName           string `env:"POSTGRES_DB,notEmpty"`
	DBUser           string `env:"POSTGRES_USER,notEmpty"`
	DBPass           string `env:"POSTGRES_PASSWORD,notEmpty"`
	DBHost           string `env:"POSTGRES_HOST,notEmpty"`
	DBMaxConnections int32  `env:"DB_MAX_CONNECTIONS,notEmpty"`

	// Networks
	EthereumNodesURL string `env:"ETHEREUM_NODES_URL,notEmpty"`
}

func run() error {
	var cfg conf
	if err := env.Parse(&cfg); err != nil {
		return fmt.Errorf("error loading env vars: %w", err)
	}

	ctx := context.Background()
	dbURI := fmt.Sprintf("postgres://%s:%s@%s/%s", cfg.DBUser, cfg.DBPass, cfg.DBHost, cfg.DBName)
	dbpool, err := connPool(ctx, dbURI, cfg.DBMaxConnections)
	if err != nil {
		return fmt.Errorf("error creating conn pool: %w", err)
	}
	_ = dbpool

	// ethereum
	ethFetcher1 := blockchain.NewEthereum(cfg.EthereumNodesURL, pkg.Network_ETHEREUM_MAINNET)
	ethFetcher2 := blockchain.NewEthereum(cfg.EthereumNodesURL, pkg.Network_ETHEREUM_MAINNET)
	ledgerStorage := storage.NewLedger(dbpool, pkg.Blockchain_ETHEREUM, pkg.Network_ETHEREUM_MAINNET)
	worker0 := syncronizer.NewWorker(ledgerStorage, []syncronizer.Fetcher{ethFetcher1, ethFetcher2})

	sync := syncronizer.New(&ethFetcher1, ledgerStorage, []syncronizer.Worker{
		worker0,
	})
	mgr := manager.New()
	mgr.AddService(manager.ServiceFactory("worker", sync.Run))

	mgr.WaitForInterrupt()
	return nil
}

func connPool(ctx context.Context, dbURI string, maxConnections int32) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dbURI)
	if err != nil {
		return nil, fmt.Errorf("parsing DB URI %s. err: %w", dbURI, err)
	}
	config.MaxConns = maxConnections
	dbpool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("error connecting to postgres pool: %w", err)
	}
	return dbpool, nil
}
