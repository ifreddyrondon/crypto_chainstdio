package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/caarlos0/env/v6"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/internal/collecting"
	"github.com/ifreddyrondon/crypto_chainstdio/internal/storage"
	"github.com/ifreddyrondon/crypto_chainstdio/internal/storing"
	"github.com/ifreddyrondon/crypto_chainstdio/internal/synchronizer"
	"github.com/ifreddyrondon/crypto_chainstdio/internal/synchronizer/conciliating"
	"github.com/ifreddyrondon/crypto_chainstdio/internal/synchronizer/updating"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg/blockchain"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg/manager"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error running syncronizer: %s\n", err)
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
		return errors.Wrap(err, "error loading env vars")
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		return errors.Wrap(err, "error creating logger")
	}

	ctx := context.Background()
	dbURI := fmt.Sprintf("postgres://%s:%s@%s/%s", cfg.DBUser, cfg.DBPass, cfg.DBHost, cfg.DBName)
	dbPool, err := connPool(ctx, dbURI, cfg.DBMaxConnections)
	if err != nil {
		return errors.Wrap(err, "error creating DB connection pool")
	}

	ledgerStorage, err := storage.NewSyncTX(dbPool, pkg.Blockchain_ETHEREUM, pkg.Network_MAINNET)
	if err != nil {
		return errors.Wrap(err, "error creating ledger storage")
	}
	c := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 20,
	}}
	ethClient := blockchain.NewEthereum(cfg.EthereumNodesURL, pkg.Network_MAINNET, c)
	ethClients := []collecting.BlockchainFetcher{
		ethClient,
		blockchain.NewEthereum(cfg.EthereumNodesURL, pkg.Network_MAINNET, c),
		blockchain.NewEthereum(cfg.EthereumNodesURL, pkg.Network_MAINNET, c),
		blockchain.NewEthereum(cfg.EthereumNodesURL, pkg.Network_MAINNET, c),
	}
	syncCollector := collecting.NewSync(logger, ethClients)
	syncStorer := storing.NewSync(logger, ledgerStorage)
	ledgerConciliator := conciliating.New(ledgerStorage, syncCollector, syncStorer, logger)

	asyncCollector := collecting.NewAsync(logger, ethClients)
	asyncStorer := storing.NewAsync(logger, ledgerStorage)
	ledgerUpdater := updating.New(ledgerStorage, ethClient, asyncCollector, asyncStorer, logger)
	sync := synchronizer.New(ledgerConciliator, ledgerUpdater, logger)
	mgr := manager.New(logger)
	mgr.AddService(manager.ServiceFactory("synchronizer", sync.Run))
	mgr.AddShutdownHook(func() {
		logger.Info("closing connection pool")
		dbPool.Close()
	})

	mgr.WaitForInterrupt()
	return nil
}

func connPool(ctx context.Context, dbURI string, maxConnections int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dbURI)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing DB URI %s", dbURI)
	}
	cfg.MaxConns = maxConnections
	dbpool, err := pgxpool.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "error connecting to postgres pool")
	}
	return dbpool, nil
}
