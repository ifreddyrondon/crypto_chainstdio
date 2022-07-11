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
	"github.com/ifreddyrondon/crypto_chainstdio/internal/conciliator"
	"github.com/ifreddyrondon/crypto_chainstdio/internal/storage"
	"github.com/ifreddyrondon/crypto_chainstdio/internal/synchronizer"
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

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.Wrap(err, "error creating logger")
	}

	ctx := context.Background()
	dbURI := fmt.Sprintf("postgres://%s:%s@%s/%s", cfg.DBUser, cfg.DBPass, cfg.DBHost, cfg.DBName)
	dbpool, err := connPool(ctx, dbURI, cfg.DBMaxConnections)
	if err != nil {
		return errors.Wrap(err, "error creating DB connection pool")
	}

	ledgerStorage, err := storage.NewLedger(
		dbpool,
		pkg.Blockchain_ETHEREUM,
		pkg.Network_ETHEREUM_MAINNET,
		"ledgers",
		"transactions",
	)
	if err != nil {
		return errors.Wrap(err, "error creating ledger Storage")
	}
	c := fetcherClient()
	ethFetcher1 := blockchain.NewEthereum(cfg.EthereumNodesURL, pkg.Network_ETHEREUM_MAINNET, c)
	ethFetcher2 := blockchain.NewEthereum(cfg.EthereumNodesURL, pkg.Network_ETHEREUM_MAINNET, c)
	ethFetcher3 := blockchain.NewEthereum(cfg.EthereumNodesURL, pkg.Network_ETHEREUM_MAINNET, c)
	ethFetcher4 := blockchain.NewEthereum(cfg.EthereumNodesURL, pkg.Network_ETHEREUM_MAINNET, c)
	collector1 := collecting.NewCollector(log, ledgerStorage, []collecting.BlockchainFetcher{
		ethFetcher1,
		ethFetcher2,
		ethFetcher3,
		ethFetcher4,
	})
	ledgerConciliator := conciliator.New(ledgerStorage, ethFetcher1, collector1, log)
	sync := synchronizer.New(ledgerConciliator, log)
	mgr := manager.New(log)
	mgr.AddService(manager.ServiceFactory("synchronizer", sync.Run))
	mgr.AddShutdownHook(func() {
		log.Info("closing connection pool")
		dbpool.Close()
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

func fetcherClient() *http.Client {
	// limit the max idle connections to avoid connection reset by peer
	return &http.Client{Transport: &http.Transport{
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 20,
	}}
}
