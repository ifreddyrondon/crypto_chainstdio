package conciliator

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/internal/storage"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	BlockchainFetcher interface {
		Latest(ctx context.Context) (pkg.Ledger, error)
	}
	LedgerStorage interface {
		Missing(ctx context.Context) ([]pkg.Identifier, error)
		Latest(ctx context.Context) (pkg.Ledger, error)
	}
	LedgerCollector interface {
		Collect(ctx context.Context, ids ...pkg.Identifier) error
		CollectByInterval(ctx context.Context, from, to uint64) error
	}
)

type Conciliator struct {
	storage   LedgerStorage
	fetcher   BlockchainFetcher
	collector LedgerCollector
	log       *zap.Logger
}

func New(storage LedgerStorage, fetcher BlockchainFetcher, collector LedgerCollector, log *zap.Logger) Conciliator {
	return Conciliator{
		storage:   storage,
		fetcher:   fetcher,
		collector: collector,
		log:       log.Named("conciliator"),
	}
}

func (c Conciliator) Conciliate(ctx context.Context) error {
	c.log.Info("starting conciliation...")
	doneCh := make(chan bool)
	errCh := make(chan error)
	go func() {
		if err := c.conciliate(ctx); err != nil {
			errCh <- err
		}
		doneCh <- true
	}()
	select {
	case <-doneCh:
		c.log.Info("network conciliated")
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		c.log.Info("shutting down conciliation...")
		<-doneCh
		c.log.Info("stopped conciliation...")
		return nil
	}
}

func (c Conciliator) conciliate(ctx context.Context) error {
	// check last ledger stored
	stored, err := c.storage.Latest(ctx)
	if err != nil {
		if err != storage.ErrNotFoundLedgers {
			return errors.Wrap(err, "error getting latest stored ledger")
		}
		// collect genesis block
		c.log.Info("first time syncing this network")
		c.log.Info("collecting genesis block")
		if err := c.collector.Collect(ctx, pkg.Identifier{Index: 0}); err != nil {
			return errors.Wrap(err, "error conciliating genesis block")
		}
		stored, err = c.storage.Latest(ctx)
		if err != nil {
			return errors.Wrap(err, "error getting latest stored ledger")
		}
	}
	// check the missing stored ledgers
	c.log.Info("checking for missing stored ledgers")
	missing, err := c.storage.Missing(ctx)
	if len(missing) > 0 {
		c.log.Info("there are missing ledgers")
		if err := c.collector.Collect(ctx, missing...); err != nil {
			return errors.Wrap(err, "error conciliating missing ledgers")
		}
	}

	// latest ledger on chain
	c.log.Info("checking for latest ledger on chain")
	latest, err := c.fetcher.Latest(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return errors.Wrap(err, "error getting latest on chain ledger")
	}
	from := stored.Identifier.Index + 1
	toOnChain := latest.Identifier.Index
	if from == toOnChain {
		return nil
	}
	for from < toOnChain && ctx.Err() == nil {
		to := from + 5000
		if to > toOnChain {
			to = toOnChain
		}
		if err := c.collector.CollectByInterval(ctx, from, to); err != nil {
			return errors.Wrap(err, "error conciliating ledgers")
		}
		from = to
	}
	return nil
}
