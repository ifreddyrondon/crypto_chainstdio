package conciliating

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	LocalFetcher interface {
		Missing(ctx context.Context) ([]pkg.Identifier, error)
	}
	Collector interface {
		Collect(ctx context.Context, ids ...pkg.Identifier) ([]pkg.Ledger, error)
	}
	Storer interface {
		Save(ctx context.Context, l []pkg.Ledger) error
	}
)

type Conciliator struct {
	localFetcher LocalFetcher
	collector    Collector
	storer       Storer
	log          *zap.Logger
}

func New(localFetcher LocalFetcher, collector Collector, storer Storer, log *zap.Logger) *Conciliator {
	return &Conciliator{
		localFetcher: localFetcher,
		collector:    collector,
		storer:       storer,
		log:          log.Named("conciliator"),
	}
}

func (c Conciliator) Conciliate(ctx context.Context) error {
	c.log.Info("starting...")
	defer func() {
		c.log.Info("done")
	}()
	doneCh := make(chan bool)
	errCh := make(chan error)
	go func() {
		if err := c.conciliate(ctx); err != nil {
			errCh <- err
			return
		}
		doneCh <- true
	}()
	select {
	case <-doneCh:
		return nil
	case err := <-errCh:
		return errors.Wrap(err, "error conciliating")
	case <-ctx.Done():
		c.log.Info("shutting down...")
		<-doneCh
		return nil
	}
}

func (c Conciliator) conciliate(ctx context.Context) error {
	c.log.Info("checking for missing local ledgers")
	missing, err := c.localFetcher.Missing(ctx)
	if err != nil {
		return errors.Wrap(err, "error getting missing local ledgers")
	}
	if len(missing) == 0 {
		return nil
	}
	c.log.Info("there are missing ledgers")
	ledgers, err := c.collector.Collect(ctx, missing...)
	if err != nil {
		return errors.Wrap(err, "error collecting missing ledgers")
	}
	if len(ledgers) == 0 {
		return errors.New("missing ledgers not found")
	}
	if err := c.storer.Save(ctx, ledgers); err != nil {
		return errors.Wrap(err, "error saving ledgers")
	}
	return nil
}
