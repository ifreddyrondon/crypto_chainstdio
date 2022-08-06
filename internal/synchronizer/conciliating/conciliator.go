package conciliating

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/internal/storage"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	LocalFetcher interface {
		Highest(ctx context.Context) (uint64, error)
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
	var missing []pkg.Identifier
	c.log.Info("checking genesis ledger")
	if _, err := c.localFetcher.Highest(ctx); err != nil {
		if !errors.Is(err, storage.ErrNotFoundLedgers) {
			return errors.Wrap(err, "error getting highest local ledger")
		}
		c.log.Info("missing genesis ledger")
		missing = []pkg.Identifier{{
			Index: 0,
		}}
	}
	if len(missing) == 0 {
		c.log.Info("checking for missing local ledgers")
		var err error
		missing, err = c.localFetcher.Missing(ctx)
		if err != nil {
			return errors.Wrap(err, "error getting missing local ledgers")
		}
	}
	if len(missing) == 0 {
		return nil
	}
	c.log.Sugar().Infof("there are %v missing ledgers", len(missing))
	from := 0
	to := len(missing)
	for from < to {
		toLocal := from + 5000
		if toLocal > to {
			toLocal = to
		}
		ledgers, err := c.collector.Collect(ctx, missing[from:toLocal]...)
		if err != nil {
			return errors.Wrap(err, "error collecting missing ledgers")
		}
		if len(ledgers) == 0 {
			return errors.New("missing ledgers not found")
		}
		if err := c.storer.Save(ctx, ledgers); err != nil {
			return errors.Wrap(err, "error saving ledgers")
		}
		from = toLocal
	}
	return nil
}
