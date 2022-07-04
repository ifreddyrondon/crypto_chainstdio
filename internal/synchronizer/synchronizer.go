package synchronizer

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	BlockchainFetcher interface {
		Latest(ctx context.Context) (pkg.Ledger, error)
	}

	Conciliator interface {
		Conciliate(ctx context.Context) error
	}
)

type Synchronizer struct {
	conciliator Conciliator
	log         *zap.Logger
}

func New(conciliator Conciliator, log *zap.Logger) Synchronizer {
	return Synchronizer{
		conciliator: conciliator,
		log:         log.Named("synchronizer"),
	}
}

func (s Synchronizer) Run(ctx context.Context) error {
	errCh := make(chan error)
	doneCh := make(chan bool)
	go s.sync(ctx, errCh, doneCh)
	for {
		select {
		case <-ctx.Done():
			s.log.Info("shutting down synchronizer...")
			<-doneCh
			s.log.Info("stopped synchronizer")
			return nil
		case err := <-errCh:
			return err
		}
	}
}

func (s Synchronizer) sync(ctx context.Context, errCh chan error, doneCh chan bool) {
	defer func() {
		doneCh <- true
	}()
	s.log.Info("starting syncing...")

	// conciliation old ledgers
	conciliatorCh := make(chan bool)
	errConciliatorCh := make(chan error)
	go func() {
		s.log.Info("starting conciliation process")
		if err := s.conciliator.Conciliate(ctx); err != nil {
			errConciliatorCh <- err
		}
		conciliatorCh <- true
	}()
	// waiting for reconciling before start pooling
	select {
	case <-conciliatorCh:
		s.log.Info("conciliation process done")
	case err := <-errConciliatorCh:
		errCh <- err
		return
	case <-ctx.Done():
		s.log.Info("shutting down conciliation process...")
		<-conciliatorCh
		s.log.Info("stopped conciliation process...")
		return
	}

	// pooling
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	s.log.Info("starting ledger pooling...")
	for {
		select {
		case <-ticker.C:
			s.log.Info("pooling...")
		case <-ctx.Done():
			s.log.Info("shutting down ledger pooling...")
			s.log.Info("stopped ledger pooling...")
			return
		}
	}
}
