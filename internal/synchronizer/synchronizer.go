package synchronizer

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	Conciliator interface {
		Conciliate(ctx context.Context) error
	}
	Updater interface {
		Update(ctx context.Context) error
	}
)

type Synchronizer struct {
	conciliator Conciliator
	updater     Updater
	log         *zap.Logger
}

func New(conciliator Conciliator, updater Updater, log *zap.Logger) *Synchronizer {
	return &Synchronizer{
		conciliator: conciliator,
		updater:     updater,
		log:         log.Named("synchronizer"),
	}
}

func (s Synchronizer) Run(ctx context.Context) error {
	errCh := make(chan error)
	doneCh := make(chan bool)
	ctx2, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		s.log.Info("done")
	}()
	go s.sync(ctx2, errCh, doneCh)
	for {
		select {
		case <-doneCh: // not a real case given a blockchain always should be alive
			return nil
		case <-ctx.Done():
			s.log.Info("shutting down...")
			<-doneCh
			return nil
		case err := <-errCh:
			cancel()
			<-doneCh
			return errors.Wrap(err, "error synchronizing")
		}
	}
}

func (s Synchronizer) sync(ctx context.Context, errCh chan error, doneCh chan bool) {
	defer func() {
		doneCh <- true
	}()
	s.log.Info("starting...")

	// conciliation old ledgers
	conciliationDoneCh := make(chan bool)
	conciliationErrCh := make(chan error)
	go func() {
		defer func() {
			conciliationDoneCh <- true
		}()
		if err := s.conciliator.Conciliate(ctx); err != nil {
			conciliationErrCh <- err
		}
	}()
	// waiting for reconciling before start updater
	select {
	case <-conciliationDoneCh: // it will continue
	case err := <-conciliationErrCh:
		errCh <- err
		return
	case <-ctx.Done():
		<-conciliationDoneCh
		return
	}

	// updater
	updaterDoneCh := make(chan bool)
	updaterErrCh := make(chan error)
	go func() {
		defer func() {
			updaterDoneCh <- true
		}()
		if err := s.updater.Update(ctx); err != nil {
			updaterErrCh <- err
		}
	}()
	select {
	case err := <-updaterErrCh:
		errCh <- err
		return
	case <-ctx.Done():
		<-updaterDoneCh
		return
	}
}
