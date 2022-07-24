package storing

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type Async struct {
	cancelFn func()
	doneCh   chan bool
	s        Storage
	logger   *zap.Logger
}

func NewAsync(logger *zap.Logger, s Storage) *Async {
	logger = logger.Named("async_storer")

	return &Async{
		s:      s,
		logger: logger,
	}
}

func (a *Async) Save() (chan<- []pkg.Ledger, <-chan error) {
	jobCh := make(chan []pkg.Ledger, 1)
	errCh := make(chan error)

	ctx, cancel := context.WithCancel(context.Background())
	a.cancelFn = cancel
	a.doneCh = make(chan bool)

	go func() {
		defer func() {
			close(a.doneCh)
			a.logger.Info("done")
		}()
		for {
			select {
			case job := <-jobCh:
				t0 := time.Now()
				txsAmount, err := a.s.Save(ctx, false, job)
				if err != nil {
					errCh <- errors.Wrap(err, "error saving ledgers")
					continue
				}
				a.logger.Debug("finish",
					zap.Int("tx_amount", txsAmount),
					zap.Duration("duration", time.Since(t0)),
				)
			case <-ctx.Done():
				a.logger.Info("shutting down...")
				return
			}
		}
	}()
	return jobCh, errCh
}

func (a Async) Close() {
	a.cancelFn()
	<-a.doneCh
}
