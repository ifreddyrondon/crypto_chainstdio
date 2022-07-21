package collecting

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type worker struct {
	f                  BlockchainFetcher
	logger             *zap.Logger
	retryAttempts      int
	retrySleepDuration time.Duration
	exponentialBackoff int
}

func (w *worker) work(ctx context.Context, wg *sync.WaitGroup, jobs <-chan []pkg.Identifier, results chan<- []pkg.Ledger, errCh chan<- error) {
	defer func() {
		w.logger.Info("done")
		wg.Done()
	}()
	for {
		select {
		case job, ok := <-jobs:
			t0 := time.Now()
			if !ok {
				w.logger.Debug("empty jobs, closing worker")
				return
			}
			var err error
			var ledgers []pkg.Ledger
			sleep := w.retrySleepDuration
			for i := 0; i < w.retryAttempts; i++ {
				if i > 0 {
					w.logger.Debug("retrying after error", zap.Error(err))
					time.Sleep(sleep)
					sleep *= 2
				}
				ledgers, err = w.f.Ledgers(ctx, job...)
				if err == nil {
					break
				}
				if err != nil && !errors.Is(err, syscall.ECONNRESET) {
					break
				}
			}
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					errCh <- err
					break
				}
				w.logger.Info("shutting down...")
				return
			}
			w.logger.Debug("fetching completed", zap.Int("amount", len(ledgers)), zap.Duration("duration", time.Since(t0)))
			results <- ledgers
		}
	}
}
