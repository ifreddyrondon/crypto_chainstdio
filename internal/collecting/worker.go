package collecting

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type worker struct {
	f   BlockchainFetcher
	log *zap.Logger
}

func (w *worker) work(ctx context.Context, wg *sync.WaitGroup, jobs <-chan []pkg.Identifier, results chan<- []pkg.Ledger, errCh chan<- error) {
	defer wg.Done()
	for {
		select {
		case j, ok := <-jobs:
			if !ok {
				return
			}
			t0 := time.Now()
			l, err := w.f.Ledgers(ctx, j...)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					errCh <- err
					break
				}
				return
			}
			w.log.Debug("fetching completed", zap.Int("amount", len(j)), zap.Duration("duration", time.Since(t0)))
			results <- l
		case <-ctx.Done():
			w.log.Info("shutting down worker...")
			return
		}
	}
}
