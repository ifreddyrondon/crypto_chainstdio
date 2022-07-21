package collecting

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type Async struct {
	cancelFn func()
	doneCh   chan bool
	workers  []worker
	logger   *zap.Logger
}

func NewAsync(logger *zap.Logger, fetchers []BlockchainFetcher) *Async {
	logger = logger.Named("async_collector")
	workers := make([]worker, 0, len(fetchers))
	for i, f := range fetchers {
		workers = append(workers, worker{
			f:                  f,
			retryAttempts:      3,
			retrySleepDuration: time.Millisecond * 100,
			exponentialBackoff: 2,
			logger:             logger.Named(fmt.Sprintf("worker-%v", i+1)),
		})
	}
	return &Async{
		workers: workers,
		logger:  logger,
	}
}

func (a *Async) Collect() (chan<- []pkg.Identifier, <-chan []pkg.Ledger, <-chan error) {
	jobCh := make(chan []pkg.Identifier, len(a.workers))
	resultCh := make(chan []pkg.Ledger, len(a.workers))
	errCh := make(chan error)

	ctx, cancel := context.WithCancel(context.Background())
	a.cancelFn = cancel
	a.doneCh = make(chan bool)

	var wg sync.WaitGroup
	wg.Add(len(a.workers))
	// init workers
	for _, fw := range a.workers {
		fw := fw
		go fw.work(ctx, &wg, jobCh, resultCh, errCh)
	}
	// listing to workers to be done
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(resultCh)
		a.doneCh <- true
	}(&wg)

	go func() {
		select {
		case <-a.doneCh:
			close(a.doneCh)
		case <-ctx.Done():
			a.logger.Info("shutting down...")
			<-a.doneCh
			a.logger.Info("done")
			return
		}
	}()
	return jobCh, resultCh, errCh
}

func (a Async) Close() {
	a.cancelFn()
	<-a.doneCh
}
