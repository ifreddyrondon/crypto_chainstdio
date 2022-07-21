package collecting

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	BlockchainFetcher interface {
		Ledgers(ctx context.Context, ids ...pkg.Identifier) ([]pkg.Ledger, error)
	}
)

type Sync struct {
	workers   []worker
	batchSize uint64
	log       *zap.Logger
}

func NewSync(logger *zap.Logger, fetchers []BlockchainFetcher) *Sync {
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
	return &Sync{
		workers:   workers,
		batchSize: 500,
		log:       logger.Named("collector"),
	}
}

func (w Sync) Collect(ctx context.Context, ids ...pkg.Identifier) ([]pkg.Ledger, error) {
	var result []pkg.Ledger
	if len(ids) == 0 {
		return result, nil
	}
	t0 := time.Now()
	chunks := splitSlice(ids, w.batchSize)
	w.log.Debug("collecting",
		zap.Int("total", len(ids)),
		zap.Int("chunks", len(chunks)),
		zap.Uint64("batch_size", w.batchSize))

	jobs := make(chan []pkg.Identifier, len(chunks))
	results := make(chan []pkg.Ledger, len(chunks))
	errCh := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(w.workers))
	for _, fw := range w.workers {
		fw := fw
		go fw.work(ctx, &wg, jobs, results, errCh)
	}
	doneCh := make(chan bool)
	go func(wg *sync.WaitGroup) {
		for _, chunk := range chunks {
			jobs <- chunk
		}
		close(jobs)
		wg.Wait()
		close(results)
		doneCh <- true
	}(&wg)
	select {
	case <-doneCh:
		close(doneCh)
	case err := <-errCh:
		return result, errors.Wrap(err, "error fetching ledgers")
	case <-ctx.Done():
		w.log.Info("shutting down...")
		<-doneCh
		w.log.Info("stopped")
		return result, nil
	}
	result = make([]pkg.Ledger, 0, len(ids))
	for r := range results {
		result = append(result, r...)
	}
	w.log.Debug("finish", zap.Duration("duration", time.Since(t0)))
	return result, nil
}

func splitSlice(in []pkg.Identifier, size uint64) [][]pkg.Identifier {
	chunkSize := int(size)
	if len(in) <= chunkSize {
		return [][]pkg.Identifier{in}
	}
	chunks := make([][]pkg.Identifier, 0, (len(in)/chunkSize)+1)
	for i := 0; i < len(in); i += chunkSize {
		end := i + chunkSize
		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(in) {
			end = len(in)
		}
		chunks = append(chunks, in[i:end])
	}
	return chunks
}
