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
	LedgerStorage interface {
		Save(ctx context.Context, l ...pkg.Ledger) (int, error)
		SaveWithoutTransactionsChecks(ctx context.Context, l ...pkg.Ledger) (int, error)
	}
)

type Collector struct {
	workers   []worker
	storage   LedgerStorage
	batchSize uint64
	log       *zap.Logger
}

func NewCollector(log *zap.Logger, s LedgerStorage, fetchers []BlockchainFetcher) Collector {
	workers := make([]worker, 0, len(fetchers))
	for i, f := range fetchers {
		workers = append(workers, worker{
			f:                  f,
			retryAttempts:      3,
			retrySleepDuration: time.Millisecond * 100,
			exponentialBackoff: 2,
			log:                log.Named(fmt.Sprintf("worker%v", i+1)),
		})
	}
	return Collector{
		storage:   s,
		workers:   workers,
		batchSize: 500,
		log:       log.Named("collector"),
	}
}

func (w Collector) Collect(ctx context.Context, ids ...pkg.Identifier) error {
	if len(ids) == 0 {
		return nil
	}
	chunks := splitSlice(ids, w.batchSize)
	w.log.Info("starting collecting",
		zap.Int("total", len(ids)),
		zap.Int("chunks", len(chunks)),
		zap.Uint64("batch_size", w.batchSize))
	stats, err := w.collect(ctx, chunks, uint64(len(ids)), true)
	if err != nil {
		return errors.Wrap(err, "error collecting ledgers")
	}
	w.log.Info("finish collecting",
		zap.Int("tx_amount", stats.txAmount),
		zap.Duration("fetching_duration", stats.fetchingDuration),
		zap.Duration("storing_duration", stats.storingDuration),
		zap.Duration("total_duration", stats.totalDuration),
	)
	return nil
}

// CollectByInterval ledgers will fetch the ledgers [from, to) and stored
func (w Collector) CollectByInterval(ctx context.Context, from, to uint64) error {
	total := to - from
	if total == 0 {
		return nil
	}
	chunks := splitInterval(from, to, w.batchSize)
	w.log.Info("starting collecting",
		zap.Uint64("from", chunks[0][0].Index),
		zap.Uint64("to", chunks[len(chunks)-1][len(chunks[len(chunks)-1])-1].Index),
		zap.Uint64("total", total),
		zap.Int("chunks", len(chunks)),
		zap.Uint64("batch_size", w.batchSize))
	stats, err := w.collect(ctx, chunks, total, false)
	if err != nil {
		return errors.Wrap(err, "error collecting ledgers by interval")
	}
	w.log.Info("finish collecting",
		zap.Int("tx_amount", stats.txAmount),
		zap.Duration("fetching_duration", stats.fetchingDuration),
		zap.Duration("storing_duration", stats.storingDuration),
		zap.Duration("total_duration", stats.totalDuration),
	)
	return nil
}

type stats struct {
	txAmount         int
	fetchingDuration time.Duration
	storingDuration  time.Duration
	totalDuration    time.Duration
}

func (w Collector) collect(ctx context.Context, chunks [][]pkg.Identifier, total uint64, storeSave bool) (stats, error) {
	var stats stats
	totalTime := time.Now()
	fetchingTime := time.Now()
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
		return stats, errors.Wrap(err, "error fetching ledgers")
	case <-ctx.Done():
		w.log.Info("shutting down collector...")
		<-doneCh
		w.log.Info("stopped collector")
		return stats, nil
	}
	stats.fetchingDuration = time.Since(fetchingTime)
	ledgers := make([]pkg.Ledger, 0, total)
	for r := range results {
		ledgers = append(ledgers, r...)
	}
	storingTime := time.Now()
	var txsAmount int
	var err error
	if storeSave {
		txsAmount, err = w.storage.Save(ctx, ledgers...)
		if err != nil {
			return stats, errors.Wrap(err, "error saving ledgers")
		}
	} else {
		txsAmount, err = w.storage.SaveWithoutTransactionsChecks(ctx, ledgers...)
		if err != nil {
			return stats, errors.Wrap(err, "error saving ledgers")
		}
	}
	stats.txAmount = txsAmount
	stats.storingDuration = time.Since(storingTime)
	stats.totalDuration = time.Since(totalTime)
	return stats, nil
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

func splitInterval(from, to, size uint64) [][]pkg.Identifier {
	chunks := make([][]pkg.Identifier, 0, ((to-from)/size)+1)
	for (to - from) >= size {
		chunk := make([]pkg.Identifier, 0, size)
		for i := from; i < from+size; i++ {
			chunk = append(chunk, pkg.Identifier{Index: i})
		}
		chunks = append(chunks, chunk)
		from = from + size
	}
	if (to - from) > 0 {
		chunk := make([]pkg.Identifier, 0, to-from)
		for i := from; i < to; i++ {
			chunk = append(chunk, pkg.Identifier{Index: i})
		}
		chunks = append(chunks, chunk)
	}
	return chunks
}
