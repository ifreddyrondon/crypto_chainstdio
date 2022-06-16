package syncronizer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	Fetcher interface {
		Ledgers(ctx context.Context, ids ...pkg.Identifier) ([]pkg.Ledger, error)
	}
	Storage interface {
		Save(ctx context.Context, l ...pkg.Ledger) (int, error)
	}
)

type Worker struct {
	fWorkers  []fWorker
	storage   Storage
	batchSize uint64
}

func NewWorker(s Storage, fetchers []Fetcher) Worker {
	fWorkers := make([]fWorker, 0, len(fetchers))
	for i, f := range fetchers {
		fWorkers = append(fWorkers, fWorker{id: i + 1, f: f})
	}
	return Worker{
		storage:   s,
		fWorkers:  fWorkers,
		batchSize: 500,
	}
}

func (w Worker) Run(ctx context.Context, from, to uint64) error {
	log.Printf("syncing ledgers from %v to %v\n", from, to)

	tTotal := time.Now()
	chunks := split(from, to, w.batchSize)
	jobs := make(chan []pkg.Identifier, len(chunks))
	results := make(chan []pkg.Ledger, len(chunks))
	var wg sync.WaitGroup
	for _, fw := range w.fWorkers {
		wg.Add(1)
		fw := fw
		go fw.worker(ctx, &wg, jobs, results)
	}
	for _, chunk := range chunks {
		jobs <- chunk
	}
	close(jobs)
	wg.Wait()
	close(results)

	ledgers := make([]pkg.Ledger, 0, (to-from)+1)
	for r := range results {
		ledgers = append(ledgers, r...)
	}

	t1 := time.Now()
	txnNumber, err := w.storage.Save(ctx, ledgers...)
	if err != nil {
		return errors.Wrap(err, "error saving ledgers")
	}
	log.Printf("stored %s\n", time.Since(t1))
	log.Printf("[worker0] stored total: %v ledgers and %v transactions on %s\n", len(ledgers), txnNumber, time.Since(tTotal))
	return nil
}

type fWorker struct {
	id int
	f  Fetcher
}

func (fw *fWorker) worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan []pkg.Identifier, results chan<- []pkg.Ledger) {
	defer wg.Done()
	for {
		select {
		case j, ok := <-jobs:
			if !ok {
				return
			}
			t0 := time.Now()
			l, _ := fw.f.Ledgers(ctx, j...)
			// if err != nil {
			// 	return errors.Wrap(err, "error polling ledgers")
			// }
			log.Printf("[fetcher_worker_%v] fetched %v ledgers on time %s\n", fw.id, len(j), time.Since(t0))
			results <- l
		case <-ctx.Done():
			log.Printf("cancelled worker. Error detail: %v\n", ctx.Err())
			return
		}
	}
}

func split(from, to, limit uint64) [][]pkg.Identifier {
	chunks := make([][]pkg.Identifier, 0, ((to-from)/limit)+1)
	for (to - from) >= limit {
		chunk := make([]pkg.Identifier, 0, limit)
		for i := from; i < from+limit; i++ {
			chunk = append(chunk, pkg.Identifier{Index: i})
		}
		chunks = append(chunks, chunk)
		from = from + limit
	}
	to++ // close interval
	if (to - from) > 0 {
		chunk := make([]pkg.Identifier, 0, to-from)
		for i := from; i < to; i++ {
			chunk = append(chunk, pkg.Identifier{Index: i})
		}
		chunks = append(chunks, chunk)
	}
	return chunks
}
