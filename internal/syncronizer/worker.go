package syncronizer

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	fetcher interface {
		Ledgers(ctx context.Context, ids ...pkg.Identifier) ([]pkg.Ledger, error)
	}
	storer interface {
		Save(ctx context.Context, l ...pkg.Ledger) (int, error)
	}
)

type Worker struct {
	fetcher   fetcher
	storer    storer
	batchSize uint64
}

func NewWorker(f fetcher, s storer) Worker {
	return Worker{
		fetcher:   f,
		storer:    s,
		batchSize: 500,
	}
}

func (w Worker) Run(ctx context.Context, from, to uint64) error {
	chunks := split(from, to, w.batchSize)
	tTotal := time.Now()
	ledgers := make([]pkg.Ledger, 0, (to-from)+1)
	for _, chunk := range chunks {
		t0 := time.Now()
		l, err := w.fetcher.Ledgers(ctx, chunk...)
		if err != nil {
			return errors.Wrap(err, "error polling ledgers")
		}
		log.Printf("fetching %v ledgers on time %s\n", len(chunk), time.Since(t0))
		ledgers = append(ledgers, l...)
	}
	t1 := time.Now()
	txnNumber, err := w.storer.Save(ctx, ledgers...)
	if err != nil {
		return errors.Wrap(err, "error saving ledgers")
	}
	log.Printf("store %s\n", time.Since(t1))
	log.Printf("[worker0] stored total: %v ledgers and %v transactions on %s\n", len(ledgers), txnNumber, time.Since(tTotal))
	return nil
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
