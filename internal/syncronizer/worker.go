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
	txnTotal := 0
	tTotal := time.Now()
	for i, chunk := range chunks {
		tChunk := time.Now()
		ledgers, err := w.fetcher.Ledgers(ctx, chunk...)
		if err != nil {
			return errors.Wrap(err, "error polling ledgers")
		}
		txnNumber, err := w.storer.Save(ctx, ledgers...)
		if err != nil {
			return errors.Wrap(err, "error saving ledgers")
		}
		txnTotal += txnNumber
		log.Printf("[worker0][chunk-%v] stored %v ledgers [%v-%v] and %v transactions in %s\n",
			i, len(chunk), chunk[0].Index, chunk[len(chunk)-1].Index, txnNumber, time.Since(tChunk))
	}
	log.Printf("[worker0] stored total: %v ledgers and %v transactions on %s\n", to-from, txnTotal, time.Since(tTotal))
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
