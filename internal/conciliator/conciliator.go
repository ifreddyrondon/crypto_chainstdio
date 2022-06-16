package syncronizer

import (
	"context"
	"log"

	"github.com/pkg/errors"

	"github.com/ifreddyrondon/crypto_chainstdio/internal/storage"
)

type Conciliator struct {
	fetcherON  LatestFetcherOn
	fetcherOFF LatestFetcherOff
	workerPool []Worker
}

func (c Conciliator) Conciliate(ctx context.Context) error {
	log.Printf("starting conciliation...")
	// check last ledger saved
	latestLedgerOFF, err := c.fetcherOFF.Latest(ctx)
	if err != nil {
		if err != storage.ErrNotFoundLedgers {
			return errors.Wrap(err, "error getting latest stored ledger")
		}
		// first time syncing network
		log.Printf("first time syncing this network")
		if err := c.workerPool[0].Run(ctx, 0, 0); err != nil {
			return errors.Wrap(err, "error handling genesis ledger")
		}
		latestLedgerOFF, err = c.fetcherOFF.Latest(ctx)
		if err != nil {
			return errors.Wrap(err, "error getting latest stored ledger")
		}
	}
	// latest ledger on chain
	latestLedgerON, err := c.fetcherON.Latest(ctx)
	if err != nil {
		return errors.Wrap(err, "error getting latest network ledger")
	}
	from := latestLedgerOFF.Identifier.Index + 1
	latest := latestLedgerON.Identifier.Index

	for from < latest {
		to := from + 2000 // tiempo promedio de guardado 350 a 400ms
		if to > latest {
			to = latest
		}
		if err := c.workerPool[0].Run(ctx, from, to); err != nil {
			return errors.Wrap(err, "error syncing ledgers")
		}
		from = to + 1
	}
	return nil
}
