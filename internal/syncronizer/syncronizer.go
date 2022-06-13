package syncronizer

import (
	"context"

	"github.com/pkg/errors"

	"github.com/ifreddyrondon/crypto_chainstdio/internal/storage"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	LatestFetcherOn interface {
		Latest(ctx context.Context) (pkg.Ledger, error)
	}
	LatestFetcherOff interface {
		Latest(ctx context.Context) (pkg.Ledger, error)
	}
)

type Synchronizer struct {
	fetcherON  LatestFetcherOn
	fetcherOFF LatestFetcherOff
	workerPool []Worker
}

func New(on LatestFetcherOn, off LatestFetcherOff, pool []Worker) Synchronizer {
	return Synchronizer{
		fetcherON:  on,
		fetcherOFF: off,
		workerPool: pool,
	}
}

func (s Synchronizer) Run(ctx context.Context) error {
	// check last ledger saved
	latestLedgerOFF, err := s.fetcherOFF.Latest(ctx)
	if err != nil {
		if err != storage.ErrNotFoundLedgers {
			return errors.Wrap(err, "error getting latest stored ledger")
		}
		// first time syncing network
		if err := s.workerPool[0].Run(ctx, 0, 0); err != nil {
			return errors.Wrap(err, "error handling genesis ledger")
		}
		latestLedgerOFF, err = s.fetcherOFF.Latest(ctx)
		if err != nil {
			return errors.Wrap(err, "error getting latest stored ledger")
		}
	}
	// latest ledger on chain
	latestLedgerON, err := s.fetcherON.Latest(ctx)
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
		if err := s.workerPool[0].Run(ctx, from, to); err != nil {
			return errors.Wrap(err, "error syncing ledgers")
		}
		from = to + 1
	}
	return nil
}
