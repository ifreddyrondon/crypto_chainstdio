package syncronizer

import (
	"context"
	"log"
	"time"

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
	errCh := make(chan error)
	doneCh := make(chan bool)
	go s.sync(ctx, errCh, doneCh)
	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down syncronizer...")
			<-doneCh
			return nil
		case err := <-errCh:
			log.Printf("err syncing: %v\n", err)
		}
	}
}

func (s Synchronizer) sync(ctx context.Context, errCh chan error, doneCh chan bool) {
	defer func() {
		doneCh <- true
	}()
	log.Printf("starting syncing...")
	// reconciling old ledgers
	doneReconcilerCh := make(chan bool)
	go s.reconciler(ctx, errCh, doneReconcilerCh)
	// waiting for reconciler before start pooling
	<-doneReconcilerCh
	// pooling
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	log.Printf("starting ledger pooling...")
	for {
		select {
		case <-ticker.C:
			// TODO: keep syncing after all the old ledgers were synced
		case <-ctx.Done():
			log.Println("shutting down ledger pooling...")
			return
		}
	}
}

func (s Synchronizer) reconciler(ctx context.Context, errCh chan error, doneCh chan bool) {
	defer func() {
		doneCh <- true
	}()
	log.Printf("starting reconciling...")

	doneCh2 := make(chan bool)
	go s.reconciler2(ctx, errCh, doneCh2)
	select {
	case <-doneCh2:
		log.Println("network reconciled")
		return
	case <-ctx.Done():
		log.Println("shutting down reconciling...")
		return
	}
}

func (s Synchronizer) reconciler2(ctx context.Context, errCh chan error, doneCh chan bool) {
	defer func() {
		doneCh <- true
	}()
	// check last ledger saved
	latestLedgerOFF, err := s.fetcherOFF.Latest(ctx)
	if err != nil {
		if err != storage.ErrNotFoundLedgers {
			errCh <- errors.Wrap(err, "error getting latest stored ledger")
		}
		// first time syncing network
		log.Printf("first time syncing this network")
		if err := s.workerPool[0].Run(ctx, 0, 0); err != nil {
			errCh <- errors.Wrap(err, "error handling genesis ledger")
		}
		latestLedgerOFF, err = s.fetcherOFF.Latest(ctx)
		if err != nil {
			errCh <- errors.Wrap(err, "error getting latest stored ledger")
		}
	}
	// latest ledger on chain
	latestLedgerON, err := s.fetcherON.Latest(ctx)
	if err != nil {
		errCh <- errors.Wrap(err, "error getting latest network ledger")
	}
	from := latestLedgerOFF.Identifier.Index + 1
	latest := latestLedgerON.Identifier.Index

	for from < latest {
		to := from + 2000 // tiempo promedio de guardado 350 a 400ms
		if to > latest {
			to = latest
		}
		if err := s.workerPool[0].Run(ctx, from, to); err != nil {
			errCh <- errors.Wrap(err, "error syncing ledgers")
		}
		from = to + 1
	}
}
