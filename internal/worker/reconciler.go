package worker

import (
	"context"
	"fmt"

	"github.com/ifreddyrondon/crypto_chainstdio/internal/store"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type Reconciler struct {
	fetcher LedgerFetcher
	storer  LedgerStore
}

func NewReconciler(f LedgerFetcher, s LedgerStore) Reconciler {
	return Reconciler{
		fetcher: f,
		storer:  s,
	}
}

func (r Reconciler) Process(ctx context.Context) error {
	storedLedger, err := r.storer.Latest(ctx)
	if err != nil {
		if err != store.ErrNotFoundLedgers {
			return fmt.Errorf("error reconciling: %w", err)
		}
		l, err := r.genesis(ctx)
		if err != nil {
			return fmt.Errorf("error reconciling: %w", err)
		}
		storedLedger, err = r.storer.Save(ctx, l)
		if err != nil {
			return fmt.Errorf("error reconciling: error saving genesis ledger: %w", err)
		}
	}
	fetchedLedger, err := r.fetcher.Latest(ctx)
	total := fetchedLedger.Identifier.Index - storedLedger.Identifier.Index
	fmt.Println(total)
	return nil
}

func (r Reconciler) genesis(ctx context.Context) (pkg.Ledger, error) {
	ledgers, err := r.fetcher.Ledgers(ctx, pkg.Identifier{Index: 0})
	if err != nil || len(ledgers) == 0 {
		return pkg.Ledger{}, fmt.Errorf("error getting genesis ledger: %w", err)
	}
	return ledgers[0], nil
}

func (r Reconciler) fetch(from, to uint64) {

}
