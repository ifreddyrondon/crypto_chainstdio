package syncronizer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ifreddyrondon/crypto_chainstdio/internal/store"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	LedgerFetcher interface {
		Ledgers(ctx context.Context, ids ...pkg.Identifier) ([]pkg.Ledger, error)
		Latest(ctx context.Context) (pkg.Ledger, error)
	}
	LedgerStore interface {
		Latest(ctx context.Context) (pkg.Ledger, error)
		Save(ctx context.Context, l ...pkg.Ledger) error
	}
)

type Synchronizer struct {
	pollerInterval  time.Duration
	pollerBatchSize uint64
	fetcher         LedgerFetcher
	ledgerStore     LedgerStore
}

func New(f LedgerFetcher, ledgerStore LedgerStore) Synchronizer {
	return Synchronizer{
		pollerBatchSize: 5,
		pollerInterval:  time.Second * 15,
		fetcher:         f,
		ledgerStore:     ledgerStore,
	}
}

func (s Synchronizer) Run(ctx context.Context) error {
	// check last ledger saved
	latestLedgerStored, err := s.ledgerStore.Latest(ctx)
	if err != nil {
		if err != store.ErrNotFoundLedgers {
			return fmt.Errorf("error getting latest stored ledger: %w", err)
		}
		// first time syncing network
		l, err := s.genesis(ctx)
		if err != nil {
			return err
		}
		if err = s.ledgerStore.Save(ctx, l); err != nil {
			return fmt.Errorf("error saving genesis ledger: %w", err)
		}
		latestLedgerStored = l
	}
	// latest ledger on chain
	latestLedgerChain, err := s.fetcher.Latest(ctx)
	chunks := split(latestLedgerStored.Identifier.Index+1, latestLedgerChain.Identifier.Index, 500)
	for _, chunk := range chunks {
		ledgers, err := s.poller(ctx, chunk)
		if err != nil {
			return err
		}
		if err := s.ledgerStore.Save(ctx, ledgers...); err != nil {
			return err
		}
	}
	return nil
	// ticker := time.NewTicker(s.pollerInterval)
	// for {
	// 	select {
	// 	case <-ctx.Done():
	// 		return nil
	// 	case <-ticker.C:
	// 		if err := s.poll(ctx); err != nil {
	// 			log.Println(err)
	// 		}
	// 	}
	// }
}

func (s Synchronizer) poller(ctx context.Context, ids []pkg.Identifier) ([]pkg.Ledger, error) {
	l, err := s.fetcher.Ledgers(ctx, ids...)
	if err != nil {
		return nil, fmt.Errorf("error polling ledgers. err: %w", err)
	}
	return l, nil
}

func (s Synchronizer) genesis(ctx context.Context) (pkg.Ledger, error) {
	ledgers, err := s.fetcher.Ledgers(ctx, pkg.Identifier{Index: 0})
	if err != nil || len(ledgers) == 0 {
		return pkg.Ledger{}, fmt.Errorf("error getting genesis ledger: %w", err)
	}
	return ledgers[0], nil
}

func pprint(v interface{}) {
	b, _ := json.MarshalIndent(v, "", "    ")
	fmt.Println(string(b))
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
