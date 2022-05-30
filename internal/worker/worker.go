package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type LedgerFetcher interface {
	Ledgers(ctx context.Context, ids ...pkg.Identifier) ([]pkg.Ledger, error)
	Latest(ctx context.Context) (pkg.Ledger, error)
}

type LedgerStore interface {
	Latest(ctx context.Context) (pkg.Ledger, error)
	Save(ctx context.Context, l pkg.Ledger) (pkg.Ledger, error)
}

type Sequencer struct {
	pollerInterval  time.Duration
	pollerBatchSize uint64
	fetcher         LedgerFetcher
	reconciler      Reconciler
}

func New(f LedgerFetcher, r Reconciler) Sequencer {
	return Sequencer{
		pollerBatchSize: 5,
		pollerInterval:  time.Second * 15,
		fetcher:         f,
		reconciler:      r,
	}
}

func (s Sequencer) Run(ctx context.Context) error {
	if err := s.reconciler.Process(ctx); err != nil {
		return fmt.Errorf("error running: %w", err)
	}
	if err := s.poll(ctx); err != nil {
		log.Println(err)
	}
	ticker := time.NewTicker(s.pollerInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.poll(ctx); err != nil {
				log.Println(err)
			}
		}
	}
}

func (s Sequencer) poll(ctx context.Context) error {
	batchIndex := uint64(1000000)
	ids := make([]pkg.Identifier, 0, s.pollerBatchSize)
	for i := batchIndex; i < batchIndex+s.pollerBatchSize; i++ {
		ids = append(ids, pkg.Identifier{Index: i})
	}
	l, err := s.fetcher.Ledgers(ctx, ids...)
	if err != nil {
		return fmt.Errorf("error polling blocks from %v to %v. err: %w", batchIndex, batchIndex+s.pollerBatchSize, err)
	}
	pprint(l)
	return nil
}

func pprint(v interface{}) {
	b, _ := json.MarshalIndent(v, "", "    ")
	fmt.Println(string(b))
}
