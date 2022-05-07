package sequencer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type Ledger interface {
	Ledgers(ctx context.Context, ids ...pkg.Identifier) ([]pkg.Ledger, error)
}

type Sequencer struct {
	pollerInterval  time.Duration
	pollerBatchSize uint64
	ledger          Ledger
	genesisIndex    uint64
}

func New(l Ledger) Sequencer {
	return Sequencer{
		pollerBatchSize: 5,
		pollerInterval:  time.Second * 15,
		ledger:          l,
		genesisIndex:    0,
	}
}

func (w Sequencer) Run(ctx context.Context) error {
	if err := w.poll(ctx); err != nil {
		log.Println(err)
	}
	ticker := time.NewTicker(w.pollerInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := w.poll(ctx); err != nil {
				log.Println(err)
			}
		}
	}
}

func (w Sequencer) reconcile(ctx context.Context) error {
	return nil
}

func (w Sequencer) poll(ctx context.Context) error {
	batchIndex := uint64(1000000)
	ids := make([]pkg.Identifier, 0, w.pollerBatchSize)
	for i := batchIndex; i < batchIndex+w.pollerBatchSize; i++ {
		ids = append(ids, pkg.Identifier{Index: i})
	}
	l, err := w.ledger.Ledgers(ctx, ids...)
	if err != nil {
		return fmt.Errorf("error polling blocks from %v to %v. err: %w", batchIndex, batchIndex+w.pollerBatchSize, err)
	}
	pprint(l)
	return nil
}

func pprint(v interface{}) {
	b, _ := json.MarshalIndent(v, "", "    ")
	fmt.Println(string(b))
}
