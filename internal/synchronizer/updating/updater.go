package updating

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	LocalFetcher interface {
		Latest(ctx context.Context) (pkg.Ledger, error)
	}
	BlockchainFetcher interface {
		Latest(ctx context.Context) (pkg.Ledger, error)
	}
	Collector interface {
		Collect() (chan<- []pkg.Identifier, <-chan []pkg.Ledger, <-chan error)
		Close()
	}
)

type Updater struct {
	localFetcher      LocalFetcher
	blockchainFetcher BlockchainFetcher
	collector         Collector
	logger            *zap.Logger
}

func New(localFetcher LocalFetcher, blockchainFetcher BlockchainFetcher, collector Collector, logger *zap.Logger) *Updater {
	return &Updater{
		localFetcher:      localFetcher,
		blockchainFetcher: blockchainFetcher,
		collector:         collector,
		logger:            logger.Named("updater"),
	}
}

func (u Updater) Update(ctx context.Context) error {
	u.logger.Info("starting...")
	doneCh := make(chan bool)
	defer func() {
		close(doneCh)
		u.logger.Info("done")
	}()
	u.logger.Info("getting latest ledgers local and on chain")
	local, err := u.localFetcher.Latest(ctx)
	if err != nil {
		return errors.Wrap(err, "error getting latest local ledger")
	}
	onChain, err := u.blockchainFetcher.Latest(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return errors.Wrap(err, "error getting latest on chain ledger")
	}
	from := local.Identifier.Index + 1
	toOnChain := onChain.Identifier.Index
	if from == toOnChain {
		return nil
	}
	jobsCh, resultCh, errCollectingCh := u.collector.Collect()
	var wg sync.WaitGroup
	wg.Add(2)
	// collecting
	go func() {
		defer func() {
			close(jobsCh)
			wg.Done()
		}()
		for from < toOnChain {
			// context cancelled time to close
			if ctx.Err() != nil {
				return
			}
			to := from + 5000
			if to > toOnChain {
				to = toOnChain
			}
			chunks := splitInterval(from, to, 500)
			for _, chunk := range chunks {
				jobsCh <- chunk
			}
		}
	}()
	// storing
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case result, ok := <-resultCh:
				if !ok {
					return
				}
				_ = result
			}
		}
	}()

	go func(wg *sync.WaitGroup) {
		wg.Wait()
		doneCh <- true
	}(&wg)
	select {
	case <-doneCh:
		return nil
	case err := <-errCollectingCh:
		return errors.Wrap(err, "error collecting ledgers")
	case <-ctx.Done():
		u.logger.Info("shutting down...")
		<-doneCh
		u.collector.Close()
		return nil
	}
}

func splitInterval(from, to, size uint64) [][]pkg.Identifier {
	chunks := make([][]pkg.Identifier, 0, ((to-from)/size)+1)
	for (to - from) >= size {
		chunk := make([]pkg.Identifier, 0, size)
		for i := from; i < from+size; i++ {
			chunk = append(chunk, pkg.Identifier{Index: i})
		}
		chunks = append(chunks, chunk)
		from = from + size
	}
	if (to - from) > 0 {
		chunk := make([]pkg.Identifier, 0, to-from)
		for i := from; i < to; i++ {
			chunk = append(chunk, pkg.Identifier{Index: i})
		}
		chunks = append(chunks, chunk)
	}
	return chunks
}
