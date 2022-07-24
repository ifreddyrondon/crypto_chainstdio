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
	Storer interface {
		Save() (chan<- []pkg.Ledger, <-chan error)
		Close()
	}
)

type Updater struct {
	localFetcher       LocalFetcher
	blockchainFetcher  BlockchainFetcher
	collector          Collector
	collectorBatchSize uint64
	storer             Storer
	logger             *zap.Logger
}

func New(lFetcher LocalFetcher, bFetcher BlockchainFetcher, c Collector, s Storer, l *zap.Logger) *Updater {
	return &Updater{
		localFetcher:       lFetcher,
		blockchainFetcher:  bFetcher,
		collector:          c,
		collectorBatchSize: 500,
		storer:             s,
		logger:             l.Named("updater"),
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
	u.logger.Sugar().Infof("latest local ledger index %v", local.Identifier.Index)
	onChain, err := u.blockchainFetcher.Latest(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return errors.Wrap(err, "error getting latest on chain ledger")
	}
	u.logger.Sugar().Infof("latest on-chain ledger index %v", onChain.Identifier.Index)
	from := local.Identifier.Index + 1
	to := onChain.Identifier.Index
	if from == to {
		return nil
	}
	u.logger.Sugar().Infof("there are %v ledgers to update", onChain.Identifier.Index-local.Identifier.Index)
	collectorJobCh, collectorResultCh, collectorErrCh := u.collector.Collect()
	saverJobCh, saverErrCh := u.storer.Save()
	var wg sync.WaitGroup
	wg.Add(2)
	// collecting
	go func() {
		defer func() {
			close(collectorJobCh)
			wg.Done()
		}()
		for from < to {
			select {
			case <-ctx.Done():
				return
			default:
				openSpots := cap(collectorJobCh) - len(collectorJobCh)
				// skip until get open spots
				if openSpots == 0 {
					continue
				}
				// only get the amount of open spots
				total := u.collectorBatchSize * uint64(openSpots)
				localTo := from + total
				if localTo > to {
					localTo = to
				}
				chunks := splitInterval(from, localTo, u.collectorBatchSize)
				for _, chunk := range chunks {
					collectorJobCh <- chunk
				}
				from += total
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
			case result, ok := <-collectorResultCh:
				if !ok {
					return
				}
				saverJobCh <- result
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
	case err := <-collectorErrCh:
		return errors.Wrap(err, "error collecting ledgers")
	case err := <-saverErrCh:
		return errors.Wrap(err, "error saving ledgers")
	case <-ctx.Done():
		u.logger.Info("shutting down...")
		<-doneCh
		u.collector.Close()
		u.storer.Close()
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
