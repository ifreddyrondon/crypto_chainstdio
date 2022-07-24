package storing

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
)

type (
	Storage interface {
		Save(ctx context.Context, checkTx bool, l []pkg.Ledger) (int, error)
	}
)

type Sync struct {
	s      Storage
	logger *zap.Logger
}

func NewSync(logger *zap.Logger, s Storage) *Sync {
	return &Sync{
		s:      s,
		logger: logger.Named("sync_storer"),
	}
}

func (s Sync) Save(ctx context.Context, l []pkg.Ledger) error {
	t0 := time.Now()
	txsAmount, err := s.s.Save(ctx, true, l)
	if err != nil {
		return errors.Wrap(err, "error saving ledgers")
	}
	s.logger.Debug("finish",
		zap.Int("tx_amount", txsAmount),
		zap.Duration("duration", time.Since(t0)),
	)
	return nil
}
