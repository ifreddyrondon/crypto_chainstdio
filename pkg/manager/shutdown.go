package manager

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

// TermDelay specifies the timeout after which the service will be forced to terminate.
const TermDelay = time.Second * 20

// GracefulShutdown sets up a shutdown handler that calls killFunc when the process needs to clean up and terminate.
// killFunc will be executed at most once.
func GracefulShutdown(log *zap.Logger, killFunc func()) {
	intCh := make(chan os.Signal)
	signal.Notify(intCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	var sigCount int
	go func() {
		for sig := range intCh {
			log.Debug("received termination signal", zap.String("signal", sig.String()))
			switch sigCount {
			case 0:
				go func() {
					log.Info("shutdown requested", zap.String("signal", sig.String()))
					killFunc()
				}()
			case 1:
				log.Info("delayed forced termination requested",
					zap.Duration("delay", TermDelay),
					zap.String("signal", sig.String()))
				time.AfterFunc(TermDelay, func() {
					os.Exit(2)
				})
			default:
				log.Warn("Forced termination requested", zap.String("signal", sig.String()))
				_ = log.Sync() // #nosec
				os.Exit(2)
			}
			sigCount++
		}
	}()
}
