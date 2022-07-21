package manager

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type Service interface {
	Name() string
	Run(context.Context) error
}

func ServiceFactory(name string, runner func(ctx context.Context) error) Service {
	return service{name: name, runner: runner}
}

type service struct {
	name   string
	runner func(ctx context.Context) error
}

func (s service) Name() string {
	return s.name
}

func (s service) Run(ctx context.Context) error {
	return s.runner(ctx)
}

type Manager struct {
	rootCtx       context.Context
	serviceCtx    context.Context
	cancelFn      context.CancelFunc
	servicesWg    sync.WaitGroup
	log           *zap.Logger
	shutdownHooks []func()
	shutdownCh    chan bool
}

func New(log *zap.Logger) Manager {
	ctx := context.Background()
	serviceCtx, cancelFn := context.WithCancel(ctx)
	return Manager{
		rootCtx:    ctx,
		serviceCtx: serviceCtx,
		cancelFn:   cancelFn,
		servicesWg: sync.WaitGroup{},
		shutdownCh: make(chan bool),
		log:        log.Named("manager"),
	}
}

func (m *Manager) AddService(s Service) {
	m.servicesWg.Add(1)
	go func() {
		defer m.servicesWg.Done()
		m.log.Info("adding service", zap.String("serviceName", s.Name()))
		if err := s.Run(m.serviceCtx); err != nil {
			m.log.Error("service exited with error",
				zap.Error(err),
				zap.String("serviceName", s.Name()),
			)
			return
		}
		m.log.Info("service exited successfully", zap.String("serviceName", s.Name()))
	}()
}

func (m *Manager) AddShutdownHook(fn func()) {
	m.shutdownHooks = append(m.shutdownHooks, fn)
}

// WaitForInterrupt will block until a signal SIGINT or SIGTERM
// is received on manager.shutdownCh. After receiving a shutdown
// signal, cancels service context then runs shutdown hooks.
func (m *Manager) WaitForInterrupt() {
	// 1 signal is graceful, 2 starts a 20 second to force kill, 3 is immediate force kill
	GracefulShutdown(m.log, func() {
		close(m.shutdownCh)
	})
	<-m.shutdownCh
	m.log.Info("shutting down services")
	m.cancelFn()
	// Wait for all services finish
	m.servicesWg.Wait()
	m.log.Info("running shutdown hooks")
	for _, hook := range m.shutdownHooks {
		hook()
	}
	m.log.Info("hooks stopped")
}
