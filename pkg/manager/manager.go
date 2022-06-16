package manager

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
	shutdownHooks []func()
}

func New() Manager {
	ctx := context.Background()
	serviceCtx, cancelFn := context.WithCancel(ctx)
	return Manager{
		rootCtx:    ctx,
		serviceCtx: serviceCtx,
		cancelFn:   cancelFn,
		servicesWg: sync.WaitGroup{},
	}
}

func (m *Manager) AddService(s Service) {
	m.servicesWg.Add(1)
	go func() {
		defer m.servicesWg.Done()
		log.Printf("adding service '%s'\n", s.Name())
		if err := s.Run(m.serviceCtx); err != nil {
			log.Printf("service '%s' exited with error: %v\n", s.Name(), err)
			return
		}
		log.Printf("service '%s' exited successfully\n", s.Name())
	}()
}

func (m *Manager) AddShutdownHook(fn func()) {
	m.shutdownHooks = append(m.shutdownHooks, fn)
}

// WaitForInterrupt will block until a signal is sent to shut down the process
func (m *Manager) WaitForInterrupt() {
	gracefulShutdown()
	log.Printf("shutting down process...")
	m.cancelFn()
	// Wait for all services finish
	m.servicesWg.Wait()
	log.Printf("running shutdown hooks")
	for _, hook := range m.shutdownHooks {
		hook()
	}
}

func gracefulShutdown() {
	termCh := make(chan os.Signal)
	signal.Notify(termCh, syscall.SIGTERM, syscall.SIGINT)
	// Wait for the notify signal
	for {
		select {
		case <-termCh:
			log.Printf("shutdown requested\n")
			return
		}
	}
}
