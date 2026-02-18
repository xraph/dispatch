package dispatch

import (
	"context"
	"log/slog"
)

// Option configures a Dispatcher.
type Option func(*Dispatcher) error

// Storer is the minimal store interface held by the Dispatcher.
// It covers lifecycle operations only. The full composite interface
// (store.Store) is used in subsystem layers that don't create import
// cycles. Implementations satisfy store.Store which embeds all
// subsystem stores.
type Storer interface {
	Migrate(ctx context.Context) error
	Ping(ctx context.Context) error
	Close() error
}

// poolRunner is an internal interface for worker pool lifecycle.
type poolRunner interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// extensionEmitter is an internal interface for extension lifecycle events.
type extensionEmitter interface {
	EmitShutdown(ctx context.Context)
}

// Dispatcher is the central coordinator for job processing, workflows,
// cron scheduling, and distributed coordination.
//
// Create one with New() and functional options. The Dispatcher holds
// references to subsystem components via internal interfaces to avoid
// import cycles. Use the Setup() function from the dispatch/setup
// package to wire everything together.
type Dispatcher struct {
	config     Config
	logger     *slog.Logger
	store      Storer
	extensions extensionEmitter
	pool       poolRunner

	// started tracks whether Start has been called.
	started bool
}

// New creates a new Dispatcher with the given options.
func New(opts ...Option) (*Dispatcher, error) {
	d := &Dispatcher{
		config: DefaultConfig(),
		logger: slog.Default(),
	}
	for _, opt := range opts {
		if err := opt(d); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// Logger returns the dispatcher's logger.
func (d *Dispatcher) Logger() *slog.Logger { return d.logger }

// Store returns the dispatcher's store.
func (d *Dispatcher) Store() Storer { return d.store }

// Config returns a copy of the dispatcher's configuration.
func (d *Dispatcher) Config() Config { return d.config }

// SetPool sets the worker pool (called by setup package).
func (d *Dispatcher) SetPool(p poolRunner) { d.pool = p }

// SetExtensions sets the extension emitter (called by setup package).
func (d *Dispatcher) SetExtensions(e extensionEmitter) { d.extensions = e }

// Start begins job processing.
func (d *Dispatcher) Start(ctx context.Context) error {
	if d.pool == nil {
		return ErrNoStore
	}
	if err := d.pool.Start(ctx); err != nil {
		return err
	}
	d.started = true
	return nil
}

// Stop gracefully shuts down the dispatcher.
func (d *Dispatcher) Stop(ctx context.Context) error {
	if d.pool != nil && d.started {
		if err := d.pool.Stop(ctx); err != nil {
			d.logger.Error("pool stop error", "error", err)
		}
	}
	if d.extensions != nil {
		d.extensions.EmitShutdown(ctx)
	}
	if d.store != nil {
		return d.store.Close()
	}
	return nil
}

// WithConcurrency sets the maximum number of concurrent job processors.
func WithConcurrency(n int) Option {
	return func(d *Dispatcher) error {
		d.config.Concurrency = n
		return nil
	}
}

// WithQueues sets the queues the dispatcher will poll.
func WithQueues(queues []string) Option {
	return func(d *Dispatcher) error {
		d.config.Queues = queues
		return nil
	}
}

// WithLogger sets the structured logger for the dispatcher.
func WithLogger(l *slog.Logger) Option {
	return func(d *Dispatcher) error {
		d.logger = l
		return nil
	}
}

// WithStore sets the persistence backend for the dispatcher.
// The store must implement Storer at minimum; typically it will be a
// store.Store which embeds all subsystem store interfaces.
func WithStore(s Storer) Option {
	return func(d *Dispatcher) error {
		d.store = s
		return nil
	}
}
