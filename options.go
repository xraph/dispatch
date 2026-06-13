package dispatch

import (
	"context"
	"time"

	log "github.com/xraph/go-utils/log"
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
	logger     log.Logger
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
		logger: log.NewNoopLogger(),
	}
	for _, opt := range opts {
		if err := opt(d); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// Logger returns the dispatcher's logger.
func (d *Dispatcher) Logger() log.Logger { return d.logger }

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
			d.logger.Error("pool stop error", log.String("error", err.Error()))
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
func WithLogger(l log.Logger) Option {
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

// WithPollInterval sets how often each worker polls for new jobs.
// Lower values reduce job pickup latency at the cost of constant
// driver-pool pressure. Default 1s.
func WithPollInterval(d time.Duration) Option {
	return func(disp *Dispatcher) error {
		disp.config.PollInterval = d
		return nil
	}
}

// WithMaxPollInterval caps the worker fetcher's idle backoff. Empty polls
// double the poll interval from PollInterval up to this value; new work or
// an in-process enqueue resets it. Default 30s.
func WithMaxPollInterval(d time.Duration) Option {
	return func(disp *Dispatcher) error {
		disp.config.MaxPollInterval = d
		return nil
	}
}

// WithHeartbeatInterval sets how often running jobs send heartbeats.
// Default 10s. Set to 0 to disable heartbeats entirely (workers won't
// extend lease on long-running jobs).
func WithHeartbeatInterval(d time.Duration) Option {
	return func(disp *Dispatcher) error {
		disp.config.HeartbeatInterval = d
		return nil
	}
}

// WithStaleJobThreshold sets how long without a heartbeat before a
// job is considered stale and reaped. Default 30s. The reaper runs
// at this interval too, so larger values reduce the reap query rate.
// Set to 0 to disable stale-job reaping entirely.
func WithStaleJobThreshold(d time.Duration) Option {
	return func(disp *Dispatcher) error {
		disp.config.StaleJobThreshold = d
		return nil
	}
}

// WithWorkerStoreCallTimeout caps a single worker store roundtrip.
// Bounds how long a stalled driver session can hold a pool connection
// before the worker abandons it. Default 5s; negative disables (test-only).
func WithWorkerStoreCallTimeout(d time.Duration) Option {
	return func(disp *Dispatcher) error {
		disp.config.WorkerStoreCallTimeout = d
		return nil
	}
}

// WithCronTickInterval sets how often the cron scheduler checks for
// due entries. Default 1s. Ticks run against in-memory leadership and
// cron-list caches, so they cost no store traffic between refreshes.
func WithCronTickInterval(d time.Duration) Option {
	return func(disp *Dispatcher) error {
		disp.config.CronTickInterval = d
		return nil
	}
}

// WithCronLeaderTTL sets the leader-election lock TTL. The leader
// loop renews at half this interval, so 60s = renew every 30s.
// Default 60s; failover after a leader crash takes up to the TTL.
func WithCronLeaderTTL(d time.Duration) Option {
	return func(disp *Dispatcher) error {
		disp.config.CronLeaderTTL = d
		return nil
	}
}

// WithCronRefreshInterval sets how often the cron leader re-lists entries
// from the store. Between refreshes ticks run against an in-memory cache;
// in-process registrations invalidate it immediately. Default 30s.
func WithCronRefreshInterval(d time.Duration) Option {
	return func(disp *Dispatcher) error {
		disp.config.CronRefreshInterval = d
		return nil
	}
}

// WithCronLockTTL sets the per-entry distributed lock TTL fired
// around each cron entry execution. Default 30s.
func WithCronLockTTL(d time.Duration) Option {
	return func(disp *Dispatcher) error {
		disp.config.CronLockTTL = d
		return nil
	}
}

// WithCronStoreCallTimeout caps a single cron-scheduler store
// roundtrip (GetLeader, RenewLeadership, ListCrons, Acquire/Release
// lock, Update entry). Default 5s; negative disables (test-only).
func WithCronStoreCallTimeout(d time.Duration) Option {
	return func(disp *Dispatcher) error {
		disp.config.CronStoreCallTimeout = d
		return nil
	}
}
