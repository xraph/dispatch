// Package engine wires all Dispatch subsystems together. It creates the
// extension registry, job registry, middleware chain, worker pool, and
// provides Register/Enqueue operations.
//
// This package exists to break the import cycle: the root dispatch package
// defines Entity (imported by job, workflow, etc.) and so cannot import
// those packages back. The engine package sits above all subsystem packages
// and below the application layer.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	mw "github.com/xraph/dispatch/middleware"
	"github.com/xraph/dispatch/observability"
	"github.com/xraph/dispatch/queue"
	"github.com/xraph/dispatch/scope"
	"github.com/xraph/dispatch/worker"
	"github.com/xraph/dispatch/workflow"
)

// extRunEmitter adapts *ext.Registry to satisfy workflow.RunEmitter.
// This breaks the import cycle: workflow defines the interface,
// ext.Registry provides the implementation, and the engine layer
// plugs them together.
type extRunEmitter struct {
	r *ext.Registry
}

func (a *extRunEmitter) EmitStepCompleted(ctx context.Context, run *workflow.Run, stepName string, elapsed time.Duration) {
	a.r.EmitWorkflowStepCompleted(ctx, run, stepName, elapsed)
}

func (a *extRunEmitter) EmitStepFailed(ctx context.Context, run *workflow.Run, stepName string, err error) {
	a.r.EmitWorkflowStepFailed(ctx, run, stepName, err)
}

func (a *extRunEmitter) EmitWorkflowStarted(ctx context.Context, run *workflow.Run) {
	a.r.EmitWorkflowStarted(ctx, run)
}

func (a *extRunEmitter) EmitWorkflowCompleted(ctx context.Context, run *workflow.Run, elapsed time.Duration) {
	a.r.EmitWorkflowCompleted(ctx, run, elapsed)
}

func (a *extRunEmitter) EmitWorkflowFailed(ctx context.Context, run *workflow.Run, err error) {
	a.r.EmitWorkflowFailed(ctx, run, err)
}

// Engine wraps a Dispatcher with typed subsystem access.
// Use Build() to create one from a Dispatcher.
type Engine struct {
	d          *dispatch.Dispatcher
	extensions *ext.Registry
	registry   *job.Registry
	jobStore   job.Store
	dlqService *dlq.Service
	bo         backoff.Strategy
	pool       *worker.Pool
	mws        []mw.Middleware
	logger     *slog.Logger

	// Workflow subsystem.
	wfRegistry *workflow.Registry
	wfRunner   *workflow.Runner
	eventBus   *event.Bus

	// Cron subsystem.
	cronStore    cron.Store
	clusterStore cluster.Store
	scheduler    *cron.Scheduler

	// Queue subsystem.
	queueConfigs []queue.Config
	queueManager *queue.Manager

	// OpenTelemetry providers (optional; nil means use global).
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
}

// Option configures an Engine.
type Option func(*Engine)

// WithExtension registers an extension with the engine.
func WithExtension(e ext.Extension) Option {
	return func(eng *Engine) {
		eng.extensions.Register(e)
	}
}

// WithMiddleware adds middleware to the engine's chain.
func WithMiddleware(m mw.Middleware) Option {
	return func(eng *Engine) {
		eng.mws = append(eng.mws, m)
	}
}

// WithBackoff sets the retry backoff strategy for the engine.
// If not set, backoff.DefaultStrategy() (exponential with jitter) is used.
func WithBackoff(b backoff.Strategy) Option {
	return func(eng *Engine) {
		eng.bo = b
	}
}

// WithQueueConfig registers queue-level rate limiting and concurrency
// configurations. Queues not listed have no limits.
func WithQueueConfig(configs ...queue.Config) Option {
	return func(eng *Engine) {
		eng.queueConfigs = append(eng.queueConfigs, configs...)
	}
}

// WithTracerProvider sets a custom OTel TracerProvider for the engine.
// When set, the tracing middleware uses this provider instead of the global one.
// If not set, the global otel.GetTracerProvider() is used.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(eng *Engine) {
		eng.tracerProvider = tp
	}
}

// WithMeterProvider sets a custom OTel MeterProvider for the engine.
// When set, both the metrics middleware and the observability extension
// use this provider instead of the global one.
// If not set, the global otel.GetMeterProvider() is used.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(eng *Engine) {
		eng.meterProvider = mp
	}
}

// Build creates an Engine from an existing Dispatcher.
// The Dispatcher's store must implement job.Store.
func Build(d *dispatch.Dispatcher, opts ...Option) (*Engine, error) {
	logger := d.Logger()
	store := d.Store()

	if store == nil {
		return nil, dispatch.ErrNoStore
	}

	// Type-assert the store to get the job.Store interface.
	js, ok := store.(job.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement job.Store")
	}

	// Type-assert the store to get the dlq.Store interface.
	ds, ok := store.(dlq.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement dlq.Store")
	}

	// Type-assert the store to get the workflow.Store interface.
	ws, ok := store.(workflow.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement workflow.Store")
	}

	// Type-assert the store to get the event.Store interface.
	es, ok := store.(event.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement event.Store")
	}

	// Type-assert the store to get the cron.Store interface.
	cs, ok := store.(cron.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement cron.Store")
	}

	// Type-assert the store to get the cluster.Store interface.
	cls, ok := store.(cluster.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement cluster.Store")
	}

	eng := &Engine{
		d:          d,
		extensions: ext.NewRegistry(logger),
		registry:   job.NewRegistry(),
		jobStore:   js,
		logger:     logger,
	}

	for _, opt := range opts {
		opt(eng)
	}

	// Default backoff strategy if none provided.
	if eng.bo == nil {
		eng.bo = backoff.DefaultStrategy()
	}

	// Create the DLQ service.
	eng.dlqService = dlq.NewService(ds, js)

	// Create the workflow subsystem.
	emitter := &extRunEmitter{r: eng.extensions}
	eng.wfRegistry = workflow.NewRegistry()
	eng.eventBus = event.NewBus(es)
	eng.wfRunner = workflow.NewRunner(eng.wfRegistry, ws, es, emitter, logger)

	// Build tracing middleware (custom provider or global).
	var tracingMw mw.Middleware
	if eng.tracerProvider != nil {
		tracer := eng.tracerProvider.Tracer("github.com/xraph/dispatch")
		tracingMw = mw.TracingWithTracer(tracer)
	} else {
		tracingMw = mw.Tracing()
	}

	// Build metrics middleware (custom provider or global).
	var metricsMw mw.Middleware
	if eng.meterProvider != nil {
		meter := eng.meterProvider.Meter("github.com/xraph/dispatch")
		metricsMw = mw.MetricsWithMeter(meter)
	} else {
		metricsMw = mw.Metrics()
	}

	// Register the observability metrics extension.
	var obsExt *observability.MetricsExtension
	if eng.meterProvider != nil {
		meter := eng.meterProvider.Meter("github.com/xraph/dispatch/observability")
		obsExt = observability.NewMetricsExtensionWithMeter(meter)
	} else {
		obsExt = observability.NewMetricsExtension()
	}
	eng.extensions.Register(obsExt)

	// Build default middleware stack: recover → tracing → metrics → logging → scope → timeout.
	defaultMws := []mw.Middleware{
		mw.Recover(logger),
		tracingMw,
		metricsMw,
		mw.Logging(logger),
		mw.Scope(),
		mw.Timeout(logger),
	}
	allMws := make([]mw.Middleware, 0, len(defaultMws)+len(eng.mws))
	allMws = append(allMws, defaultMws...)
	allMws = append(allMws, eng.mws...)

	// Create executor and pool.
	config := d.Config()
	executor := worker.NewExecutor(eng.registry, eng.extensions, eng.jobStore, eng.dlqService, eng.bo, logger, allMws...)

	poolOpts := []worker.PoolOption{
		worker.WithPoolConcurrency(config.Concurrency),
		worker.WithPoolQueues(config.Queues),
		worker.WithPollInterval(config.PollInterval),
		worker.WithHeartbeatInterval(config.HeartbeatInterval),
		worker.WithStaleJobThreshold(config.StaleJobThreshold),
	}

	// Create queue manager if queue configs were provided.
	if len(eng.queueConfigs) > 0 {
		eng.queueManager = queue.NewManager(eng.queueConfigs...)
		poolOpts = append(poolOpts, worker.WithQueueManager(eng.queueManager))
	}

	eng.pool = worker.NewPool(
		eng.jobStore,
		executor,
		eng.extensions,
		logger,
		poolOpts...,
	)

	// Wire back into the Dispatcher.
	d.SetPool(eng.pool)
	d.SetExtensions(eng.extensions)

	// Create cron scheduler.
	eng.cronStore = cs
	eng.clusterStore = cls
	enqueueFunc := func(ctx context.Context, name string, payload []byte, opts ...job.Option) (id.JobID, error) {
		j, err := eng.EnqueueRaw(ctx, name, payload, opts...)
		if err != nil {
			return id.JobID{}, err
		}
		return j.ID, nil
	}
	eng.scheduler = cron.NewScheduler(cs, cls, enqueueFunc, eng.extensions, eng.pool.WorkerID(), logger)

	// Register this worker in the cluster store.
	hostname, hostnameErr := os.Hostname()
	if hostnameErr != nil {
		hostname = "unknown"
	}
	w := &cluster.Worker{
		ID:          eng.pool.WorkerID(),
		Hostname:    hostname,
		Queues:      config.Queues,
		Concurrency: config.Concurrency,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}
	if regErr := cls.RegisterWorker(context.Background(), w); regErr != nil {
		logger.Warn("failed to register worker in cluster store", slog.String("error", regErr.Error()))
	}

	return eng, nil
}

// Register registers a typed job definition with the engine.
func Register[T any](eng *Engine, def *job.Definition[T]) {
	job.RegisterDefinition(eng.registry, def)
}

// Enqueue creates and enqueues a job.
func Enqueue[T any](ctx context.Context, eng *Engine, name string, payload T, opts ...job.Option) (*job.Job, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload for job %q: %w", name, err)
	}

	return eng.EnqueueRaw(ctx, name, data, opts...)
}

// EnqueueRaw enqueues a job with a pre-serialized payload.
func (eng *Engine) EnqueueRaw(ctx context.Context, name string, payload []byte, opts ...job.Option) (*job.Job, error) {
	// Capture scope from context.
	appID, orgID := scope.Capture(ctx)

	now := time.Now().UTC()
	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       name,
		Payload:    payload,
		State:      job.StatePending,
		MaxRetries: 3,
		Queue:      "default",
		Priority:   0,
		RunAt:      now,
		ScopeAppID: appID,
		ScopeOrgID: orgID,
	}

	// Apply functional options.
	jobOpts := job.DefaultOptions()
	for _, opt := range opts {
		opt(&jobOpts)
	}
	j.Queue = jobOpts.Queue
	j.Priority = jobOpts.Priority
	j.MaxRetries = jobOpts.MaxRetries
	j.Timeout = jobOpts.Timeout
	if !jobOpts.RunAt.IsZero() {
		j.RunAt = jobOpts.RunAt
	}

	if err := eng.jobStore.EnqueueJob(ctx, j); err != nil {
		return nil, err
	}

	eng.extensions.EmitJobEnqueued(ctx, j)
	return j, nil
}

// Start begins job processing by starting the worker pool and cron scheduler.
// It also resumes any workflow runs left in "running" state (crash recovery).
func (eng *Engine) Start(ctx context.Context) error {
	// Resume any interrupted workflow runs (best-effort, non-fatal).
	if resumeErr := eng.wfRunner.ResumeAll(ctx); resumeErr != nil {
		eng.logger.Warn("failed to resume workflow runs",
			slog.String("error", resumeErr.Error()),
		)
	}

	// Start the cron scheduler before the pool so leadership can be acquired.
	if err := eng.scheduler.Start(ctx); err != nil {
		return fmt.Errorf("start cron scheduler: %w", err)
	}

	return eng.d.Start(ctx)
}

// Stop gracefully shuts down the engine.
func (eng *Engine) Stop(ctx context.Context) error {
	// Deregister this worker from the cluster.
	if err := eng.clusterStore.DeregisterWorker(ctx, eng.pool.WorkerID()); err != nil {
		eng.logger.Warn("failed to deregister worker", slog.String("error", err.Error()))
	}

	// Stop the cron scheduler.
	if err := eng.scheduler.Stop(ctx); err != nil {
		eng.logger.Error("cron scheduler stop error", slog.String("error", err.Error()))
	}

	return eng.d.Stop(ctx)
}

// Extensions returns the extension registry.
func (eng *Engine) Extensions() *ext.Registry { return eng.extensions }

// Registry returns the job registry.
func (eng *Engine) Registry() *job.Registry { return eng.registry }

// Dispatcher returns the underlying Dispatcher.
func (eng *Engine) Dispatcher() *dispatch.Dispatcher { return eng.d }

// DLQService returns the engine's DLQ service for replay and inspection.
func (eng *Engine) DLQService() *dlq.Service { return eng.dlqService }

// WorkflowRunner returns the workflow runner.
func (eng *Engine) WorkflowRunner() *workflow.Runner { return eng.wfRunner }

// EventBus returns the event bus.
func (eng *Engine) EventBus() *event.Bus { return eng.eventBus }

// CronStore returns the cron store.
func (eng *Engine) CronStore() cron.Store { return eng.cronStore }

// Scheduler returns the cron scheduler.
func (eng *Engine) Scheduler() *cron.Scheduler { return eng.scheduler }

// QueueManager returns the queue manager, or nil if no queue configs
// were provided.
func (eng *Engine) QueueManager() *queue.Manager { return eng.queueManager }

// RegisterCron registers a typed cron definition with the engine.
// It validates the schedule expression, computes the initial NextRunAt,
// and persists the entry. Re-registration of the same name is idempotent.
func RegisterCron[T any](ctx context.Context, eng *Engine, def *cron.Definition[T]) error {
	// Validate the cron expression.
	sched, err := cron.ParseSchedule(def.Schedule)
	if err != nil {
		return fmt.Errorf("invalid cron schedule %q: %w", def.Schedule, err)
	}

	// Marshal the default payload.
	payload, err := json.Marshal(def.Payload)
	if err != nil {
		return fmt.Errorf("marshal cron payload: %w", err)
	}

	// Compute the initial NextRunAt.
	now := time.Now().UTC()
	next := sched.Next(now)

	entry := &cron.Entry{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewCronID(),
		Name:      def.Name,
		Schedule:  def.Schedule,
		JobName:   def.JobName,
		Queue:     def.Queue,
		Payload:   payload,
		NextRunAt: &next,
		Enabled:   true,
	}

	if err := eng.cronStore.RegisterCron(ctx, entry); err != nil {
		// Idempotent: ignore duplicate cron entries.
		if errors.Is(err, dispatch.ErrDuplicateCron) {
			return nil
		}
		return fmt.Errorf("register cron %q: %w", def.Name, err)
	}

	eng.logger.Info("cron registered",
		slog.String("name", def.Name),
		slog.String("schedule", def.Schedule),
		slog.String("job_name", def.JobName),
		slog.Time("next_run_at", next),
	)

	return nil
}

// RegisterWorkflow registers a typed workflow definition with the engine.
func RegisterWorkflow[T any](eng *Engine, def *workflow.Definition[T]) {
	workflow.RegisterDefinition(eng.wfRegistry, def)
}

// StartWorkflow starts a workflow run with a typed input.
func StartWorkflow[T any](ctx context.Context, eng *Engine, name string, input T) (*workflow.Run, error) {
	return workflow.Start(ctx, eng.wfRunner, name, input)
}
