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
	"os"
	"slices"
	"time"

	log "github.com/xraph/go-utils/log"
	gu "github.com/xraph/go-utils/metrics"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	mw "github.com/xraph/dispatch/middleware"
	"github.com/xraph/dispatch/observability"
	"github.com/xraph/dispatch/queue"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/scope"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/stream"
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
	logger     log.Logger

	// Workflow subsystem.
	wfRegistry *workflow.Registry
	wfRunner   *workflow.Runner
	eventBus   *event.Bus

	// Cron subsystem.
	cronStore    cron.Store
	clusterStore cluster.Store
	scheduler    *cron.Scheduler

	// wakeStop terminates the store wake listener (store.WakeNotifier);
	// nil when the store has no push capability.
	wakeStop func()

	// Stream broker (real-time event pub/sub).
	broker       *stream.Broker
	brokerOpts   []stream.BrokerOption
	enableBroker bool

	// Artifact plane (optional; nil means disabled).
	artifacts     *artifact.Service
	artifactCache *cache.Cache

	// Resource model (optional; zero values mean no requirements and no
	// capacity check, which is exactly today's behaviour).
	estimator       resource.Estimator
	resourceDefault resource.Set
	queueResources  map[string]resource.Set
	workerCapacity  resource.Set

	// resources is the shared admission ledger. It must be the SAME
	// instance the staging cache was built with, or the cache's staged
	// bytes are invisible to the pool's disk budget. Nil disables the
	// model outright.
	resources resource.Manager
	// workerCustomKeys narrows the custom keys this worker advertises at
	// dequeue. Empty derives them from the manager's capacity.
	workerCustomKeys []string

	// Queue subsystem.
	queueConfigs []queue.Config
	queueManager *queue.Manager

	// OpenTelemetry providers (optional; nil means use global).
	tracerProvider trace.TracerProvider
	// metricFactory is the go-utils MetricFactory for engine-level metrics.
	// nil means use gu.NewMetricsCollector default.
	metricFactory gu.MetricFactory

	// executors is the registry job attempts are dispatched through. It
	// always has the in-process executor as its default.
	executors *exec.Registry
	// extraExecutors accumulates executors added via WithExecutor until
	// buildExecutors assembles them into executors.
	extraExecutors []exec.Executor
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

// WithEstimator installs the resource estimator consulted at enqueue.
//
// The estimator sits above a definition's static declaration and below
// a per-enqueue override. It receives the declaration in the request and
// may return it unchanged, so installing one is an explicit opt-in to
// letting inference override declaration. An estimator that errors is
// ignored: it must never fail an enqueue.
func WithEstimator(e resource.Estimator) Option {
	return func(eng *Engine) { eng.estimator = e }
}

// WithResourceDefaults sets the fleet-wide default requirement and any
// per-queue overrides. Both are the lowest-precedence sources, below a
// definition's own declaration.
func WithResourceDefaults(global resource.Set, perQueue map[string]resource.Set) Option {
	return func(eng *Engine) {
		eng.resourceDefault = global
		eng.queueResources = perQueue
	}
}

// WithWorkerCapacity declares the largest single-worker capacity in the
// fleet, and is the ONLY thing that turns the enqueue-time unschedulable
// check on. Leave it unset — the default — and Enqueue never rejects a
// job for being too big for any worker.
//
// It is a fleet-wide statement, not a description of this process, and
// the distinction is the whole reason the check is opt-in. Declare it
// and a job requiring more than this on any dimension fails Enqueue with
// ErrUnschedulable, wherever it was enqueued from: a light API pod that
// declared its own 2 GiB would hard-reject the tessellation job the
// heavy tier runs perfectly well.
//
// The check cannot derive the fleet maximum for itself, because
// cluster.Worker.Capacity does not round-trip. Only store/memory carries
// it; postgres, sqlite, mongo, redis and the k8s provider all enumerate
// worker fields by hand and drop it, so a worker registered with
// {memory: 64GiB} reads back an empty map. MaxWorkerCapacity therefore
// sees this value and — on memory alone — whatever live workers
// published, never the real fleet maximum. Persisting Capacity in those
// four models would make the derivation honest and is tracked as
// follow-up work; until then, declaring the ceiling is the operator's
// job or the check stays off.
//
// It is deliberately NOT defaulted from WithResourceManager's capacity.
// That default read as a convenience and behaved as a silent rescope of
// a fleet-wide question to one process.
func WithWorkerCapacity(c resource.Set) Option {
	return func(eng *Engine) { eng.workerCapacity = c }
}

// WithResourceManager installs the admission ledger the worker pool
// admits jobs against.
//
// The manager passed here MUST be the same instance the staging cache
// was built with (cache.WithManager). One ledger is the whole design:
// the cache holds a lease per cached entry and registers itself as the
// manager's disk reclaimer, and the pool's dequeue budget offers disk as
// free PLUS what that reclaimer could evict. Give the cache a private
// manager — which it constructs for itself when none is supplied — and
// the pool's Reclaimable() is permanently zero, staged bytes are never
// offered back to the budget, and the disk path quietly does nothing.
// It presents as a worker that went quiet, not as an error.
//
// Leaving this unset is the supported default: the pool passes an
// unbounded DequeueOpts, every backend skips its fit predicate, and no
// leases are taken. That is exactly how Dispatch behaved before the
// resource model existed.
//
// Installing a manager does NOT declare a fleet capacity, and does not
// turn the enqueue-time unschedulable check on. See WithWorkerCapacity
// for why that is opt-in and separate.
//
// WARNING — leases. A pool that dequeues through job.LeaseStore calls
// DequeueLeased(queues, limit), which carries no budget, no custom-key
// containment and no locality. Every guarantee this manager provides at
// the STORE is absent on that path: the pool still admits locally, so a
// job too large for this worker is claimed, refused and requeued on
// every poll rather than left for a worker that fits. Turning leases and
// resources on together is the natural upgrade and the combination that
// looks correctly configured while behaving least like it. Build logs a
// warning when it sees both; see job.LeaseStore.DequeueLeased.
func WithResourceManager(m resource.Manager) Option {
	return func(eng *Engine) { eng.resources = m }
}

// WithWorkerCustomKeys narrows the custom resource keys this worker
// advertises at dequeue.
//
// The default — every custom key the manager has capacity for — is
// usually right. This exists to shrink it, so a worker draining a device
// can stop attracting work for it without being reconfigured. Keep the
// list a subset of the manager's custom capacity: dequeue matches custom
// keys by containment and never by quantity, so a key advertised here
// with no capacity behind it passes the store's filter and is then
// refused locally, on every attempt.
func WithWorkerCustomKeys(keys []string) Option {
	return func(eng *Engine) { eng.workerCustomKeys = slices.Clone(keys) }
}

// WithTracerProvider sets a custom OTel TracerProvider for the engine.
// When set, the tracing middleware uses this provider instead of the global one.
// If not set, the global otel.GetTracerProvider() is used.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(eng *Engine) {
		eng.tracerProvider = tp
	}
}

// WithMetricFactory sets the go-utils MetricFactory for engine-level metrics.
// Use fapp.Metrics() in forge applications to integrate with the forge metrics system.
// If not set, a default metrics collector is used.
func WithMetricFactory(factory gu.MetricFactory) Option {
	return func(eng *Engine) {
		eng.metricFactory = factory
	}
}

// WithStreamBroker enables the real-time stream broker for event pub/sub.
// The broker is automatically registered as an extension so it receives
// all job and workflow lifecycle events. It is required for DWP (Dispatch
// Wire Protocol) real-time subscriptions.
func WithStreamBroker(opts ...stream.BrokerOption) Option {
	return func(eng *Engine) {
		eng.enableBroker = true
		eng.brokerOpts = opts
	}
}

// Build creates an Engine from an existing Dispatcher.
// The Dispatcher's store must implement job.Store.
func Build(d *dispatch.Dispatcher, opts ...Option) (*Engine, error) {
	logger := d.Logger()
	st := d.Store()

	if st == nil {
		return nil, dispatch.ErrNoStore
	}

	// Type-assert the store to get the job.Store interface.
	js, ok := st.(job.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement job.Store")
	}

	// Type-assert the store to get the dlq.Store interface.
	ds, ok := st.(dlq.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement dlq.Store")
	}

	// Type-assert the store to get the workflow.Store interface.
	ws, ok := st.(workflow.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement workflow.Store")
	}

	// Type-assert the store to get the event.Store interface.
	es, ok := st.(event.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement event.Store")
	}

	// Type-assert the store to get the cron.Store interface.
	cs, ok := st.(cron.Store)
	if !ok {
		return nil, fmt.Errorf("dispatch: store does not implement cron.Store")
	}

	// Type-assert the store to get the cluster.Store interface.
	cls, ok := st.(cluster.Store)
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

	// Assemble the executor registry now that WithExecutor options have
	// populated extraExecutors, and before any definition is registered
	// or the runner is built, since both consult it.
	eng.buildExecutors()

	// Create stream broker if enabled (must be before pool so events flow).
	if eng.enableBroker {
		eng.broker = stream.NewBroker(logger, eng.brokerOpts...)
		eng.extensions.Register(eng.broker)
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

	// Build metrics middleware and observability extension using the metric factory.
	factory := eng.metricFactory
	if factory == nil {
		factory = gu.NewMetricsCollector("dispatch")
	}
	metricsMw := mw.MetricsWithFactory(factory)
	obsExt := observability.NewMetricsExtensionWithFactory(factory)
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

	// Create runner and pool.
	config := d.Config()
	runner := worker.NewRunner(
		eng.registry, eng.extensions, eng.jobStore, eng.dlqService,
		eng.bo, eng.executors, logger, allMws...,
	)

	poolOpts := []worker.PoolOption{
		worker.WithPoolConcurrency(config.Concurrency),
		worker.WithPoolQueues(config.Queues),
		worker.WithPollInterval(config.PollInterval),
		worker.WithHeartbeatInterval(config.HeartbeatInterval),
		worker.WithStaleJobThreshold(config.StaleJobThreshold),
	}
	if config.MaxPollInterval > 0 {
		poolOpts = append(poolOpts, worker.WithMaxPollInterval(config.MaxPollInterval))
	}
	// Per-call timeout. Zero leaves the worker package's
	// defaultStoreCallTimeout in place; non-zero overrides it.
	if config.WorkerStoreCallTimeout != 0 {
		poolOpts = append(poolOpts, worker.WithStoreCallTimeout(config.WorkerStoreCallTimeout))
	}

	// Create queue manager if queue configs were provided.
	if len(eng.queueConfigs) > 0 {
		eng.queueManager = queue.NewManager(eng.queueConfigs...)
		poolOpts = append(poolOpts, worker.WithQueueManager(eng.queueManager))
	}

	// Hand the pool the shared ledger. The timing check runs only when a
	// manager is present, because admission is the only thing that can
	// stall the fetcher: with no manager, admissionBudget hands back the
	// pool's own context and admit returns immediately.
	if eng.resources != nil {
		if err := checkReaperMargin(config); err != nil {
			return nil, err
		}

		poolOpts = append(poolOpts, worker.WithResourceManager(eng.resources))

		// Deliberately NOT seeding eng.workerCapacity from the ledger.
		// The manager describes THIS process; workerCapacity is the floor
		// of a fleet-wide check. Defaulting one from the other rescoped
		// the question to one process, and because cluster.Worker.Capacity
		// does not round-trip on four of the five backends, nothing could
		// raise it back to the fleet maximum afterwards — so a light API
		// worker rejected at enqueue every job bigger than itself. See
		// WithWorkerCapacity.
		//
		// No construction-time warning about leases is emitted here, and
		// the omission is deliberate rather than an oversight. The
		// combination that loses every guarantee this manager provides is
		// a pool that DEQUEUES through job.LeaseStore, and no such pool
		// exists in this tree yet — worker.Pool has exactly one dequeue
		// path and it is DequeueJobs. Warning on the only fact that is
		// observable today, "the store happens to implement LeaseStore",
		// would fire for every postgres, sqlite, mongo and redis
		// deployment that turns resources on, about something none of
		// them are doing. The guard is stated where the widening will
		// happen instead: job.LeaseStore.DequeueLeased and
		// WithResourceManager.
	}

	if len(eng.workerCustomKeys) > 0 {
		poolOpts = append(poolOpts, worker.WithWorkerCustomKeys(eng.workerCustomKeys))
	}

	eng.pool = worker.NewPool(
		eng.jobStore,
		runner,
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
	// Translate dispatch.Config cron tunables into scheduler options.
	// Zero values leave the scheduler's own defaults in place; only
	// non-zero values override.
	var schedOpts []cron.SchedulerOption
	if config.CronTickInterval > 0 {
		schedOpts = append(schedOpts, cron.WithTickInterval(config.CronTickInterval))
	}
	if config.CronLeaderTTL > 0 {
		schedOpts = append(schedOpts, cron.WithLeaderTTL(config.CronLeaderTTL))
	}
	if config.CronRefreshInterval > 0 {
		schedOpts = append(schedOpts, cron.WithCronRefreshInterval(config.CronRefreshInterval))
	}
	if config.CronLockTTL > 0 {
		schedOpts = append(schedOpts, cron.WithLockTTL(config.CronLockTTL))
	}
	if config.CronStoreCallTimeout != 0 {
		schedOpts = append(schedOpts, cron.WithSchedulerStoreCallTimeout(config.CronStoreCallTimeout))
	}
	eng.scheduler = cron.NewScheduler(cs, cls, enqueueFunc, eng.extensions, eng.pool.WorkerID(), logger, schedOpts...)

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
		Capacity:    eng.workerCapacity.Clone(),
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}
	if regErr := cls.RegisterWorker(context.Background(), w); regErr != nil {
		logger.Warn("failed to register worker in cluster store", log.String("error", regErr.Error()))
	}

	return eng, nil
}

// Register registers a typed job definition with the engine.
//
// Use RegisterChecked when the definition declares artifact inputs and
// you want the declaration validated against the staging budget.
func Register[T any](eng *Engine, def *job.Definition[T]) {
	job.RegisterDefinition(eng.registry, def)
}

// RegisterChecked registers a definition and validates its artifact
// declarations and execution policy, so a job that could never be staged
// or could never be isolated as it requires fails here rather than on
// every worker that picks it up.
func RegisterChecked[T any](eng *Engine, def *job.Definition[T]) error {
	if err := eng.ValidateArtifactInputs(def.Name, def.Opts.Inputs); err != nil {
		return err
	}
	if err := eng.checkExecutionPolicy(def.Name, def.Opts.Execution); err != nil {
		return err
	}

	job.RegisterDefinition(eng.registry, def)

	return nil
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

	if err := eng.applyBindings(ctx, j, jobOpts.Bindings); err != nil {
		return nil, err
	}

	// After applyBindings, so the bindings this reads are already
	// validated; before EnqueueJob, so an unschedulable job never
	// reaches the store.
	if err := eng.resolveResources(ctx, j, jobOpts); err != nil {
		return nil, err
	}

	if err := eng.jobStore.EnqueueJob(ctx, j); err != nil {
		return nil, err
	}

	// Nudge the local worker pool so in-process enqueues are picked up
	// immediately instead of waiting out the idle poll backoff.
	if eng.pool != nil {
		eng.pool.Wake()
	}

	eng.extensions.EmitJobEnqueued(ctx, j)
	return j, nil
}

// Health checks the health of the engine by pinging the dispatcher's store.
func (eng *Engine) Health(ctx context.Context) error {
	return eng.d.Store().Ping(ctx)
}

// Start begins job processing by starting the worker pool and cron scheduler.
// It also resumes any workflow runs left in "running" state (crash recovery).
func (eng *Engine) Start(ctx context.Context) error {
	// Resume any interrupted workflow runs (best-effort, non-fatal).
	if resumeErr := eng.wfRunner.ResumeAll(ctx); resumeErr != nil {
		eng.logger.Warn("failed to resume workflow runs",
			log.String("error", resumeErr.Error()),
		)
	}

	// Start the cron scheduler before the pool so leadership can be acquired.
	if err := eng.scheduler.Start(ctx); err != nil {
		return fmt.Errorf("start cron scheduler: %w", err)
	}

	if err := eng.d.Start(ctx); err != nil {
		return err
	}

	// Stores that support push notifications (store.WakeNotifier, e.g.
	// Postgres LISTEN/NOTIFY) wake the worker pool on cross-instance
	// enqueues so jobs are picked up without waiting out the idle poll
	// backoff. Best-effort: polling remains the correctness mechanism.
	if wn, ok := eng.jobStore.(store.WakeNotifier); ok {
		stop, err := wn.StartWakeListener(ctx, eng.pool.Wake)
		if err != nil {
			eng.logger.Warn("wake listener unavailable; relying on polling",
				log.String("error", err.Error()),
			)
		} else {
			eng.wakeStop = stop
		}
	}

	return nil
}

// Stop gracefully shuts down the engine.
func (eng *Engine) Stop(ctx context.Context) error {
	// Stop the store wake listener first; it only reduces poll latency.
	if eng.wakeStop != nil {
		eng.wakeStop()
		eng.wakeStop = nil
	}

	// Deregister this worker from the cluster.
	if err := eng.clusterStore.DeregisterWorker(ctx, eng.pool.WorkerID()); err != nil {
		eng.logger.Warn("failed to deregister worker", log.String("error", err.Error()))
	}

	// Stop the cron scheduler.
	if err := eng.scheduler.Stop(ctx); err != nil {
		eng.logger.Error("cron scheduler stop error", log.String("error", err.Error()))
	}

	stopErr := eng.d.Stop(ctx)

	// Close the executors last. The dispatcher stop above drains the worker
	// pool, so no attempt is still running through a rung when its resources
	// go away. In-process Close is a no-op; an out-of-process rung releases
	// its clients and child processes here or leaks them.
	eng.closeExecutors()

	return stopErr
}

// closeExecutors releases every configured executor's resources, logging
// failures rather than propagating them: shutdown continues regardless.
func (eng *Engine) closeExecutors() {
	if eng.executors == nil {
		return
	}

	for _, e := range eng.executors.Executors() {
		if err := e.Close(); err != nil {
			eng.logger.Warn("executor close failed",
				log.String("executor", e.Name()),
				log.String("error", err.Error()),
			)
		}
	}
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

// ClusterStore returns the cluster store for worker management.
func (eng *Engine) ClusterStore() cluster.Store { return eng.clusterStore }

// WorkerID returns this engine's unique worker identifier.
func (eng *Engine) WorkerID() id.WorkerID { return eng.pool.WorkerID() }

// StreamBroker returns the real-time event broker, or nil if not enabled.
func (eng *Engine) StreamBroker() *stream.Broker { return eng.broker }

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

	// The scheduler caches its cron list; make the new entry visible
	// before the next scheduled refresh.
	if eng.scheduler != nil {
		eng.scheduler.InvalidateCronCache()
	}

	eng.logger.Info("cron registered",
		log.String("name", def.Name),
		log.String("schedule", def.Schedule),
		log.String("job_name", def.JobName),
		log.Time("next_run_at", next),
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
