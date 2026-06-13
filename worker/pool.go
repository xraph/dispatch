package worker

import (
	"context"
	"sync"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// QueueManager controls per-queue and per-tenant rate limiting and
// concurrency. The worker pool calls Acquire before executing a dequeued
// job and Release after execution completes.
type QueueManager interface {
	// Acquire checks rate limits and concurrency for the queue/tenant
	// combination. Returns true if the job is allowed to proceed.
	Acquire(queue, tenantID string) bool
	// Release decrements the active count for the queue/tenant pair.
	Release(queue, tenantID string)
}

// defaultStoreCallTimeout caps how long a single store roundtrip
// (DequeueJobs, HeartbeatJob, ReapStaleJobs, UpdateJob) is allowed
// to run before the worker abandons it. Without this, a slow Mongo /
// Postgres session selection would let dequeue calls stack on the
// shared driver pool until every connection is checked out — which
// is exactly the cascade that produces "context deadline exceeded"
// floods at boot. 5 seconds is generous enough for a healthy
// roundtrip and tight enough that 10 workers polling every second
// can't pile up more than 50 in-flight calls at once.
const defaultStoreCallTimeout = 5 * time.Second

// Pool manages a set of concurrent worker goroutines fed by a single
// fetcher that polls the store for jobs and executes them through the
// Executor.
type Pool struct {
	store        job.Store
	executor     *Executor
	extensions   *ext.Registry
	concurrency  int
	queues       []string
	pollInterval time.Duration

	// maxPollInterval caps the fetcher's idle backoff. Empty polls double
	// the poll interval from pollInterval up to this value so an idle pool
	// doesn't issue a dequeue (a write command on MongoDB, an UPDATE on
	// SQL) every pollInterval. Dequeued work or Wake resets the cadence.
	maxPollInterval time.Duration

	workerID id.WorkerID
	logger   log.Logger

	// Heartbeat / reaper configuration.
	heartbeatInterval time.Duration
	staleJobThreshold time.Duration

	// storeCallTimeout caps how long a single store call (dequeue,
	// heartbeat, reap, update) may hold a driver-pool connection
	// before being abandoned. Zero means use defaultStoreCallTimeout;
	// negative disables the timeout (legacy behaviour, only useful
	// for tests that don't want their fakes wrapped in a deadline).
	storeCallTimeout time.Duration

	// Queue manager (optional).
	queueManager QueueManager

	stopCh     chan struct{}
	wakeCh     chan struct{}      // nudges the fetcher out of its idle backoff
	jobCh      chan *job.Job      // hand-off from the fetcher to the workers
	slots      chan struct{}      // free-worker tokens; capacity == concurrency
	cancelCtx  context.Context    // Cancelled on Stop to interrupt in-flight store operations
	cancelFunc context.CancelFunc // Cancels cancelCtx
	wg         sync.WaitGroup
	mu         sync.Mutex
	running    bool
	activeJobs map[string]context.CancelFunc
	activeMu   sync.Mutex
}

// PoolOption configures a Pool.
type PoolOption func(*Pool)

// WithPoolConcurrency sets the number of concurrent worker goroutines.
func WithPoolConcurrency(n int) PoolOption {
	return func(p *Pool) { p.concurrency = n }
}

// WithPoolQueues sets the queues the pool will poll.
func WithPoolQueues(queues []string) PoolOption {
	return func(p *Pool) { p.queues = queues }
}

// WithPollInterval sets how often workers poll for new jobs.
func WithPollInterval(d time.Duration) PoolOption {
	return func(p *Pool) { p.pollInterval = d }
}

// WithMaxPollInterval caps the fetcher's idle backoff. Empty polls double
// the poll interval up to this value; dequeued work or Wake resets it.
func WithMaxPollInterval(d time.Duration) PoolOption {
	return func(p *Pool) { p.maxPollInterval = d }
}

// WithHeartbeatInterval sets how often the pool sends heartbeats for
// active jobs. A zero value disables heartbeats.
func WithHeartbeatInterval(d time.Duration) PoolOption {
	return func(p *Pool) { p.heartbeatInterval = d }
}

// WithStaleJobThreshold sets the threshold after which running jobs
// without a heartbeat are considered stale and reaped. A zero value
// disables stale job reaping.
func WithStaleJobThreshold(d time.Duration) PoolOption {
	return func(p *Pool) { p.staleJobThreshold = d }
}

// WithQueueManager sets the queue manager for rate limiting and
// concurrency control.
func WithQueueManager(m QueueManager) PoolOption {
	return func(p *Pool) { p.queueManager = m }
}

// WithStoreCallTimeout caps a single store roundtrip. Pass a positive
// duration to override defaultStoreCallTimeout, zero to leave the
// default in place, or a negative value to disable the timeout
// entirely. Disabling is only intended for unit tests.
func WithStoreCallTimeout(d time.Duration) PoolOption {
	return func(p *Pool) { p.storeCallTimeout = d }
}

// NewPool creates a worker pool.
func NewPool(
	store job.Store,
	executor *Executor,
	extensions *ext.Registry,
	logger log.Logger,
	opts ...PoolOption,
) *Pool {
	p := &Pool{
		store:           store,
		executor:        executor,
		extensions:      extensions,
		concurrency:     10,
		queues:          []string{"default"},
		pollInterval:    time.Second,
		maxPollInterval: 30 * time.Second,
		workerID:        id.NewWorkerID(),
		logger:          logger,
		stopCh:          make(chan struct{}),
		wakeCh:          make(chan struct{}, 1),
		activeJobs:      make(map[string]context.CancelFunc),
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.maxPollInterval < p.pollInterval {
		p.maxPollInterval = p.pollInterval
	}
	return p
}

// Wake nudges the fetcher to poll for jobs immediately, resetting any idle
// backoff. It is non-blocking and safe to call from any goroutine, including
// before Start. Call it after enqueuing a job in-process so the job is
// picked up without waiting out the idle poll interval.
func (p *Pool) Wake() {
	select {
	case p.wakeCh <- struct{}{}:
	default:
	}
}

// WorkerID returns the pool's unique worker identifier.
func (p *Pool) WorkerID() id.WorkerID { return p.workerID }

// Start launches the worker goroutines. It returns immediately.
func (p *Pool) Start(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return nil
	}
	p.running = true
	p.cancelCtx, p.cancelFunc = context.WithCancel(context.Background())

	p.logger.Info("worker pool starting",
		log.String("worker_id", p.workerID.String()),
		log.Int("concurrency", p.concurrency),
		log.Any("queues", p.queues),
	)

	// One fetcher claims jobs in batches sized to the free worker slots;
	// concurrency worker goroutines execute them. A single poller issues
	// one DequeueJobs call per cycle instead of `concurrency` concurrent
	// calls, which kept idle pools writing to the store every second and
	// could exhaust the shared driver pool on its own.
	p.jobCh = make(chan *job.Job)
	p.slots = make(chan struct{}, p.concurrency)
	for range p.concurrency {
		p.slots <- struct{}{}
	}

	for range p.concurrency {
		p.wg.Add(1)
		go p.workerLoop()
	}
	p.wg.Add(1)
	go p.fetchLoop()

	// Launch heartbeat goroutine if configured.
	if p.heartbeatInterval > 0 {
		p.wg.Add(1)
		go p.heartbeatLoop()
	}

	// Launch reaper goroutine if configured.
	if p.staleJobThreshold > 0 {
		p.wg.Add(1)
		go p.reaperLoop()
	}

	return nil
}

// Stop signals all workers to stop and waits for them to finish.
// If the context has a deadline, active jobs are cancelled when time runs out.
func (p *Pool) Stop(ctx context.Context) error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return nil
	}
	p.running = false
	p.mu.Unlock()

	p.logger.Info("worker pool stopping", log.String("worker_id", p.workerID.String()))

	// Signal all workers to stop and cancel in-flight store operations.
	close(p.stopCh)
	if p.cancelFunc != nil {
		p.cancelFunc()
	}

	// Wait for completion or context deadline.
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("worker pool stopped gracefully")
	case <-ctx.Done():
		p.logger.Warn("worker pool shutdown timed out, cancelling active jobs")
		p.cancelActiveJobs()
		p.wg.Wait()
	}

	return nil
}

// callCtx wraps p.cancelCtx with the configured per-call timeout so a
// stalled driver session can't pin a pool connection indefinitely.
// Returns the unwrapped cancelCtx + a no-op cancel when the timeout
// is disabled (negative) so callers can use defer cancel() uniformly.
func (p *Pool) callCtx() (context.Context, context.CancelFunc) {
	if p.storeCallTimeout < 0 {
		return p.cancelCtx, func() {}
	}
	d := p.storeCallTimeout
	if d == 0 {
		d = defaultStoreCallTimeout
	}
	return context.WithTimeout(p.cancelCtx, d)
}

// fetchLoop is the single poller. Each cycle it reserves every free worker
// slot, dequeues at most that many jobs in ONE store call, and hands them to
// workers. Empty polls back off exponentially up to maxPollInterval; Wake or
// dequeued work resets the cadence to pollInterval.
func (p *Pool) fetchLoop() {
	defer p.wg.Done()

	interval := p.pollInterval

	for {
		// Block until at least one worker is free so we never claim a job
		// we cannot immediately run, then grab the remaining free slots
		// for the batch.
		select {
		case <-p.stopCh:
			return
		case <-p.cancelCtx.Done():
			return
		case <-p.slots:
		}
		held := 1
		for held < p.concurrency {
			select {
			case <-p.slots:
				held++
				continue
			default:
			}
			break
		}

		dqCtx, dqCancel := p.callCtx()
		jobs, err := p.store.DequeueJobs(dqCtx, p.queues, held)
		dqCancel()
		if err != nil {
			p.releaseSlots(held)
			if p.cancelCtx.Err() != nil {
				return // Clean exit during shutdown
			}
			p.logger.Error("dequeue error", log.String("error", err.Error()))
			// Back off on errors too; hot-spinning at pollInterval only
			// piles onto a struggling store.
			interval = min(interval*2, p.maxPollInterval)
			woken, ok := p.wait(interval)
			if !ok {
				return
			}
			if woken {
				interval = p.pollInterval
			}
			continue
		}

		for _, j := range jobs {
			// Check queue/tenant rate limit and concurrency.
			if p.queueManager != nil && !p.queueManager.Acquire(j.Queue, j.ScopeOrgID) {
				p.requeueRateLimited(j)
				continue
			}

			select {
			case p.jobCh <- j:
				held-- // The worker now owns this slot.
			case <-p.stopCh:
				p.requeueUndispatched(j)
				p.releaseSlots(held)
				return
			case <-p.cancelCtx.Done():
				p.requeueUndispatched(j)
				p.releaseSlots(held)
				return
			}
		}
		p.releaseSlots(held)

		if len(jobs) > 0 {
			// There may be more ready work; poll again immediately.
			interval = p.pollInterval
			continue
		}

		interval = min(interval*2, p.maxPollInterval)
		woken, ok := p.wait(interval)
		if !ok {
			return
		}
		if woken {
			interval = p.pollInterval
		}
	}
}

// wait blocks for d, a Wake nudge, or shutdown. woken reports a Wake nudge
// (the caller should reset its backoff); ok is false when the pool is
// stopping.
func (p *Pool) wait(d time.Duration) (woken, ok bool) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-p.stopCh:
		return false, false
	case <-p.cancelCtx.Done():
		return false, false
	case <-p.wakeCh:
		return true, true
	case <-timer.C:
		return false, true
	}
}

// releaseSlots returns n unused worker tokens. Token conservation (one per
// idle worker in the channel, one per job in flight, the rest held briefly
// by the fetcher) keeps this from ever blocking.
func (p *Pool) releaseSlots(n int) {
	for range n {
		p.slots <- struct{}{}
	}
}

// requeueRateLimited returns a job the queue manager refused to pending
// with a small delay.
func (p *Pool) requeueRateLimited(j *job.Job) {
	j.State = job.StatePending
	j.RunAt = time.Now().Add(p.pollInterval)
	updCtx, updCancel := p.callCtx()
	updateErr := p.store.UpdateJob(updCtx, j)
	updCancel()
	if updateErr != nil && p.cancelCtx.Err() == nil {
		p.logger.Error("failed to re-enqueue rate-limited job",
			log.String("job_id", j.ID.String()),
			log.String("error", updateErr.Error()),
		)
	}
}

// requeueUndispatched best-effort returns a claimed-but-not-started job to
// pending during shutdown so it isn't stranded in the running state with no
// heartbeat. Uses a fresh context: the pool's own contexts are already done.
func (p *Pool) requeueUndispatched(j *job.Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	j.State = job.StatePending
	j.RunAt = time.Now().UTC()
	j.StartedAt = nil
	if err := p.store.UpdateJob(ctx, j); err != nil {
		p.logger.Warn("failed to return undispatched job to pending",
			log.String("job_id", j.ID.String()),
			log.String("error", err.Error()),
		)
	}
}

// workerLoop executes jobs handed off by the fetcher, returning its slot
// token after each job.
func (p *Pool) workerLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return
		case <-p.cancelCtx.Done():
			return
		case j := <-p.jobCh:
			p.runJob(j)
			p.slots <- struct{}{}
		}
	}
}

func (p *Pool) runJob(j *job.Job) {
	p.extensions.EmitJobStarted(p.cancelCtx, j)

	ctx, cancel := context.WithCancel(p.cancelCtx)
	p.trackJob(j.ID.String(), cancel)

	execErr := p.executor.Execute(ctx, j)
	if execErr != nil {
		p.logger.Debug("job execution failed",
			log.String("job_id", j.ID.String()),
			log.String("job_name", j.Name),
			log.String("error", execErr.Error()),
		)
	}

	p.untrackJob(j.ID.String())
	cancel()

	// Release the queue/tenant slot.
	if p.queueManager != nil {
		p.queueManager.Release(j.Queue, j.ScopeOrgID)
	}
}

// heartbeatLoop periodically sends heartbeats for all active jobs.
func (p *Pool) heartbeatLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.sendHeartbeats()
		}
	}
}

func (p *Pool) sendHeartbeats() {
	p.activeMu.Lock()
	jobIDs := make([]string, 0, len(p.activeJobs))
	for jobID := range p.activeJobs {
		jobIDs = append(jobIDs, jobID)
	}
	p.activeMu.Unlock()

	for _, jobIDStr := range jobIDs {
		parsedID, parseErr := id.ParseJobID(jobIDStr)
		if parseErr != nil {
			p.logger.Warn("heartbeat: invalid job id", log.String("job_id", jobIDStr))
			continue
		}
		hbCtx, hbCancel := p.callCtx()
		err := p.store.HeartbeatJob(hbCtx, parsedID, p.workerID)
		hbCancel()
		if err != nil {
			p.logger.Warn("heartbeat failed",
				log.String("job_id", jobIDStr),
				log.String("error", err.Error()),
			)
		}
	}
}

// reaperLoop periodically reaps stale jobs whose heartbeat has expired.
func (p *Pool) reaperLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.staleJobThreshold)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.reapStaleJobs()
		}
	}
}

func (p *Pool) reapStaleJobs() {
	reapCtx, reapCancel := p.callCtx()
	stale, err := p.store.ReapStaleJobs(reapCtx, p.staleJobThreshold)
	reapCancel()
	if err != nil {
		p.logger.Error("reap stale jobs error", log.String("error", err.Error()))
		return
	}

	reset := 0
	for _, j := range stale {
		j.State = job.StatePending
		j.RunAt = time.Now().UTC()
		j.WorkerID = id.WorkerID{} // Clear the worker assignment.
		j.HeartbeatAt = nil
		j.StartedAt = nil

		updCtx, updCancel := p.callCtx()
		updateErr := p.store.UpdateJob(updCtx, j)
		updCancel()
		if updateErr != nil {
			p.logger.Error("reap: failed to reset stale job",
				log.String("job_id", j.ID.String()),
				log.String("error", updateErr.Error()),
			)
			continue
		}

		reset++
		p.logger.Info("reaped stale job",
			log.String("job_id", j.ID.String()),
			log.String("job_name", j.Name),
		)
	}

	// The reset jobs are pending again; wake the fetcher so they are
	// retried now rather than after the idle poll backoff.
	if reset > 0 {
		p.Wake()
	}
}

func (p *Pool) trackJob(jobID string, cancel context.CancelFunc) {
	p.activeMu.Lock()
	p.activeJobs[jobID] = cancel
	p.activeMu.Unlock()
}

func (p *Pool) untrackJob(jobID string) {
	p.activeMu.Lock()
	delete(p.activeJobs, jobID)
	p.activeMu.Unlock()
}

func (p *Pool) cancelActiveJobs() {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	for jobID, cancel := range p.activeJobs {
		p.logger.Warn("cancelling active job", log.String("job_id", jobID))
		cancel()
	}
}
