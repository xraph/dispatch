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

// Pool manages a set of concurrent worker goroutines that poll for
// jobs and execute them through the Executor.
type Pool struct {
	store        job.Store
	executor     *Executor
	extensions   *ext.Registry
	concurrency  int
	queues       []string
	pollInterval time.Duration
	workerID     id.WorkerID
	logger       log.Logger

	// Heartbeat / reaper configuration.
	heartbeatInterval time.Duration
	staleJobThreshold time.Duration

	// Queue manager (optional).
	queueManager QueueManager

	stopCh     chan struct{}
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

// NewPool creates a worker pool.
func NewPool(
	store job.Store,
	executor *Executor,
	extensions *ext.Registry,
	logger log.Logger,
	opts ...PoolOption,
) *Pool {
	p := &Pool{
		store:        store,
		executor:     executor,
		extensions:   extensions,
		concurrency:  10,
		queues:       []string{"default"},
		pollInterval: time.Second,
		workerID:     id.NewWorkerID(),
		logger:       logger,
		stopCh:       make(chan struct{}),
		activeJobs:   make(map[string]context.CancelFunc),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
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

	for range p.concurrency {
		p.wg.Add(1)
		go p.dequeueLoop()
	}

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

// dequeueLoop is run by each worker goroutine.
func (p *Pool) dequeueLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return
		case <-p.cancelCtx.Done():
			return
		default:
		}

		jobs, err := p.store.DequeueJobs(p.cancelCtx, p.queues, 1)
		if err != nil {
			if p.cancelCtx.Err() != nil {
				return // Clean exit during shutdown
			}
			p.logger.Error("dequeue error", log.String("error", err.Error()))
			p.sleep()
			continue
		}

		if len(jobs) == 0 {
			p.sleep()
			continue
		}

		j := jobs[0]

		// Check queue/tenant rate limit and concurrency.
		if p.queueManager != nil && !p.queueManager.Acquire(j.Queue, j.ScopeOrgID) {
			// Rate limited — return the job to pending with a small delay.
			j.State = job.StatePending
			j.RunAt = time.Now().Add(p.pollInterval)
			if updateErr := p.store.UpdateJob(p.cancelCtx, j); updateErr != nil {
				if p.cancelCtx.Err() != nil {
					return
				}
				p.logger.Error("failed to re-enqueue rate-limited job",
					log.String("job_id", j.ID.String()),
					log.String("error", updateErr.Error()),
				)
			}
			p.sleep()
			continue
		}

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
		if err := p.store.HeartbeatJob(p.cancelCtx, parsedID, p.workerID); err != nil {
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
	stale, err := p.store.ReapStaleJobs(p.cancelCtx, p.staleJobThreshold)
	if err != nil {
		p.logger.Error("reap stale jobs error", log.String("error", err.Error()))
		return
	}

	for _, j := range stale {
		j.State = job.StatePending
		j.RunAt = time.Now().UTC()
		j.WorkerID = id.WorkerID{} // Clear the worker assignment.
		j.HeartbeatAt = nil
		j.StartedAt = nil

		if updateErr := p.store.UpdateJob(p.cancelCtx, j); updateErr != nil {
			p.logger.Error("reap: failed to reset stale job",
				log.String("job_id", j.ID.String()),
				log.String("error", updateErr.Error()),
			)
			continue
		}

		p.logger.Info("reaped stale job",
			log.String("job_id", j.ID.String()),
			log.String("job_name", j.Name),
		)
	}
}

func (p *Pool) sleep() {
	select {
	case <-time.After(p.pollInterval):
	case <-p.stopCh:
	case <-p.cancelCtx.Done():
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
