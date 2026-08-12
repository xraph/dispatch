// Package worker provides the job execution engine — a Runner that
// orchestrates a single job attempt through middleware and an exec.Executor,
// and a Pool that manages concurrent worker goroutines polling for jobs.
package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
)

// maxLaunchAttempts bounds how many times one job may fail to launch on this
// worker before it is sent to the dead letter queue.
//
// A launch failure deliberately does not consume the retry budget: an image
// that will not pull says nothing about the work. But nothing else bounds it
// either — the job returns to pending at the first step of the backoff curve
// and is dequeued again about a second later, forever, costing a store write
// and a worker slot each time. Five attempts rides out the case this
// leniency exists for, a job reaching a worker that has not been deployed
// its handler yet, and stops a genuinely undeployable job from spinning.
const maxLaunchAttempts = 5

// launchAttemptTTL is how long a job's launch count survives without being
// touched.
//
// The count is normally deleted the moment the job succeeds, retries, or is
// dead-lettered here. A job requeued by this worker and then picked up by a
// worker that does have its handler never comes back for that deletion, so
// entries expire as well and the map cannot grow with every such job for the
// life of the process.
const launchAttemptTTL = 30 * time.Minute

// Runner executes a single job attempt: it selects an executor from the
// job's policy, runs the attempt through the middleware chain, then
// handles retry logic, DLQ push, state updates, and lifecycle events.
//
// Runner orchestrates the attempt. It does not itself invoke the handler —
// that is exec.Executor's job, which is what lets the same attempt run
// in-process or in a pod without this file changing.
type Runner struct {
	registry   *job.Registry
	extensions *ext.Registry
	store      job.Store
	dlqService *dlq.Service
	backoff    backoff.Strategy
	executors  *exec.Registry
	mw         middleware.Middleware
	logger     log.Logger

	// launchMu guards launches. One Runner is shared by every worker
	// goroutine in the pool, so the counter is mutex-guarded rather than
	// living on the job value.
	launchMu sync.Mutex
	launches map[string]launchAttempt
}

// launchAttempt is one job's running launch-failure count on this worker.
type launchAttempt struct {
	count int
	seen  time.Time
}

// NewRunner creates a Runner with the given dependencies.
//
// A nil executors registry means handlers are called directly, which is
// the behaviour the deprecated NewExecutor preserves.
func NewRunner(
	registry *job.Registry,
	extensions *ext.Registry,
	store job.Store,
	dlqService *dlq.Service,
	bo backoff.Strategy,
	executors *exec.Registry,
	logger log.Logger,
	mws ...middleware.Middleware,
) *Runner {
	return &Runner{
		registry:   registry,
		extensions: extensions,
		store:      store,
		dlqService: dlqService,
		backoff:    bo,
		executors:  executors,
		mw:         middleware.Chain(mws...),
		logger:     logger,
		launches:   make(map[string]launchAttempt),
	}
}

// Reclaim asks every configured executor to release sandboxes this worker
// leaked across a restart. The pool calls it once at startup.
//
// Failures are joined rather than fatal: a rung that cannot sweep should not
// stop the worker from running the jobs it can still execute.
func (r *Runner) Reclaim(ctx context.Context, workerID id.WorkerID) error {
	if r.executors == nil {
		return nil
	}

	var errs []error
	for _, e := range r.executors.Executors() {
		if err := e.Reclaim(ctx, workerID); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", e.Name(), err))
		}
	}

	return errors.Join(errs...)
}

// Close releases every configured executor's own resources.
//
// An engine closes its own registry when it stops, since it outlives the
// pool; this is the equivalent for a caller that assembles a Runner itself.
// Call it after the pool has finished its in-flight attempts.
func (r *Runner) Close() error {
	if r.executors == nil {
		return nil
	}

	var errs []error
	for _, e := range r.executors.Executors() {
		if err := e.Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", e.Name(), err))
		}
	}

	return errors.Join(errs...)
}

// Execute runs a job through the middleware chain and its executor.
// On success: marks completed, emits JobCompleted.
// On failure with retries remaining: marks retrying with backoff, emits JobRetrying.
// On failure with retries exhausted: marks failed, pushes to DLQ, emits JobFailed + JobDLQ.
func (r *Runner) Execute(ctx context.Context, j *job.Job) error {
	terminal, err := r.terminalFor(j)
	if err != nil {
		return err
	}

	start := time.Now()
	execErr := r.mw(ctx, j, terminal)
	elapsed := time.Since(start)

	now := time.Now().UTC()
	j.UpdatedAt = now

	if execErr != nil {
		return r.handleFailure(ctx, j, execErr, now)
	}

	return r.handleSuccess(ctx, j, now, elapsed)
}

// terminalFor builds the innermost handler for this job.
//
// Everything cross-cutting — recover, tracing, metrics, logging, scope,
// timeout, and artifact staging — wraps this closure, which is precisely
// why staging keeps running in the worker process and an out-of-process
// handler receives a directory rather than storage credentials.
func (r *Runner) terminalFor(j *job.Job) (middleware.Handler, error) {
	if r.executors == nil {
		handler, ok := r.registry.Get(j.Name)
		if !ok {
			return nil, fmt.Errorf("no handler registered for job %q", j.Name)
		}

		return func(ctx context.Context) error {
			return handler(ctx, j.Payload)
		}, nil
	}

	policy := r.registry.Policy(j.Name)
	executor, err := r.executors.Select(policy)
	if err != nil {
		return nil, fmt.Errorf("dispatch/worker: select executor for job %q: %w", j.Name, err)
	}

	return func(ctx context.Context) error {
		res, runErr := executor.Run(ctx, r.request(j, policy))
		if runErr != nil {
			// Run reserves its error return for launch failures: the handler
			// never ran, so the retry budget must not pay for it.
			//
			// An invalid request is the exception. It is a caller programming
			// error that will fail identically on every attempt, and since a
			// launch failure never increments RetryCount it would requeue
			// forever. Fail it permanently instead.
			if errors.Is(runErr, exec.ErrInvalidRequest) {
				return fmt.Errorf("%w: %w", dispatch.ErrPermanent, runErr)
			}

			return &exec.Error{Status: exec.StatusLaunchFailed, Msg: runErr.Error()}
		}

		return res.Err()
	}, nil
}

// request builds the execution request for one attempt.
func (r *Runner) request(j *job.Job, policy exec.Policy) *exec.Request {
	req := &exec.Request{
		JobID:   j.ID,
		Name:    j.Name,
		Payload: j.Payload,
		Attempt: j.RetryCount,
		// The fingerprint states which handler set this attempt was built
		// for. An out-of-process rung compares it against the set it
		// actually links, so a stale image running an old handler is a
		// launch failure rather than a silent wrong answer. Computed per
		// attempt rather than cached: registration is a startup activity
		// in practice, but a stale fingerprint would disable exactly the
		// check it exists to make.
		Fingerprint: exec.Fingerprint(r.registry.Names()),
		Policy:      policy,
		ScopeAppID:  j.ScopeAppID,
		ScopeOrgID:  j.ScopeOrgID,
	}
	if j.Timeout > 0 {
		req.Deadline = time.Now().Add(j.Timeout)
	}

	return req
}

// handleSuccess marks the job as completed and emits the lifecycle event.
func (r *Runner) handleSuccess(ctx context.Context, j *job.Job, now time.Time, elapsed time.Duration) error {
	r.forgetLaunchFailures(j.ID.String())

	j.State = job.StateCompleted
	j.CompletedAt = &now

	if updateErr := r.store.UpdateJob(ctx, j); updateErr != nil {
		r.logger.Error("failed to update job after success",
			log.String("job_id", j.ID.String()),
			log.String("job_name", j.Name),
			log.String("error", updateErr.Error()),
		)
		return updateErr
	}

	r.extensions.EmitJobCompleted(ctx, j, elapsed)
	return nil
}

// handleFailure either requeues the job or increments the retry counter and
// retries, depending on whether the failure was the work's fault.
//
// A failure marked dispatch.ErrPermanent skips the remaining attempts. The
// retry schedule exists to outlast a transient fault, and spending it on a
// condition that cannot change wastes worker time proportional to the backoff
// curve: a job whose input was deleted would otherwise rediscover that the
// object is still gone once per attempt, minutes to hours apart, before
// arriving at the same dead letter queue it could have reached immediately.
func (r *Runner) handleFailure(ctx context.Context, j *job.Job, handlerErr error, now time.Time) error {
	j.LastError = handlerErr.Error()

	var execErr *exec.Error
	isExecErr := errors.As(handlerErr, &execErr)

	// A launch failure means the handler never ran: an image that would
	// not pull, an exhausted quota, a missing runtime. Consuming the
	// retry budget for it would let one bad node send healthy work to
	// the DLQ, so the job is requeued without counting the attempt —
	// up to a bound, since nothing else stops a job that can never launch
	// from requeueing itself forever.
	if isExecErr && !execErr.Status.CountsAgainstRetries() {
		if n := r.recordLaunchFailure(j.ID.String(), now); n > maxLaunchAttempts {
			capped := fmt.Errorf("%w: job %s failed to launch %d times: %s",
				dispatch.ErrPermanent, j.Name, n, j.LastError)
			j.LastError = capped.Error()

			return r.sendToDLQ(ctx, j, capped)
		}

		return r.requeueAfterLaunchFailure(ctx, j, now)
	}

	j.RetryCount++

	// Permanence reaches here two ways. In-process the handler's own error
	// chain survives, so errors.Is finds the sentinel. Out of process it
	// cannot, so the rung sets a flag on the Result instead. Both mean the
	// retry schedule would only rediscover the same condition.
	if errors.Is(handlerErr, dispatch.ErrPermanent) || (isExecErr && execErr.Permanent) {
		r.logger.Info("job failed permanently, skipping remaining retries",
			log.String("job_id", j.ID.String()),
			log.String("job_name", j.Name),
			log.Int("retry_count", j.RetryCount),
			log.Int("max_retries", j.MaxRetries),
			log.String("error", handlerErr.Error()),
		)

		return r.sendToDLQ(ctx, j, handlerErr)
	}

	if j.RetryCount <= j.MaxRetries {
		return r.scheduleRetry(ctx, j, now)
	}

	return r.sendToDLQ(ctx, j, handlerErr)
}

// recordLaunchFailure counts one launch failure for a job and returns the
// running total. It is safe for concurrent use.
func (r *Runner) recordLaunchFailure(jobID string, now time.Time) int {
	r.launchMu.Lock()
	defer r.launchMu.Unlock()

	// Expire stale entries first. A job requeued here and then run
	// elsewhere never returns for its deletion, and the sweep is what keeps
	// the map bounded in that case. It runs only on launch failures, which
	// are rare, over a map that holds only jobs currently failing to launch.
	for key, attempt := range r.launches {
		if now.Sub(attempt.seen) > launchAttemptTTL {
			delete(r.launches, key)
		}
	}

	attempt := r.launches[jobID]
	attempt.count++
	attempt.seen = now
	r.launches[jobID] = attempt

	return attempt.count
}

// forgetLaunchFailures drops a job's launch count. Every path that ends an
// attempt for good — success, retry, dead letter — calls it, so the map
// holds only jobs that are currently failing to launch.
func (r *Runner) forgetLaunchFailures(jobID string) {
	r.launchMu.Lock()
	defer r.launchMu.Unlock()
	delete(r.launches, jobID)
}

// requeueAfterLaunchFailure returns the job to pending with a backoff
// delay derived from the retry count without advancing it.
func (r *Runner) requeueAfterLaunchFailure(ctx context.Context, j *job.Job, now time.Time) error {
	delay := r.backoff.Delay(j.RetryCount + 1)
	j.RunAt = now.Add(delay)
	j.State = job.StatePending

	if updateErr := r.store.UpdateJob(ctx, j); updateErr != nil {
		r.logger.Error("failed to requeue job after launch failure",
			log.String("job_id", j.ID.String()),
			log.String("error", updateErr.Error()),
		)

		return updateErr
	}

	r.logger.Warn("sandbox launch failed; requeued without consuming a retry",
		log.String("job_id", j.ID.String()),
		log.String("job_name", j.Name),
		log.String("error", j.LastError),
		log.Duration("delay", delay),
	)

	return fmt.Errorf("job %s launch failed: %s", j.Name, j.LastError)
}

// scheduleRetry sets the job to StateRetrying with a backoff delay.
func (r *Runner) scheduleRetry(ctx context.Context, j *job.Job, now time.Time) error {
	// The attempt reached the handler, so whatever launch trouble this job
	// had is behind it.
	r.forgetLaunchFailures(j.ID.String())

	delay := r.backoff.Delay(j.RetryCount)
	nextRunAt := now.Add(delay)
	j.RunAt = nextRunAt
	j.State = job.StateRetrying

	if updateErr := r.store.UpdateJob(ctx, j); updateErr != nil {
		r.logger.Error("failed to update job for retry",
			log.String("job_id", j.ID.String()),
			log.String("error", updateErr.Error()),
		)
		return updateErr
	}

	r.extensions.EmitJobRetrying(ctx, j, j.RetryCount, nextRunAt)

	r.logger.Info("job scheduled for retry",
		log.String("job_id", j.ID.String()),
		log.String("job_name", j.Name),
		log.Int("attempt", j.RetryCount),
		log.Int("max_retries", j.MaxRetries),
		log.Duration("delay", delay),
	)

	return fmt.Errorf("job %s retry %d/%d: %w", j.Name, j.RetryCount, j.MaxRetries, fmt.Errorf("%s", j.LastError))
}

// sendToDLQ marks the job as failed, pushes it to the DLQ, and emits events.
func (r *Runner) sendToDLQ(ctx context.Context, j *job.Job, handlerErr error) error {
	r.forgetLaunchFailures(j.ID.String())

	j.State = job.StateFailed

	if updateErr := r.store.UpdateJob(ctx, j); updateErr != nil {
		r.logger.Error("failed to update job as failed",
			log.String("job_id", j.ID.String()),
			log.String("error", updateErr.Error()),
		)
		return updateErr
	}

	if r.dlqService != nil {
		if dlqErr := r.dlqService.Push(ctx, j, handlerErr); dlqErr != nil {
			r.logger.Error("failed to push job to DLQ",
				log.String("job_id", j.ID.String()),
				log.String("error", dlqErr.Error()),
			)
		}
	}

	r.extensions.EmitJobFailed(ctx, j, handlerErr)
	r.extensions.EmitJobDLQ(ctx, j, handlerErr)

	// Not always "after exhausting retries" any more: a permanent failure
	// arrives here on its first attempt.
	r.logger.Warn("job moved to DLQ",
		log.String("job_id", j.ID.String()),
		log.String("job_name", j.Name),
		log.Int("retry_count", j.RetryCount),
		log.String("error", handlerErr.Error()),
	)

	return handlerErr
}
