// Package worker provides the job execution engine — a Runner that
// orchestrates a single job attempt through middleware and an exec.Executor,
// and a Pool that manages concurrent worker goroutines polling for jobs.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
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

	// artifacts is the artifact plane an out-of-process rung's outputs
	// are committed through, and PriorOutputs is resolved from. Nil
	// leaves both off — see WithArtifacts.
	artifacts *artifact.Service

	// scratchRoot is the directory an out-of-process attempt's scratch
	// OutputDir is created under. Empty means os.TempDir(), resolved at
	// request time rather than here so a change to the process's temp
	// directory after construction still takes effect.
	scratchRoot string

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

// WithArtifacts configures the artifact plane an out-of-process rung
// commits its outputs through, and the root directory its scratch
// OutputDir is created under. It returns r so callers can chain it onto
// NewRunner.
//
// Never calling this, or passing a nil or disabled svc, leaves output
// committing off: an out-of-process attempt still gets a fresh, empty
// OutputDir that is removed once the attempt ends, but PriorOutputs
// stays empty and nothing the sandbox wrote is committed — exactly
// Runner's behaviour before this existed. An empty scratchRoot defaults
// to os.TempDir().
func (r *Runner) WithArtifacts(svc *artifact.Service, scratchRoot string) *Runner {
	r.artifacts = svc
	r.scratchRoot = scratchRoot

	return r
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
		req := r.request(j, policy)

		// A rung above in-process gets a scratch directory to write its
		// outputs into and, when the artifact plane is configured, every
		// output an earlier attempt of this job already committed. Without
		// the latter the sandbox's in-memory store has no notion of prior
		// attempts and Existing/IfAbsent would answer "no" every time,
		// silently redoing work a previous attempt already finished.
		if executor.Level() > exec.LevelNone {
			dir, cleanup, dirErr := r.prepareOutputDir(j)
			if dirErr != nil {
				return &exec.Error{Status: exec.StatusLaunchFailed, Msg: dirErr.Error()}
			}
			defer cleanup()

			req.OutputDir = dir

			if r.artifacts != nil && r.artifacts.Enabled() {
				prior, priorErr := r.resolvePriorOutputs(ctx, j)
				if priorErr != nil {
					return &exec.Error{Status: exec.StatusLaunchFailed, Msg: priorErr.Error()}
				}

				req.PriorOutputs = prior
			} else {
				r.logger.Debug("artifact plane disabled; running without prior outputs",
					log.String("job_id", j.ID.String()),
					log.String("job_name", j.Name),
				)
			}
		}

		res, runErr := executor.Run(ctx, req)
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

		// Commit what the sandbox actually left on disk before reporting
		// the attempt as done. This runs ahead of the lease-fenced terminal
		// write Execute makes afterward (see abandonLostLease) rather than
		// after it — deliberately: a commit here lands under this
		// attempt's own attempt-scoped ephemeral key, the same key space
		// every in-process handler's Accessor.Create already writes to
		// mid-attempt, so it never collides with or overwrites anything a
		// winning attempt owns. If the lease already moved on, updateJob
		// still returns ErrLeaseLost and abandonLostLease still discards
		// the outcome — the commit just becomes an orphaned-ephemeral row
		// the existing sweeper collects, exactly the fate that comment
		// already documents for any other handler side effect. Only a
		// genuinely failed attempt (res.Status != StatusOK) skips this.
		if executor.Level() > exec.LevelNone && res.Status == exec.StatusOK {
			if commitErr := r.commitOutputs(ctx, j, req, res); commitErr != nil {
				return fmt.Errorf("dispatch/worker: commit outputs for job %s: %w", j.ID, commitErr)
			}
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

// prepareOutputDir creates a fresh, empty scratch directory for one
// out-of-process attempt to write its outputs into, under r.scratchRoot
// — os.TempDir() when that is unset.
//
// The returned cleanup removes the directory and must be deferred by the
// caller regardless of how the attempt ends: a stray directory per
// out-of-process attempt would otherwise accumulate on disk for the life
// of the worker process.
func (r *Runner) prepareOutputDir(j *job.Job) (dir string, cleanup func(), err error) {
	root := r.scratchRoot
	if root == "" {
		root = os.TempDir()
	}

	dir, err = os.MkdirTemp(root, "dispatch-out-"+j.ID.String()+"-")
	if err != nil {
		return "", func() {}, fmt.Errorf("dispatch/worker: create output directory: %w", err)
	}

	cleanup = func() {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			r.logger.Warn("failed to remove scratch output directory",
				log.String("job_id", j.ID.String()),
				log.String("dir", dir),
				log.String("error", rmErr.Error()),
			)
		}
	}

	return dir, cleanup, nil
}

// resolvePriorOutputs returns one PriorOutput per name any earlier
// attempt of j already committed, keeping the highest-attempt link when
// more than one attempt produced the same name — the same tie-break
// FindLinkByName applies for a single-name lookup.
//
// This is the worker-side half of PriorOutputs (see exec.PriorOutput):
// an out-of-process rung's artifact store is in-memory and local to one
// attempt, with no notion of earlier ones, so without this a retried
// handler's Existing/IfAbsent check would answer "no" every time and
// quietly redo work a previous attempt had already finished.
func (r *Runner) resolvePriorOutputs(ctx context.Context, j *job.Job) ([]exec.PriorOutput, error) {
	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: j.ID.String()}

	links, err := r.artifacts.Store().ListLinks(ctx, owner)
	if err != nil {
		return nil, fmt.Errorf("dispatch/worker: list prior links for job %s: %w", j.ID, err)
	}

	best := make(map[string]*artifact.Link, len(links))
	for _, link := range links {
		if link.Role != artifact.RoleOutput {
			continue
		}

		if cur, ok := best[link.Name]; !ok || link.Attempt > cur.Attempt {
			best[link.Name] = link
		}
	}

	if len(best) == 0 {
		return nil, nil
	}

	// Sorted so the request a given job history produces is deterministic
	// rather than following map iteration order.
	names := make([]string, 0, len(best))
	for name := range best {
		names = append(names, name)
	}
	sort.Strings(names)

	prior := make([]exec.PriorOutput, 0, len(names))
	for _, name := range names {
		a, getErr := r.artifacts.Get(ctx, best[name].ArtifactID)
		if getErr != nil {
			return nil, fmt.Errorf("dispatch/worker: resolve prior output %q for job %s: %w", name, j.ID, getErr)
		}

		prior = append(prior, exec.PriorOutput{Name: name, Ref: a.Ref()})
	}

	return prior, nil
}

// commitOutputs walks req.OutputDir for the files the sandbox actually
// left behind and commits each one through the artifact service,
// linking it to j as an output of this attempt.
//
// It is driven entirely by what is really on disk, never by res.Outputs:
// that claim crossed a process boundary a compromised handler fully
// controls, so a name, size, or hash it reports is not evidence that
// anything landed in the artifact store. A handler that lists an output
// it never wrote gets no artifact row for it, because commitOutputs
// never consults the claim to decide what to commit — only to carry
// forward a declared content type when a walked file's name happens to
// match one.
//
// If the artifact plane is disabled, this logs once at debug and does
// nothing: the sandbox's outputs are discarded along with the scratch
// directory, exactly as they were before out-of-process committing
// existed.
func (r *Runner) commitOutputs(ctx context.Context, j *job.Job, req *exec.Request, res *exec.Result) error {
	if r.artifacts == nil || !r.artifacts.Enabled() {
		r.logger.Debug("artifact plane disabled; not committing sandbox outputs",
			log.String("job_id", j.ID.String()),
			log.String("job_name", j.Name),
		)

		return nil
	}

	claimed := make(map[string]exec.OutputFile, len(res.Outputs))
	for _, o := range res.Outputs {
		claimed[o.Name] = o
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: j.ID.String()}

	walkErr := filepath.WalkDir(req.OutputDir, func(path string, d fs.DirEntry, walkEntryErr error) error {
		if walkEntryErr != nil {
			return walkEntryErr
		}

		// Dot-prefixed entries are a rung's own uncommitted temp files
		// (see exec/shim's LocalFS), never a finished output.
		if d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			return nil
		}

		return r.commitOutputFile(ctx, owner, j.RetryCount, d.Name(), path, claimed[d.Name()])
	})
	if walkErr != nil {
		if errors.Is(walkErr, fs.ErrNotExist) {
			// The handler removed its own OutputDir, or wrote nothing to
			// it. Either way there is nothing to commit.
			return nil
		}

		return fmt.Errorf("dispatch/worker: walk output directory: %w", walkErr)
	}

	return nil
}

// commitOutputFile reads one file the walk in commitOutputs found on
// disk and commits its actual bytes through the artifact service. claim
// supplies only a content-type hint, when the sandbox reported one for
// this name; the size and hash the artifact row ends up with come from
// what CommitWriter actually saw pass through it, never from claim.
func (r *Runner) commitOutputFile(
	ctx context.Context,
	owner artifact.OwnerRef,
	attempt int,
	name, path string,
	claim exec.OutputFile,
) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("dispatch/worker: open output %q: %w", name, err)
	}
	defer f.Close()

	var opts []artifact.CreateOption
	if claim.ContentType != "" {
		opts = append(opts, artifact.ContentType(claim.ContentType))
	}

	w, err := r.artifacts.Create(ctx, owner, attempt, name, opts...)
	if err != nil {
		return fmt.Errorf("dispatch/worker: create output %q: %w", name, err)
	}

	if _, err := io.Copy(w, f); err != nil {
		_ = w.Abort() //nolint:errcheck // best-effort cleanup; the write error below is what the caller acts on

		return fmt.Errorf("dispatch/worker: write output %q: %w", name, err)
	}

	if _, err := w.Commit(ctx); err != nil {
		return fmt.Errorf("dispatch/worker: commit output %q: %w", name, err)
	}

	return nil
}

// updateJob persists j's terminal state, fenced on the lease this worker
// held at claim time whenever ctx carries one — see withLeaseFence.
//
// A store that does not implement job.LeaseStore, or a Runner driven
// without a Pool, never sees a fence attached, so this falls back to the
// original unfenced UpdateJob: that path must not become a hard
// requirement of using Runner at all.
//
// job.ErrLeaseLost is returned to the caller exactly like any other
// error here — this method does no special-casing of it. Every call
// site does, through abandonLostLease, because ErrLeaseLost is not "the
// write failed, log and propagate," it is "someone else owns this job
// now, stop."
func (r *Runner) updateJob(ctx context.Context, j *job.Job) error {
	if fence, ok := leaseFenceFromContext(ctx); ok {
		return fence.store.UpdateLeasedJob(ctx, j, fence.workerID, fence.epoch)
	}

	return r.store.UpdateJob(ctx, j)
}

// abandonLostLease is what every fenced terminal write does on
// job.ErrLeaseLost: the lease moved on before this write landed, so the
// job is running under a different worker's epoch now and this attempt
// has no coherent claim left to make about it.
//
// It does not retry, requeue, or DLQ — both of those write, and the
// winner's outcome must stand untouched. The handler's own side effects
// need no cleanup here either: they already commit under attempt-scoped
// ephemeral artifact keys, so a losing attempt's outputs are orphaned-
// ephemeral and the existing sweeper collects them.
//
// The extension registry emit reuses EmitJobFailed rather than adding a
// new event: audit_hook and relay_hook both already implement
// ext.JobFailed, so they observe a lost lease with no new plumbing.
func (r *Runner) abandonLostLease(ctx context.Context, j *job.Job, cause error) error {
	r.logger.Warn("lease lost, discarding terminal write",
		log.String("job_id", j.ID.String()),
		log.String("job_name", j.Name),
	)

	r.extensions.EmitJobFailed(ctx, j, cause)

	return cause
}

// handleSuccess marks the job as completed and emits the lifecycle event.
func (r *Runner) handleSuccess(ctx context.Context, j *job.Job, now time.Time, elapsed time.Duration) error {
	r.forgetLaunchFailures(j.ID.String())

	j.State = job.StateCompleted
	j.CompletedAt = &now

	if updateErr := r.updateJob(ctx, j); updateErr != nil {
		if errors.Is(updateErr, job.ErrLeaseLost) {
			return r.abandonLostLease(ctx, j, updateErr)
		}

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

	if updateErr := r.updateJob(ctx, j); updateErr != nil {
		if errors.Is(updateErr, job.ErrLeaseLost) {
			return r.abandonLostLease(ctx, j, updateErr)
		}

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

	if updateErr := r.updateJob(ctx, j); updateErr != nil {
		if errors.Is(updateErr, job.ErrLeaseLost) {
			return r.abandonLostLease(ctx, j, updateErr)
		}

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
