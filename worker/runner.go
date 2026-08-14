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
	"strconv"
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

// scratchDirPrefix names every scratch directory prepareOutputDir
// creates. sweepStaleScratchDirs matches on it so Reclaim only ever
// removes directories this package itself created, never an unrelated
// entry that happens to share a temp root.
const scratchDirPrefix = "dispatch-out-"

// staleScratchDirAge is how old a leftover scratch directory must be
// before sweepStaleScratchDirs removes it. Generous on purpose: Reclaim
// runs once at worker startup, so this only ever removes directories a
// PREVIOUS process left behind by dying before its own deferred cleanup
// ran — not one a currently running sibling process sharing the same
// scratch root is still writing into.
const staleScratchDirAge = time.Hour

// errFenceLost marks a commit-outputs failure caused specifically by
// the lease fence, as opposed to an ordinary artifact-plane failure —
// see commitOutputs and terminalFor's classification of its error.
var errFenceLost = errors.New("dispatch/worker: lease fence lost")

// errDuplicateOutputName marks two files under one OutputDir that would
// commit under the identical name — see collectOutputEntries.
var errDuplicateOutputName = errors.New("dispatch/worker: duplicate output name")

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
// leaked across a restart, and removes stale scratch output directories
// a previous process left behind. The pool calls it once at startup.
//
// Failures are joined rather than fatal: a rung that cannot sweep should not
// stop the worker from running the jobs it can still execute.
func (r *Runner) Reclaim(ctx context.Context, workerID id.WorkerID) error {
	// Unconditional here, but not unconditional in effect: this Runner's
	// own scratch directories can only exist if it has an artifact plane
	// configured (see WithArtifacts), and sweepStaleScratchDirs' own
	// first line returns immediately when it does not — a Runner without
	// one has no scratch root of its own to sweep, and must not go
	// looking through os.TempDir() on a config it never opted into.
	r.sweepStaleScratchDirs()

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
		// write Execute makes afterward (see abandonLostLease), not gated
		// on it.
		//
		// What actually protects against two workers each believing they
		// hold the same job at the same RetryCount at once — a lease
		// reclaim racing a zombie that has not yet noticed its lease
		// expired — is CreateFenced: commitOutputs commits under this
		// worker's lease epoch as the fence token when one is available,
		// so two holders at the same nominal attempt resolve to different
		// backend objects, never the same one. That protection does not
		// depend on either holder knowing anything is wrong.
		//
		// commitOutputs ALSO checks context.Cause(ctx) for job.ErrLeaseLost
		// specifically before committing anything, and rechecks it before
		// every individual file. Be precise about what this catches and
		// what it does not: the pool's heartbeat loop only sets that cause
		// once a RenewLease call comes back with a *definitive*
		// job.ErrLeaseLost (Pool.sendHeartbeats), which can lag the actual
		// reclaim by up to one heartbeat interval, and — the case that
		// matters most — never fires at all while this worker cannot reach
		// the store, since a transient renewal error deliberately does not
		// cancel a healthy job. A network partition, the canonical way a
		// lease is actually lost, is exactly what this check cannot see.
		// It is worth having anyway: it is nearly free, and it turns the
		// common case (the heartbeat noticed before Run even returned)
		// into an outright refusal to commit rather than relying solely on
		// CreateFenced to make the collision harmless. Call it what it is
		// — an opportunistic check for a signal the pool may already have
		// — not a fence of its own.
		//
		// Only a genuinely failed attempt (res.Status != StatusOK) skips
		// this outright.
		if executor.Level() > exec.LevelNone && res.Status == exec.StatusOK {
			if commitErr := r.commitOutputs(ctx, j, req); commitErr != nil {
				if errors.Is(commitErr, errFenceLost) {
					// Must NOT become an *exec.Error with
					// StatusLaunchFailed: handleFailure routes that
					// status through requeueAfterLaunchFailure, which
					// writes via the plain, UNFENCED store.UpdateJob —
					// exactly the write a fenced-out attempt must never
					// make, since it could stomp whatever the actual
					// current holder has already done to the row. An
					// ordinary wrapped error instead takes the normal
					// retry path, whose own scheduleRetry already calls
					// the FENCED updateJob and already routes
					// ErrLeaseLost to abandonLostLease — the same
					// protection every other kind of failure racing a
					// reclaim relies on today; this is not a new
					// mechanism, just this failure declining to bypass it.
					return fmt.Errorf("dispatch/worker: job %s: %w", j.ID, commitErr)
				}

				// An ordinary commit failure — a duplicate output name,
				// a backend error — is an artifact-plane fault, not a
				// verdict on the handler's own work: the handler already
				// ran to completion and reported success. Routing it
				// through StatusLaunchFailed keeps it off the job's real
				// retry budget, since a non-idempotent handler should not
				// pay for storage being unavailable, and bounds a
				// deterministic failure (see collectOutputEntries' own
				// duplicate-name check) at maxLaunchAttempts instead of
				// burning the whole retry schedule on something retrying
				// can never fix.
				return &exec.Error{Status: exec.StatusLaunchFailed, Msg: commitErr.Error()}
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

// scratchDirPattern returns the os.MkdirTemp pattern prepareOutputDir
// uses for one job's scratch directory. It embeds this process's PID
// between scratchDirPrefix and jobID so sweepStaleScratchDirs can later
// decide who might still be using a leftover directory by asking
// whether that PID is a live process, rather than guessing from age —
// see parseScratchDirPID and sweepStaleScratchDirs.
func scratchDirPattern(jobID string) string {
	return fmt.Sprintf("%s%d-%s-", scratchDirPrefix, os.Getpid(), jobID)
}

// parseScratchDirPID extracts the PID scratchDirPattern embedded in a
// scratch directory's name. ok is false for anything that does not
// match the expected shape — never one of ours, or corrupted — which
// sweepStaleScratchDirs treats identically to "not ours": left alone.
func parseScratchDirPID(name string) (pid int, ok bool) {
	rest, hasPrefix := strings.CutPrefix(name, scratchDirPrefix)
	if !hasPrefix {
		return 0, false
	}

	sep := strings.IndexByte(rest, '-')
	if sep <= 0 {
		return 0, false
	}

	n, err := strconv.Atoi(rest[:sep])
	if err != nil || n <= 0 {
		return 0, false
	}

	return n, true
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

	dir, err = os.MkdirTemp(root, scratchDirPattern(j.ID.String()))
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

// sweepStaleScratchDirs removes scratch directories prepareOutputDir
// left behind because the worker process that created them died before
// its own deferred cleanup ran. It is best-effort: a removal failure is
// logged, not returned, since one stuck directory must not stop Reclaim
// from doing the rest of what it does at startup.
//
// It does nothing at all when this Runner has no artifact plane
// configured: nothing this Runner does creates or commits scratch
// output without one (see WithArtifacts), so a Runner without one has
// no basis for deciding anything found here is its own business to
// remove — and calling Reclaim on such a Runner must not reach into a
// scratch root a differently-configured sibling process is legitimately
// using.
//
// Ownership, not age, is what decides whether a directory is touched:
// a directory whose embedded PID (see parseScratchDirPID) belongs to a
// process that is still alive — this one or a sibling sharing the same
// scratch root — is left alone regardless of how old it looks, because
// a live process rewriting the files inside it does not advance the
// directory's own mtime. Age is only a courtesy tie-break once the
// owning PID is confirmed gone, against the sliver of a window between
// MkdirTemp creating the directory and the owning process actually
// beginning to use it.
func (r *Runner) sweepStaleScratchDirs() {
	if r.artifacts == nil || !r.artifacts.Enabled() {
		return
	}

	root := r.scratchRoot
	if root == "" {
		root = os.TempDir()
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		// Best-effort: an unreadable or (already-gone) scratch root is
		// not something Reclaim should fail startup over.
		return
	}

	cutoff := time.Now().Add(-staleScratchDirAge)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		pid, ok := parseScratchDirPID(entry.Name())
		if !ok {
			continue // never one of ours
		}

		if processAlive(pid) {
			continue
		}

		info, infoErr := entry.Info()
		if infoErr != nil || info.ModTime().After(cutoff) {
			continue
		}

		stale := filepath.Join(root, entry.Name())
		if rmErr := os.RemoveAll(stale); rmErr != nil {
			r.logger.Warn("failed to remove stale scratch directory",
				log.String("dir", stale),
				log.String("error", rmErr.Error()),
			)
		}
	}
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

// outputEntry is one regular file collectOutputEntries found on disk,
// ready to commit under name.
type outputEntry struct {
	name string
	path string
}

// commitOutputs commits the regular files the sandbox actually left in
// req.OutputDir through the artifact service, linking each to j as an
// output of this attempt.
//
// It is driven entirely by what collectOutputEntries finds really on
// disk, never by anything the sandbox itself reported: a claim crossed
// a process boundary a compromised handler fully controls, so nothing
// about it — a name, a size, a hash, a content type — is evidence that
// anything actually landed anywhere. A handler that claims an output it
// never wrote gets no artifact row for it, because nothing here ever
// reads such a claim to decide what to commit.
//
// If the artifact plane is disabled, this logs once at debug and does
// nothing: the sandbox's outputs are discarded along with the scratch
// directory, exactly as they were before out-of-process committing
// existed.
func (r *Runner) commitOutputs(ctx context.Context, j *job.Job, req *exec.Request) error {
	if r.artifacts == nil || !r.artifacts.Enabled() {
		r.logger.Debug("artifact plane disabled; not committing sandbox outputs",
			log.String("job_id", j.ID.String()),
			log.String("job_name", j.Name),
		)

		return nil
	}

	// Checked before anything else, and specifically for job.ErrLeaseLost
	// — not any cancellation. ctx is also cancelled for reasons that have
	// nothing to do with who owns the job (a graceful shutdown mid-commit
	// being the obvious one), and misreporting THAT as "lease fence lost"
	// would be actively wrong: it is not what happened, and it changes
	// how the resulting failure gets classified below. Only the specific,
	// known signal the pool's heartbeat loop sets — job.ErrLeaseLost, the
	// moment RenewLease comes back definitive (Pool.sendHeartbeats /
	// cancelJob) — is treated as a fence loss here. See the longer
	// comment above this closure for what this check does and does not
	// cover; it is read-only regardless: nothing here renews or rewrites
	// anything lease_fence.go or the pool itself owns.
	if cause := context.Cause(ctx); errors.Is(cause, job.ErrLeaseLost) {
		return fmt.Errorf("%w: %w", errFenceLost, cause)
	}

	entries, err := collectOutputEntries(req.OutputDir)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: j.ID.String()}

	return r.commitOutputEntries(ctx, owner, j.RetryCount, fenceToken(ctx), entries)
}

// collectOutputEntries walks dir for the regular files a sandbox left
// behind, returning one outputEntry per unique base name in a
// deterministic order.
//
// Non-regular entries — symlinks, FIFOs, sockets, devices — are skipped
// without ever being opened. WalkDir reports each entry's type from an
// Lstat taken at listing time, so a symlink is identified and skipped
// here, before any path derived from it is ever handed to a file-open
// call anywhere in this package. Opening what a symlink resolves to
// would let a compromised handler point one at anything this worker
// process can read — its own config, cloud credentials, a mounted
// service-account token — and have the bytes published as an ordinary
// job output; opening a FIFO with no writer on the other end blocks
// forever, wedging a worker goroutine, which is just as fatal on a
// smaller scale. Both are excluded by the same type check, which is
// what makes it sufficient on its own: a directory-entry check alone
// (d.IsDir()) catches neither, since both report false for it.
//
// Dot-prefixed files are a rung's own uncommitted temp files (see
// exec/shim's LocalFS) or otherwise hidden by convention. A dot-prefixed
// directory is skipped with fs.SkipDir specifically, not a bare nil:
// WalkDir descends into a directory regardless of what the callback
// returns for it unless told SkipDir, so returning nil for a hidden
// directory would still walk — and still commit — whatever non-hidden
// files happen to live inside it.
//
// Two files at different paths sharing one base name are reported as
// errDuplicateOutputName rather than letting the second silently
// resolve to the same committed name as the first: Create's own name
// parameter may not contain a path separator, so any nested directory
// structure under OutputDir is necessarily flattened to its leaf name
// by the time it reaches the artifact plane, and two leaves colliding
// is a structural problem this function must surface, not paper over
// by committing whichever one the walk happened to visit last.
//
// dir not existing at all is a hard error, never an empty result. This
// function's only caller creates dir itself before the sandbox ever
// runs (see prepareOutputDir), so the ONLY way it can be missing here
// is something having removed it after the fact — the sandbox
// deleting its own OutputDir, or, before sweepStaleScratchDirs was
// made ownership-based, another process's sweep colliding with a
// still-running attempt. A vanished output directory is never a
// legitimate "the handler produced nothing": that is an empty,
// EXISTING directory, which WalkDir already reports as zero entries
// with no error, handled below without reaching this branch at all.
func collectOutputEntries(dir string) ([]outputEntry, error) {
	var entries []outputEntry
	seenAt := make(map[string]string)

	walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkEntryErr error) error {
		if walkEntryErr != nil {
			return walkEntryErr
		}

		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") {
				return fs.SkipDir
			}

			return nil
		}

		if strings.HasPrefix(d.Name(), ".") {
			return nil
		}

		if !d.Type().IsRegular() {
			return nil
		}

		name := d.Name()
		if prior, dup := seenAt[name]; dup {
			return fmt.Errorf("dispatch/worker: %q and %q would both commit as output %q: %w",
				prior, path, name, errDuplicateOutputName)
		}
		seenAt[name] = path

		entries = append(entries, outputEntry{name: name, path: path})

		return nil
	})
	if walkErr != nil {
		if errors.Is(walkErr, fs.ErrNotExist) {
			return nil, fmt.Errorf("dispatch/worker: output directory %q no longer exists: %w", dir, walkErr)
		}

		return nil, fmt.Errorf("dispatch/worker: walk output directory: %w", walkErr)
	}

	// Sorted so a partial commit's boundary — which entries land before a
	// later one fails, and so which ones commitOutputFile's own
	// FindCommitted check will recognise as already-done on a retry — is
	// a deterministic function of name, not of the filesystem's own
	// directory-listing order, which is unspecified and can vary between
	// platforms or even between runs on the same one.
	sort.Slice(entries, func(i, k int) bool { return entries[i].name < entries[k].name })

	return entries, nil
}

// fenceToken returns the lease epoch ctx carries, stringified, or "" if
// ctx carries no fence at all — a bare Runner driven without a Pool, or
// a store that does not implement job.LeaseStore. It is read-only: this
// neither renews nor otherwise touches anything leaseFenceFromContext's
// own package (lease_fence.go) owns.
func fenceToken(ctx context.Context) string {
	fence, ok := leaseFenceFromContext(ctx)
	if !ok {
		return ""
	}

	return strconv.Itoa(fence.epoch)
}

// commitOutputEntries commits each entry through the artifact service
// under token — see artifact.Service.CreateFenced — checking the lease
// fence again before every individual commit.
//
// A failure partway through leaves whatever already landed in place:
// nothing here rolls a prior success back. That is deliberate, not an
// omission — see commitOutputFile's own doc comment for why undoing a
// partial commit is worse than leaving it. A losing attempt may
// therefore end this call with some entries committed and others not;
// what matters is that every name it DOES leave committed stays valid
// and reusable, by this same attempt's own retry or by
// resolvePriorOutputs on a later one.
func (r *Runner) commitOutputEntries(
	ctx context.Context,
	owner artifact.OwnerRef,
	attempt int,
	token string,
	entries []outputEntry,
) error {
	for _, entry := range entries {
		if cause := context.Cause(ctx); errors.Is(cause, job.ErrLeaseLost) {
			return fmt.Errorf("%w: %w", errFenceLost, cause)
		}

		if _, err := r.commitOutputFile(ctx, owner, attempt, token, entry.name, entry.path); err != nil {
			return err
		}
	}

	return nil
}

// commitOutputFile reads one file collectOutputEntries found on disk
// and commits its actual bytes through the artifact service, returning
// the ref it was recorded under. The size, hash, and content type the
// resulting artifact row carries all come from what the backend
// actually saw pass through it while committing these exact bytes —
// nothing here is influenced by anything the sandbox itself claimed
// about its outputs.
//
// It checks FindCommitted first and treats a hit as a no-op success
// rather than re-writing anything: a commit failure is classified as
// StatusLaunchFailed (see terminalFor), which — like any other launch
// failure — requeues without advancing RetryCount, so a retry of it
// reuses the identical (owner, attempt) and, once a lease epoch is
// available, the identical fence token. Without this check, a name an
// earlier, partially-failed pass of the SAME attempt already committed
// would collide with itself on every subsequent retry — forever, on a
// store with no lease grants at all, since nothing there ever changes
// the key a retry computes. Recognising it as already-done instead
// makes a retry converge once whatever caused the original failure
// clears, regardless of whether the handler itself is careful enough
// to consult PriorOutputs and skip regenerating it.
func (r *Runner) commitOutputFile(
	ctx context.Context,
	owner artifact.OwnerRef,
	attempt int,
	token string,
	name, path string,
) (artifact.Ref, error) {
	if existing, err := r.artifacts.FindCommitted(ctx, owner, attempt, name, token); err == nil {
		return existing, nil
	} else if !errors.Is(err, artifact.ErrNotFound) {
		return artifact.Ref{}, fmt.Errorf("dispatch/worker: check existing output %q: %w", name, err)
	}

	f, err := openRegularNoFollow(path)
	if err != nil {
		return artifact.Ref{}, fmt.Errorf("dispatch/worker: open output %q: %w", name, err)
	}
	defer f.Close()

	// A second, TOCTOU-closing layer behind collectOutputEntries' own
	// Lstat-based filter (see its doc comment): openRegularNoFollow
	// already refuses to follow a symlink at the final path component on
	// platforms that support it, and this confirms what was actually
	// opened is still a plain regular file even so — catching the entry
	// that was one when listed but has since become something else.
	info, statErr := f.Stat()
	if statErr != nil {
		return artifact.Ref{}, fmt.Errorf("dispatch/worker: stat output %q: %w", name, statErr)
	}

	if !info.Mode().IsRegular() {
		return artifact.Ref{}, fmt.Errorf("dispatch/worker: output %q is no longer a regular file", name)
	}

	var w *artifact.CommitWriter
	if token != "" {
		w, err = r.artifacts.CreateFenced(ctx, owner, attempt, name, token)
	} else {
		w, err = r.artifacts.Create(ctx, owner, attempt, name)
	}
	if err != nil {
		return artifact.Ref{}, fmt.Errorf("dispatch/worker: create output %q: %w", name, err)
	}

	if _, copyErr := io.Copy(w, f); copyErr != nil {
		_ = w.Abort() //nolint:errcheck // best-effort cleanup; the write error below is what the caller acts on

		return artifact.Ref{}, fmt.Errorf("dispatch/worker: write output %q: %w", name, copyErr)
	}

	ref, err := w.Commit(ctx)
	if err != nil {
		return artifact.Ref{}, fmt.Errorf("dispatch/worker: commit output %q: %w", name, err)
	}

	return ref, nil
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
