package engine

import (
	"context"
	"fmt"
	"sort"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// Resources returns the shared admission ledger, or nil when the
// resource model is off. It is the instance the staging cache must have
// been built with.
func (eng *Engine) Resources() resource.Manager { return eng.resources }

// reaperSafetyFactor is the multiple of the claim-to-first-heartbeat
// window that StaleJobThreshold has to clear.
//
// Two, and the second one is not padding. The window itself —
// PollInterval + HeartbeatInterval — is what the fetcher can legitimately
// spend between claiming a job and that job's first heartbeat: admission
// may stall the batch for up to one poll interval while it reclaims disk,
// and the job then waits up to one heartbeat tick to be written down as
// alive. A threshold merely larger than that window leaves no room for
// the heartbeat itself to be slow, and the heartbeat is a store write on
// the same connection pool the dequeue just used. Doubling buys exactly
// one missed heartbeat round, which is the smallest slack that survives a
// briefly busy store.
//
// Below it the failure is not a stalled worker but a corrupted one: the
// reaper reclaims jobs the fetcher is still holding — rows already in
// running state, already claimed, not yet heartbeating — and the same job
// runs twice.
//
// The stock configuration clears it comfortably (1s + 10s, doubled, is
// 22s against a 30s threshold), so turning the resource model on does not
// force anybody to retune. What it catches is the combination that looks
// harmless: raising PollInterval to spare a shared database, or dropping
// StaleJobThreshold to fail over faster, without noticing the other.
const reaperSafetyFactor = 2

// checkReaperMargin rejects a configuration in which the stale-job
// reaper could reclaim a job the fetcher has claimed but not yet handed
// to a worker.
//
// It runs only when a resource manager is installed, because admission is
// what introduced the stall: Pool.admit calls Manager.Acquire under a
// one-poll-interval budget so it can evict cached disk to make room, and
// that budget is shared by the whole batch. Without a manager the fetcher
// never waits and the relationship does not exist.
//
// This fails Build rather than warning. The resource model is opt-in, so
// nobody arrives here by accident, and the symptom it prevents — a job
// executing twice because two subsystems disagreed about who owned it —
// is not one an operator can be expected to diagnose from a log line.
//
// leaseAware reports whether the store implements job.LeaseStore. It
// decides which number actually governs reclamation: on a lease-aware
// backend — every first-party one — worker/pool.go's reapStaleJobs routes
// to reclaimExpiredLeases, which reclaims purely on lease expiry
// (leaseTTLFor's chain: DefaultLeaseTTL, then StaleJobThreshold, then
// job.DefaultLeaseTTL) and never looks at StaleJobThreshold directly.
// Policing StaleJobThreshold alone on such a backend checks a number
// nothing reads: a deployment can set DefaultLeaseTTL to a few seconds,
// clear this check with a generous StaleJobThreshold, and still have the
// reaper reclaim a job the fetcher is still holding, because the lease
// granted at claim time expires long before the margin the threshold
// implied. A backend that is not lease-aware falls back to
// reapStaleJobsLegacy, which does read StaleJobThreshold directly, so
// that is what this check polices there instead.
func checkReaperMargin(cfg dispatch.Config, leaseAware bool) error {
	if cfg.StaleJobThreshold <= 0 {
		// The reaper is disabled; nothing can reclaim anything.
		return nil
	}

	window := max(cfg.PollInterval, 0) + max(cfg.HeartbeatInterval, 0)
	minimum := reaperSafetyFactor * window

	effective := cfg.StaleJobThreshold
	if leaseAware {
		effective = effectiveReclaimWindow(cfg)
	}

	if effective >= minimum {
		return nil
	}

	return fmt.Errorf(
		"dispatch: the effective reclamation window (%s) is too low for resource-aware "+
			"admission: the fetcher may hold a claimed job for up to PollInterval (%s) while it "+
			"reclaims capacity, and that job is not heartbeated for a further HeartbeatInterval "+
			"(%s), so the reaper could reclaim a job this worker is still holding; set "+
			"StaleJobThreshold (currently %s) and DefaultLeaseTTL (currently %s) so the window "+
			"that actually governs reclamation is at least %s, or leave the resource manager unset",
		effective, cfg.PollInterval, cfg.HeartbeatInterval,
		cfg.StaleJobThreshold, cfg.DefaultLeaseTTL, minimum)
}

// effectiveReclaimWindow mirrors worker.Pool.leaseTTLFor(nil): the lease
// TTL a freshly granted lease gets when no job overrides it with its own
// job.WithLeaseTTL. That per-job override can only raise a specific job's
// window above this floor, never lower it below what a config-time check
// can see, so checking the floor is sound.
func effectiveReclaimWindow(cfg dispatch.Config) time.Duration {
	if cfg.DefaultLeaseTTL > 0 {
		return cfg.DefaultLeaseTTL
	}
	if cfg.StaleJobThreshold > 0 {
		return cfg.StaleJobThreshold
	}

	return job.DefaultLeaseTTL
}

// resolveResources computes the job's resource spec and writes it onto
// the job before it is persisted.
//
// This runs once, in the enqueuing process. Everything downstream — the
// dequeue predicate, admission, pod sizing — reads the stored columns,
// so no user code ever runs on the scheduling path and a job's
// requirement cannot differ between workers.
func (eng *Engine) resolveResources(ctx context.Context, j *job.Job, opts job.Options) error {
	sizes, total, primaryHash := inputSizes(opts.Bindings)

	j.InputBytes = total
	j.PrimaryInputHash = primaryHash

	decl := eng.registry.Resources(j.Name)

	// Nothing anywhere constrains this job, so there is nothing to
	// resolve and nothing to check it against. Skipping keeps enqueue a
	// single insert for every job written before this feature existed.
	if !eng.resourcesInPlay(decl, opts) {
		return nil
	}

	// A definition declares; an enqueue overrides. Both the func and the
	// class are single-valued rather than merged per key, so the
	// enqueue-time value replaces the declared one outright when given.
	resFunc := decl.Func
	if opts.ResourceFunc != nil {
		resFunc = opts.ResourceFunc
	}

	class := decl.Class
	if opts.ResourceClass != "" {
		class = opts.ResourceClass
	}

	spec, err := resource.Resolve(ctx, resource.ResolveInput{
		GlobalDefault:  eng.resourceDefault,
		QueueDefault:   eng.queueResources[j.Queue],
		Declared:       decl.Requests,
		Func:           resFunc,
		Estimator:      eng.estimator,
		Override:       opts.Resources,
		DeclaredLimits: decl.Limits,
		OverrideLimits: opts.ResourceLimits,
		Class:          class,
		MaxCapacity:    eng.MaxWorkerCapacity(ctx),
		// Neither source may fail an enqueue, but neither may fail
		// silently either: both run once per enqueue in this process, so
		// a broken one is broken for every job of that name from here on
		// and the only symptom is jobs quietly sized from the static
		// declaration.
		OnError: func(source string, err error) {
			eng.logger.Warn("resource: sizing source failed; falling back to the declaration",
				log.String("source", source),
				log.String("job", j.Name),
				log.String("queue", j.Queue),
				log.String("error", err.Error()))
		},
		Request: resource.Request{
			JobName:    j.Name,
			Queue:      j.Queue,
			Payload:    j.Payload,
			Inputs:     sizes,
			InputBytes: total,
			Attempt:    j.RetryCount,
			ScopeOrgID: j.ScopeOrgID,
		},
	})
	if err != nil {
		return err
	}

	// A zero spec leaves the columns nil rather than storing an empty
	// map, so an unconstrained job looks exactly as it did before.
	if !spec.Requests.IsZero() {
		j.Resources = spec.Requests
	}

	if !spec.Limits.IsZero() {
		j.ResourceLimits = spec.Limits
	}

	j.ResourceClass = spec.Class

	return nil
}

// resourcesInPlay reports whether any source could produce a
// requirement for this job.
//
// Worker capacity is deliberately not a source: it only ever rejects a
// requirement, and an empty requirement exceeds nothing.
func (eng *Engine) resourcesInPlay(decl job.ResourceDecl, opts job.Options) bool {
	return !decl.IsZero() ||
		eng.estimator != nil ||
		len(eng.resourceDefault) > 0 ||
		len(eng.queueResources) > 0 ||
		len(opts.Resources) > 0 ||
		len(opts.ResourceLimits) > 0 ||
		opts.ResourceFunc != nil ||
		opts.ResourceClass != ""
}

// inputSizes flattens artifact bindings into estimator input.
//
// artifact.Ref already carries Size and ContentHash, so this needs no
// store round-trip and enqueue stays a single insert.
//
// The returned hash is that of the largest input, ties broken by slot
// name. Determinism matters: the same job enqueued twice must advertise
// the same locality signal, and Go map iteration order would not.
func inputSizes(bindings map[string]artifact.Ref) (
	sizes []resource.InputSize, total int64, primaryHash string,
) {
	if len(bindings) == 0 {
		return nil, 0, ""
	}

	sizes = make([]resource.InputSize, 0, len(bindings))

	for name, ref := range bindings {
		sizes = append(sizes, resource.InputSize{
			Name:  name,
			Bytes: ref.Size,
			Hash:  ref.ContentHash,
		})

		total += ref.Size
	}

	sort.Slice(sizes, func(i, k int) bool {
		if sizes[i].Bytes != sizes[k].Bytes {
			return sizes[i].Bytes > sizes[k].Bytes
		}

		return sizes[i].Name < sizes[k].Name
	})

	return sizes, total, sizes[0].Hash
}

// MaxWorkerCapacity returns the per-key maximum capacity the
// unschedulable check may compare a job against, or an empty Set when
// the check is off.
//
// An empty result disables the check rather than rejecting everything.
//
// It is empty unless an operator called WithWorkerCapacity, and that
// gate is the whole correctness argument. The registry cannot supply the
// fleet maximum on its own: cluster.Worker.Capacity round-trips only on
// store/memory — postgres, sqlite, mongo, redis and the k8s provider all
// enumerate worker fields by hand and drop it — so a fleet whose largest
// worker has 64 GiB reads back as a fleet of workers with no capacity at
// all. Deriving the ceiling from whatever this process happens to know
// therefore does not converge on the truth; it converges on THIS
// process, and rejects at enqueue every job bigger than the pod that
// enqueued it. Requiring the declaration also keeps the common path free
// of a ListWorkers round trip per enqueue.
//
// When the declaration is present the registry is still consulted, so
// the ceiling can only rise toward the real fleet maximum on a backend
// that carries capacity. "Live" means both an active state and a recent
// heartbeat. State alone is not enough: nothing in Dispatch ever writes
// WorkerDead — a worker registers active and is either deregistered on
// clean shutdown or its row is deleted by DeleteStaleWorkers. A worker
// killed by SIGKILL, an OOM or a pod eviction therefore stays "active"
// in the registry until something sweeps it, and counting its capacity
// would admit jobs no live worker can run.
func (eng *Engine) MaxWorkerCapacity(ctx context.Context) resource.Set {
	if len(eng.workerCapacity) == 0 {
		return nil
	}

	maxCap := eng.workerCapacity.Clone()

	if eng.clusterStore == nil {
		return maxCap
	}

	workers, err := eng.clusterStore.ListWorkers(ctx)
	if err != nil {
		// Capacity is advisory here. Failing an enqueue because the
		// cluster registry was briefly unreachable would be worse than
		// admitting a job that later needs rescheduling.
		eng.logger.Warn("resource: worker capacity unavailable; "+
			"skipping the unschedulable check",
			log.String("error", err.Error()))

		return maxCap
	}

	cutoff := time.Now().UTC().Add(-eng.staleWorkerThreshold())

	for _, w := range workers {
		if w == nil || w.State != cluster.WorkerActive {
			continue
		}

		if w.LastSeen.Before(cutoff) {
			continue
		}

		maxCap = maxCap.Max(w.Capacity)
	}

	return maxCap
}

// staleWorkerThreshold is how long a worker may go without a heartbeat
// before its capacity stops counting.
//
// It reuses the sweep threshold from extension.go — max(5×heartbeat,
// 5 minutes) — rather than inventing a second notion of staleness, so a
// worker's capacity stops counting at roughly the same moment the rest
// of the cluster layer stops believing in the worker.
//
// The threshold is deliberately generous in the same direction: shrinking
// the fleet view too eagerly turns a valid enqueue into a hard
// ErrUnschedulable, whereas holding a dead worker's capacity a little too
// long only lets a job pend. A loud false rejection is the worse failure.
func (eng *Engine) staleWorkerThreshold() time.Duration {
	threshold := 5 * eng.d.Config().HeartbeatInterval
	if threshold < 5*time.Minute {
		threshold = 5 * time.Minute
	}

	return threshold
}
