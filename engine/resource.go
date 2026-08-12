package engine

import (
	"context"
	"sort"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

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
	// single insert for every job written before this feature existed —
	// MaxWorkerCapacity reads the cluster registry, and paying for that
	// on an unconstrained enqueue would be a regression for no answer.
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

// MaxWorkerCapacity returns the per-key maximum capacity across active
// workers, or an empty Set when capacity is unknown.
//
// An empty result disables the unschedulable check rather than
// rejecting everything, which is the right behaviour for a
// single-process engine that has registered no workers yet.
func (eng *Engine) MaxWorkerCapacity(ctx context.Context) resource.Set {
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

	for _, w := range workers {
		if w == nil || w.State != cluster.WorkerActive {
			continue
		}

		maxCap = maxCap.Max(w.Capacity)
	}

	return maxCap
}
