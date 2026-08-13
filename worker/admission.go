package worker

import (
	"context"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// admitted is a claimed job together with the local resource lease held
// on its behalf, as handed from the fetcher to a worker.
//
// The lease travels with the job rather than being looked up later,
// because the two are acquired in different goroutines: the fetcher
// admits the job, a worker runs it, and nothing between them may lose
// track of the capacity that was reserved. A nil lease means no manager
// is configured — the degraded path where the pool behaves exactly as it
// did before the resource model existed.
type admitted struct {
	job   *job.Job
	lease resource.Lease
}

// inflight is the pool's record of one job it currently owns.
//
// It holds the job's cancel func and its resource lease together so the
// single release path can return everything the job took. Keeping the
// lease here rather than on job.Job is deliberate: job.Job is the
// persisted row, shared with the store and serialized to it, and a live
// lease is process-local state that must never be written down.
type inflight struct {
	cancel context.CancelCauseFunc
	lease  resource.Lease

	// leaseEpoch is the lease epoch this worker was granted when it
	// claimed the job. Every renewal presents it, and a renewal that no
	// longer matches the row means another worker owns the job now.
	//
	// Unlike the resource lease above — which is live process state that
	// must never be written down — this is the opposite: a process-local
	// copy of a persisted value, held so renewal need not re-read the row.
	leaseEpoch int

	// leaseTTL is how far each renewal pushes the expiry out, resolved
	// once at claim time from the job's own LeaseTTL and the pool's
	// defaults. Held here so the renewal loop does not issue a GetJob per
	// job per interval purely to recover a duration it already knew.
	leaseTTL time.Duration
}

// dequeueBudget is the capacity ceiling this worker offers the store,
// or nil when no resource manager is configured.
//
// Every key is the manager's free capacity EXCEPT disk, which is free
// plus what a registered reclaimer could evict. The asymmetry is the
// whole point of the reclaimer interface, and it is wrong in both
// directions:
//
//   - Disk that is cached but unleased is available to a new job, since
//     staging evicts to make room. Offering only Free() there would let a
//     warm cache stop the worker claiming anything, which is exactly
//     backwards — a full cache is a healthy cache.
//   - Memory (and CPU, and GPU) held by a running job cannot be handed
//     to a second job by any amount of eviction. Offering Free() +
//     Reclaimable() there would admit work the box cannot run, which is
//     the OOM cascade this whole model exists to prevent. A reclaimer
//     registered for memory is therefore ignored HERE on purpose; it
//     still serves Manager.Acquire, which can afford to wait.
//
// Custom keys are passed through untouched. The store's fit predicate
// ignores quantities on custom dimensions (see job.DequeueOpts.Budget)
// and matches them by key through CustomKeys instead, so the quantity a
// custom key carries here is informational — it is enforced locally by
// admit, after the claim.
func (p *Pool) dequeueBudget() resource.Set {
	if p.resources == nil {
		return nil
	}

	budget := p.resources.Free()
	if budget == nil {
		budget = make(resource.Set)
	}

	if extra := p.resources.Reclaimable()[resource.Disk]; extra > 0 {
		budget[resource.Disk] += extra
	}

	return budget
}

// offeredCustomKeys is the set of custom resource keys this worker
// advertises at dequeue.
//
// An explicit WithWorkerCustomKeys wins, so a worker can offer a
// capability it does not meter. Otherwise the keys are derived from the
// manager's capacity, which is the honest default: a worker configured
// with 2 fpga has 2 fpga to offer, and one configured with none must not
// claim work that needs one.
func (p *Pool) offeredCustomKeys() []string {
	if len(p.customKeys) > 0 {
		return p.customKeys
	}

	if p.resources == nil {
		return nil
	}

	return p.resources.Capacity().CustomKeys()
}

// admit reserves local capacity for a job that has already been claimed.
//
// It uses Acquire under a short deadline, NOT TryAcquire, and the
// distinction is the difference between working and deadlocking.
// TryAcquire never reclaims — its own doc says a caller that cannot wait
// cannot afford eviction I/O either — but dequeueBudget offers disk as
// free PLUS reclaimable. Pairing the two would tell the store that 100
// GiB is available, take delivery of the job it sends back, and then
// refuse it against free alone. Every poll. Forever. The budget's promise
// has to be redeemable by the thing that redeems it, so admission has to
// be able to evict.
//
// Acquire reclaims first and only waits if reclamation was not enough, so
// a deadline turns "block until someone finishes" into "evict if you can,
// then give up". The instinct behind TryAcquire — never block the fetcher,
// which is sitting on claimed, running jobs whose heartbeats are ticking —
// is served by the deadline instead of by refusing to reclaim.
//
// The deadline is pollInterval, derived rather than invented: a refusal
// only costs one requeue and the next poll retries, so the fetcher should
// never stall longer than the cadence it would have waited anyway.
//
// ctx is the budget for the WHOLE batch, not for this job — see
// admissionBudget. Each successive job gets whatever is left of it.
//
// A failure is reachable in normal operation even though the store already
// applied a fit predicate. Dequeue matches custom resources by key only,
// never by quantity, so a worker offering "fpga" can legitimately claim a
// job wanting four of them. It is also reachable on the canonical keys,
// because the budget was computed before the claim and another job may
// have been admitted since. The returned error names the dimensions that
// did not fit, which is what the requeue path logs.
func (p *Pool) admit(ctx context.Context, j *job.Job) (resource.Lease, error) {
	if p.resources == nil {
		return nil, nil
	}

	return p.resources.Acquire(ctx, j.ID.String(), j.Resources)
}

// admissionBudget bounds how long the fetcher may spend reclaiming for
// one batch of claimed jobs.
//
// One budget for the batch, not one per job. Per job, the worst case is
// batch size × deadline, and both terms are independently tunable: a
// concurrency of 20 with a 5s poll interval is a 100s stall against a
// 30s stale-job threshold — the reaper would start reclaiming jobs this
// fetcher is still holding, in running state and not yet heartbeating.
// Sharing one deadline makes the worst case the deadline itself, whatever
// the batch size.
//
// Spending the budget does not poison the whole batch, but it does cost
// the jobs behind more than just the wait, and the difference matters.
//
// Anything that FITS is granted outright, expired context or not:
// resource.Manager.Acquire returns before it ever looks at ctx when the
// request fits current free capacity. So a batch of jobs this worker has
// room for is admitted in full however long the first one took.
//
// What an exhausted budget also costs is RECLAMATION. Acquire's loop
// checks ctx.Err() at the top and returns before calling reclaimLocked
// below it, so once the budget is spent a job that would have fitted
// after evicting some cached artifact bytes is no longer given the
// chance: it is refused and requeued as a misfit even though the disk it
// needed was reclaimable. That is a correct outcome — the job stays
// pending, nothing is lost, and the next poll gets a fresh budget — but
// it is a throughput cost paid by every job after the one that burned
// the budget, not "the fetcher merely stopped waiting".
//
// It mirrors callCtx: no manager or no jobs means no budget to spend, and
// the caller still gets a cancel func so it can defer uniformly.
func (p *Pool) admissionBudget(batch int) (context.Context, context.CancelFunc) {
	if p.resources == nil || batch == 0 {
		return p.cancelCtx, func() {}
	}

	return context.WithTimeout(p.cancelCtx, p.admitTimeout())
}

// admitTimeout is the admission budget's duration. A non-positive poll
// interval would expire the context before Acquire's first iteration,
// degrading it back into the TryAcquire behaviour that cannot redeem the
// disk budget, so it floors at something small rather than at zero.
func (p *Pool) admitTimeout() time.Duration {
	if p.pollInterval > 0 {
		return p.pollInterval
	}

	return time.Millisecond
}

// requeueLocalMisfit returns a job this worker claimed but could not
// admit to pending, so another worker — or this one, later — can run it.
//
// It reuses the rate-limited requeue path verbatim: same state, same
// short delay. A job that no worker in the fleet can ever fit will bounce
// on that delay rather than run; detecting that condition is the job of
// unschedulable sweeping, which is a later phase and deliberately not
// approximated here. What does pace it is the fetch loop, which treats a
// batch that dispatched nothing as an empty poll and backs off.
//
// cause is logged rather than recomputed. resource.Manager already names
// the dimensions that did not fit in its error, and re-deriving them here
// would take the manager's mutex and call every reclaimer's Available on
// the misfit path — to produce a worse answer, since the natural thing to
// compare against is the ceiling the store was offered rather than the
// free capacity the acquisition actually failed on.
func (p *Pool) requeueLocalMisfit(j *job.Job, cause error) {
	if p.cancelCtx.Err() != nil {
		// Admission was interrupted by shutdown, not by a shortfall. The
		// rate-limited path would write through the pool's own cancelled
		// context and silently fail, stranding a running job with no
		// worker until the reaper; the undispatched path uses a fresh one.
		p.requeueUndispatched(j)

		return
	}

	p.logger.Debug("job does not fit local capacity, returning to pending",
		log.String("job_id", j.ID.String()),
		log.String("job_name", j.Name),
		log.Any("required", j.Resources),
		log.String("error", cause.Error()),
	)

	p.requeueRateLimited(j)
}

// releaseQueueSlot returns the queue/tenant token acquired for j, if the
// pool has a queue manager. Safe to call for a job that never ran.
func (p *Pool) releaseQueueSlot(j *job.Job) {
	if p.queueManager != nil {
		p.queueManager.Release(j.Queue, j.ScopeOrgID)
	}
}

// abandon gives back everything an undispatched job holds during
// shutdown: its row goes back to pending, and its queue token and
// resource lease are released. Without this a stopping pool would leave
// capacity spoken for by a job it never ran.
func (p *Pool) abandon(a admitted) {
	p.requeueUndispatched(a.job)
	p.releaseQueueSlot(a.job)

	if a.lease != nil {
		a.lease.Release()
	}
}

// finishJob returns everything one attempt held: the tracking entry and
// its cancel func, the queue/tenant token, the resource lease, and the
// worker slot.
//
// It runs from a single defer in runJob so a panicking handler cannot
// leak capacity. A pool without middleware.Recover installed will still
// crash on that panic — that is the caller's choice — but it will not
// first strand a lease that nothing else can release, leaving the worker
// permanently short of the memory that job was holding.
//
// The lease is taken from the admitted value rather than read back out
// of the in-flight record, so it is released even if the panic happened
// before the record was ever written. Lease.Release is idempotent, so the
// two paths cannot double-credit the ledger.
func (p *Pool) finishJob(a admitted) {
	if rec := p.untrackJob(a.job.ID.String()); rec != nil {
		rec.cancel(nil)
	}

	p.releaseQueueSlot(a.job)

	if a.lease != nil {
		a.lease.Release()
	}

	p.slots <- struct{}{}
}
