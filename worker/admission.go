package worker

import (
	"context"

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
	cancel context.CancelFunc
	lease  resource.Lease
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
// It is deliberately non-blocking. The fetcher holds claimed, running
// jobs at this point: blocking here would hold them hostage behind
// whatever is currently executing, past their heartbeat and into the
// reaper. TryAcquire also never reclaims, which is right for the same
// reason — a caller that cannot wait cannot afford eviction I/O either.
//
// The false return is reachable in normal operation even though the
// store already applied a fit predicate: dequeue matches custom
// resources by key only, never by quantity, so a worker offering "fpga"
// can legitimately claim a job wanting four of them. It is also reachable
// on the canonical keys, because the budget was computed before the claim
// and another job may have been admitted since.
func (p *Pool) admit(j *job.Job) (resource.Lease, bool) {
	if p.resources == nil {
		return nil, true
	}

	return p.resources.TryAcquire(j.ID.String(), j.Resources)
}

// requeueLocalMisfit returns a job this worker claimed but cannot fit to
// pending, so another worker — or this one, later — can run it.
//
// It reuses the rate-limited requeue path verbatim: same state, same
// short delay. A job that no worker in the fleet can ever fit will bounce
// on that delay rather than run; detecting that condition is the job of
// unschedulable sweeping, which is a later phase and deliberately not
// approximated here.
func (p *Pool) requeueLocalMisfit(j *job.Job) {
	p.logger.Debug("job does not fit local capacity, returning to pending",
		log.String("job_id", j.ID.String()),
		log.String("job_name", j.Name),
		log.Any("required", j.Resources),
		log.Any("short", j.Resources.Exceeds(p.dequeueBudget())),
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
		rec.cancel()
	}

	p.releaseQueueSlot(a.job)

	if a.lease != nil {
		a.lease.Release()
	}

	p.slots <- struct{}{}
}
