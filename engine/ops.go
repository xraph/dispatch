package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// cancelAttempts bounds how many times CancelJob reads a running job
// again after its fenced write found the row had moved on. Each retry
// means the job changed state between the read and the write (it
// finished, failed, or was reclaimed), so a handful is plenty: the next
// read sees a state that is either cancellable without a fence or not
// cancellable at all.
const cancelAttempts = 3

// CancelJob moves a pending, retrying or running job to cancelled.
//
// A pending or retrying job is cancelled on the spot and JobCancelled
// fires before CancelJob returns. A running job is written cancelled
// while its worker still holds it, and the worker finds out through the
// lease: its next RenewLease, or its terminal write, comes back
// job.ErrLeaseLost because the row is no longer running. The pool then
// cancels the handler's context and the runner reports JobCancelled
// instead of JobFailed once it reads the row and sees why. Any other
// state wraps dispatch.ErrInvalidState.
//
// The running write is fenced on the holder and epoch read here, through
// job.LeaseStore.UpdateLeasedJob, rather than a plain UpdateJob. A plain
// write landing just after the worker completed the job would overwrite
// completed with cancelled. The fenced one is refused instead, and
// CancelJob reads the row again and decides on what it finds. A store
// without the lease capability, or a running job claimed without a
// lease, falls back to UpdateJob: there is no fence to write through.
//
// The returned job is the row as written. One OperatorAction of kind
// ext.ActionJobCancelled is emitted on success.
func (eng *Engine) CancelJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	for attempt := 1; ; attempt++ {
		j, err := eng.jobStore.GetJob(ctx, jobID)
		if err != nil {
			return nil, err
		}

		now := time.Now().UTC()

		switch j.State {
		case job.StatePending, job.StateRetrying:
			j.State = job.StateCancelled
			j.CompletedAt = &now

			if err := eng.jobStore.UpdateJob(ctx, j); err != nil {
				return nil, fmt.Errorf("cancel job %s: %w", jobID, err)
			}

			eng.extensions.EmitJobCancelled(ctx, j)

		case job.StateRunning:
			j.State = job.StateCancelled
			j.CompletedAt = &now

			err := eng.writeRunningCancel(ctx, j)
			if errors.Is(err, job.ErrLeaseLost) && attempt < cancelAttempts {
				// The row moved between the read and the write. Look
				// again rather than guess what it moved to.
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("cancel job %s: %w", jobID, err)
			}

			// JobCancelled for a running job is the worker's to emit,
			// once it stops. Emitting it here as well would report one
			// cancellation twice.

		default:
			return nil, fmt.Errorf("%w: job %s is %s", dispatch.ErrInvalidState, jobID, j.State)
		}

		eng.extensions.EmitOperatorAction(ctx, ext.Action{
			Kind:  ext.ActionJobCancelled,
			JobID: jobID,
		})

		return j, nil
	}
}

// writeRunningCancel writes a running job's cancellation, fenced on the
// worker and epoch j was read with whenever the store and the job allow
// it. See CancelJob.
func (eng *Engine) writeRunningCancel(ctx context.Context, j *job.Job) error {
	if ls, ok := eng.jobStore.(job.LeaseStore); ok && !j.WorkerID.IsNil() {
		return ls.UpdateLeasedJob(ctx, j, j.WorkerID, j.LeaseEpoch)
	}

	return eng.jobStore.UpdateJob(ctx, j)
}

// RetryJob puts a failed job back to pending, as if it had just been
// enqueued: no retries spent, no error, no owner. Any other state wraps
// dispatch.ErrInvalidState.
//
// A failed job usually has a dead letter entry, and replaying that entry
// would run the same failure a second time. So when one exists the retry
// claims it first, with the retried job as the claim's job, and a claim
// that is already taken refuses the retry with
// dispatch.ErrDLQAlreadyReplayed. If the job write then fails the claim
// is released, so the entry can be replayed or retried later. A failed
// job with no entry retries without a claim.
//
// Without an entry there is nothing to serialise two concurrent retries
// on. Both write the same pending row, which is harmless unless a worker
// claims the job between the two writes.
//
// The returned job is the row as written. One OperatorAction of kind
// ext.ActionJobRetried is emitted on success, carrying the claimed
// entry's ID when there was one.
func (eng *Engine) RetryJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	j, err := eng.jobStore.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}

	if j.State != job.StateFailed {
		return nil, fmt.Errorf("%w: job %s is %s", dispatch.ErrInvalidState, jobID, j.State)
	}

	ds := eng.dlqService.DLQStore()

	var claimed id.DLQID

	entry, err := ds.GetDLQByJobID(ctx, jobID)
	switch {
	case err == nil:
		claimErr := ds.ClaimReplay(ctx, entry.ID, jobID)
		switch {
		case claimErr == nil:
			claimed = entry.ID
		case errors.Is(claimErr, dispatch.ErrDLQNotFound):
			// Deleted between the read and the claim: there is no
			// longer anything a replay could run twice.
		default:
			return nil, fmt.Errorf("retry job %s: %w", jobID, claimErr)
		}
	case errors.Is(err, dispatch.ErrDLQNotFound):
		// Nothing to claim. The job failed without reaching the DLQ.
	default:
		return nil, fmt.Errorf("retry job %s: %w", jobID, err)
	}

	j.State = job.StatePending
	j.RetryCount = 0
	j.LastError = ""
	j.RunAt = time.Now().UTC()
	j.CompletedAt = nil
	// Clears StartedAt along with the worker and lease fields the failed
	// run left behind. Without it the retried job carries a lapsed
	// lease_expires_at into pending, which a claim that grants no lease
	// never overwrites. See job.Job.ClearOwnership for why that livelocks.
	j.ClearOwnership()

	if err := eng.jobStore.UpdateJob(ctx, j); err != nil {
		if !claimed.IsNil() {
			// Detached, so a write that failed because ctx ended does not
			// also strand the claim.
			relCtx := context.WithoutCancel(ctx)
			if relErr := ds.ReleaseReplay(relCtx, claimed, jobID); relErr != nil {
				eng.logger.Warn("retry: release of the dlq claim failed",
					log.String("job_id", jobID.String()),
					log.String("dlq_id", claimed.String()),
					log.String("error", relErr.Error()),
				)
			}
		}

		return nil, fmt.Errorf("retry job %s: %w", jobID, err)
	}

	if eng.pool != nil {
		eng.pool.Wake()
	}

	eng.extensions.EmitOperatorAction(ctx, ext.Action{
		Kind:  ext.ActionJobRetried,
		JobID: jobID,
		DLQID: claimed,
	})

	return j, nil
}
