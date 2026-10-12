package worker

import (
	"context"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/artifact/staging"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// attemptUsage carries an attempt's measurements out of the terminal
// closure, which is the only place the executor's Result is visible.
//
// The closure runs beneath the whole middleware chain, so it cannot
// return anything but an error without changing that chain's signature.
// A pointer threaded down and read back after is the narrow way to get
// the numbers out.
type attemptUsage struct {
	usage    exec.Usage
	executor string
	measured bool
}

// recordUsage persists what an attempt consumed, beside what was
// predicted for it.
//
// It is best effort by contract. Telemetry that can fail a job is worse
// than no telemetry, so every failure here is logged at debug and
// swallowed: the job has already reached its terminal state by the time
// this runs, and nothing downstream depends on the row existing.
func (r *Runner) recordUsage(
	ctx context.Context,
	j *job.Job,
	at *attemptUsage,
	status job.State,
	elapsed time.Duration,
	workerID id.WorkerID,
) {
	recorder, ok := r.store.(job.UsageRecorder)
	if !ok {
		// The backend has no usage table. Running jobs is unaffected;
		// there is simply no history to size them from later.
		return
	}

	u := &job.Usage{
		ID:         id.NewUsageID(),
		JobID:      j.ID,
		Name:       j.Name,
		Queue:      j.Queue,
		Attempt:    j.RetryCount,
		Status:     status,
		InputBytes: boundInputBytes(j),
		Resources:  j.Resources,
		WallTime:   elapsed,
		ScopeAppID: j.ScopeAppID,
		ScopeOrgID: j.ScopeOrgID,
		WorkerID:   workerID,
		RecordedAt: time.Now().UTC(),
	}

	if at != nil && at.measured {
		u.Executor = at.executor
		u.CPUTime = at.usage.CPUTime
		u.PeakRSS = at.usage.PeakRSS
		u.DiskWritten = at.usage.DiskWritten

		// The rung's own wall time is the better figure when it has one:
		// it excludes the staging and middleware that ran around it.
		if at.usage.WallTime > 0 {
			u.WallTime = at.usage.WallTime
		}
	}

	// A cancelled job context must not stop the record being written —
	// that is exactly the attempt worth knowing about. The store's own
	// timeout still bounds this.
	if err := recorder.RecordJobUsage(context.WithoutCancel(ctx), u); err != nil {
		r.logger.Debug("dispatch/worker: could not record job usage",
			log.String("job_id", j.ID.String()),
			log.String("error", err.Error()),
		)
	}
}

// boundInputBytes totals the artifacts bound to a job.
//
// This is the feature a later estimator regresses on, so it is recorded
// even when the resource model is off: the correlation between input size
// and footprint is the thing worth having history of.
func boundInputBytes(j *job.Job) int64 {
	bindings, err := staging.GetBindings(j)
	if err != nil {
		return 0
	}

	return staging.TotalBoundSize(bindings)
}
