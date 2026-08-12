package job

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
)

// ListOpts controls pagination and filtering for job list queries.
type ListOpts struct {
	// Limit is the maximum number of jobs to return. Zero means no limit.
	Limit int
	// Offset is the number of jobs to skip.
	Offset int
	// Queue filters by queue name. Empty means all queues.
	Queue string
}

// CountOpts controls filtering for job count queries.
type CountOpts struct {
	// Queue filters by queue name. Empty means all queues.
	Queue string
	// State filters by job state. Empty means all states.
	State State
}

// Store defines the persistence contract for jobs.
type Store interface {
	// EnqueueJob persists a new job in pending state.
	EnqueueJob(ctx context.Context, j *Job) error

	// DequeueJobs atomically claims up to limit pending jobs from the given
	// queues, sets them to running, and returns them. Jobs are ordered by
	// priority (descending) then RunAt (ascending).
	DequeueJobs(ctx context.Context, queues []string, limit int) ([]*Job, error)

	// GetJob retrieves a job by ID.
	GetJob(ctx context.Context, jobID id.JobID) (*Job, error)

	// UpdateJob persists changes to an existing job.
	UpdateJob(ctx context.Context, j *Job) error

	// DeleteJob removes a job by ID.
	DeleteJob(ctx context.Context, jobID id.JobID) error

	// ListJobsByState returns jobs matching the given state.
	ListJobsByState(ctx context.Context, state State, opts ListOpts) ([]*Job, error)

	// HeartbeatJob updates the heartbeat timestamp for a running job,
	// indicating the worker is still alive.
	HeartbeatJob(ctx context.Context, jobID id.JobID, workerID id.WorkerID) error

	// ReapStaleJobs returns running jobs whose last heartbeat is older than
	// the given threshold, indicating the worker may have crashed.
	ReapStaleJobs(ctx context.Context, threshold time.Duration) ([]*Job, error)

	// CountJobs returns the number of jobs matching the given options.
	CountJobs(ctx context.Context, opts CountOpts) (int64, error)
}

// LeaseStore is the opt-in lease capability.
//
// It is deliberately not part of Store. A backend that implements Store
// alone keeps compiling and keeps behaving exactly as it does today,
// reaped on the pool's single global threshold. A backend that also
// implements LeaseStore gets per-definition lease TTLs, epoch fencing,
// and atomic reclamation. This mirrors the capability idiom the artifact
// backend already uses for RangeReader and Presigner.
//
// Every method takes an absolute leaseUntil rather than a TTL. If the
// store computed now+ttl it would need per-dialect interval arithmetic
// over a nanosecond integer — and SQLite, Mongo, and Redis have no
// interval type at all. Passing a timestamp means every backend only
// writes a value, and lease policy lives in one place.
type LeaseStore interface {
	// DequeueLeased claims up to limit ready jobs, sets them running,
	// assigns workerID, increments lease_epoch, and sets lease_expires_at
	// to leaseUntil. The returned jobs carry the epoch they were granted.
	//
	// leaseUntil is a short initial grant that only has to survive until
	// the holder's first renewal; the renewal then extends it using the
	// job's own LeaseTTL.
	DequeueLeased(
		ctx context.Context,
		queues []string,
		limit int,
		workerID id.WorkerID,
		leaseUntil time.Time,
	) ([]*Job, error)

	// RenewLease extends the lease to leaseUntil, but only if the job is
	// still running, still assigned to workerID, and still at epoch.
	//
	// It returns ErrLeaseLost when that condition does not hold. That
	// return is the entire fencing mechanism: a worker that was reclaimed
	// while paused learns it no longer owns the job within one heartbeat
	// interval, instead of continuing to write for hours.
	RenewLease(
		ctx context.Context,
		jobID id.JobID,
		workerID id.WorkerID,
		epoch int,
		leaseUntil time.Time,
	) error

	// ReclaimExpiredLeases returns to pending every running job whose
	// lease has expired, clearing the worker assignment, incrementing
	// lease_epoch to fence the previous holder, and incrementing
	// evict_count. RetryCount is never touched — a lost lease is
	// infrastructure, not a handler failure.
	//
	// The claim and the read are one atomic statement, so two pools
	// reclaiming concurrently cannot both take the same job.
	ReclaimExpiredLeases(ctx context.Context, limit int) ([]*Job, error)
}
