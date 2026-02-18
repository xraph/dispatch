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
