package job

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
)

// Usage is what one attempt actually consumed, recorded against what was
// predicted for it.
//
// The pairing is the point. Resources is the estimate the enqueuing
// process computed; WallTime, CPUTime, PeakRSS, and DiskWritten are what
// the attempt really used. Without both on the same row there is no way
// to tell a good estimate from a lucky one, and no way to improve either.
//
// InputBytes is recorded separately because it is the feature an
// estimator regresses on: for the workloads this exists to serve, a job's
// footprint is mostly a function of how big its input was.
type Usage struct {
	ID      id.UsageID `json:"id"`
	JobID   id.JobID   `json:"job_id"`
	Name    string     `json:"name"`
	Queue   string     `json:"queue"`
	Attempt int        `json:"attempt"`

	// Status is the terminal state this attempt reached. Failed attempts
	// are recorded too: a job that OOMs is the single most informative
	// data point about how much memory it needed.
	Status State `json:"status"`

	// InputBytes totals the artifacts bound to the job, or zero when it
	// declared none.
	InputBytes int64 `json:"input_bytes"`

	// Resources is what was predicted, empty when the resource model is
	// off.
	Resources resource.Set `json:"resources,omitempty"`

	// Measured consumption. Every field is zero when the rung that ran
	// the attempt could not account it: the in-process rung knows only
	// wall time, because a handler sharing the worker's address space has
	// no separable RSS.
	WallTime    time.Duration `json:"wall_time"`
	CPUTime     time.Duration `json:"cpu_time,omitempty"`
	PeakRSS     int64         `json:"peak_rss,omitempty"`
	DiskWritten int64         `json:"disk_written,omitempty"`

	// Executor names the rung that ran the attempt, so measurements taken
	// under different isolation are not silently pooled.
	Executor string `json:"executor,omitempty"`

	ScopeAppID string      `json:"scope_app_id,omitempty"`
	ScopeOrgID string      `json:"scope_org_id,omitempty"`
	WorkerID   id.WorkerID `json:"worker_id,omitempty"`
	RecordedAt time.Time   `json:"recorded_at"`
}

// UsageListOpts filters a usage query.
type UsageListOpts struct {
	// Name filters to one job definition. Empty means all.
	Name string
	// Since bounds the query to attempts recorded at or after this time.
	Since time.Time
	// Limit caps the rows returned. Zero means no limit.
	Limit int
	// Offset skips rows, for paging.
	Offset int
}

// UsageRecorder is an optional store capability: backends that can retain
// per-attempt measurements implement it, and the worker records through
// it when they do.
//
// It is deliberately not part of the composite Store. Usage is telemetry,
// not correctness — a backend without it runs jobs exactly the same way,
// it just accumulates no history to size them from later. Making it
// optional is what lets a backend gain the capability without every other
// backend having to gain it at the same moment.
//
// Callers must treat every method as best effort. A failure to record
// what a job consumed must never fail the job that consumed it.
type UsageRecorder interface {
	// RecordJobUsage persists one attempt's measurements.
	RecordJobUsage(ctx context.Context, u *Usage) error

	// ListJobUsage returns recorded attempts, most recent first.
	ListJobUsage(ctx context.Context, opts UsageListOpts) ([]*Usage, error)

	// PurgeJobUsage deletes records older than before, up to limit rows,
	// and returns how many it removed.
	//
	// This table grows with every attempt the fleet makes, so something
	// has to bound it. Retention is the operator's call, which is why the
	// cutoff is a parameter rather than a policy baked in here.
	PurgeJobUsage(ctx context.Context, before time.Time, limit int) (int64, error)
}
