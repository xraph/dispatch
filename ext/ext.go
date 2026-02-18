// Package ext defines the extension system for Dispatch.
// Extensions are notified of lifecycle events (job enqueued, completed,
// failed, etc.) and can react to them — logging, metrics, tracing, etc.
//
// Each lifecycle hook is a separate interface so extensions opt in only
// to the events they care about.
package ext

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Extension is the base interface all extensions must implement.
type Extension interface {
	// Name returns a unique human-readable name for the extension.
	Name() string
}

// ──────────────────────────────────────────────────
// Job lifecycle hooks
// ──────────────────────────────────────────────────

// JobEnqueued is called after a job is successfully enqueued.
type JobEnqueued interface {
	OnJobEnqueued(ctx context.Context, j *job.Job) error
}

// JobStarted is called when a worker begins executing a job.
type JobStarted interface {
	OnJobStarted(ctx context.Context, j *job.Job) error
}

// JobCompleted is called after a job finishes successfully.
type JobCompleted interface {
	OnJobCompleted(ctx context.Context, j *job.Job, elapsed time.Duration) error
}

// JobFailed is called when a job fails terminally (no more retries).
type JobFailed interface {
	OnJobFailed(ctx context.Context, j *job.Job, err error) error
}

// JobRetrying is called when a job fails but is scheduled for retry.
type JobRetrying interface {
	OnJobRetrying(ctx context.Context, j *job.Job, attempt int, nextRunAt time.Time) error
}

// JobDLQ is called when a job is moved to the dead letter queue.
type JobDLQ interface {
	OnJobDLQ(ctx context.Context, j *job.Job, err error) error
}

// ──────────────────────────────────────────────────
// Workflow lifecycle hooks
// ──────────────────────────────────────────────────

// WorkflowStarted is called when a workflow run begins.
type WorkflowStarted interface {
	OnWorkflowStarted(ctx context.Context, r *workflow.Run) error
}

// WorkflowStepCompleted is called after a workflow step completes.
type WorkflowStepCompleted interface {
	OnWorkflowStepCompleted(ctx context.Context, r *workflow.Run, stepName string, elapsed time.Duration) error
}

// WorkflowStepFailed is called when a workflow step fails.
type WorkflowStepFailed interface {
	OnWorkflowStepFailed(ctx context.Context, r *workflow.Run, stepName string, err error) error
}

// WorkflowCompleted is called after a workflow run finishes successfully.
type WorkflowCompleted interface {
	OnWorkflowCompleted(ctx context.Context, r *workflow.Run, elapsed time.Duration) error
}

// WorkflowFailed is called when a workflow run fails terminally.
type WorkflowFailed interface {
	OnWorkflowFailed(ctx context.Context, r *workflow.Run, err error) error
}

// ──────────────────────────────────────────────────
// Other lifecycle hooks
// ──────────────────────────────────────────────────

// CronFired is called when a cron entry fires and enqueues a job.
type CronFired interface {
	OnCronFired(ctx context.Context, entryName string, jobID id.JobID) error
}

// Shutdown is called during graceful shutdown.
type Shutdown interface {
	OnShutdown(ctx context.Context) error
}
