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

// JobCancelled fires when a job reaches cancelled: immediately for a pending
// or retrying job, and for a running job when its worker observes the cancel
// (lease lost to a cancelled row) instead of reporting job.failed.
type JobCancelled interface {
	OnJobCancelled(ctx context.Context, j *job.Job) error
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

// ──────────────────────────────────────────────────
// Operator actions
// ──────────────────────────────────────────────────

// OperatorActionObserver sees every operator action taken through the engine.
type OperatorActionObserver interface {
	OnOperatorAction(ctx context.Context, a Action) error
}

// ActionKind names one kind of operator action.
type ActionKind string

// Operator action kinds. The engine emits exactly one Action per
// successful operator call.
const (
	ActionJobCancelled     ActionKind = "job.cancelled"
	ActionJobRetried       ActionKind = "job.retried"
	ActionDLQReplayed      ActionKind = "dlq.replayed"
	ActionDLQDeleted       ActionKind = "dlq.deleted"
	ActionDLQPurged        ActionKind = "dlq.purged"
	ActionCronEnabled      ActionKind = "cron.enabled"
	ActionCronDisabled     ActionKind = "cron.disabled"
	ActionCronDeleted      ActionKind = "cron.deleted"
	ActionCronTriggered    ActionKind = "cron.triggered"
	ActionWorkflowReplayed ActionKind = "workflow.replayed"
)

// Action describes one operator action. Only the fields that apply to
// its Kind are set; the rest keep their zero values.
type Action struct {
	Kind     ActionKind
	Actor    string   // from ActorFrom(ctx); empty when unknown
	JobID    id.JobID // job acted on
	NewJobID id.JobID // job created (replay, cron trigger)
	DLQID    id.DLQID
	CronID   id.CronID
	RunID    id.RunID
	Step     string // workflow replay step
	Count    int64  // purge count, replay-all count
	At       time.Time
}

// actorKey is the context key for the acting subject. Unexported, so no
// other package can collide with it or set it except through WithActor.
type actorKey struct{}

// WithActor returns a copy of ctx that carries the subject taking an
// operator action. The engine reads it back through ActorFrom.
func WithActor(ctx context.Context, subject string) context.Context {
	return context.WithValue(ctx, actorKey{}, subject)
}

// ActorFrom returns the subject stored by WithActor, or "" when ctx
// carries none.
func ActorFrom(ctx context.Context) string {
	if subject, ok := ctx.Value(actorKey{}).(string); ok {
		return subject
	}
	return ""
}
