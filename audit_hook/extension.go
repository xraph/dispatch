package audithook

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Compile-time interface checks.
var (
	_ ext.Extension             = (*Extension)(nil)
	_ ext.JobEnqueued           = (*Extension)(nil)
	_ ext.JobStarted            = (*Extension)(nil)
	_ ext.JobCompleted          = (*Extension)(nil)
	_ ext.JobFailed             = (*Extension)(nil)
	_ ext.JobRetrying           = (*Extension)(nil)
	_ ext.JobDLQ                = (*Extension)(nil)
	_ ext.WorkflowStarted       = (*Extension)(nil)
	_ ext.WorkflowStepCompleted = (*Extension)(nil)
	_ ext.WorkflowStepFailed    = (*Extension)(nil)
	_ ext.WorkflowCompleted     = (*Extension)(nil)
	_ ext.WorkflowFailed        = (*Extension)(nil)
	_ ext.CronFired             = (*Extension)(nil)
)

// Recorder is the interface that audit backends must implement.
// This matches chronicle.Emitter but is defined locally so that the
// audit_hook package does not import Chronicle directly — callers inject
// the concrete *chronicle.Chronicle at wiring time.
type Recorder interface {
	// Record persists a fully-formed audit event.
	Record(ctx context.Context, event *AuditEvent) error
}

// AuditEvent is a local representation of an audit event.
// It mirrors chronicle/audit.Event but avoids a module dependency.
// Callers provide a RecorderFunc adapter that bridges to their audit backend.
type AuditEvent struct {
	// What happened
	Action   string `json:"action"`
	Resource string `json:"resource"`
	Category string `json:"category"`

	// Details
	ResourceID string         `json:"resource_id,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	Outcome    string         `json:"outcome"`
	Severity   string         `json:"severity"`
	Reason     string         `json:"reason,omitempty"`
}

// RecorderFunc is an adapter to use a plain function as a Recorder.
//
// Example bridging to Chronicle:
//
//	audithook.RecorderFunc(func(ctx context.Context, evt *audithook.AuditEvent) error {
//	    b := chronicle.Info(ctx, evt.Action, evt.Resource, evt.ResourceID).
//	        Category(evt.Category).
//	        Outcome(evt.Outcome)
//	    for k, v := range evt.Metadata {
//	        b = b.Meta(k, v)
//	    }
//	    return b.Record()
//	})
type RecorderFunc func(ctx context.Context, event *AuditEvent) error

func (f RecorderFunc) Record(ctx context.Context, event *AuditEvent) error {
	return f(ctx, event)
}

// Severity constants (mirror chronicle/audit).
const (
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)

// Outcome constants (mirror chronicle/audit).
const (
	OutcomeSuccess = "success"
	OutcomeFailure = "failure"
)

// Extension bridges Dispatch lifecycle events to an audit trail backend.
// Each lifecycle hook emits a structured audit event through the [Recorder].
type Extension struct {
	recorder Recorder
	enabled  map[string]bool // nil = all enabled
	logger   *slog.Logger
}

// New creates an Extension that emits audit events through the provided Recorder.
func New(r Recorder, opts ...Option) *Extension {
	e := &Extension{
		recorder: r,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Name implements ext.Extension.
func (e *Extension) Name() string { return "audit-hook" }

// ── Job lifecycle hooks ─────────────────────────────

// OnJobEnqueued implements ext.JobEnqueued.
func (e *Extension) OnJobEnqueued(ctx context.Context, j *job.Job) error {
	return e.record(ctx, ActionJobEnqueued, SeverityInfo, OutcomeSuccess,
		ResourceJob, j.ID.String(), CategoryJob, nil,
		"job_name", j.Name,
		"queue", j.Queue,
	)
}

// OnJobStarted implements ext.JobStarted.
func (e *Extension) OnJobStarted(ctx context.Context, j *job.Job) error {
	return e.record(ctx, ActionJobStarted, SeverityInfo, OutcomeSuccess,
		ResourceJob, j.ID.String(), CategoryJob, nil,
		"job_name", j.Name,
		"queue", j.Queue,
		"worker_id", j.WorkerID.String(),
	)
}

// OnJobCompleted implements ext.JobCompleted.
func (e *Extension) OnJobCompleted(ctx context.Context, j *job.Job, elapsed time.Duration) error {
	return e.record(ctx, ActionJobCompleted, SeverityInfo, OutcomeSuccess,
		ResourceJob, j.ID.String(), CategoryJob, nil,
		"job_name", j.Name,
		"queue", j.Queue,
		"elapsed_ms", elapsed.Milliseconds(),
	)
}

// OnJobFailed implements ext.JobFailed.
func (e *Extension) OnJobFailed(ctx context.Context, j *job.Job, jobErr error) error {
	return e.record(ctx, ActionJobFailed, SeverityCritical, OutcomeFailure,
		ResourceJob, j.ID.String(), CategoryJob, jobErr,
		"job_name", j.Name,
		"queue", j.Queue,
		"retry_count", j.RetryCount,
		"max_retries", j.MaxRetries,
	)
}

// OnJobRetrying implements ext.JobRetrying.
func (e *Extension) OnJobRetrying(ctx context.Context, j *job.Job, attempt int, nextRunAt time.Time) error {
	return e.record(ctx, ActionJobRetrying, SeverityWarning, OutcomeFailure,
		ResourceJob, j.ID.String(), CategoryJob, nil,
		"job_name", j.Name,
		"queue", j.Queue,
		"attempt", attempt,
		"next_run_at", nextRunAt.Format(time.RFC3339),
	)
}

// OnJobDLQ implements ext.JobDLQ.
func (e *Extension) OnJobDLQ(ctx context.Context, j *job.Job, jobErr error) error {
	return e.record(ctx, ActionJobDLQ, SeverityCritical, OutcomeFailure,
		ResourceJob, j.ID.String(), CategoryJob, jobErr,
		"job_name", j.Name,
		"queue", j.Queue,
		"retry_count", j.RetryCount,
	)
}

// ── Workflow lifecycle hooks ────────────────────────

// OnWorkflowStarted implements ext.WorkflowStarted.
func (e *Extension) OnWorkflowStarted(ctx context.Context, r *workflow.Run) error {
	return e.record(ctx, ActionWorkflowStarted, SeverityInfo, OutcomeSuccess,
		ResourceWorkflow, r.ID.String(), CategoryWorkflow, nil,
		"workflow_name", r.Name,
	)
}

// OnWorkflowStepCompleted implements ext.WorkflowStepCompleted.
func (e *Extension) OnWorkflowStepCompleted(ctx context.Context, r *workflow.Run, stepName string, elapsed time.Duration) error {
	return e.record(ctx, ActionWorkflowStepCompleted, SeverityInfo, OutcomeSuccess,
		ResourceWorkflow, r.ID.String(), CategoryWorkflow, nil,
		"workflow_name", r.Name,
		"step_name", stepName,
		"elapsed_ms", elapsed.Milliseconds(),
	)
}

// OnWorkflowStepFailed implements ext.WorkflowStepFailed.
func (e *Extension) OnWorkflowStepFailed(ctx context.Context, r *workflow.Run, stepName string, stepErr error) error {
	return e.record(ctx, ActionWorkflowStepFailed, SeverityWarning, OutcomeFailure,
		ResourceWorkflow, r.ID.String(), CategoryWorkflow, stepErr,
		"workflow_name", r.Name,
		"step_name", stepName,
	)
}

// OnWorkflowCompleted implements ext.WorkflowCompleted.
func (e *Extension) OnWorkflowCompleted(ctx context.Context, r *workflow.Run, elapsed time.Duration) error {
	return e.record(ctx, ActionWorkflowCompleted, SeverityInfo, OutcomeSuccess,
		ResourceWorkflow, r.ID.String(), CategoryWorkflow, nil,
		"workflow_name", r.Name,
		"elapsed_ms", elapsed.Milliseconds(),
	)
}

// OnWorkflowFailed implements ext.WorkflowFailed.
func (e *Extension) OnWorkflowFailed(ctx context.Context, r *workflow.Run, runErr error) error {
	return e.record(ctx, ActionWorkflowFailed, SeverityCritical, OutcomeFailure,
		ResourceWorkflow, r.ID.String(), CategoryWorkflow, runErr,
		"workflow_name", r.Name,
	)
}

// ── Cron lifecycle hooks ────────────────────────────

// OnCronFired implements ext.CronFired.
func (e *Extension) OnCronFired(ctx context.Context, entryName string, jobID id.JobID) error {
	return e.record(ctx, ActionCronFired, SeverityInfo, OutcomeSuccess,
		ResourceCron, entryName, CategoryCron, nil,
		"job_id", jobID.String(),
	)
}

// ── Internal helpers ────────────────────────────────

// record builds and sends an audit event if the action is enabled.
// The kvPairs argument is a list of key-value pairs added to Metadata.
func (e *Extension) record(
	ctx context.Context,
	action, severity, outcome string,
	resource, resourceID, category string,
	err error,
	kvPairs ...any,
) error {
	if e.enabled != nil && !e.enabled[action] {
		return nil
	}

	meta := make(map[string]any, len(kvPairs)/2+1)
	for i := 0; i+1 < len(kvPairs); i += 2 {
		key, ok := kvPairs[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", kvPairs[i])
		}
		meta[key] = kvPairs[i+1]
	}

	var reason string
	if err != nil {
		reason = err.Error()
		meta["error"] = err.Error()
	}

	evt := &AuditEvent{
		Action:     action,
		Resource:   resource,
		Category:   category,
		ResourceID: resourceID,
		Metadata:   meta,
		Outcome:    outcome,
		Severity:   severity,
		Reason:     reason,
	}

	if recErr := e.recorder.Record(ctx, evt); recErr != nil {
		e.logger.Warn("audit_hook: failed to record audit event",
			"action", action,
			"resource_id", resourceID,
			"error", recErr,
		)
	}
	return nil
}
