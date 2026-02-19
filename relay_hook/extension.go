package relayhook

import (
	"context"
	"time"

	"github.com/xraph/relay"
	"github.com/xraph/relay/event"

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

// Extension bridges Dispatch lifecycle events to Relay for webhook
// delivery. Each lifecycle hook emits a typed event via [relay.Relay.Send].
type Extension struct {
	relay    *relay.Relay
	enabled  map[string]bool        // nil = all enabled
	payloads map[string]PayloadFunc // custom payload builders
}

// New creates an Extension that emits Dispatch lifecycle events
// through the provided Relay instance.
func New(r *relay.Relay, opts ...Option) *Extension {
	h := &Extension{relay: r}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Name implements ext.Extension.
func (h *Extension) Name() string { return "relay-hook" }

// ── Job lifecycle hooks ─────────────────────────────

// OnJobEnqueued implements ext.JobEnqueued.
func (h *Extension) OnJobEnqueued(ctx context.Context, j *job.Job) error {
	return h.send(ctx, EventJobEnqueued, j.ScopeOrgID, newJobPayload(j))
}

// OnJobStarted implements ext.JobStarted.
func (h *Extension) OnJobStarted(ctx context.Context, j *job.Job) error {
	return h.send(ctx, EventJobStarted, j.ScopeOrgID, newJobPayload(j))
}

// OnJobCompleted implements ext.JobCompleted.
func (h *Extension) OnJobCompleted(ctx context.Context, j *job.Job, elapsed time.Duration) error {
	return h.send(ctx, EventJobCompleted, j.ScopeOrgID, &jobCompletedPayload{
		jobPayload: *newJobPayload(j),
		ElapsedMs:  elapsed.Milliseconds(),
	})
}

// OnJobFailed implements ext.JobFailed.
func (h *Extension) OnJobFailed(ctx context.Context, j *job.Job, jobErr error) error {
	return h.send(ctx, EventJobFailed, j.ScopeOrgID, &jobFailedPayload{
		jobPayload: *newJobPayload(j),
		Error:      jobErr.Error(),
	})
}

// OnJobRetrying implements ext.JobRetrying.
func (h *Extension) OnJobRetrying(ctx context.Context, j *job.Job, attempt int, nextRunAt time.Time) error {
	return h.send(ctx, EventJobRetrying, j.ScopeOrgID, &jobRetryingPayload{
		jobPayload: *newJobPayload(j),
		Attempt:    attempt,
		NextRunAt:  nextRunAt.Format(time.RFC3339),
	})
}

// OnJobDLQ implements ext.JobDLQ.
func (h *Extension) OnJobDLQ(ctx context.Context, j *job.Job, jobErr error) error {
	return h.send(ctx, EventJobDLQ, j.ScopeOrgID, &jobDLQPayload{
		jobPayload: *newJobPayload(j),
		Error:      jobErr.Error(),
	})
}

// ── Workflow lifecycle hooks ────────────────────────

// OnWorkflowStarted implements ext.WorkflowStarted.
func (h *Extension) OnWorkflowStarted(ctx context.Context, r *workflow.Run) error {
	return h.send(ctx, EventWorkflowStarted, r.ScopeOrgID, newWorkflowPayload(r))
}

// OnWorkflowStepCompleted implements ext.WorkflowStepCompleted.
func (h *Extension) OnWorkflowStepCompleted(ctx context.Context, r *workflow.Run, stepName string, elapsed time.Duration) error {
	return h.send(ctx, EventWorkflowStepCompleted, r.ScopeOrgID, &workflowStepPayload{
		workflowPayload: *newWorkflowPayload(r),
		StepName:        stepName,
		ElapsedMs:       elapsed.Milliseconds(),
	})
}

// OnWorkflowStepFailed implements ext.WorkflowStepFailed.
func (h *Extension) OnWorkflowStepFailed(ctx context.Context, r *workflow.Run, stepName string, stepErr error) error {
	return h.send(ctx, EventWorkflowStepFailed, r.ScopeOrgID, &workflowStepPayload{
		workflowPayload: *newWorkflowPayload(r),
		StepName:        stepName,
		Error:           stepErr.Error(),
	})
}

// OnWorkflowCompleted implements ext.WorkflowCompleted.
func (h *Extension) OnWorkflowCompleted(ctx context.Context, r *workflow.Run, elapsed time.Duration) error {
	return h.send(ctx, EventWorkflowCompleted, r.ScopeOrgID, &workflowCompletedPayload{
		workflowPayload: *newWorkflowPayload(r),
		ElapsedMs:       elapsed.Milliseconds(),
	})
}

// OnWorkflowFailed implements ext.WorkflowFailed.
func (h *Extension) OnWorkflowFailed(ctx context.Context, r *workflow.Run, runErr error) error {
	return h.send(ctx, EventWorkflowFailed, r.ScopeOrgID, &workflowFailedPayload{
		workflowPayload: *newWorkflowPayload(r),
		Error:           runErr.Error(),
	})
}

// ── Cron lifecycle hooks ────────────────────────────

// OnCronFired implements ext.CronFired.
func (h *Extension) OnCronFired(ctx context.Context, entryName string, jobID id.JobID) error {
	return h.send(ctx, EventCronFired, "", &cronPayload{
		EntryName: entryName,
		JobID:     jobID.String(),
	})
}

// ── Internal helpers ────────────────────────────────

// send emits an event through Relay if the event type is enabled.
func (h *Extension) send(ctx context.Context, eventType, tenantID string, defaultData any) error {
	if h.enabled != nil && !h.enabled[eventType] {
		return nil
	}

	data := defaultData
	if fn, ok := h.payloads[eventType]; ok {
		custom, err := fn(defaultData)
		if err != nil {
			return err
		}
		data = custom
	}

	return h.relay.Send(ctx, &event.Event{
		Type:     eventType,
		TenantID: tenantID,
		Data:     data,
	})
}

// ── Default payload types ───────────────────────────

type jobPayload struct {
	JobID      string `json:"job_id"`
	JobName    string `json:"job_name"`
	Queue      string `json:"queue"`
	ScopeAppID string `json:"scope_app_id,omitempty"`
	ScopeOrgID string `json:"scope_org_id,omitempty"`
}

func newJobPayload(j *job.Job) *jobPayload {
	return &jobPayload{
		JobID:      j.ID.String(),
		JobName:    j.Name,
		Queue:      j.Queue,
		ScopeAppID: j.ScopeAppID,
		ScopeOrgID: j.ScopeOrgID,
	}
}

type jobCompletedPayload struct {
	jobPayload
	ElapsedMs int64 `json:"elapsed_ms"`
}

type jobFailedPayload struct {
	jobPayload
	Error string `json:"error"`
}

type jobRetryingPayload struct {
	jobPayload
	Attempt   int    `json:"attempt"`
	NextRunAt string `json:"next_run_at"`
}

type jobDLQPayload struct {
	jobPayload
	Error string `json:"error"`
}

type workflowPayload struct {
	RunID      string `json:"run_id"`
	Name       string `json:"name"`
	ScopeAppID string `json:"scope_app_id,omitempty"`
	ScopeOrgID string `json:"scope_org_id,omitempty"`
}

func newWorkflowPayload(r *workflow.Run) *workflowPayload {
	return &workflowPayload{
		RunID:      r.ID.String(),
		Name:       r.Name,
		ScopeAppID: r.ScopeAppID,
		ScopeOrgID: r.ScopeOrgID,
	}
}

type workflowCompletedPayload struct {
	workflowPayload
	ElapsedMs int64 `json:"elapsed_ms"`
}

type workflowFailedPayload struct {
	workflowPayload
	Error string `json:"error"`
}

type workflowStepPayload struct {
	workflowPayload
	StepName  string `json:"step_name"`
	ElapsedMs int64  `json:"elapsed_ms,omitempty"`
	Error     string `json:"error,omitempty"`
}

type cronPayload struct {
	EntryName string `json:"entry_name"`
	JobID     string `json:"job_id"`
}
