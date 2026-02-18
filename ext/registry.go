package ext

import (
	"context"
	"log/slog"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Named entry types pair a hook implementation with the extension name
// captured at registration time. This avoids type-asserting back to
// Extension inside the emit methods.
type jobEnqueuedEntry struct {
	name string
	hook JobEnqueued
}

type jobStartedEntry struct {
	name string
	hook JobStarted
}

type jobCompletedEntry struct {
	name string
	hook JobCompleted
}

type jobFailedEntry struct {
	name string
	hook JobFailed
}

type jobRetryingEntry struct {
	name string
	hook JobRetrying
}

type jobDLQEntry struct {
	name string
	hook JobDLQ
}

type workflowStartedEntry struct {
	name string
	hook WorkflowStarted
}

type workflowStepCompletedEntry struct {
	name string
	hook WorkflowStepCompleted
}

type workflowStepFailedEntry struct {
	name string
	hook WorkflowStepFailed
}

type workflowCompletedEntry struct {
	name string
	hook WorkflowCompleted
}

type workflowFailedEntry struct {
	name string
	hook WorkflowFailed
}

type cronFiredEntry struct {
	name string
	hook CronFired
}

type shutdownEntry struct {
	name string
	hook Shutdown
}

// Registry holds registered extensions and dispatches lifecycle events
// to them. It type-caches extensions at registration time so emit calls
// iterate only over extensions that implement the relevant hook.
type Registry struct {
	extensions []Extension
	logger     *slog.Logger

	// Type-cached slices for each lifecycle hook.
	jobEnqueued           []jobEnqueuedEntry
	jobStarted            []jobStartedEntry
	jobCompleted          []jobCompletedEntry
	jobFailed             []jobFailedEntry
	jobRetrying           []jobRetryingEntry
	jobDLQ                []jobDLQEntry
	workflowStarted       []workflowStartedEntry
	workflowStepCompleted []workflowStepCompletedEntry
	workflowStepFailed    []workflowStepFailedEntry
	workflowCompleted     []workflowCompletedEntry
	workflowFailed        []workflowFailedEntry
	cronFired             []cronFiredEntry
	shutdown              []shutdownEntry
}

// NewRegistry creates an extension registry with the given logger.
func NewRegistry(logger *slog.Logger) *Registry {
	return &Registry{logger: logger}
}

// Register adds an extension and type-asserts it into all applicable
// hook caches. Extensions are notified in registration order.
func (r *Registry) Register(e Extension) {
	r.extensions = append(r.extensions, e)
	name := e.Name()

	if h, ok := e.(JobEnqueued); ok {
		r.jobEnqueued = append(r.jobEnqueued, jobEnqueuedEntry{name, h})
	}
	if h, ok := e.(JobStarted); ok {
		r.jobStarted = append(r.jobStarted, jobStartedEntry{name, h})
	}
	if h, ok := e.(JobCompleted); ok {
		r.jobCompleted = append(r.jobCompleted, jobCompletedEntry{name, h})
	}
	if h, ok := e.(JobFailed); ok {
		r.jobFailed = append(r.jobFailed, jobFailedEntry{name, h})
	}
	if h, ok := e.(JobRetrying); ok {
		r.jobRetrying = append(r.jobRetrying, jobRetryingEntry{name, h})
	}
	if h, ok := e.(JobDLQ); ok {
		r.jobDLQ = append(r.jobDLQ, jobDLQEntry{name, h})
	}
	if h, ok := e.(WorkflowStarted); ok {
		r.workflowStarted = append(r.workflowStarted, workflowStartedEntry{name, h})
	}
	if h, ok := e.(WorkflowStepCompleted); ok {
		r.workflowStepCompleted = append(r.workflowStepCompleted, workflowStepCompletedEntry{name, h})
	}
	if h, ok := e.(WorkflowStepFailed); ok {
		r.workflowStepFailed = append(r.workflowStepFailed, workflowStepFailedEntry{name, h})
	}
	if h, ok := e.(WorkflowCompleted); ok {
		r.workflowCompleted = append(r.workflowCompleted, workflowCompletedEntry{name, h})
	}
	if h, ok := e.(WorkflowFailed); ok {
		r.workflowFailed = append(r.workflowFailed, workflowFailedEntry{name, h})
	}
	if h, ok := e.(CronFired); ok {
		r.cronFired = append(r.cronFired, cronFiredEntry{name, h})
	}
	if h, ok := e.(Shutdown); ok {
		r.shutdown = append(r.shutdown, shutdownEntry{name, h})
	}
}

// Extensions returns all registered extensions.
func (r *Registry) Extensions() []Extension { return r.extensions }

// ──────────────────────────────────────────────────
// Job event emitters
// ──────────────────────────────────────────────────

// EmitJobEnqueued notifies all extensions that implement JobEnqueued.
func (r *Registry) EmitJobEnqueued(ctx context.Context, j *job.Job) {
	for _, e := range r.jobEnqueued {
		if err := e.hook.OnJobEnqueued(ctx, j); err != nil {
			r.logHookError("OnJobEnqueued", e.name, err)
		}
	}
}

// EmitJobStarted notifies all extensions that implement JobStarted.
func (r *Registry) EmitJobStarted(ctx context.Context, j *job.Job) {
	for _, e := range r.jobStarted {
		if err := e.hook.OnJobStarted(ctx, j); err != nil {
			r.logHookError("OnJobStarted", e.name, err)
		}
	}
}

// EmitJobCompleted notifies all extensions that implement JobCompleted.
func (r *Registry) EmitJobCompleted(ctx context.Context, j *job.Job, elapsed time.Duration) {
	for _, e := range r.jobCompleted {
		if err := e.hook.OnJobCompleted(ctx, j, elapsed); err != nil {
			r.logHookError("OnJobCompleted", e.name, err)
		}
	}
}

// EmitJobFailed notifies all extensions that implement JobFailed.
func (r *Registry) EmitJobFailed(ctx context.Context, j *job.Job, jobErr error) {
	for _, e := range r.jobFailed {
		if err := e.hook.OnJobFailed(ctx, j, jobErr); err != nil {
			r.logHookError("OnJobFailed", e.name, err)
		}
	}
}

// EmitJobRetrying notifies all extensions that implement JobRetrying.
func (r *Registry) EmitJobRetrying(ctx context.Context, j *job.Job, attempt int, nextRunAt time.Time) {
	for _, e := range r.jobRetrying {
		if err := e.hook.OnJobRetrying(ctx, j, attempt, nextRunAt); err != nil {
			r.logHookError("OnJobRetrying", e.name, err)
		}
	}
}

// EmitJobDLQ notifies all extensions that implement JobDLQ.
func (r *Registry) EmitJobDLQ(ctx context.Context, j *job.Job, jobErr error) {
	for _, e := range r.jobDLQ {
		if err := e.hook.OnJobDLQ(ctx, j, jobErr); err != nil {
			r.logHookError("OnJobDLQ", e.name, err)
		}
	}
}

// ──────────────────────────────────────────────────
// Workflow event emitters
// ──────────────────────────────────────────────────

// EmitWorkflowStarted notifies all extensions that implement WorkflowStarted.
func (r *Registry) EmitWorkflowStarted(ctx context.Context, run *workflow.Run) {
	for _, e := range r.workflowStarted {
		if err := e.hook.OnWorkflowStarted(ctx, run); err != nil {
			r.logHookError("OnWorkflowStarted", e.name, err)
		}
	}
}

// EmitWorkflowStepCompleted notifies all extensions that implement WorkflowStepCompleted.
func (r *Registry) EmitWorkflowStepCompleted(ctx context.Context, run *workflow.Run, stepName string, elapsed time.Duration) {
	for _, e := range r.workflowStepCompleted {
		if err := e.hook.OnWorkflowStepCompleted(ctx, run, stepName, elapsed); err != nil {
			r.logHookError("OnWorkflowStepCompleted", e.name, err)
		}
	}
}

// EmitWorkflowStepFailed notifies all extensions that implement WorkflowStepFailed.
func (r *Registry) EmitWorkflowStepFailed(ctx context.Context, run *workflow.Run, stepName string, stepErr error) {
	for _, e := range r.workflowStepFailed {
		if err := e.hook.OnWorkflowStepFailed(ctx, run, stepName, stepErr); err != nil {
			r.logHookError("OnWorkflowStepFailed", e.name, err)
		}
	}
}

// EmitWorkflowCompleted notifies all extensions that implement WorkflowCompleted.
func (r *Registry) EmitWorkflowCompleted(ctx context.Context, run *workflow.Run, elapsed time.Duration) {
	for _, e := range r.workflowCompleted {
		if err := e.hook.OnWorkflowCompleted(ctx, run, elapsed); err != nil {
			r.logHookError("OnWorkflowCompleted", e.name, err)
		}
	}
}

// EmitWorkflowFailed notifies all extensions that implement WorkflowFailed.
func (r *Registry) EmitWorkflowFailed(ctx context.Context, run *workflow.Run, runErr error) {
	for _, e := range r.workflowFailed {
		if err := e.hook.OnWorkflowFailed(ctx, run, runErr); err != nil {
			r.logHookError("OnWorkflowFailed", e.name, err)
		}
	}
}

// ──────────────────────────────────────────────────
// Other event emitters
// ──────────────────────────────────────────────────

// EmitCronFired notifies all extensions that implement CronFired.
func (r *Registry) EmitCronFired(ctx context.Context, entryName string, jobID id.JobID) {
	for _, e := range r.cronFired {
		if err := e.hook.OnCronFired(ctx, entryName, jobID); err != nil {
			r.logHookError("OnCronFired", e.name, err)
		}
	}
}

// EmitShutdown notifies all extensions that implement Shutdown.
func (r *Registry) EmitShutdown(ctx context.Context) {
	for _, e := range r.shutdown {
		if err := e.hook.OnShutdown(ctx); err != nil {
			r.logHookError("OnShutdown", e.name, err)
		}
	}
}

// logHookError logs a warning when a lifecycle hook returns an error.
// Errors from hooks are never propagated — they must not block the pipeline.
func (r *Registry) logHookError(hook, extName string, err error) {
	r.logger.Warn("extension hook error",
		slog.String("hook", hook),
		slog.String("extension", extName),
		slog.String("error", err.Error()),
	)
}
