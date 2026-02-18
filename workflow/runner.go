package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/scope"
)

// RunEmitter emits workflow-level lifecycle events.
// This interface is satisfied by ext.Registry (via an adapter in the
// engine package) to break the import cycle between workflow and ext.
type RunEmitter interface {
	StepEmitter
	EmitWorkflowStarted(ctx context.Context, run *Run)
	EmitWorkflowCompleted(ctx context.Context, run *Run, elapsed time.Duration)
	EmitWorkflowFailed(ctx context.Context, run *Run, err error)
}

// Runner orchestrates workflow execution: creating runs, building
// the Workflow context, invoking handlers, and managing state.
type Runner struct {
	registry   *Registry
	store      Store
	eventStore event.Store
	emitter    RunEmitter
	logger     *slog.Logger
}

// NewRunner creates a workflow runner.
func NewRunner(
	registry *Registry,
	store Store,
	eventStore event.Store,
	emitter RunEmitter,
	logger *slog.Logger,
) *Runner {
	return &Runner{
		registry:   registry,
		store:      store,
		eventStore: eventStore,
		emitter:    emitter,
		logger:     logger,
	}
}

// Registry returns the workflow registry.
func (r *Runner) Registry() *Registry { return r.registry }

// Start starts a new workflow run with a typed input.
// The input is JSON-marshaled and stored on the Run.
func Start[T any](ctx context.Context, runner *Runner, name string, input T) (*Run, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal input for workflow %q: %w", name, err)
	}

	return runner.StartRaw(ctx, name, data)
}

// StartRaw starts a workflow run with pre-serialized JSON input.
func (r *Runner) StartRaw(ctx context.Context, name string, input []byte) (*Run, error) {
	runner, ok := r.registry.Get(name)
	if !ok {
		return nil, fmt.Errorf("no workflow registered for %q", name)
	}

	// Capture scope.
	appID, orgID := scope.Capture(ctx)

	now := time.Now().UTC()
	run := &Run{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewRunID(),
		Name:       name,
		State:      RunStateRunning,
		Input:      input,
		ScopeAppID: appID,
		ScopeOrgID: orgID,
		StartedAt:  now,
	}

	if err := r.store.CreateRun(ctx, run); err != nil {
		return nil, fmt.Errorf("create run for workflow %q: %w", name, err)
	}

	r.emitter.EmitWorkflowStarted(ctx, run)

	// Execute the workflow synchronously.
	r.executeRun(ctx, run, runner, input)

	return run, nil
}

// executeRun runs the workflow handler and handles completion/failure.
func (r *Runner) executeRun(ctx context.Context, run *Run, runner RunnerFunc, input []byte) {
	// Restore scope into context.
	ctx = scope.Restore(ctx, run.ScopeAppID, run.ScopeOrgID)

	start := time.Now()

	wf := NewWorkflowContext(ctx, run, r.store, r.eventStore, r.emitter, r.logger)

	err := runner(wf, input)
	elapsed := time.Since(start)

	now := time.Now().UTC()

	if err != nil {
		run.State = RunStateFailed
		run.Error = err.Error()
		run.CompletedAt = &now
		if updateErr := r.store.UpdateRun(ctx, run); updateErr != nil {
			r.logger.Error("failed to update run as failed",
				slog.String("run_id", run.ID.String()),
				slog.String("error", updateErr.Error()),
			)
		}
		r.emitter.EmitWorkflowFailed(ctx, run, err)
		return
	}

	run.State = RunStateCompleted
	run.CompletedAt = &now
	if updateErr := r.store.UpdateRun(ctx, run); updateErr != nil {
		r.logger.Error("failed to update run as completed",
			slog.String("run_id", run.ID.String()),
			slog.String("error", updateErr.Error()),
		)
	}
	r.emitter.EmitWorkflowCompleted(ctx, run, elapsed)
}

// Resume resumes a workflow run that was in "running" state (crash recovery).
// It re-executes the handler; steps with checkpoints are skipped automatically.
func (r *Runner) Resume(ctx context.Context, runID id.RunID) error {
	run, err := r.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("get run %s: %w", runID, err)
	}
	if run.State != RunStateRunning {
		return fmt.Errorf("run %s is in state %q, not running", runID, run.State)
	}

	runner, ok := r.registry.Get(run.Name)
	if !ok {
		return fmt.Errorf("no workflow registered for %q (run %s)", run.Name, runID)
	}

	r.executeRun(ctx, run, runner, run.Input)
	return nil
}

// ResumeAll finds all runs in "running" state and resumes them.
// Called at startup for crash recovery.
func (r *Runner) ResumeAll(ctx context.Context) error {
	runs, err := r.store.ListRuns(ctx, ListOpts{State: RunStateRunning})
	if err != nil {
		return fmt.Errorf("list running workflow runs: %w", err)
	}

	for _, run := range runs {
		r.logger.Info("resuming workflow run",
			slog.String("run_id", run.ID.String()),
			slog.String("workflow", run.Name),
		)
		if resumeErr := r.Resume(ctx, run.ID); resumeErr != nil {
			r.logger.Error("failed to resume workflow run",
				slog.String("run_id", run.ID.String()),
				slog.String("error", resumeErr.Error()),
			)
		}
	}

	return nil
}
