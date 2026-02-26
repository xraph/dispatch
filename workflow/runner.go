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
// The run is stamped with the latest registered version.
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
		Version:    r.registry.LatestVersion(name),
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
// It injects the Runner itself as the ChildStarter, and triggers saga
// compensations on workflow failure.
func (r *Runner) executeRun(ctx context.Context, run *Run, runner RunnerFunc, input []byte) {
	// Restore scope into context.
	ctx = scope.Restore(ctx, run.ScopeAppID, run.ScopeOrgID)

	start := time.Now()

	wf := NewWorkflowContext(ctx, run, r.store, r.eventStore, r.emitter, r.logger)
	wf.SetChildStarter(r) // Inject self as child starter.

	err := runner(wf, input)
	elapsed := time.Since(start)

	now := time.Now().UTC()

	if err != nil {
		// Run saga compensations before marking as failed.
		if len(wf.Compensations()) > 0 {
			r.logger.Info("running saga compensations",
				slog.String("run_id", run.ID.String()),
				slog.Int("count", len(wf.Compensations())),
			)
			if compErr := wf.RunCompensations(); compErr != nil {
				r.logger.Error("compensation errors during workflow failure",
					slog.String("run_id", run.ID.String()),
					slog.String("error", compErr.Error()),
				)
			}
		}

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

// StartChildRaw starts a child workflow synchronously, blocking until
// it completes. The child run's ParentRunID is set to the given parent.
// Implements ChildStarter.
func (r *Runner) StartChildRaw(ctx context.Context, parentRunID id.RunID, name string, input []byte) (*Run, error) {
	runner, ok := r.registry.Get(name)
	if !ok {
		return nil, fmt.Errorf("no workflow registered for %q", name)
	}

	appID, orgID := scope.Capture(ctx)

	now := time.Now().UTC()
	run := &Run{
		Entity:      dispatch.NewEntity(),
		ID:          id.NewRunID(),
		Name:        name,
		State:       RunStateRunning,
		Input:       input,
		Version:     r.registry.LatestVersion(name),
		ScopeAppID:  appID,
		ScopeOrgID:  orgID,
		StartedAt:   now,
		ParentRunID: &parentRunID,
	}

	if err := r.store.CreateRun(ctx, run); err != nil {
		return nil, fmt.Errorf("create child run for workflow %q: %w", name, err)
	}

	r.emitter.EmitWorkflowStarted(ctx, run)

	// Execute synchronously (blocks until child completes).
	r.executeRun(ctx, run, runner, input)

	return run, nil
}

// SpawnChildRaw starts a child workflow asynchronously, returning immediately
// after the run is persisted. The child executes in a background goroutine.
// Implements ChildStarter.
func (r *Runner) SpawnChildRaw(ctx context.Context, parentRunID id.RunID, name string, input []byte) (*Run, error) {
	runner, ok := r.registry.Get(name)
	if !ok {
		return nil, fmt.Errorf("no workflow registered for %q", name)
	}

	appID, orgID := scope.Capture(ctx)

	now := time.Now().UTC()
	run := &Run{
		Entity:      dispatch.NewEntity(),
		ID:          id.NewRunID(),
		Name:        name,
		State:       RunStateRunning,
		Input:       input,
		Version:     r.registry.LatestVersion(name),
		ScopeAppID:  appID,
		ScopeOrgID:  orgID,
		StartedAt:   now,
		ParentRunID: &parentRunID,
	}

	if err := r.store.CreateRun(ctx, run); err != nil {
		return nil, fmt.Errorf("create spawned run for workflow %q: %w", name, err)
	}

	r.emitter.EmitWorkflowStarted(ctx, run)

	// Execute in background goroutine.
	go r.executeRun(ctx, run, runner, input)

	return run, nil
}

// Resume resumes a workflow run that was in "running" state (crash recovery).
// It re-executes the handler; steps with checkpoints are skipped automatically.
// The run continues on its stamped version (not necessarily the latest).
func (r *Runner) Resume(ctx context.Context, runID id.RunID) error {
	run, err := r.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("get run %s: %w", runID, err)
	}
	if run.State != RunStateRunning {
		return fmt.Errorf("run %s is in state %q, not running", runID, run.State)
	}

	// Use version-aware lookup so existing runs continue on their version.
	runner, ok := r.registry.GetVersion(run.Name, run.Version)
	if !ok {
		return fmt.Errorf("no workflow registered for %q version %d (run %s)", run.Name, run.Version, runID)
	}

	r.executeRun(ctx, run, runner, run.Input)
	return nil
}

// MigrateRun upgrades a workflow run to a new version. It updates the
// run's Version field and re-executes on the new handler. Checkpoints
// are preserved so completed steps are skipped during replay on the
// new version's handler.
func (r *Runner) MigrateRun(ctx context.Context, runID id.RunID, toVersion int) error {
	run, err := r.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("get run %s: %w", runID, err)
	}

	// Verify the target version exists.
	runner, ok := r.registry.GetVersion(run.Name, toVersion)
	if !ok {
		return fmt.Errorf("no workflow registered for %q version %d", run.Name, toVersion)
	}

	// Update version and reset state.
	run.Version = toVersion
	run.State = RunStateRunning
	run.Error = ""
	run.CompletedAt = nil

	if err := r.store.UpdateRun(ctx, run); err != nil {
		return fmt.Errorf("update run %s version to %d: %w", runID, toVersion, err)
	}

	r.emitter.EmitWorkflowStarted(ctx, run)
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
