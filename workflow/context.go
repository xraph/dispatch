package workflow

import (
	"context"
	"log/slog"
	"time"

	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
)

// StepEmitter is called by the Workflow to emit step lifecycle events.
// This interface is satisfied by ext.Registry (via an adapter in the
// runner package) to break the import cycle between workflow and ext.
type StepEmitter interface {
	EmitStepCompleted(ctx context.Context, run *Run, stepName string, elapsed time.Duration)
	EmitStepFailed(ctx context.Context, run *Run, stepName string, err error)
}

// Compensation represents a deferred undo action for a completed step.
type Compensation struct {
	StepName   string
	Compensate func(ctx context.Context) error
}

// ChildStarter is the interface used by the Workflow to start child
// workflow runs. It is satisfied by the workflow.Runner, injected by
// the engine layer to break import cycles.
type ChildStarter interface {
	// StartChildRaw starts a child workflow synchronously, blocking until it completes.
	// The child run's ParentRunID is set to the given parent.
	StartChildRaw(ctx context.Context, parentRunID id.RunID, name string, input []byte) (*Run, error)

	// SpawnChildRaw starts a child workflow asynchronously, returning immediately
	// after the run is persisted. The child executes in a background goroutine.
	SpawnChildRaw(ctx context.Context, parentRunID id.RunID, name string, input []byte) (*Run, error)
}

// RunGetter retrieves a workflow run by ID. Satisfied by workflow.Store.
type RunGetter interface {
	GetRun(ctx context.Context, runID id.RunID) (*Run, error)
}

// Workflow is the execution context passed to workflow handler functions.
// It provides durable step execution, parallel fan-out, event waiting,
// durable sleep, saga compensations, and child workflow composition.
// Each method automatically checkpoints results to enable crash recovery.
type Workflow struct {
	ctx        context.Context
	run        *Run
	store      Store
	eventStore event.Store
	emitter    StepEmitter
	logger     *slog.Logger

	// childStarter is used by RunChild/SpawnChild to start child workflows.
	childStarter ChildStarter

	// compensations is a LIFO stack of undo actions. On workflow failure,
	// these are executed in reverse order.
	compensations []Compensation
}

// NewWorkflowContext creates a new Workflow execution context.
// This is called by the workflow runner, not by users.
func NewWorkflowContext(
	ctx context.Context,
	run *Run,
	store Store,
	eventStore event.Store,
	emitter StepEmitter,
	logger *slog.Logger,
) *Workflow {
	return &Workflow{
		ctx:        ctx,
		run:        run,
		store:      store,
		eventStore: eventStore,
		emitter:    emitter,
		logger:     logger,
	}
}

// SetChildStarter injects the child workflow starter. Called by the runner.
func (w *Workflow) SetChildStarter(cs ChildStarter) {
	w.childStarter = cs
}

// Context returns the underlying context.Context.
func (w *Workflow) Context() context.Context { return w.ctx }

// RunID returns the workflow run ID.
func (w *Workflow) RunID() id.RunID { return w.run.ID }

// Run returns the workflow run.
func (w *Workflow) Run() *Run { return w.run }

// Compensations returns the list of registered compensations (for runner use).
func (w *Workflow) Compensations() []Compensation { return w.compensations }

// RunCompensations executes all registered compensations in reverse order.
// Each compensation is checkpointed for crash recovery. Called by the runner
// when a workflow fails.
func (w *Workflow) RunCompensations() error {
	for i := len(w.compensations) - 1; i >= 0; i-- {
		comp := w.compensations[i]
		stepName := "compensate:" + comp.StepName

		// Check if already compensated (checkpoint exists).
		data, err := w.store.GetCheckpoint(w.ctx, w.run.ID, stepName)
		if err != nil {
			w.logger.Error("failed to check compensation checkpoint",
				slog.String("step", comp.StepName),
				slog.String("error", err.Error()),
			)
			continue
		}
		if data != nil {
			continue // Already compensated.
		}

		// Execute compensation.
		start := time.Now()
		if compErr := comp.Compensate(w.ctx); compErr != nil {
			w.emitter.EmitStepFailed(w.ctx, w.run, stepName, compErr)
			w.logger.Error("compensation failed",
				slog.String("step", comp.StepName),
				slog.String("error", compErr.Error()),
			)
			continue // Best-effort: continue with remaining compensations.
		}

		// Checkpoint the compensation.
		if saveErr := w.store.SaveCheckpoint(w.ctx, w.run.ID, stepName, []byte{}); saveErr != nil {
			w.logger.Error("failed to checkpoint compensation",
				slog.String("step", comp.StepName),
				slog.String("error", saveErr.Error()),
			)
		}

		elapsed := time.Since(start)
		w.emitter.EmitStepCompleted(w.ctx, w.run, stepName, elapsed)
	}
	return nil
}
