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

// Workflow is the execution context passed to workflow handler functions.
// It provides durable step execution, parallel fan-out, event waiting,
// and durable sleep. Each method automatically checkpoints results to
// enable crash recovery.
type Workflow struct {
	ctx        context.Context
	run        *Run
	store      Store
	eventStore event.Store
	emitter    StepEmitter
	logger     *slog.Logger
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

// Context returns the underlying context.Context.
func (w *Workflow) Context() context.Context { return w.ctx }

// RunID returns the workflow run ID.
func (w *Workflow) RunID() id.RunID { return w.run.ID }

// Run returns the workflow run.
func (w *Workflow) Run() *Run { return w.run }
