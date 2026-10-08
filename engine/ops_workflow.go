package engine

import (
	"context"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

// PlanWorkflowReplay reports what ReplayWorkflowFrom would do for the run
// and step, without changing anything. See workflow.Runner.PlanReplay for
// the refusals; a running run plans fine and its State says a replay
// would be refused.
func (eng *Engine) PlanWorkflowReplay(ctx context.Context, runID id.RunID, fromStep string) (*workflow.ReplayPlan, error) {
	return eng.wfRunner.PlanReplay(ctx, runID, fromStep)
}

// ReplayWorkflowFrom re-runs a finished run from a step on the run's
// stamped version, keeping the checkpoints up to and including fromStep.
// It returns once the replay has started on the runner's background
// launcher; Stop waits for it. Refusals are workflow.Runner.ReplayFrom's:
// dispatch.ErrRunNotFound, dispatch.ErrInvalidState (running, claimed by
// another replay, no checkpoint for the step, version not registered),
// or workflow.ErrRunnerShutdown after Stop.
//
// On success it emits one ext.ActionWorkflowReplayed with the run and step.
func (eng *Engine) ReplayWorkflowFrom(ctx context.Context, runID id.RunID, fromStep string) (*workflow.ReplayPlan, error) {
	plan, err := eng.wfRunner.ReplayFrom(ctx, runID, fromStep)
	if err != nil {
		return nil, err
	}

	eng.emitWorkflowReplay(ctx, runID, fromStep)
	return plan, nil
}

// ReplayWorkflowFromGeneration starts replay only if the reviewed generation
// still matches. It emits an operator action only after a successful launch.
func (eng *Engine) ReplayWorkflowFromGeneration(ctx context.Context, runID id.RunID, fromStep string, generation int64) (*workflow.ReplayPlan, error) {
	plan, err := eng.wfRunner.ReplayFromGeneration(ctx, runID, fromStep, generation)
	if err != nil {
		return nil, err
	}
	eng.emitWorkflowReplay(ctx, runID, fromStep)
	return plan, nil
}

func (eng *Engine) emitWorkflowReplay(ctx context.Context, runID id.RunID, fromStep string) {
	eng.extensions.EmitOperatorAction(ctx, ext.Action{
		Kind: ext.ActionWorkflowReplayed, RunID: runID, Step: fromStep,
	})
}
