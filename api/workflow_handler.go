// Package api provides HTTP handlers for the Dispatch API.
package api

import (
	"fmt"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

func (a *API) listWorkflowNames(ctx forge.Context) error {
	names := a.eng.WorkflowRunner().Registry().Names()
	return ctx.JSON(http.StatusOK, ListWorkflowNamesResponse{Names: names})
}

func (a *API) listWorkflowRuns(ctx forge.Context, req *ListWorkflowRunsRequest) ([]*workflow.Run, error) {
	var state workflow.RunState
	if req.State != "" {
		state = workflow.RunState(req.State)
	}

	ws, ok := a.eng.Dispatcher().Store().(workflow.Store)
	if !ok {
		return nil, fmt.Errorf("store does not implement workflow.Store")
	}

	runs, err := ws.ListRuns(ctx.Context(), workflow.ListOpts{
		Limit:  defaultLimit(req.Limit),
		Offset: req.Offset,
		State:  state,
	})
	if err != nil {
		return nil, fmt.Errorf("list workflow runs: %w", err)
	}

	return runs, ctx.JSON(http.StatusOK, runs)
}

func (a *API) getWorkflowRun(ctx forge.Context, _ *GetWorkflowRunRequest) (*workflow.Run, error) {
	runID, err := id.ParseRunID(ctx.Param("runId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid run ID: %v", err))
	}

	ws, ok := a.eng.Dispatcher().Store().(workflow.Store)
	if !ok {
		return nil, fmt.Errorf("store does not implement workflow.Store")
	}

	run, err := ws.GetRun(ctx.Context(), runID)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return nil, ctx.JSON(http.StatusOK, run)
}

// planWorkflowReplay says what replaying the run from a step would do,
// without changing anything. A step with no checkpoint, or a run on a
// version no longer registered, answers 409.
func (a *API) planWorkflowReplay(ctx forge.Context, req *PlanWorkflowReplayRequest) (*workflow.ReplayPlan, error) {
	runID, err := id.ParseRunID(ctx.Param("runId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid run ID: %v", err))
	}
	if req.Step == "" {
		return nil, forge.BadRequest("step is required")
	}

	plan, err := a.eng.PlanWorkflowReplay(ctx.Context(), runID, req.Step)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return nil, ctx.JSON(http.StatusOK, plan)
}

// replayWorkflow re-runs a finished run from a step and answers 202 with
// the plan once the replay has started; the run goes on in the
// background. A running run answers 409, as do the plan's refusals, and
// a runner that has shut down answers 503.
func (a *API) replayWorkflow(ctx forge.Context, req *ReplayWorkflowRequest) (*workflow.ReplayPlan, error) {
	runID, err := id.ParseRunID(ctx.Param("runId"))
	if err != nil {
		return nil, forge.BadRequest(fmt.Sprintf("invalid run ID: %v", err))
	}
	if req.Step == "" {
		return nil, forge.BadRequest("step is required")
	}

	plan, err := a.eng.ReplayWorkflowFrom(ctx.Context(), runID, req.Step)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return nil, ctx.JSON(http.StatusAccepted, plan)
}
