package contract

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

type WorkflowsListInput struct {
	State      workflow.RunState `json:"state"`
	NamePrefix string            `json:"namePrefix"`
	ScopeAppID string            `json:"scopeAppId"`
	ScopeOrgID string            `json:"scopeOrgId"`
	Cursor     string            `json:"cursor"`
	Limit      int               `json:"limit"`
}
type WorkflowReplayInput struct {
	ID       string `json:"id"`
	FromStep string `json:"fromStep"`
}
type WorkflowReplayCommandInput struct {
	WorkflowReplayInput
	ExpectedGeneration *int64 `json:"expectedGeneration"`
}

func parseRunID(raw string) (id.RunID, error) {
	parsed, err := id.ParseRunID(raw)
	if err != nil || parsed.IsNil() {
		return id.RunID{}, badRequest("id must be a workflow run ID")
	}
	return parsed, nil
}
func workflowsListHandler(deps Deps) func(context.Context, WorkflowsListInput, fc.Principal) (Page[WorkflowRow], error) {
	return handle(deps, "workflows.list", false, func(ctx context.Context, input WorkflowsListInput, _ fc.Principal) (Page[WorkflowRow], error) {
		limit, err := pageLimit(input.Limit)
		if err != nil {
			return Page[WorkflowRow]{}, err
		}
		switch input.State {
		case "", workflow.RunStateRunning, workflow.RunStateCompleted, workflow.RunStateFailed:
		default:
			return Page[WorkflowRow]{}, badRequest("unknown workflow state")
		}
		page, err := deps.Store.ListRunsPage(ctx, workflow.ListRunsPageOpts{State: input.State, NamePrefix: input.NamePrefix, ScopeAppID: input.ScopeAppID,
			ScopeOrgID: input.ScopeOrgID, Cursor: input.Cursor, Limit: limit})
		if err != nil {
			return Page[WorkflowRow]{}, err
		}
		at := time.Now()
		rows := make([]WorkflowRow, 0, len(page.Runs))
		for _, run := range page.Runs {
			rows = append(rows, projectWorkflow(run, at))
		}
		return newPage(rows, page.NextCursor, page.Complete, at), nil
	})
}
func workflowsGetHandler(deps Deps) func(context.Context, IDInput, fc.Principal) (WorkflowDetail, error) {
	return handle(deps, "workflows.get", false, func(ctx context.Context, input IDInput, _ fc.Principal) (WorkflowDetail, error) {
		runID, err := parseRunID(input.ID)
		if err != nil {
			return WorkflowDetail{}, err
		}
		run, err := deps.Store.GetRun(ctx, runID)
		if err != nil {
			return WorkflowDetail{}, err
		}
		checkpoints, err := deps.Store.ListCheckpoints(ctx, runID)
		if err != nil {
			return WorkflowDetail{}, err
		}
		children, err := deps.Store.ListChildRuns(ctx, runID)
		if err != nil {
			return WorkflowDetail{}, err
		}
		// Detach before sorting: custom stores may share their slice containers.
		checkpoints = slices.Clone(checkpoints)
		slices.SortFunc(checkpoints, workflow.CompareCheckpoints)
		children = slices.Clone(children)
		slices.SortFunc(children, func(a, b *workflow.Run) int {
			if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
				return c
			}
			return strings.Compare(a.ID.String(), b.ID.String())
		})
		at := time.Now()
		row := projectWorkflow(run, at)
		_, registered := deps.Engine.WorkflowRunner().Registry().GetVersion(run.Name, row.Version)
		out := WorkflowDetail{WorkflowRow: row, Input: projectPayload(run.Input, false), Output: projectPayload(run.Output, false), Error: nullable(run.Error),
			Checkpoints: []WorkflowCheckpoint{}, Children: []WorkflowRow{}, VersionRegistered: registered, AsOf: at.UTC().Format(time.RFC3339Nano)}
		for _, cp := range checkpoints {
			out.Checkpoints = append(out.Checkpoints, WorkflowCheckpoint{ID: cp.ID.String(), StepName: cp.StepName,
				CreatedAt: timestamp(cp.CreatedAt), Payload: projectPayload(cp.Data, true)})
		}
		for _, child := range children {
			out.Children = append(out.Children, projectWorkflow(child, at))
		}
		return out, nil
	})
}
func workflowConflict(ctx context.Context, deps Deps, runID id.RunID) error {
	current, err := deps.Store.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	return &fc.Error{Code: fc.CodeConflict, Message: "the run changed or cannot replay from this checkpoint",
		Details: map[string]any{"state": current.State, "generation": current.ReplayGeneration}}
}
func validateReplayInput(input WorkflowReplayInput) (id.RunID, error) {
	runID, err := parseRunID(input.ID)
	if err != nil {
		return id.RunID{}, err
	}
	if strings.TrimSpace(input.FromStep) == "" {
		return id.RunID{}, badRequest("fromStep must name a checkpointed step")
	}
	return runID, nil
}
func workflowsReplayPreviewHandler(deps Deps) func(context.Context, WorkflowReplayInput, fc.Principal) (WorkflowReplayPreview, error) {
	return handle(deps, "workflows.replayPreview", false, func(ctx context.Context, input WorkflowReplayInput, _ fc.Principal) (WorkflowReplayPreview, error) {
		runID, err := validateReplayInput(input)
		if err != nil {
			return WorkflowReplayPreview{}, err
		}
		plan, err := deps.Engine.PlanWorkflowReplay(ctx, runID, input.FromStep)
		if errors.Is(err, dispatch.ErrInvalidState) {
			return WorkflowReplayPreview{}, workflowConflict(ctx, deps, runID)
		}
		if err != nil {
			return WorkflowReplayPreview{}, err
		}
		return projectWorkflowReplay(plan, time.Now()), nil
	})
}
func workflowsReplayFromHandler(deps Deps) func(context.Context, WorkflowReplayCommandInput, fc.Principal) (WorkflowReplayResult, error) {
	return handle(deps, "workflows.replayFrom", true, func(ctx context.Context, input WorkflowReplayCommandInput, _ fc.Principal) (WorkflowReplayResult, error) {
		runID, err := validateReplayInput(input.WorkflowReplayInput)
		if err != nil {
			return WorkflowReplayResult{}, err
		}
		if input.ExpectedGeneration == nil || *input.ExpectedGeneration < 0 {
			return WorkflowReplayResult{}, badRequest("expectedGeneration is required and must not be negative")
		}
		plan, err := deps.Engine.ReplayWorkflowFromGeneration(ctx, runID, input.FromStep, *input.ExpectedGeneration)
		if errors.Is(err, dispatch.ErrInvalidState) {
			return WorkflowReplayResult{}, workflowConflict(ctx, deps, runID)
		}
		if err != nil {
			return WorkflowReplayResult{}, err
		}
		at := time.Now()
		return WorkflowReplayResult{Plan: projectWorkflowReplay(plan, at), AcceptedGeneration: plan.Generation + 1, AsOf: at.UTC().Format(time.RFC3339Nano)}, nil
	})
}
