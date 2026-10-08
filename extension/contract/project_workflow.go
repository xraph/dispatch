package contract

import (
	"time"

	"github.com/xraph/dispatch/workflow"
)

type WorkflowRow struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	State            workflow.RunState `json:"state"`
	Version          int               `json:"version"`
	RecordedVersion  int               `json:"recordedVersion"`
	ReplayGeneration int64             `json:"replayGeneration"`
	ParentRunID      *string           `json:"parentRunId"`
	ScopeAppID       *string           `json:"scopeAppId"`
	ScopeOrgID       *string           `json:"scopeOrgId"`
	CreatedAt        *string           `json:"createdAt"`
	UpdatedAt        *string           `json:"updatedAt"`
	StartedAt        *string           `json:"startedAt"`
	CompletedAt      *string           `json:"completedAt"`
	Duration         *Duration         `json:"duration"`
}
type WorkflowCheckpoint struct {
	ID        string  `json:"id"`
	StepName  string  `json:"stepName"`
	CreatedAt *string `json:"createdAt"`
	Payload   Payload `json:"payload"`
}
type WorkflowDetail struct {
	WorkflowRow
	Input             Payload              `json:"input"`
	Output            Payload              `json:"output"`
	Error             *string              `json:"error"`
	Checkpoints       []WorkflowCheckpoint `json:"checkpoints"`
	Children          []WorkflowRow        `json:"children"`
	VersionRegistered bool                 `json:"versionRegistered"`
	AsOf              string               `json:"asOf"`
}
type WorkflowReplayPreview struct {
	RunID      string            `json:"runId"`
	FromStep   string            `json:"fromStep"`
	Version    int               `json:"version"`
	Generation int64             `json:"generation"`
	State      workflow.RunState `json:"state"`
	Reruns     []string          `json:"reruns"`
	AsOf       string            `json:"asOf"`
}
type WorkflowReplayResult struct {
	Plan               WorkflowReplayPreview `json:"plan"`
	AcceptedGeneration int64                 `json:"acceptedGeneration"`
	AsOf               string                `json:"asOf"`
}

func projectWorkflow(run *workflow.Run, at time.Time) WorkflowRow {
	version := run.Version
	if version <= 0 {
		version = 1
	}
	row := WorkflowRow{ID: run.ID.String(), Name: run.Name, State: run.State, Version: version, RecordedVersion: run.Version,
		ReplayGeneration: run.ReplayGeneration, ScopeAppID: nullable(run.ScopeAppID), ScopeOrgID: nullable(run.ScopeOrgID),
		CreatedAt: timestamp(run.CreatedAt), UpdatedAt: timestamp(run.UpdatedAt), StartedAt: timestamp(run.StartedAt), CompletedAt: timestampPtr(run.CompletedAt)}
	if run.ParentRunID != nil {
		row.ParentRunID = nullable(run.ParentRunID.String())
	}
	var end time.Time
	if run.CompletedAt != nil {
		end = *run.CompletedAt
	} else if run.State == workflow.RunStateRunning {
		end = at
	}
	if !run.StartedAt.IsZero() && !end.IsZero() && !end.Before(run.StartedAt) {
		d := duration(end.Sub(run.StartedAt))
		row.Duration = &d
	}
	return row
}
func projectWorkflowReplay(plan *workflow.ReplayPlan, at time.Time) WorkflowReplayPreview {
	reruns := append([]string{}, plan.Reruns...)
	return WorkflowReplayPreview{RunID: plan.RunID.String(), FromStep: plan.FromStep, Version: plan.Version, Generation: plan.Generation,
		State: plan.State, Reruns: reruns, AsOf: at.UTC().Format(time.RFC3339Nano)}
}
