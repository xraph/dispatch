package workflow

import (
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
)

// RunState represents the lifecycle state of a workflow run.
type RunState string

const (
	// RunStateRunning means the workflow is currently executing.
	RunStateRunning RunState = "running"
	// RunStateCompleted means the workflow finished successfully.
	RunStateCompleted RunState = "completed"
	// RunStateFailed means the workflow failed terminally.
	RunStateFailed RunState = "failed"
)

// Run represents a single execution of a workflow.
type Run struct {
	dispatch.Entity

	ID          id.RunID   `json:"id"`
	Name        string     `json:"name"`
	State       RunState   `json:"state"`
	Input       []byte     `json:"input,omitempty"`
	Output      []byte     `json:"output,omitempty"`
	Error       string     `json:"error,omitempty"`
	ScopeAppID  string     `json:"scope_app_id,omitempty"`
	ScopeOrgID  string     `json:"scope_org_id,omitempty"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}
