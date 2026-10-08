package workflow

import (
	"context"

	"github.com/xraph/dispatch/id"
)

// ListOpts controls pagination for workflow run list queries.
type ListOpts struct {
	// Limit is the maximum number of runs to return. Zero means no limit.
	Limit int
	// Offset is the number of runs to skip.
	Offset int
	// State filters by run state. Empty means all states.
	State RunState
}

// Store defines the persistence contract for workflows.
type Store interface {
	Reopener

	// CreateRun persists a new workflow run.
	CreateRun(ctx context.Context, run *Run) error

	// GetRun retrieves a workflow run by ID.
	GetRun(ctx context.Context, runID id.RunID) (*Run, error)

	// UpdateRun persists changes to an existing workflow run. It refuses a
	// ReplayGeneration that no longer matches with dispatch.ErrInvalidState,
	// so an old execution cannot overwrite a later replay's claim.
	UpdateRun(ctx context.Context, run *Run) error

	// ListRuns returns workflow runs matching the given options.
	ListRuns(ctx context.Context, opts ListOpts) ([]*Run, error)

	// SaveCheckpoint persists checkpoint data for a workflow step.
	// If a checkpoint already exists for the same run/step, it is replaced.
	SaveCheckpoint(ctx context.Context, runID id.RunID, stepName string, data []byte) error

	// GetCheckpoint retrieves checkpoint data for a specific workflow step.
	// Returns nil data if no checkpoint exists.
	GetCheckpoint(ctx context.Context, runID id.RunID, stepName string) ([]byte, error)

	// ListCheckpoints returns checkpoints in ascending creation-time/ID order.
	ListCheckpoints(ctx context.Context, runID id.RunID) ([]*Checkpoint, error)

	// ListChildRuns returns all child workflow runs for a parent.
	ListChildRuns(ctx context.Context, parentRunID id.RunID) ([]*Run, error)

	// DeleteCheckpointsAfter removes checkpoints strictly after the named
	// step in creation-time/ID order, preserving the target and earlier ties.
	DeleteCheckpointsAfter(ctx context.Context, runID id.RunID, afterStep string) error
}
