package workflow

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/xraph/dispatch/id"
)

// TimelineEntry represents a single step in a workflow's execution history.
// The timeline is ordered by checkpoint creation time, providing a
// chronological view of how the workflow executed.
type TimelineEntry struct {
	StepName  string    `json:"step_name"`
	Data      []byte    `json:"data,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// GetTimeline returns an ordered timeline of all steps executed in
// a workflow run. Each entry corresponds to a checkpoint, sorted by
// creation time. This enables point-in-time inspection of workflow state.
func (r *Runner) GetTimeline(ctx context.Context, runID id.RunID) ([]TimelineEntry, error) {
	checkpoints, err := r.store.ListCheckpoints(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("list checkpoints for run %s: %w", runID, err)
	}

	// Sort by creation time, using checkpoint ID as tiebreaker when
	// timestamps are identical (IDs are UUIDv7 K-sortable).
	sort.SliceStable(checkpoints, func(i, j int) bool {
		if checkpoints[i].CreatedAt.Equal(checkpoints[j].CreatedAt) {
			return checkpoints[i].ID.String() < checkpoints[j].ID.String()
		}
		return checkpoints[i].CreatedAt.Before(checkpoints[j].CreatedAt)
	})

	entries := make([]TimelineEntry, len(checkpoints))
	for i, cp := range checkpoints {
		entries[i] = TimelineEntry{
			StepName:  cp.StepName,
			Data:      cp.Data,
			CreatedAt: cp.CreatedAt,
		}
	}

	return entries, nil
}

// InspectStep returns the raw checkpoint data for a specific step in
// a workflow run. This allows decoding and examining the result that
// was saved when the step completed.
func (r *Runner) InspectStep(ctx context.Context, runID id.RunID, stepName string) ([]byte, error) {
	data, err := r.store.GetCheckpoint(ctx, runID, stepName)
	if err != nil {
		return nil, fmt.Errorf("get checkpoint %q for run %s: %w", stepName, runID, err)
	}
	if data == nil {
		return nil, fmt.Errorf("no checkpoint %q found for run %s", stepName, runID)
	}
	return data, nil
}

// ReplayFrom re-executes a workflow from a specific step by deleting all
// checkpoints after the given step and resuming the workflow. The workflow
// handler replays from the beginning, but checkpoints up to and including
// fromStep are preserved so those steps are skipped. Steps after fromStep
// re-execute with fresh state.
func (r *Runner) ReplayFrom(ctx context.Context, runID id.RunID, fromStep string) error {
	run, err := r.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("get run %s: %w", runID, err)
	}

	// Verify the step exists.
	data, err := r.store.GetCheckpoint(ctx, runID, fromStep)
	if err != nil {
		return fmt.Errorf("get checkpoint %q for run %s: %w", fromStep, runID, err)
	}
	if data == nil {
		return fmt.Errorf("no checkpoint %q found for run %s", fromStep, runID)
	}

	// Delete all checkpoints after fromStep.
	if err := r.store.DeleteCheckpointsAfter(ctx, runID, fromStep); err != nil {
		return fmt.Errorf("delete checkpoints after %q for run %s: %w", fromStep, runID, err)
	}

	// Reset the run state to running.
	run.State = RunStateRunning
	run.Error = ""
	run.CompletedAt = nil
	if err := r.store.UpdateRun(ctx, run); err != nil {
		return fmt.Errorf("reset run %s to running: %w", runID, err)
	}

	// Look up the handler and re-execute.
	runner, ok := r.registry.Get(run.Name)
	if !ok {
		return fmt.Errorf("no workflow registered for %q (run %s)", run.Name, runID)
	}

	r.emitter.EmitWorkflowStarted(ctx, run)
	r.executeRun(ctx, run, runner, run.Input)

	return nil
}
