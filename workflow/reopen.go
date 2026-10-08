package workflow

import (
	"context"

	"github.com/xraph/dispatch/id"
)

// Reopener is the conditional claim that makes replay-from-step safe.
//
// Replaying a run from a step re-executes it under the same run ID, so two
// replays started together would run the tail of the workflow twice, side
// by side. The claim is the state change itself: only one caller can move
// a run out of a finished state into running.
type Reopener interface {
	// ReopenRun sets state = running, error = "" and completed_at = null
	// (and stamps updated_at) only if the run is not already running and
	// ReplayGeneration equals expectedGeneration. It increments the generation
	// atomically. When the run is running or the generation changed it returns
	// an error wrapping dispatch.ErrInvalidState that names the state;
	// when the run does not exist, dispatch.ErrRunNotFound. The check and
	// the write are one atomic step: of two concurrent reopens, exactly
	// one wins.
	ReopenRun(ctx context.Context, runID id.RunID, expectedGeneration int64) error
}
