package workflow

import (
	"time"

	"github.com/xraph/dispatch/id"
)

// Checkpoint stores the serialized state of a completed workflow step,
// enabling crash recovery by replaying from the last checkpoint.
type Checkpoint struct {
	ID        id.CheckpointID `json:"id"`
	RunID     id.RunID        `json:"run_id"`
	StepName  string          `json:"step_name"`
	Data      []byte          `json:"data"`
	CreatedAt time.Time       `json:"created_at"`
}
