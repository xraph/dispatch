package workflow

import (
	"strings"
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

// CompareCheckpoints orders checkpoints by creation time, then ID. Timeline,
// replay preview and storage pruning use the same tie boundary.
func CompareCheckpoints(a, b *Checkpoint) int {
	if order := a.CreatedAt.Compare(b.CreatedAt); order != 0 {
		return order
	}
	return strings.Compare(a.ID.String(), b.ID.String())
}
