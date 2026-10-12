package memory

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/storetest"
)

func TestCheckpointOrderConformance(t *testing.T) {
	s := New()
	storetest.RunCheckpointOrderSuite(t, s, func(_ context.Context, runID id.RunID, step string, at time.Time) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.checkpoints[checkpointKey(runID, step)].CreatedAt = at
		return nil
	})
}
