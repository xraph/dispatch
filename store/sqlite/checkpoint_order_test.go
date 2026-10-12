package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/storetest"
)

func TestCheckpointOrderConformance(t *testing.T) {
	s, drv, _ := openMigratedWithDriver(t)
	storetest.RunCheckpointOrderSuite(t, s, func(ctx context.Context, runID id.RunID, step string, at time.Time) error {
		_, err := drv.Exec(ctx, "UPDATE dispatch_checkpoints SET created_at = ? WHERE run_id = ? AND step_name = ?", at, runID.String(), step)
		return err
	})
}
