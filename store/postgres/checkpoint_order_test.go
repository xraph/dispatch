//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/storetest"
)

func TestCheckpointOrderConformance(t *testing.T) {
	s := setupTestStore(t)
	conn := dedicatedConn(t, s)
	storetest.RunCheckpointOrderSuite(t, s, func(ctx context.Context, runID id.RunID, step string, at time.Time) error {
		_, err := conn.Exec(ctx, "UPDATE dispatch_checkpoints SET created_at = $1 WHERE run_id = $2 AND step_name = $3", at, runID.String(), step)
		return err
	})
}
