package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "repair_durable_retry_capacity_closure", Version: "20261026120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;`+executionTimeoutGuardSQL)
		return err
	}, Down: func(ctx context.Context, exec migrate.Executor) error {
		// This repair adds no columns. Keep the stricter lease checks on downgrade;
		// retained suppression history requires readers that understand its outcome.
		_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 DO $$ BEGIN IF EXISTS(SELECT 1 FROM dispatch_execution_events WHERE type='workflow.retry_suppressed') THEN
 RAISE EXCEPTION 'retained workflow retry suppression prevents downgrade'; END IF; END $$;`)
		return err
	}})
}
