package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "add_durable_timeout_grants", Version: "20261012120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			if _, err := exec.Exec(ctx, `ALTER TABLE dispatch_execution_tasks ADD COLUMN IF NOT EXISTS lease_kind TEXT NOT NULL DEFAULT '' CHECK (lease_kind IN ('','timeout'))`); err != nil {
				return err
			}
			_, err := exec.Exec(ctx, `CREATE INDEX IF NOT EXISTS dispatch_execution_timeouts ON dispatch_execution_tasks(namespace,deadline_at,workflow_id,run_id,task_id) WHERE kind='activity' AND NOT done AND deadline_at IS NOT NULL`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			if _, err := exec.Exec(ctx, `DROP INDEX IF EXISTS dispatch_execution_timeouts`); err != nil {
				return err
			}
			_, err := exec.Exec(ctx, `ALTER TABLE dispatch_execution_tasks DROP COLUMN lease_kind`)
			return err
		},
	})
}
