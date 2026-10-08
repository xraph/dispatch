package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{
		Name: "add_durable_async_activity_grants", Version: "20261014120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `ALTER TABLE dispatch_execution_tasks
                ADD COLUMN IF NOT EXISTS async_key_hash TEXT NOT NULL DEFAULT '';
                ALTER TABLE dispatch_execution_tasks
                DROP CONSTRAINT IF EXISTS dispatch_execution_tasks_lease_kind_check,
                ADD CONSTRAINT dispatch_execution_tasks_lease_kind_check CHECK (lease_kind IN ('','timeout','async'))`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			// Refuse downgrade while asynchronous rows remain.
			_, err := exec.Exec(ctx, `ALTER TABLE dispatch_execution_tasks
                DROP CONSTRAINT IF EXISTS dispatch_execution_tasks_lease_kind_check,
                ADD CONSTRAINT dispatch_execution_tasks_lease_kind_check CHECK (lease_kind IN ('','timeout'));
                ALTER TABLE dispatch_execution_tasks DROP COLUMN IF EXISTS async_key_hash`)
			return err
		},
	})
}
