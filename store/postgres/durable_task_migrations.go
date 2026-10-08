package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{
		Name: "add_durable_task_control", Version: "20261011120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `ALTER TABLE dispatch_execution_tasks
                ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
                ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS progress BYTEA NOT NULL DEFAULT ''`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `ALTER TABLE dispatch_execution_tasks
                DROP COLUMN progress, DROP COLUMN deadline_at, DROP COLUMN version`)
			return err
		},
	})
}
