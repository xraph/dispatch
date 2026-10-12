package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{
		Name:    "workflow_replay_generation",
		Version: "20261009140000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			return withLockTimeout(ctx, exec, `ALTER TABLE dispatch_workflow_runs ADD COLUMN IF NOT EXISTS replay_generation BIGINT NOT NULL DEFAULT 0`)
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			return withLockTimeout(ctx, exec, `ALTER TABLE dispatch_workflow_runs DROP COLUMN IF EXISTS replay_generation`)
		},
	})
}
