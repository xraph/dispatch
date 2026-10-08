package sqlite

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{
		Name:    "workflow_replay_generation",
		Version: "20261009140000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			return addColumnIfMissing(ctx, exec, "dispatch_workflow_runs", "replay_generation", `INTEGER NOT NULL DEFAULT 0`)
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			return dropColumnIfPresent(ctx, exec, "dispatch_workflow_runs", "replay_generation")
		},
	})
}
