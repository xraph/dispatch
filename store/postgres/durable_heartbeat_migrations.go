package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "add_durable_heartbeat_state", Version: "20261013120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `ALTER TABLE dispatch_execution_tasks
                ADD COLUMN IF NOT EXISTS heartbeat_timeout_ns BIGINT NOT NULL DEFAULT 0 CHECK (heartbeat_timeout_ns >= 0),
                ADD COLUMN IF NOT EXISTS heartbeat_limit TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS heartbeat_sequence BIGINT NOT NULL DEFAULT 0 CHECK (heartbeat_sequence >= 0),
                ADD COLUMN IF NOT EXISTS heartbeat_epoch BIGINT NOT NULL DEFAULT 0 CHECK (heartbeat_epoch >= 0)`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `ALTER TABLE dispatch_execution_tasks DROP COLUMN heartbeat_timeout_ns,
                DROP COLUMN heartbeat_limit, DROP COLUMN heartbeat_at, DROP COLUMN heartbeat_sequence, DROP COLUMN heartbeat_epoch`)
			return err
		},
	})
}
