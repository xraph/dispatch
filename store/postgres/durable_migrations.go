package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{
		Name: "create_durable_execution_tables", Version: "20261010120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `
CREATE TABLE IF NOT EXISTS dispatch_executions (
    namespace TEXT NOT NULL, workflow_id TEXT NOT NULL, run_id TEXT NOT NULL,
    workflow_type TEXT NOT NULL, build_id TEXT NOT NULL, state TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    last_sequence BIGINT NOT NULL CHECK (last_sequence >= revision),
    input BYTEA NOT NULL, output BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (namespace, workflow_id, run_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS dispatch_executions_open_workflow
    ON dispatch_executions (namespace, workflow_id) WHERE state = 'running';
CREATE TABLE IF NOT EXISTS dispatch_execution_events (
    namespace TEXT NOT NULL, workflow_id TEXT NOT NULL, run_id TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0), type TEXT NOT NULL,
    payload BYTEA NOT NULL, occurred_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (namespace, workflow_id, run_id, sequence),
    FOREIGN KEY (namespace, workflow_id, run_id)
        REFERENCES dispatch_executions (namespace, workflow_id, run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS dispatch_execution_tasks (
    namespace TEXT NOT NULL, workflow_id TEXT NOT NULL, run_id TEXT NOT NULL,
    task_id TEXT NOT NULL, kind TEXT NOT NULL, queue TEXT NOT NULL,
    payload BYTEA NOT NULL, available_at TIMESTAMPTZ NOT NULL,
    owner TEXT NOT NULL DEFAULT '', epoch BIGINT NOT NULL DEFAULT 0,
    attempt BIGINT NOT NULL DEFAULT 0, lease_until TIMESTAMPTZ,
    done BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (namespace, workflow_id, run_id, task_id),
    FOREIGN KEY (namespace, workflow_id, run_id)
        REFERENCES dispatch_executions (namespace, workflow_id, run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS dispatch_execution_tasks_poll
    ON dispatch_execution_tasks (namespace, kind, queue, available_at) WHERE NOT done;
CREATE TABLE IF NOT EXISTS dispatch_execution_receipts (
    namespace TEXT NOT NULL, workflow_id TEXT NOT NULL, run_id TEXT NOT NULL,
    request_id TEXT NOT NULL, digest TEXT NOT NULL,
    revision BIGINT NOT NULL, first_sequence BIGINT NOT NULL, last_sequence BIGINT NOT NULL,
    PRIMARY KEY (namespace, workflow_id, run_id, request_id),
    FOREIGN KEY (namespace, workflow_id, run_id)
        REFERENCES dispatch_executions (namespace, workflow_id, run_id) ON DELETE CASCADE
);`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `DROP TABLE dispatch_execution_receipts,
                dispatch_execution_tasks, dispatch_execution_events, dispatch_executions`)
			return err
		},
	})
}
