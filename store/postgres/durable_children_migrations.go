package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "create_durable_child_executions", Version: "20261018120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `CREATE TABLE IF NOT EXISTS dispatch_child_executions (
 namespace TEXT NOT NULL, parent_workflow_id TEXT NOT NULL, parent_run_id TEXT NOT NULL,
 command_id TEXT NOT NULL, child_workflow_id TEXT NOT NULL, child_run_id TEXT NOT NULL,
 start_request JSONB NOT NULL, parent_queue TEXT NOT NULL,
 parent_close_policy TEXT NOT NULL CHECK (parent_close_policy IN ('terminate','request_cancel','abandon')),
 created_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(namespace,parent_workflow_id,parent_run_id,command_id),
 UNIQUE(namespace,child_workflow_id,child_run_id),
 FOREIGN KEY(namespace,parent_workflow_id,parent_run_id) REFERENCES dispatch_executions(namespace,workflow_id,run_id),
 FOREIGN KEY(namespace,child_workflow_id,child_run_id) REFERENCES dispatch_executions(namespace,workflow_id,run_id)
)`)
			return err
		},
		Down: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `DO $$ BEGIN
 IF to_regclass('dispatch_child_executions') IS NOT NULL THEN
   LOCK TABLE dispatch_child_executions IN ACCESS EXCLUSIVE MODE;
   IF EXISTS (SELECT 1 FROM dispatch_child_executions) THEN
     RAISE EXCEPTION 'retained child relationships prevent downgrade';
   END IF;
   DROP TABLE dispatch_child_executions;
 END IF;
 END $$`)
			return err
		},
	})
}
