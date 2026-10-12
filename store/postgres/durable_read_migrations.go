package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "durable_operator_reads", Version: "20261030120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `CREATE INDEX IF NOT EXISTS dispatch_namespace_read_order ON dispatch_durable_namespaces(installation_id,namespace COLLATE "C");
 CREATE INDEX IF NOT EXISTS dispatch_execution_read_order ON dispatch_executions(namespace,created_at DESC,workflow_id COLLATE "C" DESC,run_id COLLATE "C" DESC);
 CREATE INDEX IF NOT EXISTS dispatch_task_read_order ON dispatch_execution_tasks(namespace,workflow_id,run_id,task_id COLLATE "C");
 CREATE INDEX IF NOT EXISTS dispatch_outbox_scoped_reads ON dispatch_durable_outbox(installation_id,destination,namespace,workflow_id,run_id,id);`)
		return err
	}, Down: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `DROP INDEX IF EXISTS dispatch_namespace_read_order;DROP INDEX IF EXISTS dispatch_execution_read_order;DROP INDEX IF EXISTS dispatch_task_read_order;DROP INDEX IF EXISTS dispatch_outbox_scoped_reads`)
		return err
	}})
}
