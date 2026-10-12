package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "durable_workflow_task_deferral", Version: "20261102120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, workflowTaskDeferralSQL)
		return err
	}, Down: func(context.Context, migrate.Executor) error {
		return fmt.Errorf("durable task deferral receipts prohibit downgrade")
	}})
}

const workflowTaskDeferralSQL = `
CREATE TABLE dispatch_workflow_task_deferral_receipts (
 namespace TEXT NOT NULL, workflow_id TEXT NOT NULL, run_id TEXT NOT NULL, request_id TEXT NOT NULL,
 response JSONB NOT NULL,
 PRIMARY KEY(namespace,workflow_id,run_id,request_id),
 FOREIGN KEY(namespace,workflow_id,run_id,request_id) REFERENCES dispatch_execution_receipts(namespace,workflow_id,run_id,request_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE dispatch_workflow_task_deferrals (
 namespace TEXT NOT NULL, workflow_id TEXT NOT NULL, run_id TEXT NOT NULL, task_id TEXT NOT NULL, request_id TEXT NOT NULL,
 PRIMARY KEY(namespace,workflow_id,run_id,task_id),
 FOREIGN KEY(namespace,workflow_id,run_id,task_id) REFERENCES dispatch_execution_tasks(namespace,workflow_id,run_id,task_id),
 FOREIGN KEY(namespace,workflow_id,run_id,request_id) REFERENCES dispatch_workflow_task_deferral_receipts(namespace,workflow_id,run_id,request_id)
);
CREATE TRIGGER dispatch_retirement_write BEFORE INSERT OR UPDATE OR DELETE ON dispatch_workflow_task_deferral_receipts FOR EACH ROW EXECUTE FUNCTION dispatch_retirement_write_guard();
CREATE TRIGGER dispatch_retirement_write BEFORE INSERT OR UPDATE OR DELETE ON dispatch_workflow_task_deferrals FOR EACH ROW EXECUTE FUNCTION dispatch_retirement_write_guard();
CREATE TRIGGER dispatch_deferral_immutable BEFORE UPDATE OR DELETE ON dispatch_workflow_task_deferral_receipts FOR EACH ROW EXECUTE FUNCTION dispatch_lifecycle_receipt_immutable();
`
