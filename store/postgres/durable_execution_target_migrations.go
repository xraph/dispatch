package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "create_durable_execution_heads", Version: "20261020120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE IF NOT EXISTS dispatch_execution_heads(
 namespace TEXT NOT NULL,workflow_id TEXT NOT NULL,run_id TEXT,
 PRIMARY KEY(namespace,workflow_id),
 FOREIGN KEY(namespace,workflow_id,run_id) REFERENCES dispatch_executions(namespace,workflow_id,run_id));
 INSERT INTO dispatch_execution_heads(namespace,workflow_id,run_id)
 SELECT namespace,workflow_id,CASE WHEN count(*)=1 THEN max(run_id)
 ELSE max(run_id) FILTER (WHERE state='running') END
 FROM dispatch_executions GROUP BY namespace,workflow_id ON CONFLICT DO NOTHING;
 CREATE OR REPLACE FUNCTION dispatch_record_execution_head() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 INSERT INTO dispatch_execution_heads(namespace,workflow_id,run_id) VALUES(NEW.namespace,NEW.workflow_id,NEW.run_id)
 ON CONFLICT(namespace,workflow_id) DO UPDATE SET run_id=EXCLUDED.run_id;
 RETURN NEW;
 END $$;
 DROP TRIGGER IF EXISTS dispatch_execution_head_insert ON dispatch_executions;
 CREATE TRIGGER dispatch_execution_head_insert AFTER INSERT ON dispatch_executions
 FOR EACH ROW EXECUTE FUNCTION dispatch_record_execution_head()`)
			return err
		}, Down: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `DO $$ BEGIN
 IF to_regclass('dispatch_execution_heads') IS NOT NULL THEN
 LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE dispatch_execution_heads IN ACCESS EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM dispatch_execution_heads) THEN
 RAISE EXCEPTION 'retained execution heads prevent downgrade';
 END IF;
 DROP TRIGGER IF EXISTS dispatch_execution_head_insert ON dispatch_executions;
 DROP FUNCTION IF EXISTS dispatch_record_execution_head();
 DROP TABLE dispatch_execution_heads;
 END IF; END $$`)
			return err
		}})
}
