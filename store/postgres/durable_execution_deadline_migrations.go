package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "add_durable_execution_deadlines", Version: "20261021120000",
		Up: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE dispatch_execution_tasks IN SHARE ROW EXCLUSIVE MODE;
 ALTER TABLE dispatch_executions ADD COLUMN IF NOT EXISTS run_deadline_at TIMESTAMPTZ,
 ADD COLUMN IF NOT EXISTS execution_deadline_at TIMESTAMPTZ;
 CREATE INDEX IF NOT EXISTS idx_dispatch_execution_deadline ON dispatch_executions(namespace,LEAST(run_deadline_at,execution_deadline_at))
 WHERE state='running' AND (run_deadline_at IS NOT NULL OR execution_deadline_at IS NOT NULL);
 CREATE OR REPLACE FUNCTION dispatch_guard_execution_deadline() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 IF NEW.run_deadline_at IS DISTINCT FROM OLD.run_deadline_at OR NEW.execution_deadline_at IS DISTINCT FROM OLD.execution_deadline_at THEN
 RAISE EXCEPTION USING ERRCODE='DX002',MESSAGE='workflow deadlines are immutable'; END IF;
 IF OLD.state='running' AND LEAST(OLD.run_deadline_at,OLD.execution_deadline_at)<=clock_timestamp() THEN
 RAISE EXCEPTION USING ERRCODE='DX001',MESSAGE='workflow deadline expired'; END IF;
 RETURN NEW;
 END $$;
 DROP TRIGGER IF EXISTS dispatch_execution_deadline_update ON dispatch_executions;
 CREATE TRIGGER dispatch_execution_deadline_update BEFORE UPDATE ON dispatch_executions
 FOR EACH ROW EXECUTE FUNCTION dispatch_guard_execution_deadline();
 CREATE OR REPLACE FUNCTION dispatch_guard_task_execution_deadline() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 IF EXISTS(SELECT 1 FROM dispatch_executions e WHERE e.namespace=OLD.namespace AND e.workflow_id=OLD.workflow_id AND e.run_id=OLD.run_id
 AND e.state='running' AND LEAST(e.run_deadline_at,e.execution_deadline_at)<=clock_timestamp()) THEN
 RAISE EXCEPTION USING ERRCODE='DX001',MESSAGE='workflow deadline expired'; END IF;
 RETURN NEW;
 END $$;
 DROP TRIGGER IF EXISTS dispatch_task_execution_deadline_update ON dispatch_execution_tasks;
 CREATE TRIGGER dispatch_task_execution_deadline_update BEFORE UPDATE ON dispatch_execution_tasks
 FOR EACH ROW EXECUTE FUNCTION dispatch_guard_task_execution_deadline()`)

			return err
		}, Down: func(ctx context.Context, exec migrate.Executor) error {
			_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE dispatch_execution_tasks IN SHARE ROW EXCLUSIVE MODE;
 DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='dispatch_executions'::regclass AND attname='run_deadline_at' AND NOT attisdropped) THEN
 IF EXISTS(SELECT 1 FROM dispatch_executions WHERE run_deadline_at IS NOT NULL OR execution_deadline_at IS NOT NULL) THEN
 RAISE EXCEPTION 'retained workflow deadlines prevent downgrade'; END IF; END IF; END $$;
 DROP TRIGGER IF EXISTS dispatch_task_execution_deadline_update ON dispatch_execution_tasks;
 DROP FUNCTION IF EXISTS dispatch_guard_task_execution_deadline();
 DROP TRIGGER IF EXISTS dispatch_execution_deadline_update ON dispatch_executions;
 DROP FUNCTION IF EXISTS dispatch_guard_execution_deadline();
 DROP INDEX IF EXISTS idx_dispatch_execution_deadline;
 ALTER TABLE dispatch_executions DROP COLUMN IF EXISTS run_deadline_at,DROP COLUMN IF EXISTS execution_deadline_at`)
			return err
		}})
}
