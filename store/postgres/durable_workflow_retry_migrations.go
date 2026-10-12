package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "add_durable_workflow_retries", Version: "20261025120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE dispatch_execution_tasks IN SHARE ROW EXCLUSIVE MODE;
 ALTER TABLE dispatch_executions ADD COLUMN IF NOT EXISTS retry_policy JSONB,
 ADD COLUMN IF NOT EXISTS retry_attempt BIGINT NOT NULL DEFAULT 1 CHECK(retry_attempt>0),
 ADD COLUMN IF NOT EXISTS run_available_at TIMESTAMPTZ;
 DROP TRIGGER IF EXISTS dispatch_execution_deadline_update ON dispatch_executions;
 DROP TRIGGER IF EXISTS dispatch_execution_retry_update ON dispatch_executions;
 UPDATE dispatch_executions SET run_available_at=created_at WHERE run_available_at IS NULL;
 SET CONSTRAINTS dispatch_continuation_pair IMMEDIATE;
 ALTER TABLE dispatch_executions ALTER COLUMN run_available_at SET NOT NULL;
 SET CONSTRAINTS dispatch_continuation_pair DEFERRED;
 CREATE OR REPLACE FUNCTION dispatch_initialize_workflow_retry() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 IF NEW.run_available_at IS NULL THEN NEW.run_available_at:=NEW.created_at; END IF;
 IF NEW.retry_attempt<1 OR NEW.retry_attempt>NEW.run_number OR NEW.run_available_at<NEW.created_at
 OR (NEW.retry_attempt=1 AND NEW.run_available_at<>NEW.created_at)
 OR (NEW.retry_attempt>1 AND (NEW.retry_policy IS NULL OR NEW.run_available_at<=NEW.created_at))
 OR (NEW.retry_policy IS NOT NULL AND jsonb_typeof(NEW.retry_policy)<>'object') THEN
 RAISE EXCEPTION USING ERRCODE='DX004',MESSAGE='invalid workflow retry metadata'; END IF;
 RETURN NEW;
 END $$;
 CREATE OR REPLACE FUNCTION dispatch_guard_workflow_retry() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 IF NEW.retry_policy IS DISTINCT FROM OLD.retry_policy OR NEW.retry_attempt IS DISTINCT FROM OLD.retry_attempt OR NEW.run_available_at IS DISTINCT FROM OLD.run_available_at THEN
 RAISE EXCEPTION USING ERRCODE='DX004',MESSAGE='workflow retry metadata is immutable'; END IF;
 RETURN NEW;
 END $$;
 DROP TRIGGER IF EXISTS dispatch_execution_retry_insert ON dispatch_executions;
 CREATE TRIGGER dispatch_execution_retry_insert BEFORE INSERT ON dispatch_executions FOR EACH ROW EXECUTE FUNCTION dispatch_initialize_workflow_retry();
 CREATE TRIGGER dispatch_execution_retry_update BEFORE UPDATE ON dispatch_executions FOR EACH ROW EXECUTE FUNCTION dispatch_guard_workflow_retry();
 CREATE TRIGGER dispatch_execution_deadline_update BEFORE UPDATE ON dispatch_executions FOR EACH ROW EXECUTE FUNCTION dispatch_guard_execution_deadline();
`+workflowRetryIntegritySQL+continuationIntegritySQL+executionTimeoutGuardSQL)
		return err
	}, Down: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE dispatch_execution_tasks IN SHARE ROW EXCLUSIVE MODE;
 DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='dispatch_executions'::regclass AND attname='retry_attempt' AND NOT attisdropped) THEN
 IF EXISTS(SELECT 1 FROM dispatch_executions WHERE retry_policy IS NOT NULL OR retry_attempt<>1 OR run_available_at<>created_at) THEN
 RAISE EXCEPTION 'retained workflow retry metadata prevents downgrade'; END IF; END IF; END $$;
 DROP TRIGGER IF EXISTS dispatch_execution_retry_insert ON dispatch_executions;
 DROP TRIGGER IF EXISTS dispatch_execution_retry_update ON dispatch_executions;
 DROP FUNCTION IF EXISTS dispatch_initialize_workflow_retry();
 DROP FUNCTION IF EXISTS dispatch_guard_workflow_retry();
 DROP FUNCTION IF EXISTS dispatch_workflow_retry_link_allowed(dispatch_executions,dispatch_executions);
 DROP FUNCTION IF EXISTS dispatch_workflow_retry_pair_allowed(dispatch_executions,dispatch_executions);
 DROP FUNCTION IF EXISTS dispatch_workflow_retry_timeout_allowed(dispatch_executions,dispatch_executions);
 ALTER TABLE dispatch_executions DROP COLUMN IF EXISTS retry_policy,DROP COLUMN IF EXISTS retry_attempt,DROP COLUMN IF EXISTS run_available_at`)
		return err
	}})
}

const workflowRetryIntegritySQL = `CREATE OR REPLACE FUNCTION dispatch_workflow_retry_link_allowed(source dispatch_executions,target dispatch_executions) RETURNS boolean LANGUAGE sql AS $$
 SELECT source.next_run_id='' AND source.state='running' AND target.state IN ('failed','timed_out')
 AND source.retry_policy IS NOT NULL AND target.next_run_id<>'' AND target.next_run_id NOT IN(source.run_id,source.first_run_id,source.previous_run_id)
 AND target.revision=source.revision+1 AND target.last_sequence>=source.last_sequence+2
 AND (source.execution_deadline_at IS NULL OR source.execution_deadline_at>clock_timestamp())
 $$;
 CREATE OR REPLACE FUNCTION dispatch_workflow_retry_pair_allowed(source dispatch_executions,target dispatch_executions) RETURNS boolean LANGUAGE sql AS $$
 SELECT source.retry_policy IS NOT DISTINCT FROM target.retry_policy AND
 ((source.state='continued_as_new' AND target.retry_attempt=1 AND target.run_available_at=target.created_at) OR
 (source.state IN ('failed','timed_out') AND source.retry_policy IS NOT NULL AND target.retry_attempt>source.retry_attempt AND target.retry_attempt-source.retry_attempt=1
 AND target.run_available_at>target.created_at AND (target.execution_deadline_at IS NULL OR target.run_available_at<target.execution_deadline_at)
 AND target.workflow_type=source.workflow_type AND target.build_id=source.build_id AND target.input=source.input AND target.run_timeout=source.run_timeout))
 $$;
 CREATE OR REPLACE FUNCTION dispatch_workflow_retry_timeout_allowed(source dispatch_executions,target dispatch_executions) RETURNS boolean LANGUAGE sql AS $$
 SELECT source.state='running' AND source.run_deadline_at IS NOT NULL AND source.run_deadline_at<=clock_timestamp()
 AND (source.execution_deadline_at IS NULL OR source.execution_deadline_at>clock_timestamp())
 AND dispatch_workflow_retry_link_allowed(source,target) AND target.state='timed_out'
 AND target.timeout_owner='' AND target.timeout_lease_until IS NULL AND target.last_sequence=source.last_sequence+2
 AND target.output=''::bytea AND target.updated_at>=source.run_deadline_at
 AND (to_jsonb(source)-ARRAY['state','revision','last_sequence','output','updated_at','timeout_owner','timeout_lease_until','next_run_id'])=(to_jsonb(target)-ARRAY['state','revision','last_sequence','output','updated_at','timeout_owner','timeout_lease_until','next_run_id'])
 $$;`
