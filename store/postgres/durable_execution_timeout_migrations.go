package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "add_durable_execution_timeout_grants", Version: "20261022120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE dispatch_execution_tasks IN SHARE ROW EXCLUSIVE MODE;
 ALTER TABLE dispatch_executions ADD COLUMN IF NOT EXISTS timeout_owner TEXT NOT NULL DEFAULT '',
 ADD COLUMN IF NOT EXISTS timeout_epoch BIGINT NOT NULL DEFAULT 0 CHECK(timeout_epoch>=0),
 ADD COLUMN IF NOT EXISTS timeout_attempt BIGINT NOT NULL DEFAULT 0 CHECK(timeout_attempt>=0),
 ADD COLUMN IF NOT EXISTS timeout_lease_until TIMESTAMPTZ;
`+executionTimeoutGuardSQL+executionDeadlineGuardSQL)
		return err
	}, Down: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE dispatch_execution_tasks IN SHARE ROW EXCLUSIVE MODE;
 DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='dispatch_executions'::regclass AND attname='timeout_epoch' AND NOT attisdropped) THEN
 IF EXISTS(SELECT 1 FROM dispatch_executions WHERE timeout_epoch<>0) THEN RAISE EXCEPTION 'retained timeout grants prevent downgrade'; END IF;
 END IF; END $$;
 DROP FUNCTION IF EXISTS dispatch_execution_timeout_update_allowed(dispatch_executions,dispatch_executions);
 ALTER TABLE dispatch_executions DROP COLUMN IF EXISTS timeout_owner,DROP COLUMN IF EXISTS timeout_epoch,
 DROP COLUMN IF EXISTS timeout_attempt,DROP COLUMN IF EXISTS timeout_lease_until`)
		return err
	}})
}

const executionTimeoutGuardSQL = ` CREATE OR REPLACE FUNCTION dispatch_execution_timeout_update_allowed(old_run dispatch_executions,new_run dispatch_executions)
 RETURNS boolean LANGUAGE plpgsql AS $$
 DECLARE grant_keys text[] := ARRAY['timeout_owner','timeout_epoch','timeout_attempt','timeout_lease_until']; allowed boolean;
 BEGIN
 IF to_regprocedure('dispatch_workflow_retry_timeout_allowed(dispatch_executions,dispatch_executions)') IS NOT NULL
 AND to_jsonb(old_run) ? 'next_run_id' AND to_jsonb(old_run)->>'retry_policy' IS NOT NULL THEN
 EXECUTE 'SELECT dispatch_workflow_retry_timeout_allowed($1,$2)' INTO allowed USING old_run,new_run;
 IF allowed THEN
 IF old_run.timeout_owner='' OR old_run.timeout_epoch<1 OR old_run.timeout_lease_until IS NULL OR old_run.timeout_lease_until<=clock_timestamp() THEN
 RAISE EXCEPTION USING ERRCODE='DX003',MESSAGE='execution timeout lease expired'; END IF;
 RETURN TRUE; END IF; END IF;
 IF (to_jsonb(old_run)-grant_keys)=(to_jsonb(new_run)-grant_keys)
 AND new_run.timeout_epoch>old_run.timeout_epoch AND new_run.timeout_epoch-old_run.timeout_epoch=1
 AND new_run.timeout_attempt>old_run.timeout_attempt AND new_run.timeout_attempt-old_run.timeout_attempt=1
 AND new_run.timeout_owner<>'' AND (old_run.timeout_lease_until IS NULL OR old_run.timeout_lease_until<=clock_timestamp()) THEN
 IF new_run.timeout_lease_until IS NULL OR new_run.timeout_lease_until<=clock_timestamp() THEN
 RAISE EXCEPTION USING ERRCODE='DX003',MESSAGE='execution timeout lease expired'; END IF;
 RETURN TRUE;
 END IF;
 IF new_run.state='timed_out' AND new_run.timeout_owner='' AND new_run.timeout_lease_until IS NULL AND new_run.revision=old_run.revision+1 AND (new_run.last_sequence=old_run.last_sequence+1 OR
 (new_run.last_sequence=old_run.last_sequence+2 AND to_jsonb(old_run)->>'retry_policy' IS NOT NULL
 AND EXISTS(SELECT 1 FROM dispatch_execution_events WHERE namespace=old_run.namespace AND workflow_id=old_run.workflow_id AND run_id=old_run.run_id
 AND sequence=old_run.last_sequence+1 AND type='workflow.retry_suppressed' AND occurred_at=new_run.updated_at)))
 AND new_run.output=''::bytea AND new_run.updated_at>=LEAST(old_run.run_deadline_at,old_run.execution_deadline_at)
 AND (to_jsonb(old_run)-ARRAY['state','revision','last_sequence','output','updated_at','timeout_owner','timeout_lease_until'])=(to_jsonb(new_run)-ARRAY['state','revision','last_sequence','output','updated_at','timeout_owner','timeout_lease_until']) THEN
 IF old_run.timeout_owner='' OR old_run.timeout_epoch<1 OR old_run.timeout_lease_until IS NULL OR old_run.timeout_lease_until<=clock_timestamp() THEN
 RAISE EXCEPTION USING ERRCODE='DX003',MESSAGE='execution timeout lease expired'; END IF;
 RETURN TRUE;
 END IF;
 RETURN FALSE;
 END $$;`
