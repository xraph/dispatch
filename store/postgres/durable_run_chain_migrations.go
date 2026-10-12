package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "add_durable_run_lineage", Version: "20261023120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE dispatch_execution_tasks IN SHARE ROW EXCLUSIVE MODE;
 ALTER TABLE dispatch_executions ADD COLUMN IF NOT EXISTS first_run_id TEXT NOT NULL DEFAULT '',
 ADD COLUMN IF NOT EXISTS previous_run_id TEXT NOT NULL DEFAULT '',
 ADD COLUMN IF NOT EXISTS next_run_id TEXT NOT NULL DEFAULT '',
 ADD COLUMN IF NOT EXISTS run_number BIGINT NOT NULL DEFAULT 1 CHECK(run_number>0),
 ADD COLUMN IF NOT EXISTS first_started_at TIMESTAMPTZ,
 ADD COLUMN IF NOT EXISTS run_timeout BIGINT NOT NULL DEFAULT 0 CHECK(run_timeout=0 OR run_timeout>=1000);
 DROP TRIGGER IF EXISTS dispatch_execution_chain_update ON dispatch_executions;
 DROP TRIGGER IF EXISTS dispatch_execution_deadline_update ON dispatch_executions;
 UPDATE dispatch_executions SET first_run_id=run_id,first_started_at=created_at,
 run_timeout=CASE WHEN run_deadline_at IS NULL THEN 0 ELSE (EXTRACT(EPOCH FROM (run_deadline_at-created_at))*1000000000)::bigint END
 WHERE first_run_id='' AND first_started_at IS NULL;
 DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='dispatch_executions'::regclass AND tgname='dispatch_continuation_pair') THEN
 SET CONSTRAINTS dispatch_continuation_pair IMMEDIATE; END IF; END $$;
 ALTER TABLE dispatch_executions ALTER COLUMN first_started_at SET NOT NULL;
 DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='dispatch_executions'::regclass AND tgname='dispatch_continuation_pair') THEN
 SET CONSTRAINTS dispatch_continuation_pair DEFERRED; END IF; END $$;
 CREATE OR REPLACE FUNCTION dispatch_initialize_run_chain() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 IF NEW.first_run_id='' THEN NEW.first_run_id:=NEW.run_id; END IF;
 IF NEW.first_started_at IS NULL THEN NEW.first_started_at:=NEW.created_at; END IF;
 IF NEW.run_timeout=0 AND NEW.run_deadline_at IS NOT NULL THEN
 NEW.run_timeout:=(EXTRACT(EPOCH FROM (NEW.run_deadline_at-NEW.created_at))*1000000000)::bigint;
 END IF;
 IF NEW.first_started_at>NEW.created_at OR NEW.first_run_id='' OR
 (NEW.run_number=1 AND (NEW.first_run_id<>NEW.run_id OR NEW.previous_run_id<>'' OR NEW.first_started_at<>NEW.created_at)) OR
 (NEW.run_number>1 AND (NEW.first_run_id=NEW.run_id OR NEW.previous_run_id='' OR NEW.previous_run_id=NEW.run_id)) OR
 (NEW.next_run_id<>'' AND (NEW.next_run_id=NEW.run_id OR NEW.next_run_id=NEW.first_run_id OR NEW.next_run_id=NEW.previous_run_id OR NEW.state='running')) THEN
 RAISE EXCEPTION USING ERRCODE='DX004',MESSAGE='invalid run lineage'; END IF;
 RETURN NEW;
 END $$;
 DROP TRIGGER IF EXISTS dispatch_execution_chain_insert ON dispatch_executions;
 CREATE TRIGGER dispatch_execution_chain_insert BEFORE INSERT ON dispatch_executions FOR EACH ROW EXECUTE FUNCTION dispatch_initialize_run_chain();
`+runChainGuardSQL+`
 CREATE TRIGGER dispatch_execution_chain_update BEFORE UPDATE ON dispatch_executions FOR EACH ROW EXECUTE FUNCTION dispatch_guard_run_chain();
 CREATE TRIGGER dispatch_execution_deadline_update BEFORE UPDATE ON dispatch_executions FOR EACH ROW EXECUTE FUNCTION dispatch_guard_execution_deadline();
 CREATE INDEX IF NOT EXISTS idx_dispatch_execution_chain ON dispatch_executions(namespace,workflow_id,first_run_id,run_number)`)
		return err
	}, Down: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE dispatch_execution_tasks IN SHARE ROW EXCLUSIVE MODE;
 DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='dispatch_executions'::regclass AND attname='first_run_id' AND NOT attisdropped) THEN
 IF EXISTS(SELECT 1 FROM dispatch_executions WHERE run_number<>1 OR first_run_id<>run_id OR previous_run_id<>'' OR next_run_id<>'') THEN
 RAISE EXCEPTION 'retained run chains prevent downgrade'; END IF; END IF; END $$;
 DROP TRIGGER IF EXISTS dispatch_execution_chain_insert ON dispatch_executions;
 DROP TRIGGER IF EXISTS dispatch_execution_chain_update ON dispatch_executions;
 DROP FUNCTION IF EXISTS dispatch_initialize_run_chain();
 DROP FUNCTION IF EXISTS dispatch_guard_run_chain();
 DROP INDEX IF EXISTS idx_dispatch_execution_chain;
 ALTER TABLE dispatch_executions DROP COLUMN IF EXISTS first_run_id,DROP COLUMN IF EXISTS previous_run_id,
 DROP COLUMN IF EXISTS next_run_id,DROP COLUMN IF EXISTS run_number,DROP COLUMN IF EXISTS first_started_at,DROP COLUMN IF EXISTS run_timeout`)
		return err
	}})
}

// Retrying the base migration preserves the optional atomic-handoff guard.
const runChainGuardSQL = ` CREATE OR REPLACE FUNCTION dispatch_guard_run_chain() RETURNS trigger LANGUAGE plpgsql AS $$
 DECLARE allowed boolean;
 BEGIN
 IF NEW.first_run_id IS DISTINCT FROM OLD.first_run_id OR NEW.previous_run_id IS DISTINCT FROM OLD.previous_run_id OR
 NEW.run_number IS DISTINCT FROM OLD.run_number OR NEW.first_started_at IS DISTINCT FROM OLD.first_started_at OR NEW.run_timeout IS DISTINCT FROM OLD.run_timeout OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
 RAISE EXCEPTION USING ERRCODE='DX004',MESSAGE='run lineage is immutable'; END IF;
 IF NEW.next_run_id IS DISTINCT FROM OLD.next_run_id THEN
 IF to_regprocedure('dispatch_continuation_link_allowed(dispatch_executions,dispatch_executions)') IS NOT NULL THEN
 EXECUTE 'SELECT dispatch_continuation_link_allowed($1,$2)' INTO allowed USING OLD,NEW;
 IF allowed THEN RETURN NEW; END IF; END IF;
 RAISE EXCEPTION USING ERRCODE='DX004',MESSAGE='run successor is immutable'; END IF;
 RETURN NEW;
 END $$;
`
