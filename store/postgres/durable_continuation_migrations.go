package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "add_durable_continuation_integrity", Version: "20261024120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE dispatch_execution_tasks IN SHARE ROW EXCLUSIVE MODE;
`+runChainGuardSQL+`
 CREATE UNIQUE INDEX IF NOT EXISTS idx_dispatch_chain_run_number ON dispatch_executions(namespace,workflow_id,first_run_id,run_number);
 CREATE OR REPLACE FUNCTION dispatch_continuation_link_allowed(oldrow dispatch_executions,newrow dispatch_executions) RETURNS boolean LANGUAGE sql AS $$
 SELECT oldrow.next_run_id='' AND oldrow.state='running' AND newrow.state='continued_as_new' AND newrow.next_run_id<>''
 AND newrow.next_run_id NOT IN(oldrow.run_id,oldrow.first_run_id,oldrow.previous_run_id)
 AND newrow.revision=oldrow.revision+1 AND newrow.last_sequence>oldrow.last_sequence
 $$;
 CREATE OR REPLACE FUNCTION dispatch_check_continuation_pair() RETURNS trigger LANGUAGE plpgsql AS $$
 DECLARE currentrow dispatch_executions; otherrow dispatch_executions;
 BEGIN
 SELECT * INTO currentrow FROM dispatch_executions WHERE namespace=NEW.namespace AND workflow_id=NEW.workflow_id AND run_id=NEW.run_id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF currentrow.previous_run_id<>'' THEN
 SELECT * INTO otherrow FROM dispatch_executions WHERE namespace=currentrow.namespace AND workflow_id=currentrow.workflow_id AND run_id=currentrow.previous_run_id;
 IF NOT FOUND OR otherrow.next_run_id<>currentrow.run_id OR otherrow.state<>'continued_as_new'
 OR currentrow.first_run_id<>otherrow.first_run_id OR currentrow.run_number<>otherrow.run_number+1
 OR currentrow.first_started_at<>otherrow.first_started_at OR currentrow.created_at<>otherrow.updated_at
 OR currentrow.execution_deadline_at IS DISTINCT FROM otherrow.execution_deadline_at THEN
 RAISE EXCEPTION USING ERRCODE='DX004',MESSAGE='invalid predecessor relationship'; END IF;
 END IF;
 IF currentrow.next_run_id<>'' THEN
 SELECT * INTO otherrow FROM dispatch_executions WHERE namespace=currentrow.namespace AND workflow_id=currentrow.workflow_id AND run_id=currentrow.next_run_id;
 IF NOT FOUND OR otherrow.previous_run_id<>currentrow.run_id OR currentrow.state<>'continued_as_new'
 OR otherrow.first_run_id<>currentrow.first_run_id OR otherrow.run_number<>currentrow.run_number+1
 OR otherrow.first_started_at<>currentrow.first_started_at OR otherrow.created_at<>currentrow.updated_at
 OR otherrow.execution_deadline_at IS DISTINCT FROM currentrow.execution_deadline_at THEN
 RAISE EXCEPTION USING ERRCODE='DX004',MESSAGE='invalid successor relationship'; END IF;
 END IF;
 RETURN NULL;
 END $$;
 DROP TRIGGER IF EXISTS dispatch_continuation_pair ON dispatch_executions;
 CREATE CONSTRAINT TRIGGER dispatch_continuation_pair AFTER INSERT OR UPDATE ON dispatch_executions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_check_continuation_pair()`)
		return err
	}, Down: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `LOCK TABLE dispatch_executions IN SHARE ROW EXCLUSIVE MODE;
 DO $$ BEGIN IF EXISTS(SELECT 1 FROM dispatch_executions WHERE previous_run_id<>'' OR next_run_id<>'') THEN RAISE EXCEPTION 'retained run chains prevent downgrade'; END IF; END $$;
 DROP TRIGGER IF EXISTS dispatch_continuation_pair ON dispatch_executions;
 DROP FUNCTION IF EXISTS dispatch_check_continuation_pair();
 DROP FUNCTION IF EXISTS dispatch_continuation_link_allowed(dispatch_executions,dispatch_executions);
 DROP INDEX IF EXISTS idx_dispatch_chain_run_number`)
		return err
	}})
}
