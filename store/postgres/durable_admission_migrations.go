package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "durable_build_admission", Version: "20261101120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, buildAdmissionSQL)
		return err
	}, Down: func(context.Context, migrate.Executor) error {
		return fmt.Errorf("durable admission epochs prohibit downgrade")
	}})
}

const buildAdmissionSQL = `
ALTER TABLE dispatch_lifecycle_receipts DROP CONSTRAINT dispatch_lifecycle_receipts_operation_check;
ALTER TABLE dispatch_lifecycle_receipts ADD CONSTRAINT dispatch_lifecycle_receipts_operation_check CHECK(operation IN ('namespace.retirement.enroll','build.register','build.retirement.begin','build.retirement.finalize','build.retirement.abort'));
ALTER TABLE dispatch_executions ADD COLUMN admission_epoch BIGINT NOT NULL DEFAULT 0 CHECK(admission_epoch>=0);
ALTER TABLE dispatch_executions ALTER COLUMN admission_epoch SET DEFAULT -1;
CREATE OR REPLACE FUNCTION dispatch_admission_metadata_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 PERFORM dispatch_audit_writer_lock(NEW.namespace);
 IF TG_OP='UPDATE' THEN
  IF NEW.admission_epoch IS DISTINCT FROM OLD.admission_epoch THEN RAISE EXCEPTION USING ERRCODE='DL003',MESSAGE='execution admission epoch is immutable'; END IF;
 ELSE
  IF NEW.admission_epoch=-1 THEN
   SELECT COALESCE((SELECT epoch FROM dispatch_build_lifecycle WHERE namespace=NEW.namespace AND build_id=NEW.build_id),0) INTO NEW.admission_epoch;
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER dispatch_admission_metadata BEFORE INSERT OR UPDATE ON dispatch_executions FOR EACH ROW EXECUTE FUNCTION dispatch_admission_metadata_guard();
CREATE OR REPLACE FUNCTION dispatch_build_admission_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE b dispatch_build_lifecycle; source dispatch_executions; kind TEXT:='root'; command TEXT:=''; expected BIGINT; inherited BOOLEAN:=FALSE;
BEGIN
 PERFORM dispatch_audit_writer_lock(NEW.namespace);
 IF NOT EXISTS(SELECT 1 FROM dispatch_retirement_namespaces WHERE namespace=NEW.namespace) THEN
  IF NEW.admission_epoch<>0 THEN RAISE EXCEPTION USING ERRCODE='DL003',MESSAGE='invalid unenrolled admission epoch'; END IF;
  RETURN NEW;
 END IF;
 SELECT * INTO b FROM dispatch_build_lifecycle WHERE namespace=NEW.namespace AND build_id=NEW.build_id;
 IF NOT FOUND THEN b.build_id:=NEW.build_id;b.state:='unregistered';b.epoch:=0; END IF;
 IF NEW.previous_run_id<>'' THEN
  kind:='continuation';
  SELECT * INTO source FROM dispatch_executions WHERE namespace=NEW.namespace AND workflow_id=NEW.workflow_id AND run_id=NEW.previous_run_id;
 ELSE
  SELECT c.command_id INTO command FROM dispatch_child_executions c WHERE (c.namespace,c.child_workflow_id,c.child_run_id)=(NEW.namespace,NEW.workflow_id,NEW.run_id);
  IF FOUND THEN
   kind:='child';
   SELECT p.* INTO source FROM dispatch_child_executions c JOIN dispatch_executions p ON (p.namespace,p.workflow_id,p.run_id)=(c.namespace,c.parent_workflow_id,c.parent_run_id) WHERE (c.namespace,c.child_workflow_id,c.child_run_id)=(NEW.namespace,NEW.workflow_id,NEW.run_id);
  END IF;
 END IF;
 inherited:=source.run_id IS NOT NULL AND source.build_id=NEW.build_id;
 expected:=CASE WHEN inherited THEN source.admission_epoch ELSE b.epoch END;
 IF (b.state='accepting' OR (b.state='retiring' AND inherited AND source.admission_epoch<=b.cutoff_epoch)) AND NEW.admission_epoch=expected THEN RETURN NEW; END IF;
 RAISE EXCEPTION USING ERRCODE='DL003',MESSAGE='build admission refused',DETAIL=json_build_object('BuildID',NEW.build_id,'State',b.state,'RetirementEpoch',b.epoch,'ReferenceKind',kind,'CommandID',COALESCE(command,''))::TEXT;
END $$;
CREATE CONSTRAINT TRIGGER dispatch_build_admission AFTER INSERT ON dispatch_executions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_build_admission_guard();
CREATE OR REPLACE FUNCTION dispatch_build_intent_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM dispatch_lifecycle_receipts r WHERE r.namespace=NEW.namespace AND r.operation IN ('build.register','build.retirement.begin','build.retirement.finalize','build.retirement.abort') AND r.response->'Build'->>'BuildID'=NEW.build_id AND (r.response->'Build'->>'Version')::BIGINT=NEW.version AND r.response->'Build'->>'State'=NEW.state AND (r.response->'Build'->>'Epoch')::BIGINT=NEW.epoch AND (r.response->'Build'->>'CutoffEpoch')::BIGINT=NEW.cutoff_epoch AND r.accepted_at=NEW.changed_at) THEN RETURN NEW; END IF;
 IF TG_OP='INSERT' AND NEW.state='accepting' AND NEW.epoch=1 AND NEW.version=1 AND EXISTS(SELECT 1 FROM dispatch_retirement_namespaces n JOIN dispatch_lifecycle_receipts r USING(namespace) WHERE n.namespace=NEW.namespace AND n.enrolled_at=NEW.changed_at AND r.operation='namespace.retirement.enroll' AND r.accepted_at=n.enrolled_at) THEN RETURN NEW; END IF;
 RAISE EXCEPTION USING ERRCODE='DA002',MESSAGE='required build lifecycle receipt missing';
END $$;
CREATE CONSTRAINT TRIGGER dispatch_build_intent AFTER INSERT OR UPDATE ON dispatch_build_lifecycle DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_build_intent_guard();
`
