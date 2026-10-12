package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "durable_worker_drain_requests", Version: "20261104120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, workerDrainSQL)
		return err
	}, Down: func(context.Context, migrate.Executor) error {
		return fmt.Errorf("durable worker drain receipts prohibit downgrade")
	}})
}

const workerDrainSQL = `ALTER TABLE dispatch_lifecycle_receipts DROP CONSTRAINT dispatch_lifecycle_receipts_operation_check;
ALTER TABLE dispatch_lifecycle_receipts ADD CONSTRAINT dispatch_lifecycle_receipts_operation_check CHECK(operation IN ('namespace.retirement.enroll','build.register','build.retirement.begin','build.retirement.finalize','build.retirement.abort','query_runtime.register','query_runtime.verify','query_runtime.remove','query_runtime.finish','query_runtime.abort','worker.drain.request'));
CREATE OR REPLACE FUNCTION dispatch_lifecycle_intent_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE source TEXT; n dispatch_durable_namespaces;
BEGIN
 PERFORM dispatch_audit_writer_lock(NEW.namespace);
 SELECT * INTO n FROM dispatch_durable_namespaces WHERE namespace=NEW.namespace;
 source:=encode(sha256(convert_to(octet_length('dispatch.lifecycle.v1')::TEXT||':'||'dispatch.lifecycle.v1'||octet_length(NEW.namespace)::TEXT||':'||NEW.namespace||octet_length(NEW.operation)::TEXT||':'||NEW.operation||octet_length(NEW.request_id)::TEXT||':'||NEW.request_id,'UTF8')),'hex');
 IF NOT n.require_audit OR NOT EXISTS(SELECT 1 FROM dispatch_durable_outbox o WHERE o.id=NEW.delivery_id AND o.namespace=n.namespace AND o.installation_id=n.installation_id AND o.app_id=n.app_id AND o.tenant_id=n.tenant_id AND o.schema_version=n.schema_version AND o.destination='chronicle' AND o.workflow_id='' AND o.run_id='' AND o.source_kind='security' AND o.source_id=source AND o.sequence=0 AND o.envelope->>'Action'=CASE NEW.operation WHEN 'namespace.retirement.enroll' THEN 'dispatch.retirement.enroll' WHEN 'build.register' THEN 'dispatch.build.register' WHEN 'build.retirement.begin' THEN 'dispatch.build.retire' WHEN 'build.retirement.finalize' THEN 'dispatch.build.finalize' WHEN 'build.retirement.abort' THEN 'dispatch.build.resume' WHEN 'query_runtime.register' THEN 'dispatch.query_runtime.register' WHEN 'query_runtime.verify' THEN 'dispatch.query_runtime.verify' WHEN 'query_runtime.remove' THEN 'dispatch.query_runtime.remove' WHEN 'query_runtime.finish' THEN 'dispatch.query_runtime.finish' WHEN 'query_runtime.abort' THEN 'dispatch.query_runtime.abort' WHEN 'worker.drain.request' THEN 'dispatch.worker.drain.request' END AND o.envelope->>'Outcome'='accepted') THEN
  RAISE EXCEPTION USING ERRCODE='DA002',MESSAGE='required lifecycle delivery intent missing';
 END IF;
 RETURN NEW;
END $$;
CREATE FUNCTION dispatch_worker_drain_schema_version() RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$;
`
