package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "durable_retirement_coordination", Version: "20261031120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, retirementCoordinationSQL)
		return err
	}, Down: func(_ context.Context, _ migrate.Executor) error {
		return fmt.Errorf("durable retirement writer floors prohibit downgrade")
	}})
}

const retirementCoordinationSQL = `
CREATE TABLE dispatch_retirement_namespaces (
 namespace TEXT PRIMARY KEY REFERENCES dispatch_durable_namespaces(namespace),
 schema_version INTEGER NOT NULL CHECK(schema_version=1),
 writer_protocol INTEGER NOT NULL CHECK(writer_protocol=1),
 version BIGINT NOT NULL CHECK(version>0),
 enrolled_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE dispatch_build_lifecycle (
 namespace TEXT NOT NULL REFERENCES dispatch_retirement_namespaces(namespace) DEFERRABLE INITIALLY DEFERRED,
 build_id TEXT NOT NULL CHECK(octet_length(build_id) BETWEEN 1 AND 512),
 state TEXT NOT NULL CHECK(state IN ('accepting','retiring','retired')),
 epoch BIGINT NOT NULL CHECK(epoch>0), version BIGINT NOT NULL CHECK(version>0),
 cutoff_epoch BIGINT NOT NULL DEFAULT 0 CHECK(cutoff_epoch>=0),
 changed_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(namespace,build_id)
);
CREATE TABLE dispatch_lifecycle_receipts (
 namespace TEXT NOT NULL REFERENCES dispatch_durable_namespaces(namespace),
 operation TEXT NOT NULL CHECK(operation IN ('namespace.retirement.enroll')),
 request_id TEXT NOT NULL CHECK(octet_length(request_id) BETWEEN 1 AND 256),
 request_digest TEXT NOT NULL, command_digest TEXT NOT NULL DEFAULT '',
 response_version INTEGER NOT NULL CHECK(response_version=1), response JSONB NOT NULL,
 accepted_at TIMESTAMPTZ NOT NULL, delivery_id TEXT NOT NULL REFERENCES dispatch_durable_outbox(id),
 PRIMARY KEY(namespace,operation,request_id)
);
CREATE OR REPLACE FUNCTION dispatch_retirement_check_writer(ns TEXT) RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE floor dispatch_retirement_namespaces; marker TEXT;
BEGIN
 -- This separate query runs after coordination with a fresh READ COMMITTED
 -- snapshot, including when the caller's outer statement started earlier.
 SELECT * INTO floor FROM dispatch_retirement_namespaces WHERE namespace=ns;
 IF NOT FOUND THEN RETURN; END IF;
 marker:=current_setting('dispatch.retirement_writer_protocol',TRUE);
 IF floor.schema_version<>1 OR floor.writer_protocol<>1 OR marker IS DISTINCT FROM '1' THEN
  RAISE EXCEPTION USING ERRCODE='DL001', MESSAGE='incompatible retirement writer protocol';
 END IF;
END $$;
CREATE OR REPLACE FUNCTION dispatch_audit_writer_lock(ns TEXT) RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='DA001',MESSAGE='durable audit requires READ COMMITTED';
 END IF;
 -- Unchanged old code and row guards can enter after taking a work-row lock.
 -- Never wait behind an exclusive coordinator while retaining that row.
 IF NOT pg_try_advisory_xact_lock_shared(dispatch_audit_lock_key(ns)) THEN
  RAISE EXCEPTION USING ERRCODE='DL002',MESSAGE='durable namespace coordination busy';
 END IF;
 PERFORM dispatch_retirement_check_writer(ns);
END $$;
CREATE OR REPLACE FUNCTION dispatch_retirement_writer_lock(ns TEXT, protocol INTEGER) RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='DA001',MESSAGE='durable audit requires READ COMMITTED';
 END IF;
 IF protocol<>1 OR protocol IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='DL001',MESSAGE='incompatible retirement writer protocol';
 END IF;
 PERFORM pg_advisory_xact_lock_shared(dispatch_audit_lock_key(ns));
 PERFORM set_config('dispatch.retirement_writer_protocol','1',TRUE);
 PERFORM dispatch_retirement_check_writer(ns);
END $$;
CREATE OR REPLACE FUNCTION dispatch_retirement_coordinator_lock(ns TEXT, protocol INTEGER) RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='DA001',MESSAGE='durable audit requires READ COMMITTED';
 END IF;
 IF protocol<>1 OR protocol IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='DL001',MESSAGE='incompatible retirement writer protocol';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND mode='ShareLock'
 AND classid=((dispatch_audit_lock_key(ns)>>32)&4294967295)::oid
 AND objid=(dispatch_audit_lock_key(ns)&4294967295)::oid AND objsubid=1) AND NOT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND mode='ExclusiveLock' AND granted AND classid=((dispatch_audit_lock_key(ns)>>32)&4294967295)::oid AND objid=(dispatch_audit_lock_key(ns)&4294967295)::oid AND objsubid=1) THEN
  RAISE EXCEPTION USING ERRCODE='DL002',MESSAGE='retirement coordination cannot upgrade a writer transaction';
 END IF;
 PERFORM pg_advisory_xact_lock(dispatch_audit_lock_key(ns));
 PERFORM set_config('dispatch.retirement_writer_protocol','1',TRUE);
 PERFORM dispatch_retirement_check_writer(ns);
END $$;
CREATE OR REPLACE FUNCTION dispatch_retirement_write_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE ns TEXT;
BEGIN
 IF TG_OP='DELETE' THEN ns:=OLD.namespace; ELSE ns:=NEW.namespace; END IF;
 IF TG_OP='UPDATE' AND NEW.namespace IS DISTINCT FROM OLD.namespace THEN
  RAISE EXCEPTION USING ERRCODE='DL001',MESSAGE='durable namespace identity is immutable';
 END IF;
 PERFORM dispatch_audit_writer_lock(ns);
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
DO $$ DECLARE t TEXT; BEGIN
 FOREACH t IN ARRAY ARRAY['dispatch_executions','dispatch_execution_heads','dispatch_execution_tasks','dispatch_execution_events','dispatch_execution_receipts','dispatch_signal_receipts','dispatch_cancellation_receipts','dispatch_child_executions','dispatch_child_deliveries','dispatch_child_delivery_receipts','dispatch_build_lifecycle','dispatch_lifecycle_receipts'] LOOP
 EXECUTE format('CREATE TRIGGER dispatch_retirement_write BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION dispatch_retirement_write_guard()',t);
 END LOOP;
END $$;
CREATE OR REPLACE FUNCTION dispatch_retirement_floor_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION USING ERRCODE='DL001',MESSAGE='retirement writer floor is immutable'; END IF;
 PERFORM dispatch_retirement_coordinator_lock(NEW.namespace,1);
 IF NOT EXISTS(SELECT 1 FROM dispatch_durable_namespaces WHERE namespace=NEW.namespace AND require_audit) THEN
  RAISE EXCEPTION USING ERRCODE='DA002',MESSAGE='retirement requires transactional audit';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER dispatch_retirement_floor BEFORE INSERT OR UPDATE OR DELETE ON dispatch_retirement_namespaces FOR EACH ROW EXECUTE FUNCTION dispatch_retirement_floor_guard();
CREATE OR REPLACE FUNCTION dispatch_lifecycle_receipt_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'lifecycle receipt is immutable'; END $$;
CREATE TRIGGER dispatch_lifecycle_immutable BEFORE UPDATE OR DELETE ON dispatch_lifecycle_receipts FOR EACH ROW EXECUTE FUNCTION dispatch_lifecycle_receipt_immutable();
CREATE OR REPLACE FUNCTION dispatch_lifecycle_intent_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE source TEXT; n dispatch_durable_namespaces;
BEGIN
 PERFORM dispatch_audit_writer_lock(NEW.namespace);
 SELECT * INTO n FROM dispatch_durable_namespaces WHERE namespace=NEW.namespace;
 source:=encode(sha256(convert_to(octet_length('dispatch.lifecycle.v1')::TEXT||':'||'dispatch.lifecycle.v1'||octet_length(NEW.namespace)::TEXT||':'||NEW.namespace||octet_length(NEW.operation)::TEXT||':'||NEW.operation||octet_length(NEW.request_id)::TEXT||':'||NEW.request_id,'UTF8')),'hex');
 IF NOT n.require_audit OR NOT EXISTS(SELECT 1 FROM dispatch_durable_outbox o WHERE o.id=NEW.delivery_id AND o.namespace=n.namespace AND o.installation_id=n.installation_id AND o.app_id=n.app_id AND o.tenant_id=n.tenant_id AND o.schema_version=n.schema_version AND o.destination='chronicle' AND o.workflow_id='' AND o.run_id='' AND o.source_kind='security' AND o.source_id=source AND o.sequence=0 AND o.envelope->>'Action'=CASE NEW.operation WHEN 'namespace.retirement.enroll' THEN 'dispatch.retirement.enroll' WHEN 'build.register' THEN 'dispatch.build.register' WHEN 'build.retirement.begin' THEN 'dispatch.build.retire' WHEN 'build.retirement.finalize' THEN 'dispatch.build.finalize' WHEN 'build.retirement.abort' THEN 'dispatch.build.resume' END AND o.envelope->>'Outcome'='accepted') THEN
  RAISE EXCEPTION USING ERRCODE='DA002',MESSAGE='required lifecycle delivery intent missing';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER dispatch_lifecycle_intent AFTER INSERT ON dispatch_lifecycle_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_lifecycle_intent_guard();
CREATE OR REPLACE FUNCTION dispatch_retirement_enrollment_receipt() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM dispatch_lifecycle_receipts r WHERE r.namespace=NEW.namespace AND r.operation='namespace.retirement.enroll') THEN
  RAISE EXCEPTION USING ERRCODE='DA002',MESSAGE='required retirement enrollment receipt missing';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER dispatch_retirement_enrollment AFTER INSERT ON dispatch_retirement_namespaces DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_retirement_enrollment_receipt();
`
