package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "durable_namespace_outbox", Version: "20261027120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, namespaceOutboxSQL)
		return err
	}, Down: func(_ context.Context, _ migrate.Executor) error {
		return fmt.Errorf("durable audit ownership and retained intent evidence prohibit downgrade")
	}})
}

const namespaceOutboxSQL = `
CREATE TABLE dispatch_durable_namespaces (
 namespace TEXT PRIMARY KEY CHECK(octet_length(namespace) BETWEEN 1 AND 256),
 installation_id TEXT NOT NULL CHECK(octet_length(installation_id) BETWEEN 1 AND 256),
 app_id TEXT NOT NULL CHECK(octet_length(app_id) BETWEEN 1 AND 256),
 tenant_id TEXT NOT NULL CHECK(octet_length(tenant_id) BETWEEN 1 AND 256),
 require_audit BOOLEAN NOT NULL, require_hooks BOOLEAN NOT NULL,
 schema_version INTEGER NOT NULL CHECK(schema_version=1),
 coverage_started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 writer_protocol INTEGER NOT NULL DEFAULT 1 CHECK(writer_protocol=1)
);
CREATE INDEX dispatch_durable_namespaces_installation ON dispatch_durable_namespaces(installation_id,namespace);
CREATE TABLE dispatch_durable_outbox (
 id TEXT PRIMARY KEY, installation_id TEXT NOT NULL, namespace TEXT NOT NULL REFERENCES dispatch_durable_namespaces(namespace),
 destination TEXT NOT NULL CHECK(destination IN ('chronicle','relay')), schema_version INTEGER NOT NULL CHECK(schema_version=1),
 app_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
 workflow_id TEXT NOT NULL, run_id TEXT NOT NULL, source_kind TEXT NOT NULL, source_id TEXT NOT NULL, sequence BIGINT NOT NULL,
 fingerprint TEXT NOT NULL, envelope JSONB NOT NULL,
 accepted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 owner TEXT NOT NULL DEFAULT '', epoch BIGINT NOT NULL DEFAULT 0, attempts BIGINT NOT NULL DEFAULT 0,
 lease_until TIMESTAMPTZ, next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 delivered_at TIMESTAMPTZ, receipt JSONB, error_category TEXT NOT NULL DEFAULT '',
 UNIQUE(installation_id,destination,namespace,workflow_id,run_id,source_kind,source_id)
);
CREATE INDEX dispatch_outbox_claim ON dispatch_durable_outbox(installation_id,destination,next_attempt_at,id) WHERE delivered_at IS NULL;
CREATE OR REPLACE FUNCTION dispatch_audit_lock_key(ns TEXT) RETURNS BIGINT LANGUAGE SQL IMMUTABLE AS $$ SELECT hashtextextended('dispatch.audit.namespace:'||ns, 764831) $$;
CREATE OR REPLACE FUNCTION dispatch_audit_writer_lock(ns TEXT) RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF current_setting('transaction_isolation') <> 'read committed' THEN RAISE EXCEPTION USING ERRCODE='DA001',MESSAGE='durable audit requires READ COMMITTED'; END IF;
 PERFORM pg_advisory_xact_lock_shared(dispatch_audit_lock_key(ns));
END $$;
CREATE OR REPLACE FUNCTION dispatch_audit_catalog_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF current_setting('transaction_isolation') <> 'read committed' THEN RAISE EXCEPTION USING ERRCODE='DA001',MESSAGE='durable audit requires READ COMMITTED'; END IF;
 IF TG_OP <> 'INSERT' THEN RAISE EXCEPTION 'durable namespace ownership is immutable'; END IF;
 -- Registration is catalog-only. Reject lock upgrades by checking this backend's
 -- granted shared advisory lock before requesting the exclusive form.
 IF EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND mode='ShareLock'
 AND classid=((dispatch_audit_lock_key(NEW.namespace)>>32)&4294967295)::oid
 AND objid=(dispatch_audit_lock_key(NEW.namespace)&4294967295)::oid AND objsubid=1) THEN
 RAISE EXCEPTION 'namespace registration cannot upgrade a writer transaction'; END IF;
 PERFORM pg_advisory_xact_lock(dispatch_audit_lock_key(NEW.namespace));
 NEW.coverage_started_at:=clock_timestamp(); NEW.writer_protocol:=1;
 RETURN NEW;
END $$;
CREATE TRIGGER dispatch_namespace_guard BEFORE INSERT OR UPDATE OR DELETE ON dispatch_durable_namespaces FOR EACH ROW EXECUTE FUNCTION dispatch_audit_catalog_guard();
CREATE OR REPLACE FUNCTION dispatch_audit_write_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 PERFORM dispatch_audit_writer_lock(NEW.namespace);
 RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION dispatch_audit_intent_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE n dispatch_durable_namespaces; rowdata JSONB; kind TEXT; source TEXT; wf TEXT; run TEXT; seq BIGINT:=0; dest TEXT;
BEGIN
 PERFORM dispatch_audit_writer_lock(NEW.namespace);
 SELECT * INTO n FROM dispatch_durable_namespaces WHERE namespace=NEW.namespace;
 IF NOT FOUND THEN RETURN NEW; END IF;
 rowdata:=to_jsonb(NEW);wf:=rowdata->>'workflow_id';run:=rowdata->>'run_id';
 CASE TG_TABLE_NAME
 WHEN 'dispatch_execution_events' THEN kind:='event';source:=rowdata->>'sequence';seq:=(rowdata->>'sequence')::BIGINT;
 WHEN 'dispatch_execution_receipts' THEN kind:='execution_receipt';source:=rowdata->>'request_id';
 WHEN 'dispatch_signal_receipts' THEN kind:='signal_receipt';source:=rowdata->>'request_id';
 WHEN 'dispatch_cancellation_receipts' THEN kind:='cancellation_receipt';source:=rowdata->>'request_id';
 WHEN 'dispatch_child_delivery_receipts' THEN kind:='child_receipt';wf:=rowdata->>'source_workflow_id';run:=rowdata->>'source_run_id';
 source:=encode(sha256(convert_to(octet_length(rowdata->>'delivery_id')::TEXT||':'||(rowdata->>'delivery_id')||octet_length(rowdata->>'request_id')::TEXT||':'||(rowdata->>'request_id'),'UTF8')),'hex');
 END CASE;
 IF kind IN ('execution_receipt','signal_receipt','cancellation_receipt') THEN source:=encode(sha256(convert_to(octet_length(source)::TEXT||':'||source,'UTF8')),'hex'); END IF;
 FOREACH dest IN ARRAY ARRAY['chronicle','relay'] LOOP
 IF (dest='chronicle' AND n.require_audit) OR (dest='relay' AND n.require_hooks AND kind='event') THEN
 IF NOT EXISTS(SELECT 1 FROM dispatch_durable_outbox o WHERE o.namespace=n.namespace AND o.installation_id=n.installation_id AND o.app_id=n.app_id AND o.tenant_id=n.tenant_id AND o.schema_version=n.schema_version AND o.destination=dest AND o.workflow_id=wf AND o.run_id=run AND o.source_kind=kind AND o.source_id=source AND o.sequence=seq) THEN
 RAISE EXCEPTION USING ERRCODE='DA002',MESSAGE='required durable delivery intent missing'; END IF;
 END IF;
 END LOOP;
 RETURN NEW;
END $$;
DO $$ DECLARE t TEXT; BEGIN
 FOREACH t IN ARRAY ARRAY['dispatch_execution_events','dispatch_execution_receipts','dispatch_signal_receipts','dispatch_cancellation_receipts','dispatch_child_delivery_receipts'] LOOP
 EXECUTE format('CREATE TRIGGER dispatch_audit_write BEFORE INSERT ON %I FOR EACH ROW EXECUTE FUNCTION dispatch_audit_write_guard()',t);
 EXECUTE format('CREATE CONSTRAINT TRIGGER dispatch_audit_intent AFTER INSERT ON %I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_audit_intent_guard()',t);
 END LOOP;
END $$;
CREATE OR REPLACE FUNCTION dispatch_outbox_immutable_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'delivery evidence cannot be deleted'; END IF;
 IF (to_jsonb(NEW)-ARRAY['owner','epoch','attempts','lease_until','next_attempt_at','delivered_at','receipt','error_category']) IS DISTINCT FROM
 (to_jsonb(OLD)-ARRAY['owner','epoch','attempts','lease_until','next_attempt_at','delivered_at','receipt','error_category']) THEN RAISE EXCEPTION 'delivery envelope is immutable'; END IF;
 IF OLD.delivered_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'accepted delivery evidence is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER dispatch_outbox_immutable BEFORE UPDATE OR DELETE ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION dispatch_outbox_immutable_guard();
`
