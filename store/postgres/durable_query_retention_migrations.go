package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "durable_query_retention", Version: "20261103120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, queryRetentionSQL)
		return err
	}, Down: func(context.Context, migrate.Executor) error {
		return fmt.Errorf("durable query retention fences prohibit downgrade")
	}})
}

const queryRetentionSQL = `
ALTER TABLE dispatch_build_lifecycle ADD COLUMN query_identity JSONB;
CREATE TABLE dispatch_query_runtimes (
 namespace TEXT NOT NULL, build_id TEXT NOT NULL, runtime_id TEXT NOT NULL,
 instance_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('active','removing','removed')),
 version BIGINT NOT NULL CHECK(version>0), binding JSONB NOT NULL,
 PRIMARY KEY(namespace,runtime_id),
 FOREIGN KEY(namespace,build_id) REFERENCES dispatch_build_lifecycle(namespace,build_id)
);
CREATE INDEX dispatch_query_instance ON dispatch_query_runtimes(namespace,instance_id,runtime_id);
CREATE TRIGGER dispatch_retirement_write BEFORE INSERT OR UPDATE OR DELETE ON dispatch_query_runtimes FOR EACH ROW EXECUTE FUNCTION dispatch_retirement_write_guard();
ALTER TABLE dispatch_lifecycle_receipts DROP CONSTRAINT dispatch_lifecycle_receipts_operation_check;
ALTER TABLE dispatch_lifecycle_receipts ADD CONSTRAINT dispatch_lifecycle_receipts_operation_check CHECK(operation IN ('namespace.retirement.enroll','build.register','build.retirement.begin','build.retirement.finalize','build.retirement.abort','query_runtime.register','query_runtime.verify','query_runtime.remove','query_runtime.finish','query_runtime.abort'));
CREATE OR REPLACE FUNCTION dispatch_lifecycle_intent_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE source TEXT; n dispatch_durable_namespaces;
BEGIN
 PERFORM dispatch_audit_writer_lock(NEW.namespace);
 SELECT * INTO n FROM dispatch_durable_namespaces WHERE namespace=NEW.namespace;
 source:=encode(sha256(convert_to(octet_length('dispatch.lifecycle.v1')::TEXT||':'||'dispatch.lifecycle.v1'||octet_length(NEW.namespace)::TEXT||':'||NEW.namespace||octet_length(NEW.operation)::TEXT||':'||NEW.operation||octet_length(NEW.request_id)::TEXT||':'||NEW.request_id,'UTF8')),'hex');
 IF NOT n.require_audit OR NOT EXISTS(SELECT 1 FROM dispatch_durable_outbox o WHERE o.id=NEW.delivery_id AND o.namespace=n.namespace AND o.installation_id=n.installation_id AND o.app_id=n.app_id AND o.tenant_id=n.tenant_id AND o.schema_version=n.schema_version AND o.destination='chronicle' AND o.workflow_id='' AND o.run_id='' AND o.source_kind='security' AND o.source_id=source AND o.sequence=0 AND o.envelope->>'Action'=CASE NEW.operation WHEN 'namespace.retirement.enroll' THEN 'dispatch.retirement.enroll' WHEN 'build.register' THEN 'dispatch.build.register' WHEN 'build.retirement.begin' THEN 'dispatch.build.retire' WHEN 'build.retirement.finalize' THEN 'dispatch.build.finalize' WHEN 'build.retirement.abort' THEN 'dispatch.build.resume' WHEN 'query_runtime.register' THEN 'dispatch.query_runtime.register' WHEN 'query_runtime.verify' THEN 'dispatch.query_runtime.verify' WHEN 'query_runtime.remove' THEN 'dispatch.query_runtime.remove' WHEN 'query_runtime.finish' THEN 'dispatch.query_runtime.finish' WHEN 'query_runtime.abort' THEN 'dispatch.query_runtime.abort' END AND o.envelope->>'Outcome'='accepted') THEN
  RAISE EXCEPTION USING ERRCODE='DA002',MESSAGE='required lifecycle delivery intent missing';
 END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION dispatch_query_coordinator_owned(ns TEXT) RETURNS BOOLEAN LANGUAGE sql VOLATILE AS $$
 SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND mode='ExclusiveLock' AND granted AND classid=((dispatch_audit_lock_key(ns)>>32)&4294967295)::oid AND objid=(dispatch_audit_lock_key(ns)&4294967295)::oid AND objsubid=1)
$$;
CREATE FUNCTION dispatch_query_binding_verified(b JSONB,identity JSONB,observed TIMESTAMPTZ) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $$
 SELECT identity IS NOT NULL AND b->>'State'='active' AND b->'BuildIdentity'=identity
 AND b->'Verification'->'Identity'=jsonb_build_object('InstallationID',b->'InstallationID','Namespace',b->'Namespace','BuildID',b->'BuildID','RuntimeID',b->'RuntimeID','InstanceID',b->'InstanceID','IdentityVersion',b->'IdentityVersion','BuildIdentity',b->'BuildIdentity')
 AND b->'Verification'->>'VerifierID'=identity->>'VerifierID'
 AND b->'Verification'->>'ProbePolicyID'=identity->>'ProbePolicyID'
 AND b->'Verification'->>'ProbePolicyVersion'=identity->>'ProbePolicyVersion'
 AND length(b->'Verification'->>'ProofID') BETWEEN 1 AND 256
 AND b->'Verification'->>'EvidenceDigest' ~ '^[0-9a-f]{64}$'
 AND (b->'Verification'->>'VerifiedAt')::TIMESTAMPTZ<=observed
 AND (b->'Verification'->>'AcceptedAt')::TIMESTAMPTZ BETWEEN (b->'Verification'->>'VerifiedAt')::TIMESTAMPTZ AND observed
 AND (b->'Verification'->>'ValidUntil')::TIMESTAMPTZ>observed
 AND (b->'Verification'->>'ValidUntil')::TIMESTAMPTZ-(b->'Verification'->>'VerifiedAt')::TIMESTAMPTZ<=((identity->>'MaximumProofValidity')::NUMERIC/1000000000)*INTERVAL '1 second'
$$;
CREATE FUNCTION dispatch_query_build_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE observed TIMESTAMPTZ;
BEGIN
 IF TG_OP='UPDATE' AND NEW.query_identity IS DISTINCT FROM OLD.query_identity THEN
  IF NOT dispatch_query_coordinator_owned(NEW.namespace) THEN RAISE EXCEPTION USING ERRCODE='DL002',MESSAGE='query identity requires existing namespace coordinator'; END IF;
  IF OLD.query_identity IS NOT NULL OR NEW.query_identity IS NULL THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='query build identity is immutable'; END IF;
 END IF;
 IF TG_OP='UPDATE' AND NEW.state='retired' AND OLD.state<>'retired' THEN
  -- A row fallback must never acquire or upgrade exclusive coordination.
  IF NOT dispatch_query_coordinator_owned(NEW.namespace) THEN RAISE EXCEPTION USING ERRCODE='DL002',MESSAGE='query retirement requires existing namespace coordinator'; END IF;
  observed:=clock_timestamp();
  IF EXISTS(SELECT 1 FROM dispatch_executions WHERE namespace=NEW.namespace AND build_id=NEW.build_id)
  AND NOT EXISTS(SELECT 1 FROM dispatch_query_runtimes q WHERE q.namespace=NEW.namespace AND q.build_id=NEW.build_id AND q.state='active' AND dispatch_query_binding_verified(q.binding,NEW.query_identity,observed)) THEN
   RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='verified query retention unavailable';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER dispatch_query_build BEFORE INSERT OR UPDATE ON dispatch_build_lifecycle FOR EACH ROW EXECUTE FUNCTION dispatch_query_build_guard();
CREATE FUNCTION dispatch_query_binding_guard() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF NOT dispatch_query_coordinator_owned(NEW.namespace) THEN RAISE EXCEPTION USING ERRCODE='DL002',MESSAGE='query binding requires existing namespace coordinator'; END IF;
 IF TG_OP='UPDATE' AND (NEW.build_id<>OLD.build_id OR NEW.runtime_id<>OLD.runtime_id OR NEW.instance_id<>OLD.instance_id OR NEW.binding->'BuildIdentity' IS DISTINCT FROM OLD.binding->'BuildIdentity' OR NEW.binding->'IdentityVersion' IS DISTINCT FROM OLD.binding->'IdentityVersion') THEN
  RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='query runtime identity is immutable';
 END IF;
 IF NEW.binding->>'Namespace' IS DISTINCT FROM NEW.namespace OR NEW.binding->>'BuildID' IS DISTINCT FROM NEW.build_id OR NEW.binding->>'RuntimeID' IS DISTINCT FROM NEW.runtime_id OR NEW.binding->>'InstanceID' IS DISTINCT FROM NEW.instance_id OR NEW.binding->>'State' IS DISTINCT FROM NEW.state OR (NEW.binding->>'Version')::BIGINT IS DISTINCT FROM NEW.version THEN
  RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='query runtime binding is inconsistent';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER dispatch_query_binding BEFORE INSERT OR UPDATE ON dispatch_query_runtimes FOR EACH ROW EXECUTE FUNCTION dispatch_query_binding_guard();
`
