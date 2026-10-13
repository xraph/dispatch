package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"

	"github.com/xraph/dispatch/durable"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "durable_query_removal_guards", Version: "20261105120000", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, fmt.Sprintf(queryRemovalGuardSQL, durable.QueryRemovalCommitReceiptLimit))
		return err
	}, Down: func(context.Context, migrate.Executor) error {
		return fmt.Errorf("durable query removal guards prohibit downgrade")
	}})
}

const queryRemovalGuardSQL = `
CREATE TABLE dispatch_query_removal_guards (
 namespace TEXT NOT NULL REFERENCES dispatch_durable_namespaces(namespace), guard_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('active','closed')), data JSON NOT NULL CHECK(octet_length(data::text)<=8192),
 PRIMARY KEY(namespace,guard_id),
 CHECK(data->>'GuardID'=guard_id AND data->>'State'=state)
);
CREATE TABLE dispatch_query_removal_receipts (
 namespace TEXT NOT NULL, request_id TEXT NOT NULL, guard_id TEXT NOT NULL,
 candidate_id TEXT NOT NULL, survivor_id TEXT NOT NULL, build_id TEXT NOT NULL, removal_epoch BIGINT NOT NULL,
 material_digest TEXT NOT NULL, delivery_id TEXT NOT NULL REFERENCES dispatch_durable_outbox(id) DEFERRABLE INITIALLY DEFERRED,
 data JSON NOT NULL CHECK(octet_length(data::text)<=524288),
 PRIMARY KEY(namespace,request_id), UNIQUE(namespace,guard_id,request_id,material_digest),
 FOREIGN KEY(namespace,guard_id) REFERENCES dispatch_query_removal_guards(namespace,guard_id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(data->>'GuardID'=guard_id AND data->'Request'->>'RequestID'=request_id AND data->'Candidate'->>'Namespace'=namespace AND data->'Candidate'->>'RuntimeID'=candidate_id AND data->'Candidate'->>'BuildID'=build_id AND COALESCE(data->'Survivor'->>'RuntimeID','')=survivor_id AND (data->>'RemovalEpoch')::BIGINT=removal_epoch AND data->>'MaterialDigest'=material_digest AND data->>'DeliveryID'=delivery_id)
);
CREATE INDEX dispatch_query_guard_candidate ON dispatch_query_removal_receipts(namespace,candidate_id,guard_id);
CREATE INDEX dispatch_query_guard_survivor ON dispatch_query_removal_receipts(namespace,survivor_id,guard_id);
CREATE INDEX dispatch_query_guard_build ON dispatch_query_removal_receipts(namespace,build_id,guard_id);
CREATE TABLE dispatch_query_removal_current (
 namespace TEXT NOT NULL, guard_id TEXT NOT NULL, request_id TEXT NOT NULL, material_digest TEXT NOT NULL,
 data JSON NOT NULL CHECK(octet_length(data::text)<=1024), PRIMARY KEY(namespace,guard_id),
 FOREIGN KEY(namespace,guard_id,request_id,material_digest) REFERENCES dispatch_query_removal_receipts(namespace,guard_id,request_id,material_digest) DEFERRABLE INITIALLY DEFERRED,
 CHECK(data->>'GuardID'=guard_id AND data->>'RequestID'=request_id AND data->>'MaterialDigest'=material_digest)
);
CREATE TABLE dispatch_query_removal_commits (
 namespace TEXT NOT NULL, request_id TEXT NOT NULL, guard_id TEXT NOT NULL,
 delivery_id TEXT NOT NULL REFERENCES dispatch_durable_outbox(id) DEFERRABLE INITIALLY DEFERRED,
 data JSON NOT NULL CHECK(octet_length(data::text)<=%d), PRIMARY KEY(namespace,request_id),
 FOREIGN KEY(namespace,guard_id) REFERENCES dispatch_query_removal_guards(namespace,guard_id),
 CHECK(data->'Request'->>'RequestID'=request_id AND data->'Request'->'Guard'->>'GuardID'=guard_id AND data->'Request'->'Guard'->>'Namespace'=namespace AND data->>'DeliveryID'=delivery_id)
);
CREATE TABLE dispatch_query_removal_closes (
 namespace TEXT NOT NULL, request_id TEXT NOT NULL, guard_id TEXT NOT NULL,
 delivery_id TEXT NOT NULL REFERENCES dispatch_durable_outbox(id) DEFERRABLE INITIALLY DEFERRED,
 data JSON NOT NULL CHECK(octet_length(data::text)<=16384), PRIMARY KEY(namespace,request_id), UNIQUE(namespace,guard_id),
 FOREIGN KEY(namespace,guard_id) REFERENCES dispatch_query_removal_guards(namespace,guard_id),
 CHECK(data->'Request'->>'RequestID'=request_id AND data->'Request'->'Guard'->>'GuardID'=guard_id AND data->'Request'->'Guard'->>'Namespace'=namespace AND data->>'DeliveryID'=delivery_id)
);
CREATE FUNCTION dispatch_query_guard_write() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE g dispatch_query_removal_guards; olddata JSONB; newdata JSONB;
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='query guard authority is retained'; END IF;
 IF NOT dispatch_query_coordinator_owned(NEW.namespace) THEN RAISE EXCEPTION USING ERRCODE='DL002',MESSAGE='query guard requires existing namespace coordinator'; END IF;
 IF TG_TABLE_NAME='dispatch_query_removal_guards' THEN
  IF TG_OP='INSERT' AND NEW.state<>'active' THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='query guard must start active'; END IF;
  IF TG_OP='UPDATE' THEN
   olddata:=OLD.data::jsonb-ARRAY['State','CloseRequestID','CloseDigest'];newdata:=NEW.data::jsonb-ARRAY['State','CloseRequestID','CloseDigest'];
   IF OLD.namespace<>NEW.namespace OR OLD.guard_id<>NEW.guard_id OR OLD.state<>'active' OR NEW.state<>'closed' OR olddata<>newdata OR NOT EXISTS(SELECT 1 FROM dispatch_query_removal_closes c WHERE c.namespace=NEW.namespace AND c.guard_id=NEW.guard_id AND c.request_id=NEW.data->>'CloseRequestID' AND c.data->>'ReceiptDigest'=NEW.data->>'CloseDigest') THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='query guard terminal transition refused'; END IF;
  END IF;
 ELSE
  SELECT * INTO g FROM dispatch_query_removal_guards WHERE namespace=NEW.namespace AND guard_id=NEW.guard_id;
  IF NOT FOUND OR g.state<>'active' OR NEW.data->>'RootDigest' IS NOT NULL AND NEW.data->>'RootDigest'<>g.data->>'RootDigest' THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='query guard inactive or inconsistent'; END IF;
  IF TG_OP='UPDATE' THEN
   IF TG_TABLE_NAME<>'dispatch_query_removal_current' OR OLD.namespace<>NEW.namespace OR OLD.guard_id<>NEW.guard_id OR NOT ((NEW.data->>'ObservedAt')::TIMESTAMPTZ>(OLD.data->>'ObservedAt')::TIMESTAMPTZ) THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='query guard record immutable or current rollback'; END IF;
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER dispatch_query_guard_write BEFORE INSERT OR UPDATE OR DELETE ON dispatch_query_removal_guards FOR EACH ROW EXECUTE FUNCTION dispatch_query_guard_write();
CREATE TRIGGER dispatch_query_guard_write BEFORE INSERT OR UPDATE OR DELETE ON dispatch_query_removal_current FOR EACH ROW EXECUTE FUNCTION dispatch_query_guard_write();
CREATE TRIGGER dispatch_query_guard_write BEFORE INSERT OR UPDATE OR DELETE ON dispatch_query_removal_receipts FOR EACH ROW EXECUTE FUNCTION dispatch_query_guard_write();
CREATE TRIGGER dispatch_query_guard_write BEFORE INSERT OR UPDATE OR DELETE ON dispatch_query_removal_commits FOR EACH ROW EXECUTE FUNCTION dispatch_query_guard_write();
CREATE TRIGGER dispatch_query_guard_write BEFORE INSERT OR UPDATE OR DELETE ON dispatch_query_removal_closes FOR EACH ROW EXECUTE FUNCTION dispatch_query_guard_write();
CREATE FUNCTION dispatch_query_guard_links() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE g dispatch_query_removal_guards;
BEGIN
 SELECT * INTO g FROM dispatch_query_removal_guards WHERE namespace=NEW.namespace AND guard_id=NEW.guard_id;
 IF NOT EXISTS(SELECT 1 FROM dispatch_query_removal_receipts r WHERE r.namespace=g.namespace AND r.guard_id=g.guard_id AND r.request_id=g.data->>'InitialRequestID' AND r.data->>'RootDigest'=g.data->>'RootDigest') OR NOT EXISTS(SELECT 1 FROM dispatch_query_removal_current c WHERE c.namespace=g.namespace AND c.guard_id=g.guard_id AND c.data->>'RootDigest'=g.data->>'RootDigest') THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='query guard material association missing'; END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER dispatch_query_guard_links AFTER INSERT OR UPDATE ON dispatch_query_removal_guards DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_query_guard_links();
CREATE FUNCTION dispatch_query_guard_intent() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE kind TEXT; source TEXT;
BEGIN
 kind:=CASE TG_TABLE_NAME WHEN 'dispatch_query_removal_receipts' THEN 'revalidate' WHEN 'dispatch_query_removal_commits' THEN 'commit' ELSE 'guard.close' END;
 source:=encode(sha256(convert_to(octet_length('dispatch.query-removal.v1')::TEXT||':'||'dispatch.query-removal.v1'||octet_length(NEW.namespace)::TEXT||':'||NEW.namespace||octet_length(kind)::TEXT||':'||kind||octet_length(NEW.request_id)::TEXT||':'||NEW.request_id,'UTF8')),'hex');
 IF NOT EXISTS(SELECT 1 FROM dispatch_durable_namespaces n JOIN dispatch_durable_outbox o ON o.namespace=n.namespace AND o.installation_id=n.installation_id AND o.app_id=n.app_id AND o.tenant_id=n.tenant_id AND o.schema_version=n.schema_version WHERE n.namespace=NEW.namespace AND n.require_audit AND o.id=NEW.delivery_id AND o.destination='chronicle' AND o.source_kind='security' AND o.source_id=source AND o.workflow_id='' AND o.run_id='' AND o.sequence=0 AND o.envelope->>'Action'='dispatch.query_runtime.'||kind AND o.envelope->>'Outcome'='accepted') THEN RAISE EXCEPTION USING ERRCODE='DA002',MESSAGE='query guard audit intent missing'; END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER dispatch_query_guard_intent AFTER INSERT ON dispatch_query_removal_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_query_guard_intent();
CREATE CONSTRAINT TRIGGER dispatch_query_guard_intent AFTER INSERT ON dispatch_query_removal_commits DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_query_guard_intent();
CREATE CONSTRAINT TRIGGER dispatch_query_guard_intent AFTER INSERT ON dispatch_query_removal_closes DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dispatch_query_guard_intent();
CREATE FUNCTION dispatch_query_pinned_binding() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE pinned_candidate BOOLEAN; pinned_survivor BOOLEAN;
BEGIN
 IF TG_OP='INSERT' THEN RETURN NEW; END IF;
 IF NOT dispatch_query_coordinator_owned(OLD.namespace) THEN RAISE EXCEPTION USING ERRCODE='DL002',MESSAGE='query pin mutation requires existing coordinator'; END IF;
 SELECT EXISTS(SELECT 1 FROM dispatch_query_removal_receipts r JOIN dispatch_query_removal_guards g USING(namespace,guard_id) WHERE r.namespace=OLD.namespace AND r.candidate_id=OLD.runtime_id AND g.state='active'),EXISTS(SELECT 1 FROM dispatch_query_removal_receipts r JOIN dispatch_query_removal_guards g USING(namespace,guard_id) WHERE r.namespace=OLD.namespace AND r.survivor_id=OLD.runtime_id AND g.state='active') INTO pinned_candidate,pinned_survivor;
 IF TG_OP='DELETE' THEN
  IF pinned_candidate OR pinned_survivor THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='protected query runtime deletion'; END IF;
  RETURN OLD;
 END IF;
 IF pinned_candidate AND NEW IS DISTINCT FROM OLD OR pinned_survivor AND (NEW.state<>'active' OR (NEW.binding-ARRAY['Version','Verification','ChangedAt']) IS DISTINCT FROM (OLD.binding-ARRAY['Version','Verification','ChangedAt'])) THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='protected query runtime mutation'; END IF;
 IF OLD.state='removing' AND NEW.state IN ('active','removed') AND EXISTS(SELECT 1 FROM dispatch_query_removal_receipts r JOIN dispatch_query_removal_guards g USING(namespace,guard_id) LEFT JOIN dispatch_query_removal_closes c ON c.namespace=g.namespace AND c.request_id=g.data->>'CloseRequestID' WHERE r.namespace=OLD.namespace AND r.candidate_id=OLD.runtime_id AND r.removal_epoch=(OLD.binding->>'RemovalEpoch')::BIGINT AND g.state='closed' AND (c.data->>'ReceiptDigest' IS DISTINCT FROM g.data->>'CloseDigest' OR c.data->'Request'->'Closure'->>'Disposition' IS DISTINCT FROM CASE NEW.state WHEN 'removed' THEN 'all_positive' ELSE 'settled_without_deletion' END)) THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='query guard closure disposition mismatch'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER dispatch_query_pinned_binding BEFORE INSERT OR UPDATE OR DELETE ON dispatch_query_runtimes FOR EACH ROW EXECUTE FUNCTION dispatch_query_pinned_binding();
CREATE FUNCTION dispatch_query_pinned_build() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF TG_OP='INSERT' THEN RETURN NEW; END IF;
 IF EXISTS(SELECT 1 FROM dispatch_query_removal_receipts r JOIN dispatch_query_removal_guards g USING(namespace,guard_id) WHERE r.namespace=OLD.namespace AND r.build_id=OLD.build_id AND g.state='active') THEN
  IF NOT dispatch_query_coordinator_owned(OLD.namespace) THEN RAISE EXCEPTION USING ERRCODE='DL002',MESSAGE='query build pin requires existing coordinator'; END IF;
  IF TG_OP='DELETE' THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='protected query build deletion'; END IF;
  IF NEW IS DISTINCT FROM OLD AND NOT (NEW.namespace=OLD.namespace AND NEW.build_id=OLD.build_id AND NEW.query_identity=OLD.query_identity AND NEW.epoch=OLD.epoch AND NEW.cutoff_epoch=OLD.cutoff_epoch AND OLD.state='retiring' AND NEW.state='retired' AND NEW.version=OLD.version+1) THEN RAISE EXCEPTION USING ERRCODE='DL004',MESSAGE='protected query build mutation'; END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
CREATE TRIGGER dispatch_query_pinned_build BEFORE INSERT OR UPDATE OR DELETE ON dispatch_build_lifecycle FOR EACH ROW EXECUTE FUNCTION dispatch_query_pinned_build();
`
