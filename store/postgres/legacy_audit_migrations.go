package postgres

import (
	"context"
	"errors"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.MustRegister(&migrate.Migration{Name: "legacy_audit_attempts", Version: "20261027120001", Up: func(ctx context.Context, exec migrate.Executor) error {
		_, err := exec.Exec(ctx, `CREATE TABLE dispatch_legacy_audit_attempts (
 attempt_id TEXT PRIMARY KEY REFERENCES dispatch_durable_outbox(id),
 installation_id TEXT NOT NULL, namespace TEXT NOT NULL REFERENCES dispatch_durable_namespaces(namespace),
 outcome_id TEXT UNIQUE REFERENCES dispatch_durable_outbox(id)
 );
 CREATE INDEX dispatch_legacy_audit_unresolved ON dispatch_legacy_audit_attempts(installation_id,namespace,attempt_id) WHERE outcome_id IS NULL;
 CREATE FUNCTION dispatch_legacy_audit_guard() RETURNS trigger LANGUAGE plpgsql AS $$
 DECLARE a dispatch_durable_outbox; o dispatch_durable_outbox;
 BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'legacy audit evidence is retained'; END IF;
 IF TG_OP='UPDATE' AND (NEW.attempt_id<>OLD.attempt_id OR NEW.installation_id<>OLD.installation_id OR NEW.namespace<>OLD.namespace OR (OLD.outcome_id IS NOT NULL AND NEW.outcome_id IS DISTINCT FROM OLD.outcome_id)) THEN RAISE EXCEPTION 'legacy audit identity is immutable'; END IF;
 SELECT * INTO STRICT a FROM dispatch_durable_outbox WHERE id=NEW.attempt_id;
 IF a.installation_id<>NEW.installation_id OR a.namespace<>NEW.namespace OR a.source_kind<>'security' OR a.destination<>'chronicle' OR a.envelope->>'Outcome'<>'attempted' THEN RAISE EXCEPTION 'invalid legacy audit attempt'; END IF;
 IF NEW.outcome_id IS NOT NULL THEN
 SELECT * INTO STRICT o FROM dispatch_durable_outbox WHERE id=NEW.outcome_id;
 IF o.installation_id<>a.installation_id OR o.namespace<>a.namespace OR o.source_kind<>'security' OR o.destination<>'chronicle' OR o.envelope->>'Action'<>a.envelope->>'Action' OR o.envelope->>'Target'<>a.envelope->>'Target' OR o.envelope->'Metadata'<>a.envelope->'Metadata' OR o.envelope->>'Outcome' NOT IN ('returned_success','returned_error') THEN RAISE EXCEPTION 'invalid legacy audit outcome'; END IF;
 END IF; RETURN NEW;
 END $$;
 CREATE TRIGGER dispatch_legacy_audit_guard BEFORE INSERT OR UPDATE OR DELETE ON dispatch_legacy_audit_attempts FOR EACH ROW EXECUTE FUNCTION dispatch_legacy_audit_guard();`)
		return err
	}, Down: func(context.Context, migrate.Executor) error {
		return errors.New("legacy audit evidence prohibits downgrade")
	}})
}
