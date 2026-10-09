//go:build integration

package postgres_test

import (
	"errors"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

func TestLegacyAuditOutcomeRecovery(t *testing.T) {
	s := setupTestStore(t)
	ctx := t.Context()
	n := durable.NamespaceConfig{InstallationID: "installation", Namespace: "audit", AppID: "app", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(ctx, n); err != nil {
		t.Fatal(err)
	}
	a, err := durable.CaptureSecurityAudit(n.InstallationID, n.Namespace, "legacy.cancel", "attempted", "", durable.AuditMetadata{ActorKind: "service_acct", ActorID: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := s.BeginLegacyAttempt(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.BeginLegacyAttempt(ctx, a)
	if err != nil || again.Attempt != attempt.Attempt {
		t.Fatal(again, err)
	}
	audit, err := durable.CaptureSecurityAudit(n.InstallationID, n.Namespace, a.Action, "returned_success", "", a.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	outcome := durable.LegacyOutcome{AttemptID: attempt.Attempt.ID, Audit: audit}
	pg := pgdriver.Unwrap(s.DB())
	_, err = pg.Exec(ctx, `CREATE FUNCTION fail_legacy_outcome() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.envelope->>'Outcome'='returned_success' THEN RAISE EXCEPTION 'injected outcome failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_legacy_outcome BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION fail_legacy_outcome()`)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteLegacyAttempt(ctx, outcome); err == nil {
		t.Fatal("outcome failure ignored")
	}
	restarted := postgres.New(s.DB())
	request := durable.LegacyAttemptList{InstallationID: n.InstallationID, Namespace: n.Namespace, Limit: 100}
	pending, err := restarted.UnresolvedLegacyAttempts(ctx, request)
	if err != nil || len(pending) != 1 || pending[0].Attempt.ID != attempt.Attempt.ID {
		t.Fatal(pending, err)
	}
	_, err = pg.Exec(ctx, `DROP TRIGGER fail_legacy_outcome ON dispatch_durable_outbox`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = restarted.CompleteLegacyAttempt(ctx, outcome); err != nil {
			t.Fatal(err)
		}
	}
	pending, err = restarted.UnresolvedLegacyAttempts(ctx, request)
	if err != nil || len(pending) != 0 {
		t.Fatal(pending, err)
	}
	conflict := outcome
	conflict.Audit.Outcome = "returned_error"
	if err = restarted.CompleteLegacyAttempt(ctx, conflict); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatal(err)
	}
	foreign := outcome
	foreign.Audit.InstallationID = "foreign"
	if err = restarted.CompleteLegacyAttempt(ctx, foreign); err == nil {
		t.Fatal("foreign outcome accepted")
	}
	secondAudit, err := durable.CaptureSecurityAudit(n.InstallationID, n.Namespace, a.Action, "attempted", "", a.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.BeginLegacyAttempt(ctx, secondAudit)
	if err != nil {
		t.Fatal(err)
	}
	reused := outcome
	reused.AttemptID = second.Attempt.ID
	if err = s.CompleteLegacyAttempt(ctx, reused); err == nil {
		t.Fatal("outcome reused across attempts")
	}
	status, err := s.DeliveryStatus(ctx, durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: n.InstallationID, Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil || len(status.Records) != 3 {
		t.Fatal(status, err)
	}
}
