package security_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type failingAudit struct {
	*memory.Store
	fail bool
}

func (s *failingAudit) AppendSecurityAudit(ctx context.Context, a durable.SecurityAudit) (durable.Delivery, error) {
	if s.fail {
		return durable.Delivery{}, errors.New("private provider detail")
	}
	return s.Store.AppendSecurityAudit(ctx, a)
}
func (s *failingAudit) CompleteLegacyAttempt(ctx context.Context, o durable.LegacyOutcome) error {
	if s.fail {
		return errors.New("outcome unavailable")
	}
	return s.Store.CompleteLegacyAttempt(ctx, o)
}
func auditBoundary(t *testing.T) (security.Boundary, *failingAudit) {
	t.Helper()
	s := &failingAudit{Store: memory.New()}
	r := security.Resource{InstallationID: "installation", PolicyTenant: "tenant"}
	_, err := s.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: r.InstallationID, Namespace: "audit", AppID: "app", TenantID: r.PolicyTenant, RequireAudit: true, SchemaVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	b := security.Boundary{Resource: r, Audit: &security.AuditService{}, Authorizer: security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error { return nil })}
	if err = b.Audit.Activate(t.Context(), s, s, r, "audit", false); err != nil {
		t.Fatal(err)
	}
	return b, s
}
func TestAuditRequiredAndDeniedFailure(t *testing.T) {
	b, s := auditBoundary(t)
	p := security.Principal{Subject: "machine", Kind: "api_key"}
	op := security.Operation{Action: security.OperatorRead}
	copyWithoutAudit := b
	copyWithoutAudit.Audit = nil
	if err := copyWithoutAudit.Check(t.Context(), p, op); !errors.Is(err, security.ErrUnavailable) {
		t.Fatal(err)
	}
	if err := b.Check(t.Context(), p, op); err != nil {
		t.Fatal(err)
	}
	status, err := s.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "installation", Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil || len(status.Records) != 1 || status.Records[0].Delivery.Metadata.ActorKind != "api_key" {
		t.Fatal(status, err)
	}
	s.fail = true
	if err = b.Check(t.Context(), p, op); !errors.Is(err, security.ErrUnavailable) {
		t.Fatal(err)
	}
	b.Authorizer = security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error {
		return security.ErrForbidden
	})
	if err = b.Check(t.Context(), p, op); !errors.Is(err, security.ErrForbidden) {
		t.Fatal(err)
	}
	if err = b.AuthenticationDenied(t.Context(), op); !errors.Is(err, security.ErrUnauthenticated) {
		t.Fatal(err)
	}
}
func TestAuditBindingCopiedAndNamespaceCannotRedirect(t *testing.T) {
	b, s := auditBoundary(t)
	p := security.Principal{Subject: "operator", Kind: "user"}
	op := security.Operation{Action: security.OperatorRead}
	copyBefore := b
	copyBefore.Audit = &security.AuditService{}
	copyAfter := copyBefore
	if err := copyBefore.Check(t.Context(), p, op); !errors.Is(err, security.ErrUnavailable) {
		t.Fatal(err)
	}
	if err := copyAfter.Audit.Activate(t.Context(), s, s, b.Resource, "audit", false); err != nil {
		t.Fatal(err)
	}
	if err := copyBefore.Check(t.Context(), p, op); err != nil {
		t.Fatal(err)
	}
	if err := b.CheckNamespace(t.Context(), p, op, "unknown"); !errors.Is(err, security.ErrUnavailable) {
		t.Fatal(err)
	}
	b.Authorizer = security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error {
		return security.ErrForbidden
	})
	if err := b.CheckNamespace(t.Context(), p, op, "unknown"); !errors.Is(err, security.ErrForbidden) {
		t.Fatal(err)
	}
	status, _ := s.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "installation", Destination: durable.DestinationChronicle}, Limit: 100})
	for _, r := range status.Records {
		if r.Delivery.Namespace != "audit" {
			t.Fatal(r)
		}
	}
	bad := &security.AuditService{}
	if err := bad.Activate(t.Context(), s, s, security.Resource{InstallationID: "installation", PolicyTenant: "foreign"}, "audit", false); err == nil {
		t.Fatal("foreign binding")
	}
}
func TestLegacyOutcomeFailureSurvivesServiceRestart(t *testing.T) {
	b, s := auditBoundary(t)
	p := security.Principal{Subject: "machine", Kind: "service_acct"}
	op := security.Operation{Action: security.OperatorWrite}
	attempt, err := b.BeginCommand(t.Context(), p, op)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := attempt.CaptureOutcome(true)
	if err != nil {
		t.Fatal(err)
	}
	s.fail = true
	err = b.AcceptOutcome(t.Context(), outcome)
	var unconfirmed *security.OutcomeUnconfirmedError
	if !errors.As(err, &unconfirmed) || unconfirmed.AttemptID != attempt.Record.Attempt.ID {
		t.Fatal(err)
	}
	restarted := b
	restarted.Audit = &security.AuditService{}
	if err = restarted.Audit.Activate(t.Context(), s, s, b.Resource, "audit", false); err != nil {
		t.Fatal(err)
	}
	request := durable.LegacyAttemptList{InstallationID: "installation", Namespace: "audit", Limit: 100}
	pending, err := s.UnresolvedLegacyAttempts(t.Context(), request)
	if err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
	s.fail = false
	for i := 0; i < 2; i++ {
		if err = restarted.AcceptOutcome(t.Context(), outcome); err != nil {
			t.Fatal(err)
		}
	}
	pending, err = s.UnresolvedLegacyAttempts(t.Context(), request)
	if err != nil || len(pending) != 0 {
		t.Fatal(pending, err)
	}
	another, err := b.BeginCommand(t.Context(), p, op)
	if err != nil {
		t.Fatal(err)
	}
	reused := outcome
	reused.AttemptID = another.Record.Attempt.ID
	if err = s.CompleteLegacyAttempt(t.Context(), reused); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatal("outcome reused for another attempt", err)
	}
	conflict := outcome
	conflict.Audit.Outcome = "failed"
	if err = s.CompleteLegacyAttempt(t.Context(), conflict); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatal(err)
	}
	foreign := outcome
	foreign.Audit.InstallationID = "foreign"
	if err = s.CompleteLegacyAttempt(t.Context(), foreign); err == nil {
		t.Fatal("foreign outcome accepted")
	}
}
