package api_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xraph/dispatch/api"
	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type failingOutcomeStore struct{ *memory.Store }

func (s *failingOutcomeStore) CompleteLegacyAttempt(context.Context, durable.LegacyOutcome) error {
	return errors.New("private outcome error")
}
func TestRESTLegacyOutcomeFailureReturnsUnconfirmed(t *testing.T) {
	f := newFixture(t)
	j := jobInState(t, f, job.StatePending)
	s := &failingOutcomeStore{Store: memory.New()}
	n := durable.NamespaceConfig{InstallationID: "installation", Namespace: "audit", AppID: "app", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	b := security.Boundary{Resource: security.Resource{InstallationID: n.InstallationID, PolicyTenant: n.TenantID}, Audit: &security.AuditService{}, Authorizer: security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error { return nil })}
	if err := b.Audit.Activate(t.Context(), s, s, b.Resource, n.Namespace, false); err != nil {
		t.Fatal(err)
	}
	auth := security.AuthenticatorFunc(func(context.Context, *http.Request) (security.Principal, error) {
		return security.Principal{Subject: "machine", Kind: "api_key"}, nil
	})
	handler := api.New(f.eng, nil, api.WithSecurity(auth, b)).Handler()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/jobs/"+j.ID.String()+"/cancel", nil))
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "outcome unconfirmed") || strings.Contains(rec.Body.String(), "private") {
		t.Fatal(rec.Code, rec.Body)
	}
	if got := storedJob(t, f, j.ID); got.State != job.StateCancelled {
		t.Fatal("command did not run", got.State)
	}
	pending, err := s.UnresolvedLegacyAttempts(t.Context(), durable.LegacyAttemptList{InstallationID: n.InstallationID, Namespace: n.Namespace, Limit: 100})
	if err != nil || len(pending) != 1 || !strings.Contains(rec.Body.String(), pending[0].Attempt.ID) {
		t.Fatal(pending, err, rec.Body)
	}
}
func TestRESTAnonymousDenialAcceptedWithoutProviderText(t *testing.T) {
	f := newFixture(t)
	s := memory.New()
	n := durable.NamespaceConfig{InstallationID: "installation", Namespace: "audit", AppID: "app", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	b := security.Boundary{Resource: security.Resource{InstallationID: n.InstallationID, PolicyTenant: n.TenantID}, Audit: &security.AuditService{}}
	if err := b.Audit.Activate(t.Context(), s, s, b.Resource, n.Namespace, false); err != nil {
		t.Fatal(err)
	}
	auth := security.AuthenticatorFunc(func(context.Context, *http.Request) (security.Principal, error) {
		return security.Principal{}, errors.New("private credential")
	})
	rec := httptest.NewRecorder()
	api.New(f.eng, nil, api.WithSecurity(auth, b)).Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/stats", nil))
	if rec.Code != 401 {
		t.Fatal(rec.Code, rec.Body)
	}
	status, err := s.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: n.InstallationID, Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil || len(status.Records) != 1 || status.Records[0].Delivery.Metadata.ActorKind != "anonymous" || status.Records[0].Delivery.Outcome != "unauthenticated" {
		t.Fatal(status, err)
	}
}
