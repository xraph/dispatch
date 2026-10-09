package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

func TestResponseMarshalFailureDoesNotClaimRollback(t *testing.T) {
	s := memory.New()
	n := durable.NamespaceConfig{InstallationID: "installation", Namespace: "audit", AppID: "app", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	b := security.Boundary{Resource: security.Resource{InstallationID: n.InstallationID, PolicyTenant: n.TenantID}, Audit: &security.AuditService{}, Authorizer: security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error { return nil })}
	if err := b.Audit.Activate(t.Context(), s, s, b.Resource, n.Namespace, false); err != nil {
		t.Fatal(err)
	}
	a := New(nil, nil, WithSecurity(security.AuthenticatorFunc(func(context.Context, *http.Request) (security.Principal, error) {
		return security.Principal{Subject: "operator", Kind: "user"}, nil
	}), b))
	router := forge.NewRouter()
	key := durable.Key{Namespace: n.Namespace, WorkflowID: "persisted", RunID: "run"}
	err := router.POST("/jobs/:jobId/cancel", func(ctx forge.Context) error {
		if _, startErr := s.StartExecution(ctx.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "test", Queue: "queue", BuildID: "build"}); startErr != nil {
			return startErr
		}
		return ctx.JSON(200, map[string]any{"unsupported": make(chan int)})
	}, forge.WithMiddleware(a.guard(http.MethodPost, "/jobs/:jobId/cancel")))
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	router.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/jobs/"+id.NewJobID().String()+"/cancel", nil))
	if rec.Code < 400 {
		t.Fatal("serialization error returned success", rec.Code, rec.Body)
	}
	if _, err = s.GetExecution(t.Context(), key); err != nil {
		t.Fatal("mutation lost", err)
	}
	status, err := s.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: n.InstallationID, Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range status.Records {
		if r.Delivery.Outcome == "returned_error" {
			found = true
		}
	}
	if !found {
		t.Fatal("missing response error audit", status)
	}
}
