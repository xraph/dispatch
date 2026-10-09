package dwp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type nilAuthenticator struct{}

func (nilAuthenticator) Authenticate(context.Context, string) (*Identity, error) { return nil, nil }
func TestDWPDefaultNilIdentityDirectAndPolicyDenial(t *testing.T) {
	eng, _ := setupTestEngine(t)
	handler := NewHandler(eng, eng.StreamBroker(), testLogger())
	server := NewServer(eng.StreamBroker(), handler)
	if _, err := server.authenticate(context.Background(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/dwp", nil), "token"); err == nil {
		t.Fatal("default auth allowed")
	}
	server.auth = nilAuthenticator{}
	if _, err := server.authenticate(context.Background(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/dwp", nil), "token"); err == nil {
		t.Fatal("nil success allowed")
	}
	frame := &Frame{ID: "1", Method: MethodJobEnqueue, Data: mustJSON(JobEnqueueRequest{Name: "test", Payload: []byte(`{}`)})}
	conn := NewConnection("direct", &Identity{Subject: "operator", Scopes: []string{"*"}}, &JSONCodec{})
	if response := handler.Handle(context.Background(), frame, conn); response.Error == nil || response.Error.Code != 503 {
		t.Fatal(response)
	}
	handler.security = testBoundary()
	handler.security.Authorizer = security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error {
		return security.ErrForbidden
	})
	if response := handler.Handle(context.Background(), frame, conn); response.Error == nil || response.Error.Code != 403 {
		t.Fatal(response)
	}
	if response := handler.Handle(context.Background(), frame, nil); response.Error == nil || response.Error.Code != 401 {
		t.Fatal(response)
	}
}
func TestDWPActualHTTPRPCAndSSEFailClosed(t *testing.T) {
	eng, s := setupTestEngine(t)
	for _, policyErr := range []error{nil, security.ErrForbidden, errors.New("private policy detail")} {
		j, err := eng.EnqueueRaw(t.Context(), "test", []byte(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		boundary := testBoundary()
		boundary.Authorizer = security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error { return policyErr })
		h := NewHandler(eng, eng.StreamBroker(), testLogger(), boundary)
		server := NewServer(eng.StreamBroker(), h, WithAuth(NewAPIKeyAuthenticator(APIKeyEntry{Token: "explicit", Identity: Identity{Subject: "operator"}})))
		router := forge.NewRouter()
		server.RegisterRoutes(router)
		body := `{"id":"1","method":"job.cancel","token":"explicit","data":{"job_id":"` + j.ID.String() + `"}}`
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/dwp/rpc", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.Handler().ServeHTTP(rec, req)
		want := 200
		if errors.Is(policyErr, security.ErrForbidden) {
			want = 403
		}
		if policyErr != nil && !errors.Is(policyErr, security.ErrForbidden) {
			want = 503
		}
		if rec.Code != want {
			t.Fatalf("RPC %d: %s", rec.Code, rec.Body)
		}
		stored, err := s.GetJob(t.Context(), j.ID)
		if err != nil {
			t.Fatal(err)
		}
		expected := job.StatePending
		if policyErr == nil {
			expected = job.StateCancelled
		}
		if stored.State != expected {
			t.Fatal("denied RPC mutated job", stored.State)
		}
		// SSE rejects anonymous admission before subscribing.
		rec = httptest.NewRecorder()
		router.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/dwp/sse?channel=jobs", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("anonymous SSE %d", rec.Code)
		}
		if policyErr != nil {
			rec = httptest.NewRecorder()
			req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/dwp/sse?channel=jobs", nil)
			req.Header.Set("Authorization", "explicit")
			router.Handler().ServeHTTP(rec, req)
			if rec.Code != want {
				t.Fatalf("policy denied SSE %d %s", rec.Code, rec.Body)
			}
		}
	}
}
func TestForgeDWPRequestProofAndTokenOnlyDenial(t *testing.T) {
	key := dwpProofKey{}
	ctx := context.WithValue(context.Background(), key, "provenance")
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "https://host/dwp", nil).WithContext(ctx)
	request.Header.Set("DPoP", "proof")
	adapter := &ForgeAuthenticator{Auth: security.AuthenticatorFunc(func(ctx context.Context, r *http.Request) (security.Principal, error) {
		if r.Method != http.MethodGet || r.URL.String() != request.URL.String() || r.Header.Get("DPoP") != "proof" || ctx.Value(key) != "provenance" || r.Header.Get("Authorization") != "DPoP token" {
			t.Fatal("lost real request")
		}
		return security.Principal{Subject: "operator", Kind: "service"}, nil
	})}
	if _, err := adapter.Authenticate(ctx, "token"); err == nil {
		t.Fatal("token-only proof accepted")
	}
	id, err := adapter.AuthenticateRequest(ctx, request, "DPoP token")
	if err != nil || id.Kind != "service" {
		t.Fatal(id, err)
	}
}

type dwpProofKey struct{}

func TestDWPAnonymousAuditAndUnconfirmedOutcome(t *testing.T) {
	eng, jobs := setupTestEngine(t)
	audit := &failingOutcomeAudit{Store: memory.New()}
	n := durable.NamespaceConfig{InstallationID: "test", Namespace: "audit", AppID: "test", TenantID: "test", RequireAudit: true, SchemaVersion: 1}
	if _, err := audit.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	b := testBoundary()
	b.Audit = &security.AuditService{}
	if err := b.Audit.Activate(t.Context(), audit, audit, b.Resource, n.Namespace, false); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(eng, eng.StreamBroker(), testLogger(), b)
	server := NewServer(eng.StreamBroker(), h, WithAuth(nilAuthenticator{}))
	if _, err := server.authenticate(t.Context(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/dwp/rpc", nil), "secret", security.DWPOperation(MethodJobCancel)); err == nil {
		t.Fatal("anonymous accepted")
	}
	status, err := audit.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "test", Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil || len(status.Records) != 1 || status.Records[0].Delivery.Metadata.ActorKind != "anonymous" {
		t.Fatal(status, err)
	}
	j, err := eng.EnqueueRaw(t.Context(), "test", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	response := h.Handle(t.Context(), &Frame{ID: "request", Method: MethodJobCancel, Data: mustJSON(JobCancelRequest{JobID: j.ID.String()})}, NewConnection("direct", &Identity{Subject: "machine", Kind: "service_acct"}, &JSONCodec{}))
	if response.Error == nil || response.Error.Code != 503 || !strings.Contains(response.Error.Message, "outcome unconfirmed") {
		t.Fatal(response)
	}
	got, err := jobs.GetJob(t.Context(), j.ID)
	if err != nil || got.State != job.StateCancelled {
		t.Fatal(got, err)
	}
	pending, err := audit.UnresolvedLegacyAttempts(t.Context(), durable.LegacyAttemptList{InstallationID: "test", Namespace: "audit", Limit: 100})
	if err != nil || len(pending) != 1 || pending[0].Attempt.Metadata.ActorKind != "service_acct" || pending[0].Attempt.Action != "dwp:job.cancel" || pending[0].Attempt.Target != "job:"+j.ID.String() {
		t.Fatal(pending, err)
	}
}

type failingOutcomeAudit struct{ *memory.Store }

func (s *failingOutcomeAudit) CompleteLegacyAttempt(context.Context, durable.LegacyOutcome) error {
	return errors.New("outcome persistence unavailable")
}

func TestDWPHTTPAuditSelectorsAndDeniedTarget(t *testing.T) {
	eng, _ := setupTestEngine(t)
	s := memory.New()
	n := durable.NamespaceConfig{InstallationID: "test", Namespace: "audit", AppID: "test", TenantID: "test", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	b := testBoundary()
	b.Audit = &security.AuditService{}
	if err := b.Audit.Activate(t.Context(), s, s, b.Resource, n.Namespace, false); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(eng, eng.StreamBroker(), testLogger(), b)
	server := NewServer(eng.StreamBroker(), h, WithAuth(NewAPIKeyAuthenticator(APIKeyEntry{Token: "explicit", Identity: Identity{Subject: "operator"}})))
	router := forge.NewRouter()
	server.RegisterRoutes(router)
	request := func(method string, data any, token string, want int) {
		t.Helper()
		frame := &Frame{ID: "request", Method: method, Data: mustJSON(data), Token: token}
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/dwp/rpc", strings.NewReader(string(mustJSON(frame))))
		req.Header.Set("Content-Type", "application/json")
		router.Handler().ServeHTTP(rec, req)
		if rec.Code != want {
			t.Fatal(rec.Code, rec.Body)
		}
	}
	request(MethodJobEnqueue, JobEnqueueRequest{Name: "test", Queue: "mail", Payload: []byte(`{"secret":"never-audit-me"}`)}, "explicit", 200)
	j, err := eng.EnqueueRaw(t.Context(), "test", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	request(MethodJobCancel, JobCancelRequest{JobID: j.ID.String()}, "wrong-token", 401)
	status, err := s.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: n.InstallationID, Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Records) != 4 {
		t.Fatal(status)
	}
	for _, r := range status.Records {
		d := r.Delivery
		switch d.Action {
		case "dwp:job.enqueue":
			if d.Target != `{"kind":"job-selector","name":"test","queue":"mail"}` {
				t.Fatal(d)
			}
		case "dwp:job.cancel":
			if d.Target != "job:"+j.ID.String() || d.Outcome != "unauthenticated" {
				t.Fatal(d)
			}
		default:
			t.Fatal("missing method", d)
		}
		if strings.Contains(string(mustJSON(d)), "never-audit-me") {
			t.Fatal("payload in audit")
		}
	}
}
