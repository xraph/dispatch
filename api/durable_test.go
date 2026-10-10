package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type callbackProvider struct {
	kind    *string
	failure *error
}

func (callbackProvider) Name() string                       { return "session" }
func (callbackProvider) Type() auth.SecuritySchemeType      { return auth.SecurityTypeHTTP }
func (callbackProvider) OpenAPIScheme() auth.SecurityScheme { return auth.SecurityScheme{} }
func (callbackProvider) Middleware() forge.Middleware       { return nil }
func (p callbackProvider) Authenticate(_ context.Context, r *http.Request) (*auth.AuthContext, error) {
	if p.failure != nil && *p.failure != nil {
		return nil, *p.failure
	}
	if r.Header.Get("Authorization") != "Bearer fixture" {
		return nil, security.ErrUnauthenticated
	}
	return &auth.AuthContext{Subject: "callback", Claims: map[string]any{"principal_kind": *p.kind}, Metadata: map[string]any{"credential_scheme": "bearer"}}, nil
}
func TestDurableCallbackTypedHTTPGate(t *testing.T) {
	store := memory.New()
	_, err := store.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "host", Namespace: "namespace", AppID: "app", TenantID: "tenant", RequireAudit: true, RequireHooks: true, SchemaVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	boundary := security.Boundary{Resource: security.Resource{InstallationID: "host", PolicyTenant: "tenant"}, Audit: &security.AuditService{}}
	if err = boundary.Audit.Activate(t.Context(), store, store, boundary.Resource, "namespace", false); err != nil {
		t.Fatal(err)
	}
	var handle drt.AsyncActivityHandle
	worker, err := drt.NewWorker(store, drt.Options{Namespace: "namespace", BuildID: "build", Queue: "queue", Owner: "worker", Workflows: map[string]drt.WorkflowFunc{"workflow": func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ActivityWithOptions("task", "task", "", nil, drt.ActivityOptions{StartToCloseTimeout: time.Minute}).Get()
	}}, Activities: map[string]drt.ActivityFunc{"task": func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		var e error
		handle, e = info.DeferCompletion(ctx)
		return nil, e
	}}})
	if err != nil {
		t.Fatal(err)
	}
	key := durable.Key{Namespace: "namespace", WorkflowID: "workflow", RunID: "run"}
	if _, err = worker.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "workflow", BuildID: "build", Queue: "queue"}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity} {
		if worked, e := worker.RunOnce(t.Context(), kind); e != nil || !worked {
			t.Fatalf("worker: %v %v", worked, e)
		}
	}
	allowed := true
	var policyError, providerError error
	runtimeMode := "exact"
	wrongWorker, err := drt.NewWorker(store, drt.Options{Namespace: "namespace", BuildID: "wrong-build", Queue: "queue", Owner: "wrong-worker"})
	if err != nil {
		t.Fatal(err)
	}
	service, err := operator.New(operator.Options{Store: store, Reads: store, Catalog: store, InstallationID: "host", Audit: boundary, CursorKeys: operator.CursorKeys{Active: "v1", Keys: map[string][]byte{"v1": make([]byte, 32)}}, Authorizer: operator.AuthorizerFunc(func(context.Context, security.Principal, string, operator.Resource) error {
		if policyError != nil {
			return policyError
		}
		if !allowed {
			return security.ErrForbidden
		}
		return nil
	}), Runtime: func(string, string) (*drt.Worker, error) {
		switch runtimeMode {
		case "missing":
			return nil, errors.New("private runtime internals")
		case "wrong":
			return wrongWorker, nil
		}
		return worker, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	kind := "service_acct"
	registry := auth.NewRegistry(nil, forge.NewNoopLogger())
	if err = registry.Register(callbackProvider{kind: &kind, failure: &providerError}); err != nil {
		t.Fatal(err)
	}
	authenticator := security.NewForgeAuthenticator(func() (auth.Registry, error) { return registry, nil })
	handler := New(nil, nil, WithSecurity(nil, boundary), WithDurableCallbacks(service, authenticator)).Handler()
	input := operator.CompletionInput{Handle: operator.CallbackHandle{Version: handle.Version, Key: handle.Key, BuildID: handle.BuildID, Secret: handle.Secret, InitialHeartbeatSequence: handle.InitialHeartbeatSequence, Token: operator.CallbackToken{TaskID: handle.Token.TaskID, Owner: handle.Token.Owner, Epoch: handle.Token.Epoch, LeaseKind: handle.Token.LeaseKind}}, RequestID: "complete", Output: []byte("result")}
	sendRaw := func(raw []byte, want int) string {
		t.Helper()
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/durable/activities/complete", bytes.NewReader(raw))
		request.Header.Set("Authorization", "Bearer fixture")
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("callback HTTP %d: %s", response.Code, response.Body)
		}
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("cacheable callback")
		}
		if bytes.Contains(response.Body.Bytes(), []byte("private")) {
			t.Fatal("internal error disclosed")
		}
		if bytes.Contains(response.Body.Bytes(), []byte(handle.Secret)) {
			t.Fatal("callback secret disclosed")
		}
		return response.Body.String()
	}
	send := func(want int) string {
		t.Helper()
		raw, e := json.Marshal(input)
		if e != nil {
			t.Fatal(e)
		}
		return sendRaw(raw, want)
	}
	kind = "user"
	send(403)
	kind = "service_acct"
	original := input.Handle.Secret
	input.Handle.Secret = "invalid"
	send(400)
	input.Handle.Secret = strings.Repeat("a", 64)
	send(409)
	input.Handle.Secret = original
	input.Handle.Token.Epoch++
	send(409)
	input.Handle.Token.Epoch--
	for _, mode := range []string{"missing", "wrong"} {
		runtimeMode = mode
		send(503)
	}
	runtimeMode = "exact"
	policyError = errors.New("private policy internals")
	send(503)
	policyError = nil
	providerError = errors.New("private authentication internals")
	send(401)
	providerError = nil
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	sendRaw(bytes.Replace(raw, []byte(`"epoch":"1"`), []byte(`"epoch":9007199254740993`), 1), 400)
	sendRaw([]byte(`{"handle":{"secret":"private malformed proof"},"request_id":`), 400)
	first := send(200)
	if again := send(200); again != first {
		t.Fatalf("recovery changed: %s %s", first, again)
	}
	allowed = false
	send(403)
	allowed = true
	input.Output = []byte("changed")
	send(409)
}
