package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/security"
	ds "github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
)

type durableContractStore interface {
	ds.Store
	durable.Store
	durable.ReadStore
	durable.NamespaceStore
	durable.OutboxStore
}

func durableDeps(t *testing.T, store durableContractStore) Deps {
	return durableDepsRuntime(t, store, nil)
}
func durableDepsRuntime(t *testing.T, store durableContractStore, resolve func(string, string) (*drt.Worker, error)) Deps {
	t.Helper()
	d := contractDeps(t, store)
	d.Security.Authorizer = security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error {
		return security.ErrForbidden
	})
	for _, ns := range []string{"allowed", "foreign"} {
		if _, err := store.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "test", Namespace: ns, AppID: "trusted-app", TenantID: "tenant-" + ns, RequireAudit: true, RequireHooks: true, SchemaVersion: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.StartExecution(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: ns, WorkflowID: "workflow", RunID: "run"}, RequestID: "start", WorkflowType: "workflow", BuildID: "historic", Queue: "queue", Input: []byte("SECRET_PAYLOAD")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "test", Namespace: "contract-audit", AppID: "trusted-app", TenantID: "test", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	d.Security.Audit = &security.AuditService{}
	if err := d.Security.Audit.Activate(t.Context(), store, store, d.Security.Resource, "contract-audit", false); err != nil {
		t.Fatal(err)
	}
	var err error
	d.Durable, err = operator.New(operator.Options{Runtime: resolve, Store: store, Reads: store, Catalog: store, InstallationID: "test", Audit: d.Security, CursorKeys: operator.CursorKeys{Active: "v1", Keys: map[string][]byte{"v1": make([]byte, 32)}}, Authorizer: operator.AuthorizerFunc(func(_ context.Context, p security.Principal, a string, r operator.Resource) error {
		if r.Namespace == "contract-audit" {
			return security.ErrForbidden
		}
		if r.AppID != "trusted-app" || r.TenantID != "tenant-"+r.Namespace {
			t.Fatalf("untrusted ownership %+v", r)
		}
		if r.Namespace == "allowed" && p.Subject == "payload-reader" && a == operator.ReadPayload {
			return nil
		}
		if r.Namespace == "allowed" && p.Subject == "operator" && a != operator.ReadPayload {
			return nil
		}
		return security.ErrForbidden
	})})
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func TestDurableNamespaceHTTPAndDirectDispatch(t *testing.T) {
	deps := durableDeps(t, memory.New())
	runDurableContract(t, deps)
}
func runDurableContract(t *testing.T, deps Deps) {
	if _, e := deps.Durable.Namespaces(t.Context(), security.Principal{Subject: "operator", Kind: "user"}, operator.NamespaceInput{}); e != nil {
		t.Fatalf("namespace service: %v", e)
	}
	reg, wreg := fc.NewRegistry(), fc.NewWardenRegistry()
	d := dispatcher.New(nil)
	if err := Register(d, reg, wreg, deps); err != nil {
		t.Fatal(err)
	}
	handler := transport.NewHandler(reg, wreg, d, nil)
	for _, intent := range []string{"namespaces", "executions", "execution", "history", "tasks", "chain", "children", "audit", "hooks"} {
		t.Run(intent, func(t *testing.T) {
			input := map[string]any{"namespace": "allowed", "workflow_id": "workflow", "run_id": "run", "app_id": "forged", "tenant_id": "forged"}
			raw, _ := json.Marshal(input)
			// Direct dispatch skips Warden admission, so this exercises the service gate.
			req := fc.Request{Contributor: ContributorName, Intent: "durable." + intent, IntentVersion: 1, Kind: fc.KindQuery, Payload: raw}
			if _, _, err := d.Dispatch(t.Context(), req, testPrincipal()); err != nil {
				t.Fatal(err)
			}
			request, _ := json.Marshal(map[string]any{"envelope": "v1", "kind": "query", "contributor": "dispatch", "intent": "durable." + intent, "params": map[string]any{"namespace": "foreign"}, "payload": input})
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, httptest.NewRequestWithContext(testContext(), http.MethodPost, "/api/dashboard/v1", bytes.NewReader(request)))
			var out fc.Response
			if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil || !out.OK {
				t.Fatalf("%s %v", rr.Body, err)
			}
			if strings.Contains(rr.Body.String(), "SECRET_PAYLOAD") || strings.Contains(rr.Body.String(), "forged") {
				t.Fatal(rr.Body.String())
			}
			if rr.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("cacheable durable read")
			}
		})
	}
	for _, intent := range []string{"execution", "history", "tasks", "chain", "children", "audit", "hooks", "payload"} {
		for _, scope := range []string{"foreign", ""} {
			raw := []byte(`{"namespace":"` + scope + `","workflow_id":"workflow","run_id":"run","app_id":"trusted-app","tenant_id":"trusted-tenant"}`)
			req := fc.Request{Contributor: ContributorName, Intent: "durable." + intent, IntentVersion: 1, Kind: fc.KindQuery, Payload: raw, Params: map[string]any{"namespace": "allowed"}}
			if _, _, err := d.Dispatch(t.Context(), req, testPrincipal()); !errors.Is(err, fc.ErrPermissionDenied) {
				t.Fatalf("%s scope=%s: %v", intent, scope, err)
			}
			request, _ := json.Marshal(map[string]any{"envelope": "v1", "kind": "query", "contributor": "dispatch", "intent": req.Intent, "params": req.Params, "payload": json.RawMessage(raw)})
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, httptest.NewRequestWithContext(testContext(), http.MethodPost, "/api/dashboard/v1", bytes.NewReader(request)))
			var out fc.Response
			if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil || out.OK {
				t.Fatalf("denied target %s %v", rr.Body, err)
			}
		}
	}
	raw := []byte(`{"namespace":"allowed","workflow_id":"workflow","run_id":"run"}`)
	for _, intent := range []string{"durable.payload", "jobs.counts"} {
		if _, _, err := d.Dispatch(t.Context(), fc.Request{Contributor: ContributorName, Intent: intent, IntentVersion: 1, Kind: fc.KindQuery, Payload: raw}, testPrincipal()); !errors.Is(err, fc.ErrPermissionDenied) {
			t.Fatal(intent, err)
		}
	}
	w := durableWarden{deps: deps}
	for _, a := range []fc.Action{{Contributor: "foreign", Intent: "durable.execution", Kind: fc.KindQuery}, {Contributor: ContributorName, Intent: "durable.execution", Kind: fc.KindCommand}, {Contributor: ContributorName, Intent: "unknown", Kind: fc.KindQuery}} {
		if decision, err := w.Authorize(t.Context(), testPrincipal(), a); err == nil || decision.Allow {
			t.Fatal(a)
		}
	}
}

func TestDurablePayloadHTTPAuditAndAnonymous(t *testing.T) {
	deps := durableDeps(t, memory.New())
	reg, wreg := fc.NewRegistry(), fc.NewWardenRegistry()
	d := dispatcher.New(nil)
	if err := Register(d, reg, wreg, deps); err != nil {
		t.Fatal(err)
	}
	handler := transport.NewHandler(reg, wreg, d, nil)
	request := []byte(`{"envelope":"v1","kind":"query","contributor":"dispatch","intent":"durable.payload","payload":{"namespace":"allowed","workflow_id":"workflow","run_id":"run"}}`)
	for _, principal := range []string{"", "operator", "payload-reader"} {
		ctx := context.Background()
		if principal != "" {
			ctx = dashauth.WithUser(ctx, &dashauth.UserInfo{Subject: principal})
		}
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/dashboard/v1", bytes.NewReader(request)))
		var out fc.Response
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if out.OK != (principal == "payload-reader") {
			t.Fatal(principal, rr.Body.String())
		}
	}
}
