package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type contractQueryHost struct{}

func (contractQueryHost) ResolveBinding(_ context.Context, target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error) {
	return durable.QueryRuntimeIdentity{QueryRuntimeTarget: target, InstanceID: "trusted-instance", IdentityVersion: 1, BuildIdentity: durabletest.QueryIdentityFixture()}, nil
}
func (contractQueryHost) Verify(_ context.Context, b durable.QueryRuntimeBinding) (durable.QueryRuntimeVerification, error) {
	return durabletest.QueryProofFixture(b.QueryRuntimeIdentity, "trusted-proof"), nil
}

type contractQueryStore struct{ *memory.Store }

func (s *contractQueryStore) ListQueryRuntimes(ctx context.Context, in durable.QueryRuntimeList) (durable.QueryRuntimePage, error) {
	p, err := s.Store.ListQueryRuntimes(ctx, in)
	for i := range p.Items {
		p.Items[i].Version = 9007199254740993
		p.Items[i].RemovalEpoch = 9007199254740995
	}
	return p, err
}

func TestQueryLifecyclePublicContracts(t *testing.T) {
	store := &contractQueryStore{Store: memory.New()}
	deps := durableDeps(t, store)
	for _, ns := range []string{"allowed", "foreign"} {
		target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "test", Namespace: ns}, BuildID: "historic"}
		if _, err := store.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
			t.Fatal(err)
		}
		identity := durabletest.QueryIdentityFixture()
		if _, err := store.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "identity", ExpectedVersion: 1, Identity: &identity}); err != nil {
			t.Fatal(err)
		}
	}
	var err error
	deps.Durable, err = operator.New(operator.Options{Store: store, Reads: store, Catalog: store, InstallationID: "test", Audit: deps.Security, QueryHost: contractQueryHost{}, CursorKeys: operator.CursorKeys{Active: "v1", Keys: map[string][]byte{"v1": make([]byte, 32)}}, Authorizer: operator.AuthorizerFunc(func(_ context.Context, p security.Principal, _ string, r operator.Resource) error {
		if p.Subject == "operator" && r.Namespace == "allowed" && r.AppID == "trusted-app" && r.TenantID == "tenant-allowed" {
			return nil
		}
		return security.ErrForbidden
	})})
	if err != nil {
		t.Fatal(err)
	}
	registry, wardens, d := fc.NewRegistry(), fc.NewWardenRegistry(), dispatcher.New(nil)
	if err = Register(d, registry, wardens, deps); err != nil {
		t.Fatal(err)
	}
	handler := transport.NewHandler(registry, wardens, d, nil)
	call := func(intent string, payload map[string]any) *httptest.ResponseRecorder {
		body, marshalErr := json.Marshal(map[string]any{"envelope": "v1", "kind": string(queryLifecycleDurableKind(intent)), "contributor": "dispatch", "intent": intent, "intentVersion": 1, "csrf": "test", "idempotencyKey": intent, "params": map[string]string{"namespace": "allowed"}, "payload": payload})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequestWithContext(testContext(), http.MethodPost, "/api/dashboard/v1", bytes.NewReader(body)))
		return response
	}
	for _, name := range []string{"durable.queryRuntime", "durable.queryRuntimeRegister", "durable.queryRuntimeVerify", "durable.queryRuntimeRemove", "durable.queryRuntimeRemovalCheck", "durable.queryRuntimeFinish", "durable.queryRuntimeAbort"} {
		t.Run(name, func(t *testing.T) {
			intent, ok := registry.Intent(ContributorName, name, 1)
			if !ok || intent.Schema.Input["type"] != "object" || intent.Schema.Output == nil {
				t.Fatal("registered query lifecycle schema missing")
			}
			input := map[string]any{"namespace": "foreign", "build_id": "historic", "runtime_id": "runtime", "request_id": "request", "expected_version": "1", "reservation_request_id": "reservation", "reservation_version": "1"}
			if name == "durable.queryRuntimeRegister" {
				input["expected_version"] = "0"
			}
			encoded, e := json.Marshal(input)
			if e != nil {
				t.Fatal(e)
			}
			if _, _, e = d.Dispatch(t.Context(), fc.Request{Contributor: ContributorName, Intent: name, IntentVersion: 1, Kind: queryLifecycleDurableKind(name), Payload: encoded}, testPrincipal()); e == nil {
				t.Fatal("direct foreign query control allowed")
			}
			if response := call(name, input); response.Code != http.StatusForbidden {
				t.Fatalf("outer params bypassed payload ownership: %d %s", response.Code, response.Body)
			}
		})
	}
	input := map[string]any{"namespace": "allowed", "build_id": "historic", "runtime_id": "runtime", "request_id": "register", "expected_version": "0", "instance_id": "forged", "verifier_url": "https://untrusted.invalid", "evidence_digest": "forged"}
	response := call("durable.queryRuntimeRegister", input)
	var envelope fc.Response
	if err = json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || !envelope.OK || len(envelope.Meta.Invalidates) != 4 {
		t.Fatalf("registration %s %v", response.Body, err)
	}
	var accepted operator.QueryRuntimeAcceptance
	if err = json.Unmarshal(envelope.Data, &accepted); err != nil || accepted.Binding.InstanceID != "trusted-instance" || accepted.Binding.Version != "1" {
		t.Fatalf("caller replaced trusted identity %+v %v", accepted, err)
	}
	response = call("durable.queryRuntime", map[string]any{"namespace": "allowed", "build_id": "historic", "runtime_id": "runtime"})
	if err = json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || !envelope.OK {
		t.Fatalf("query read %s %v", response.Body, err)
	}
	var wire map[string]any
	if err = json.Unmarshal(envelope.Data, &wire); err != nil || wire["version"] != "9007199254740993" || wire["removal_epoch"] != "9007199254740995" {
		t.Fatalf("query wire precision %+v %v", wire, err)
	}
	input["expected_version"] = int64(9007199254740993)
	if response = call("durable.queryRuntimeVerify", input); response.Code != http.StatusBadRequest {
		t.Fatalf("numeric version accepted %s", response.Body)
	}
}
