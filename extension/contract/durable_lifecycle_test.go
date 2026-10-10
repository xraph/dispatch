package contract

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/memory"
)

func TestDurableLifecycleHTTPAuthorizesDecodedTarget(t *testing.T) {
	store := memory.New()
	deps := durableDeps(t, store)
	for _, ns := range []string{"allowed", "foreign"} {
		if _, err := store.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: durable.NamespaceTarget{InstallationID: "test", Namespace: ns}, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
			t.Fatal(err)
		}
	}
	reg, wreg := fc.NewRegistry(), fc.NewWardenRegistry()
	dispatch := dispatcher.New(nil)
	if err := Register(dispatch, reg, wreg, deps); err != nil {
		t.Fatal(err)
	}
	handler := transport.NewHandler(reg, wreg, dispatch, nil)
	for _, intent := range []string{"durable.buildRetire", "durable.buildFinalize", "durable.buildResume", "durable.buildRegister", "durable.retirementEnroll"} {
		t.Run(intent, func(t *testing.T) {
			input := map[string]any{"namespace": "foreign", "build_id": "historic", "request_id": intent, "expected_version": "1", "expected_epoch": "1"}
			payload, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, dispatchErr := dispatch.Dispatch(t.Context(), fc.Request{Contributor: ContributorName, Intent: intent, IntentVersion: 1, Kind: fc.KindCommand, Payload: payload}, testPrincipal()); dispatchErr == nil {
				t.Fatal("direct foreign command authorized")
			}
			body, err := json.Marshal(map[string]any{"envelope": "v1", "kind": "command", "contributor": "dispatch", "intent": intent, "intentVersion": 1, "csrf": "test", "idempotencyKey": intent, "params": map[string]string{"namespace": "allowed"}, "payload": input})
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequestWithContext(testContext(), http.MethodPost, "/api/dashboard/v1", bytes.NewReader(body)))
			if response.Code != http.StatusForbidden {
				t.Fatalf("outer scope authorized foreign payload: %d %s", response.Code, response.Body)
			}
			facts, inspectErr := store.InspectBuildLifecycle(t.Context(), durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "test", Namespace: "foreign"}, BuildID: "historic"})
			if inspectErr != nil || facts.Admission.Version != 1 || facts.Admission.State != durable.BuildAccepting {
				t.Fatalf("denied command mutated target %+v %v", facts, inspectErr)
			}
		})
	}
	input := map[string]any{"namespace": "allowed", "build_id": "historic", "request_id": "retire-allowed", "expected_version": "1", "expected_epoch": "1"}
	body, err := json.Marshal(map[string]any{"envelope": "v1", "kind": "command", "contributor": "dispatch", "intent": "durable.buildRetire", "intentVersion": 1, "csrf": "test", "idempotencyKey": "retire-allowed", "payload": input})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequestWithContext(testContext(), http.MethodPost, "/api/dashboard/v1", bytes.NewReader(body)))
	var out fc.Response
	if err = json.Unmarshal(response.Body.Bytes(), &out); err != nil || !out.OK || len(out.Meta.Invalidates) != 5 {
		t.Fatalf("allowed lifecycle command %s %v", response.Body, err)
	}
}
