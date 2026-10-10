package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/memory"
)

func TestRegisteredLifecycleSchemas(t *testing.T) {
	registry := fc.NewRegistry()
	if err := Register(dispatcher.New(nil), registry, fc.NewWardenRegistry(), durableDeps(t, memory.New())); err != nil {
		t.Fatal(err)
	}
	inputs := map[string][]string{
		"durable.compatibility": {"namespace"}, "durable.build": {"namespace", "build_id"},
		"durable.retirementEnroll":   {"namespace", "request_id"},
		"durable.buildRegister":      {"namespace", "build_id", "request_id", "expected_version"},
		"durable.buildRetire":        {"namespace", "build_id", "request_id", "expected_version", "expected_epoch"},
		"durable.buildFinalize":      {"namespace", "build_id", "request_id", "expected_version", "expected_epoch"},
		"durable.buildResume":        {"namespace", "build_id", "request_id", "expected_version", "expected_epoch"},
		"durable.workerStatus":       {"namespace", "build_id", "runtime_id"},
		"durable.workerDrain":        {"namespace", "build_id", "runtime_id", "request_id", "operation_id", "deadline"},
		"durable.workerDrainReceipt": {"namespace", "build_id", "runtime_id", "request_id", "operation_id", "deadline"},
	}
	for name, fields := range inputs {
		t.Run(name, func(t *testing.T) {
			intent, ok := registry.Intent(ContributorName, name, 1)
			if !ok {
				t.Fatal("registered intent missing")
			}
			encoded, err := json.Marshal(intent.Schema)
			if err != nil {
				t.Fatal(err)
			}
			var schema map[string]map[string]any
			if err = json.Unmarshal(encoded, &schema); err != nil {
				t.Fatal(err)
			}
			for _, side := range []string{"input", "output"} {
				if schema[side]["type"] != "object" {
					t.Fatalf("missing public %s schema: %s", side, encoded)
				}
			}
			properties := schema["input"]["properties"].(map[string]any)
			required := schema["input"]["required"].([]any)
			for _, field := range fields {
				property, exists := properties[field].(map[string]any)
				if !exists || property["type"] != "string" || !slices.Contains(required, any(field)) {
					t.Fatalf("missing required string %s: %s", field, encoded)
				}
				if (field == "expected_version" || field == "expected_epoch") && property["pattern"] == nil {
					t.Fatalf("missing canonical decimal constraint: %s", encoded)
				}
				if field == "deadline" && property["format"] != "date-time" {
					t.Fatalf("missing deadline format: %s", encoded)
				}
			}
			output := schema["output"]["properties"].(map[string]any)
			for _, field := range []string{"epoch", "version", "compatibility_version", "retained_executions", "verified_query_runtimes", "in_flight", "unknown_claims"} {
				if property, exists := output[field].(map[string]any); exists && (property["type"] != "string" || property["pattern"] == nil) {
					t.Fatalf("lossy %s output: %s", field, encoded)
				}
			}
		})
	}
}

type largeLifecycleStore struct{ *memory.Store }

func (s *largeLifecycleStore) InspectBuildLifecycle(ctx context.Context, target durable.BuildTarget) (durable.BuildLifecycleFacts, error) {
	f, err := s.Store.InspectBuildLifecycle(ctx, target)
	f.Admission.Epoch = 9007199254740993
	f.Admission.Version = 9007199254740995
	f.ObservationVersion.CompatibilityVersion = 9007199254740997
	f.Blockers.OpenExecutions = 9007199254740999
	f.QueryRetention.RetainedExecutions = 9007199254741001
	f.QueryRetention.VerifiedBindings = 9007199254741003
	return f, err
}

func TestLifecycleHTTPPreservesDecimalIntegers(t *testing.T) {
	store := &largeLifecycleStore{Store: memory.New()}
	deps := durableDeps(t, store)
	if _, err := store.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: durable.NamespaceTarget{InstallationID: "test", Namespace: "allowed"}, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	registry, wardens := fc.NewRegistry(), fc.NewWardenRegistry()
	d := dispatcher.New(nil)
	if err := Register(d, registry, wardens, deps); err != nil {
		t.Fatal(err)
	}
	handler := transport.NewHandler(registry, wardens, d, nil)
	call := func(intent, kind string, payload map[string]any) *httptest.ResponseRecorder {
		body, err := json.Marshal(map[string]any{"envelope": "v1", "kind": kind, "contributor": "dispatch", "intent": intent, "intentVersion": 1, "csrf": "test", "idempotencyKey": "decimal-" + intent, "payload": payload})
		if err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequestWithContext(testContext(), http.MethodPost, "/api/dashboard/v1", bytes.NewReader(body)))
		return response
	}
	response := call("durable.build", "query", map[string]any{"namespace": "allowed", "build_id": "historic"})
	var envelope fc.Response
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || !envelope.OK {
		t.Fatalf("build wire: %s %v", response.Body, err)
	}
	var data map[string]any
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]string{"epoch": "9007199254740993", "version": "9007199254740995", "compatibility_version": "9007199254740997", "retained_executions": "9007199254741001", "verified_query_runtimes": "9007199254741003"} {
		if data[field] != want {
			t.Fatalf("%s lost precision: %#v", field, data[field])
		}
	}
	if data["blockers"].(map[string]any)["open_executions"] != "9007199254740999" {
		t.Fatal("blocker lost precision")
	}
	for _, invalid := range []any{int64(9007199254740993), float64(9007199254740993), "9.007199254740993e15", "09007199254740993"} {
		response = call("durable.buildRetire", "command", map[string]any{"namespace": "allowed", "build_id": "historic", "request_id": "invalid", "expected_version": invalid, "expected_epoch": "1"})
		if response.Code != http.StatusBadRequest {
			t.Fatalf("lossy version accepted: %v %s", invalid, response.Body)
		}
	}
}
