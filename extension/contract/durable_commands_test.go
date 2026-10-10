package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"

	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func TestDurableCommandHTTPDecodedTargetAndInvalidation(t *testing.T) {
	store := memory.New()
	worker, err := drt.NewWorker(store, drt.Options{Namespace: "allowed", BuildID: "historic", Queue: "queue", Owner: "operator", Workflows: map[string]drt.WorkflowFunc{"workflow": func(_ *drt.Workflow, input []byte) ([]byte, error) { return input, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	deps := durableDepsRuntime(t, store, func(string, string) (*drt.Worker, error) { return worker, nil })
	reg, wreg := fc.NewRegistry(), fc.NewWardenRegistry()
	d := dispatcher.New(nil)
	if err = Register(d, reg, wreg, deps); err != nil {
		t.Fatal(err)
	}
	handler := transport.NewHandler(reg, wreg, d, nil)
	for _, intent := range []string{"signal", "cancel", "start", "signalStart"} {
		t.Run(intent, func(t *testing.T) {
			input := map[string]any{"namespace": "allowed", "workflow_id": "workflow", "run_id": "run", "request_id": intent, "build_id": "historic", "workflow_type": "workflow", "queue": "queue", "name": "signal", "input": "OTAwNzE5OTI1NDc0MDk5Mw=="}
			if intent == "start" {
				input["workflow_id"] = "new-workflow"
			}
			if intent == "signalStart" {
				input = map[string]any{"start": input, "name": "signal"}
			}
			send := func(scope string) *httptest.ResponseRecorder {
				t.Helper()
				target := input
				if intent == "signalStart" {
					target = input["start"].(map[string]any)
				}
				target["namespace"] = scope
				raw, _ := json.Marshal(map[string]any{"envelope": "v1", "kind": "command", "contributor": "dispatch", "intent": "durable." + intent, "intentVersion": 1, "csrf": "test", "idempotencyKey": intent, "params": map[string]any{"namespace": "allowed"}, "payload": input})
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequestWithContext(testContext(), http.MethodPost, "/api/dashboard/v1", bytes.NewReader(raw)))
				return response
			}
			if response := send("foreign"); response.Code != http.StatusForbidden {
				t.Fatalf("forged target: %d %s", response.Code, response.Body)
			}
			response := send("allowed")
			var out fc.Response
			if err = json.Unmarshal(response.Body.Bytes(), &out); err != nil || !out.OK {
				t.Fatalf("command: %s %v", response.Body, err)
			}
			if len(out.Meta.Invalidates) != 11 {
				t.Fatalf("missing invalidation: %+v", out.Meta.Invalidates)
			}
			raw, _ := json.Marshal(input)
			if _, _, err = d.Dispatch(context.Background(), fc.Request{Contributor: ContributorName, Intent: "durable." + intent, IntentVersion: 1, Kind: fc.KindQuery, Payload: raw}, testPrincipal()); err == nil {
				t.Fatal("wrong direct dispatch kind accepted")
			}
			if _, err = (durableWarden{deps: deps}).Authorize(t.Context(), testPrincipal(), fc.Action{Contributor: ContributorName, Intent: "durable." + intent, Kind: fc.KindQuery}); !errors.Is(err, fc.ErrPermissionDenied) {
				t.Fatal("wrong admission kind", err)
			}
		})
	}
}
