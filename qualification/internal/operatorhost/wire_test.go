package operatorhost

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/xraph/dispatch/store/memory"
)

func TestHTTPErrorFixtures(t *testing.T) {
	h, err := New(t.Context(), memory.New())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	server := httptest.NewServer(h.Handler)
	defer server.Close()
	reader := h.Credentials["reader"].Token
	type entry struct {
		Status   int             `json:"http_status"`
		Envelope json.RawMessage `json:"response"`
	}
	fixtures := map[string]entry{}
	for _, test := range []struct {
		name, token, intent string
		payload             any
		status              int
		code                string
	}{
		{"anonymous", "", "durable.execution", map[string]any{"namespace": "production", "workflow_id": "invoice", "run_id": "run-1"}, 401, "UNAUTHENTICATED"},
		{"forbidden", h.Credentials["denied"].Token, "durable.execution", map[string]any{"namespace": "production", "workflow_id": "invoice", "run_id": "run-1"}, 403, "PERMISSION_DENIED"},
		{"payload_denied", reader, "durable.payload", map[string]any{"namespace": "production", "workflow_id": "invoice", "run_id": "run-1"}, 403, "PERMISSION_DENIED"},
		{"invalid_limit", reader, "durable.executions", map[string]any{"namespace": "production", "limit": 101}, 400, "BAD_REQUEST"},
		{"not_found", reader, "durable.execution", map[string]any{"namespace": "production", "workflow_id": "missing", "run_id": "missing"}, 404, "NOT_FOUND"},
	} {
		status, raw := call(t, server.URL, test.token, test.intent, test.payload, false)
		requireError(t, status, raw, test.status, test.code)
		fixtures[test.name] = entry{status, raw}
	}
	h.audit.Audit.Deactivate()
	status, raw := call(t, server.URL, reader, "durable.execution", map[string]any{"namespace": "production", "workflow_id": "invoice", "run_id": "run-1"}, false)
	requireError(t, status, raw, 503, "UNAVAILABLE")
	fixtures["unavailable"] = entry{status, raw}
	encoded, err := json.MarshalIndent(fixtures, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	const path = "testdata/http-errors.json"
	if os.Getenv("UPDATE_OPERATOR_HTTP_WIRE") == "1" {
		if err = os.WriteFile(path, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(expected) != string(encoded) {
		t.Fatal("published HTTP error behavior changed; review fixture")
	}
}
