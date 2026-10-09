package operatorhost

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xraph/authsome/authprovider"
	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	_ "github.com/xraph/grove/drivers/pgdriver/pgmigrate"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/store/memory"
	pgstore "github.com/xraph/dispatch/store/postgres"
)

func call(t *testing.T, url, token, intent string, payload any, cookie bool) (int, []byte) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"envelope": "v1", "kind": "query", "contributor": "dispatch", "intent": intent, "payload": payload, "params": map[string]any{"namespace": "foreign"}})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url+"/api/dashboard/v1", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-App-ID", "forged")
	request.Header.Set("X-Tenant-ID", "forged")
	if token != "" {
		if cookie {
			request.AddCookie(&http.Cookie{Name: authprovider.DefaultSessionCookieName, Value: token})
		} else {
			request.Header.Set("Authorization", "Bearer "+token)
		}
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, data
}
func data[T any](t *testing.T, raw []byte) T {
	t.Helper()
	var response fc.Response
	if err := json.Unmarshal(raw, &response); err != nil || !response.OK {
		t.Fatalf("response refused: %s %v", raw, err)
	}
	var result T
	if err := json.Unmarshal(response.Data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func TestMemoryHTTP(t *testing.T) { runHost(t, memory.New()) }
func TestPostgresHTTP(t *testing.T) {
	dsn := os.Getenv("DISPATCH_OPERATOR_DSN")
	if dsn == "" {
		if os.Getenv("DISPATCH_OPERATOR_REQUIRED") == "1" {
			t.Fatal("dedicated PostgreSQL required")
		}
		t.Skip("dedicated PostgreSQL required")
	}
	driver := pgdriver.New()
	if err := driver.Open(t.Context(), dsn); err != nil {
		t.Fatal(err)
	}
	db, err := grove.Open(driver)
	if err != nil {
		t.Fatal(err)
	}
	runHost(t, pgstore.New(db))
}
func runHost(t *testing.T, store Store) {
	t.Helper()
	h, err := New(t.Context(), store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := h.Close(context.Background()); closeErr != nil {
			t.Error(closeErr)
		}
	})
	server := httptest.NewServer(h.Handler)
	defer server.Close()
	reader := h.Credentials["reader"].Token
	payload := h.Credentials["payload"].Token
	key := map[string]any{"namespace": "production", "workflow_id": "invoice", "run_id": "run-1"}
	for _, cookie := range []bool{false, true} {
		for _, intent := range []string{"executions", "execution", "history", "tasks", "chain", "children", "audit", "hooks"} {
			status, raw := call(t, server.URL, reader, "durable."+intent, key, cookie)
			if status != 200 {
				t.Fatalf("%s returned %d: %s", intent, status, raw)
			}
			if strings.Contains(string(raw), "protected input") || strings.Contains(string(raw), "cHJvdGVjdGVk") || strings.Contains(string(raw), `"input"`) || strings.Contains(string(raw), `"receipt"`) {
				t.Fatal("metadata payload leak")
			}
		}
	}
	status, raw := call(t, server.URL, reader, "durable.namespaces", map[string]any{}, false)
	if status != 200 {
		t.Fatal(status)
	}
	first := data[operator.Page[operator.Namespace]](t, raw)
	if first.Complete || len(first.Items) != 0 || first.Cursor == "" || first.Total != nil {
		t.Fatalf("discovery %+v", first)
	}
	_, raw = call(t, server.URL, reader, "durable.namespaces", map[string]any{"cursor": first.Cursor}, false)
	last := data[operator.Page[operator.Namespace]](t, raw)
	if !last.Complete || len(last.Items) != 1 || last.Items[0].Namespace != "production" {
		t.Fatalf("discovery %+v", last)
	}
	for _, check := range []struct {
		token  string
		status int
		code   string
	}{{"", 401, "UNAUTHENTICATED"}, {"invalid", 401, "UNAUTHENTICATED"}, {h.Credentials["denied"].Token, 403, "PERMISSION_DENIED"}} {
		status, raw = call(t, server.URL, check.token, "durable.execution", key, false)
		requireError(t, status, raw, check.status, check.code)
	}
	status, raw = call(t, server.URL, reader, "durable.execution", map[string]any{"namespace": "foreign", "workflow_id": "invoice", "run_id": "run-1", "app_id": "forged", "tenant_id": "tenant-production"}, false)
	requireError(t, status, raw, 403, "PERMISSION_DENIED")
	status, raw = call(t, server.URL, reader, "jobs.counts", map[string]any{}, false)
	requireError(t, status, raw, 403, "PERMISSION_DENIED")
	status, raw = call(t, server.URL, reader, "durable.payload", key, false)
	requireError(t, status, raw, 403, "PERMISSION_DENIED")
	status, raw = call(t, server.URL, payload, "durable.payload", key, false)
	if status != 200 {
		t.Fatal(status, string(raw))
	}
	reveal := data[operator.Payload](t, raw)
	if !strings.Contains(string(reveal.Input), "protected input") {
		t.Fatal("reveal missing")
	}
	outbox, err := store.ReadDeliveryStatus(t.Context(), durable.ScopedDeliveryStatus{Key: durable.Key{Namespace: "production", WorkflowID: "invoice", RunID: "run-1"}, DeliveryStatusRequest: durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "operator-host", Destination: durable.DestinationChronicle}, Limit: 100}})
	if err != nil {
		t.Fatal(err)
	}
	audited := false
	for _, record := range outbox.Records {
		if record.Delivery.Action == operator.ReadPayload && record.Delivery.Outcome == "allowed" && record.Delivery.Metadata.ActorID == h.Credentials["payload"].Subject {
			audited = true
		}
	}
	if !audited {
		t.Fatal("persisted payload audit missing")
	}
	_, raw = call(t, server.URL, reader, "durable.executions", map[string]any{"namespace": "production", "limit": 1}, false)
	page := data[operator.Page[operator.Execution]](t, raw)
	if page.Cursor == "" {
		t.Fatal("execution continuation absent")
	}
	if err = h.Policies.DeletePolicy(t.Context(), "tenant-production", h.ReaderPolicy); err != nil {
		t.Fatal(err)
	}
	status, raw = call(t, server.URL, reader, "durable.executions", map[string]any{"namespace": "production", "limit": 1, "cursor": page.Cursor}, false)
	requireError(t, status, raw, 403, "PERMISSION_DENIED")
	session, err := h.Auth.ResolveSessionByToken(t.Context(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Auth.RevokeSession(t.Context(), session.ID); err != nil {
		t.Fatal(err)
	}
	status, raw = call(t, server.URL, payload, "durable.payload", key, false)
	requireError(t, status, raw, 401, "UNAUTHENTICATED")
}

func requireError(t *testing.T, status int, raw []byte, wantStatus int, code string) {
	t.Helper()
	var response struct {
		OK    bool `json:"ok"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &response); err != nil || status != wantStatus || response.OK || response.Error.Code != code {
		t.Fatalf("error response: status=%d want=%d code=%s want=%s parse=%v", status, wantStatus, response.Error.Code, code, err)
	}
}
