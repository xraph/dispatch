package operatorhost

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xraph/authsome/authprovider"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"

	"github.com/xraph/dispatch/store/memory"
)

func TestDashboardBootstrapAuthentication(t *testing.T) {
	h, err := New(t.Context(), memory.New())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := h.Close(context.Background()); closeErr != nil {
			t.Error(closeErr)
		}
	})
	server := httptest.NewServer(h.Handler)
	t.Cleanup(server.Close)
	paths := []string{"/principal", "/capabilities"}
	for _, path := range paths {
		for _, token := range []string{"", "invalid"} {
			status, _, raw := bootstrapGET(t, server.URL, path, token, false)
			if status != http.StatusUnauthorized || strings.Contains(string(raw), h.Credentials["reader"].Subject) {
				t.Fatalf("unauthenticated bootstrap %s: status %d", path, status)
			}
		}
	}
	for _, role := range []string{"reader", "denied"} {
		for _, cookie := range []bool{false, true} {
			credential := h.Credentials[role]
			status, cache, raw := bootstrapGET(t, server.URL, "/principal", credential.Token, cookie)
			var principal struct {
				Authenticated bool   `json:"authenticated"`
				Subject       string `json:"subject"`
			}
			if decodeErr := json.Unmarshal(raw, &principal); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if status != http.StatusOK || cache != "no-store" || !principal.Authenticated || principal.Subject != credential.Subject || strings.Contains(string(raw), credential.Token) {
				t.Fatalf("principal did not reflect the authenticated %s session: status %d", role, status)
			}
			status, cache, raw = bootstrapGET(t, server.URL, "/capabilities", credential.Token, cookie)
			var capabilities transport.CapabilitiesResponse
			if decodeErr := json.Unmarshal(raw, &capabilities); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if status != http.StatusOK || cache != "no-store" || len(capabilities.Contributors) != 1 || capabilities.Contributors[0].Name != "dispatch" {
				t.Fatalf("registered capabilities unavailable: status %d", status)
			}
			found := false
			for _, intent := range capabilities.Contributors[0].Intents {
				found = found || intent.Name == "durable.executions"
			}
			if !found || strings.Contains(string(raw), credential.Token) || strings.Contains(string(raw), "protected input") {
				t.Fatal("capabilities missing durable reads or exposing private state")
			}
		}
	}
	status, raw := call(t, server.URL, h.Credentials["denied"].Token, "durable.execution", map[string]any{"namespace": "production", "workflow_id": "invoice", "run_id": "run-1"}, false)
	requireError(t, status, raw, http.StatusForbidden, "PERMISSION_DENIED")
	session, err := h.Auth.ResolveSessionByToken(t.Context(), h.Credentials["reader"].Token)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Auth.RevokeSession(t.Context(), session.ID); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		for _, cookie := range []bool{false, true} {
			status, _, _ = bootstrapGET(t, server.URL, path, h.Credentials["reader"].Token, cookie)
			if status != http.StatusUnauthorized {
				t.Fatalf("revoked bootstrap session accepted at %s: %d", path, status)
			}
		}
	}
}

func bootstrapGET(t *testing.T, base, path, token string, cookie bool) (int, string, []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"/api/dashboard/v1"+path, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-User-ID", "forged")
	request.Header.Set("X-Tenant-ID", "forged")
	if token != "" {
		if cookie {
			request.AddCookie(&http.Cookie{Name: authprovider.DefaultSessionCookieName, Value: token})
		} else {
			request.Header.Set("Authorization", "Bearer "+token)
		}
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, response.Header.Get("Cache-Control"), raw
}
