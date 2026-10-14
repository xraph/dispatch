package managedhost

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	ft "github.com/xraph/forge/extensions/dashboard/contract/transport"

	"github.com/xraph/dispatch/operator"
)

func TestNativeClientFetchesAuthenticatedCSRFBeforeCommand(t *testing.T) {
	csrfCalls, commands := 0, 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-machine" {
			t.Error("missing machine credential")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/api/dashboard/v1/csrf" {
			csrfCalls++
			if r.Method != http.MethodGet {
				t.Error("CSRF method")
			}
			_ = json.NewEncoder(w).Encode(ft.CSRFTokenResponse{Token: "authenticated-token", ExpiresAt: time.Now().Add(time.Minute)})
			return
		}
		commands++
		var request fc.Request
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.CSRF != "authenticated-token" || request.IdempotencyKey == "" || request.Contributor != "dispatch" || request.Intent != "durable.buildRegister" {
			t.Error("wrong command envelope")
		}
		_ = json.NewEncoder(w).Encode(fc.Response{OK: true, Envelope: "v1", Kind: fc.KindCommand, Data: json.RawMessage(`{}`), Meta: fc.ResponseMeta{IntentVersion: 1}})
	}))
	defer server.Close()
	client := &NativeClient{endpoint: server.URL, bearer: "private-machine", client: server.Client()}
	for range 2 {
		if _, err := client.RegisterBuild(t.Context(), operator.RegisterBuildInput{}); err != nil {
			t.Fatal(err)
		}
	}
	if csrfCalls != 2 || commands != 2 {
		t.Fatalf("CSRF=%d commands=%d", csrfCalls, commands)
	}
}
func TestNativeClientDeniedCSRFNeverSendsCommand(t *testing.T) {
	commands := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/dashboard/v1/csrf" {
			commands++
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	client := &NativeClient{endpoint: server.URL, bearer: "machine", client: server.Client()}
	if _, err := client.RegisterBuild(t.Context(), operator.RegisterBuildInput{}); err == nil {
		t.Fatal("denied CSRF accepted")
	}
	if commands != 0 {
		t.Fatal("command reached transport without CSRF")
	}
}
func TestNativeResponseCompleteLimitAndStrictShape(t *testing.T) {
	type value struct {
		Padding string `json:"padding"`
	}
	raw, err := json.Marshal(value{Padding: strings.Repeat("a", (64<<10)-14)})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 64<<10 {
		t.Fatal("wrong complete cap")
	}
	var result value
	if decodeNativeResponse(strings.NewReader(string(raw)), &result) != nil {
		t.Fatal("exact cap refused")
	}
	for _, bad := range []string{string(raw) + " ", `{"unknown":1}`, `{"padding":"one"}{"padding":"two"}`} {
		if decodeNativeResponse(strings.NewReader(bad), &result) == nil {
			t.Fatal("invalid response accepted")
		}
	}
}
