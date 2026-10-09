package dwp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"

	"github.com/xraph/dispatch/security"
)

func TestWebSocketFrameCredentialPresentation(t *testing.T) {
	for _, tc := range []struct {
		name, nested, outer, scheme, token string
		invalid                            bool
	}{
		{name: "bare", outer: "ExactToken", scheme: "Bearer", token: "ExactToken"},
		{name: "normalized", outer: "  bEaReR   ExactToken  ", scheme: "Bearer", token: "ExactToken"},
		{name: "nested-precedence", nested: "dPoP ProofToken", outer: "ignored", scheme: "DPoP", token: "ProofToken"},
		{name: "empty", scheme: "Bearer", token: "cookie-token"},
		{name: "malformed-selected", nested: "Bearer", outer: "valid-fallback", invalid: true},
		{name: "unsupported", outer: "Basic token", invalid: true},
		{name: "control", outer: "Bearer\ttoken", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, _ := setupTestEngine(t)
			var calls atomic.Int64
			adapter := &ForgeAuthenticator{Auth: security.AuthenticatorFunc(func(ctx context.Context, r *http.Request) (security.Principal, error) {
				calls.Add(1)
				if ctx.Value(dwpProofKey{}) != "cookie-provenance" || r.Context().Value(dwpProofKey{}) != "cookie-provenance" || r.Method != http.MethodGet || r.URL.Path != "/dwp" || r.Header.Get("DPoP") != "handshake-proof" || r.Header.Get("Cookie") != "session=cookie-token" {
					t.Error("lost handshake context or proof headers")
				}
				if r.Header.Get("Authorization") != tc.scheme+" "+tc.token {
					t.Error("authorization differs from parsed presentation")
				}
				marked := auth.MatchesExplicitFrameCredential(ctx, tc.scheme, tc.token)
				if marked != (tc.nested != "" || tc.outer != "") {
					t.Error("incorrect explicit-frame provenance")
				}
				return security.Principal{Subject: "verified", Kind: "user"}, nil
			})}
			server := NewServer(eng.StreamBroker(), NewHandler(eng, eng.StreamBroker(), testLogger(), testBoundary()), WithAuth(adapter))
			router := forge.NewRouter()
			router.Use(func(next forge.Handler) forge.Handler {
				return func(c forge.Context) error {
					c.WithContext(context.WithValue(c.Context(), dwpProofKey{}, "cookie-provenance"))
					c.Request().Header.Set("Authorization", "Bearer cookie-token")
					return next(c)
				}
			})
			server.RegisterRoutes(router)
			host := httptest.NewServer(router.Handler())
			defer host.Close()
			dialer := ws.Dialer{Header: ws.HandshakeHeaderHTTP(http.Header{"Cookie": {"session=cookie-token"}, "Dpop": {"handshake-proof"}})}
			conn, reader, _, err := dialer.Dial(t.Context(), "ws"+strings.TrimPrefix(host.URL, "http")+"/dwp")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if deadlineErr := conn.SetDeadline(time.Now().Add(3 * time.Second)); deadlineErr != nil {
				t.Fatal(deadlineErr)
			}
			frame := Frame{ID: "auth", Method: MethodAuth, Token: tc.outer, Data: mustJSON(AuthRequest{Token: tc.nested})}
			if writeErr := wsutil.WriteClientMessage(conn, ws.OpText, mustJSON(frame)); writeErr != nil {
				t.Fatal(writeErr)
			}
			var input io.ReadWriter = conn
			if reader != nil {
				input = struct {
					io.Reader
					io.Writer
				}{reader, conn}
			}
			raw, _, err := wsutil.ReadServerData(input)
			if err != nil {
				t.Fatal(err)
			}
			var response Frame
			if err := json.Unmarshal(raw, &response); err != nil {
				t.Fatal(err)
			}
			if tc.invalid {
				if response.Error == nil || response.Error.Code != ErrCodeUnauthorized || calls.Load() != 0 {
					t.Fatal("malformed presentation reached provider or was admitted")
				}
			} else if response.Error != nil || calls.Load() != 1 {
				t.Fatal("valid presentation did not reach provider", response.Error)
			}
		})
	}
}

func TestGenericRequestAdapterDoesNotMarkCredentials(t *testing.T) {
	adapter := &ForgeAuthenticator{Auth: security.AuthenticatorFunc(func(ctx context.Context, r *http.Request) (security.Principal, error) {
		if auth.MatchesExplicitFrameCredential(ctx, "Bearer", "cookie-token") || auth.MatchesExplicitFrameCredential(r.Context(), "Bearer", "cookie-token") {
			t.Fatal("generic request adapter promoted a bridged header")
		}
		return security.Principal{Subject: "verified", Kind: "user"}, nil
	})}
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/dwp/sse", nil)
	r.Header.Set("Authorization", "Bearer cookie-token")
	if _, err := adapter.AuthenticateRequest(r.Context(), r, r.Header.Get("Authorization")); err != nil {
		t.Fatal(err)
	}
}
