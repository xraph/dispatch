package dwp

import (
	"context"
	"encoding/json"
	"errors"
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

func TestActualWebSocketAuthenticationAndRevocation(t *testing.T) {
	eng, _ := setupTestEngine(t)
	boundary := testBoundary()
	var revoked atomic.Bool
	boundary.Authorizer = security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error {
		if revoked.Load() {
			return security.ErrForbidden
		}
		return nil
	})
	h := NewHandler(eng, eng.StreamBroker(), testLogger(), boundary)
	server := NewServer(eng.StreamBroker(), h, WithAuth(NewAPIKeyAuthenticator(APIKeyEntry{Token: "explicit", Identity: Identity{Subject: "operator"}})))
	router := forge.NewRouter()
	server.RegisterRoutes(router)
	httpServer := httptest.NewServer(router.Handler())
	defer httpServer.Close()
	for _, token := range []string{"", "explicit"} {
		t.Run(token, func(t *testing.T) {
			conn, reader, _, err := ws.Dial(t.Context(), "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/dwp")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if deadlineErr := conn.SetDeadline(time.Now().Add(8 * time.Second)); deadlineErr != nil {
				t.Fatal(deadlineErr)
			}
			frame := Frame{ID: "auth", Method: MethodAuth, Token: token}
			raw, err := json.Marshal(frame)
			if err != nil {
				t.Fatal(err)
			}
			if writeErr := wsutil.WriteClientMessage(conn, ws.OpText, raw); writeErr != nil {
				t.Fatal(writeErr)
			}
			var input io.ReadWriter = conn
			if reader != nil {
				input = struct {
					io.Reader
					io.Writer
				}{Reader: reader, Writer: conn}
			}
			raw, _, err = wsutil.ReadServerData(input)
			if err != nil {
				t.Fatal(err)
			}
			var response Frame
			if decodeErr := json.Unmarshal(raw, &response); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if token == "" {
				if response.Error == nil || response.Error.Code != 401 {
					t.Fatal(response)
				}
				return
			}
			if response.Type != FrameResponse {
				t.Fatal(response)
			}
			revoked.Store(true)
			// Passive subscribers are closed by the five-second recheck, with no frames.
			if _, _, err = wsutil.ReadServerData(input); err == nil {
				t.Fatal("idle revoked connection survived")
			}
		})
	}
}

// cookieSessionProvider validates one known session over either credential path.
// Its trusted provenance lets the stock adapter distinguish the cookie from a token.
type cookieSessionProvider struct{}

func (cookieSessionProvider) Name() string                       { return "session" }
func (cookieSessionProvider) Type() auth.SecuritySchemeType      { return auth.SecurityTypeHTTP }
func (cookieSessionProvider) OpenAPIScheme() auth.SecurityScheme { return auth.SecurityScheme{} }
func (cookieSessionProvider) Middleware() forge.Middleware       { return nil }
func (cookieSessionProvider) Authenticate(_ context.Context, r *http.Request) (*auth.AuthContext, error) {
	credential, scheme := "", "cookie"
	if header := r.Header.Get("Authorization"); header != "" {
		if !strings.HasPrefix(header, "Bearer ") {
			return nil, security.ErrUnauthenticated
		}
		credential, scheme = strings.TrimPrefix(header, "Bearer "), "bearer"
	} else if cookie, err := r.Cookie("session"); err == nil {
		credential = cookie.Value
	}
	if credential != "valid-operator" {
		return nil, security.ErrUnauthenticated
	}
	return &auth.AuthContext{Subject: "operator", Metadata: map[string]any{"credential_scheme": scheme}}, nil
}

func TestForeignOriginCookieWebSocketRejected(t *testing.T) {
	provider := cookieSessionProvider{}
	cookieRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://host/dwp", nil)
	cookieRequest.AddCookie(&http.Cookie{Name: "session", Value: "valid-operator"})
	verified, err := provider.Authenticate(cookieRequest.Context(), cookieRequest)
	if err != nil || verified == nil || verified.Subject != "operator" || verified.Metadata["credential_scheme"] != "cookie" {
		t.Fatalf("operator cookie must be valid to the configured provider: %v, %v", verified, err)
	}
	registry := auth.NewRegistry(nil, forge.NewNoopLogger())
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	adapter := &ForgeAuthenticator{Auth: security.NewForgeAuthenticator(func() (auth.Registry, error) { return registry, nil })}
	eng, _ := setupTestEngine(t)
	boundary := testBoundary()
	boundary.Authorizer = security.AuthorizerFunc(func(_ context.Context, principal security.Principal, _ string, _ security.Resource) error {
		if principal.Subject != "operator" || principal.Kind != "user" {
			return security.ErrForbidden
		}
		return nil
	})
	h := NewHandler(eng, eng.StreamBroker(), testLogger(), boundary)
	server := NewServer(eng.StreamBroker(), h, WithAuth(adapter))
	router := forge.NewRouter()
	server.RegisterRoutes(router)
	httpServer := httptest.NewServer(router.Handler())
	defer httpServer.Close()
	url := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/dwp"

	t.Run("foreign-origin-cookie", func(t *testing.T) {
		dialer := ws.Dialer{Timeout: 2 * time.Second, Header: ws.HandshakeHeaderHTTP(http.Header{
			"Origin": {"https://foreign.example"}, "Cookie": {"session=valid-operator"},
		})}
		conn, _, _, err := dialer.Dial(t.Context(), url)
		if conn != nil {
			defer conn.Close()
		}
		var status ws.StatusError
		if !errors.As(err, &status) || int(status) != http.StatusForbidden {
			t.Fatalf("want origin refusal HTTP 403, got %v", err)
		}
	})

	for _, variant := range []string{"same-origin-cookie", "explicit-header", "explicit-frame"} {
		t.Run(variant, func(t *testing.T) {
			headers := http.Header{"Origin": {httpServer.URL}, "Cookie": {"session=valid-operator"}}
			frame := Frame{ID: "auth", Type: FrameRequest, Method: MethodAuth}
			switch variant {
			case "explicit-header":
				headers.Set("Authorization", "Bearer valid-operator")
			case "explicit-frame":
				frame.Token = "valid-operator"
			}
			dialer := ws.Dialer{Timeout: 2 * time.Second, Header: ws.HandshakeHeaderHTTP(headers)}
			conn, reader, _, err := dialer.Dial(t.Context(), url)
			if err != nil {
				t.Fatalf("same-origin connection must upgrade: %v", err)
			}
			defer conn.Close()
			if deadlineErr := conn.SetDeadline(time.Now().Add(2 * time.Second)); deadlineErr != nil {
				t.Fatal(deadlineErr)
			}
			if writeErr := wsutil.WriteClientMessage(conn, ws.OpText, mustJSON(frame)); writeErr != nil {
				t.Fatal(writeErr)
			}
			var input io.ReadWriter = conn
			if reader != nil {
				input = struct {
					io.Reader
					io.Writer
				}{Reader: reader, Writer: conn}
			}
			raw, _, err := wsutil.ReadServerData(input)
			if err != nil {
				t.Fatalf("expected a DWP auth response, got transport failure: %v", err)
			}
			var response Frame
			if err := json.Unmarshal(raw, &response); err != nil {
				t.Fatal(err)
			}
			if response.CorrelID != frame.ID {
				t.Fatalf("unrelated auth response: %+v", response)
			}
			if variant == "same-origin-cookie" {
				if response.Type != FrameErr || response.Error == nil || response.Error.Code != ErrCodeUnauthorized || response.Error.Message != "authentication failed" {
					t.Fatalf("want DWP authentication refusal 401, got %+v", response)
				}
				return
			}
			var admitted AuthResponse
			if err := json.Unmarshal(response.Data, &admitted); err != nil {
				t.Fatal(err)
			}
			if response.Type != FrameResponse || response.Error != nil || admitted.SessionID == "" {
				t.Fatalf("explicit credential must authenticate through the same mount: %+v", response)
			}
		})
	}
}

func TestPassiveSSERevocationClosesAndUnsubscribes(t *testing.T) {
	eng, _ := setupTestEngine(t)
	var revoked atomic.Bool
	boundary := testBoundary()
	boundary.Authorizer = security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error {
		if revoked.Load() {
			return security.ErrForbidden
		}
		return nil
	})
	h := NewHandler(eng, eng.StreamBroker(), testLogger(), boundary)
	server := NewServer(eng.StreamBroker(), h, WithAuth(NewAPIKeyAuthenticator(APIKeyEntry{Token: "explicit", Identity: Identity{Subject: "operator"}})))
	router := forge.NewRouter()
	server.RegisterRoutes(router)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/dwp/sse?channel=jobs", nil)
	request.Header.Set("Authorization", "explicit")
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { router.Handler().ServeHTTP(recorder, request); close(done) }()
	deadline := time.Now().Add(time.Second)
	for eng.StreamBroker().Stats().SubscriberCount != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if eng.StreamBroker().Stats().SubscriberCount != 1 {
		t.Fatal("SSE did not subscribe")
	}
	revoked.Store(true)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("idle revoked SSE survived")
	}
	if eng.StreamBroker().Stats().SubscriberCount != 0 {
		t.Fatal("revoked SSE left subscriber")
	}
}
