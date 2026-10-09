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
func TestForeignOriginCookieWebSocketRejected(t *testing.T) {
	eng, _ := setupTestEngine(t)
	h := NewHandler(eng, eng.StreamBroker(), testLogger(), testBoundary())
	server := NewServer(eng.StreamBroker(), h)
	router := forge.NewRouter()
	server.RegisterRoutes(router)
	httpServer := httptest.NewServer(router.Handler())
	defer httpServer.Close()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, httpServer.URL+"/dwp", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Origin", "https://foreign.example")
	req.Header.Set("Cookie", "session=valid-operator")
	recorder := httptest.NewRecorder()
	router.Handler().ServeHTTP(recorder, req)
	if recorder.Code == http.StatusSwitchingProtocols {
		t.Fatal("foreign cookie attempt upgraded")
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
