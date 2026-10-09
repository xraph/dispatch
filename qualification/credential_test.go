package qualification_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xraph/authsome/authprovider"
	authmw "github.com/xraph/authsome/middleware"
	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"

	"github.com/xraph/dispatch/dwp"
	"github.com/xraph/dispatch/security"
)

func TestExplicitFrameCredentials(t *testing.T) {
	for _, tc := range []struct {
		name   string
		who    int
		nested bool
	}{
		{"equal-cookie-and-frame", 0, false},
		{"different-user-frame", 1, false},
		{"nested-frame-precedence", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newCredentialHost(t)
			s := h.socket(t, nil)
			frame := dwp.Frame{ID: "auth", Method: dwp.MethodAuth, Token: h.sessions[tc.who].Token}
			if tc.nested {
				var err error
				frame.Data, err = json.Marshal(dwp.AuthRequest{Token: "  bEaReR   " + h.sessions[tc.who].Token + "  "})
				if err != nil {
					t.Fatal(err)
				}
				frame.Token = h.sessions[0].Token
			}
			h.assertAdmission(t, s.exchange(t, frame), tc.who)
			response := s.exchange(t, dwp.Frame{ID: "stats", Method: dwp.MethodStats})
			if tc.who == 0 {
				if response.Error == nil || response.Error.Code != dwp.ErrCodeForbidden {
					t.Fatal("userA unexpectedly received userB read grant")
				}
			} else if response.Type != dwp.FrameResponse || response.Error != nil {
				t.Fatal("userB lost its read grant", response.Error)
			}
			checks := h.policyChecks()
			if len(checks) != 3 || checks[2].principal.Subject != h.users[tc.who].ID.String() || checks[2].action != security.OperatorRead || checks[2].resource != h.boundary.Resource {
				t.Fatal("operation used wrong Warden subject or target")
			}
			if tc.who == 0 && !errors.Is(checks[2].err, security.ErrForbidden) {
				t.Fatal("real Warden did not deny userA")
			}
			if tc.who == 1 && checks[2].err != nil {
				t.Fatal("real Warden did not allow userB")
			}
			found := false
			for _, d := range h.deliveries(t) {
				if d.Metadata.ActorID != h.users[tc.who].ID.String() || d.Metadata.ActorKind != "user" || d.Target != "installation" || d.InstallationID != "host" || d.Namespace != "audit" {
					t.Fatal("cookie identity replaced frame audit actor")
				}
				if d.Action == "dwp:stats" {
					found = true
					want := "allowed"
					if tc.who == 0 {
						want = "denied"
					}
					if d.Outcome != want {
						t.Fatal("incorrect operation audit outcome")
					}
				}
			}
			if !found {
				t.Fatal("missing operation audit")
			}
		})
	}
}

func (h *credentialHost) assertAdmission(t *testing.T, response dwp.Frame, who int) {
	t.Helper()
	var admitted dwp.AuthResponse
	if response.Type != dwp.FrameResponse || response.Error != nil {
		t.Fatal("explicit credential rejected", response.Error)
	}
	if err := json.Unmarshal(response.Data, &admitted); err != nil {
		t.Fatal(err)
	}
	conn, exists := h.dwp.Connections().Get(admitted.SessionID)
	if admitted.SessionID == "" || !exists || conn == nil || conn.Identity.Subject != h.users[who].ID.String() || conn.Identity.Kind != "user" {
		t.Fatal("admitted identity is not the verified frame user")
	}
	checks := h.policyChecks()
	if len(checks) != 2 {
		t.Fatalf("want two Warden admission checks, got %d", len(checks))
	}
	for i, action := range []string{security.Subscribe, security.PayloadRead} {
		if checks[i].principal.Subject != h.users[who].ID.String() || checks[i].principal.Kind != "user" || checks[i].action != action || checks[i].resource != h.boundary.Resource || checks[i].err != nil {
			t.Fatal("incorrect Warden admission identity, action or target")
		}
	}
	deliveries := h.deliveries(t)
	if len(deliveries) != 1 || deliveries[0].Metadata.ActorID != h.users[who].ID.String() || deliveries[0].Action != "dwp:subscribe" || deliveries[0].Target != "installation" || deliveries[0].Outcome != "allowed" {
		t.Fatal("incorrect admission audit actor, target or outcome")
	}
}

func TestCookieOnlyAndInvalidFramesDenied(t *testing.T) {
	for _, tc := range []struct{ name, token string }{{"cookie-only", ""}, {"invalid-token", "invalid"}, {"malformed", "Bearer"}} {
		t.Run(tc.name, func(t *testing.T) {
			h := newCredentialHost(t)
			response := h.socket(t, nil).exchange(t, dwp.Frame{ID: "auth", Method: dwp.MethodAuth, Token: tc.token})
			h.assertAuthenticationDenied(t, response)
		})
	}
	for _, path := range []string{"/v1/stats", "/dwp/sse?channel=jobs"} {
		t.Run(path, func(t *testing.T) {
			h := newCredentialHost(t)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, h.http.URL+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Cookie", h.cookie())
			response, err := h.http.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusUnauthorized || !strings.Contains(string(body), "authentication required") || len(h.policyChecks()) != 0 {
				t.Fatal("cookie-only HTTP admission was not rejected before Warden")
			}
			d := h.deliveries(t)
			if len(d) != 1 || d[0].Metadata.ActorKind != "anonymous" || d[0].Outcome != "unauthenticated" {
				t.Fatal("missing anonymous HTTP denial audit")
			}
		})
	}
}

func (h *credentialHost) assertAuthenticationDenied(t *testing.T, response dwp.Frame) {
	t.Helper()
	if response.Type != dwp.FrameErr || response.Error == nil || response.Error.Code != dwp.ErrCodeUnauthorized || response.Error.Message != "authentication failed" {
		t.Fatal("expected authentication refusal after WebSocket upgrade", response.Error)
	}
	if len(h.policyChecks()) != 0 {
		t.Fatal("authentication refusal reached Warden")
	}
	d := h.deliveries(t)
	if len(d) != 1 || d[0].Metadata.ActorKind != "anonymous" || d[0].Metadata.ActorID != "" || d[0].Outcome != "unauthenticated" {
		t.Fatal("missing anonymous authentication denial audit")
	}
}

func TestMismatchedMarkerCannotPromoteBridgedCookie(t *testing.T) {
	h := newCredentialHost(t)
	router := forge.NewRouter()
	router.Use(h.auth.AuthMiddleware())
	if err := router.GET("/probe", func(c forge.Context) error {
		ctx, err := auth.WithExplicitFrameCredential(c.Context(), "Bearer", h.sessions[1].Token)
		if err != nil {
			return err
		}
		scheme, token := authmw.ExtractCredentialFromContext(ctx, c.Request(), authprovider.DefaultSessionCookieName)
		if scheme != "cookie" || token != h.sessions[0].Token {
			t.Error("mismatched marker promoted cookie")
		}
		if _, authErr := h.adapter.Authenticate(ctx, c.Request()); !errors.Is(authErr, security.ErrUnauthenticated) {
			t.Error("mismatched marker admitted unchanged bridged cookie")
		}
		// An unrelated valid header retains ordinary explicit classification.
		r := c.Request().Clone(ctx)
		r.Header.Set("Authorization", "Bearer "+h.sessions[1].Token)
		mismatch, err := auth.WithExplicitFrameCredential(ctx, "Bearer", "unrelated")
		if err != nil {
			return err
		}
		p, err := h.adapter.Authenticate(mismatch, r.WithContext(mismatch))
		if err != nil || p.Subject != h.users[1].ID.String() {
			t.Error("unrelated marker changed ordinary explicit classification")
		}
		return c.JSON(http.StatusOK, map[string]bool{"checked": true})
	}); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/probe", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Cookie", h.cookie())
	rec := httptest.NewRecorder()
	router.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatal("probe did not reach provider")
	}
}
