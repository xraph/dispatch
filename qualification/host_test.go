package qualification_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	authsome "github.com/xraph/authsome"
	"github.com/xraph/authsome/app"
	"github.com/xraph/authsome/authprovider"
	aid "github.com/xraph/authsome/id"
	authmw "github.com/xraph/authsome/middleware"
	"github.com/xraph/authsome/session"
	authmem "github.com/xraph/authsome/store/memory"
	"github.com/xraph/authsome/user"
	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	log "github.com/xraph/go-utils/log"
	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	wmem "github.com/xraph/warden/store/memory"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/api"
	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/dwp"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type policyCheck struct {
	principal security.Principal
	action    string
	resource  security.Resource
	err       error
}

type credentialHost struct {
	auth     *authsome.Engine
	app      aid.AppID
	users    [2]*user.User
	sessions [2]*session.Session
	http     *httptest.Server
	dwp      *dwp.Server
	adapter  *security.ForgeAuthenticator
	audit    *memory.Store
	boundary security.Boundary
	mu       sync.Mutex
	checks   []policyCheck
}

func newCredentialHost(t *testing.T) *credentialHost {
	t.Helper()
	ctx := t.Context()
	st := authmem.New()
	h := &credentialHost{app: aid.NewAppID(), audit: memory.New()}
	now := time.Now()
	if err := st.CreateApp(ctx, &app.App{ID: h.app, Name: "Credential qualification", Slug: "platform", IsPlatform: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	policies := wmem.New()
	w, err := warden.NewEngine(warden.WithStore(policies))
	if err != nil {
		t.Fatal(err)
	}
	h.auth, err = authority.New(st, w, h.app.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if startErr := h.auth.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if stopErr := h.auth.Stop(context.Background()); stopErr != nil {
			t.Error(stopErr)
		}
	})
	for i, email := range []string{"a@example.test", "b@example.test"} {
		u := &user.User{ID: aid.NewUserID(), AppID: h.app, Email: email, EmailVerified: true, CreatedAt: now, UpdatedAt: now}
		if createErr := st.CreateUser(ctx, u); createErr != nil {
			t.Fatal(createErr)
		}
		h.users[i] = u
		h.sessions[i] = h.issue(t, i, "")
		actions := []string{security.Subscribe, security.PayloadRead}
		if i == 1 {
			actions = append(actions, security.OperatorRead)
		}
		if createErr := policies.CreatePolicy(ctx, &policy.Policy{ID: wid.NewPolicyID(), TenantID: "host", Name: email, IsActive: true, Effect: policy.EffectAllow, Subjects: []policy.SubjectMatch{{Kind: "user", ID: u.ID.String()}}, Actions: actions, Resources: []string{"dispatch_installation:host"}}); createErr != nil {
			t.Fatal(createErr)
		}
	}
	if h.sessions[0].Token == h.sessions[1].Token {
		t.Fatal("issued sessions must differ")
	}
	h.adapter = security.NewForgeAuthenticator(func() (auth.Registry, error) { return h.auth.AuthRegistry(), nil })
	dispatcher, err := dispatch.New(dispatch.WithStore(memory.New()))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Build(dispatcher, engine.WithStreamBroker())
	if err != nil {
		t.Fatal(err)
	}
	wa := &security.WardenAuthorizer{Engine: func() (*warden.Engine, error) { return w, nil }}
	h.boundary = security.Boundary{Resource: security.Resource{InstallationID: "host", PolicyTenant: "host"}, Audit: &security.AuditService{}, Authorizer: security.AuthorizerFunc(func(ctx context.Context, p security.Principal, action string, r security.Resource) error {
		err := wa.Authorize(ctx, p, action, r)
		h.mu.Lock()
		h.checks = append(h.checks, policyCheck{p, action, r, err})
		h.mu.Unlock()
		return err
	})}
	if _, registerErr := h.audit.RegisterNamespace(ctx, durable.NamespaceConfig{InstallationID: "host", Namespace: "audit", AppID: h.app.String(), TenantID: "host", RequireAudit: true, SchemaVersion: 1}); registerErr != nil {
		t.Fatal(registerErr)
	}
	if activateErr := h.boundary.Audit.Activate(ctx, h.audit, h.audit, h.boundary.Resource, "audit", false); activateErr != nil {
		t.Fatal(activateErr)
	}
	h.dwp = dwp.NewServer(eng.StreamBroker(), dwp.NewHandler(eng, eng.StreamBroker(), log.NewNoopLogger(), h.boundary), dwp.WithAuth(&dwp.ForgeAuthenticator{Auth: h.adapter}))
	router := forge.NewRouter()
	router.Use(h.auth.AuthMiddleware())
	router.Use(func(next forge.Handler) forge.Handler {
		return func(c forge.Context) error {
			if cookie, cookieErr := c.Request().Cookie(authprovider.DefaultSessionCookieName); cookieErr == nil {
				who := 0
				if cookie.Value != h.sessions[0].Token {
					who = 1
				}
				scheme, token := authmw.ExtractCredentialFromContext(c.Context(), c.Request(), authprovider.DefaultSessionCookieName)
				u, ok := authmw.UserFrom(c.Context())
				if scheme != "cookie" || token != cookie.Value || c.Request().Header.Get("Authorization") != "Bearer "+cookie.Value || !ok || u == nil || u.ID != h.users[who].ID {
					t.Error("global middleware did not establish the cookie user and provenance")
				}
			}
			return next(c)
		}
	})
	if routeErr := api.New(eng, router, api.WithSecurity(h.adapter, h.boundary)).RegisterRoutes(router); routeErr != nil {
		t.Fatal(routeErr)
	}
	h.dwp.RegisterRoutes(router)
	h.http = httptest.NewServer(router.Handler())
	t.Cleanup(h.http.Close)
	return h
}

func (h *credentialHost) issue(t *testing.T, who int, jkt string) *session.Session {
	t.Helper()
	issued, err := h.auth.IssueSession(t.Context(), &authsome.IssueSessionRequest{User: h.users[who], AppID: h.app, AuthMethod: "qualification", IPAddress: "127.0.0.1", DPoPJKT: jkt})
	if err != nil {
		t.Fatal(err)
	}
	if issued == nil || issued.Session == nil || issued.Session.Token == "" {
		t.Fatal("no issued session")
	}
	return issued.Session
}

func (h *credentialHost) cookie() string {
	return authprovider.DefaultSessionCookieName + "=" + h.sessions[0].Token
}
func (h *credentialHost) policyChecks() []policyCheck {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]policyCheck(nil), h.checks...)
}
func (h *credentialHost) deliveries(t *testing.T) []durable.Delivery {
	t.Helper()
	status, err := h.audit.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "host", Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	out := make([]durable.Delivery, 0, len(status.Records))
	for _, record := range status.Records {
		out = append(out, record.Delivery)
	}
	if h.boundary.Audit.AcceptanceFailures() != 0 {
		t.Fatal("audit acceptance failed")
	}
	return out
}

type socket struct{ io.ReadWriter }

func (h *credentialHost) socket(t *testing.T, headers http.Header) *socket {
	t.Helper()
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("Origin", h.http.URL)
	if headers.Get("Cookie") == "" {
		headers.Set("Cookie", h.cookie())
	}
	dialer := ws.Dialer{Timeout: 3 * time.Second, Header: ws.HandshakeHeaderHTTP(headers)}
	conn, reader, _, err := dialer.Dial(t.Context(), "ws"+strings.TrimPrefix(h.http.URL, "http")+"/dwp")
	if conn != nil {
		t.Cleanup(func() { _ = conn.Close() })
	}
	if err != nil {
		t.Fatalf("handshake must upgrade before frame validation: %v", err)
	}
	if deadlineErr := conn.SetDeadline(time.Now().Add(3 * time.Second)); deadlineErr != nil {
		t.Fatal(deadlineErr)
	}
	var input io.ReadWriter = conn
	if reader != nil {
		input = struct {
			io.Reader
			io.Writer
		}{reader, conn}
	}
	return &socket{input}
}
func (s *socket) exchange(t *testing.T, frame dwp.Frame) dwp.Frame {
	t.Helper()
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	if writeErr := wsutil.WriteClientMessage(s, ws.OpText, raw); writeErr != nil {
		t.Fatal(writeErr)
	}
	raw, _, err = wsutil.ReadServerData(s)
	if err != nil {
		t.Fatalf("expected a protocol response after upgrade: %v", err)
	}
	var response dwp.Frame
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	if response.CorrelID != frame.ID {
		t.Fatal("unrelated protocol response")
	}
	return response
}
