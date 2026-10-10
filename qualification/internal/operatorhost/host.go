// Package operatorhost composes the published durable contract for local qualification.
package operatorhost

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"time"

	"github.com/xraph/authsome"
	"github.com/xraph/authsome/app"
	aid "github.com/xraph/authsome/id"
	authmw "github.com/xraph/authsome/middleware"
	authmem "github.com/xraph/authsome/store/memory"
	"github.com/xraph/authsome/user"
	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	"github.com/xraph/forge/extensions/dashboard"
	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/idempotency"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"
	dashsecurity "github.com/xraph/forge/extensions/dashboard/security"
	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	wmem "github.com/xraph/warden/store/memory"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/api"
	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/engine"
	dc "github.com/xraph/dispatch/extension/contract"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/security"
	ds "github.com/xraph/dispatch/store"
)

type Store interface {
	ds.Store
	durable.Store
	durable.ReadStore
	durable.NamespaceStore
	durable.OutboxStore
}

type Credential struct {
	Subject string `json:"subject"`
	Token   string `json:"token"`
}
type Host struct {
	LegacyPolicy    wid.PolicyID
	authStore       *authmem.Store
	machineProvider *authority.Provider
	Handler         http.Handler
	Credentials     map[string]Credential
	Store           Store
	Auth            *authsome.Engine
	Policies        *wmem.Store
	ReaderPolicy    wid.PolicyID
	CommandPolicies map[string]wid.PolicyID
	Machine         authority.Credential
	runtime         commandRuntime
	engine          *engine.Engine
	audit           security.Boundary
	lifecycle       *lifecycleHost
}

// New provisions real Authsome sessions and Warden grants for two namespaces.
// The host closes the injected store. Credentials are private fixture material.
func New(ctx context.Context, store Store) (host *Host, returnErr error) {
	return newHost(ctx, store, nil)
}

// NewWithLifecycle enables trusted process and retained-query qualification.
// The supplied policies and identity belong to the host, not an HTTP request.
func NewWithLifecycle(ctx context.Context, store Store, options LifecycleOptions) (*Host, error) {
	return newHost(ctx, store, &options)
}

func newHost(ctx context.Context, store Store, options *LifecycleOptions) (host *Host, returnErr error) {
	h := &Host{Store: store, Credentials: map[string]Credential{}}
	defer func() {
		if returnErr != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = h.Close(cleanup)
		}
	}()
	if err := store.Migrate(ctx); err != nil {
		return nil, err
	}
	appID := aid.NewAppID()
	if record, err := store.GetNamespace(ctx, "operator-host", "production"); err == nil {
		appID, err = aid.ParseAppID(record.AppID)
		if err != nil {
			return nil, err
		}
	} else if !errors.Is(err, durable.ErrNotFound) {
		return nil, err
	}
	authStore := authmem.New()
	h.authStore = authStore
	now := time.Now().UTC()
	if err := authStore.CreateApp(ctx, &app.App{ID: appID, Name: "Operator qualification", Slug: "operator-host", IsPlatform: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		return nil, err
	}
	policies := wmem.New()
	w, err := warden.NewEngine(warden.WithStore(policies))
	if err != nil {
		return nil, err
	}
	encryptionKey := make([]byte, 32)
	if _, keyErr := rand.Read(encryptionKey); keyErr != nil {
		return nil, keyErr
	}
	registry := auth.NewRegistry(nil, forge.NewNoopLogger())
	authEngine, err := authority.NewEncrypted(authStore, w, appID.String(), registry, encryptionKey)
	if err != nil {
		return nil, err
	}
	h.Auth, h.Policies = authEngine, policies
	if startErr := authEngine.Start(ctx); startErr != nil {
		return nil, startErr
	}
	for _, role := range []string{"reader", "payload", "denied", "commander"} {
		u := &user.User{ID: aid.NewUserID(), AppID: appID, Email: role + "@operator.example.test", EmailVerified: true, CreatedAt: now, UpdatedAt: now}
		if createErr := authStore.CreateUser(ctx, u); createErr != nil {
			return nil, createErr
		}
		session, issueErr := authEngine.IssueSession(ctx, &authsome.IssueSessionRequest{User: u, AppID: appID, AuthMethod: "qualification", IPAddress: "127.0.0.1"})
		if issueErr != nil {
			return nil, issueErr
		}
		if session == nil || session.Session == nil || session.Session.Token == "" {
			return nil, errors.New("operator fixture: session issuance failed")
		}
		h.Credentials[role] = Credential{Subject: u.ID.String(), Token: session.Session.Token}
		if role == "denied" {
			continue
		}
		actions := []string{operator.Discover, operator.ListExecutions, operator.ReadExecution, operator.ReadHistory, operator.ReadTasks, operator.ReadChain, operator.ReadAudit, operator.ReadHooks}
		if role == "payload" || role == "commander" {
			actions = append(actions, operator.ReadPayload)
		}
		p := &policy.Policy{ID: wid.NewPolicyID(), TenantID: "tenant-production", NamespacePath: "production", AppID: appID.String(), Name: role, IsActive: true, Effect: policy.EffectAllow, Subjects: []policy.SubjectMatch{{Kind: "user", ID: u.ID.String()}}, Actions: actions, Resources: []string{"dispatch_namespace:production"}, Conditions: []policy.Condition{{Field: "resource.installation_id", Operator: policy.OpEquals, Value: "operator-host"}, {Field: "resource.app_id", Operator: policy.OpEquals, Value: appID.String()}, {Field: "resource.tenant_id", Operator: policy.OpEquals, Value: "tenant-production"}}}
		if policyErr := policies.CreatePolicy(ctx, p); policyErr != nil {
			return nil, policyErr
		}
		if role == "reader" {
			h.ReaderPolicy = p.ID
		}
	}
	for _, namespace := range []string{"audit", "production", "foreign"} {
		record, registerErr := store.RegisterNamespace(ctx, durable.NamespaceConfig{InstallationID: "operator-host", Namespace: namespace, AppID: appID.String(), TenantID: "tenant-" + namespace, RequireAudit: true, RequireHooks: true, SchemaVersion: 1})
		if registerErr != nil {
			return nil, registerErr
		}
		if schemaErr := operator.RegisterNamespaceSchema(ctx, policies, record); schemaErr != nil {
			return nil, schemaErr
		}
	}
	h.audit = security.Boundary{Resource: security.Resource{InstallationID: "operator-host", PolicyTenant: "tenant-audit"}, Authorizer: &security.WardenAuthorizer{Engine: func() (*warden.Engine, error) { return w, nil }}, Audit: &security.AuditService{}}
	if auditErr := h.audit.Audit.Activate(ctx, store, store, h.audit.Resource, "audit", false); auditErr != nil {
		return nil, auditErr
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		return nil, err
	}
	if options != nil {
		h.lifecycle, err = newLifecycleHost(h, *options)
		if err != nil {
			return nil, err
		}
	}
	if configureErr := h.configureCommands(ctx, appID, registry); configureErr != nil {
		return nil, configureErr
	}
	operatorOptions := operator.Options{Store: store, Reads: store, Catalog: store, InstallationID: "operator-host", Runtime: h.worker, Audit: h.audit, Authorizer: &operator.WardenAuthorizer{Engine: func() (*warden.Engine, error) { return w, nil }}, CursorKeys: operator.CursorKeys{Active: "fixture-v1", Keys: map[string][]byte{"fixture-v1": key}}}
	if h.lifecycle != nil {
		operatorOptions.BuildIdentity = h.lifecycle.buildIdentity
		operatorOptions.WorkerControl = h.lifecycle.workerControl
		operatorOptions.QueryHost = h.lifecycle
		operatorOptions.QueryRegistrationPolicy = h.lifecycle.options.RegistrationPolicy
	}
	operators, err := operator.New(operatorOptions)
	if err != nil {
		return nil, err
	}
	core, err := dispatch.New(dispatch.WithStore(store))
	if err != nil {
		return nil, err
	}
	h.engine, err = engine.Build(core)
	if err != nil {
		return nil, err
	}
	reg, wreg := fc.NewRegistry(), fc.NewWardenRegistry()
	contractDispatcher := dispatcher.NewWithOptions(nil, dispatcher.WithIdempotencyStore(dashboard.AdaptIdempotencyStore(idempotency.NewInMemoryStore())))
	if registerErr := dc.Register(contractDispatcher, reg, wreg, dc.Deps{Engine: h.engine, Store: store, Security: h.audit, Durable: operators}); registerErr != nil {
		return nil, registerErr
	}
	csrf := dashsecurity.NewCSRFManager()
	contract := transport.NewHandlerWithCSRF(reg, wreg, contractDispatcher, nil, csrf)
	router := forge.NewRouter()
	router.Use(authEngine.AuthMiddleware())
	router.Use(dashauth.ForgeMiddleware(contextUser{}))
	csrfHandler := transport.NewCSRFTokenHandler(csrf, 10*time.Minute)
	if err := router.GET("/api/dashboard/v1/csrf", func(c forge.Context) error { csrfHandler.ServeHTTP(c.Response(), c.Request()); return nil }); err != nil {
		return nil, err
	}
	if routeErr := router.POST("/api/dashboard/v1", func(c forge.Context) error { contract.ServeHTTP(c.Response(), c.Request()); return nil }); routeErr != nil {
		return nil, routeErr
	}
	authenticator := security.NewForgeAuthenticator(func() (auth.Registry, error) { return registry, nil }, "session", authority.ProviderName)
	callbacks := api.New(h.engine, nil, api.WithSecurity(authenticator, h.audit), api.WithDurableCallbacks(operators, authenticator)).Handler()
	for _, path := range []string{"/v1/durable/activities/complete", "/v1/durable/activities/heartbeat"} {
		if routeErr := router.POST(path, func(c forge.Context) error { callbacks.ServeHTTP(c.Response(), c.Request()); return nil }); routeErr != nil {
			return nil, routeErr
		}
	}
	h.Handler = router.Handler()
	if seedErr := seed(ctx, store); seedErr != nil {
		return nil, seedErr
	}
	return h, nil
}

// Authsome's real middleware already verified the session and DPoP binding.
// This adapter only translates that trusted user to Forge dashboard identity.
type contextUser struct{}

func (contextUser) CheckAuth(ctx context.Context, _ *http.Request) (*dashauth.UserInfo, error) {
	u, ok := authmw.UserFrom(ctx)
	if !ok || u == nil {
		return nil, nil
	}
	return &dashauth.UserInfo{Subject: u.ID.String(), Email: u.Email}, nil
}
func (h *Host) Close(ctx context.Context) error {
	h.audit.Audit.Deactivate()
	var storeErr, authErr error
	if h.engine != nil {
		storeErr = h.engine.Stop(ctx)
	} else if h.Store != nil {
		// Until Build succeeds, the partial host still owns store cleanup.
		storeErr = h.Store.Close()
	}
	if h.Auth != nil {
		authErr = h.Auth.Stop(ctx)
	}
	return errors.Join(storeErr, authErr)
}
