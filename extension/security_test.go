package extension_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	forgetesting "github.com/xraph/forge/testing"
	"github.com/xraph/vessel"
	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	wm "github.com/xraph/warden/store/memory"

	"github.com/xraph/dispatch/extension"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type operatorProvider struct{}

func (operatorProvider) Name() string                       { return "session" }
func (operatorProvider) Type() auth.SecuritySchemeType      { return auth.SecurityTypeHTTP }
func (operatorProvider) OpenAPIScheme() auth.SecurityScheme { return auth.SecurityScheme{} }
func (operatorProvider) Middleware() forge.Middleware       { return nil }
func (operatorProvider) Authenticate(context.Context, *http.Request) (*auth.AuthContext, error) {
	return &auth.AuthContext{Subject: "operator", Metadata: map[string]any{"credential_scheme": "bearer"}}, nil
}
func TestLazyHostSecurityRecoversAfterProviderAndWardenRegistration(t *testing.T) {
	app := forgetesting.NewTestApp("lazy-security", "1")
	reg := auth.NewRegistry(app.Container(), forge.NewNoopLogger())
	if err := vessel.Provide(app.Container(), func() auth.Registry { return reg }); err != nil {
		t.Fatal(err)
	}
	e := extension.New(extension.WithStore(memory.New()), extension.WithMemoryAuditForTesting(auditConfig()), extension.WithOperatorSecurity(extension.SecurityConfig{InstallationID: "installation", PolicyTenant: "policy-tenant"}))
	if err := e.Register(app); err != nil {
		t.Fatal(err)
	}
	read := func() int {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/dispatch/v1/stats", nil)
		req.Header.Set("Authorization", "Bearer explicit")
		rec := httptest.NewRecorder()
		app.Router().Handler().ServeHTTP(rec, req)
		return rec.Code
	}
	if code := read(); code != 401 {
		t.Fatal("missing provider", code)
	}
	if err := reg.Register(operatorProvider{}); err != nil {
		t.Fatal(err)
	}
	if code := read(); code != 503 {
		t.Fatal("missing Warden", code)
	}
	policyStore := wm.New()
	wardenEngine, err := warden.NewEngine(warden.WithStore(policyStore))
	if err != nil {
		t.Fatal(err)
	}
	if err := policyStore.CreatePolicy(t.Context(), &policy.Policy{ID: wid.NewPolicyID(), TenantID: "policy-tenant", Name: "explicit installation grant", IsActive: true, Effect: policy.EffectAllow, Subjects: []policy.SubjectMatch{{Kind: "user", ID: "operator"}}, Actions: []string{security.OperatorRead}, Resources: []string{"dispatch_installation:installation"}}); err != nil {
		t.Fatal(err)
	}
	if err := vessel.Provide(app.Container(), func() *warden.Engine { return wardenEngine }); err != nil {
		t.Fatal(err)
	}
	if code := read(); code != 503 {
		t.Fatal("audit activated before Start", code)
	}
	if err := e.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(context.Background()) })
	if code := read(); code != 200 {
		t.Fatal("late dependencies did not recover", code)
	}
}
