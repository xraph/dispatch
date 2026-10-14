package host

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xraph/authsome"
	"github.com/xraph/authsome/app"
	"github.com/xraph/authsome/environment"
	aid "github.com/xraph/authsome/id"
	am "github.com/xraph/authsome/store/memory"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	wm "github.com/xraph/warden/store/memory"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

// This exercises real Authsome and Warden decisions, with a synthetic already
// verified TLS state. It does not qualify the launcher proxy or TLS handshake.
func TestMachineAndChannelBoundary(t *testing.T) {
	for _, name := range []string{"allow", "plaintext", "forged-header", "unverified-chain", "wrong-tenant", "wrong-bearer", "policy-deny", "policy-obligation", "revoked-key"} {
		t.Run(name, func(t *testing.T) {
			st := am.New()
			policies := wm.New()
			w, e := warden.NewEngine(warden.WithStore(policies))
			if e != nil {
				t.Fatal(e)
			}
			appID := aid.NewAppID()
			now := time.Now()
			if e = st.CreateApp(t.Context(), &app.App{ID: appID, Name: "Admission", Slug: "admission", IsPlatform: true, CreatedAt: now, UpdatedAt: now}); e != nil {
				t.Fatal(e)
			}
			engine, e := authority.New(st, w, appID.String(), nil)
			if e != nil {
				t.Fatal(e)
			}
			if e = engine.Start(t.Context()); e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() {
				if stopErr := engine.Stop(context.Background()); stopErr != nil {
					t.Error(stopErr)
				}
			})
			env := &environment.Environment{ID: aid.NewEnvironmentID(), AppID: appID, Name: "Test", Slug: "test", Type: environment.TypeDevelopment}
			if e = engine.CreateEnvironment(t.Context(), env); e != nil {
				t.Fatal(e)
			}
			account, e := engine.CreateServiceAccountInEnvironment(t.Context(), appID, env.ID, "client", "", []string{"writerfloor.delegate"})
			if e != nil {
				t.Fatal(e)
			}
			key, secret, e := engine.CreateServiceAccountAPIKey(t.Context(), account.ID, "client", []string{"writerfloor.delegate"}, nil)
			if e != nil {
				t.Fatal(e)
			}
			provider := &authority.Provider{Engine: engine, EnvironmentID: env.ID.String(), Binding: ecosystem.Binding{Producer: "dispatch", InstallationID: "qualification", Namespace: "n", AppID: appID.String(), OrgID: "org", TenantID: "tenant"}, Credentials: []authority.Credential{{KeyID: key.ID, AccountID: account.ID}}}
			p := &policy.Policy{ID: wid.NewPolicyID(), AppID: appID.String(), TenantID: "tenant", Name: "delegate", Effect: policy.EffectAllow, IsActive: true, Subjects: []policy.SubjectMatch{{Kind: "service_acct", ID: account.ID.String()}}, Actions: []string{"writerfloor.delegate"}, Resources: []string{"writerfloor_authority:authority"}}
			if name == "policy-deny" {
				p.Effect = policy.EffectDeny
			}
			if name == "policy-obligation" {
				p.Obligations = []string{"require-mfa"}
			}
			if e = policies.CreatePolicy(t.Context(), p); e != nil {
				t.Fatal(e)
			}
			boundary, e := New(provider, w, boundaryRuntime(t, "authority"), "authority")
			if e != nil {
				t.Fatal(e)
			}
			r := httptest.NewRequestWithContext(t.Context(), "POST", "https://admission/delegate", nil)
			r.Header.Set("Authorization", "Bearer "+secret)
			leaf := &x509.Certificate{Raw: []byte("fixture-private-proxy-certificate")}
			r.TLS = &tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
			inv := admission.Invocation{Tenant: "tenant", Target: "target", Operation: "operation", Kind: "provider"}
			switch name {
			case "plaintext":
				r.TLS = nil
			case "forged-header":
				r.TLS = nil
				r.Header.Set("X-Workload-Certificate", managed.Digest(leaf.Raw))
			case "unverified-chain":
				r.TLS.VerifiedChains = nil
			case "wrong-tenant":
				inv.Tenant = "other"
			case "wrong-bearer":
				r.Header.Set("Authorization", "Bearer copied-invalid")
			case "revoked-key":
				key.Revoked = true
				if e = st.UpdateAPIKey(t.Context(), key); e != nil {
					t.Fatal(e)
				}
			}
			peer, e := boundary.Authenticate(t.Context(), r, inv)
			if name == "allow" {
				if e != nil || peer.Principal != account.ID.String() || peer.CertificateDigest != managed.Digest(leaf.Raw) {
					t.Fatalf("%+v %v", peer, e)
				}
			} else if e == nil {
				t.Fatal("invalid channel or authority admitted")
			}
		})
	}
}
func TestTLSConfigurationRefusesMissingTrust(t *testing.T) {
	if _, e := TLSConfig(tls.Certificate{}, nil); e == nil {
		t.Fatal("missing trust accepted")
	}
}

// These dependencies are constructor-only witnesses and must never grant admission.
type boundaryDependencies struct{}

func (boundaryDependencies) Observe(context.Context) (admission.Observation, error) {
	return admission.Observation{}, admission.ErrDenied
}
func (boundaryDependencies) Verify(context.Context, admission.Invocation) error {
	return admission.ErrDenied
}
func (boundaryDependencies) Dispatch(context.Context, admission.Delegation) error {
	return admission.ErrDenied
}
func boundaryRuntime(t *testing.T, authorityID string) *admission.Runtime {
	t.Helper()
	dependencies := boundaryDependencies{}
	runtime, err := admission.NewRuntime(&sql.DB{}, authorityID, dependencies, dependencies, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

type boundaryCheckerFunc func(context.Context, *warden.CheckRequest, ...warden.CallOption) (*warden.CheckResult, error)

func (f boundaryCheckerFunc) Check(ctx context.Context, request *warden.CheckRequest, options ...warden.CallOption) (*warden.CheckResult, error) {
	return f(ctx, request, options...)
}

type boundaryPointerChecker struct{}

func (*boundaryPointerChecker) Check(context.Context, *warden.CheckRequest, ...warden.CallOption) (*warden.CheckResult, error) {
	return nil, admission.ErrDenied
}
func TestBoundaryRejectsAuthorityMismatch(t *testing.T) {
	provider := &authority.Provider{Engine: &authsome.Engine{}}
	policy := &boundaryPointerChecker{}
	runtime := boundaryRuntime(t, "actual-authority")
	if _, err := New(provider, policy, runtime, "different-policy-authority"); !errors.Is(err, admission.ErrDenied) {
		t.Fatalf("mismatched authority accepted: %v", err)
	}
	boundary, err := New(provider, policy, runtime, "actual-authority")
	if err != nil {
		t.Fatal(err)
	}
	if boundary.authorityID != "actual-authority" {
		t.Fatal("policy authority differs from runtime")
	}
}
func TestBoundaryRejectsTypedNilChecker(t *testing.T) {
	provider := &authority.Provider{Engine: &authsome.Engine{}}
	runtime := boundaryRuntime(t, "authority")
	var function boundaryCheckerFunc
	var pointer *boundaryPointerChecker
	for _, checker := range []authority.Checker{nil, function, pointer} {
		if _, err := New(provider, checker, runtime, "authority"); !errors.Is(err, admission.ErrDenied) {
			t.Fatalf("nil checker accepted: %T: %v", checker, err)
		}
	}
}
