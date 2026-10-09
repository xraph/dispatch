package authority

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xraph/authsome/apikey"
	"github.com/xraph/authsome/app"
	"github.com/xraph/authsome/environment"
	aid "github.com/xraph/authsome/id"
	"github.com/xraph/authsome/principal"
	"github.com/xraph/authsome/serviceaccount"
	am "github.com/xraph/authsome/store/memory"
	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	wm "github.com/xraph/warden/store/memory"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

func TestMachinePersistedAuthority(t *testing.T) {
	for _, name := range []string{"valid", "revoked", "expired-key", "inactive", "expired-account", "missing-account", "wrong-app", "wrong-kind", "wrong-org", "wrong-env", "scope-growth", "human-key", "wrong-config-binding", "cookie-only", "forged-app", "duplicate-authorization"} {
		t.Run(name, func(t *testing.T) {
			st := am.New()
			policies := wm.New()
			w, err := warden.NewEngine(warden.WithStore(policies))
			if err != nil {
				t.Fatal(err)
			}
			appID := aid.NewAppID()
			now := time.Now()
			if err = st.CreateApp(t.Context(), &app.App{ID: appID, Name: "Machine", Slug: "machine", IsPlatform: true, CreatedAt: now, UpdatedAt: now}); err != nil {
				t.Fatal(err)
			}
			engine, err := New(st, w, appID.String(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = engine.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if stopErr := engine.Stop(context.Background()); stopErr != nil {
					t.Error(stopErr)
				}
			})
			env := &environment.Environment{ID: aid.NewEnvironmentID(), AppID: appID, Name: "Test", Slug: "test", Type: environment.TypeDevelopment}
			if err = engine.CreateEnvironment(t.Context(), env); err != nil {
				t.Fatal(err)
			}
			account, err := engine.CreateServiceAccountInEnvironment(t.Context(), appID, env.ID, "sink", "", []string{"accept"})
			if err != nil {
				t.Fatal(err)
			}
			key, secret, err := engine.CreateServiceAccountAPIKey(t.Context(), account.ID, "sink", []string{"accept"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			provider := Provider{Engine: engine, EnvironmentID: env.ID.String(), Binding: ecosystem.Binding{Producer: "dispatch", InstallationID: "test", Namespace: "n", AppID: appID.String(), OrgID: "trusted-org", TenantID: "tenant"}, Credentials: []Credential{{KeyID: key.ID, AccountID: account.ID}}}
			request := httptest.NewRequestWithContext(t.Context(), "POST", "http://localhost/accept", nil)
			request.Header.Set("Authorization", "Bearer "+secret)
			expired := now.Add(-time.Hour)
			switch name {
			case "revoked":
				key.Revoked = true
			case "expired-key":
				key.ExpiresAt = &expired
			case "inactive":
				account.Active = false
			case "expired-account":
				account.ExpiresAt = &expired
			case "wrong-app":
				account.AppID = aid.NewAppID()
			case "wrong-kind":
				account.Kind = principal.KindAgent
			case "wrong-org":
				account.OrgID = aid.NewOrgID()
			case "wrong-env":
				account.EnvID = aid.NewEnvironmentID()
			case "scope-growth":
				key.Scopes = append(key.Scopes, "other")
			case "human-key":
				key.UserID = aid.NewUserID()
				key.ServiceAccountID = aid.ServiceAccountID{}
			case "wrong-config-binding":
				provider.Credentials[0].AccountID = aid.NewServiceAccountID()
			case "cookie-only":
				request.Header.Del("Authorization")
				request.Header.Set("Cookie", "session="+secret)
			case "forged-app":
				request.Header.Set("X-App-ID", aid.NewAppID().String())
			case "duplicate-authorization":
				request.Header.Add("Authorization", "Bearer "+secret)
			}
			saveMachine(t, st, key, account)
			if name == "missing-account" {
				if err = st.DeleteServiceAccount(t.Context(), account.ID); err != nil {
					t.Fatal(err)
				}
			}
			identity, err := provider.Authenticate(t.Context(), request)
			if name == "valid" {
				if err != nil || identity == nil || identity.Subject != account.ID.String() || identity.Claims["principal_kind"] != "service_account" {
					t.Fatalf("valid machine refused: %v", err)
				}
			} else if err == nil || identity != nil {
				t.Fatal("invalid authority admitted")
			}
		})
	}
}
func saveMachine(t *testing.T, st *am.Store, key *apikey.APIKey, account *serviceaccount.ServiceAccount) {
	t.Helper()
	if err := st.UpdateAPIKey(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateServiceAccount(t.Context(), account); err != nil {
		t.Fatal(err)
	}
}
func TestDestinationWarden(t *testing.T) {
	for _, name := range []string{"allow", "deny", "obligation", "wrong-resource", "wrong-scope"} {
		t.Run(name, func(t *testing.T) {
			st := wm.New()
			w, err := warden.NewEngine(warden.WithStore(st))
			if err != nil {
				t.Fatal(err)
			}
			b := ecosystem.Binding{AppID: "app", InstallationID: "installation", TenantID: "tenant", OrgID: "org", Producer: "dispatch"}
			p := &policy.Policy{ID: wid.NewPolicyID(), AppID: b.AppID, TenantID: b.TenantID, Name: name, Effect: policy.EffectAllow, IsActive: true, Subjects: []policy.SubjectMatch{{Kind: "service_acct", ID: "service"}}, Actions: []string{"accept"}, Resources: []string{"dispatch_installation:installation"}}
			switch name {
			case "deny":
				p.Effect = policy.EffectDeny
			case "obligation":
				p.Obligations = []string{"require-mfa"}
			case "wrong-resource":
				p.Resources = []string{"dispatch_installation:other"}
			case "wrong-scope":
				p.TenantID = "other"
			}
			if err = st.CreatePolicy(t.Context(), p); err != nil {
				t.Fatal(err)
			}
			err = Authorize(t.Context(), w, b, b.TenantID, "service", "relay", "accept")
			if (err == nil) != (name == "allow") {
				t.Fatalf("decision: %v", err)
			}
		})
	}
}
