package security

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/warden"
	"github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	memory "github.com/xraph/warden/store/memory"
)

func TestWardenInstallationTenantAndObligations(t *testing.T) {
	for _, obligations := range [][]string{nil, {"require-mfa"}} {
		store := memory.New()
		eng, err := warden.NewEngine(warden.WithStore(store))
		if err != nil {
			t.Fatal(err)
		}
		err = store.CreatePolicy(context.Background(), &policy.Policy{ID: id.NewPolicyID(), TenantID: "policy-tenant", Name: "operator", IsActive: true, Effect: policy.EffectAllow, Actions: []string{OperatorRead}, Resources: []string{"dispatch_installation:installation"}, Obligations: obligations})
		if err != nil {
			t.Fatal(err)
		}
		adapter := &WardenAuthorizer{Engine: func() (*warden.Engine, error) { return eng, nil }}
		err = adapter.Authorize(warden.WithNamespace(warden.WithTenant(context.Background(), "foreign-app", "foreign-tenant"), "foreign-namespace"), Principal{Subject: "operator", Kind: "user"}, OperatorRead, Resource{InstallationID: "installation", PolicyTenant: "policy-tenant"})
		if (err == nil) != (len(obligations) == 0) {
			t.Fatalf("obligations %v: %v", obligations, err)
		}
		if err := adapter.Authorize(context.Background(), Principal{Subject: "operator", Kind: "user"}, OperatorRead, Resource{InstallationID: "foreign", PolicyTenant: "policy-tenant"}); !errors.Is(err, ErrForbidden) {
			t.Fatal(err)
		}
	}
	if err := (&WardenAuthorizer{}).Authorize(context.Background(), Principal{}, OperatorRead, Resource{}); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}
