package operator

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/warden"
	"github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	wm "github.com/xraph/warden/store/memory"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

func TestWardenPersistedScopeAndObligations(t *testing.T) {
	for _, obligations := range [][]string{nil, {"require-mfa"}} {
		store := wm.New()
		if schemaErr := RegisterNamespaceSchema(t.Context(), store, durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: "install", Namespace: "namespace", AppID: "app", TenantID: "tenant", SchemaVersion: 1}}); schemaErr != nil {
			t.Fatal(schemaErr)
		}
		engine, err := warden.NewEngine(warden.WithStore(store))
		if err != nil {
			t.Fatal(err)
		}
		err = store.CreatePolicy(t.Context(), &policy.Policy{ID: id.NewPolicyID(), TenantID: "tenant", NamespacePath: "namespace", AppID: "app", Name: "namespace-reader", IsActive: true, Effect: policy.EffectAllow, Subjects: []policy.SubjectMatch{{Kind: "user", ID: "reader"}}, Actions: []string{ReadExecution}, Resources: []string{"dispatch_namespace:namespace"}, Obligations: obligations, Conditions: []policy.Condition{{Field: "resource.installation_id", Operator: policy.OpEquals, Value: "install"}, {Field: "resource.app_id", Operator: policy.OpEquals, Value: "app"}, {Field: "resource.tenant_id", Operator: policy.OpEquals, Value: "tenant"}, {Field: "resource.namespace", Operator: policy.OpEquals, Value: "namespace"}}})
		if err != nil {
			t.Fatal(err)
		}
		a := &WardenAuthorizer{Engine: func() (*warden.Engine, error) { return engine, nil }}
		r := Resource{InstallationID: "install", Namespace: "namespace", AppID: "app", TenantID: "tenant"}
		ctx := warden.WithNamespace(warden.WithTenant(context.Background(), "forged-app", "forged-tenant"), "forged")
		if err = a.Authorize(ctx, reader(), ReadExecution, r); (err == nil) != (len(obligations) == 0) {
			t.Fatalf("obligations=%v err=%v", obligations, err)
		}
		r.InstallationID = "foreign"
		if err = a.Authorize(ctx, reader(), ReadExecution, r); !errors.Is(err, security.ErrForbidden) {
			t.Fatal(err)
		}
		if err = a.Authorize(ctx, reader(), "unknown", r); !errors.Is(err, security.ErrForbidden) {
			t.Fatal(err)
		}
	}
}
