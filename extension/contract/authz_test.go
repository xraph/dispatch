package contract

import (
	"context"
	"errors"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"

	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

func TestEveryIntentRequiresWardenAndDirectDispatchDenies(t *testing.T) {
	deps := contractDeps(t, memory.New())
	deps.Security = security.Boundary{}
	reg, wreg := fc.NewRegistry(), fc.NewWardenRegistry()
	d := dispatcher.New(nil)
	if err := Register(d, reg, wreg, deps); err != nil {
		t.Fatal(err)
	}
	for _, intent := range loadManifest(t).Intents {
		if intent.Requires.Warden != WardenName || security.ContractOperation(intent.Name).Action == "" {
			t.Fatal("ungated intent", intent.Name)
		}
		kind := fc.KindQuery
		if intent.Kind == fc.IntentKindCommand {
			kind = fc.KindCommand
		}
		request := fc.Request{Contributor: ContributorName, Intent: intent.Name, IntentVersion: 1, Kind: kind, Payload: []byte(`{}`)}
		if _, _, err := d.Dispatch(context.Background(), request, fc.Principal{}); !errors.Is(err, fc.ErrUnauthenticated) {
			t.Fatalf("%s anonymous: %v", intent.Name, err)
		}
		if _, _, err := d.Dispatch(context.Background(), request, testPrincipal()); !errors.Is(err, fc.ErrUnavailable) {
			t.Fatalf("%s missing policy: %v", intent.Name, err)
		}
	}
}
func TestContractPolicyDenialErrorsPayloadAndForeignContributor(t *testing.T) {
	deps := contractDeps(t, memory.New())
	for _, policyErr := range []error{security.ErrForbidden, errors.New("secret backend")} {
		deps.Security.Authorizer = security.AuthorizerFunc(func(context.Context, security.Principal, string, security.Resource) error { return policyErr })
		if _, err := jobsListHandler(deps)(context.Background(), JobsListInput{}, testPrincipal()); err == nil {
			t.Fatal("direct handler bypass")
		}
	}
	deps.Security.Authorizer = security.AuthorizerFunc(func(_ context.Context, _ security.Principal, action string, _ security.Resource) error {
		if action == security.PayloadRead {
			return security.ErrForbidden
		}
		return nil
	})
	if _, err := jobsListHandler(deps)(context.Background(), JobsListInput{}, testPrincipal()); !errors.Is(err, fc.ErrPermissionDenied) {
		t.Fatal(err)
	}
	w := operatorWarden{deps: deps}
	for _, action := range []fc.Action{{Contributor: "foreign", Intent: "jobs.counts", Kind: fc.KindQuery}, {Contributor: ContributorName, Intent: "unknown", Kind: fc.KindQuery}, {Contributor: ContributorName, Intent: "jobs.cancel", Kind: fc.KindQuery}} {
		if decision, err := w.Authorize(context.Background(), testPrincipal(), action); err == nil || decision.Allow {
			t.Fatal(action)
		}
	}
}
