package contract

import (
	"context"
	"errors"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"

	"github.com/xraph/dispatch/durable"
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

func TestWardenAndHandlerAuditHaveIndependentIDs(t *testing.T) {
	s := memory.New()
	n := durable.NamespaceConfig{InstallationID: "test", Namespace: "audit", AppID: "test", TenantID: "test", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	deps := Deps{Security: testBoundary()}
	deps.Security.Audit = &security.AuditService{}
	if err := deps.Security.Audit.Activate(t.Context(), s, s, deps.Security.Resource, n.Namespace, false); err != nil {
		t.Fatal(err)
	}
	w := operatorWarden{deps: deps}
	if _, err := w.Authorize(t.Context(), testPrincipal(), fc.Action{Contributor: ContributorName, Intent: "jobs.counts", Kind: fc.KindQuery}); err != nil {
		t.Fatal(err)
	}
	handler := handle(deps, "jobs.counts", false, func(context.Context, struct{}, fc.Principal) (string, error) { return "read", nil })
	if _, err := handler(t.Context(), struct{}{}, testPrincipal()); err != nil {
		t.Fatal(err)
	}
	status, err := s.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "test", Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil || len(status.Records) != 2 || status.Records[0].Delivery.SourceID == status.Records[1].Delivery.SourceID {
		t.Fatal(status, err)
	}
	if err = deps.authorize(t.Context(), fc.Principal{}, "jobs.counts"); !errors.Is(err, fc.ErrUnauthenticated) {
		t.Fatal(err)
	}
	status, err = s.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "test", Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil || len(status.Records) != 3 {
		t.Fatal(status, err)
	}
}
