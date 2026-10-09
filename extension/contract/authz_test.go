package contract

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/job"
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
		if (intent.Requires.Warden != WardenName || security.ContractOperation(intent.Name).Action == "") && (intent.Requires.Warden != DurableWardenName || DurableAction(intent.Name) == "") {
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

func TestContractDispatcherAuditRetainsConcreteActionsOnOutcomeFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("outcome-failure-", fail), func(t *testing.T) {
			s := memory.New()
			deps := contractDeps(t, s)
			n := durable.NamespaceConfig{InstallationID: "test", Namespace: "audit", AppID: "test", TenantID: "test", RequireAudit: true, SchemaVersion: 1}
			if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
				t.Fatal(err)
			}
			deps.Security.Audit = &security.AuditService{}
			var audit durable.OutboxStore = s
			if fail {
				audit = &failedContractOutcome{Store: s}
			}
			if err := deps.Security.Audit.Activate(t.Context(), audit, s, deps.Security.Resource, n.Namespace, false); err != nil {
				t.Fatal(err)
			}
			d := dispatcher.New(nil)
			if err := Register(d, fc.NewRegistry(), fc.NewWardenRegistry(), deps); err != nil {
				t.Fatal(err)
			}
			targets := map[string]string{}
			for _, command := range []string{"cancel", "retry"} {
				state := job.StatePending
				if command == "retry" {
					state = job.StateFailed
				}
				j := seedJob(t, deps, command, state, "", "", "default")
				intent := "jobs." + command
				targets["contract:"+intent] = "job:" + j.ID.String()
				payload, err := json.Marshal(IDInput{ID: j.ID.String()})
				if err != nil {
					t.Fatal(err)
				}
				_, _, err = d.Dispatch(t.Context(), fc.Request{Contributor: ContributorName, Intent: intent, IntentVersion: 1, Kind: fc.KindCommand, Payload: payload}, testPrincipal())
				if fail {
					if !errors.Is(err, fc.ErrUnavailable) {
						t.Fatal("outcome uncertainty not returned", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				got, err := s.GetJob(t.Context(), j.ID)
				if err != nil {
					t.Fatal(err)
				}
				want := job.StateCancelled
				if command == "retry" {
					want = job.StatePending
				}
				if got.State != want {
					t.Fatal("handler did not execute", got.State, want)
				}
			}
			status, err := s.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: n.InstallationID, Destination: durable.DestinationChronicle}, Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, record := range status.Records {
				a := record.Delivery
				if a.Target != targets[a.Action] {
					t.Fatal("lost concrete command identity", a)
				}
				counts[a.Action]++
			}
			want := 3
			if fail {
				want = 2
			}
			if counts["contract:jobs.cancel"] != want || counts["contract:jobs.retry"] != want {
				t.Fatal(counts)
			}
			// A fresh service observes the persisted unresolved markers without replaying commands.
			restarted := &security.AuditService{}
			if err = restarted.Activate(t.Context(), s, s, deps.Security.Resource, n.Namespace, false); err != nil {
				t.Fatal(err)
			}
			pending, err := s.UnresolvedLegacyAttempts(t.Context(), durable.LegacyAttemptList{InstallationID: n.InstallationID, Namespace: n.Namespace, Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			expected := 0
			if fail {
				expected = 2
			}
			if len(pending) != expected {
				t.Fatal(pending)
			}
			for _, record := range pending {
				if record.Attempt.Target != targets[record.Attempt.Action] {
					t.Fatal("restart marker lost target", record)
				}
			}
		})
	}
}

type failedContractOutcome struct{ *memory.Store }

func (s *failedContractOutcome) CompleteLegacyAttempt(context.Context, durable.LegacyOutcome) error {
	return errors.New("private persistence error")
}
