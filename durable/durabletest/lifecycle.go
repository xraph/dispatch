package durabletest

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

// RunRetirementEnrollment checks exact responses and historical admission accounting.
func RunRetirementEnrollment(t *testing.T, s durable.Store) {
	t.Helper()
	lifecycle, ok := s.(durable.RetirementEnrollmentStore)
	if !ok {
		t.Fatal("enrollment capability missing")
	}
	catalog, ok := s.(durable.NamespaceStore)
	if !ok {
		t.Fatal("namespace capability missing")
	}
	namespace := fmt.Sprintf("enroll-%d", time.Now().UnixNano())
	target := durable.NamespaceTarget{InstallationID: "retirement-test", Namespace: namespace}
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: target.InstallationID, Namespace: namespace, AppID: "app", TenantID: "tenant", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	for _, build := range []string{"old-a", "old-b", "old-a", strings.Repeat("z", durable.MaxIdentifierBytes)} {
		start := durable.StartRequest{Key: durable.Key{Namespace: namespace, WorkflowID: fmt.Sprintf("run-%d", time.Now().UnixNano()), RunID: "one"}, RequestID: "start", WorkflowType: "wf", BuildID: build, Queue: "q"}
		if _, err := s.StartExecution(t.Context(), start); err != nil {
			t.Fatal(err)
		}
	}
	facts, err := lifecycle.InspectCompatibility(t.Context(), target)
	if err != nil || facts.Enrolled {
		t.Fatalf("before: %+v %v", facts, err)
	}
	request := durable.RetirementEnrollmentRequest{NamespaceTarget: target, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}
	accepted, err := lifecycle.EnrollRetirement(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := durable.Fingerprint("retirement-historical-builds.v1", []string{"old-a", "old-b", strings.Repeat("z", durable.MaxIdentifierBytes)})
	if accepted.Enrollment == nil || accepted.Enrollment.HistoricalBuildCount != 3 || accepted.Enrollment.HistoricalBuildDigest != digest || !accepted.Enrollment.Compatibility.Enrolled || accepted.DeliveryID == "" {
		t.Fatalf("accepted: %+v", accepted)
	}
	verifyLifecycleDelivery(t, s, accepted)
	repeated, err := lifecycle.EnrollRetirement(t.Context(), request)
	if err != nil || !reflect.DeepEqual(accepted, repeated) {
		t.Fatalf("replay changed: %+v %v", repeated, err)
	}
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: target, Operation: accepted.Operation, RequestID: request.RequestID, RequestDigest: accepted.RequestDigest}
	recovered, err := lifecycle.LookupLifecycleReceipt(t.Context(), lookup)
	if err != nil || !reflect.DeepEqual(accepted, recovered) {
		t.Fatalf("lookup changed: %+v %v", recovered, err)
	}
	lookup.RequestDigest = "different"
	if _, err = lifecycle.LookupLifecycleReceipt(t.Context(), lookup); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed input: %v", err)
	}
	request.RequestID = "another"
	if _, err = lifecycle.EnrollRetirement(t.Context(), request); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("second enrollment: %v", err)
	}
	facts, err = lifecycle.InspectCompatibility(t.Context(), target)
	if err != nil || !facts.Enrolled || facts.WriterProtocol != 1 {
		t.Fatalf("after: %+v %v", facts, err)
	}
	target.InstallationID = "other"
	if _, err = lifecycle.InspectCompatibility(t.Context(), target); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("foreign target: %v", err)
	}
}
