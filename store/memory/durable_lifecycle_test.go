package memory

import (
	"errors"
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestRetirementEnrollment(t *testing.T) { durabletest.RunRetirementEnrollment(t, New()) }

func TestRetirementEnrollmentIntentFailureRollsBack(t *testing.T) {
	s := New()
	n := durable.NamespaceConfig{InstallationID: "i", Namespace: "n", AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}
	if _, err := s.RegisterNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected intent failure")
	s.outboxPrepare = func(durable.Delivery) error { return failure }
	r := durable.RetirementEnrollmentRequest{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: "n"}, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}
	if _, err := s.EnrollRetirement(t.Context(), r); !errors.Is(err, failure) {
		t.Fatalf("intent: %v", err)
	}
	if len(s.retirementNamespaces) != 0 || len(s.lifecycleReceipts) != 0 || len(s.buildAdmissions) != 0 || len(s.outbox) != 0 {
		t.Fatal("partial enrollment published")
	}
	s.outboxPrepare = nil
	if _, err := s.EnrollRetirement(t.Context(), r); err != nil {
		t.Fatal(err)
	}
}

func TestBuildRetirement(t *testing.T) { durabletest.RunBuildRetirement(t, New()) }
func TestRetiringContinuationLineage(t *testing.T) {
	durabletest.RunRetiringContinuationLineage(t, New())
}
func TestRetirementLateChildBlocker(t *testing.T) {
	durabletest.RunRetirementLateChildBlocker(t, New())
}

func TestWorkflowTaskDeferral(t *testing.T) { durabletest.RunWorkflowTaskDeferral(t, New()) }

func TestWorkflowTaskDeferralIntentRollback(t *testing.T) {
	s := New()
	durabletest.RunWorkflowTaskDeferralIntentRollback(t, s, func() func() {
		s.outboxPrepare = func(durable.Delivery) error { return errors.New("injected deferral intent failure") }
		return func() { s.outboxPrepare = nil }
	})
}
