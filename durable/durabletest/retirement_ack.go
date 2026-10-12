package durabletest

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

// RunRetirementCancellationAcknowledgment covers future acknowledgments even
// after the original parent and current child have both closed.
func RunRetirementCancellationAcknowledgment(t *testing.T, s durable.Store) {
	t.Helper()
	for _, childBuild := range []string{"a", "b"} {
		for _, cancellations := range []int{1, 2} {
			t.Run(fmt.Sprintf("child_%s/messages_%d", childBuild, cancellations), func(t *testing.T) { retirementCancellationAcknowledgment(t, s, childBuild, cancellations) })
		}
	}
}
func retirementCancellationAcknowledgment(t *testing.T, s durable.Store, childBuild string, cancellations int) {
	t.Helper()
	life, catalog := lifecycleCapabilities(t, s)
	ns := fmt.Sprintf("late-ack-%d", time.Now().UnixNano())
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: ns, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	parent := lifecycleStart(t, s, ns, "parent", "a")
	child := parent
	child.WorkflowID = "child"
	child.Queue = "child"
	child.BuildID = childBuild
	commit := func(start durable.StartRequest, r durable.CommitRequest) {
		t.Helper()
		task := lifecycleClaim(t, s, start)
		e, err := s.GetExecution(t.Context(), start.Key)
		if err != nil {
			t.Fatal(err)
		}
		r.Key = start.Key
		r.Token = task.Token()
		r.ExpectedRevision = e.Revision
		r.Events = []durable.EventInput{{Type: "decision"}}
		if _, err = s.CommitTransition(t.Context(), r); err != nil {
			t.Fatal(err)
		}
	}
	commit(parent, durable.CommitRequest{RequestID: "child", Children: []durable.ChildStartSpec{{CommandID: "child", Start: child, ParentQueue: parent.Queue, ParentClosePolicy: durable.ParentCloseAbandon}}, Tasks: []durable.TaskSpec{{ID: "cancel", Kind: durable.TaskWorkflow, Queue: parent.Queue}}})
	cancels := make([]durable.ChildCancellationSpec, 0, cancellations)
	for i := range cancellations {
		cancels = append(cancels, durable.ChildCancellationSpec{CommandID: fmt.Sprintf("cancel-%d", i), TargetID: "child"})
	}
	commit(parent, durable.CommitRequest{RequestID: "cancel", CancelChildren: cancels, Tasks: []durable.TaskSpec{{ID: "close", Kind: durable.TaskWorkflow, Queue: parent.Queue}}})
	commit(parent, durable.CommitRequest{RequestID: "close", State: durable.StateCompleted})
	commit(child, durable.CommitRequest{RequestID: "close", State: durable.StateCompleted})
	apply := func(build string) durable.ChildDelivery {
		t.Helper()
		d := claimChildMessage(t, s, ns, build, time.Minute)
		request := deliveryRequest(d)
		accepted, err := s.ApplyChildDelivery(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		replay, replayErr := s.ApplyChildDelivery(t.Context(), request)
		if replayErr != nil || !reflect.DeepEqual(accepted, replay) {
			t.Fatalf("delivery replay changed: %+v %v", replay, replayErr)
		}
		return *d
	}
	if childBuild != "a" {
		if d := apply("a"); d.Kind != durable.ChildDeliveryResult {
			t.Fatalf("expected original result: %+v", d)
		}
	}
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: ns}, BuildID: "a"}
	if _, err := life.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := life.BeginBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "begin", ExpectedEpoch: 1, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	final := durable.BuildRetirementRequest{BuildTarget: target, RequestID: "final", ExpectedEpoch: 2, ExpectedVersion: 2}
	facts, err := life.InspectBuildLifecycle(t.Context(), target)
	if err != nil || facts.Blockers.OpenExecutions != 0 || facts.Blockers.PendingTasks != 0 || facts.Blockers.ChildObligations != int64(cancellations) {
		t.Fatalf("future acknowledgments missing: %+v %v", facts, err)
	}
	blocked := func() {
		t.Helper()
		if _, finalErr := life.FinalizeBuildRetirement(t.Context(), final); !errors.Is(finalErr, durable.ErrRetirementBlocked) {
			t.Fatalf("premature finalize: %v", finalErr)
		}
	}
	blocked()
	if childBuild != "a" {
		for i := range cancellations {
			d := apply(childBuild)
			if d.Kind != durable.ChildDeliveryCancel || d.Message.CancellationID == "" {
				t.Fatalf("expected explicit cancel: %+v", d)
			}
			facts, err = life.InspectBuildLifecycle(t.Context(), target)
			if err != nil || facts.Blockers.ChildObligations != int64(cancellations-i-1) || facts.Blockers.PendingChildDeliveries != int64(i+1) {
				t.Fatalf("ack handoff gap: %+v %v", facts, err)
			}
			blocked()
		}
		for i := range cancellations {
			if d := apply("a"); d.Kind != durable.ChildDeliveryCancelAck {
				t.Fatalf("expected acknowledgment: %+v", d)
			}
			if i < cancellations-1 {
				blocked()
			}
		}
	} else {
		for i := range 1 + 2*cancellations {
			apply("a")
			if i < 2*cancellations {
				blocked()
			}
		}
	}
	facts, err = life.InspectBuildLifecycle(t.Context(), target)
	if err != nil || !facts.Blockers.Empty() {
		t.Fatalf("consumed acknowledgments still block: %+v %v", facts, err)
	}
	final.ExpectedVersion = queryRetirementFixture(t, s, target, final.ExpectedVersion)
	accepted, err := life.FinalizeBuildRetirement(t.Context(), final)
	if err != nil || accepted.Build.State != durable.BuildRetired {
		t.Fatalf("final: %+v %v", accepted, err)
	}
	replay, err := life.FinalizeBuildRetirement(t.Context(), final)
	if err != nil || !reflect.DeepEqual(accepted, replay) {
		t.Fatalf("final replay changed: %+v %v", replay, err)
	}
}
