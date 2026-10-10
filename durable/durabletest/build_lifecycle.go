package durabletest

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func lifecycleStart(t *testing.T, s durable.Store, namespace, workflow, build string) durable.StartRequest {
	t.Helper()
	r := durable.StartRequest{Key: durable.Key{Namespace: namespace, WorkflowID: workflow, RunID: "root"}, RequestID: "start", WorkflowType: "wf", BuildID: build, Queue: workflow}
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	return r
}
func lifecycleClaim(t *testing.T, s durable.Store, r durable.StartRequest) durable.Task {
	t.Helper()
	v, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, BuildID: r.BuildID, Queue: r.Queue, Kind: durable.TaskWorkflow, Owner: "owner", LeaseDuration: time.Minute})
	if err != nil || v == nil {
		t.Fatalf("claim %+v: %v", r.Key, err)
	}
	return *v
}

func RunBuildRetirement(t *testing.T, s durable.Store) {
	t.Helper()
	l, catalog := lifecycleCapabilities(t, s)
	ns := fmt.Sprintf("build-%d", time.Now().UnixNano())
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: ns}, BuildID: "a"}
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: ns, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	root := lifecycleStart(t, s, ns, "parent", "a")
	if _, err := l.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	b := target
	b.BuildID = "b"
	if _, err := l.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: b, RequestID: "register-b"}); err != nil {
		t.Fatal(err)
	}
	begin := durable.BuildRetirementRequest{BuildTarget: target, RequestID: "begin", ExpectedVersion: 1, ExpectedEpoch: 1}
	accepted, err := l.BeginBuildRetirement(t.Context(), begin)
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Build == nil || accepted.Build.State != durable.BuildRetiring || accepted.Build.Epoch != 2 || accepted.Build.CutoffEpoch != 1 {
		t.Fatalf("begin: %+v", accepted)
	}
	verifyLifecycleDelivery(t, s, accepted)
	blocked := root
	blocked.WorkflowID = "blocked"
	if _, err = s.StartExecution(t.Context(), blocked); !errors.Is(err, durable.ErrBuildAdmission) {
		t.Fatalf("new root admitted: %v", err)
	}
	if _, err = s.StartExecution(t.Context(), root); err != nil {
		t.Fatalf("accepted root replay failed: %v", err)
	}
	other := lifecycleStart(t, s, ns, "other", "b")
	otherTask := lifecycleClaim(t, s, other)
	incoming := durable.CommitRequest{Key: other.Key, RequestID: "incoming", ExpectedRevision: 1, Token: otherTask.Token(), Events: []durable.EventInput{{Type: "decision"}}, Children: []durable.ChildStartSpec{{CommandID: "incoming", Start: blocked, ParentQueue: other.Queue, ParentClosePolicy: durable.ParentCloseAbandon}}}
	if _, err = s.CommitTransition(t.Context(), incoming); !errors.Is(err, durable.ErrBuildAdmission) {
		t.Fatalf("incoming child: %v", err)
	}
	if e, readErr := s.GetExecution(t.Context(), other.Key); readErr != nil || e.Revision != 1 {
		t.Fatalf("refusal changed source: %+v %v", e, readErr)
	}
	task := lifecycleClaim(t, s, root)
	child := root
	child.WorkflowID = "inherited"
	child.Queue = "inherited"
	inherited := durable.CommitRequest{Key: root.Key, RequestID: "inherited", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "decision"}}, Children: []durable.ChildStartSpec{{CommandID: "inherited", Start: child, ParentQueue: root.Queue, ParentClosePolicy: durable.ParentCloseAbandon}}}
	if _, err = s.CommitTransition(t.Context(), inherited); err != nil {
		t.Fatalf("same-build child: %v", err)
	}
	if e, readErr := s.GetExecution(t.Context(), child.Key); readErr != nil || e.AdmissionEpoch != 0 {
		t.Fatalf("inherited epoch: %+v %v", e, readErr)
	}
	final := durable.BuildRetirementRequest{BuildTarget: target, RequestID: "final", ExpectedVersion: 2, ExpectedEpoch: 2}
	if _, err = l.FinalizeBuildRetirement(t.Context(), final); !errors.Is(err, durable.ErrRetirementBlocked) {
		t.Fatalf("unfinished finalized: %v", err)
	}
	facts, err := l.InspectBuildLifecycle(t.Context(), target)
	if err != nil || facts.Blockers.OpenExecutions != 2 || facts.Blockers.PendingTasks != 1 || facts.Blockers.ChildObligations != 1 {
		t.Fatalf("blockers: %+v %v", facts, err)
	}
	abort := final
	abort.RequestID = "abort"
	resumed, err := l.AbortBuildRetirement(t.Context(), abort)
	if err != nil || resumed.Build.Epoch != 3 || resumed.Build.State != durable.BuildAccepting {
		t.Fatalf("resume: %+v %v", resumed, err)
	}
	repeated, err := l.BeginBuildRetirement(t.Context(), begin)
	if err != nil || !reflect.DeepEqual(accepted, repeated) {
		t.Fatalf("receipt after resume: %+v %v", repeated, err)
	}
	newRoot := lifecycleStart(t, s, ns, "resumed", "a")
	if e, readErr := s.GetExecution(t.Context(), newRoot.Key); readErr != nil || e.AdmissionEpoch != 3 {
		t.Fatalf("new epoch: %+v %v", e, readErr)
	}
}

func RunRetiringContinuationLineage(t *testing.T, s durable.Store) {
	t.Helper()
	l, catalog := lifecycleCapabilities(t, s)
	ns := fmt.Sprintf("lineage-%d", time.Now().UnixNano())
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: ns}, BuildID: "a"}
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: ns, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	root := lifecycleStart(t, s, ns, "chain", "a")
	if _, err := l.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	b := target
	b.BuildID = "b"
	if _, err := l.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: b, RequestID: "register-b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.BeginBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "begin", ExpectedEpoch: 1, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	for i, build := range []string{"a", "b", "a"} {
		task := lifecycleClaim(t, s, root)
		e, err := s.GetExecution(t.Context(), root.Key)
		if err != nil {
			t.Fatal(err)
		}
		r := durable.CommitRequest{Key: root.Key, RequestID: "continue", ExpectedRevision: e.Revision, Token: task.Token(), Events: []durable.EventInput{{Type: "decision"}}, State: durable.StateContinuedAsNew, Continuation: &durable.ContinueSpec{RunID: fmt.Sprintf("next-%d", i), WorkflowType: root.WorkflowType, BuildID: build, Queue: root.Queue}}
		_, err = s.CommitTransition(t.Context(), r)
		if i == 2 {
			if !errors.Is(err, durable.ErrBuildAdmission) {
				t.Fatalf("A-B-A inherited incorrectly: %v", err)
			}
			after, readErr := s.GetExecution(t.Context(), root.Key)
			if readErr != nil || after.Revision != e.Revision || after.State != durable.StateRunning {
				t.Fatalf("refusal mutated: %+v %v", after, readErr)
			}
			return
		}
		if err != nil {
			t.Fatalf("handoff %d: %v", i, err)
		}
		root.RunID = r.Continuation.RunID
		root.BuildID = build
		if build == "a" {
			next, readErr := s.GetExecution(t.Context(), root.Key)
			if readErr != nil || next.AdmissionEpoch != 0 {
				t.Fatalf("same-build epoch: %+v %v", next, readErr)
			}
		}
	}
}

func RunRetirementLateChildBlocker(t *testing.T, s durable.Store) {
	t.Helper()
	l, catalog := lifecycleCapabilities(t, s)
	ns := fmt.Sprintf("late-%d", time.Now().UnixNano())
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: ns}, BuildID: "parent-build"}
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: ns, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	parent := lifecycleStart(t, s, ns, "parent", target.BuildID)
	task := lifecycleClaim(t, s, parent)
	child := durable.StartRequest{Key: durable.Key{Namespace: ns, WorkflowID: "child", RunID: "root"}, RequestID: "start", WorkflowType: "wf", BuildID: "child-build", Queue: "child"}
	r := durable.CommitRequest{Key: parent.Key, RequestID: "child", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "decision"}}, Tasks: []durable.TaskSpec{{ID: "resume", Kind: durable.TaskWorkflow, Queue: parent.Queue}}, Children: []durable.ChildStartSpec{{CommandID: "child", Start: child, ParentQueue: parent.Queue, ParentClosePolicy: durable.ParentCloseAbandon}}}
	if _, err := s.CommitTransition(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	// Enrollment must discover a historical child build even though this host
	// enrolled only the parent's namespace, with no artifact mapping supplied.
	enrolled, err := l.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1})
	if err != nil || enrolled.Enrollment.HistoricalBuildCount != 2 {
		t.Fatalf("historical child accounting: %+v %v", enrolled, err)
	}
	if _, err = l.BeginBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "begin", ExpectedEpoch: 1, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	closeLinkedExecution(t, s, parent, durable.StateCompleted)
	final := durable.BuildRetirementRequest{BuildTarget: target, RequestID: "final", ExpectedEpoch: 2, ExpectedVersion: 2}
	facts, err := l.InspectBuildLifecycle(t.Context(), target)
	if err != nil || facts.Blockers.OpenExecutions != 0 || facts.Blockers.PendingTasks != 0 || facts.Blockers.ChildObligations != 1 {
		t.Fatalf("late obligation missing: %+v %v", facts, err)
	}
	if _, err = l.FinalizeBuildRetirement(t.Context(), final); !errors.Is(err, durable.ErrRetirementBlocked) {
		t.Fatalf("late child finalized: %v", err)
	}
	closeLinkedExecution(t, s, child, durable.StateCompleted)
	facts, err = l.InspectBuildLifecycle(t.Context(), target)
	if err != nil || facts.Blockers.ChildObligations != 0 || facts.Blockers.PendingChildDeliveries != 1 || facts.ObservationVersion.BuildVersion != 2 {
		t.Fatalf("late delivery missing: %+v %v", facts, err)
	}
	if _, err = l.FinalizeBuildRetirement(t.Context(), final); !errors.Is(err, durable.ErrRetirementBlocked) {
		t.Fatalf("late delivery finalized: %v", err)
	}
	message := claimChildMessage(t, s, ns, parent.BuildID, time.Minute)
	if _, err = s.ApplyChildDelivery(t.Context(), deliveryRequest(message)); err != nil {
		t.Fatal(err)
	}
	accepted, err := l.FinalizeBuildRetirement(t.Context(), final)
	if err != nil || accepted.Build.State != durable.BuildRetired || accepted.Build.Version != 3 || accepted.Build.Epoch != 2 {
		t.Fatalf("finalize: %+v %v", accepted, err)
	}
	verifyLifecycleDelivery(t, s, accepted)
	repeated, err := l.FinalizeBuildRetirement(t.Context(), final)
	if err != nil || !reflect.DeepEqual(accepted, repeated) {
		t.Fatalf("final receipt changed: %+v %v", repeated, err)
	}
	if _, err = s.StartExecution(t.Context(), parent); err != nil {
		t.Fatalf("accepted root receipt lost after finalization: %v", err)
	}
	denied := parent
	denied.WorkflowID = "new-root"
	if _, err = s.StartExecution(t.Context(), denied); !errors.Is(err, durable.ErrBuildAdmission) {
		t.Fatalf("retired root admitted: %v", err)
	}
	if _, err = s.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: denied, Name: "wake"}); !errors.Is(err, durable.ErrBuildAdmission) {
		t.Fatalf("retired signal-start admitted: %v", err)
	}
	if _, err = s.GetExecution(t.Context(), denied.Key); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("refusal left root: %v", err)
	}
}

func lifecycleCapabilities(t *testing.T, s durable.Store) (durable.LifecycleStore, durable.NamespaceStore) {
	t.Helper()
	l, ok := s.(durable.LifecycleStore)
	if !ok {
		t.Fatal("lifecycle capability missing")
	}
	catalog, ok := s.(durable.NamespaceStore)
	if !ok {
		t.Fatal("catalog capability missing")
	}
	return l, catalog
}
