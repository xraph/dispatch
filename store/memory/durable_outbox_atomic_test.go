package memory

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func outboxStart(t *testing.T, s *Store, namespace string) durable.StartRequest {
	t.Helper()
	_, err := s.RegisterNamespace(t.Context(), durable.NamespaceConfig{Namespace: namespace, InstallationID: "host", AppID: "app", TenantID: "tenant", RequireAudit: true, RequireHooks: true, SchemaVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	r := durable.StartRequest{Key: durable.Key{Namespace: namespace, WorkflowID: "parent", RunID: "run"}, RequestID: "start", WorkflowType: "parent", BuildID: "v1", Queue: "q"}
	if _, err = s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	return r
}
func assertCandidateUnchanged(t *testing.T, s, before *Store, outbox map[string]durable.DeliveryRecord) {
	t.Helper()
	if !reflect.DeepEqual(s.executions, before.executions) || !reflect.DeepEqual(s.executionHeads, before.executionHeads) || !reflect.DeepEqual(s.childParents, before.childParents) || !reflect.DeepEqual(s.childDeliveries, before.childDeliveries) || !reflect.DeepEqual(s.childDeliveryReceipts, before.childDeliveryReceipts) || !reflect.DeepEqual(s.signalReceipts, before.signalReceipts) || !reflect.DeepEqual(s.cancellationReceipts, before.cancellationReceipts) || !reflect.DeepEqual(s.outbox, outbox) {
		t.Fatal("failed preparation published durable state")
	}
}
func TestOutboxAtomicLateChildAndDestination(t *testing.T) {
	for _, failure := range []string{"second_destination", "late_child"} {
		t.Run(failure, func(t *testing.T) {
			s := New()
			r := outboxStart(t, s, "test")
			other := outboxStart(t, s, "unrelated")
			foreign := s.executions[other.Key]
			task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
			if err != nil || task == nil {
				t.Fatal(err)
			}
			child := func(id string) durable.ChildStartSpec {
				return durable.ChildStartSpec{CommandID: id, Start: durable.StartRequest{Key: durable.Key{Namespace: r.Namespace, WorkflowID: id, RunID: "run"}, RequestID: "child-start", WorkflowType: "child", BuildID: "v1", Queue: "children"}, ParentQueue: r.Queue, ParentClosePolicy: durable.ParentCloseTerminate}
			}
			request := durable.CommitRequest{Key: r.Key, RequestID: "children", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "decision"}}, Children: []durable.ChildStartSpec{child("z-child-a"), child("z-child-b")}}
			before := s.durableCandidate(r.Namespace)
			outbox := map[string]durable.DeliveryRecord{}
			for k, v := range s.outbox {
				outbox[k] = v
			}
			injected := errors.New("injected preparation failure")
			count := 0
			s.outboxPrepare = func(d durable.Delivery) error {
				count++
				if failure == "second_destination" && d.Destination == durable.DestinationRelay || failure == "late_child" && d.WorkflowID == "z-child-b" {
					return injected
				}
				return nil
			}
			if _, err = s.CommitTransition(t.Context(), request); !errors.Is(err, injected) {
				t.Fatalf("failure was not exercised: %v", err)
			}
			if count < 2 {
				t.Fatalf("not a late failure: %d", count)
			}
			assertCandidateUnchanged(t, s, before, outbox)
			if s.executions[other.Key] != foreign {
				t.Fatal("unrelated namespace copied or changed")
			}
			s.outboxPrepare = nil
			if _, err = s.CommitTransition(t.Context(), request); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestOutboxHeartbeatRollback(t *testing.T) {
	s := New()
	r := outboxStart(t, s, "heartbeat")
	ctx := t.Context()
	workflow, err := s.ClaimTask(ctx, durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || workflow == nil {
		t.Fatal(err)
	}
	_, err = s.CommitTransition(ctx, durable.CommitRequest{Key: r.Key, RequestID: "schedule", ExpectedRevision: 1, Token: workflow.Token(), Events: []durable.EventInput{{Type: "scheduled"}}, Tasks: []durable.TaskSpec{{ID: "activity", Kind: durable.TaskActivity, Queue: "external"}}})
	if err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimTask(ctx, durable.ClaimRequest{Namespace: r.Namespace, Queue: "external", Kind: durable.TaskActivity, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatal(err)
	}
	_, err = s.CommitTransition(ctx, durable.CommitRequest{Key: r.Key, RequestID: "attempt", ExpectedRevision: 2, Token: task.Token(), Events: []durable.EventInput{{Type: "attempt.started"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep, DeadlineAfter: new(time.Minute), Heartbeat: &durable.HeartbeatConfig{}}})
	if err != nil {
		t.Fatal(err)
	}
	before := s.durableCandidate(r.Namespace)
	outbox := map[string]durable.DeliveryRecord{}
	for k, v := range s.outbox {
		outbox[k] = v
	}
	injected := errors.New("receipt intent unavailable")
	s.outboxPrepare = func(d durable.Delivery) error {
		if d.SourceKind == "execution_receipt" {
			return injected
		}
		return nil
	}
	request := durable.HeartbeatRequest{Key: r.Key, RequestID: "heartbeat", Token: task.Token(), Sequence: 1, Progress: []byte("private-progress"), LeaseDuration: time.Minute}
	if _, err = s.RecordHeartbeat(ctx, request); !errors.Is(err, injected) {
		t.Fatalf("heartbeat failure: %v", err)
	}
	assertCandidateUnchanged(t, s, before, outbox)
	s.outboxPrepare = nil
	if _, err = s.RecordHeartbeat(ctx, request); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range s.outbox {
		if entry.Delivery.Action == "activity.heartbeat" {
			found = true
		}
	}
	if !found {
		t.Fatal("heartbeat action not preserved")
	}
	count := len(s.outbox)
	if _, err = s.RecordHeartbeat(ctx, request); err != nil || len(s.outbox) != count {
		t.Fatalf("heartbeat replay: %v", err)
	}
}

func TestOutboxContinuationRollback(t *testing.T) {
	s := New()
	r := outboxStart(t, s, "continuation")
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatal(err)
	}
	request := durable.CommitRequest{Key: r.Key, RequestID: "continue", ExpectedRevision: 1, Token: task.Token(), State: durable.StateContinuedAsNew, Events: []durable.EventInput{{Type: "continued"}}, Continuation: &durable.ContinueSpec{RunID: "z-next", WorkflowType: r.WorkflowType, BuildID: r.BuildID, Queue: r.Queue}}
	before := s.durableCandidate(r.Namespace)
	outbox := map[string]durable.DeliveryRecord{}
	for k, v := range s.outbox {
		outbox[k] = v
	}
	injected := errors.New("successor intent failure")
	s.outboxPrepare = func(d durable.Delivery) error {
		if d.RunID == "z-next" {
			return injected
		}
		return nil
	}
	if _, err = s.CommitTransition(t.Context(), request); !errors.Is(err, injected) {
		t.Fatalf("successor failure: %v", err)
	}
	assertCandidateUnchanged(t, s, before, outbox)
	s.outboxPrepare = nil
	if _, err = s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
}
