package durabletest

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func childPair(t *testing.T, s durable.Store, policy durable.ParentClosePolicy) (durable.StartRequest, durable.ChildStartSpec) {
	t.Helper()
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	child := childSpec(r, "child")
	child.ParentClosePolicy = policy
	if _, err := s.CommitTransition(t.Context(), childCommit(r, task, child)); err != nil {
		t.Fatal(err)
	}
	return r, child
}

func closeLinkedExecution(t *testing.T, s durable.Store, r durable.StartRequest, state durable.State) durable.Receipt {
	t.Helper()
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, Kind: durable.TaskWorkflow, Owner: "close", BuildID: r.BuildID, LeaseDuration: time.Minute})
	if err != nil || task == nil || task.Key != r.Key {
		t.Fatalf("closure claim: %+v %v", task, err)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	output := []byte(nil)
	if state == durable.StateCompleted {
		output = []byte("child result")
	}
	receipt, err := s.CommitTransition(t.Context(), durable.CommitRequest{Key: r.Key, RequestID: "close", ExpectedRevision: e.Revision, Token: task.Token(), State: state, Output: output, Events: []durable.EventInput{{Type: "workflow." + string(state), Payload: output}}})
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func claimChildMessage(t *testing.T, s durable.Store, namespace, build string, ttl time.Duration) *durable.ChildDelivery {
	t.Helper()
	message, err := s.ClaimChildDelivery(t.Context(), durable.ChildDeliveryClaimRequest{Namespace: namespace, BuildID: build, Owner: "delivery-worker", LeaseDuration: ttl})
	if err != nil || message == nil {
		t.Fatalf("delivery claim: %+v %v", message, err)
	}
	return message
}

func deliveryRequest(message *durable.ChildDelivery) durable.ChildDeliveryRequest {
	return durable.ChildDeliveryRequest{Source: message.Source, DeliveryID: message.ID, RequestID: "apply", Owner: message.Owner, Epoch: message.Epoch}
}

func childResultDelivery(t *testing.T, s durable.Store) {
	for _, state := range []durable.State{durable.StateCompleted, durable.StateFailed, durable.StateCancelled, durable.StateTimedOut, durable.StateTerminated} {
		t.Run(string(state), func(t *testing.T) {
			parent, child := childPair(t, s, durable.ParentCloseAbandon)
			closeLinkedExecution(t, s, child.Start, state)
			messages, err := s.ListChildDeliveries(t.Context(), child.Start.Key, "", 10)
			if err != nil || len(messages) != 1 || messages[0].Kind != durable.ChildDeliveryResult || messages[0].Target != parent.Key || messages[0].Message.State != state {
				t.Fatalf("result outbox: %+v %v", messages, err)
			}
			if message, pollErr := s.ClaimChildDelivery(t.Context(), durable.ChildDeliveryClaimRequest{Namespace: parent.Namespace, BuildID: "wrong", Owner: "wrong", LeaseDuration: time.Second}); pollErr != nil || message != nil {
				t.Fatalf("wrong build: %+v %v", message, pollErr)
			}
			message := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
			request := deliveryRequest(message)
			receipt, err := s.ApplyChildDelivery(t.Context(), request)
			if err != nil || receipt.Target != parent.Key || receipt.Disposition != durable.ChildDeliveryApplied || receipt.Revision != 3 {
				t.Fatalf("applied result: %+v %v", receipt, err)
			}
			if again, retryErr := s.ApplyChildDelivery(t.Context(), request); retryErr != nil || again != receipt {
				t.Fatalf("result retry: %+v %v", again, retryErr)
			}
			saved, err := s.GetChildDelivery(t.Context(), message.Source, message.ID)
			if err != nil || !saved.Done || saved.Disposition != durable.ChildDeliveryApplied {
				t.Fatalf("delivery state: %+v %v", saved, err)
			}
			events, err := s.ReadHistory(t.Context(), parent.Key, 3, 100)
			if err != nil || len(events) != 1 || events[0].Type != durable.EventChildCompleted {
				t.Fatalf("parent result history: %+v %v", events, err)
			}
			closeLinkedExecution(t, s, parent, durable.StateCompleted)
			if again, retryErr := s.ApplyChildDelivery(t.Context(), request); retryErr != nil || again != receipt {
				t.Fatalf("closed receipt: %+v %v", again, retryErr)
			}
			request.Epoch++
			if _, err = s.ApplyChildDelivery(t.Context(), request); !errors.Is(err, durable.ErrRequestConflict) {
				t.Fatalf("changed delivery retry: %v", err)
			}
		})
	}
}

func childParentCloseDelivery(t *testing.T, s durable.Store) {
	for _, policy := range []durable.ParentClosePolicy{durable.ParentCloseTerminate, durable.ParentCloseRequestCancel, durable.ParentCloseAbandon} {
		t.Run(string(policy), func(t *testing.T) {
			parent, child := childPair(t, s, policy)
			closeLinkedExecution(t, s, parent, durable.StateCompleted)
			messages, err := s.ListChildDeliveries(t.Context(), parent.Key, "", 10)
			wantCount := 1
			if policy == durable.ParentCloseAbandon {
				wantCount = 0
			}
			if err != nil || len(messages) != wantCount {
				t.Fatalf("policy outbox: %+v %v", messages, err)
			}
			if wantCount != 0 {
				message := claimChildMessage(t, s, parent.Namespace, child.Start.BuildID, time.Minute)
				if message.Kind != durable.ChildDeliveryClose || message.Message.Policy != policy {
					t.Fatalf("policy message: %+v", message)
				}
				if _, err = s.ApplyChildDelivery(t.Context(), deliveryRequest(message)); err != nil {
					t.Fatal(err)
				}
			}
			e, err := s.GetExecution(t.Context(), child.Start.Key)
			wantState := durable.StateRunning
			if policy == durable.ParentCloseTerminate {
				wantState = durable.StateTerminated
			}
			if err != nil || e.State != wantState {
				t.Fatalf("policy state: %+v %v", e, err)
			}
			if policy == durable.ParentCloseRequestCancel {
				events, readErr := s.ReadHistory(t.Context(), child.Start.Key, 1, 100)
				if readErr != nil || len(events) != 1 || events[0].Type != durable.EventCancellationRequested {
					t.Fatalf("cancel accepted: %+v %v", events, readErr)
				}
			}
			if policy != durable.ParentCloseTerminate {
				closeLinkedExecution(t, s, child.Start, durable.StateCompleted)
			}
			before, err := s.GetExecution(t.Context(), parent.Key)
			if err != nil {
				t.Fatal(err)
			}
			result := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
			receipt, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(result))
			if err != nil || receipt.Disposition != durable.ChildDeliveryIgnoredClosed {
				t.Fatalf("closed parent result: %+v %v", receipt, err)
			}
			after, err := s.GetExecution(t.Context(), parent.Key)
			if err != nil || after.Revision != before.Revision || after.LastSequence != before.LastSequence || after.State != before.State {
				t.Fatalf("closed parent reopened: %+v %v", after, err)
			}
		})
	}
}

func childDeliveryLease(t *testing.T, s durable.Store) {
	parent, child := childPair(t, s, durable.ParentCloseAbandon)
	closeLinkedExecution(t, s, child.Start, durable.StateCompleted)
	first := claimChildMessage(t, s, parent.Namespace, parent.BuildID, 20*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(first)); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("expired unreclaimed owner: %v", err)
	}
	second := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
	if second.ID != first.ID || second.Epoch != first.Epoch+1 || second.Attempt != first.Attempt+1 {
		t.Fatalf("reclaim: %+v -> %+v", first, second)
	}
	if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(first)); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("stale owner: %v", err)
	}
	if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(second)); err != nil {
		t.Fatal(err)
	}
}

func childExplicitCancellationDelivery(t *testing.T, s durable.Store) {
	parent, child := childPair(t, s, durable.ParentCloseAbandon)
	task := claim(t, s, parent, time.Minute)
	request := durable.CommitRequest{Key: parent.Key, RequestID: "request-child-cancel", ExpectedRevision: 2, Token: task.Token(), Events: []durable.EventInput{{Type: "cancel-child"}}, CancelChildren: []durable.ChildCancellationSpec{{CommandID: "cancel", TargetID: child.CommandID}}}
	if _, err := s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	cancel := claimChildMessage(t, s, parent.Namespace, child.Start.BuildID, time.Minute)
	if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(cancel)); err != nil {
		t.Fatal(err)
	}
	ack := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
	if ack.Kind != durable.ChildDeliveryCancelAck || ack.Message.CancellationID != "cancel" {
		t.Fatalf("cancel ack: %+v", ack)
	}
	if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(ack)); err != nil {
		t.Fatal(err)
	}
	events, err := s.ReadHistory(t.Context(), parent.Key, 4, 100)
	if err != nil || len(events) != 1 || events[0].Type != durable.EventChildCancellationAcknowledged {
		t.Fatalf("ack event: %+v %v", events, err)
	}
	e, err := s.GetExecution(t.Context(), child.Start.Key)
	if err != nil || e.State != durable.StateRunning {
		t.Fatalf("acceptance claimed closure: %+v %v", e, err)
	}
}

func childCancellationFence(t *testing.T, s durable.Store) {
	parent, existing := childPair(t, s, durable.ParentCloseAbandon)
	task := claim(t, s, parent, time.Minute)
	cleanup := childSpec(parent, "cleanup")
	r := durable.CommitRequest{Key: parent.Key, RequestID: "fence", ExpectedRevision: 2, Token: task.Token(), Events: []durable.EventInput{{Type: "fence"}}, CancelPendingTasks: true, Children: []durable.ChildStartSpec{cleanup}}
	if _, err := s.CommitTransition(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	messages, err := s.ListChildDeliveries(t.Context(), parent.Key, "", 10)
	if err != nil || len(messages) != 1 || messages[0].Target != existing.Start.Key || messages[0].Kind != durable.ChildDeliveryCancel {
		t.Fatalf("fence crossed cleanup boundary: %+v %v", messages, err)
	}
	message := claimChildMessage(t, s, parent.Namespace, existing.Start.BuildID, time.Minute)
	if _, err = s.ApplyChildDelivery(t.Context(), deliveryRequest(message)); err != nil {
		t.Fatal(err)
	}
	for _, child := range []durable.ChildStartSpec{existing, cleanup} {
		e, readErr := s.GetExecution(t.Context(), child.Start.Key)
		want := int64(1)
		if child.CommandID == existing.CommandID {
			want = 2
		}
		if readErr != nil || e.State != durable.StateRunning || e.Revision != want {
			t.Fatalf("fence lifecycle: %+v %v", e, readErr)
		}
	}
}

func childCancellationAtCreation(t *testing.T, s durable.Store) {
	parent := start(t, s)
	task := claim(t, s, parent, time.Minute)
	child := childSpec(parent, "child")
	request := childCommit(parent, task, child)
	request.CancelChildren = []durable.ChildCancellationSpec{{CommandID: "cancel", TargetID: child.CommandID}}
	if _, err := s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	message := claimChildMessage(t, s, parent.Namespace, child.Start.BuildID, time.Minute)
	if message.Message.CancellationID != "cancel" {
		t.Fatalf("missing same-decision cancellation: %+v", message)
	}
	if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(message)); err != nil {
		t.Fatal(err)
	}
}

func childCancellationConflict(t *testing.T, s durable.Store) {
	parent, child := childPair(t, s, durable.ParentCloseAbandon)
	task := claim(t, s, parent, time.Minute)
	r := durable.CommitRequest{Key: parent.Key, RequestID: "cancel", ExpectedRevision: 2, Token: task.Token(), Events: []durable.EventInput{{Type: "cancel"}}, CancelChildren: []durable.ChildCancellationSpec{{CommandID: "cancel", TargetID: "missing"}}, Tasks: []durable.TaskSpec{{ID: "next-cancel", Kind: durable.TaskWorkflow, Queue: parent.Queue}}}
	if _, err := s.CommitTransition(t.Context(), r); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("missing target: %v", err)
	}
	e, err := s.GetExecution(t.Context(), parent.Key)
	if err != nil || e.Revision != 2 {
		t.Fatalf("partial cancel: %+v %v", e, err)
	}
	r.CancelChildren[0].TargetID = child.CommandID
	if _, err = s.CommitTransition(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	task = claim(t, s, parent, time.Minute)
	r.RequestID = "repeat"
	r.ExpectedRevision = 3
	r.Token = task.Token()
	r.Tasks = nil
	if _, err = s.CommitTransition(t.Context(), r); !errors.Is(err, durable.ErrExists) {
		t.Fatalf("duplicate cancellation command: %v", err)
	}
	e, err = s.GetExecution(t.Context(), parent.Key)
	if err != nil || e.Revision != 3 {
		t.Fatalf("partial duplicate: %+v %v", e, err)
	}
}

func childCancellationReceiptCollision(t *testing.T, s durable.Store) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			parent, child := childPair(t, s, durable.ParentCloseRequestCancel)
			closeLinkedExecution(t, s, parent, durable.StateCompleted)
			d := claimChildMessage(t, s, parent.Namespace, child.Start.BuildID, time.Minute)
			cancellation := d.CancellationRequest()
			if conflict {
				cancellation.Reason = "different request"
			}
			accepted, err := s.RequestCancelExecution(t.Context(), cancellation)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(d))
			if conflict && !errors.Is(err, durable.ErrRequestConflict) || !conflict && (err != nil || got.Receipt != accepted.Receipt) {
				t.Fatalf("receipt collision: %+v %v", got, err)
			}
			childState, readErr := s.GetExecution(t.Context(), child.Start.Key)
			if readErr != nil || childState.Revision != 2 || childState.LastSequence != 2 {
				t.Fatalf("duplicate cancellation history: %+v %v", childState, readErr)
			}
			saved, readErr := s.GetChildDelivery(t.Context(), d.Source, d.ID)
			if readErr != nil || saved.Done == conflict {
				t.Fatalf("collision completion: %+v %v", saved, readErr)
			}
		})
	}
}

func childWinningResult(t *testing.T, s durable.Store) {
	parent, child := childPair(t, s, durable.ParentCloseAbandon)
	task := claim(t, s, parent, time.Minute)
	if _, err := s.CommitTransition(t.Context(), durable.CommitRequest{Key: parent.Key, RequestID: "cancel", Token: task.Token(), ExpectedRevision: 2, Events: []durable.EventInput{{Type: "cancel"}}, CancelChildren: []durable.ChildCancellationSpec{{CommandID: "cancel", TargetID: child.CommandID}}}); err != nil {
		t.Fatal(err)
	}
	closeLinkedExecution(t, s, child.Start, durable.StateCompleted)
	before, err := s.GetExecution(t.Context(), child.Start.Key)
	if err != nil {
		t.Fatal(err)
	}
	cancel := claimChildMessage(t, s, parent.Namespace, child.Start.BuildID, time.Minute)
	receipt, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(cancel))
	if err != nil || receipt.Disposition != durable.ChildDeliveryIgnoredClosed {
		t.Fatalf("winning result: %+v %v", receipt, err)
	}
	for range 2 {
		message := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
		if message.Kind == durable.ChildDeliveryCancelAck && message.Message.Disposition != durable.ChildDeliveryIgnoredClosed {
			t.Fatalf("wrong acceptance: %+v", message)
		}
		if _, err = s.ApplyChildDelivery(t.Context(), deliveryRequest(message)); err != nil {
			t.Fatal(err)
		}
	}
	after, err := s.GetExecution(t.Context(), child.Start.Key)
	if err != nil || after.Revision != before.Revision || after.State != durable.StateCompleted {
		t.Fatalf("winning child overwritten: %+v %v", after, err)
	}
	events, err := s.ReadHistory(t.Context(), parent.Key, 4, 100)
	if err != nil || len(events) != 2 {
		t.Fatalf("outcome and acknowledgment: %+v %v", events, err)
	}
}

func childDeliveryCopiesAndConcurrentRetry(t *testing.T, s durable.Store) {
	parent, child := childPair(t, s, durable.ParentCloseAbandon)
	closeLinkedExecution(t, s, child.Start, durable.StateCompleted)
	d := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
	d.Message.Output[0] = 'X'
	d.Message.CloseEvent.Payload[0] = 'X'
	saved, err := s.GetChildDelivery(t.Context(), d.Source, d.ID)
	if err != nil || string(saved.Message.Output) != "child result" || string(saved.Message.CloseEvent.Payload) != "child result" {
		t.Fatalf("claim aliases: %+v %v", saved, err)
	}
	saved.Message.Output[0] = 'Y'
	page, err := s.ListChildDeliveries(t.Context(), d.Source, "", 1)
	if err != nil || len(page) != 1 || string(page[0].Message.Output) != "child result" {
		t.Fatalf("read aliases: %+v %v", page, err)
	}
	page[0].Message.Output[0] = 'Z'
	page, err = s.ListChildDeliveries(t.Context(), d.Source, d.ID, 1)
	if err != nil || len(page) != 0 {
		t.Fatalf("exclusive cursor: %+v %v", page, err)
	}
	other := d.Source
	other.Namespace += "-other"
	if _, err = s.GetChildDelivery(t.Context(), other, d.ID); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("namespace read: %v", err)
	}
	if got, pollErr := s.ClaimChildDelivery(t.Context(), durable.ChildDeliveryClaimRequest{Namespace: other.Namespace, Owner: "other", LeaseDuration: time.Minute}); pollErr != nil || got != nil {
		t.Fatalf("namespace poll: %+v %v", got, pollErr)
	}
	var wg sync.WaitGroup
	results := make(chan durable.ChildDeliveryReceipt, 8)
	failures := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			got, applyErr := s.ApplyChildDelivery(t.Context(), deliveryRequest(d))
			results <- got
			failures <- applyErr
		})
	}
	wg.Wait()
	close(results)
	close(failures)
	var first durable.ChildDeliveryReceipt
	for result := range results {
		if first.Revision == 0 {
			first = result
		}
		if result != first {
			t.Fatalf("changed retry: %+v %+v", first, result)
		}
	}
	for failure := range failures {
		if failure != nil {
			t.Fatal(failure)
		}
	}
	request := deliveryRequest(d)
	request.RequestID = "new-request"
	if _, err = s.ApplyChildDelivery(t.Context(), request); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("completed delivery reused: %v", err)
	}
	events, err := s.ReadHistory(t.Context(), parent.Key, 3, 100)
	if err != nil || len(events) != 1 {
		t.Fatalf("duplicate result: %+v %v", events, err)
	}
	var outcome durable.ChildMessage
	if err = json.Unmarshal(events[0].Payload, &outcome); err != nil || string(outcome.Output) != "child result" || string(outcome.CloseEvent.Payload) != "child result" {
		t.Fatalf("mutated delivered bytes: %+v %v", outcome, err)
	}
}

func childDeliveryCascade(t *testing.T, s durable.Store) {
	parent, child := childPair(t, s, durable.ParentCloseTerminate)
	childTask := claim(t, s, child.Start, time.Minute)
	grandchild := childSpec(child.Start, "grandchild")
	grandchild.Start.Queue = "grandchildren"
	grandchild.Start.BuildID = "grandchild-v1"
	if _, err := s.CommitTransition(t.Context(), childCommit(child.Start, childTask, grandchild)); err != nil {
		t.Fatal(err)
	}
	grandTask := claim(t, s, grandchild.Start, time.Minute)
	closeLinkedExecution(t, s, parent, durable.StateCompleted)
	for _, build := range []string{child.Start.BuildID, grandchild.Start.BuildID} {
		message := claimChildMessage(t, s, parent.Namespace, build, time.Minute)
		if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(message)); err != nil {
			t.Fatal(err)
		}
	}
	for _, start := range []durable.StartRequest{child.Start, grandchild.Start} {
		e, err := s.GetExecution(t.Context(), start.Key)
		if err != nil || e.State != durable.StateTerminated {
			t.Fatalf("cascade state: %+v %v", e, err)
		}
	}
	if _, err := s.CommitTransition(t.Context(), durable.CommitRequest{Key: grandchild.Start.Key, RequestID: "stale-grandchild", ExpectedRevision: 1, Token: grandTask.Token(), Events: []durable.EventInput{{Type: "late"}}}); !errors.Is(err, durable.ErrClosed) {
		t.Fatalf("terminated grant accepted: %v", err)
	}
	for _, build := range []string{parent.BuildID, child.Start.BuildID} {
		result := claimChildMessage(t, s, parent.Namespace, build, time.Minute)
		receipt, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(result))
		if err != nil || receipt.Disposition != durable.ChildDeliveryIgnoredClosed {
			t.Fatalf("cascade result: %+v %v", receipt, err)
		}
	}
}
