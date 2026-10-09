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

func continueRequest(t *testing.T, s durable.Store, start durable.StartRequest, next string) durable.CommitRequest {
	t.Helper()
	task := claim(t, s, start, time.Minute)
	if task.Key != start.Key {
		t.Fatalf("wrong continuation source: %+v", task)
	}
	e, err := s.GetExecution(t.Context(), start.Key)
	if err != nil {
		t.Fatal(err)
	}
	r := completion(start, task)
	r.ExpectedRevision, r.State = e.Revision, durable.StateContinuedAsNew
	r.Continuation = &durable.ContinueSpec{RunID: next, WorkflowType: start.WorkflowType, BuildID: start.BuildID, Queue: start.Queue, Input: []byte("next input"), RunTimeout: start.RunTimeout}
	return r
}

func continuationRejection(t *testing.T, s durable.Store) {
	for _, mode := range []string{"cancelled", "unknown_signal", "out_of_order", "oversized", "missing_successor"} {
		t.Run(mode, func(t *testing.T) {
			r := start(t, s)
			request := continueRequest(t, s, r, "next")
			switch mode {
			case "cancelled":
				if _, err := s.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: r.Key, RequestID: "cancel", BuildID: r.BuildID}); err != nil {
					t.Fatal(err)
				}
			case "unknown_signal", "out_of_order", "oversized":
				count := 2
				input := []byte("message")
				if mode == "oversized" {
					count = 4
					input = make([]byte, 1<<20)
				}
				for i := 0; i < count; i++ {
					if _, err := s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: fmt.Sprint(i), BuildID: r.BuildID, Name: "message", Input: input}); err != nil {
						t.Fatal(err)
					}
				}
				if mode != "oversized" {
					id := "missing"
					if mode == "out_of_order" {
						id = "1"
					}
					payload, encodeErr := json.Marshal(durable.SignalConsumption{Version: 1, CommandID: "receive", SignalID: id})
					if encodeErr != nil {
						t.Fatal(encodeErr)
					}
					request.Events = []durable.EventInput{{Type: "workflow.command_scheduled", Payload: []byte(`{"version":1,"id":"receive","kind":"signal","name":"message"}`)}, {Type: durable.EventSignalConsumed, Payload: payload}}
				}
			case "missing_successor":
				request.Continuation = nil
			}
			before, err := s.GetExecution(t.Context(), r.Key)
			if err != nil {
				t.Fatal(err)
			}
			request.ExpectedRevision = before.Revision
			if _, err = s.CommitTransition(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("invalid %s accepted: %v", mode, err)
			}
			after, err := s.GetExecution(t.Context(), r.Key)
			if err != nil || after.State != durable.StateRunning || after.Revision != before.Revision || after.LastSequence != before.LastSequence || after.NextRunID != "" {
				t.Fatalf("rejection changed source: %+v %v", after, err)
			}
			key := r.Key
			key.RunID = "next"
			if _, err = s.GetExecution(t.Context(), key); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("rejection leaked successor: %v", err)
			}
			task, err := s.GetTask(t.Context(), r.Key, request.Token.TaskID)
			if err != nil || task.Done {
				t.Fatalf("rejection consumed grant: %+v %v", task, err)
			}
		})
	}
}

func continuationInputRace(t *testing.T, s durable.Store) {
	for _, mode := range []string{"signal", "cancel", "start", "continue"} {
		t.Run(mode, func(t *testing.T) {
			r := start(t, s)
			request := continueRequest(t, s, r, "next")
			var handoffErr, inputErr error
			var signal durable.SignalReceipt
			var cancel durable.CancelExecutionReceipt
			ready := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() { <-ready; _, handoffErr = s.CommitTransition(t.Context(), request) })
			wg.Go(func() {
				<-ready
				key := r.Key
				key.RunID = ""
				switch mode {
				case "signal":
					signal, inputErr = s.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "race", BuildID: r.BuildID, Name: "message"})
				case "cancel":
					cancel, inputErr = s.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: key, RequestID: "race", BuildID: r.BuildID})
				case "start":
					other := r
					other.RunID = "unrelated"
					_, inputErr = s.StartExecution(t.Context(), other)
				case "continue":
					other := request
					spec := *request.Continuation
					spec.RunID = "competitor"
					other.Continuation = &spec
					other.RequestID = "other"
					_, inputErr = s.CommitTransition(t.Context(), other)
				}
			})
			close(ready)
			wg.Wait()
			if mode == "start" {
				if !errors.Is(inputErr, durable.ErrExists) || handoffErr != nil {
					t.Fatalf("ownership gap: %v %v", inputErr, handoffErr)
				}
				return
			}
			if mode == "continue" {
				if (inputErr == nil) == (handoffErr == nil) {
					t.Fatalf("continuation winners: %v %v", inputErr, handoffErr)
				}
				e, err := s.ResolveExecution(t.Context(), targetFor(r.Key, durable.RunCurrent))
				if err != nil || (e.RunID != "next" && e.RunID != "competitor") {
					t.Fatalf("winner: %+v %v", e, err)
				}
				return
			}
			if inputErr != nil {
				t.Fatal(inputErr)
			}
			if handoffErr != nil {
				if !errors.Is(handoffErr, durable.ErrRevisionConflict) {
					t.Fatal(handoffErr)
				}
				e, err := s.GetExecution(t.Context(), r.Key)
				if err != nil {
					t.Fatal(err)
				}
				request.ExpectedRevision = e.Revision
				_, handoffErr = s.CommitTransition(t.Context(), request)
			}
			if mode == "cancel" {
				if cancel.Key == r.Key {
					if !errors.Is(handoffErr, durable.ErrInvalid) {
						t.Fatalf("cancellation bypass: %v", handoffErr)
					}
				} else if cancel.RunID != "next" || handoffErr != nil {
					t.Fatalf("raced cancellation: %+v %v", cancel, handoffErr)
				}
				return
			}
			if handoffErr != nil {
				t.Fatal(handoffErr)
			}
			key := r.Key
			key.RunID = "next"
			history, err := s.ReadHistory(t.Context(), key, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, e := range history {
				if e.Type == durable.EventSignalReceived || e.Type == durable.EventSignalCarried {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("signal lost/duplicated: %+v receipt %+v", history, signal)
			}
		})
	}
}

func continuationParentClose(t *testing.T, s durable.Store) {
	for _, policy := range []durable.ParentClosePolicy{durable.ParentCloseTerminate, durable.ParentCloseRequestCancel, durable.ParentCloseAbandon} {
		t.Run(string(policy), func(t *testing.T) {
			parent, child := childPair(t, s, policy)
			r := continueRequest(t, s, parent, "parent-next")
			if _, err := s.CommitTransition(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			key := parent.Key
			key.RunID = "parent-next"
			children, err := s.ListChildExecutions(t.Context(), key, "", 100)
			if err != nil || len(children) != 0 {
				t.Fatalf("successor adopted old children: %+v %v", children, err)
			}
			messages, err := s.ListChildDeliveries(t.Context(), parent.Key, "", 100)
			want := 1
			if policy == durable.ParentCloseAbandon {
				want = 0
			}
			if err != nil || len(messages) != want {
				t.Fatalf("parent continuation close policy: %+v %v", messages, err)
			}
			link, err := s.GetParentExecution(t.Context(), child.Start.Key)
			if err != nil || link.Parent != parent.Key {
				t.Fatalf("original parent changed: %+v %v", link, err)
			}
		})
	}
}

func continuationChildCancellation(t *testing.T, s durable.Store) {
	parent, child := childPair(t, s, durable.ParentCloseAbandon)
	r := continueRequest(t, s, child.Start, "child-next")
	if _, err := s.CommitTransition(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	request := continueRequest(t, s, parent, "unused")
	request.Continuation = nil
	request.State = ""
	request.CancelChildren = []durable.ChildCancellationSpec{{CommandID: "cancel-child", TargetID: child.CommandID}}
	if _, err := s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	d := claimChildMessage(t, s, parent.Namespace, child.Start.BuildID, time.Minute)
	result, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(d))
	next := child.Start.Key
	next.RunID = "child-next"
	if err != nil || result.Target != next || result.Disposition != durable.ChildDeliveryApplied {
		t.Fatalf("successor cancellation: %+v %v", result, err)
	}
	ack := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
	if ack.Kind != durable.ChildDeliveryCancelAck || ack.Source != child.Start.Key || ack.Message.Child != child.Start.Key {
		t.Fatalf("ack changed invocation: %+v", ack)
	}
	if _, err = s.ApplyChildDelivery(t.Context(), deliveryRequest(ack)); err != nil {
		t.Fatal(err)
	}
}

func continuationChildUnrelatedRoot(t *testing.T, s durable.Store) {
	parent, child := childPair(t, s, durable.ParentCloseTerminate)
	closeLinkedExecution(t, s, parent, durable.StateCompleted)
	request := continueRequest(t, s, child.Start, "child-next")
	if _, err := s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	closeMessage := claimChildMessage(t, s, parent.Namespace, child.Start.BuildID, time.Minute)
	next := child.Start
	next.RunID = "child-next"
	closeLinkedExecution(t, s, next, durable.StateCompleted)
	unrelated := child.Start
	unrelated.RunID = "unrelated"
	unrelated.RequestID = "unrelated-start"
	if _, err := s.StartExecution(t.Context(), unrelated); err != nil {
		t.Fatal(err)
	}
	receipt, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(closeMessage))
	if err != nil || receipt.Target != next.Key || receipt.Disposition != durable.ChildDeliveryIgnoredClosed {
		t.Fatalf("retargeted close: %+v %v", receipt, err)
	}
	e, err := s.GetExecution(t.Context(), unrelated.Key)
	if err != nil || e.State != durable.StateRunning || e.Revision != 1 {
		t.Fatalf("unrelated root affected: %+v %v", e, err)
	}
	link, err := s.GetChildExecution(t.Context(), parent.Key, child.CommandID)
	if err != nil || link.CurrentKey != next.Key || link.State != durable.StateCompleted {
		t.Fatalf("child relationship adopted unrelated root: %+v %v", link, err)
	}
}

func continuationChildTimeout(t *testing.T, s durable.Store) {
	parent, child := childPair(t, s, durable.ParentCloseAbandon)
	r := continueRequest(t, s, child.Start, "child-next")
	r.Continuation.RunTimeout = time.Microsecond
	if _, err := s.CommitTransition(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	grant := claimExecutionTimeout(t, s, child.Start, time.Minute)
	if grant.RunID != "child-next" {
		t.Fatalf("wrong timed out run: %+v", grant)
	}
	if _, err := s.ApplyExecutionTimeout(t.Context(), executionTimeoutRequest(grant)); err != nil {
		t.Fatal(err)
	}
	message := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
	if message.Message.Version != 2 || message.Message.FinalRun == nil || message.Message.FinalRun.RunTimeout != time.Microsecond || !message.Message.FinalRun.RunDeadlineAt.Equal(grant.DeadlineAt) || message.Message.State != durable.StateTimedOut {
		t.Fatalf("final run timeout metadata: %+v", message)
	}
	if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(message)); err != nil {
		t.Fatal(err)
	}
}

func continuationAtomic(t *testing.T, s durable.Store) {
	r := deadlineStart(t, time.Minute)
	r.ExecutionTimeout = time.Hour
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	request := continueRequest(t, s, r, "second")
	receipt, err := s.CommitTransition(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	old, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || old.State != durable.StateContinuedAsNew || old.NextRunID != "second" {
		t.Fatalf("source: %+v %v", old, err)
	}
	next := r
	next.RunID = "second"
	e, err := s.GetExecution(t.Context(), next.Key)
	if err != nil || e.FirstRunID != r.RunID || e.PreviousRunID != r.RunID || e.RunNumber != 2 || e.NextRunID != "" || e.RunTimeout != r.RunTimeout || !e.FirstStartedAt.Equal(before.CreatedAt) || !e.ExecutionDeadlineAt.Equal(before.ExecutionDeadlineAt) || string(e.Input) != "next input" {
		t.Fatalf("successor: %+v %v", e, err)
	}
	for _, selection := range []durable.RunSelection{durable.RunCurrent, durable.RunLatest} {
		assertTarget(t, s, r.Key, selection, next.Key, durable.StateRunning)
	}
	if task, taskErr := s.GetTask(t.Context(), r.Key, request.Token.TaskID); taskErr != nil || !task.Done {
		t.Fatalf("source task not fenced: %+v %v", task, taskErr)
	}
	request.Continuation.Input[0] = 'X'
	if _, err = s.CommitTransition(t.Context(), request); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed intent: %v", err)
	}
	request.Continuation.Input[0] = 'n'
	second := continueRequest(t, s, next, "third")
	if _, err = s.CommitTransition(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if got, retryErr := s.CommitTransition(t.Context(), request); retryErr != nil || got != receipt {
		t.Fatalf("old receipt after later handoff: %+v %v", got, retryErr)
	}
	third := next
	third.RunID = "third"
	conflict := continueRequest(t, s, third, r.RunID)
	if _, err = s.CommitTransition(t.Context(), conflict); !errors.Is(err, durable.ErrExists) {
		t.Fatalf("reused run: %v", err)
	}
	assertTarget(t, s, r.Key, durable.RunCurrent, third.Key, durable.StateRunning)
	conflict.Continuation.RunID = "fourth"
	if _, err = s.CommitTransition(t.Context(), conflict); err != nil {
		t.Fatalf("rejection consumed grant: %v", err)
	}
}

func continuationSignals(t *testing.T, s durable.Store) {
	r := start(t, s)
	accepted := make([]durable.SignalReceipt, 0, 2)
	for _, id := range []string{"one", "two"} {
		got, err := s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: id, BuildID: r.BuildID, Name: "message", Input: []byte(id)})
		if err != nil {
			t.Fatal(err)
		}
		accepted = append(accepted, got)
	}
	request := continueRequest(t, s, r, "next")
	request.Events = []durable.EventInput{{Type: "workflow.command_scheduled", Payload: []byte(`{"version":1,"index":1,"id":"receive","kind":"signal","name":"message"}`)}, {Type: durable.EventSignalConsumed, Payload: []byte(`{"version":1,"command_id":"receive","signal_id":"one"}`)}}
	if _, err := s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	next := r
	next.RunID = "next"
	history, err := s.ReadHistory(t.Context(), next.Key, 0, 1000)
	if err != nil || len(history) != 3 || history[2].Type != durable.EventSignalCarried {
		t.Fatalf("successor history: %+v %v", history, err)
	}
	var carried durable.CarriedSignal
	if err = json.Unmarshal(history[2].Payload, &carried); err != nil || carried.Signal.ID != "two" || carried.Source != r.Key || carried.Sequence != accepted[1].LastSequence || string(carried.Signal.Input) != "two" {
		t.Fatalf("carry provenance: %+v %v", carried, err)
	}
	if got, retryErr := s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: "two", BuildID: r.BuildID, Name: "message", Input: []byte("two")}); retryErr != nil || got != accepted[1] {
		t.Fatalf("acceptance receipt: %+v %v", got, retryErr)
	}
	second := continueRequest(t, s, next, "third")
	if _, err = s.CommitTransition(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	third := r.Key
	third.RunID = "third"
	history, err = s.ReadHistory(t.Context(), third, 0, 1000)
	if err != nil || len(history) != 3 {
		t.Fatalf("second carry: %+v %v", history, err)
	}
	var again durable.CarriedSignal
	if err = json.Unmarshal(history[2].Payload, &again); err != nil || again.Source != r.Key || again.Sequence != carried.Sequence || again.Signal.ID != "two" {
		t.Fatalf("original provenance lost: %+v %v", again, err)
	}
}

func continuationChildren(t *testing.T, s durable.Store) {
	parent, child := childPair(t, s, durable.ParentCloseAbandon)
	r := continueRequest(t, s, child.Start, "child-next")
	if _, err := s.CommitTransition(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if outbox, err := s.ListChildDeliveries(t.Context(), child.Start.Key, "", 100); err != nil || len(outbox) != 0 {
		t.Fatalf("intermediate child result: %+v %v", outbox, err)
	}
	next := child.Start
	next.RunID = "child-next"
	for _, key := range []durable.Key{child.Start.Key, next.Key} {
		link, err := s.GetParentExecution(t.Context(), key)
		if err != nil || link.Parent != parent.Key || link.Start.Key != child.Start.Key || link.CurrentKey != next.Key || link.State != durable.StateRunning {
			t.Fatalf("original/current child: %+v %v", link, err)
		}
	}
	closeLinkedExecution(t, s, next, durable.StateCompleted)
	message := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
	if message.Source != next.Key || message.Message.Child != child.Start.Key || message.Message.Version != 2 || message.Message.FinalRun == nil || message.Message.FinalRun.RunNumber != 2 {
		t.Fatalf("final child identity: %+v", message)
	}
	if receipt, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(message)); err != nil || receipt.Disposition != durable.ChildDeliveryApplied {
		t.Fatalf("final result: %+v %v", receipt, err)
	}
}

func continuationChildClose(t *testing.T, s durable.Store) {
	for _, policy := range []durable.ParentClosePolicy{durable.ParentCloseTerminate, durable.ParentCloseRequestCancel} {
		for _, order := range []string{"close_first", "continue_first"} {
			t.Run(string(policy)+"/"+order, func(t *testing.T) {
				parent, child := childPair(t, s, policy)
				if order == "close_first" {
					closeLinkedExecution(t, s, parent, durable.StateCompleted)
				}
				r := continueRequest(t, s, child.Start, "successor")
				r.Continuation.BuildID, r.Continuation.Queue = "v2", "new-queue"
				if _, err := s.CommitTransition(t.Context(), r); err != nil {
					t.Fatal(err)
				}
				if order == "continue_first" {
					closeLinkedExecution(t, s, parent, durable.StateCompleted)
				}
				d := claimChildMessage(t, s, parent.Namespace, "v2", time.Minute)
				request := deliveryRequest(d)
				receipt, err := s.ApplyChildDelivery(t.Context(), request)
				key := child.Start.Key
				key.RunID = "successor"
				if err != nil || receipt.Target != key || receipt.Disposition != durable.ChildDeliveryApplied {
					t.Fatalf("chain close: %+v %v", receipt, err)
				}
				e, err := s.GetExecution(t.Context(), key)
				if err != nil {
					t.Fatal(err)
				}
				if policy == durable.ParentCloseTerminate && e.State != durable.StateTerminated {
					t.Fatalf("successor not terminated: %+v", e)
				}
				if policy == durable.ParentCloseRequestCancel {
					next := child.Start
					next.Key, next.BuildID, next.Queue = key, "v2", "new-queue"
					blocked := continueRequest(t, s, next, "later")
					if _, err = s.CommitTransition(t.Context(), blocked); !errors.Is(err, durable.ErrInvalid) {
						t.Fatalf("accepted cancellation bypassed: %v", err)
					}
				}
				if again, retryErr := s.ApplyChildDelivery(t.Context(), request); retryErr != nil || again != receipt {
					t.Fatalf("chain close receipt: %+v %v", again, retryErr)
				}
			})
		}
	}
}
