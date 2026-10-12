package durabletest

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func deadlineStart(t *testing.T, timeout time.Duration) durable.StartRequest {
	t.Helper()
	return durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: "run"}, RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "orders", RunTimeout: timeout, ExecutionTimeout: 2 * timeout}
}

func executionDeadline(t *testing.T, s durable.Store) {
	r := deadlineStart(t, 300*time.Millisecond)
	first, err := s.StartExecution(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	task := claim(t, s, r, time.Minute)
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	if !e.RunDeadlineAt.Equal(e.CreatedAt.Add(r.RunTimeout)) || !e.ExecutionDeadlineAt.Equal(e.CreatedAt.Add(r.ExecutionTimeout)) {
		t.Fatalf("deadlines: %+v", e)
	}
	signal := durable.SignalRequest{Key: r.Key, RequestID: "signal", BuildID: r.BuildID, Name: "go"}
	savedSignal, err := s.SignalExecution(t.Context(), signal)
	if err != nil {
		t.Fatal(err)
	}
	cancel := durable.CancelExecutionRequest{Key: r.Key, RequestID: "cancel", BuildID: r.BuildID, Reason: "stop"}
	savedCancel, err := s.RequestCancelExecution(t.Context(), cancel)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(r.ExecutionTimeout)
	if got, e := s.StartExecution(t.Context(), r); e != nil || got != first {
		t.Fatalf("start receipt: %+v %v", got, e)
	}
	if got, e := s.SignalExecution(t.Context(), signal); e != nil || got != savedSignal {
		t.Fatalf("signal receipt: %+v %v", got, e)
	}
	if got, e := s.RequestCancelExecution(t.Context(), cancel); e != nil || got != savedCancel {
		t.Fatalf("cancel receipt: %+v %v", got, e)
	}
	before, readErr := s.GetExecution(t.Context(), r.Key)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if _, err = s.RenewTask(t.Context(), r.Key, task.Token(), time.Minute); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("renew: %v", err)
	}
	commit := completion(r, task)
	commit.ExpectedRevision = before.Revision
	if _, err = s.CommitTransition(t.Context(), commit); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("commit: %v", err)
	}
	signal.RequestID = "late-signal"
	if _, err = s.SignalExecution(t.Context(), signal); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("signal: %v", err)
	}
	cancel.RequestID = "late-cancel"
	if _, err = s.RequestCancelExecution(t.Context(), cancel); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("cancel: %v", err)
	}
	replacement := r
	replacement.RunID = "replacement"
	replacement.RequestID = "late-signal-start"
	if _, err = s.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: replacement, Name: "go"}); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("signal start: %v", err)
	}
	if got, e := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, Kind: durable.TaskWorkflow, Owner: "late", LeaseDuration: time.Minute}); e != nil || got != nil {
		t.Fatalf("late claim: %+v %v", got, e)
	}
	after, readErr := s.GetExecution(t.Context(), r.Key)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("expired mutations changed projection: %+v", after)
	}
	changed := r
	changed.RunTimeout++
	if _, err = s.StartExecution(t.Context(), changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed start: %v", err)
	}
}

func executionDeadlineActivity(t *testing.T, s durable.Store) {
	r := deadlineStart(t, 300*time.Millisecond)
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	request := completion(r, claim(t, s, r, time.Minute))
	request.Tasks = []durable.TaskSpec{{ID: "activity", Kind: durable.TaskActivity, Queue: "effects", DeadlineAfter: 150 * time.Millisecond}}
	saved, err := s.CommitTransition(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: "effects", Kind: durable.TaskActivity, Owner: "effect", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatalf("activity: %+v %v", task, err)
	}
	hard := 150 * time.Millisecond
	activate := durable.CommitRequest{Key: r.Key, RequestID: "activate", ExpectedRevision: 2, Token: task.Token(), Events: []durable.EventInput{{Type: "attempt.started"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep, DeadlineAfter: &hard, LeaseDuration: time.Minute, Heartbeat: &durable.HeartbeatConfig{}}}
	if _, err = s.CommitTransition(t.Context(), activate); err != nil {
		t.Fatal(err)
	}
	hb := heartbeatRequest(r, *task, 1)
	receipt, err := s.RecordHeartbeat(t.Context(), hb)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(r.ExecutionTimeout)
	if again, e := s.CommitTransition(t.Context(), request); e != nil || again != saved {
		t.Fatalf("commit receipt: %+v %v", again, e)
	}
	if again, e := s.RecordHeartbeat(t.Context(), hb); e != nil || again != receipt {
		t.Fatalf("heartbeat receipt: %+v %v", again, e)
	}
	hb.RequestID = "late-heartbeat"
	hb.Sequence++
	if _, err = s.RecordHeartbeat(t.Context(), hb); !errors.Is(err, durable.ErrExecutionDeadline) {
		t.Fatalf("heartbeat: %v", err)
	}
	if got, e := s.ClaimTimeoutTask(t.Context(), durable.TimeoutClaimRequest{Namespace: r.Namespace, Owner: "timeout", LeaseDuration: time.Minute}); e != nil || got != nil {
		t.Fatalf("activity timeout under expired execution: %+v %v", got, e)
	}
}

func executionDeadlineCreation(t *testing.T, s durable.Store) {
	t.Run("signal", func(t *testing.T) {
		r := deadlineStart(t, time.Microsecond)
		request := durable.SignalWithStartRequest{Start: r, Name: "go"}
		receipt, err := s.SignalWithStart(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		e, err := s.GetExecution(t.Context(), r.Key)
		if err != nil || !e.RunDeadlineAt.Equal(e.CreatedAt.Add(r.RunTimeout)) {
			t.Fatalf("signal deadline: %+v %v", e, err)
		}
		if again, err := s.SignalWithStart(t.Context(), request); err != nil || again != receipt {
			t.Fatalf("signal start receipt: %+v %v", again, err)
		}
	})
	t.Run("child", func(t *testing.T) {
		parent := start(t, s)
		child := childSpec(parent, "deadline")
		child.Start.RunTimeout = time.Microsecond
		request := childCommit(parent, claim(t, s, parent, time.Minute), child)
		if _, err := s.CommitTransition(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		e, err := s.GetExecution(t.Context(), child.Start.Key)
		if err != nil || !e.RunDeadlineAt.Equal(e.CreatedAt.Add(time.Microsecond)) {
			t.Fatalf("child deadline: %+v %v", e, err)
		}
		closeLinkedExecution(t, s, parent, durable.StateCompleted)
		message := claimChildMessage(t, s, parent.Namespace, child.Start.BuildID, time.Minute)
		applied, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(message))
		if err != nil || applied.Disposition != durable.ChildDeliveryIgnoredExpired {
			t.Fatalf("expired child parent-close: %+v %v", applied, err)
		}
		after, readErr := s.GetExecution(t.Context(), child.Start.Key)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !reflect.DeepEqual(e, after) {
			t.Fatalf("expired target changed: %+v", after)
		}
	})
}

func executionDeadlineChildMessages(t *testing.T, s durable.Store) {
	t.Run("result", func(t *testing.T) {
		parent := deadlineStart(t, 150*time.Millisecond)
		if _, err := s.StartExecution(t.Context(), parent); err != nil {
			t.Fatal(err)
		}
		child := childSpec(parent, "result")
		child.ParentClosePolicy = durable.ParentCloseAbandon
		if _, err := s.CommitTransition(t.Context(), childCommit(parent, claim(t, s, parent, time.Minute), child)); err != nil {
			t.Fatal(err)
		}
		closeLinkedExecution(t, s, child.Start, durable.StateCompleted)
		time.Sleep(parent.ExecutionTimeout)
		before, readErr := s.GetExecution(t.Context(), parent.Key)
		if readErr != nil {
			t.Fatal(readErr)
		}
		message := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
		receipt, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(message))
		if err != nil || receipt.Disposition != durable.ChildDeliveryIgnoredExpired {
			t.Fatalf("expired parent result: %+v %v", receipt, err)
		}
		after, readErr := s.GetExecution(t.Context(), parent.Key)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("expired parent changed: %+v", after)
		}
	})
	t.Run("cancel_ack", func(t *testing.T) {
		parent := start(t, s)
		child := childSpec(parent, "cancel")
		child.Start.ExecutionTimeout = time.Microsecond
		if _, err := s.CommitTransition(t.Context(), childCommit(parent, claim(t, s, parent, time.Minute), child)); err != nil {
			t.Fatal(err)
		}
		task := claim(t, s, parent, time.Minute)
		request := durable.CommitRequest{Key: parent.Key, RequestID: "cancel-child", ExpectedRevision: 2, Token: task.Token(), Events: []durable.EventInput{{Type: "cancel"}}, CancelChildren: []durable.ChildCancellationSpec{{CommandID: "cancel:1", TargetID: child.CommandID}}}
		if _, err := s.CommitTransition(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		message := claimChildMessage(t, s, parent.Namespace, child.Start.BuildID, time.Minute)
		receipt, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(message))
		if err != nil || receipt.Disposition != durable.ChildDeliveryIgnoredExpired {
			t.Fatalf("expired cancellation: %+v %v", receipt, err)
		}
		ack := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
		if ack.Kind != durable.ChildDeliveryCancelAck || ack.Message.Disposition != durable.ChildDeliveryIgnoredExpired {
			t.Fatalf("expired acknowledgment: %+v", ack)
		}
		if _, err = s.ApplyChildDelivery(t.Context(), deliveryRequest(ack)); err != nil {
			t.Fatal(err)
		}
	})
}
