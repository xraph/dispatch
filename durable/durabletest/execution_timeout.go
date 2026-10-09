package durabletest

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func claimExecutionTimeout(t *testing.T, s durable.Store, r durable.StartRequest, ttl time.Duration) *durable.ExecutionTimeoutTask {
	t.Helper()
	task, err := s.ClaimExecutionTimeout(t.Context(), durable.ExecutionTimeoutClaimRequest{Namespace: r.Namespace, Owner: "expiry", LeaseDuration: ttl})
	if err != nil || task == nil {
		t.Fatalf("execution timeout claim: %+v %v", task, err)
	}
	return task
}
func executionTimeoutRequest(task *durable.ExecutionTimeoutTask) durable.ExecutionTimeoutRequest {
	return durable.ExecutionTimeoutRequest{Key: task.Key, RequestID: "timeout", Owner: task.Owner, Epoch: task.Epoch}
}
func executionTimeoutClosure(t *testing.T, s durable.Store) {
	r := deadlineStart(t, time.Microsecond)
	r.BuildID = "retired"
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	grant := claimExecutionTimeout(t, s, r, time.Minute)
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Kind != durable.TimeoutRun || !grant.DeadlineAt.Equal(e.RunDeadlineAt) || grant.Epoch != 1 || grant.Attempt != 1 {
		t.Fatalf("grant: %+v", grant)
	}
	request := executionTimeoutRequest(grant)
	first, err := s.ApplyExecutionTimeout(t.Context(), request)
	if err != nil || first.Revision != 2 || first.FirstSequence != 2 || first.LastSequence != 2 {
		t.Fatalf("timeout: %+v %v", first, err)
	}
	closed, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || closed.State != durable.StateTimedOut || closed.Revision != 2 || len(closed.Output) != 0 {
		t.Fatalf("closed: %+v %v", closed, err)
	}
	history, err := s.ReadHistory(t.Context(), r.Key, 1, 100)
	if err != nil || len(history) != 1 || history[0].Type != durable.EventWorkflowTimedOut {
		t.Fatalf("history: %+v %v", history, err)
	}
	var timeout durable.ExecutionTimeout
	if err = json.Unmarshal(history[0].Payload, &timeout); err != nil || timeout.Version != 1 || timeout.Kind != grant.Kind || !timeout.DeadlineAt.Equal(grant.DeadlineAt) || history[0].Time.Before(grant.DeadlineAt) {
		t.Fatalf("payload: %+v %v", timeout, err)
	}
	task, err := s.GetTask(t.Context(), r.Key, "workflow:1")
	if err != nil || !task.Done {
		t.Fatalf("unfenced task: %+v %v", task, err)
	}
	if again, retryErr := s.ApplyExecutionTimeout(t.Context(), request); retryErr != nil || again != first {
		t.Fatalf("receipt retry: %+v %v", again, retryErr)
	}
	request.Epoch++
	if _, err = s.ApplyExecutionTimeout(t.Context(), request); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed request: %v", err)
	}
	if got, claimErr := s.ClaimExecutionTimeout(t.Context(), durable.ExecutionTimeoutClaimRequest{Namespace: r.Namespace, Owner: "other", LeaseDuration: time.Minute}); claimErr != nil || got != nil {
		t.Fatalf("closed claim: %+v %v", got, claimErr)
	}
	r.RunID = "next"
	r.RequestID = "next"
	r.RunTimeout = 0
	r.ExecutionTimeout = 0
	if _, err = s.StartExecution(t.Context(), r); err != nil {
		t.Fatalf("replacement: %v", err)
	}
}
func executionTimeoutGrants(t *testing.T, s durable.Store) {
	r := deadlineStart(t, 200*time.Millisecond)
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	poll := durable.ExecutionTimeoutClaimRequest{Namespace: r.Namespace, Owner: "expiry", LeaseDuration: 50 * time.Millisecond}
	if early, err := s.ClaimExecutionTimeout(t.Context(), poll); err != nil || early != nil {
		t.Fatalf("early claim: %+v %v", early, err)
	}
	time.Sleep(r.ExecutionTimeout)
	other := poll
	other.Namespace += "-other"
	if got, err := s.ClaimExecutionTimeout(t.Context(), other); err != nil || got != nil {
		t.Fatalf("namespace: %+v %v", got, err)
	}
	grant := claimExecutionTimeout(t, s, r, poll.LeaseDuration)
	if got, err := s.ClaimExecutionTimeout(t.Context(), poll); err != nil || got != nil {
		t.Fatalf("live reclaim: %+v %v", got, err)
	}
	request := executionTimeoutRequest(grant)
	wrong := request
	wrong.Owner = "wrong"
	if _, err := s.ApplyExecutionTimeout(t.Context(), wrong); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("wrong owner: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := s.ApplyExecutionTimeout(t.Context(), request); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("expired grant: %v", err)
	}
	next := claimExecutionTimeout(t, s, r, time.Minute)
	if next.Owner != grant.Owner || next.Epoch != grant.Epoch+1 || next.Attempt != grant.Attempt+1 {
		t.Fatalf("same-owner reclaim: %+v", next)
	}
	if _, err := s.ApplyExecutionTimeout(t.Context(), request); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("old epoch: %v", err)
	}
	if _, err := s.ApplyExecutionTimeout(t.Context(), executionTimeoutRequest(next)); err != nil {
		t.Fatal(err)
	}
}
func executionTimeoutConcurrent(t *testing.T, s durable.Store) {
	r := deadlineStart(t, time.Microsecond)
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			grant, err := s.ClaimExecutionTimeout(t.Context(), durable.ExecutionTimeoutClaimRequest{Namespace: r.Namespace, Owner: "expiry", LeaseDuration: time.Minute})
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			if grant == nil {
				return
			}
			winners.Add(1)
			request := executionTimeoutRequest(grant)
			var completion sync.WaitGroup
			for range 3 {
				completion.Go(func() {
					if _, err := s.ApplyExecutionTimeout(t.Context(), request); err != nil {
						t.Errorf("concurrent close: %v", err)
					}
				})
			}
			completion.Wait()
		})
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("winners: %d", winners.Load())
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || e.Revision != 2 || e.State != durable.StateTimedOut {
		t.Fatalf("duplicated close: %+v %v", e, err)
	}
}
func executionTimeoutChildren(t *testing.T, s durable.Store) {
	t.Run("child_result", func(t *testing.T) {
		parent := start(t, s)
		child := childSpec(parent, "timeout")
		child.Start.RunTimeout = time.Microsecond
		if _, err := s.CommitTransition(t.Context(), childCommit(parent, claim(t, s, parent, time.Minute), child)); err != nil {
			t.Fatal(err)
		}
		grant := claimExecutionTimeout(t, s, child.Start, time.Minute)
		if _, err := s.ApplyExecutionTimeout(t.Context(), executionTimeoutRequest(grant)); err != nil {
			t.Fatal(err)
		}
		message := claimChildMessage(t, s, parent.Namespace, parent.BuildID, time.Minute)
		if message.Kind != durable.ChildDeliveryResult || message.Message.State != durable.StateTimedOut || message.Message.CloseEvent.Type != durable.EventWorkflowTimedOut {
			t.Fatalf("timeout result: %+v", message)
		}
		if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(message)); err != nil {
			t.Fatal(err)
		}
	})
	for _, policy := range []durable.ParentClosePolicy{durable.ParentCloseTerminate, durable.ParentCloseRequestCancel, durable.ParentCloseAbandon} {
		t.Run(string(policy), func(t *testing.T) {
			parent := deadlineStart(t, 150*time.Millisecond)
			if _, err := s.StartExecution(t.Context(), parent); err != nil {
				t.Fatal(err)
			}
			child := childSpec(parent, "child")
			child.ParentClosePolicy = policy
			if _, err := s.CommitTransition(t.Context(), childCommit(parent, claim(t, s, parent, time.Minute), child)); err != nil {
				t.Fatal(err)
			}
			time.Sleep(parent.ExecutionTimeout)
			grant := claimExecutionTimeout(t, s, parent, time.Minute)
			if _, err := s.ApplyExecutionTimeout(t.Context(), executionTimeoutRequest(grant)); err != nil {
				t.Fatal(err)
			}
			messages, err := s.ListChildDeliveries(t.Context(), parent.Key, "", 100)
			want := 1
			if policy == durable.ParentCloseAbandon {
				want = 0
			}
			if err != nil || len(messages) != want {
				t.Fatalf("parent close delivery: %+v %v", messages, err)
			}
			if want > 0 {
				message := claimChildMessage(t, s, parent.Namespace, child.Start.BuildID, time.Minute)
				if _, err := s.ApplyChildDelivery(t.Context(), deliveryRequest(message)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func executionTimeoutValidation(t *testing.T, s durable.Store) {
	for _, r := range []durable.ExecutionTimeoutClaimRequest{
		{}, {Namespace: "n", Owner: "o"}, {Namespace: "n", Owner: "o", LeaseDuration: time.Nanosecond}, {Namespace: "n", Owner: "o", LeaseDuration: 25 * time.Hour},
	} {
		if _, err := s.ClaimExecutionTimeout(t.Context(), r); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid timeout poll: %+v %v", r, err)
		}
	}
	if _, err := s.ApplyExecutionTimeout(t.Context(), durable.ExecutionTimeoutRequest{}); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("invalid closure: %v", err)
	}
	r := start(t, s)
	if got, err := s.ClaimExecutionTimeout(t.Context(), durable.ExecutionTimeoutClaimRequest{Namespace: r.Namespace, Owner: "expiry", LeaseDuration: time.Minute}); err != nil || got != nil {
		t.Fatalf("unlimited run claimed: %+v %v", got, err)
	}
	if _, err := s.ApplyExecutionTimeout(t.Context(), durable.ExecutionTimeoutRequest{Key: r.Key, RequestID: "timeout", Owner: "expiry", Epoch: 1}); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("unowned closure: %v", err)
	}
}
