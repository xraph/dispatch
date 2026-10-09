package durabletest

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func workflowRetryFinality(t *testing.T, s durable.Store) {
	for _, mode := range []string{"nil", "zero", "exhausted", "type", "flag", "execution_budget", "cancellation", "terminated"} {
		t.Run(mode, func(t *testing.T) {
			r := deadlineStart(t, time.Hour)
			r.RetryPolicy = &durable.WorkflowRetryPolicy{InitialInterval: time.Second, MaximumAttempts: 3}
			failure := struct {
				Type         string `json:"type"`
				Message      string `json:"message"`
				NonRetryable bool   `json:"non_retryable,omitempty"`
			}{Type: "permanent"}
			switch mode {
			case "nil":
				r.RetryPolicy = nil
			case "zero":
				r.RetryPolicy = &durable.WorkflowRetryPolicy{}
			case "exhausted":
				r.RetryPolicy.MaximumAttempts = 1
			case "type":
				r.RetryPolicy.NonRetryableTypes = []string{"permanent"}
			case "flag":
				failure.NonRetryable = true
			case "execution_budget":
				r.RetryPolicy.InitialInterval = 3 * time.Hour
			}
			if _, err := s.StartExecution(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if mode == "cancellation" {
				if _, err := s.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: r.Key, RequestID: "cancel", BuildID: r.BuildID}); err != nil {
					t.Fatal(err)
				}
			}
			e, err := s.GetExecution(t.Context(), r.Key)
			if err != nil {
				t.Fatal(err)
			}
			request := completion(r, claim(t, s, r, time.Minute))
			payload, err := json.Marshal(failure)
			if err != nil {
				t.Fatal(err)
			}
			request.ExpectedRevision, request.State, request.Output = e.Revision, durable.StateFailed, nil
			request.Events = []durable.EventInput{{Type: "workflow.failed", Payload: payload}}
			if mode == "terminated" {
				request.State, request.Events[0].Type = durable.StateTerminated, durable.EventWorkflowTerminated
			}
			if _, err = s.CommitTransition(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			closed, err := s.GetExecution(t.Context(), r.Key)
			if err != nil || closed.NextRunID != "" || closed.State != request.State {
				t.Fatalf("final failure retried: %+v %v", closed, err)
			}
		})
	}
}

func workflowRetryRoots(t *testing.T, s durable.Store) {
	for _, mode := range []string{"ordinary", "signal", "child"} {
		t.Run(mode, func(t *testing.T) {
			r := deadlineStart(t, time.Hour)
			r.RetryPolicy = &durable.WorkflowRetryPolicy{MaximumAttempts: 3, NonRetryableTypes: []string{"permanent"}}
			var parent durable.StartRequest
			switch mode {
			case "ordinary":
				if _, err := s.StartExecution(t.Context(), r); err != nil {
					t.Fatal(err)
				}
			case "signal":
				if _, err := s.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: r, Name: "go"}); err != nil {
					t.Fatal(err)
				}
			case "child":
				parent = start(t, s)
				r.WorkflowID = "child"
				request := completion(parent, claim(t, s, parent, time.Minute))
				request.Children = []durable.ChildStartSpec{{CommandID: "child", Start: r, ParentQueue: parent.Queue, ParentClosePolicy: durable.ParentCloseAbandon}}
				if _, err := s.CommitTransition(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}
			r.RetryPolicy.NonRetryableTypes[0] = "caller mutation"
			e, err := s.GetExecution(t.Context(), r.Key)
			if err != nil || e.RetryPolicy == nil || e.RetryPolicy.NonRetryableTypes[0] != "permanent" || e.RetryPolicy.InitialInterval != time.Second || e.RetryAttempt != 1 || !e.RunAvailableAt.Equal(e.CreatedAt) {
				t.Fatalf("saved retry metadata: %+v %v", e, err)
			}
			e.RetryPolicy.NonRetryableTypes[0] = "reader mutation"
			got, err := s.ResolveExecution(t.Context(), durable.ExecutionTarget{Key: r.Key})
			if err != nil || got.RetryPolicy.NonRetryableTypes[0] != "permanent" {
				t.Fatalf("policy aliases reader: %+v %v", got, err)
			}
			if mode == "child" {
				link, err := s.GetChildExecution(t.Context(), parent.Key, "child")
				if err != nil || link.Start.RetryPolicy.NonRetryableTypes[0] != "permanent" {
					t.Fatalf("child policy aliases caller: %+v %v", link, err)
				}
				link.Start.RetryPolicy.NonRetryableTypes[0] = "reader mutation"
				link, err = s.GetParentExecution(t.Context(), r.Key)
				if err != nil || link.Start.RetryPolicy.NonRetryableTypes[0] != "permanent" {
					t.Fatalf("child policy aliases reader: %+v %v", link, err)
				}
			}
		})
	}
}

func workflowRetryFailure(t *testing.T, s durable.Store) {
	r := deadlineStart(t, time.Hour)
	r.RetryPolicy = &durable.WorkflowRetryPolicy{InitialInterval: 200 * time.Millisecond, MaximumAttempts: 2}
	if _, err := s.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SignalExecution(t.Context(), durable.SignalRequest{Key: r.Key, RequestID: "old", Name: "item", BuildID: r.BuildID, Input: []byte("carry")}); err != nil {
		t.Fatal(err)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil {
		t.Fatal(err)
	}
	request := completion(r, claim(t, s, r, time.Minute))
	request.ExpectedRevision = e.Revision
	request.State = durable.StateFailed
	request.Events = []durable.EventInput{{Type: "workflow.failed", Payload: []byte(`{"type":"transient","message":"retry"}`)}}
	receipt, err := s.CommitTransition(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	closed, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || closed.State != durable.StateFailed || closed.NextRunID == "" {
		t.Fatalf("failure not retried: %+v %v", closed, err)
	}
	nextKey := r.Key
	nextKey.RunID = closed.NextRunID
	next, err := s.GetExecution(t.Context(), nextKey)
	if err != nil || next.State != durable.StateRunning || next.RetryAttempt != 2 || next.RunNumber != 2 || next.FirstRunID != r.RunID || !next.RunAvailableAt.Equal(closed.UpdatedAt.Add(200*time.Millisecond)) || !next.RunDeadlineAt.Equal(next.RunAvailableAt.Add(r.RunTimeout)) || !next.ExecutionDeadlineAt.Equal(e.ExecutionDeadlineAt) {
		t.Fatalf("retry successor: %+v %v", next, err)
	}
	if _, err = s.SignalExecution(t.Context(), durable.SignalRequest{Key: nextKey, RequestID: "new", Name: "item", BuildID: r.BuildID}); err != nil {
		t.Fatal(err)
	}
	poll := durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, BuildID: r.BuildID, Kind: durable.TaskWorkflow, Owner: "retry", LeaseDuration: time.Minute}
	if task, pollErr := s.ClaimTask(t.Context(), poll); pollErr != nil || task != nil {
		t.Fatalf("signal bypassed backoff: %+v %v", task, pollErr)
	}
	if got, retryErr := s.CommitTransition(t.Context(), request); retryErr != nil || got != receipt {
		t.Fatalf("failure receipt changed: %+v %v", got, retryErr)
	}
	history, err := s.ReadHistory(t.Context(), nextKey, 0, 100)
	if err != nil || len(history) != 4 || history[2].Type != durable.EventSignalCarried {
		t.Fatalf("retry lost pending input: %+v %v", history, err)
	}
	time.Sleep(time.Until(next.RunAvailableAt) + 10*time.Millisecond)
	r.Key = nextKey
	last, err := s.GetExecution(t.Context(), nextKey)
	if err != nil {
		t.Fatal(err)
	}
	final := completion(r, claim(t, s, r, time.Minute))
	final.ExpectedRevision = last.Revision
	final.State = durable.StateFailed
	final.Events = request.Events
	if _, err = s.CommitTransition(t.Context(), final); err != nil {
		t.Fatal(err)
	}
	last, err = s.GetExecution(t.Context(), nextKey)
	if err != nil || last.State != durable.StateFailed || last.NextRunID != "" {
		t.Fatalf("retry exhaustion: %+v %v", last, err)
	}
	if _, err = s.ResolveExecution(t.Context(), durable.ExecutionTarget{Key: durable.Key{Namespace: r.Namespace, WorkflowID: r.WorkflowID}, Selection: durable.RunCurrent}); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("exhausted chain remains current: %v", err)
	}
}

func workflowRetryTimeout(t *testing.T, s durable.Store) {
	for _, mode := range []string{"run", "execution", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			r := deadlineStart(t, time.Microsecond)
			r.ExecutionTimeout = time.Hour
			r.RetryPolicy = &durable.WorkflowRetryPolicy{InitialInterval: time.Second, MaximumAttempts: 3}
			if mode == "execution" {
				r.ExecutionTimeout = time.Microsecond
			}
			if mode == "cancelled" {
				r.RunTimeout = 50 * time.Millisecond
			}
			if _, err := s.StartExecution(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if mode == "cancelled" {
				if _, err := s.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: r.Key, RequestID: "cancel", BuildID: r.BuildID}); err != nil {
					t.Fatal(err)
				}
				time.Sleep(60 * time.Millisecond)
			}
			grant := claimExecutionTimeout(t, s, r, time.Minute)
			request := executionTimeoutRequest(grant)
			receipt, err := s.ApplyExecutionTimeout(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			e, err := s.GetExecution(t.Context(), r.Key)
			if err != nil || e.State != durable.StateTimedOut || (e.NextRunID != "") != (mode == "run") {
				t.Fatalf("timeout retry finality: %+v %v", e, err)
			}
			if got, retryErr := s.ApplyExecutionTimeout(t.Context(), request); retryErr != nil || got != receipt {
				t.Fatalf("timeout receipt changed: %+v %v", got, retryErr)
			}
		})
	}
}
