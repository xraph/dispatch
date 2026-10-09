package durabletest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

type lostWorkflowRetry struct {
	durable.Store
	requests []durable.CommitRequest
}

func (s *lostWorkflowRetry) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	receipt, err := s.Store.CommitTransition(ctx, r)
	if r.State == durable.StateFailed || r.State == durable.StateContinuedAsNew {
		s.requests = append(s.requests, r)
		if err == nil && len(s.requests) == 1 {
			return durable.Receipt{}, errors.New("lost workflow closure response")
		}
	}
	return receipt, err
}

// RunWorkflowRetryChains checks replay and child delivery across worker/store
// replacement. Reopen must return the same durable data through a fresh client.
func RunWorkflowRetryChains(t *testing.T, initial durable.Store, reopen func() durable.Store) {
	t.Helper()
	s := initial
	t.Run("mixed_chain", func(t *testing.T) {
		o := retryRuntimeOptions(t)
		o.Workflows["order"] = func(w *drt.Workflow, input []byte) ([]byte, error) {
			info := w.RunInfo()
			w.SetQueryHandler("run", func([]byte) ([]byte, error) {
				return []byte(fmt.Sprintf("%d/%d", info.RunNumber, info.RetryAttempt)), nil
			})
			if info.RetryAttempt == 1 {
				return nil, &drt.ApplicationError{Type: "temporary", Message: "retry"}
			}
			if len(input) == 0 {
				return nil, w.ContinueAsNew([]byte("phase2"), drt.ContinueOptions{})
			}
			return w.ReceiveSignal("item", "items").Get()
		}
		start := durable.StartRequest{Key: durable.Key{WorkflowID: "order", RunID: "root"}, RequestID: "start"}
		start.Namespace, start.WorkflowType, start.Queue, start.BuildID = o.Namespace, "order", o.Queue, o.BuildID
		start.Input = nil
		start.RunTimeout, start.ExecutionTimeout = time.Hour, 3*time.Hour
		start.RetryPolicy = &durable.WorkflowRetryPolicy{InitialInterval: 5 * time.Millisecond, MaximumAttempts: 2}
		if _, err := s.StartExecution(t.Context(), start); err != nil {
			t.Fatal(err)
		}
		signal := durable.SignalRequest{Key: start.Key, RequestID: "pending", Name: "items", BuildID: start.BuildID, Input: []byte("carried")}
		accepted, err := s.SignalExecution(t.Context(), signal)
		if err != nil {
			t.Fatal(err)
		}
		key := start.Key
		var first durable.CommitRequest
		var firstReceipt durable.Receipt
		for number := int64(1); number <= 4; number++ {
			s = reopen()
			lost := &lostWorkflowRetry{Store: s}
			worker := retryRuntimeWorker(t, lost, o)
			before, readErr := s.GetExecution(t.Context(), key)
			if readErr != nil {
				t.Fatal(readErr)
			}
			time.Sleep(max(time.Until(before.AvailableAt())+time.Millisecond, 0))
			retryRuntimeTask(t, worker, durable.TaskWorkflow)
			e, readErr := s.GetExecution(t.Context(), key)
			if readErr != nil || e.RunNumber != number || e.WorkflowAttempt() != 1+(number-1)%2 || e.FirstRunID != start.RunID || !durable.SameWorkflowRetryPolicy(e.RetryPolicy, before.RetryPolicy) {
				t.Fatalf("mixed chain: %+v %v", e, readErr)
			}
			want := durable.StateFailed
			if number == 2 {
				want = durable.StateContinuedAsNew
			}
			if number == 4 {
				want = durable.StateCompleted
			}
			if e.State != want || !e.ExecutionDeadlineAt.Equal(before.ExecutionDeadlineAt) {
				t.Fatalf("source result: %+v", e)
			}
			q, queryErr := worker.QueryExecution(t.Context(), drt.QueryRequest{Key: key, BuildID: o.BuildID, Name: "run"})
			if queryErr != nil || string(q.Output) != fmt.Sprintf("%d/%d", number, e.WorkflowAttempt()) || q.State != want {
				t.Fatalf("historical query: %+v %v", q, queryErr)
			}
			if number < 4 {
				if e.NextRunID == "" || len(lost.requests) != 2 || !reflect.DeepEqual(lost.requests[0], lost.requests[1]) {
					t.Fatalf("lost response did not retry exactly: %+v %d", e, len(lost.requests))
				}
				if number == 1 {
					first = lost.requests[0]
					firstReceipt, err = s.CommitTransition(t.Context(), first)
					if err != nil {
						t.Fatal(err)
					}
				}
				key.RunID = e.NextRunID
				current, resolveErr := s.ResolveExecution(t.Context(), durable.ExecutionTarget{Key: durable.Key{Namespace: key.Namespace, WorkflowID: key.WorkflowID}, Selection: durable.RunCurrent})
				if resolveErr != nil || current.Key != key {
					t.Fatalf("current did not follow chain: %+v %v", current, resolveErr)
				}
			} else if string(e.Output) != "carried" {
				t.Fatalf("signal lost across retries: %+v", e)
			}
		}
		latest, err := s.ResolveExecution(t.Context(), durable.ExecutionTarget{Key: durable.Key{Namespace: key.Namespace, WorkflowID: key.WorkflowID}, Selection: durable.RunLatest})
		if err != nil || latest.Key != key {
			t.Fatalf("latest: %+v %v", latest, err)
		}
		if again, err := s.CommitTransition(t.Context(), first); err != nil || again != firstReceipt {
			t.Fatalf("old failure receipt: %+v %v", again, err)
		}
		if again, err := s.SignalExecution(t.Context(), signal); err != nil || again != accepted {
			t.Fatalf("old signal receipt: %+v %v", again, err)
		}
	})
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprintf("child_timeout_%t", timeout), func(t *testing.T) {
			o := retryRuntimeOptions(t)
			o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				result, err := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{Queue: "children", RunTimeout: time.Second, ExecutionTimeout: time.Hour,
					RetryPolicy: &durable.WorkflowRetryPolicy{InitialInterval: 5 * time.Millisecond, MaximumAttempts: 2}}).Get()
				if errors.Is(err, drt.ErrChildTimedOut) {
					return []byte("timed out"), nil
				}
				return result, err
			}
			o.Workflows["child"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				if w.RunInfo().RetryAttempt == 1 {
					return nil, &drt.ApplicationError{Type: "temporary"}
				}
				return []byte("recovered"), nil
			}
			start := durable.StartRequest{Key: durable.Key{WorkflowID: "order", RunID: "root"}, RequestID: "start"}
			start.Namespace, start.WorkflowType, start.Queue, start.BuildID = o.Namespace, "order", o.Queue, o.BuildID
			if _, err := s.StartExecution(t.Context(), start); err != nil {
				t.Fatal(err)
			}
			parent := retryRuntimeWorker(t, s, o)
			retryRuntimeTask(t, parent, durable.TaskWorkflow)
			co := o
			co.Queue = "children"
			retryRuntimeTask(t, retryRuntimeWorker(t, s, co), durable.TaskWorkflow)
			s = reopen()
			parent = retryRuntimeWorker(t, s, o)
			if worked, err := parent.RunOnce(t.Context(), drt.TaskChildDelivery); err != nil || worked {
				t.Fatalf("intermediate failure delivered: %t %v", worked, err)
			}
			link, err := s.GetChildExecution(t.Context(), start.Key, "child")
			if err != nil || link.CurrentKey == link.Start.Key || link.State != durable.StateRunning {
				t.Fatalf("child retry link: %+v %v", link, err)
			}
			next, err := s.GetExecution(t.Context(), link.CurrentKey)
			if err != nil {
				t.Fatal(err)
			}
			if timeout {
				time.Sleep(max(time.Until(next.RunDeadlineAt)+time.Millisecond, 0))
				retryRuntimeTask(t, parent, drt.TaskExecutionTimeout)
			} else {
				time.Sleep(max(time.Until(next.AvailableAt())+time.Millisecond, 0))
				retryRuntimeTask(t, retryRuntimeWorker(t, s, co), durable.TaskWorkflow)
			}
			s = reopen()
			parent = retryRuntimeWorker(t, s, o)
			retryRuntimeTask(t, parent, drt.TaskChildDelivery)
			retryRuntimeTask(t, parent, durable.TaskWorkflow)
			result, err := s.GetExecution(t.Context(), start.Key)
			want := "recovered"
			if timeout {
				want = "timed out"
			}
			if err != nil || result.State != durable.StateCompleted || string(result.Output) != want {
				t.Fatalf("final child result: %+v %v", result, err)
			}
		})
	}
}

func retryRuntimeOptions(t *testing.T) drt.Options {
	t.Helper()
	return drt.Options{Namespace: t.Name(), Queue: "workflows", BuildID: "v1", Owner: "retry-worker", LeaseDuration: 10 * time.Second, StoreTimeout: time.Second, Workflows: make(map[string]drt.WorkflowFunc)}
}

func retryRuntimeWorker(t *testing.T, s durable.Store, o drt.Options) *drt.Worker {
	t.Helper()
	w, err := drt.NewWorker(s, o)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func retryRuntimeTask(t *testing.T, w *drt.Worker, kind durable.TaskKind) {
	t.Helper()
	if worked, err := w.RunOnce(t.Context(), kind); err != nil || !worked {
		t.Fatalf("run %s: worked=%t %v", kind, worked, err)
	}
}
