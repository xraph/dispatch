package runtime_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func startRetryRun(t *testing.T, s durable.Store, o drt.Options) durable.Key {
	t.Helper()
	key := durable.Key{Namespace: o.Namespace, WorkflowID: "order", RunID: "root"}
	_, err := s.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", Queue: o.Queue, BuildID: o.BuildID, RunTimeout: time.Hour, ExecutionTimeout: 3 * time.Hour,
		RetryPolicy: &durable.WorkflowRetryPolicy{InitialInterval: 2 * time.Millisecond, MaximumAttempts: 2}})
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestWorkflowRetryReplayAndQuery(t *testing.T) {
	s := &lostResponseStore{Store: memory.New()}
	o := workerOptions(t)
	handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
		info := w.RunInfo()
		w.SetQueryHandler("run", func([]byte) ([]byte, error) {
			return []byte(fmt.Sprintf("%d/%d", info.RunNumber, info.RetryAttempt)), nil
		})
		if !w.Now().Equal(info.RunAvailableAt) {
			return nil, errors.New("wrong logical clock")
		}
		if info.RetryAttempt == 1 {
			return nil, &drt.ApplicationError{Type: "temporary", Message: "try again"}
		}
		return []byte("recovered"), nil
	}
	o.Workflows["order"] = handler
	key := startRetryRun(t, s, o)
	worker := newWorker(t, s, o)
	runTask(t, worker, durable.TaskWorkflow)
	root, events := continuationSnapshot(t, s, key)
	if root.State != durable.StateFailed || root.NextRunID == "" || len(events) != 3 {
		t.Fatalf("retry source: %+v %+v", root, events)
	}
	decision, err := drt.Evaluate(root, events, handler)
	if err != nil || decision.State != durable.StateFailed || decision.Failure.Type != "temporary" {
		t.Fatalf("failed source replay: %+v %v", decision, err)
	}
	q, err := worker.QueryExecution(t.Context(), drt.QueryRequest{Key: key, BuildID: o.BuildID, Name: "run"})
	if err != nil || string(q.Output) != "1/1" || q.State != durable.StateFailed {
		t.Fatalf("historical query: %+v %v", q, err)
	}
	key.RunID = root.NextRunID
	next, _ := continuationSnapshot(t, s, key)
	time.Sleep(time.Until(next.AvailableAt()) + time.Millisecond)
	runTask(t, newWorker(t, s, o), durable.TaskWorkflow)
	last, history := continuationSnapshot(t, s, key)
	if last.State != durable.StateCompleted || string(last.Output) != "recovered" {
		t.Fatalf("successor: %+v", last)
	}
	if _, err := drt.Evaluate(last, history, handler); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"missing", "delay", "type", "attempt", "availability", "root", "next", "clock", "nonretryable", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			e, copyEvents := root.Clone(), slices.Clone(events)
			var retry durable.WorkflowRetryScheduled
			if err := json.Unmarshal(copyEvents[1].Payload, &retry); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing":
				copyEvents[1].Type, copyEvents[1].Payload = drt.EventWorkflowWaiting, nil
			case "delay":
				retry.Delay++
			case "type":
				retry.FailureType = "other"
			case "attempt":
				retry.Next.RetryAttempt++
			case "availability":
				retry.Next.RunAvailableAt = retry.Next.RunAvailableAt.Add(time.Microsecond)
			case "root":
				retry.Next.FirstRunID = "other"
			case "next":
				e.NextRunID = "other"
			case "clock":
				copyEvents[1].Time = copyEvents[1].Time.Add(time.Microsecond)
			case "nonretryable":
				copyEvents[2].Payload = encode(t, drt.ApplicationError{Type: "temporary", NonRetryable: true})
			case "disabled":
				e.RetryPolicy = nil
			}
			if mode != "missing" {
				copyEvents[1].Payload = encode(t, retry)
			}
			if _, err := drt.Evaluate(e, copyEvents, handler); !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("corrupt retry accepted: %v", err)
			}
		})
	}
}

func TestWorkflowRetryTimeoutHistoricalQuery(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.SetQueryHandler("run", func([]byte) ([]byte, error) { return []byte(fmt.Sprint(w.RunInfo().RetryAttempt)), nil })
		return w.ReceiveSignal("wait", "never").Get()
	}
	key := durable.Key{Namespace: o.Namespace, WorkflowID: "order", RunID: "root"}
	_, err := s.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", Queue: o.Queue, BuildID: o.BuildID, RunTimeout: time.Microsecond, ExecutionTimeout: time.Hour,
		RetryPolicy: &durable.WorkflowRetryPolicy{InitialInterval: time.Millisecond, MaximumAttempts: 2}})
	if err != nil {
		t.Fatal(err)
	}
	worker := newWorker(t, s, o)
	time.Sleep(time.Millisecond)
	coordinator := o
	coordinator.BuildID, coordinator.Workflows = "unserved-build", nil
	runTask(t, newWorker(t, s, coordinator), drt.TaskExecutionTimeout)
	e, events := continuationSnapshot(t, s, key)
	if e.State != durable.StateTimedOut || e.NextRunID == "" {
		t.Fatalf("timeout retry: %+v", e)
	}
	if d, replayErr := drt.Evaluate(e, events, o.Workflows["order"]); replayErr != nil || d.State != durable.StateTimedOut {
		t.Fatalf("timeout replay: %+v %v", d, replayErr)
	}
	q, err := worker.QueryExecution(t.Context(), drt.QueryRequest{Key: key, BuildID: o.BuildID, Name: "run"})
	if err != nil || string(q.Output) != "1" {
		t.Fatalf("timeout query: %+v %v", q, err)
	}
}

func TestWorkflowRetryDecisionCapacity(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	key := startRetryRun(t, s, o)
	e, events := continuationSnapshot(t, s, key)
	for _, count := range []int{998, 999} {
		handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
			for i := 0; i < count; i++ {
				w.Activity(fmt.Sprint(i), "activity", "", nil)
			}
			return nil, &drt.ApplicationError{Type: "temporary"}
		}
		decision, err := drt.Evaluate(e, events, handler)
		if count == 999 {
			if !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("retry link capacity not reserved: commands=%d %v", len(decision.Commands), err)
			}
		} else if err != nil || len(decision.Commands) != count {
			t.Fatalf("bounded decision rejected: commands=%d %v", len(decision.Commands), err)
		}
	}
}
