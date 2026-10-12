package runtime_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func continuationSnapshot(t *testing.T, s durable.Store, key durable.Key) (durable.Execution, []durable.Event) {
	t.Helper()
	e, err := s.GetExecution(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return e, events
}

func TestContinuationReplayAndHistoricalQuery(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	timeout := 2 * time.Hour
	options := drt.ContinueOptions{WorkflowType: "next", BuildID: "v2", Queue: "successors", RunTimeout: &timeout}
	handler := func(w *drt.Workflow, input []byte) ([]byte, error) {
		w.SetQueryHandler("input", func([]byte) ([]byte, error) { return input, nil })
		return nil, w.ContinueAsNew([]byte("next input"), options)
	}
	o.Workflows["order"] = handler
	w := newWorker(t, s, o)
	key := startWorkerRun(t, w, o)
	runTask(t, w, durable.TaskWorkflow)
	e, events := continuationSnapshot(t, s, key)
	if e.State != durable.StateContinuedAsNew || e.NextRunID == "" {
		t.Fatalf("source not continued: %+v", e)
	}
	d, err := drt.Evaluate(e, events, handler)
	if err != nil || d.State != durable.StateContinuedAsNew || d.Continuation != nil || len(d.Commands) != 0 {
		t.Fatalf("terminal replay returned a new handoff: %+v %v", d, err)
	}
	for _, mutation := range []string{"input", "type", "build", "queue", "timeout", "inherit", "omit", "extra"} {
		t.Run(mutation, func(t *testing.T) {
			changed := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				opt, input := options, []byte("next input")
				switch mutation {
				case "input":
					input = []byte("changed")
				case "type":
					opt.WorkflowType = "changed"
				case "build":
					opt.BuildID = "changed"
				case "queue":
					opt.Queue = "changed"
				case "timeout":
					v := time.Minute
					opt.RunTimeout = &v
				case "inherit":
					opt.RunTimeout = nil
				case "omit":
					return nil, nil
				case "extra":
					w.Activity("extra", "extra", "", nil)
				}
				return nil, w.ContinueAsNew(input, opt)
			}
			if _, replayErr := drt.Evaluate(e, events, changed); !errors.Is(replayErr, drt.ErrNondeterministic) {
				t.Fatalf("changed continuation accepted: %v", replayErr)
			}
		})
	}
	q, err := w.QueryExecution(t.Context(), drt.QueryRequest{Key: key, BuildID: o.BuildID, Name: "input"})
	if err != nil || q.State != durable.StateContinuedAsNew || len(q.Output) != 0 {
		t.Fatalf("historical query: %+v %v", q, err)
	}
	after, _ := continuationSnapshot(t, s, key)
	if after.Revision != e.Revision || after.LastSequence != e.LastSequence || after.NextRunID != e.NextRunID {
		t.Fatal("query changed predecessor")
	}
	nextKey := key
	nextKey.RunID = e.NextRunID
	next, _ := continuationSnapshot(t, s, nextKey)
	if next.WorkflowType != "next" || next.BuildID != "v2" || next.RunTimeout != timeout || string(next.Input) != "next input" {
		t.Fatalf("resolved options lost: %+v", next)
	}
}

func TestContinuationDefaultsCarryAndMultipleRuns(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, input []byte) ([]byte, error) {
		w.SetQueryHandler("input", func([]byte) ([]byte, error) { return input, nil })
		switch string(input) {
		case "":
			value, err := w.ReceiveSignal("first", "items").Get()
			if err != nil || string(value) != "first" {
				return nil, errors.New("first signal lost")
			}
			return nil, w.ContinueAsNew([]byte("middle"), drt.ContinueOptions{})
		case "middle":
			return nil, w.ContinueAsNew([]byte("last"), drt.ContinueOptions{})
		default:
			return w.ReceiveSignal("second", "items").Get()
		}
	}
	w := newWorker(t, s, o)
	key := durable.Key{Namespace: o.Namespace, WorkflowID: "order", RunID: "root"}
	if _, err := w.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", Queue: o.Queue, BuildID: o.BuildID, RunTimeout: time.Hour, ExecutionTimeout: 3 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"first", "second"} {
		if _, err := w.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: value, BuildID: o.BuildID, Name: "items", Input: []byte(value)}); err != nil {
			t.Fatal(err)
		}
	}
	root, _ := continuationSnapshot(t, s, key)
	for number := int64(1); number <= 3; number++ {
		o.Owner = "replacement"
		w = newWorker(t, s, o)
		runTask(t, w, durable.TaskWorkflow)
		e, events := continuationSnapshot(t, s, key)
		if e.RunNumber != number || e.FirstRunID != root.RunID || e.RunTimeout != time.Hour || !e.ExecutionDeadlineAt.Equal(root.ExecutionDeadlineAt) {
			t.Fatalf("chain changed: %+v", e)
		}
		if _, err := drt.Evaluate(e, events, o.Workflows["order"]); err != nil {
			t.Fatalf("replay run %d: %v", number, err)
		}
		if number < 3 {
			if e.State != durable.StateContinuedAsNew || e.NextRunID == "" {
				t.Fatalf("missing successor: %+v", e)
			}
			key.RunID = e.NextRunID
		} else if e.State != durable.StateCompleted || string(e.Output) != "second" {
			t.Fatalf("carried signal lost: %+v", e)
		}
	}
}

func TestContinuationMustBeReturnedAndEndsDecision(t *testing.T) {
	for _, mode := range []string{"ignored", "wrapped", "joined", "output", "operation", "twice", "negative", "query", "long type", "long queue"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistory()
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				if mode == "query" {
					w.SetQueryHandler("bad", func([]byte) ([]byte, error) { return nil, w.ContinueAsNew(nil, drt.ContinueOptions{}) })
					return nil, nil
				}
				opt := drt.ContinueOptions{}
				if mode == "long type" {
					opt.WorkflowType = strings.Repeat("a", 201)
				}
				if mode == "long queue" {
					opt.Queue = strings.Repeat("a", 201)
				}
				if mode == "negative" {
					v := -time.Second
					opt.RunTimeout = &v
				}
				intent := w.ContinueAsNew(nil, opt)
				switch mode {
				case "ignored":
					return nil, nil
				case "wrapped":
					return nil, fmt.Errorf("wrapped: %w", intent)
				case "joined":
					return nil, errors.Join(intent, errors.New("failure"))
				case "output":
					return []byte("invalid"), intent
				case "operation":
					w.Timer("after", time.Hour)
				case "twice":
					return nil, w.ContinueAsNew(nil, opt)
				}
				return nil, intent
			}
			if mode == "query" {
				_, err := drt.EvaluateQuery(f.execution, f.events, handler, drt.QueryRequest{Key: f.execution.Key, BuildID: f.execution.BuildID, Name: "bad"})
				if !errors.Is(err, drt.ErrQueryMutation) {
					t.Fatalf("query scheduled successor: %v", err)
				}
			} else if _, err := drt.Evaluate(f.execution, f.events, handler); !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("invalid continuation accepted: %v", err)
			}
		})
	}
}

func TestContinuationCapturesCopiesAndOptionIntent(t *testing.T) {
	f := newHistory()
	f.execution.RunTimeout = time.Hour
	input, timeout := []byte("next"), time.Hour
	d, err := drt.Evaluate(f.execution, f.events, func(w *drt.Workflow, _ []byte) ([]byte, error) {
		intent := w.ContinueAsNew(input, drt.ContinueOptions{RunTimeout: &timeout})
		input[0], timeout = 'X', 0
		return nil, intent
	})
	if err != nil || d.Continuation == nil || string(d.Continuation.Next.Input) != "next" || d.Continuation.Next.RunTimeout != time.Hour || *d.Continuation.Options.RunTimeout != time.Hour {
		t.Fatalf("mutable inputs escaped capture: %+v %v", d, err)
	}
	s := memory.New()
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return nil, w.ContinueAsNew(nil, drt.ContinueOptions{})
	}
	w := newWorker(t, s, o)
	key := startWorkerRun(t, w, o)
	runTask(t, w, durable.TaskWorkflow)
	e, events := continuationSnapshot(t, s, key)
	for _, mode := range []string{"type", "build", "queue", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			handler := func(w *drt.Workflow, _ []byte) ([]byte, error) {
				options := drt.ContinueOptions{}
				switch mode {
				case "type":
					options.WorkflowType = e.WorkflowType
				case "build":
					options.BuildID = e.BuildID
				case "queue":
					options.Queue = o.Queue
				case "timeout":
					options.RunTimeout = new(time.Duration)
				}
				return nil, w.ContinueAsNew(nil, options)
			}
			if _, err := drt.Evaluate(e, events, handler); !errors.Is(err, drt.ErrNondeterministic) {
				t.Fatalf("explicit inherited value changed replay: %v", err)
			}
		})
	}
}

func TestContinuationReservesTwoEvents(t *testing.T) {
	for _, count := range []int{998, 999} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := newHistory()
			d, err := drt.Evaluate(f.execution, f.events, func(w *drt.Workflow, _ []byte) ([]byte, error) {
				for i := range count {
					w.Activity(fmt.Sprintf("activity-%d", i), "activity", "", nil)
				}
				return nil, w.ContinueAsNew(nil, drt.ContinueOptions{})
			})
			if count == 998 {
				if err != nil || d.State != durable.StateContinuedAsNew {
					t.Fatalf("bounded continuation rejected: %+v %v", d, err)
				}
			} else if !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("oversized continuation accepted: %v", err)
			}
		})
	}
}

func TestContinuationCancellationTakesPrecedence(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return nil, w.ContinueAsNew(nil, drt.ContinueOptions{})
	}
	w := newWorker(t, s, o)
	key := startWorkerRun(t, w, o)
	if _, err := w.RequestCancelExecution(t.Context(), durable.CancelExecutionRequest{Key: key, RequestID: "cancel", BuildID: o.BuildID}); err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskWorkflow)
	e, _ := continuationSnapshot(t, s, key)
	if e.State != durable.StateCancelled || e.NextRunID != "" {
		t.Fatalf("continuation escaped cancellation: %+v", e)
	}
}

func TestContinuationChildWaitsForFinalRun(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ChildWorkflow("child", "child", nil, drt.ChildOptions{Queue: "children", BuildID: "child-v1"}).Get()
	}
	parent := newWorker(t, s, o)
	key := startWorkerRun(t, parent, o)
	runTask(t, parent, durable.TaskWorkflow)
	co := childWorkerOptions(t, o)
	co.Workflows["child"] = func(w *drt.Workflow, input []byte) ([]byte, error) {
		if len(input) == 0 {
			return nil, w.ContinueAsNew([]byte("final result"), drt.ContinueOptions{BuildID: "child-v2", Queue: "children-v2"})
		}
		return input, nil
	}
	child := newWorker(t, s, co)
	runTask(t, child, durable.TaskWorkflow)
	if worked, err := parent.RunOnce(t.Context(), drt.TaskChildDelivery); err != nil || worked {
		t.Fatalf("intermediate run delivered: %t %v", worked, err)
	}
	co.Queue, co.BuildID = "children-v2", "child-v2"
	child = newWorker(t, s, co)
	runTask(t, child, durable.TaskWorkflow)
	runTask(t, parent, drt.TaskChildDelivery)
	runTask(t, parent, durable.TaskWorkflow)
	e, events := continuationSnapshot(t, s, key)
	if e.State != durable.StateCompleted || string(e.Output) != "final result" {
		t.Fatalf("parent did not await chain: %+v", e)
	}
	if _, err := drt.Evaluate(e, events, o.Workflows["order"]); err != nil {
		t.Fatal(err)
	}
}
