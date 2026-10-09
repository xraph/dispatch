//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

type lostContinuationRuntimeStore struct {
	durable.Store
	requests []durable.CommitRequest
}

func (s *lostContinuationRuntimeStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	receipt, err := s.Store.CommitTransition(ctx, r)
	if r.Continuation != nil {
		s.requests = append(s.requests, r)
		if err == nil && len(s.requests) == 1 {
			return durable.Receipt{}, errors.New("continuation response lost")
		}
	}
	return receipt, err
}

func TestDurableRuntimeContinuationReplacement(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	o := childRuntimeOptions(t)
	handler := func(w *drt.Workflow, input []byte) ([]byte, error) {
		w.SetQueryHandler("input", func([]byte) ([]byte, error) { return input, nil })
		switch string(input) {
		case "":
			first, err := w.ReceiveSignal("first", "items").Get()
			if err != nil || string(first) != "first" {
				return nil, errors.New("first message lost")
			}
			return nil, w.ContinueAsNew([]byte("middle"), drt.ContinueOptions{BuildID: "v2", Queue: "next", WorkflowType: "successor"})
		case "middle":
			return nil, w.ContinueAsNew([]byte("last"), drt.ContinueOptions{})
		default:
			return w.ReceiveSignal("second", "items").Get()
		}
	}
	o.Workflows = map[string]drt.WorkflowFunc{"order": handler, "successor": handler}
	worker := newChildRuntimeWorker(t, s, o)
	start := durable.StartRequest{Key: durable.Key{Namespace: o.Namespace, WorkflowID: "order", RunID: "root"}, RequestID: "start", WorkflowType: "order", Queue: o.Queue, BuildID: o.BuildID, RunTimeout: time.Hour, ExecutionTimeout: 3 * time.Hour}
	if _, err := worker.StartExecution(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	var original durable.SignalReceipt
	for _, input := range []string{"first", "second"} {
		receipt, err := worker.SignalExecution(t.Context(), durable.SignalRequest{Key: start.Key, RequestID: input, Name: "items", BuildID: o.BuildID, Input: []byte(input)})
		if err != nil {
			t.Fatal(err)
		}
		if input == "second" {
			original = receipt
		}
	}
	key := start.Key
	for number := int64(1); number <= 3; number++ {
		s = reopenAsyncStore(t, s, dsn)
		lost := &lostContinuationRuntimeStore{Store: s}
		worker = newChildRuntimeWorker(t, lost, o)
		runChildRuntimeTask(t, worker, durable.TaskWorkflow)
		e, err := s.GetExecution(t.Context(), key)
		if err != nil || e.RunNumber != number || e.FirstRunID != start.RunID || e.RunTimeout != start.RunTimeout {
			t.Fatalf("chain: %+v %v", e, err)
		}
		if number < 3 {
			if e.State != durable.StateContinuedAsNew || e.NextRunID == "" || len(lost.requests) != 2 || !reflect.DeepEqual(lost.requests[0], lost.requests[1]) {
				t.Fatalf("continuation recovery: %+v, requests=%d", e, len(lost.requests))
			}
			want := ""
			if number == 2 {
				want = "middle"
			}
			checkPostgresQuery(t, s, worker, drt.QueryRequest{Key: key, BuildID: o.BuildID, Name: "input"}, want, durable.StateContinuedAsNew)
			key.RunID = e.NextRunID
			o.BuildID, o.Queue = "v2", "next"
		} else if e.State != durable.StateCompleted || string(e.Output) != "second" {
			t.Fatalf("carried result: %+v", e)
		}
	}
	s = reopenAsyncStore(t, s, dsn)
	o.BuildID, o.Queue = start.BuildID, start.Queue
	worker = newChildRuntimeWorker(t, s, o)
	checkPostgresQuery(t, s, worker, drt.QueryRequest{Key: start.Key, BuildID: o.BuildID, Name: "input"}, "", durable.StateContinuedAsNew)
	got, err := worker.SignalExecution(t.Context(), durable.SignalRequest{Key: start.Key, RequestID: "second", Name: "items", BuildID: start.BuildID, Input: []byte("second")})
	if err != nil || got != original {
		t.Fatalf("acceptance receipt moved: %+v %v", got, err)
	}
}

func TestDurableRuntimeContinuationChildReplacement(t *testing.T) {
	for _, mode := range []string{"result", "timeout", "terminate", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, dsn := setupTestStoreConnection(t)
			o := childRuntimeOptions(t)
			o.Workflows["parent"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				policy := durable.ParentCloseTerminate
				if mode == "cancel" {
					policy = durable.ParentCloseRequestCancel
				}
				child := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{BuildID: "child-v1", Queue: "children", RunTimeout: time.Hour, ExecutionTimeout: 2 * time.Hour, ParentClosePolicy: policy})
				if mode == "terminate" || mode == "cancel" {
					_, err := child.Started()
					return nil, err
				}
				output, err := child.Get()
				if mode == "timeout" && errors.Is(err, drt.ErrChildTimedOut) {
					return []byte("handled"), nil
				}
				return output, err
			}
			o.Workflows["child"] = func(w *drt.Workflow, input []byte) ([]byte, error) {
				if len(input) == 0 {
					timeout := time.Hour
					if mode == "timeout" {
						timeout = time.Microsecond
					}
					return nil, w.ContinueAsNew([]byte("result"), drt.ContinueOptions{BuildID: "child-v2", Queue: "children-v2", RunTimeout: &timeout})
				}
				return input, nil
			}
			parent := newChildRuntimeWorker(t, s, o)
			start := durable.StartRequest{Key: durable.Key{Namespace: o.Namespace, WorkflowID: "parent", RunID: "root"}, RequestID: "start", WorkflowType: "parent", BuildID: o.BuildID, Queue: o.Queue}
			if _, err := parent.StartExecution(t.Context(), start); err != nil {
				t.Fatal(err)
			}
			runChildRuntimeTask(t, parent, durable.TaskWorkflow)
			co := o
			co.BuildID, co.Queue = "child-v1", "children"
			runChildRuntimeTask(t, newChildRuntimeWorker(t, s, co), durable.TaskWorkflow)
			s = reopenAsyncStore(t, s, dsn)
			parent = newChildRuntimeWorker(t, s, o)
			if worked, err := parent.RunOnce(t.Context(), drt.TaskChildDelivery); err != nil || worked {
				t.Fatalf("premature child result: %t %v", worked, err)
			}
			co.BuildID, co.Queue = "child-v2", "children-v2"
			child := newChildRuntimeWorker(t, s, co)
			switch mode {
			case "result":
				runChildRuntimeTask(t, child, durable.TaskWorkflow)
			case "timeout":
				runChildRuntimeTask(t, parent, drt.TaskExecutionTimeout)
			default:
				runChildRuntimeTask(t, parent, durable.TaskWorkflow)
				runChildRuntimeTask(t, child, drt.TaskChildDelivery)
				if mode == "cancel" {
					runChildRuntimeTask(t, child, durable.TaskWorkflow)
					runChildRuntimeTask(t, child, durable.TaskWorkflow)
				}
			}
			s = reopenAsyncStore(t, s, dsn)
			parent = newChildRuntimeWorker(t, s, o)
			runChildRuntimeTask(t, parent, drt.TaskChildDelivery)
			if mode == "result" || mode == "timeout" {
				runChildRuntimeTask(t, parent, durable.TaskWorkflow)
			}
			e, err := s.GetExecution(t.Context(), start.Key)
			want := ""
			if mode == "result" {
				want = "result"
			} else if mode == "timeout" {
				want = "handled"
			}
			if err != nil || e.State != durable.StateCompleted || string(e.Output) != want {
				t.Fatalf("parent result: %+v %v", e, err)
			}
			link, err := s.GetChildExecution(t.Context(), start.Key, "child")
			state := durable.StateCompleted
			switch mode {
			case "timeout":
				state = durable.StateTimedOut
			case "terminate":
				state = durable.StateTerminated
			case "cancel":
				state = durable.StateCancelled
			}
			if err != nil || link.State != state || link.CurrentKey == link.Start.Key {
				t.Fatalf("child chain: %+v %v", link, err)
			}
		})
	}
}
