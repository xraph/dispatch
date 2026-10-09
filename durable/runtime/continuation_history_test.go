package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func TestContinuationRejectsCorruptHistory(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return nil, w.ContinueAsNew(nil, drt.ContinueOptions{})
	}
	w := newWorker(t, s, o)
	key := startWorkerRun(t, w, o)
	if _, err := w.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "signal", Name: "item", BuildID: o.BuildID}); err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskWorkflow)
	root, rootEvents := continuationSnapshot(t, s, key)
	key.RunID = root.NextRunID
	next, nextEvents := continuationSnapshot(t, s, key)
	for _, mode := range []string{"terminal input", "terminal next", "capture options", "capture index", "lineage root", "lineage time", "missing lineage", "carry source", "carry time", "carry duplicate", "carry late"} {
		t.Run(mode, func(t *testing.T) {
			e, events := root, slices.Clone(rootEvents)
			switch mode {
			case "terminal input", "terminal next":
				var v durable.ContinuedRun
				if err := json.Unmarshal(events[len(events)-1].Payload, &v); err != nil {
					t.Fatal(err)
				}
				if mode == "terminal input" {
					v.Next.Input = []byte("changed")
				} else {
					e.NextRunID = "changed"
				}
				events[len(events)-1].Payload = encode(t, v)
			case "capture options", "capture index":
				var v drt.Continuation
				if err := json.Unmarshal(events[len(events)-2].Payload, &v); err != nil {
					t.Fatal(err)
				}
				if mode == "capture options" {
					v.Options.BuildID = "changed"
				} else {
					v.CommandCount++
				}
				events[len(events)-2].Payload = encode(t, v)
			default:
				e, events = next, slices.Clone(nextEvents)
				switch mode {
				case "lineage root", "lineage time":
					var v durable.RunStarted
					if err := json.Unmarshal(events[1].Payload, &v); err != nil {
						t.Fatal(err)
					}
					if mode == "lineage root" {
						v.Run.FirstRunID = "changed"
					} else {
						v.Run.CreatedAt = v.Run.CreatedAt.Add(time.Second)
					}
					events[1].Payload = encode(t, v)
				case "missing lineage":
					events[1].Type, events[1].Payload = drt.EventWorkflowWaiting, nil
				case "carry source", "carry time":
					var v durable.CarriedSignal
					if err := json.Unmarshal(events[2].Payload, &v); err != nil {
						t.Fatal(err)
					}
					if mode == "carry source" {
						v.Source.Namespace = "other"
					} else {
						v.Time = e.CreatedAt.Add(time.Second)
					}
					events[2].Payload = encode(t, v)
				case "carry duplicate":
					events = append(events, events[2])
					events[3].Sequence, e.LastSequence = 4, 4
				case "carry late":
					events = append(events, events[2])
					events[2].Type, events[2].Payload = drt.EventWorkflowWaiting, nil
					events[3].Sequence, e.LastSequence = 4, 4
				}
			}
			if _, err := drt.Evaluate(e, events, o.Workflows["order"]); !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("corrupt history accepted: %v", err)
			}
		})
	}
}

type continuationRetryStore struct {
	durable.Store
	requests []durable.CommitRequest
}

func (s *continuationRetryStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	receipt, err := s.Store.CommitTransition(ctx, r)
	if r.Continuation != nil {
		s.requests = append(s.requests, r)
		if err == nil && len(s.requests) == 1 {
			return durable.Receipt{}, errors.New("response lost")
		}
	}
	return receipt, err
}

func TestContinuationCommitResponseLoss(t *testing.T) {
	s := &continuationRetryStore{Store: memory.New()}
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return nil, w.ContinueAsNew(nil, drt.ContinueOptions{})
	}
	w := newWorker(t, s, o)
	key := startWorkerRun(t, w, o)
	runTask(t, w, durable.TaskWorkflow)
	if len(s.requests) != 2 || !reflect.DeepEqual(s.requests[0], s.requests[1]) {
		t.Fatal("continuation retry changed its request")
	}
	e, _ := continuationSnapshot(t, s, key)
	key.RunID = e.NextRunID
	next, events := continuationSnapshot(t, s, key)
	if next.RunNumber != 2 || len(events) != 2 {
		t.Fatalf("retry duplicated successor: %+v %+v", next, events)
	}
}

func TestContinuationParentCloseFollowsChildBuild(t *testing.T) {
	for _, policy := range []durable.ParentClosePolicy{durable.ParentCloseTerminate, durable.ParentCloseRequestCancel} {
		t.Run(string(policy), func(t *testing.T) {
			s := memory.New()
			o := workerOptions(t)
			o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				if _, err := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{BuildID: "child-v1", Queue: "children", ParentClosePolicy: policy}).Started(); err != nil {
					return nil, err
				}
				return nil, nil
			}
			parent := newWorker(t, s, o)
			key := startWorkerRun(t, parent, o)
			runTask(t, parent, durable.TaskWorkflow)
			co := childWorkerOptions(t, o)
			co.Workflows["child"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				return nil, w.ContinueAsNew(nil, drt.ContinueOptions{BuildID: "child-v2", Queue: "children-v2"})
			}
			runTask(t, newWorker(t, s, co), durable.TaskWorkflow)
			runTask(t, parent, durable.TaskWorkflow)
			co.BuildID, co.Queue = "child-v2", "children-v2"
			child := newWorker(t, s, co)
			runTask(t, child, drt.TaskChildDelivery)
			want := durable.StateTerminated
			if policy == durable.ParentCloseRequestCancel {
				runTask(t, child, durable.TaskWorkflow)
				runTask(t, child, durable.TaskWorkflow)
				want = durable.StateCancelled
			}
			link, err := s.GetChildExecution(t.Context(), key, "child")
			if err != nil || link.State != want || link.CurrentKey == link.Start.Key {
				t.Fatalf("close missed child successor: %+v %v", link, err)
			}
		})
	}
}

func TestContinuationChildFinalRunTimeout(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		_, err := w.ChildWorkflow("child", "child", nil, drt.ChildOptions{Queue: "children", BuildID: "child-v1", RunTimeout: time.Hour, ExecutionTimeout: 2 * time.Hour}).Get()
		if !errors.Is(err, drt.ErrChildTimedOut) {
			return nil, errors.New("expected final child timeout")
		}
		return []byte("handled"), nil
	}
	parent := newWorker(t, s, o)
	key := startWorkerRun(t, parent, o)
	runTask(t, parent, durable.TaskWorkflow)
	co := childWorkerOptions(t, o)
	co.Workflows["child"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		timeout := time.Microsecond
		return nil, w.ContinueAsNew(nil, drt.ContinueOptions{RunTimeout: &timeout, BuildID: "retired"})
	}
	runTask(t, newWorker(t, s, co), durable.TaskWorkflow)
	runTask(t, parent, drt.TaskExecutionTimeout)
	runTask(t, parent, drt.TaskChildDelivery)
	runTask(t, parent, durable.TaskWorkflow)
	e, events := continuationSnapshot(t, s, key)
	if e.State != durable.StateCompleted || string(e.Output) != "handled" {
		t.Fatalf("final timeout not handled: %+v", e)
	}
	for _, mode := range []string{"first start", "execution deadline", "run timeout", "future run", "root"} {
		t.Run(mode, func(t *testing.T) {
			changed := slices.Clone(events)
			for i, event := range changed {
				if event.Type != durable.EventChildCompleted {
					continue
				}
				var v durable.ChildMessage
				if err := json.Unmarshal(event.Payload, &v); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "first start":
					v.FinalRun.FirstStartedAt = v.FinalRun.FirstStartedAt.Add(-time.Second)
				case "execution deadline":
					v.FinalRun.ExecutionDeadlineAt = v.FinalRun.ExecutionDeadlineAt.Add(time.Hour)
				case "run timeout":
					v.FinalRun.RunTimeout = time.Hour
				case "future run":
					v.FinalRun.CreatedAt = event.Time.Add(time.Hour)
				case "root":
					v.FinalRun.FirstRunID = "other"
				}
				changed[i].Payload = encode(t, v)
			}
			if _, err := drt.Evaluate(e, changed, o.Workflows["order"]); !errors.Is(err, drt.ErrHistory) {
				t.Fatalf("invalid final-run metadata accepted: %v", err)
			}
		})
	}
}

type continuationInputRaceStore struct {
	durable.Store
	cancel   bool
	injected bool
}

func (s *continuationInputRaceStore) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	if r.Continuation != nil && !s.injected {
		s.injected = true
		execution, err := s.GetExecution(ctx, r.Key)
		if err != nil {
			return durable.Receipt{}, err
		}
		if s.cancel {
			_, err = s.RequestCancelExecution(ctx, durable.CancelExecutionRequest{Key: r.Key, RequestID: "racing-cancel", BuildID: execution.BuildID})
		} else {
			_, err = s.SignalExecution(ctx, durable.SignalRequest{Key: r.Key, RequestID: "racing-signal", Name: "items", BuildID: execution.BuildID, Input: []byte("racing input")})
		}
		if err != nil {
			return durable.Receipt{}, err
		}
	}
	return s.Store.CommitTransition(ctx, r)
}

func TestContinuationRebuildsAfterAcceptedInput(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "signal", true: "cancellation"}[cancel], func(t *testing.T) {
			s := &continuationInputRaceStore{Store: memory.New(), cancel: cancel}
			o := workerOptions(t)
			o.Workflows["order"] = func(w *drt.Workflow, input []byte) ([]byte, error) {
				if len(input) == 0 {
					return nil, w.ContinueAsNew([]byte("next"), drt.ContinueOptions{})
				}
				return w.ReceiveSignal("item", "items").Get()
			}
			w := newWorker(t, s, o)
			key := startWorkerRun(t, w, o)
			runTask(t, w, durable.TaskWorkflow)
			runTask(t, w, durable.TaskWorkflow)
			e, _ := continuationSnapshot(t, s, key)
			if cancel {
				if e.State != durable.StateCancelled || e.NextRunID != "" {
					t.Fatalf("cancellation lost: %+v", e)
				}
			} else {
				key.RunID = e.NextRunID
				next, _ := continuationSnapshot(t, s, key)
				if next.State != durable.StateCompleted || string(next.Output) != "racing input" {
					t.Fatalf("signal lost: %+v", next)
				}
			}
		})
	}
}

func TestContinuationCarriedSelectorOrderAndClock(t *testing.T) {
	s := memory.New()
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, input []byte) ([]byte, error) {
		if len(input) == 0 {
			for _, id := range []string{"a", "b", "c", "d", "e"} {
				w.Activity(id, "unused", "", nil)
			}
			if _, err := w.ReceiveSignal("go", "go").Get(); err != nil {
				return nil, err
			}
			return nil, w.ContinueAsNew([]byte("next"), drt.ContinueOptions{})
		}
		now := w.Now()
		carried, current := w.ReceiveSignal("carried", "old"), w.ReceiveSignal("current", "new")
		value, err := w.Select("first", current, carried).Get()
		if !w.Now().Equal(now) {
			return nil, errors.New("carried message moved logical time backward")
		}
		return value, err
	}
	w := newWorker(t, s, o)
	key := startWorkerRun(t, w, o)
	runTask(t, w, durable.TaskWorkflow)
	for _, name := range []string{"go", "old"} {
		if _, err := w.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: name, Name: name, BuildID: o.BuildID, Input: []byte(name)}); err != nil {
			t.Fatal(err)
		}
	}
	runTask(t, w, durable.TaskWorkflow)
	root, _ := continuationSnapshot(t, s, key)
	key.RunID = root.NextRunID
	if _, err := w.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "new", Name: "new", BuildID: o.BuildID, Input: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskWorkflow)
	e, events := continuationSnapshot(t, s, key)
	if e.State != durable.StateCompleted || string(e.Output) != "old" {
		t.Fatalf("carry order/clock lost: %+v", e)
	}
	if _, err := drt.Evaluate(e, events, o.Workflows["order"]); err != nil {
		t.Fatal(err)
	}
}
