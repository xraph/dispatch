package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

// Only the two real read methods are exposed. Any other Store method panics
// through the nil embedded interface, so a query cannot quietly mutate storage.
type queryReadStore struct {
	durable.Store
	reader      durable.Store
	reads       int
	onExecution func(*durable.Execution)
	onPage      func(int64)
	pageFault   string
}

func (s *queryReadStore) GetExecution(ctx context.Context, key durable.Key) (durable.Execution, error) {
	s.reads++
	execution, err := s.reader.GetExecution(ctx, key)
	if err == nil && s.onExecution != nil {
		s.onExecution(&execution)
	}
	return execution, err
}
func (s *queryReadStore) ReadHistory(ctx context.Context, key durable.Key, after int64, limit int) ([]durable.Event, error) {
	s.reads++
	if s.onPage != nil {
		s.onPage(after)
	}
	events, err := s.reader.ReadHistory(ctx, key, after, limit)
	if s.pageFault == "missing" {
		return nil, nil
	}
	if s.pageFault == "oversized" && len(events) > 0 {
		events = append(events, events[0])
	}
	return events, err
}

func TestQueryClientDoesNotWriteOrClaim(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.BuildID = strings.Repeat("b", 512)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		state := "pending"
		w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
		value, err := w.ReceiveSignal("approval", "approve").Get()
		state = string(value)
		return value, err
	}
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	reader := &queryReadStore{reader: s}
	queryOptions := options
	queryOptions.Queue = "query-clients"
	client := newWorker(t, reader, queryOptions)
	request := drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"}
	check := func(want string) {
		t.Helper()
		before, err := s.GetExecution(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		taskBefore, err := s.GetTask(t.Context(), key, "workflow:1")
		if err != nil {
			t.Fatal(err)
		}
		eventsBefore, err := s.ReadHistory(t.Context(), key, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		result, err := client.QueryExecution(t.Context(), request)
		if err != nil || string(result.Output) != want || result.Revision != before.Revision || result.LastSequence != before.LastSequence || result.State != before.State {
			t.Fatalf("query result: %+v %v", result, err)
		}
		after, err := s.GetExecution(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		taskAfter, err := s.GetTask(t.Context(), key, "workflow:1")
		if err != nil {
			t.Fatal(err)
		}
		eventsAfter, err := s.ReadHistory(t.Context(), key, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(taskBefore, taskAfter) || !reflect.DeepEqual(eventsBefore, eventsAfter) {
			t.Fatal("query mutated durable state")
		}
	}
	check("pending")
	runTask(t, worker, durable.TaskWorkflow)
	check("pending")
	if _, err := worker.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "approval", BuildID: options.BuildID, Name: "approve", Input: []byte("approved")}); err != nil {
		t.Fatal(err)
	}
	check("approved")
	runTask(t, worker, durable.TaskWorkflow)
	check("approved")
}

func TestQueryKeepsInitialPagedSnapshot(t *testing.T) {
	for _, mode := range []string{"signal", "closure"} {
		t.Run(mode, func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				state := "pending"
				w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
				if mode == "closure" {
					return nil, nil
				}
				value, err := w.ReceiveSignal("approval", "approve").Get()
				state = string(value)
				return value, err
			}
			worker := newWorker(t, s, options)
			key := startWorkerRun(t, worker, options)
			task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: options.Namespace, Queue: options.Queue, BuildID: options.BuildID, Kind: durable.TaskWorkflow, Owner: "fixture", LeaseDuration: time.Minute})
			if err != nil || task == nil {
				t.Fatalf("fixture claim: %+v %v", task, err)
			}
			for i, count := range []int{999, 2} {
				events := make([]durable.EventInput, count)
				for j := range events {
					events[j].Type = drt.EventWorkflowWaiting
				}
				if _, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: key, RequestID: fmt.Sprintf("fixture-%d", i), Token: task.Token(), ExpectedRevision: int64(i + 1), Events: events, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep}}); err != nil {
					t.Fatal(err)
				}
			}
			changed := false
			reader := &queryReadStore{reader: s, onPage: func(after int64) {
				if after == 0 || changed {
					return
				}
				changed = true
				if mode == "signal" {
					_, err = s.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "concurrent", BuildID: options.BuildID, Name: "approve", Input: []byte("approved")})
				} else {
					_, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: key, RequestID: "close", ExpectedRevision: 3, Token: task.Token(), Events: []durable.EventInput{{Type: drt.EventWorkflowCompleted}}, State: durable.StateCompleted})
				}
				if err != nil {
					t.Fatal(err)
				}
			}}
			client := newWorker(t, reader, options)
			request := drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"}
			first, err := client.QueryExecution(t.Context(), request)
			if err != nil || !changed || first.Revision != 3 || first.LastSequence != 1002 || first.State != durable.StateRunning || string(first.Output) != "pending" {
				t.Fatalf("mixed snapshot: %+v changed=%t %v", first, changed, err)
			}
			next, err := client.QueryExecution(t.Context(), request)
			if err != nil || next.Revision != 4 || next.LastSequence != 1003 {
				t.Fatalf("later snapshot: %+v %v", next, err)
			}
			if mode == "signal" && string(next.Output) != "approved" || mode == "closure" && next.State != durable.StateCompleted {
				t.Fatalf("later state not observed: %+v", next)
			}
		})
	}
}

func TestQueryClientCancellation(t *testing.T) {
	for _, mode := range []string{"before", "snapshot", "replay", "query"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := memory.New()
			options := workerOptions(t)
			queryCalled := false
			options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
				w.SetQueryHandler("status", func(_ []byte) ([]byte, error) {
					queryCalled = true
					if mode == "query" {
						cancel()
					}
					return []byte("success"), nil
				})
				if mode == "replay" {
					cancel()
				}
				return nil, nil
			}
			key := startWorkerRun(t, newWorker(t, s, options), options)
			reader := &queryReadStore{reader: s, onExecution: func(_ *durable.Execution) {
				if mode == "snapshot" {
					cancel()
				}
			}}
			if mode == "before" {
				cancel()
			}
			result, err := newWorker(t, reader, options).QueryExecution(ctx, drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"})
			if !errors.Is(err, context.Canceled) || result.Revision != 0 || len(result.Output) != 0 {
				t.Fatalf("cancelled query returned success: %+v %v", result, err)
			}
			if mode == "before" && reader.reads != 0 {
				t.Fatal("cancelled query performed reads")
			}
			if mode != "query" && queryCalled {
				t.Fatal("query handler ran after cancellation")
			}
		})
	}
}

func TestQueryClientRejectsRoutingAndBadSnapshots(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		w.SetQueryHandler("status", func(input []byte) ([]byte, error) { return input, nil })
		return nil, nil
	}
	key := startWorkerRun(t, newWorker(t, s, options), options)
	for _, mode := range []string{"namespace", "build", "missing_code", "wrong_key", "wrong_build", "missing_page", "oversized_page"} {
		t.Run(mode, func(t *testing.T) {
			reader := &queryReadStore{reader: s}
			request := drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"}
			clientOptions := options
			want := durable.ErrInvalid
			switch mode {
			case "namespace":
				request.Namespace = "other"
			case "build":
				request.BuildID = "other"
			case "missing_code":
				clientOptions.Workflows = nil
				want = drt.ErrHandlerNotFound
			case "wrong_key":
				reader.onExecution = func(e *durable.Execution) { e.RunID = "other" }
			case "wrong_build":
				reader.onExecution = func(e *durable.Execution) { e.BuildID = "other" }
			case "missing_page":
				reader.pageFault = "missing"
				want = drt.ErrHistory
			case "oversized_page":
				reader.pageFault = "oversized"
				want = drt.ErrHistory
			}
			if _, err := newWorker(t, reader, clientOptions).QueryExecution(t.Context(), request); !errors.Is(err, want) {
				t.Fatalf("query rejection: %v", err)
			}
			if (mode == "namespace" || mode == "build") && reader.reads != 0 {
				t.Fatal("foreign query read storage")
			}
		})
	}
	input := []byte("input")
	reader := &queryReadStore{reader: s, onExecution: func(_ *durable.Execution) { input[0] = 'X' }}
	result, err := newWorker(t, reader, options).QueryExecution(t.Context(), drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status", Input: input})
	if err != nil || string(result.Output) != "input" {
		t.Fatalf("query did not own input before reads: %+v %v", result, err)
	}
}

func TestQueryConcurrentCallsUsePrivateReplay(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		count := 0
		w.SetQueryHandler("count", func(_ []byte) ([]byte, error) { count++; return []byte(fmt.Sprint(count)), nil })
		return w.ReceiveSignal("approval", "approve").Get()
	}
	worker := newWorker(t, s, options)
	key := startWorkerRun(t, worker, options)
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			result, err := worker.QueryExecution(t.Context(), drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "count"})
			if err != nil || string(result.Output) != "1" || result.Revision != 1 {
				t.Errorf("shared query state: %+v %v", result, err)
			}
		})
	}
	group.Wait()
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.Revision != 1 || execution.LastSequence != 1 {
		t.Fatalf("concurrent queries changed execution: %+v %v", execution, err)
	}
}
