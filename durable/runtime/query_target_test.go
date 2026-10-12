package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

// Only selection and history reads are available; a second projection read or
// any mutation panics through the nil embedded Store.
type selectedQueryStore struct {
	durable.Store
	reader       durable.Store
	afterResolve func(*durable.Execution)
	resolveErr   error
	resolutions  int
	keys         []durable.Key
}

func (s *selectedQueryStore) ResolveExecution(ctx context.Context, r durable.ExecutionTarget) (durable.Execution, error) {
	s.resolutions++
	if s.resolveErr != nil {
		return durable.Execution{}, s.resolveErr
	}
	e, err := s.reader.ResolveExecution(ctx, r)
	if err == nil && s.afterResolve != nil {
		s.afterResolve(&e)
	}
	return e, err
}
func (s *selectedQueryStore) ReadHistory(ctx context.Context, key durable.Key, after int64, limit int) ([]durable.Event, error) {
	s.keys = append(s.keys, key)
	return s.reader.ReadHistory(ctx, key, after, limit)
}
func selectedQuery(key durable.Key, selection durable.RunSelection) drt.QueryRequest {
	key.RunID = ""
	return drt.QueryRequest{Key: key, Selection: selection, BuildID: "v1", Name: "status"}
}
func targetWorkflow(w *drt.Workflow, input []byte) ([]byte, error) {
	w.SetQueryHandler("status", func(query []byte) ([]byte, error) { return append(append([]byte(nil), input...), query...), nil })
	return input, nil
}
func startTargetQuery(t *testing.T, s durable.Store) (*drt.Worker, drt.Options, durable.StartRequest) {
	t.Helper()
	options := workerOptions(t)
	options.Workflows["order"] = targetWorkflow
	worker := newWorker(t, s, options)
	r := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "first"}, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue, Input: []byte("first")}
	if _, err := worker.StartExecution(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	return worker, options, r
}

func TestQueryTargetSelection(t *testing.T) {
	s := memory.New()
	worker, options, first := startTargetQuery(t, s)
	for _, selection := range []durable.RunSelection{durable.RunCurrent, durable.RunLatest} {
		before, _ := s.GetExecution(t.Context(), first.Key)
		taskBefore, _ := s.GetTask(t.Context(), first.Key, "workflow:1")
		reader := &selectedQueryStore{reader: s}
		client := newWorker(t, reader, options)
		got, err := client.QueryExecution(t.Context(), selectedQuery(first.Key, selection))
		if err != nil || got.Key != first.Key || got.State != durable.StateRunning || string(got.Output) != "first" || got.Revision != 1 || got.LastSequence != 1 {
			t.Fatalf("selected: %+v %v", got, err)
		}
		after, _ := s.GetExecution(t.Context(), first.Key)
		taskAfter, _ := s.GetTask(t.Context(), first.Key, "workflow:1")
		if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(taskBefore, taskAfter) || reader.resolutions != 1 {
			t.Fatal("query mutated state or resolved twice")
		}
	}
	runTask(t, worker, durable.TaskWorkflow)
	if _, err := worker.QueryExecution(t.Context(), selectedQuery(first.Key, durable.RunCurrent)); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("closed current: %v", err)
	}
	got, err := worker.QueryExecution(t.Context(), selectedQuery(first.Key, durable.RunLatest))
	if err != nil || got.State != durable.StateCompleted {
		t.Fatalf("closed latest: %+v %v", got, err)
	}
	next := first
	next.RunID = "second"
	next.RequestID = "second"
	next.Input = []byte("second")
	if _, err = worker.StartExecution(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	got, err = worker.QueryExecution(t.Context(), selectedQuery(first.Key, durable.RunLatest))
	if err != nil || got.Key != next.Key || string(got.Output) != "second" {
		t.Fatalf("new latest: %+v %v", got, err)
	}
	got, err = worker.QueryExecution(t.Context(), drt.QueryRequest{Key: first.Key, BuildID: options.BuildID, Name: "status"})
	if err != nil || got.Key != first.Key || string(got.Output) != "first" {
		t.Fatalf("explicit prior: %+v %v", got, err)
	}
}

func TestQueryTargetReplacementUsesSelectedSnapshot(t *testing.T) {
	for _, selection := range []durable.RunSelection{durable.RunCurrent, durable.RunLatest} {
		t.Run(string(selection), func(t *testing.T) {
			s := memory.New()
			worker, options, first := startTargetQuery(t, s)
			next := first
			next.RunID = "replacement"
			next.RequestID = "replacement"
			next.Input = []byte("replacement")
			request := selectedQuery(first.Key, selection)
			request.Input = []byte("-query")
			reader := &selectedQueryStore{reader: s, afterResolve: func(*durable.Execution) {
				request.Input[0] = 'X'
				runTask(t, worker, durable.TaskWorkflow)
				if _, err := worker.StartExecution(t.Context(), next); err != nil {
					t.Fatal(err)
				}
			}}
			client := newWorker(t, reader, options)
			got, err := client.QueryExecution(t.Context(), request)
			if err != nil || got.Key != first.Key || got.State != durable.StateRunning || got.Revision != 1 || got.LastSequence != 1 || string(got.Output) != "first-query" {
				t.Fatalf("retargeted: %+v %v", got, err)
			}
			if reader.resolutions != 1 || len(reader.keys) != 1 || reader.keys[0] != first.Key {
				t.Fatalf("reads: %+v", reader)
			}
			got, err = worker.QueryExecution(t.Context(), selectedQuery(first.Key, durable.RunLatest))
			if err != nil || got.Key != next.Key {
				t.Fatalf("replacement: %+v %v", got, err)
			}
		})
	}
}

func TestQueryTargetRejectsWrongSnapshot(t *testing.T) {
	for _, kind := range []string{"namespace", "workflow", "run", "build", "state", "handler"} {
		t.Run(kind, func(t *testing.T) {
			s := memory.New()
			_, options, first := startTargetQuery(t, s)
			called := false
			options.Workflows["order"] = func(*drt.Workflow, []byte) ([]byte, error) { called = true; return nil, nil }
			reader := &selectedQueryStore{reader: s, afterResolve: func(e *durable.Execution) {
				switch kind {
				case "namespace":
					e.Namespace = "other"
				case "workflow":
					e.WorkflowID = "other"
				case "run":
					e.RunID = ""
				case "build":
					e.BuildID = "v2"
				case "state":
					e.State = durable.StateCompleted
				case "handler":
					e.WorkflowType = "unknown"
				}
			}}
			client := newWorker(t, reader, options)
			_, err := client.QueryExecution(t.Context(), selectedQuery(first.Key, durable.RunCurrent))
			want := durable.ErrInvalid
			if kind == "handler" {
				want = drt.ErrHandlerNotFound
			}
			if !errors.Is(err, want) || called {
				t.Fatalf("wrong snapshot: %v called=%t", err, called)
			}
			if kind != "handler" && len(reader.keys) != 0 {
				t.Fatal("invalid snapshot read history")
			}
		})
	}
}

func TestQueryTargetDoesNotFallBackToCompatibleBuild(t *testing.T) {
	s := memory.New()
	worker, _, first := startTargetQuery(t, s)
	runTask(t, worker, durable.TaskWorkflow)
	next := first
	next.RunID = "v2-run"
	next.RequestID = "v2"
	next.BuildID = "v2"
	if _, err := s.StartExecution(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	for _, selection := range []durable.RunSelection{durable.RunCurrent, durable.RunLatest} {
		if _, err := worker.QueryExecution(t.Context(), selectedQuery(first.Key, selection)); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("build fallback: %v", err)
		}
	}
}

func TestQueryTargetValidationAndReadErrors(t *testing.T) {
	s := memory.New()
	_, options, first := startTargetQuery(t, s)
	for _, readErr := range []error{durable.ErrNotFound, durable.ErrAmbiguousRun, context.Canceled} {
		reader := &selectedQueryStore{reader: s, resolveErr: readErr}
		client := newWorker(t, reader, options)
		if _, err := client.QueryExecution(t.Context(), selectedQuery(first.Key, durable.RunLatest)); !errors.Is(err, readErr) {
			t.Fatalf("read error: %v", err)
		}
	}
	for _, kind := range []string{"mixed", "unknown", "namespace", "build", "missing_run"} {
		request := selectedQuery(first.Key, durable.RunLatest)
		switch kind {
		case "mixed":
			request.RunID = first.RunID
		case "unknown":
			request.Selection = "unknown"
		case "namespace":
			request.Namespace = "other"
		case "build":
			request.BuildID = "other"
		case "missing_run":
			request.Selection = durable.RunExplicit
		}
		reader := &selectedQueryStore{reader: s}
		client := newWorker(t, reader, options)
		if _, err := client.QueryExecution(t.Context(), request); !errors.Is(err, durable.ErrInvalid) || reader.resolutions != 0 {
			t.Fatalf("invalid %s: %v reads=%d", kind, err, reader.resolutions)
		}
	}
	e, err := s.GetExecution(t.Context(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	history, err := s.ReadHistory(t.Context(), first.Key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = drt.EvaluateQuery(e, history, targetWorkflow, selectedQuery(first.Key, durable.RunLatest)); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("pure query accepted selector: %v", err)
	}
}
