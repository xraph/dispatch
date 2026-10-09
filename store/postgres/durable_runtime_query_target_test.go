//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
)

type postgresSelectedQueryStore struct {
	durable.Store
	afterResolve func()
	keys         []durable.Key
}

func (s *postgresSelectedQueryStore) ResolveExecution(ctx context.Context, r durable.ExecutionTarget) (durable.Execution, error) {
	e, err := s.Store.ResolveExecution(ctx, r)
	if err == nil && s.afterResolve != nil {
		after := s.afterResolve
		s.afterResolve = nil
		after()
	}
	return e, err
}
func (s *postgresSelectedQueryStore) ReadHistory(ctx context.Context, key durable.Key, after int64, limit int) ([]durable.Event, error) {
	s.keys = append(s.keys, key)
	return s.Store.ReadHistory(ctx, key, after, limit)
}

func TestDurableRuntimeQueryTargets(t *testing.T) {
	for _, selection := range []durable.RunSelection{durable.RunCurrent, durable.RunLatest} {
		t.Run(string(selection), func(t *testing.T) {
			s, dsn := setupTestStoreConnection(t)
			options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "v1", Owner: "worker", StoreTimeout: 5 * time.Second, Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, input []byte) ([]byte, error) {
				w.SetQueryHandler("status", func([]byte) ([]byte, error) { return input, nil })
				return input, nil
			}}}
			worker, err := drt.NewWorker(s, options)
			if err != nil {
				t.Fatal(err)
			}
			first := durable.StartRequest{Key: durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "first"}, RequestID: "first", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue, Input: []byte("first")}
			if _, err = worker.StartExecution(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			next := first
			next.RunID = "next"
			next.RequestID = "next"
			next.Input = []byte("next")
			reader := &postgresSelectedQueryStore{Store: s}
			reader.afterResolve = func() {
				runQueryTask(t, worker, durable.TaskWorkflow)
				if _, startErr := worker.StartExecution(t.Context(), next); startErr != nil {
					t.Fatal(startErr)
				}
				s = reopenAsyncStore(t, s, dsn)
				reader.Store = s
			}
			client, err := drt.NewWorker(reader, options)
			if err != nil {
				t.Fatal(err)
			}
			request := drt.QueryRequest{Key: durable.Key{Namespace: first.Namespace, WorkflowID: first.WorkflowID}, Selection: selection, BuildID: options.BuildID, Name: "status"}
			result, err := client.QueryExecution(t.Context(), request)
			if err != nil || result.Key != first.Key || result.State != durable.StateRunning || result.Revision != 1 || result.LastSequence != 1 || string(result.Output) != "first" {
				t.Fatalf("selected snapshot: %+v %v", result, err)
			}
			if len(reader.keys) != 1 || reader.keys[0] != first.Key {
				t.Fatalf("mixed history: %+v", reader.keys)
			}
			result, err = client.QueryExecution(t.Context(), request)
			if err != nil || result.Key != next.Key || string(result.Output) != "next" {
				t.Fatalf("replacement: %+v %v", result, err)
			}
			runQueryTask(t, client, durable.TaskWorkflow)
			s = reopenAsyncStore(t, s, dsn)
			reader.Store = s
			request.Selection = durable.RunLatest
			result, err = client.QueryExecution(t.Context(), request)
			if err != nil || result.Key != next.Key || result.State != durable.StateCompleted || string(result.Output) != "next" {
				t.Fatalf("closed latest: %+v %v", result, err)
			}
			request.Selection = durable.RunCurrent
			if _, err = client.QueryExecution(t.Context(), request); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("closed current: %v", err)
			}
			request.Key, request.Selection = first.Key, durable.RunExplicit
			result, err = client.QueryExecution(t.Context(), request)
			if err != nil || result.Key != first.Key || result.State != durable.StateCompleted || string(result.Output) != "first" {
				t.Fatalf("prior explicit: %+v %v", result, err)
			}
		})
	}
}
