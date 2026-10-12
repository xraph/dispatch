//go:build integration

package postgres_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/postgres"
)

func TestDurableRuntimeQueryRecovery(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "completed"
		if failed {
			name = "failed"
		}
		t.Run(name, func(t *testing.T) {
			s, dsn := setupTestStoreConnection(t)
			options := drt.Options{Namespace: t.Name(), Queue: "orders", BuildID: "query-v1", Owner: "first", Workflows: map[string]drt.WorkflowFunc{"order": func(w *drt.Workflow, _ []byte) ([]byte, error) {
				state := "pending"
				w.SetQueryHandler("status", func(_ []byte) ([]byte, error) { return []byte(state), nil })
				approved, err := w.ReceiveSignal("approval", "approve").Get()
				if err != nil {
					return nil, err
				}
				state = string(approved)
				result, err := w.Activity("charge", "charge", "", approved).Get()
				if err != nil {
					state = err.Error()
				} else {
					state = string(result)
				}
				return result, err
			}}, Activities: map[string]drt.ActivityFunc{"charge": func(_ context.Context, _ drt.ActivityInfo, _ []byte) ([]byte, error) {
				if failed {
					return nil, &drt.ApplicationError{Type: "declined", Message: "declined"}
				}
				return []byte("paid"), nil
			}}}
			worker, err := drt.NewWorker(s, options)
			if err != nil {
				t.Fatal(err)
			}
			key := durable.Key{Namespace: options.Namespace, WorkflowID: "order", RunID: "run"}
			if _, err = worker.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: options.BuildID, Queue: options.Queue}); err != nil {
				t.Fatal(err)
			}
			request := drt.QueryRequest{Key: key, BuildID: options.BuildID, Name: "status"}
			checkPostgresQuery(t, s, worker, request, "pending", durable.StateRunning)
			runQueryTask(t, worker, durable.TaskWorkflow)
			checkPostgresQuery(t, s, worker, request, "pending", durable.StateRunning)
			if _, err = worker.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "approval", BuildID: options.BuildID, Name: "approve", Input: []byte("approved")}); err != nil {
				t.Fatal(err)
			}
			checkPostgresQuery(t, s, worker, request, "approved", durable.StateRunning)
			s = reopenAsyncStore(t, s, dsn)
			options.Owner = "replacement"
			worker, err = drt.NewWorker(s, options)
			if err != nil {
				t.Fatal(err)
			}
			checkPostgresQuery(t, s, worker, request, "approved", durable.StateRunning)
			runQueryTask(t, worker, durable.TaskWorkflow)
			runQueryTask(t, worker, durable.TaskActivity)
			want, state := "paid", durable.StateCompleted
			if failed {
				want, state = "declined", durable.StateFailed
			}
			checkPostgresQuery(t, s, worker, request, want, durable.StateRunning)
			runQueryTask(t, worker, durable.TaskWorkflow)
			s = reopenAsyncStore(t, s, dsn)
			worker, err = drt.NewWorker(s, options)
			if err != nil {
				t.Fatal(err)
			}
			checkPostgresQuery(t, s, worker, request, want, state)
		})
	}
}

func runQueryTask(t *testing.T, worker *drt.Worker, kind durable.TaskKind) {
	t.Helper()
	if worked, err := worker.RunOnce(t.Context(), kind); err != nil || !worked {
		t.Fatalf("run %s: %t %v", kind, worked, err)
	}
}

func checkPostgresQuery(t *testing.T, s *postgres.Store, worker *drt.Worker, request drt.QueryRequest, want string, state durable.State) {
	t.Helper()
	before, err := s.GetExecution(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	eventsBefore, err := s.ReadHistory(t.Context(), request.Key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	tasksBefore := queryTaskDigest(t, s, request.Key)
	result, err := worker.QueryExecution(t.Context(), request)
	if err != nil || string(result.Output) != want || result.State != state || result.Revision != before.Revision || result.LastSequence != before.LastSequence {
		t.Fatalf("query: %+v %v", result, err)
	}
	after, err := s.GetExecution(t.Context(), request.Key)
	if err != nil {
		t.Fatal(err)
	}
	eventsAfter, err := s.ReadHistory(t.Context(), request.Key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(eventsBefore, eventsAfter) || tasksBefore != queryTaskDigest(t, s, request.Key) {
		t.Fatal("query changed PostgreSQL execution, history or tasks")
	}
}

func queryTaskDigest(t *testing.T, s *postgres.Store, key durable.Key) string {
	t.Helper()
	var digest string
	err := pgdriver.Unwrap(s.DB()).QueryRow(t.Context(), `SELECT md5(COALESCE(jsonb_agg(to_jsonb(t) ORDER BY task_id)::text,'[]')) FROM dispatch_execution_tasks t WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, key.Namespace, key.WorkflowID, key.RunID).Scan(&digest)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
