package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func TestWorkflowDeferralExpiryAfterCoordinationWait(t *testing.T) {
	for _, kind := range []string{"lease", "execution"} {
		t.Run(kind, func(t *testing.T) {
			s, dsn, enroll, start := retirementFixture(t)
			if _, err := s.EnrollRetirement(t.Context(), enroll); err != nil {
				t.Fatal(err)
			}
			start.WorkflowID = "deadline"
			start.RequestID = "deadline-start"
			start.Queue = "deadline"
			lease := time.Second
			want := durable.ErrLeaseLost
			if kind == "execution" {
				start.ExecutionTimeout = time.Second
				lease = time.Minute
				want = durable.ErrExecutionDeadline
			}
			if _, err := s.StartExecution(t.Context(), start); err != nil {
				t.Fatal(err)
			}
			task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: start.Namespace, Queue: start.Queue, BuildID: start.BuildID, Kind: durable.TaskWorkflow, Owner: "expiry", LeaseDuration: lease})
			if err != nil || task == nil {
				t.Fatalf("claim: %+v %v", task, err)
			}
			request, err := durable.NewWorkflowTaskDeferralRequest(*task, 1, &durable.BuildAdmissionError{BuildID: "missing", State: "unregistered", ReferenceKind: "child", CommandID: "child"})
			if err != nil {
				t.Fatal(err)
			}
			before, err := s.GetTask(t.Context(), start.Key, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			until := task.LeaseUntil
			if kind == "execution" {
				execution, readErr := s.GetExecution(t.Context(), start.Key)
				if readErr != nil {
					t.Fatal(readErr)
				}
				until = execution.ExecutionDeadlineAt
			}
			gate, observe := retirementConn(t, dsn), retirementConn(t, dsn)
			tx, err := gate.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			if _, err = tx.Exec(t.Context(), `SELECT dispatch_retirement_coordinator_lock($1,1)`, enroll.Namespace); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, deferErr := s.DeferWorkflowTask(t.Context(), request); done <- deferErr }()
			queryWaitNamespace(t, observe, enroll.Namespace)
			if !until.After(time.Now()) {
				t.Fatal("fixture expired before confirmed coordination wait")
			}
			waitProtocolProof(t, until)
			if err = tx.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err = <-done; !errors.Is(err, want) {
				t.Fatalf("%s expiry: %v", kind, err)
			}
			current, err := s.GetTask(t.Context(), start.Key, task.ID)
			if err != nil || !reflect.DeepEqual(current, before) {
				t.Fatalf("refused deferral mutated task: %+v %v", current, err)
			}
			if _, err = s.GetWorkflowTaskDeferral(t.Context(), start.Key, task.ID); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("refused deferral persisted observation: %v", err)
			}
		})
	}
}
