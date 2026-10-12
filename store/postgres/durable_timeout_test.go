//go:build integration

package postgres_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

func TestDurableTimeoutGrantReopen(t *testing.T) {
	s, dsn := setupTestStoreConnection(t)
	key := durable.Key{Namespace: t.Name(), WorkflowID: "order", RunID: "run"}
	if _, err := s.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "order", BuildID: "v1", Queue: "orders"}); err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: key.Namespace, Queue: "orders", Kind: durable.TaskWorkflow, Owner: "workflow", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatalf("claim workflow: %+v %v", task, err)
	}
	_, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: key, RequestID: "schedule", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "activity.scheduled"}}, Tasks: []durable.TaskSpec{{ID: "effect", Kind: durable.TaskActivity, Queue: "offline", DeadlineAfter: 20 * time.Millisecond}}})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := s.GetTask(t.Context(), key, "effect")
	if err != nil {
		t.Fatal(err)
	}
	waitDurableStoreTime(t, s, queued.DeadlineAt)
	poll := durable.TimeoutClaimRequest{Namespace: key.Namespace, BuildID: "v1", Owner: "coordinator", LeaseDuration: time.Minute}
	grant, err := s.ClaimTimeoutTask(t.Context(), poll)
	if err != nil || grant == nil || grant.LeaseKind != durable.LeaseTimeout || grant.Attempt != 0 {
		t.Fatalf("claim timeout: %+v %v", grant, err)
	}
	if err = s.DB().Close(); err != nil {
		t.Fatal(err)
	}
	drv := pgdriver.New()
	if err = drv.Open(t.Context(), dsn); err != nil {
		t.Fatal(err)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	reopened := postgres.New(db)
	saved, err := reopened.GetTask(t.Context(), key, "effect")
	if err != nil || saved.Token() != grant.Token() || !saved.LeaseUntil.Equal(grant.LeaseUntil) || !saved.DeadlineAt.Equal(grant.DeadlineAt) {
		t.Fatalf("timeout grant changed after reopen: %+v %v", saved, err)
	}
	if second, claimErr := reopened.ClaimTimeoutTask(t.Context(), poll); claimErr != nil || second != nil {
		t.Fatalf("reopen duplicated timeout grant: %+v %v", second, claimErr)
	}
	forged := saved.Token()
	forged.LeaseKind = ""
	if _, err = reopened.RenewTask(t.Context(), key, forged, time.Minute); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("forged execution grant after reopen: %v", err)
	}
	if _, err = reopened.RenewTask(t.Context(), key, saved.Token(), time.Minute); err != nil {
		t.Fatal(err)
	}
	request := durable.CommitRequest{Key: key, RequestID: "timeout", ExpectedRevision: 2, Token: saved.Token(), Events: []durable.EventInput{{Type: "activity.timed_out"}}, State: durable.StateFailed}
	receipt, err := reopened.CommitTransition(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if again, retryErr := reopened.CommitTransition(t.Context(), request); retryErr != nil || again != receipt {
		t.Fatalf("timeout receipt: %+v %v", again, retryErr)
	}
}
