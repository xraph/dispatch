package worker_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/worker"

	log "github.com/xraph/go-utils/log"
)

// newFailingExecutor builds an executor whose only registered job fails with
// handlerErr, and returns it with the store so the test can read back the
// job's state and the DLQ.
func newFailingExecutor(t *testing.T, handlerErr error) (*worker.Executor, *memory.Store) {
	t.Helper()

	logger := log.NewNoopLogger()
	s := memory.New()
	reg := job.NewRegistry()
	extensions := ext.NewRegistry(logger)

	job.RegisterDefinition(reg, job.NewDefinition("failing",
		func(context.Context, struct{}) error { return handlerErr }))

	executor := worker.NewExecutor(
		reg, extensions, s, dlq.NewService(s, s),
		backoff.NewConstant(time.Hour), logger,
		middleware.Recover(logger),
	)

	return executor, s
}

// enqueueFailing stores a job with retries remaining, so a retry is what
// would happen if nothing intervened.
func enqueueFailing(t *testing.T, s *memory.Store) *job.Job {
	t.Helper()

	now := time.Now().UTC()

	j := &job.Job{
		Entity:     dispatch.Entity{CreatedAt: now, UpdatedAt: now},
		ID:         id.NewJobID(),
		Name:       "failing",
		Queue:      "default",
		State:      job.StateRunning,
		MaxRetries: 5,
		RunAt:      now,
	}

	if err := s.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}

	return j
}

// TestExecutePermanentFailureSkipsRetries is the behaviour the whole
// classification chain exists for. With five retries left, a permanent
// failure must still land in the DLQ on the first attempt: the retry
// schedule is there to outlast a transient fault, and a condition that
// cannot change is not one.
func TestExecutePermanentFailureSkipsRetries(t *testing.T) {
	ctx := context.Background()

	handlerErr := fmt.Errorf("stage input %q: %w", "model", dispatch.ErrPermanent)
	executor, s := newFailingExecutor(t, handlerErr)
	j := enqueueFailing(t, s)

	if err := executor.Execute(ctx, j); !errors.Is(err, dispatch.ErrPermanent) {
		t.Fatalf("Execute = %v, want the permanent error back", err)
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if got.State != job.StateFailed {
		t.Fatalf("state = %v, want %v (permanent failures must not be scheduled for retry)",
			got.State, job.StateFailed)
	}

	if got.RetryCount != 1 {
		t.Fatalf("RetryCount = %d, want 1 — the remaining %d attempts must be skipped",
			got.RetryCount, j.MaxRetries-1)
	}

	entries, err := s.ListDLQ(ctx, dlq.ListOpts{Limit: 10})
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("DLQ has %d entries, want 1 — a permanent failure goes straight there", len(entries))
	}
}

// TestExecuteTransientFailureStillRetries is the other half of the contract.
// An unclassified error keeps its retries, because dead-lettering something
// that would have succeeded costs more than retrying something that will not.
func TestExecuteTransientFailureStillRetries(t *testing.T) {
	ctx := context.Background()

	executor, s := newFailingExecutor(t, errors.New("dial tcp: connection refused"))
	j := enqueueFailing(t, s)

	if err := executor.Execute(ctx, j); err == nil {
		t.Fatal("Execute returned nil for a failing handler")
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if got.State != job.StateRetrying {
		t.Fatalf("state = %v, want %v — an unclassified failure must keep its retries",
			got.State, job.StateRetrying)
	}

	entries, err := s.ListDLQ(ctx, dlq.ListOpts{Limit: 10})
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}

	if len(entries) != 0 {
		t.Fatalf("DLQ has %d entries, want 0 — retries were still available", len(entries))
	}
}

// TestExecutePermanentFailureUnwrapsThroughLayers covers the shape the error
// actually has in production: the sentinel sits under several layers of
// context added between the storage backend and the executor.
func TestExecutePermanentFailureUnwrapsThroughLayers(t *testing.T) {
	ctx := context.Background()

	deep := fmt.Errorf("stage input %q: %w", "model",
		fmt.Errorf("stage art/gone.bin: %w",
			fmt.Errorf("dispatch/artifact: not found: %w", dispatch.ErrPermanent)))

	executor, s := newFailingExecutor(t, deep)
	j := enqueueFailing(t, s)

	if err := executor.Execute(ctx, j); err == nil {
		t.Fatal("Execute returned nil for a failing handler")
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if got.State != job.StateFailed {
		t.Fatalf("state = %v, want %v — the sentinel must be found through the wrapping",
			got.State, job.StateFailed)
	}
}
