package worker_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/worker"
)

func setupTestPool(t *testing.T, concurrency int, pollInterval time.Duration) (
	*worker.Pool, *memory.Store, *job.Registry,
) {
	t.Helper()
	logger := slog.Default()
	s := memory.New()
	reg := job.NewRegistry()
	extensions := ext.NewRegistry(logger)

	dlqSvc := dlq.NewService(s, s)
	bo := backoff.NewConstant(10 * time.Millisecond)

	executor := worker.NewExecutor(
		reg, extensions, s, dlqSvc, bo, logger,
		middleware.Recover(logger),
	)

	pool := worker.NewPool(s, executor, extensions, logger,
		worker.WithPoolConcurrency(concurrency),
		worker.WithPollInterval(pollInterval),
		worker.WithPoolQueues([]string{"default"}),
	)

	return pool, s, reg
}

func TestPool_StartStop(t *testing.T) {
	pool, _, _ := setupTestPool(t, 2, 50*time.Millisecond)

	err := pool.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected start error: %v", err)
	}

	// Double start should be no-op.
	err = pool.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected double-start error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = pool.Stop(ctx)
	if err != nil {
		t.Fatalf("unexpected stop error: %v", err)
	}

	// Double stop should be no-op.
	err = pool.Stop(ctx)
	if err != nil {
		t.Fatalf("unexpected double-stop error: %v", err)
	}
}

func TestPool_ProcessesJob(t *testing.T) {
	pool, s, reg := setupTestPool(t, 1, 10*time.Millisecond)

	var processed atomic.Bool
	job.RegisterDefinition(reg, job.NewDefinition("greet", func(_ context.Context, p struct{ Name string }) error {
		if p.Name != "Alice" {
			t.Errorf("payload.Name = %q, want %q", p.Name, "Alice")
		}
		processed.Store(true)
		return nil
	}))

	// Enqueue a job.
	payload, _ := json.Marshal(struct{ Name string }{Name: "Alice"})
	j := &job.Job{
		ID:         newTestJobID(),
		Name:       "greet",
		Queue:      "default",
		Payload:    payload,
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      time.Now().UTC(),
	}
	j.CreatedAt = time.Now().UTC()
	j.UpdatedAt = j.CreatedAt

	if err := s.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Start pool and wait for processing.
	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("start error: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job to be processed")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Stop(ctx); err != nil {
		t.Fatalf("stop error: %v", err)
	}

	// Verify job state.
	got, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("get job error: %v", err)
	}
	if got.State != job.StateCompleted {
		t.Errorf("job state = %q, want %q", got.State, job.StateCompleted)
	}
	if got.CompletedAt == nil {
		t.Error("expected CompletedAt to be set")
	}
}

func TestPool_FailedJob(t *testing.T) {
	pool, s, reg := setupTestPool(t, 1, 10*time.Millisecond)

	var processed atomic.Bool
	job.RegisterDefinition(reg, job.NewDefinition("fail-job", func(_ context.Context, _ struct{}) error {
		processed.Store(true)
		return context.DeadlineExceeded
	}))

	j := &job.Job{
		ID:         newTestJobID(),
		Name:       "fail-job",
		Queue:      "default",
		State:      job.StatePending,
		MaxRetries: 0,
		RunAt:      time.Now().UTC(),
	}
	j.CreatedAt = time.Now().UTC()
	j.UpdatedAt = j.CreatedAt

	if err := s.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("start error: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job to be processed")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Stop(ctx); err != nil {
		t.Fatalf("stop error: %v", err)
	}

	got, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("get job error: %v", err)
	}
	if got.State != job.StateFailed {
		t.Errorf("job state = %q, want %q", got.State, job.StateFailed)
	}
	if got.LastError == "" {
		t.Error("expected LastError to be set")
	}
}

func TestPool_GracefulShutdown(t *testing.T) {
	pool, _, _ := setupTestPool(t, 4, 50*time.Millisecond)

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("start error: %v", err)
	}

	// Allow workers to start polling.
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := pool.Stop(ctx)
	if err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
}

func TestPool_ExtensionFires(t *testing.T) {
	logger := slog.Default()
	s := memory.New()
	reg := job.NewRegistry()
	extensions := ext.NewRegistry(logger)

	// Register a tracking extension.
	tracker := &trackingExt{}
	extensions.Register(tracker)

	dlqSvc := dlq.NewService(s, s)
	bo := backoff.NewConstant(10 * time.Millisecond)

	executor := worker.NewExecutor(reg, extensions, s, dlqSvc, bo, logger)
	pool := worker.NewPool(s, executor, extensions, logger,
		worker.WithPoolConcurrency(1),
		worker.WithPollInterval(10*time.Millisecond),
	)

	var processed atomic.Bool
	job.RegisterDefinition(reg, job.NewDefinition("tracked", func(_ context.Context, _ struct{}) error {
		processed.Store(true)
		return nil
	}))

	j := &job.Job{
		ID:    newTestJobID(),
		Name:  "tracked",
		Queue: "default",
		State: job.StatePending,
		RunAt: time.Now().UTC(),
	}
	j.CreatedAt = time.Now().UTC()
	j.UpdatedAt = j.CreatedAt

	if err := s.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("start error: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Stop(ctx); err != nil {
		t.Fatalf("stop error: %v", err)
	}

	if !tracker.started.Load() {
		t.Error("expected OnJobStarted to fire")
	}
	if !tracker.completed.Load() {
		t.Error("expected OnJobCompleted to fire")
	}
}

// ──────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────

func newTestJobID() id.JobID {
	return id.NewJobID()
}

// trackingExt records which hooks fired.
type trackingExt struct {
	started   atomic.Bool
	completed atomic.Bool
	failed    atomic.Bool
}

func (e *trackingExt) Name() string { return "tracker" }

func (e *trackingExt) OnJobStarted(_ context.Context, _ *job.Job) error {
	e.started.Store(true)
	return nil
}

func (e *trackingExt) OnJobCompleted(_ context.Context, _ *job.Job, _ time.Duration) error {
	e.completed.Store(true)
	return nil
}

func (e *trackingExt) OnJobFailed(_ context.Context, _ *job.Job, _ error) error {
	e.failed.Store(true)
	return nil
}
