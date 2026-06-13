package worker_test

import (
	"context"
	"encoding/json"
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

	log "github.com/xraph/go-utils/log"
)

func setupTestPool(t *testing.T, concurrency int, pollInterval time.Duration) (
	*worker.Pool, *memory.Store, *job.Registry,
) {
	t.Helper()
	logger := log.NewNoopLogger()
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
	logger := log.NewNoopLogger()
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

// recordingStore wraps the memory store and records DequeueJobs call
// patterns: total calls, the limit passed, and how many calls were in
// flight concurrently.
type recordingStore struct {
	*memory.Store
	dequeueCalls atomic.Int32
	inFlight     atomic.Int32
	maxInFlight  atomic.Int32
	minLimit     atomic.Int32
	maxLimit     atomic.Int32
}

func newRecordingStore() *recordingStore {
	rs := &recordingStore{Store: memory.New()}
	rs.minLimit.Store(1 << 30)
	return rs
}

func (r *recordingStore) DequeueJobs(ctx context.Context, queues []string, limit int) ([]*job.Job, error) {
	r.dequeueCalls.Add(1)
	cur := r.inFlight.Add(1)
	defer r.inFlight.Add(-1)
	for {
		maxSeen := r.maxInFlight.Load()
		if cur <= maxSeen || r.maxInFlight.CompareAndSwap(maxSeen, cur) {
			break
		}
	}
	for {
		minSeen := r.minLimit.Load()
		if int32(limit) >= minSeen || r.minLimit.CompareAndSwap(minSeen, int32(limit)) {
			break
		}
	}
	for {
		maxSeen := r.maxLimit.Load()
		if int32(limit) <= maxSeen || r.maxLimit.CompareAndSwap(maxSeen, int32(limit)) {
			break
		}
	}
	return r.Store.DequeueJobs(ctx, queues, limit)
}

func setupRecordingPool(t *testing.T, rs *recordingStore, opts ...worker.PoolOption) (*worker.Pool, *job.Registry) {
	t.Helper()
	logger := log.NewNoopLogger()
	reg := job.NewRegistry()
	extensions := ext.NewRegistry(logger)
	dlqSvc := dlq.NewService(rs, rs)
	bo := backoff.NewConstant(10 * time.Millisecond)
	executor := worker.NewExecutor(
		reg, extensions, rs, dlqSvc, bo, logger,
		middleware.Recover(logger),
	)
	pool := worker.NewPool(rs, executor, extensions, logger, opts...)
	return pool, reg
}

// TestPool_SingleBatchFetcher locks in the single-fetcher contract: an idle
// pool with concurrency N issues ONE DequeueJobs call at a time asking for
// all N free slots, instead of N concurrent goroutines each asking for 1.
func TestPool_SingleBatchFetcher(t *testing.T) {
	rs := newRecordingStore()
	pool, _ := setupRecordingPool(t, rs,
		worker.WithPoolConcurrency(4),
		worker.WithPollInterval(10*time.Millisecond),
		worker.WithMaxPollInterval(10*time.Millisecond), // fixed cadence for this test
		worker.WithPoolQueues([]string{"default"}),
	)

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("start error: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Stop(ctx); err != nil {
		t.Fatalf("stop error: %v", err)
	}

	if got := rs.maxInFlight.Load(); got != 1 {
		t.Errorf("max concurrent DequeueJobs calls = %d, want 1 (single fetcher)", got)
	}
	if got := rs.minLimit.Load(); got != 4 {
		t.Errorf("DequeueJobs limit = %d, want 4 (batch for all free slots)", got)
	}
}

// TestPool_IdleBackoff verifies empty polls back the fetch cadence off up to
// MaxPollInterval instead of hammering the store every PollInterval.
func TestPool_IdleBackoff(t *testing.T) {
	rs := newRecordingStore()
	pool, _ := setupRecordingPool(t, rs,
		worker.WithPoolConcurrency(2),
		worker.WithPollInterval(10*time.Millisecond),
		worker.WithMaxPollInterval(160*time.Millisecond),
		worker.WithPoolQueues([]string{"default"}),
	)

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("start error: %v", err)
	}
	time.Sleep(1 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Stop(ctx); err != nil {
		t.Fatalf("stop error: %v", err)
	}

	// Fixed-rate polling would be ~100 calls/s/goroutine. With doubling
	// backoff capped at 160ms the single fetcher polls ~10 times in 1s.
	if got := rs.dequeueCalls.Load(); got > 20 {
		t.Errorf("idle DequeueJobs calls in 1s = %d, want <= 20 (backoff)", got)
	}
}

// TestPool_WakeResetsBackoff verifies Wake interrupts an inflated idle
// interval so freshly enqueued work is picked up promptly.
func TestPool_WakeResetsBackoff(t *testing.T) {
	rs := newRecordingStore()
	pool, reg := setupRecordingPool(t, rs,
		worker.WithPoolConcurrency(2),
		worker.WithPollInterval(10*time.Millisecond),
		worker.WithMaxPollInterval(5*time.Second),
		worker.WithPoolQueues([]string{"default"}),
	)

	var processed atomic.Bool
	job.RegisterDefinition(reg, job.NewDefinition("wake-me", func(_ context.Context, _ struct{}) error {
		processed.Store(true)
		return nil
	}))

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("start error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = pool.Stop(ctx)
	}()

	// Let the idle fetcher back off to a multi-second interval.
	time.Sleep(1200 * time.Millisecond)

	j := &job.Job{
		ID:         newTestJobID(),
		Name:       "wake-me",
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 1,
		RunAt:      time.Now().UTC(),
	}
	j.CreatedAt = time.Now().UTC()
	j.UpdatedAt = j.CreatedAt
	if err := rs.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}
	pool.Wake()

	deadline := time.After(700 * time.Millisecond)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("job not processed promptly after Wake; idle backoff not reset")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestPool_ReaperWakesFetcher verifies that when the reaper resets a stale
// job to pending, it wakes the fetcher so the job is retried immediately
// instead of waiting out the inflated idle poll interval.
func TestPool_ReaperWakesFetcher(t *testing.T) {
	rs := newRecordingStore()
	pool, reg := setupRecordingPool(t, rs,
		worker.WithPoolConcurrency(2),
		worker.WithPollInterval(10*time.Millisecond),
		worker.WithMaxPollInterval(5*time.Second),
		worker.WithStaleJobThreshold(100*time.Millisecond),
		worker.WithPoolQueues([]string{"default"}),
	)

	var processed atomic.Bool
	job.RegisterDefinition(reg, job.NewDefinition("reaped", func(_ context.Context, _ struct{}) error {
		processed.Store(true)
		return nil
	}))

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("start error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = pool.Stop(ctx)
	}()

	// Let the idle fetcher back off to a multi-second interval.
	time.Sleep(1200 * time.Millisecond)

	// Inject a stale running job — a worker elsewhere died mid-execution.
	now := time.Now().UTC()
	old := now.Add(-time.Hour)
	j := &job.Job{
		ID:          newTestJobID(),
		Name:        "reaped",
		Queue:       "default",
		Payload:     []byte(`{}`),
		State:       job.StateRunning,
		MaxRetries:  3,
		RunAt:       old,
		StartedAt:   &old,
		HeartbeatAt: &old,
	}
	j.CreatedAt = old
	j.UpdatedAt = old
	if err := rs.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// The reaper (100ms cadence) resets it to pending and must wake the
	// fetcher; without the wake the next poll is seconds away.
	deadline := time.After(700 * time.Millisecond)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("reaped job not processed promptly; reaper did not wake the fetcher")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
