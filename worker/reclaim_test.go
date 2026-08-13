package worker_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/worker"

	log "github.com/xraph/go-utils/log"
)

// blockingReclaimExecutor is an exec.Executor whose Reclaim blocks until
// either the test releases it or its context is cancelled. It stands in for
// an out-of-process rung doing real (and potentially slow) process or
// filesystem I/O during the pool's startup sweep.
type blockingReclaimExecutor struct {
	release  chan struct{}
	calledCh chan struct{}

	mu          sync.Mutex
	unblockedBy string // "release" or "ctx"
}

func newBlockingReclaimExecutor() *blockingReclaimExecutor {
	return &blockingReclaimExecutor{
		release:  make(chan struct{}),
		calledCh: make(chan struct{}),
	}
}

func (e *blockingReclaimExecutor) Name() string      { return "blocking" }
func (e *blockingReclaimExecutor) Level() exec.Level { return exec.LevelProcess }

func (e *blockingReclaimExecutor) Run(_ context.Context, _ *exec.Request) (*exec.Result, error) {
	return &exec.Result{Status: exec.StatusOK}, nil
}

// Reclaim blocks until release is closed or ctx is done, recording which
// one unblocked it so tests can tell a leaked block from an interrupted one.
func (e *blockingReclaimExecutor) Reclaim(ctx context.Context, _ id.WorkerID) error {
	close(e.calledCh)

	select {
	case <-e.release:
		e.mu.Lock()
		e.unblockedBy = "release"
		e.mu.Unlock()
	case <-ctx.Done():
		e.mu.Lock()
		e.unblockedBy = "ctx"
		e.mu.Unlock()
	}

	return nil
}

func (e *blockingReclaimExecutor) Close() error { return nil }

func (e *blockingReclaimExecutor) waitCalled(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-e.calledCh:
	case <-time.After(d):
		t.Fatal("Reclaim was never called")
	}
}

func (e *blockingReclaimExecutor) getUnblockedBy() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.unblockedBy
}

// erroringReclaimExecutor is an exec.Executor whose Reclaim always fails
// immediately, without blocking.
type erroringReclaimExecutor struct{}

func (e *erroringReclaimExecutor) Name() string      { return "erroring" }
func (e *erroringReclaimExecutor) Level() exec.Level { return exec.LevelProcess }

func (e *erroringReclaimExecutor) Run(_ context.Context, _ *exec.Request) (*exec.Result, error) {
	return &exec.Result{Status: exec.StatusOK}, nil
}

func (e *erroringReclaimExecutor) Reclaim(context.Context, id.WorkerID) error {
	return errors.New("sandbox cleanup failed")
}

func (e *erroringReclaimExecutor) Close() error { return nil }

// setupPoolWithExecutor builds a pool whose Runner is wired to an
// exec.Registry containing the in-process default plus extra, so a job
// with no declared isolation still runs (through in-process) while the
// pool's Reclaim sweep also visits extra.
func setupPoolWithExecutor(t *testing.T, extra exec.Executor) (
	*worker.Pool, *memory.Store, *job.Registry,
) {
	t.Helper()
	logger := log.NewNoopLogger()
	s := memory.New()
	reg := job.NewRegistry()
	extensions := ext.NewRegistry(logger)

	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(extra)

	dlqSvc := dlq.NewService(s, s)
	bo := backoff.NewConstant(10 * time.Millisecond)
	runner := worker.NewRunner(reg, extensions, s, dlqSvc, bo, executors, logger)

	pool := worker.NewPool(s, runner, extensions, logger,
		worker.WithPoolConcurrency(1),
		worker.WithPollInterval(10*time.Millisecond),
		worker.WithPoolQueues([]string{"default"}),
	)

	return pool, s, reg
}

// TestPool_StartReturnsPromptlyWhenReclaimBlocks proves the defect: Start's
// doc comment says "it returns immediately", but before the fix the
// Reclaim sweep ran synchronously, inside p.mu, before Start returned. A
// rung whose Reclaim blocks (a subprocess rung doing real I/O, not the
// in-process no-op) would hang Start forever.
func TestPool_StartReturnsPromptlyWhenReclaimBlocks(t *testing.T) {
	fake := newBlockingReclaimExecutor()
	pool, _, _ := setupPoolWithExecutor(t, fake)

	done := make(chan error, 1)
	go func() {
		done <- pool.Start(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return within 2s while Reclaim was blocked")
	}

	// Let Stop interrupt the still-blocked sweep rather than leaking it
	// past the end of the test.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Stop(ctx); err != nil {
		t.Fatalf("Stop error: %v", err)
	}
}

// TestPool_StopInterruptsBlockedReclaim verifies Stop can always interrupt
// a Reclaim sweep still in flight, and that it is released through context
// cancellation — not by the test's own release channel, which is never
// closed here — so the goroutine backing it does not leak past Stop.
func TestPool_StopInterruptsBlockedReclaim(t *testing.T) {
	fake := newBlockingReclaimExecutor()
	pool, _, _ := setupPoolWithExecutor(t, fake)

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	// Make sure the sweep has actually started blocking before we stop.
	fake.waitCalled(t, 2*time.Second)

	stopDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stopDone <- pool.Stop(ctx)
	}()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not complete while Reclaim was blocked")
	}

	if got := fake.getUnblockedBy(); got != "ctx" {
		t.Errorf("Reclaim unblocked by %q, want %q (context cancellation, not the release channel)", got, "ctx")
	}
}

// TestPool_ReclaimErrorDoesNotBlockProcessing verifies reclamation is
// best-effort: a Reclaim that returns an error must be logged and not stop
// the pool from starting and running jobs.
func TestPool_ReclaimErrorDoesNotBlockProcessing(t *testing.T) {
	pool, s, reg := setupPoolWithExecutor(t, &erroringReclaimExecutor{})

	var processed atomic.Bool
	job.RegisterDefinition(reg, job.NewDefinition("reclaim-error-job", func(_ context.Context, _ struct{}) error {
		processed.Store(true)
		return nil
	}))

	j := &job.Job{
		ID:         id.NewJobID(),
		Name:       "reclaim-error-job",
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      time.Now().UTC(),
	}
	j.CreatedAt = time.Now().UTC()
	j.UpdatedAt = j.CreatedAt
	if err := s.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job to be processed despite the reclaim error")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Stop(ctx); err != nil {
		t.Fatalf("Stop error: %v", err)
	}
}
