package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// These tests drive jobs through the pool and runner engine.Build actually
// assembles, rather than through a hand-built Runner. That seam is where
// ErrPermanent quietly stopped working: the Runner-level tests covered the
// legacy nil-registry path, which is the one path engine users never take.

// countingExecutor is a stand-in for a stronger rung: it satisfies
// LevelProcess without providing any real isolation, so a test can prove a
// job was routed to it.
type countingExecutor struct {
	mu    sync.Mutex
	names []string

	reclaimed int
	closed    int
}

func (e *countingExecutor) Name() string      { return "counting" }
func (e *countingExecutor) Level() exec.Level { return exec.LevelProcess }

func (e *countingExecutor) Run(_ context.Context, req *exec.Request) (*exec.Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.names = append(e.names, req.Name)

	return &exec.Result{Status: exec.StatusOK}, nil
}

func (e *countingExecutor) Reclaim(context.Context, id.WorkerID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reclaimed++

	return nil
}

func (e *countingExecutor) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed++

	return nil
}

func (e *countingExecutor) ran(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, n := range e.names {
		if n == name {
			return true
		}
	}

	return false
}

func (e *countingExecutor) counts() (reclaimed, closed int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.reclaimed, e.closed
}

// startEngine builds an engine over a fresh in-memory store, starts it, and
// stops it when the test ends.
func startEngine(t *testing.T, opts ...engine.Option) (*engine.Engine, *memory.Store) {
	t.Helper()

	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s), dispatch.WithConcurrency(2))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}
	eng, err := engine.Build(d, opts...)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	return eng, s
}

// waitForJob polls the store until the job satisfies cond, or fails the test.
func waitForJob(t *testing.T, s *memory.Store, jobID id.JobID, what string, cond func(*job.Job) bool) *job.Job {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := s.GetJob(context.Background(), jobID)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if cond(got) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}

	got, err := s.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	t.Fatalf("timed out waiting for %s; job state = %q, retry_count = %d, last_error = %q",
		what, got.State, got.RetryCount, got.LastError)

	return nil
}

func TestEngine_PermanentFailureReachesDLQOnTheFirstAttempt(t *testing.T) {
	// engine.Build always wires a non-nil executor registry, so this is the
	// path every engine user takes. A handler declining a retry must still
	// be able to.
	eng, s := startEngine(t)

	var attempts int
	var mu sync.Mutex
	engine.Register(eng, job.NewDefinition("permanent.job",
		func(context.Context, execPayload) error {
			mu.Lock()
			attempts++
			mu.Unlock()

			return fmt.Errorf("malformed payload: %w", dispatch.ErrPermanent)
		}))

	j, err := engine.Enqueue(context.Background(), eng, "permanent.job", execPayload{Value: 1})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if startErr := eng.Start(context.Background()); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })

	got := waitForJob(t, s, j.ID, "the job to fail", func(g *job.Job) bool {
		return g.State == job.StateFailed
	})

	if got.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1 — the backoff schedule must not be spent", got.RetryCount)
	}
	if want := "malformed payload: dispatch: permanent failure"; got.LastError != want {
		t.Errorf("LastError = %q, want %q", got.LastError, want)
	}

	mu.Lock()
	ran := attempts
	mu.Unlock()
	if ran != 1 {
		t.Errorf("handler ran %d times, want 1", ran)
	}

	// Give the pool a moment to prove it is not still retrying behind us.
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	ran = attempts
	mu.Unlock()
	if ran != 1 {
		t.Errorf("handler ran %d times after the job failed, want 1", ran)
	}
}

func TestEngine_OrdinaryFailureStillRetries(t *testing.T) {
	// The other half of the same claim: an error that is not permanent must
	// keep its retry schedule, so the fix above did not turn every failure
	// into a dead letter.
	eng, s := startEngine(t)

	engine.Register(eng, job.NewDefinition("retrying.job",
		func(context.Context, execPayload) error { return errors.New("transient") }))

	j, err := engine.Enqueue(context.Background(), eng, "retrying.job", execPayload{Value: 1})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if startErr := eng.Start(context.Background()); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })

	got := waitForJob(t, s, j.ID, "the job to be scheduled for retry", func(g *job.Job) bool {
		return g.State == job.StateRetrying
	})
	if got.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", got.RetryCount)
	}
	if got.LastError != "transient" {
		t.Errorf("LastError = %q, want %q", got.LastError, "transient")
	}
}

func TestEngine_JobIsDispatchedToTheAddedExecutor(t *testing.T) {
	// Build + WithExecutor + a policy the added rung satisfies, end to end:
	// the job must actually run there, not merely be registrable.
	rung := &countingExecutor{}
	eng, s := startEngine(t, engine.WithExecutor(rung))

	handlerRan := make(chan struct{}, 1)
	if regErr := engine.RegisterChecked(eng, job.NewDefinition("isolated.job",
		func(context.Context, execPayload) error {
			handlerRan <- struct{}{}

			return nil
		},
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	)); regErr != nil {
		t.Fatalf("RegisterChecked: %v", regErr)
	}

	j, err := engine.Enqueue(context.Background(), eng, "isolated.job", execPayload{Value: 7})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if startErr := eng.Start(context.Background()); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	waitForJob(t, s, j.ID, "the job to complete", func(g *job.Job) bool {
		return g.State == job.StateCompleted
	})

	if !rung.ran("isolated.job") {
		t.Error("the job was not dispatched to the executor its policy required")
	}
	// This rung never calls the handler, so a job that reached it cannot
	// also have run in process.
	select {
	case <-handlerRan:
		t.Error("the handler ran in process even though the job was routed to another executor")
	default:
	}

	// The pool sweeps for leaked sandboxes at startup and the engine closes
	// every rung when it stops. Both had no caller before.
	if reclaimed, _ := rung.counts(); reclaimed != 1 {
		t.Errorf("Reclaim called %d times at pool start, want 1", reclaimed)
	}
	if stopErr := eng.Stop(context.Background()); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}
	if _, closed := rung.counts(); closed != 1 {
		t.Errorf("Close called %d times at engine stop, want 1", closed)
	}
}
