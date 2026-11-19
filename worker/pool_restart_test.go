package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/worker"

	log "github.com/xraph/go-utils/log"
)

// setupRestartPool builds a pool with every background loop enabled
// (fetcher, workers, heartbeat, reaper, reclaim sweep) so a restart has to
// give each of them fresh per-run state. Jobs named "count" bump the
// returned counter.
func setupRestartPool(t *testing.T) (*worker.Pool, *memory.Store, *atomic.Int64) {
	t.Helper()
	logger := log.NewNoopLogger()
	s := memory.New()
	reg := job.NewRegistry()
	extensions := ext.NewRegistry(logger)

	var ran atomic.Int64
	job.RegisterDefinition(reg, job.NewDefinition("count", func(context.Context, struct{}) error {
		ran.Add(1)
		return nil
	}))

	executor := worker.NewExecutor(
		reg, extensions, s, dlq.NewService(s, s), backoff.NewConstant(10*time.Millisecond), logger,
		middleware.Recover(logger),
	)

	pool := worker.NewPool(s, executor, extensions, logger,
		worker.WithPoolConcurrency(2),
		worker.WithPollInterval(10*time.Millisecond),
		worker.WithMaxPollInterval(20*time.Millisecond),
		worker.WithHeartbeatInterval(10*time.Millisecond),
		worker.WithStaleJobThreshold(time.Minute),
		worker.WithReapInterval(10*time.Millisecond),
		worker.WithPoolQueues([]string{"default"}),
	)

	return pool, s, &ran
}

func enqueueCountJob(t *testing.T, s *memory.Store) *job.Job {
	t.Helper()
	payload, _ := json.Marshal(struct{}{})
	now := time.Now().UTC()
	j := &job.Job{
		ID:         newTestJobID(),
		Name:       "count",
		Queue:      "default",
		Payload:    payload,
		State:      job.StatePending,
		MaxRetries: 1,
		RunAt:      now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := s.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return j
}

func waitForCount(t *testing.T, ran *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for ran.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d jobs to run, %d ran", want, ran.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// safeCall runs fn and turns a panic into an error, so a lifecycle bug
// fails the test instead of crashing the test binary.
func safeCall(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn()
}

func stopPool(t *testing.T, p *worker.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := safeCall(func() error { return p.Stop(ctx) }); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// A pool that is stopped and started again must actually run again: the
// second run's loops must not see the first run's closed stop channel or
// cancelled context and exit at once.
func TestPool_RestartProcessesNewJob(t *testing.T) {
	pool, s, ran := setupRestartPool(t)

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	enqueueCountJob(t, s)
	pool.Wake()
	waitForCount(t, ran, 1)
	stopPool(t, pool)

	if err := pool.Ready(); err == nil {
		t.Fatal("Ready() = nil after Stop, want an error")
	}

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("restart: %v", err)
	}
	t.Cleanup(func() { stopPool(t, pool) })

	j := enqueueCountJob(t, s)
	pool.Wake()
	waitForCount(t, ran, 2)

	got, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for got.State != job.StateCompleted && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		if got, err = s.GetJob(context.Background(), j.ID); err != nil {
			t.Fatalf("get job: %v", err)
		}
	}
	if got.State != job.StateCompleted {
		t.Fatalf("job state after restart = %q, want %q", got.State, job.StateCompleted)
	}

	if err := pool.Ready(); err != nil {
		t.Fatalf("Ready() after restart = %v, want nil", err)
	}
}

// Stop is called more than once on shutdown (Engine and Dispatcher both
// stop the pool). That has to stay a no-op after a restart too, rather
// than closing the run's stop channel a second time.
func TestPool_StopTwiceAfterRestart(t *testing.T) {
	pool, _, _ := setupRestartPool(t)
	ctx := context.Background()

	for run := range 3 {
		if err := safeCall(func() error { return pool.Start(ctx) }); err != nil {
			t.Fatalf("start (run %d): %v", run, err)
		}
		stopPool(t, pool)
		stopPool(t, pool)
	}
}

// Start and Stop may race from different goroutines. Every call must be
// safe, and the pool must still work afterwards.
func TestPool_ConcurrentStartStop(t *testing.T) {
	pool, s, ran := setupRestartPool(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 20 {
				var err error
				if (g+i)%2 == 0 {
					err = safeCall(func() error { return pool.Start(ctx) })
				} else {
					stopCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
					err = safeCall(func() error { return pool.Stop(stopCtx) })
					cancel()
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent lifecycle call: %v", err)
	}
	if t.Failed() {
		return
	}

	stopPool(t, pool)
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("start after concurrent calls: %v", err)
	}
	t.Cleanup(func() { stopPool(t, pool) })

	enqueueCountJob(t, s)
	pool.Wake()
	waitForCount(t, ran, 1)
}
