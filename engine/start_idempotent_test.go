package engine_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

// startCountingStore counts the store calls only Start makes: the resume
// scan for runs left "running" and the wake listener subscription.
type startCountingStore struct {
	*memory.Store
	resumeScans atomic.Int32
	listeners   atomic.Int32
	stops       atomic.Int32
}

func (s *startCountingStore) ListRuns(ctx context.Context, opts workflow.ListOpts) ([]*workflow.Run, error) {
	if opts.State == workflow.RunStateRunning {
		s.resumeScans.Add(1)
	}
	return s.Store.ListRuns(ctx, opts)
}

func (s *startCountingStore) StartWakeListener(context.Context, func()) (func(), error) {
	s.listeners.Add(1)
	return func() { s.stops.Add(1) }, nil
}

// A second Start on a running engine must not resume the runs it is already
// executing, nor subscribe a second wake listener whose stop func is lost.
func TestStartTwiceIsNoOp(t *testing.T) {
	s := &startCountingStore{Store: memory.New()}
	d, err := dispatch.New(dispatch.WithStore(s), dispatch.WithConcurrency(1), dispatch.WithPollInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = eng.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if got := s.resumeScans.Load(); got != 1 {
		t.Fatalf("resume scans after first Start = %d, want 1", got)
	}
	if got := s.listeners.Load(); got != 1 {
		t.Fatalf("wake listeners after first Start = %d, want 1", got)
	}

	if err = eng.Start(ctx); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if got := s.resumeScans.Load(); got != 1 {
		t.Errorf("second Start resumed running workflow runs again: scans = %d, want 1", got)
	}
	if got := s.listeners.Load(); got != 1 {
		t.Errorf("second Start subscribed another wake listener: listeners = %d, want 1", got)
	}

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = eng.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := s.stops.Load(); got != 1 {
		t.Fatalf("wake listeners stopped = %d, want 1", got)
	}
	if err = eng.Start(ctx); !errors.Is(err, durable.ErrClosed) {
		t.Fatalf("Start after Stop = %v, want ErrClosed", err)
	}
}

// failOncePool fails its first Start, as a pool that could not come up would,
// and starts cleanly on the next attempt.
type failOncePool struct{ starts atomic.Int32 }

func (p *failOncePool) Start(context.Context) error {
	if p.starts.Add(1) == 1 {
		return errors.New("pool start failed")
	}
	return nil
}

func (p *failOncePool) Stop(context.Context) error { return nil }

// A Start that fails partway must leave durable workflows restartable, so the
// next Start brings the durable worker up instead of returning ErrClosed.
func TestFailedStartCanBeRetriedWithDurableWorkflows(t *testing.T) {
	d, err := dispatch.New(dispatch.WithStore(memory.New()), dispatch.WithConcurrency(1), dispatch.WithPollInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	o := drt.Options{Namespace: "retry", Queue: "work", BuildID: "v1", Owner: "worker", PollInterval: time.Millisecond,
		Workflows: map[string]drt.WorkflowFunc{"noop": func(*drt.Workflow, []byte) ([]byte, error) { return nil, nil }}}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(o))
	if err != nil {
		t.Fatal(err)
	}
	pool := &failOncePool{}
	d.SetPool(pool)

	ctx := context.Background()
	if err = eng.Start(ctx); err == nil {
		t.Fatal("first Start succeeded, want the injected pool failure")
	}
	if status, e := eng.DurableStatus(); e != nil || status.State != drt.WorkerNotStarted {
		t.Errorf("durable worker after failed Start: state %s %v, want not started", status.State, e)
	}

	if err = eng.Start(ctx); err != nil {
		t.Fatalf("retried Start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Stop(stopCtx)
	})
	if got := pool.starts.Load(); got != 2 {
		t.Fatalf("pool starts = %d, want 2", got)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		status, e := eng.DurableStatus()
		if e != nil {
			t.Fatal(e)
		}
		if status.State == drt.WorkerRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("durable worker after retried Start: %+v, want running", status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err = eng.Health(ctx); err != nil {
		t.Fatalf("Health after retried Start: %v", err)
	}
}
