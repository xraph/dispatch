package engine_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
)

// heartbeatInterval is short so the tests see several beats quickly. Each
// test polls with a deadline rather than sleeping a fixed time.
const heartbeatInterval = 20 * time.Millisecond

// buildHeartbeatEngine builds an engine on a fresh memory store with a
// short heartbeat interval and stops it when the test ends. Build has
// already registered the worker row; nothing has started.
func buildHeartbeatEngine(t *testing.T) (*engine.Engine, *memory.Store) {
	t.Helper()

	s := memory.New()
	d, err := dispatch.New(
		dispatch.WithStore(s),
		dispatch.WithConcurrency(1),
		dispatch.WithQueues([]string{"default", "render"}),
		dispatch.WithHeartbeatInterval(heartbeatInterval),
	)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d, engine.WithWorkerCapacity(resource.Set{resource.Memory: 4 << 30}))
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })

	return eng, s
}

// waitForWorker polls the store until cond holds for the engine's row, or
// fails the test after five seconds. A missing row is passed to cond as
// nil.
func waitForWorker(t *testing.T, s *memory.Store, eng *engine.Engine, what string, cond func(*cluster.Worker) bool) *cluster.Worker {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		w, err := s.GetWorker(context.Background(), eng.WorkerID())
		if err != nil && !errors.Is(err, dispatch.ErrWorkerNotFound) {
			t.Fatalf("GetWorker: %v", err)
		}
		if cond(w) {
			return w
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)

	return nil
}

func TestEngine_WorkerHeartbeatAdvancesLastSeen(t *testing.T) {
	eng, s := buildHeartbeatEngine(t)
	ctx := context.Background()

	registered, err := s.GetWorker(ctx, eng.WorkerID())
	if err != nil {
		t.Fatalf("Build did not register the worker row: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Two advances, not one: Start beats once straight away, so a single
	// advance would pass even if the ticker never fired.
	first := waitForWorker(t, s, eng, "LastSeen to pass the registration time", func(w *cluster.Worker) bool {
		return w != nil && w.LastSeen.After(registered.LastSeen)
	})
	waitForWorker(t, s, eng, "LastSeen to advance a second time", func(w *cluster.Worker) bool {
		return w != nil && w.LastSeen.After(first.LastSeen)
	})
}

// TestEngine_WorkerHeartbeatReregistersDeletedRow is the case the sweep in
// extension.Start makes real: another instance starting up deletes every
// row whose LastSeen is old, and a live worker's row must come back.
func TestEngine_WorkerHeartbeatReregistersDeletedRow(t *testing.T) {
	eng, s := buildHeartbeatEngine(t)
	ctx := context.Background()

	registered, err := s.GetWorker(ctx, eng.WorkerID())
	if err != nil {
		t.Fatalf("Build did not register the worker row: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// What another instance's stale sweep does to this row.
	if err := s.DeregisterWorker(ctx, eng.WorkerID()); err != nil {
		t.Fatalf("DeregisterWorker: %v", err)
	}

	back := waitForWorker(t, s, eng, "the heartbeat to register the deleted row again", func(w *cluster.Worker) bool {
		return w != nil
	})

	if back.Hostname != registered.Hostname || back.Concurrency != registered.Concurrency {
		t.Errorf("re-registered row = %q x%d, want %q x%d",
			back.Hostname, back.Concurrency, registered.Hostname, registered.Concurrency)
	}
	if !slices.Equal(back.Queues, registered.Queues) {
		t.Errorf("re-registered Queues = %v, want %v", back.Queues, registered.Queues)
	}
	if back.State != cluster.WorkerActive {
		t.Errorf("re-registered State = %q, want %q", back.State, cluster.WorkerActive)
	}
	if back.Capacity[resource.Memory] != 4<<30 {
		t.Errorf("re-registered Capacity = %v, want 4 GiB of memory", back.Capacity)
	}
	if !back.CreatedAt.Equal(registered.CreatedAt) {
		t.Errorf("re-registered CreatedAt = %v, want the Build-time %v", back.CreatedAt, registered.CreatedAt)
	}
	if back.LastSeen.Before(registered.LastSeen) {
		t.Errorf("re-registered LastSeen = %v, want at or after %v", back.LastSeen, registered.LastSeen)
	}
}

// TestEngine_StopLeavesNoHeartbeatWriting pins the ordering in Stop: the
// heartbeat stops before the row is deregistered, so nothing puts the row
// back afterwards. If the loop outlived Stop, the next beat would see
// ErrWorkerNotFound and register a stopped worker as live.
func TestEngine_StopLeavesNoHeartbeatWriting(t *testing.T) {
	eng, s := buildHeartbeatEngine(t)
	ctx := context.Background()

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := eng.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := eng.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	// Absence can only be shown over time: watch for ten intervals.
	for range 10 {
		if _, err := s.GetWorker(ctx, eng.WorkerID()); !errors.Is(err, dispatch.ErrWorkerNotFound) {
			t.Fatalf("GetWorker after Stop = %v, want dispatch.ErrWorkerNotFound: something is still writing the row", err)
		}
		time.Sleep(heartbeatInterval)
	}
}

// TestEngine_StopBeforeHeartbeatStartLeavesNoLoop covers Stop winning the
// race against Start: Stop finds no loop to cancel and deregisters the row,
// and the heartbeat start that follows must not launch a loop, or its first
// beat would register the stopped worker again.
func TestEngine_StopBeforeHeartbeatStartLeavesNoLoop(t *testing.T) {
	eng, s := buildHeartbeatEngine(t)
	ctx := context.Background()

	if err := eng.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	engine.StartHeartbeatForTest(ctx, eng)

	for range 10 {
		if _, err := s.GetWorker(ctx, eng.WorkerID()); !errors.Is(err, dispatch.ErrWorkerNotFound) {
			t.Fatalf("GetWorker after Stop = %v, want dispatch.ErrWorkerNotFound: a heartbeat started after Stop", err)
		}
		time.Sleep(heartbeatInterval)
	}
}
