package storetest

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
)

// RunClusterSuite pins the worker row contract the engine's heartbeat and
// the dashboard's worker list rely on: a registered row reads back whole,
// capacity included, a heartbeat moves LastSeen to now, and both reads
// and heartbeats on a row that does not exist say so with
// dispatch.ErrWorkerNotFound. The engine re-registers on exactly that
// sentinel, so a backend that returned nil or another error for a missing
// row would leave a live worker invisible after another instance's stale
// sweep removed it.
//
// newStore may return a shared store. Each case registers its own
// worker IDs and never asserts on ListWorkers, and no case sets IsLeader,
// so the single-leader index some backends keep is never contended.
func RunClusterSuite(t *testing.T, newStore func(t *testing.T) cluster.Store) {
	t.Helper()

	cases := []struct {
		name string
		fn   func(t *testing.T, s cluster.Store)
	}{
		{"GetWorkerRoundTripsEveryField", testGetWorkerRoundTripsEveryField},
		{"GetWorkerUnknownIsNotFound", testGetWorkerUnknownIsNotFound},
		{"ReregisterReplacesCapacity", testReregisterReplacesCapacity},
		{"HeartbeatAdvancesLastSeen", testHeartbeatAdvancesLastSeen},
		{"HeartbeatUnknownIsNotFound", testHeartbeatUnknownIsNotFound},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(t, newStore(t)) })
	}
}

// suiteWorker builds a worker with every field set to something a backend
// could plausibly drop. Times are truncated to the millisecond, the
// coarsest precision any backend stores (BSON dates).
//
// LeaderUntil is set while IsLeader stays false so the round trip covers
// the column without claiming leadership on a store other tests share.
func suiteWorker() *cluster.Worker {
	now := time.Now().UTC().Truncate(time.Millisecond)
	until := now.Add(time.Minute)

	return &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    "suite-host",
		Queues:      []string{"default", "render"},
		Concurrency: 7,
		State:       cluster.WorkerActive,
		Capacity: resource.Set{
			resource.CPU:    4000,
			resource.Memory: 8 * GiB,
			"fpga":          2,
		},
		LeaderUntil: &until,
		LastSeen:    now,
		Metadata:    map[string]string{"zone": "eu-west-1", "version": "1.2.3"},
		CreatedAt:   now.Add(-time.Hour),
	}
}

func registerWorker(t *testing.T, s cluster.Store, w *cluster.Worker) {
	t.Helper()

	if err := s.RegisterWorker(context.Background(), w); err != nil {
		t.Fatalf("RegisterWorker %s: %v", w.ID, err)
	}
}

func getWorker(t *testing.T, s cluster.Store, workerID id.WorkerID) *cluster.Worker {
	t.Helper()

	got, err := s.GetWorker(context.Background(), workerID)
	if err != nil {
		t.Fatalf("GetWorker %s: %v", workerID, err)
	}

	return got
}

func testGetWorkerRoundTripsEveryField(t *testing.T, s cluster.Store) {
	want := suiteWorker()
	registerWorker(t, s, want)

	got := getWorker(t, s, want.ID)

	if got.ID.String() != want.ID.String() {
		t.Errorf("ID = %s, want %s", got.ID, want.ID)
	}
	if got.Hostname != want.Hostname {
		t.Errorf("Hostname = %q, want %q", got.Hostname, want.Hostname)
	}
	if !slices.Equal(got.Queues, want.Queues) {
		t.Errorf("Queues = %v, want %v", got.Queues, want.Queues)
	}
	if got.Concurrency != want.Concurrency {
		t.Errorf("Concurrency = %d, want %d", got.Concurrency, want.Concurrency)
	}
	if got.State != want.State {
		t.Errorf("State = %q, want %q", got.State, want.State)
	}
	if !maps.Equal(got.Capacity, want.Capacity) {
		t.Errorf("Capacity = %v, want %v", got.Capacity, want.Capacity)
	}
	if got.IsLeader != want.IsLeader {
		t.Errorf("IsLeader = %v, want %v", got.IsLeader, want.IsLeader)
	}
	if got.LeaderUntil == nil || !got.LeaderUntil.Equal(*want.LeaderUntil) {
		t.Errorf("LeaderUntil = %v, want %v", got.LeaderUntil, *want.LeaderUntil)
	}
	if !got.LastSeen.Equal(want.LastSeen) {
		t.Errorf("LastSeen = %v, want %v", got.LastSeen, want.LastSeen)
	}
	if !maps.Equal(got.Metadata, want.Metadata) {
		t.Errorf("Metadata = %v, want %v", got.Metadata, want.Metadata)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
}

func testGetWorkerUnknownIsNotFound(t *testing.T, s cluster.Store) {
	got, err := s.GetWorker(context.Background(), id.NewWorkerID())
	if !errors.Is(err, dispatch.ErrWorkerNotFound) {
		t.Fatalf("GetWorker(unknown) = (%v, %v), want dispatch.ErrWorkerNotFound", got, err)
	}
}

// testReregisterReplacesCapacity covers the upsert path: the engine
// registers again after a sweep, and a worker restarted with a new
// capacity under the same ID must not keep advertising the old one.
func testReregisterReplacesCapacity(t *testing.T, s cluster.Store) {
	w := suiteWorker()
	registerWorker(t, s, w)

	again := *w
	again.Capacity = resource.Set{resource.Memory: 2 * GiB}
	again.LastSeen = w.LastSeen.Add(time.Second)
	registerWorker(t, s, &again)

	got := getWorker(t, s, w.ID)
	if !maps.Equal(got.Capacity, again.Capacity) {
		t.Errorf("Capacity after re-register = %v, want %v", got.Capacity, again.Capacity)
	}
	if !got.LastSeen.Equal(again.LastSeen) {
		t.Errorf("LastSeen after re-register = %v, want %v", got.LastSeen, again.LastSeen)
	}
}

func testHeartbeatAdvancesLastSeen(t *testing.T, s cluster.Store) {
	w := suiteWorker()
	w.LastSeen = w.LastSeen.Add(-time.Minute)
	registerWorker(t, s, w)

	if err := s.HeartbeatWorker(context.Background(), w.ID); err != nil {
		t.Fatalf("HeartbeatWorker: %v", err)
	}

	got := getWorker(t, s, w.ID)
	if !got.LastSeen.After(w.LastSeen) {
		t.Fatalf("LastSeen after heartbeat = %v, want after %v", got.LastSeen, w.LastSeen)
	}
	if drift := time.Since(got.LastSeen).Abs(); drift > 5*time.Second {
		t.Fatalf("LastSeen after heartbeat = %v, %v away from now; want within 5s", got.LastSeen, drift)
	}
}

func testHeartbeatUnknownIsNotFound(t *testing.T, s cluster.Store) {
	err := s.HeartbeatWorker(context.Background(), id.NewWorkerID())
	if !errors.Is(err, dispatch.ErrWorkerNotFound) {
		t.Fatalf("HeartbeatWorker(unknown) error = %v, want dispatch.ErrWorkerNotFound", err)
	}
}
