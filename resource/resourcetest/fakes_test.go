package resourcetest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/resource/resourcetest"
)

func TestClockAdvances(t *testing.T) {
	start := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	c := resourcetest.NewClock(start)

	if !c.Now().Equal(start) {
		t.Fatalf("Now() = %v, want %v", c.Now(), start)
	}

	c.Advance(90 * time.Second)

	if want := start.Add(90 * time.Second); !c.Now().Equal(want) {
		t.Fatalf("Now() = %v, want %v", c.Now(), want)
	}
}

// TestFakeReclaimerReturnsUnitsToManager is the point of the helper: a
// FakeReclaimer does not free capacity by decrementing a private
// counter, it frees capacity by releasing real leases it holds against
// a real Manager. This proves that end to end — the manager starts
// full, only reclamation can make room, and the ledger balances
// afterward — rather than only asserting on the fake's own bookkeeping.
func TestFakeReclaimerReturnsUnitsToManager(t *testing.T) {
	m := resource.NewManager(resource.Set{resource.Disk: 100})

	// Ten entries of ten units each, standing in for ten cached objects,
	// so a partial reclaim can be satisfied by releasing some but not all
	// of them.
	r, err := resourcetest.NewFakeReclaimer(m, resource.Disk, 10, 10)
	if err != nil {
		t.Fatalf("NewFakeReclaimer() error = %v", err)
	}
	m.RegisterReclaimer(resource.Disk, r)

	// The fake's own leases already account for the entire capacity, so
	// nothing is free and only reclamation can satisfy the next request.
	if got := m.Free()[resource.Disk]; got != 0 {
		t.Fatalf("setup: free = %d, want 0", got)
	}
	if got := m.Reclaimable()[resource.Disk]; got != 100 {
		t.Fatalf("Reclaimable() = %d, want 100", got)
	}

	lease, err := m.Acquire(context.Background(), "job-1", resource.Set{resource.Disk: 60})
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if got := lease.Held()[resource.Disk]; got != 60 {
		t.Errorf("held = %d, want 60", got)
	}

	if r.Calls() == 0 {
		t.Error("Calls() = 0, reclaimer was never invoked")
	}

	// Reclaim released six of the fake's ten-unit leases to cover the
	// sixty-unit shortfall, so it should have forty units left to offer.
	if got := r.Available(resource.Disk); got != 40 {
		t.Errorf("Available() = %d, want 40", got)
	}

	// A key the fake was not built for is never available through it and
	// never reclaimed from it.
	if got := r.Available(resource.Memory); got != 0 {
		t.Errorf("Available(memory) = %d, want 0", got)
	}
	callsBefore := r.Calls()
	if freed, err := r.Reclaim(context.Background(), resource.Memory, 10); err != nil || freed != 0 {
		t.Errorf("Reclaim(memory) = (%d, %v), want (0, nil)", freed, err)
	}
	if r.Calls() != callsBefore+1 {
		t.Errorf("Calls() = %d, want %d", r.Calls(), callsBefore+1)
	}

	// The manager's invariant must hold: every unit not free is recorded
	// against exactly one live lease, held by the job or by the fake.
	var accounted int64
	for _, info := range m.Leases() {
		accounted += info.Held[resource.Disk]
	}

	if free := m.Free()[resource.Disk]; accounted+free != 100 {
		t.Errorf("ledger invariant broken: %d held + %d free != 100 capacity", accounted, free)
	}
}

func TestFakeReclaimerSetError(t *testing.T) {
	m := resource.NewManager(resource.Set{resource.Disk: 10})

	r, err := resourcetest.NewFakeReclaimer(m, resource.Disk, 10, 1)
	if err != nil {
		t.Fatalf("NewFakeReclaimer() error = %v", err)
	}

	boom := errors.New("boom")
	r.SetError(boom)

	freed, err := r.Reclaim(context.Background(), resource.Disk, 5)
	if !errors.Is(err, boom) {
		t.Fatalf("Reclaim() error = %v, want %v", err, boom)
	}
	if freed != 0 {
		t.Errorf("freed = %d, want 0", freed)
	}
	// The lease is still held: an error must not silently release it.
	if got := r.Available(resource.Disk); got != 10 {
		t.Errorf("Available() = %d, want 10 (lease must survive an error)", got)
	}
}

func TestFakeReclaimerRefill(t *testing.T) {
	m := resource.NewManager(resource.Set{resource.Disk: 20})

	r, err := resourcetest.NewFakeReclaimer(m, resource.Disk, 10, 1)
	if err != nil {
		t.Fatalf("NewFakeReclaimer() error = %v", err)
	}

	if got := r.Available(resource.Disk); got != 10 {
		t.Fatalf("Available() = %d, want 10", got)
	}

	if err := r.Refill(10); err != nil {
		t.Fatalf("Refill() error = %v", err)
	}
	if got := r.Available(resource.Disk); got != 20 {
		t.Errorf("Available() after Refill = %d, want 20", got)
	}

	// Refill goes through the manager's real admission check, so asking
	// for more than is left must fail rather than invent capacity.
	if err := r.Refill(1); err == nil {
		t.Error("Refill() over capacity should have failed")
	}
}

func TestFakeEstimatorRecordsRequest(t *testing.T) {
	e := &resourcetest.FakeEstimator{Out: resource.MemoryGB(4)}

	got, err := e.Estimate(context.Background(), resource.Request{JobName: "tessellate"})
	if err != nil {
		t.Fatalf("Estimate() error = %v", err)
	}
	if got[resource.Memory] != 4<<30 {
		t.Errorf("got %v, want 4 GiB", got)
	}
	if e.Calls != 1 || e.Last.JobName != "tessellate" {
		t.Errorf("calls = %d, last = %+v", e.Calls, e.Last)
	}
}
