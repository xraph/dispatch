package resource_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/resource"
)

func TestManagerTryAcquire(t *testing.T) {
	m := resource.NewManager(resource.Set{resource.Memory: 100, resource.CPU: 4000})

	lease, ok := m.TryAcquire("job-1", resource.Set{resource.Memory: 60})
	if !ok {
		t.Fatal("first acquire should succeed")
	}
	if m.Free()[resource.Memory] != 40 {
		t.Errorf("free memory = %d, want 40", m.Free()[resource.Memory])
	}

	if _, ok := m.TryAcquire("job-2", resource.Set{resource.Memory: 60}); ok {
		t.Error("second acquire should not fit")
	}

	lease.Release()
	if m.Free()[resource.Memory] != 100 {
		t.Errorf("free memory after release = %d, want 100", m.Free()[resource.Memory])
	}
}

func TestManagerDoubleReleaseIsSafe(t *testing.T) {
	m := resource.NewManager(resource.Set{resource.Memory: 100})
	lease, _ := m.TryAcquire("job-1", resource.Set{resource.Memory: 60})

	lease.Release()
	lease.Release()

	if m.Free()[resource.Memory] != 100 {
		t.Errorf("double release corrupted the ledger: free = %d, want 100",
			m.Free()[resource.Memory])
	}
}

func TestManagerAcquireBlocksThenSucceeds(t *testing.T) {
	m := resource.NewManager(resource.Set{resource.Memory: 100})
	held, _ := m.TryAcquire("job-1", resource.Set{resource.Memory: 80})

	done := make(chan error, 1)
	go func() {
		_, err := m.Acquire(context.Background(), "job-2", resource.Set{resource.Memory: 80})
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("acquire returned %v while capacity was held", err)
	case <-time.After(50 * time.Millisecond):
	}

	held.Release()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acquire after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not wake after release")
	}
}

func TestManagerAcquireBoundedByContext(t *testing.T) {
	m := resource.NewManager(resource.Set{resource.Memory: 100})
	defer func() { _, _ = m.TryAcquire("x", nil) }()

	if _, ok := m.TryAcquire("job-1", resource.Set{resource.Memory: 100}); !ok {
		t.Fatal("setup acquire failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := m.Acquire(ctx, "job-2", resource.Set{resource.Memory: 50})
	if !errors.Is(err, resource.ErrCapacityExceeded) {
		t.Fatalf("got %v, want ErrCapacityExceeded", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error should wrap the context cause, got %v", err)
	}
}

func TestManagerAcquireOverCapacityFailsImmediately(t *testing.T) {
	m := resource.NewManager(resource.Set{resource.Memory: 100})

	start := time.Now()
	_, err := m.Acquire(context.Background(), "job-1", resource.Set{resource.Memory: 200})
	if !errors.Is(err, resource.ErrCapacityExceeded) {
		t.Fatalf("got %v, want ErrCapacityExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("a request larger than capacity must not block, waited %v", elapsed)
	}
}

// countingReclaimer models what the artifact cache becomes: a component
// that holds a Manager lease per cached entry and returns units by
// releasing those leases, never by crediting the ledger itself.
//
// Reclaim takes the reclaimer's lock and then the manager's, via
// Release. That is only safe because the manager drops its own lock
// across Reclaim and snapshots its reclaimer map before calling
// Available, so the manager never holds its lock while reaching for
// this one. The fake is built this way on purpose: it is the test of
// that lock ordering as much as of the accounting.
type countingReclaimer struct {
	mu      sync.Mutex
	entries []resource.Lease
	calls   int
}

// newCountingReclaimer acquires count leases of each units against m,
// standing in for count cached entries.
func newCountingReclaimer(t *testing.T, m resource.Manager, key string, count int, each int64) *countingReclaimer {
	t.Helper()

	r := &countingReclaimer{}

	for i := 0; i < count; i++ {
		l, ok := m.TryAcquire("cache-entry", resource.Set{key: each})
		if !ok {
			t.Fatalf("reclaimer setup: entry %d of %d did not fit", i, count)
		}

		r.entries = append(r.entries, l)
	}

	return r
}

func (r *countingReclaimer) Reclaim(_ context.Context, key string, need int64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls++

	var freed int64

	// Evict whole entries until the shortfall is covered. Releasing the
	// lease is what actually returns the units to the manager; the
	// returned count only tells it to re-check.
	for freed < need && len(r.entries) > 0 {
		last := len(r.entries) - 1
		entry := r.entries[last]
		r.entries = r.entries[:last]

		freed += entry.Held()[key]
		entry.Release()
	}

	return freed, nil
}

func (r *countingReclaimer) Available(key string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	var avail int64
	for _, entry := range r.entries {
		avail += entry.Held()[key]
	}

	return avail
}

func TestManagerReclaimerFreesDisk(t *testing.T) {
	m := resource.NewManager(resource.Set{resource.Disk: 100})

	// Eight cached entries of 10, then a job holding the rest, so the
	// ledger is full and only reclamation can satisfy the next request.
	rec := newCountingReclaimer(t, m, resource.Disk, 8, 10)
	m.RegisterReclaimer(resource.Disk, rec)

	if _, ok := m.TryAcquire("other", resource.Set{resource.Disk: 20}); !ok {
		t.Fatal("setup acquire failed")
	}

	if got := m.Reclaimable()[resource.Disk]; got != 80 {
		t.Errorf("Reclaimable() disk = %d, want 80", got)
	}
	if got := m.Free()[resource.Disk]; got != 0 {
		t.Fatalf("setup left %d disk free, want 0", got)
	}

	lease, err := m.Acquire(context.Background(), "job-1", resource.Set{resource.Disk: 50})
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if rec.calls == 0 {
		t.Error("reclaimer was never called")
	}
	if lease.Held()[resource.Disk] != 50 {
		t.Errorf("held = %v, want 50 disk", lease.Held())
	}

	// The invariant reclamation must not break: every unit that is not
	// free is recorded against exactly one live lease. It holds because a
	// reclaimer returns units by releasing the lease that held them, so
	// release stays the ledger's only mutator.
	var accounted int64
	for _, info := range m.Leases() {
		accounted += info.Held[resource.Disk]
	}

	if free := m.Free()[resource.Disk]; accounted+free != 100 {
		t.Errorf("ledger invariant broken: %d held + %d free != 100 capacity", accounted, free)
	}
}

func TestManagerMemoryHasNoReclaimer(t *testing.T) {
	m := resource.NewManager(resource.Set{resource.Memory: 100, resource.Disk: 100})
	rec := newCountingReclaimer(t, m, resource.Disk, 10, 10)
	m.RegisterReclaimer(resource.Disk, rec)

	if _, ok := m.TryAcquire("holder", resource.Set{resource.Memory: 100}); !ok {
		t.Fatal("setup acquire failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, err := m.Acquire(ctx, "job-1", resource.Set{resource.Memory: 50}); err == nil {
		t.Fatal("memory acquisition should not have been satisfied")
	}
	if rec.calls != 0 {
		t.Errorf("the disk reclaimer was called for a memory request (%d times)", rec.calls)
	}
}

func TestManagerNeverExceedsCapacity(t *testing.T) {
	const (
		capacity   = 1000
		goroutines = 64
		iterations = 50
	)

	m := resource.NewManager(resource.Set{resource.Memory: capacity})

	var (
		mu   sync.Mutex
		held int64
		peak int64
	)

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)

		go func(g int) {
			defer wg.Done()

			want := int64((g%8)+1) * 50

			for range iterations {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				lease, err := m.Acquire(ctx, "g", resource.Set{resource.Memory: want})
				cancel()

				if err != nil {
					continue
				}

				mu.Lock()
				held += want
				if held > peak {
					peak = held
				}
				current := held
				mu.Unlock()

				if current > capacity {
					t.Errorf("held %d exceeds capacity %d", current, capacity)
				}

				mu.Lock()
				held -= want
				mu.Unlock()

				lease.Release()
			}
		}(g)
	}

	wg.Wait()

	if peak == 0 {
		t.Fatal("no acquisition ever succeeded; the test proved nothing")
	}
	if m.Free()[resource.Memory] != capacity {
		t.Errorf("ledger leaked: free = %d, want %d", m.Free()[resource.Memory], capacity)
	}
}
