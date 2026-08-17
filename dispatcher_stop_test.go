package dispatch

import (
	"context"
	"sync"
	"testing"
)

type countingStore struct {
	closes int
}

func (s *countingStore) Migrate(_ context.Context) error { return nil }
func (s *countingStore) Ping(_ context.Context) error    { return nil }
func (s *countingStore) Close() error {
	s.closes++

	return nil
}

type countingExtensions struct {
	shutdowns int
}

func (e *countingExtensions) EmitShutdown(_ context.Context) { e.shutdowns++ }

type countingPool struct {
	stops int
}

func (p *countingPool) Start(_ context.Context) error { return nil }
func (p *countingPool) Stop(_ context.Context) error {
	p.stops++

	return nil
}

// TestDispatcherStopIsIdempotent covers a second Stop, which is not a
// hypothetical: Engine.Stop calls this, and a service shutting down from
// both a signal handler and a deferred cleanup calls Engine.Stop twice.
//
// Only the pool call used to be guarded, and only indirectly, through the
// started flag. EmitShutdown and the store Close ran every time. Extensions
// therefore saw two shutdown events and could release the same resources
// twice, and the store was closed twice, which the built-in backends
// tolerate only because their Close is a documented no-op. A custom Storer
// has no such promise, and neither does an extension.
//
// The engine has its own sync.Once over closeExecutors and documents that
// the rest of Stop tolerates a second call. That claim is only true if this
// one does.
func TestDispatcherStopIsIdempotent(t *testing.T) {
	store := &countingStore{}
	ext := &countingExtensions{}
	pool := &countingPool{}

	d, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	d.store = store
	d.SetExtensions(ext)
	d.SetPool(pool)
	d.started = true

	ctx := context.Background()
	if stopErr := d.Stop(ctx); stopErr != nil {
		t.Fatalf("first Stop: %v", stopErr)
	}
	if stopErr := d.Stop(ctx); stopErr != nil {
		t.Fatalf("second Stop: %v", stopErr)
	}

	if store.closes != 1 {
		t.Errorf("store closed %d times, want 1", store.closes)
	}
	if ext.shutdowns != 1 {
		t.Errorf("shutdown emitted %d times, want 1: extensions may release the "+
			"same resources on each one", ext.shutdowns)
	}
	if pool.stops != 1 {
		t.Errorf("pool stopped %d times, want 1", pool.stops)
	}
}

// TestDispatcherStopIsIdempotentUnderConcurrency covers the same guard
// reached from two goroutines at once, which a flag check without
// synchronisation would let through.
func TestDispatcherStopIsIdempotentUnderConcurrency(t *testing.T) {
	store := &countingStore{}
	ext := &countingExtensions{}

	d, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	d.store = store
	d.SetExtensions(ext)
	d.started = true

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = d.Stop(context.Background()) //nolint:errcheck // asserted via counts
		}()
	}
	wg.Wait()

	if store.closes != 1 {
		t.Errorf("store closed %d times, want 1", store.closes)
	}
	if ext.shutdowns != 1 {
		t.Errorf("shutdown emitted %d times, want 1", ext.shutdowns)
	}
}
