package extension

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	forgetesting "github.com/xraph/forge/testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/sweeper"
	"github.com/xraph/dispatch/store/memory"
)

// Every store call made by this empty sweep rejects access after Close.
type blockedSweepStore struct {
	*memory.Store
	entered, release chan struct{}
	enteredOnce      sync.Once
	closed           atomic.Bool
	closes, late     atomic.Int32
}

func (s *blockedSweepStore) check() error {
	if s.closed.Load() {
		s.late.Add(1)
		return errors.New("sweep used closed store")
	}
	return nil
}
func (s *blockedSweepStore) SweepEphemeral(context.Context, artifact.SweepOpts) ([]*artifact.Artifact, error) {
	s.enteredOnce.Do(func() { close(s.entered) })
	<-s.release
	return nil, s.check()
}
func (s *blockedSweepStore) SweepOrphans(context.Context, time.Time, int) ([]*artifact.Artifact, error) {
	return nil, s.check()
}
func (s *blockedSweepStore) ListPurgeable(context.Context, time.Duration, int) ([]*artifact.Artifact, error) {
	return nil, s.check()
}
func (s *blockedSweepStore) Close() error {
	s.closes.Add(1)
	s.closed.Store(true)
	return s.Store.Close()
}

func TestExtensionStopRetainsSweeperCompletionAcrossCallerDeadlines(t *testing.T) {
	s := &blockedSweepStore{Store: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
	e := New(WithStore(s))
	if err := e.Register(forgetesting.NewTestApp("sweeper-shutdown", "0.1.0")); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Install a real sweeper with a short interval. The normal artifact-plane
	// constructor uses a fifteen-minute interval, which is unsuitable for this test.
	e.sweeper = sweeper.New(s, nil, sweeper.WithInterval(time.Millisecond))
	if err := e.sweeper.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-s.release:
		default:
			close(s.release)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := e.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("sweeper did not enter store")
	}
	var drains atomic.Int32
	e.eng.Dispatcher().BeforeStoreClose(func(context.Context) error { drains.Add(1); return nil })
	first, cancelFirst := context.WithTimeout(t.Context(), 10*time.Millisecond)
	err := e.Stop(first)
	cancelFirst()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Stop: %v", err)
	}
	completion := e.sweeperStopped
	if completion == nil {
		t.Fatal("missing retained completion task")
	}
	assertWaiting := func() {
		t.Helper()
		select {
		case <-completion:
			t.Fatal("sweeper completion signaled while store call is blocked")
		default:
		}
		if s.closes.Load() != 0 || drains.Load() != 0 {
			t.Fatal("finalization preceded sweeper completion", s.closes.Load(), drains.Load())
		}
	}
	assertWaiting()
	var callers sync.WaitGroup
	for _, deadline := range []time.Duration{10 * time.Millisecond, 30 * time.Millisecond, 50 * time.Millisecond} {
		callers.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), deadline)
			defer cancel()
			if err := e.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("concurrent Stop: %v", err)
			}
		})
	}
	callers.Wait()
	if e.sweeperStopped != completion {
		t.Fatal("Stop replaced the retained completion task")
	}
	assertWaiting()
	close(s.release)
	select {
	case <-completion:
	case <-time.After(time.Second):
		t.Fatal("sweeper completion did not arrive")
	}
	for range 4 {
		callers.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if err := e.Stop(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	callers.Wait()
	if e.sweeperStopped != completion || s.closes.Load() != 1 || drains.Load() != 1 || s.late.Load() != 0 {
		t.Fatal("shutdown did not finalize once", s.closes.Load(), drains.Load(), s.late.Load())
	}
	if err := e.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.closes.Load() != 1 || drains.Load() != 1 {
		t.Fatal("repeat Stop repeated finalization")
	}
}
