package redis_test

import (
	"context"
	"testing"
	"time"
)

// TestWakeListenerFiresOnEnqueueJob proves the pub/sub wake path: a wake
// listener started on one store connection fires when a job is enqueued
// through another connection (i.e. another app instance), without waiting
// for any poll interval.
func TestWakeListenerFiresOnEnqueueJob(t *testing.T) {
	connStr := startRedis(t)
	listenerStore := openRedisStore(t, connStr)
	enqueuerStore := openRedisStore(t, connStr)
	ctx := context.Background()

	woke := make(chan struct{}, 8)
	stop, err := listenerStore.StartWakeListener(ctx, func() {
		select {
		case woke <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatalf("start wake listener: %v", err)
	}
	t.Cleanup(stop)

	if err := enqueuerStore.EnqueueJob(ctx, runningJob("wake-me", 0)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-woke:
	case <-time.After(5 * time.Second):
		t.Fatal("wake listener did not fire after cross-connection enqueue")
	}
}

// TestWakeListenerStopTerminates verifies stop returns and shuts the
// subscriber down cleanly.
func TestWakeListenerStopTerminates(t *testing.T) {
	s := openReapRedis(t)

	stop, err := s.StartWakeListener(context.Background(), func() {})
	if err != nil {
		t.Fatalf("start wake listener: %v", err)
	}

	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not return")
	}
}
