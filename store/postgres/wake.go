package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	dispatchstore "github.com/xraph/dispatch/store"
)

// wakeChannel is the LISTEN/NOTIFY channel used to signal that new jobs
// were enqueued. EnqueueJob NOTIFYs it; StartWakeListener subscribes to it.
const wakeChannel = "dispatch_jobs_wake"

// wakeRetryInterval is how long the wake listener waits before rebuilding
// a dead LISTEN subscription.
const wakeRetryInterval = time.Second

var _ dispatchstore.WakeNotifier = (*Store)(nil)

// notifyWake signals listening instances that pending jobs exist.
// Best-effort by design: polling remains the correctness mechanism, so a
// failed NOTIFY only costs poll latency and is not worth failing the
// enqueue over.
func (s *Store) notifyWake(ctx context.Context) {
	_, _ = s.pgdb.Exec(ctx, "SELECT pg_notify($1, '')", wakeChannel) //nolint:errcheck // best-effort: polling covers missed wakes
}

// StartWakeListener subscribes to the dispatch wake channel and invokes
// wake for each notification. The subscription holds one dedicated
// connection from the pool and is rebuilt with a delay if it dies
// (connection loss, failover). The returned stop function terminates the
// listener and blocks until its connection is released.
func (s *Store) StartWakeListener(ctx context.Context, wake func()) (func(), error) {
	ctx, cancel := context.WithCancel(ctx)

	handler := func(*pgdriver.Notification) { wake() }

	// Fail fast when the first subscription cannot be established —
	// callers should know wakes are unavailable and rely on polling.
	l, err := s.pgdb.Listen(ctx, wakeChannel, handler)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("dispatch/postgres: start wake listener: %w", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				_ = l.Close()
				return
			case <-l.Done():
				// Subscription died; rebuild after a delay so a flapping
				// database doesn't get hammered with reconnects.
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(wakeRetryInterval):
			}

			nl, lerr := s.pgdb.Listen(ctx, wakeChannel, handler)
			if lerr != nil {
				continue // l.Done() is already closed; loop retries after the delay.
			}
			l = nl
			// Anything enqueued while the subscription was down produced
			// no notification; poke the pool once to cover the gap.
			wake()
		}
	}()

	stop := func() {
		cancel()
		<-done
	}
	return stop, nil
}
