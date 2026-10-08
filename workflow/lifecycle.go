package workflow

import (
	"context"
	"errors"
	"fmt"
)

// ErrRunnerShutdown is what ReplayFrom returns once Shutdown has begun.
// The refusal comes before the run is claimed, so the run is left exactly
// as it was.
var ErrRunnerShutdown = errors.New("workflow: runner is shut down")

// track counts one more background run, unless Shutdown has begun. A
// caller that gets true must call r.inflight.Done exactly once.
func (r *Runner) track() bool {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	if r.closed {
		return false
	}
	r.inflight.Add(1)
	return true
}

// Shutdown stops the background launcher. It refuses every later
// ReplayFrom with ErrRunnerShutdown, cancels the context the in-flight
// background runs execute under, and waits for them to return or for
// ctx to expire, whichever is first. On expiry it returns an error
// wrapping ctx.Err() and leaves the runs to finish on their own.
//
// A run whose steps honour cancellation fails with the context error. On
// a durable store the write recording that usually fails too, since it
// shares the cancelled context, so the run stays running and the next
// ResumeAll picks it up from its checkpoints.
//
// Shutdown is safe to call more than once; a later call waits again.
func (r *Runner) Shutdown(ctx context.Context) error {
	r.lifeMu.Lock()
	r.closed = true
	r.lifeMu.Unlock()
	r.stopLife()

	done := make(chan struct{})
	go func() {
		r.inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("workflow runner shutdown: %w", ctx.Err())
	}
}
