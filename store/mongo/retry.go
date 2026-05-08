package mongo

import (
	"context"
	"errors"
	"time"

	mongod "go.mongodb.org/mongo-driver/v2/mongo"
)

// retryConfig controls the bounded retry behavior for transient mongo errors.
type retryConfig struct {
	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

// defaultRetry is the default policy for hot-path store operations: a couple
// of retries with short exponential backoff so a single network blip doesn't
// turn into a propagated error.
var defaultRetry = retryConfig{
	maxAttempts: 3,
	baseBackoff: 50 * time.Millisecond,
	maxBackoff:  500 * time.Millisecond,
}

// withRetry runs fn, retrying on transient mongo errors (network, timeout)
// with bounded exponential backoff. Non-transient errors (duplicate key,
// validation, ErrNoDocuments) are returned immediately.
func withRetry(ctx context.Context, cfg retryConfig, fn func(context.Context) error) error {
	if cfg.maxAttempts < 1 {
		cfg.maxAttempts = 1
	}

	var last error
	for attempt := 0; attempt < cfg.maxAttempts; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if !isTransient(err) {
			return err
		}
		last = err
		if attempt == cfg.maxAttempts-1 {
			break
		}

		delay := cfg.baseBackoff << attempt
		if delay <= 0 || delay > cfg.maxBackoff {
			delay = cfg.maxBackoff
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return last
}

// isTransient reports whether err is the kind of mongo error worth retrying.
// Excludes context cancellation (caller-driven, not transient).
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	// Mongo-level transient classifications.
	if mongod.IsNetworkError(err) {
		return true
	}
	if mongod.IsTimeout(err) {
		return true
	}
	return false
}
