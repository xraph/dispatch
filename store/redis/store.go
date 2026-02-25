package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Compile-time interface checks.
var (
	_ job.Store      = (*Store)(nil)
	_ workflow.Store = (*Store)(nil)
	_ cron.Store     = (*Store)(nil)
	_ dlq.Store      = (*Store)(nil)
	_ event.Store    = (*Store)(nil)
	_ cluster.Store  = (*Store)(nil)
)

// Option configures the Store.
type Option func(*Store)

// WithLogger sets a custom logger.
func WithLogger(l *slog.Logger) Option {
	return func(s *Store) { s.logger = l }
}

// Store implements the composite store.Store interface backed by Redis
// via Grove KV.
type Store struct {
	kv     *kv.Store
	rdb    goredis.UniversalClient
	logger *slog.Logger
}

// New creates a new Redis KV-backed store. The caller owns the KV store
// lifecycle.
func New(store *kv.Store, opts ...Option) *Store {
	s := &Store{
		kv:     store,
		rdb:    redisdriver.UnwrapClient(store),
		logger: slog.Default(),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// KV returns the underlying KV store.
func (s *Store) KV() *kv.Store { return s.kv }

// Migrate is a no-op for Redis (schemaless).
func (s *Store) Migrate(_ context.Context) error { return nil }

// Ping verifies the Redis connection is alive.
func (s *Store) Ping(ctx context.Context) error {
	return s.kv.Ping(ctx)
}

// Close is a no-op -- the caller owns the KV store lifecycle.
func (s *Store) Close() error { return nil }

// ── helpers ──────────────────────────────────────────────────────

// now returns the current UTC time.
func now() time.Time {
	return time.Now().UTC()
}

// isNotFound checks if an error is a KV not-found sentinel.
func isNotFound(err error) bool {
	return errors.Is(err, kv.ErrNotFound)
}

// isRedisNil checks if an error is a Redis nil (key not found).
func isRedisNil(err error) bool {
	return errors.Is(err, goredis.Nil)
}

// getEntity retrieves and decodes a JSON entity from a KV key.
func (s *Store) getEntity(ctx context.Context, key string, dest any) error {
	raw, err := s.kv.GetRaw(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dest)
}

// setEntity encodes and stores a JSON entity under a KV key.
func (s *Store) setEntity(ctx context.Context, key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("dispatch/redis: marshal entity: %w", err)
	}
	return s.kv.SetRaw(ctx, key, raw)
}

// entityExists checks if an entity exists in the KV store.
func (s *Store) entityExists(ctx context.Context, key string) (bool, error) {
	_, err := s.kv.GetRaw(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// applyPagination applies offset and limit to a slice.
func applyPagination[T any](items []*T, offset, limit int) []*T {
	if offset > 0 && offset < len(items) {
		items = items[offset:]
	} else if offset >= len(items) {
		return nil
	}
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	return items
}

// jobScore computes a sorted-set score from priority and run_at.
// Lower score = dequeued first.
// We negate priority so higher priority = lower score.
func jobScore(priority int, runAt time.Time) float64 {
	return float64(-priority) + float64(runAt.UnixMilli())/1e15
}

// sleepCtx sleeps for the given duration, or returns early if the context
// is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
