// Package redis implements store.Store using Redis for high-throughput
// ephemeral workloads. Jobs use Sorted Sets as priority queues, events use
// Streams, and all entities are stored as Redis Hashes.
//
// Usage:
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	s := redisstore.New(client)
//	if err := s.Ping(ctx); err != nil { ... }
package redis

import (
	"context"
	"log/slog"

	"github.com/redis/go-redis/v9"

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

// Store implements the composite store.Store interface backed by Redis.
type Store struct {
	client redis.Cmdable
	logger *slog.Logger
}

// New creates a new Redis-backed store. The caller owns the Redis client
// lifecycle.
func New(client redis.Cmdable, opts ...Option) *Store {
	s := &Store{client: client, logger: slog.Default()}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Client returns the underlying Redis client.
func (s *Store) Client() redis.Cmdable { return s.client }

// Migrate is a no-op for Redis (schemaless).
func (s *Store) Migrate(_ context.Context) error { return nil }

// Ping verifies the Redis connection is alive.
func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// Close is a no-op — the caller owns the Redis client lifecycle.
func (s *Store) Close(_ context.Context) error { return nil }
