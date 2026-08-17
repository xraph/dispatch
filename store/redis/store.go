package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/grove/kv"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Compile-time interface checks.
var (
	_ job.Store         = (*Store)(nil)
	_ job.LeaseStore    = (*Store)(nil)
	_ workflow.Store    = (*Store)(nil)
	_ cron.Store        = (*Store)(nil)
	_ dlq.Store         = (*Store)(nil)
	_ event.Store       = (*Store)(nil)
	_ cluster.Store     = (*Store)(nil)
	_ artifact.Store    = (*Store)(nil)
	_ job.UsageRecorder = (*Store)(nil)
)

// Option configures the Store.
type Option func(*Store)

// WithLogger sets a custom logger.
func WithLogger(l log.Logger) Option {
	return func(s *Store) { s.logger = l }
}

// WithKeyPrefix namespaces every key and channel this store touches so
// several dispatch instances can share one Redis without seeing each
// other's jobs, cron locks or leadership. Pass the tenant's own prefix
// (for example "ws_acme:"); the empty string keeps the historical keys.
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) { s.keys = newKeys(prefix) }
}

// Store implements the composite store.Store interface over Grove KV.
//
// Every operation goes through kv.Store, including the sorted sets,
// hashes, scripts, and streams the job queue depends on. Nothing here
// imports a Redis client, so the backend this runs against is whichever
// kv driver the caller opened -- provided it supports those capabilities,
// which Store checks at construction.
type Store struct {
	kv     *kv.Store
	keys   keys
	logger log.Logger
}

// New creates a new Redis KV-backed store. The caller owns the KV store
// lifecycle.
func New(store *kv.Store, opts ...Option) *Store {
	s := &Store{
		kv:     store,
		keys:   newKeys(""),
		logger: log.NewNoopLogger(),
	}
	for _, o := range opts {
		o(s)
	}

	if missing := MissingCapabilities(store); len(missing) > 0 {
		// A driver without these cannot run jobs: the queue is a sorted
		// set and the lease handoff is a script. Saying so at startup
		// beats the first dequeue failing with ErrNotSupported, which
		// reads like a bug rather than a mis-chosen backend.
		s.logger.Warn("dispatch/redis: kv driver is missing required capabilities",
			log.String("missing", strings.Join(missing, ", ")),
		)
	}

	return s
}

// MissingCapabilities reports which of the kv capabilities this store
// needs the given driver does not provide. An empty result means the
// driver can back Dispatch.
//
// Sorted sets order the queue, sets enumerate ids, hashes hold artifact
// links, scripts make the lease compare-and-set atomic, and streams carry
// events. Pub/Sub is absent from this list deliberately: it only shortens
// wake latency, and polling covers its absence.
func MissingCapabilities(store *kv.Store) []string {
	var missing []string

	for _, c := range []struct {
		name string
		has  bool
	}{
		{"sorted sets", store.SupportsSortedSets()},
		{"sets", store.SupportsSets()},
		{"hashes", store.SupportsHashes()},
		{"scripts", store.SupportsScripts()},
		{"streams", store.SupportsStreams()},
	} {
		if !c.has {
			missing = append(missing, c.name)
		}
	}

	return missing
}

// KV returns the underlying KV store.
func (s *Store) KV() *kv.Store { return s.kv }

// KeyPrefix returns the tenant prefix every key is written under; empty
// when the store uses the historical unprefixed keys.
func (s *Store) KeyPrefix() string { return s.keys.prefix }

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
