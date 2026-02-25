package mongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/mongodriver"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Collection name constants.
const (
	colJobs         = "dispatch_jobs"
	colWorkflowRuns = "dispatch_workflow_runs"
	colCheckpoints  = "dispatch_checkpoints"
	colCronEntries  = "dispatch_cron_entries"
	colDLQ          = "dispatch_dlq"
	colEvents       = "dispatch_events"
	colWorkers      = "dispatch_workers"
)

// Ensure Store implements all subsystem interfaces at compile time.
var (
	_ job.Store      = (*Store)(nil)
	_ workflow.Store = (*Store)(nil)
	_ cron.Store     = (*Store)(nil)
	_ dlq.Store      = (*Store)(nil)
	_ event.Store    = (*Store)(nil)
	_ cluster.Store  = (*Store)(nil)
)

// Store is a grove ORM implementation of store.Store using MongoDB driver.
// The caller owns the *grove.DB lifecycle; Store never closes it.
type Store struct {
	db     *grove.DB
	mdb    *mongodriver.MongoDB
	logger *slog.Logger
}

// Option configures the Store.
type Option func(*Store)

// WithLogger sets the logger for the store.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Store) {
		s.logger = logger
	}
}

// New creates a new MongoDB store. The caller owns the db lifecycle -- the
// Store will not close it on Close().
func New(db *grove.DB, opts ...Option) *Store {
	s := &Store{
		db:     db,
		mdb:    mongodriver.Unwrap(db),
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// DB returns the underlying *grove.DB for advanced usage.
func (s *Store) DB() *grove.DB {
	return s.db
}

// Migrate creates indexes for all dispatch collections.
func (s *Store) Migrate(ctx context.Context) error {
	indexes := migrationIndexes()

	for col, models := range indexes {
		if len(models) == 0 {
			continue
		}

		_, err := s.mdb.Collection(col).Indexes().CreateMany(ctx, models)
		if err != nil {
			return fmt.Errorf("dispatch/mongo: migrate %s indexes: %w", col, err)
		}
	}

	return nil
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.Ping(ctx)
}

// Close is a no-op because the caller owns the *grove.DB lifecycle.
func (s *Store) Close() error {
	return nil
}

// ── helpers ──────────────────────────────────────────────────────

// now returns the current UTC time.
func now() time.Time {
	return time.Now().UTC()
}

// isNoDocuments returns true when err indicates no MongoDB documents found.
func isNoDocuments(err error) bool {
	return errors.Is(err, mongod.ErrNoDocuments)
}

// isDuplicateKey checks if a MongoDB error is a duplicate key violation.
func isDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "duplicate key") ||
		strings.Contains(err.Error(), "E11000")
}

// migrationIndexes returns the index definitions for all dispatch collections.
func migrationIndexes() map[string][]mongod.IndexModel {
	return map[string][]mongod.IndexModel{
		colJobs: {
			// Dequeue index: queue + state + priority + run_at.
			{Keys: bson.D{
				{Key: "queue", Value: 1},
				{Key: "state", Value: 1},
				{Key: "priority", Value: -1},
				{Key: "run_at", Value: 1},
			}},
			// State index.
			{Keys: bson.D{{Key: "state", Value: 1}}},
			// Scope index.
			{Keys: bson.D{
				{Key: "scope_app_id", Value: 1},
				{Key: "scope_org_id", Value: 1},
			}},
			// Heartbeat index for reaping stale jobs.
			{Keys: bson.D{
				{Key: "state", Value: 1},
				{Key: "heartbeat_at", Value: 1},
			}},
		},
		colWorkflowRuns: {
			{Keys: bson.D{{Key: "state", Value: 1}}},
			{Keys: bson.D{{Key: "created_at", Value: 1}}},
		},
		colCheckpoints: {
			// Unique compound index on (run_id, step_name).
			{
				Keys:    bson.D{{Key: "run_id", Value: 1}, {Key: "step_name", Value: 1}},
				Options: options.Index().SetUnique(true),
			},
		},
		colCronEntries: {
			// Unique name index.
			{
				Keys:    bson.D{{Key: "name", Value: 1}},
				Options: options.Index().SetUnique(true),
			},
			// Next run index for enabled entries.
			{Keys: bson.D{
				{Key: "enabled", Value: 1},
				{Key: "next_run_at", Value: 1},
			}},
		},
		colDLQ: {
			{Keys: bson.D{
				{Key: "queue", Value: 1},
				{Key: "failed_at", Value: -1},
			}},
		},
		colEvents: {
			// Pending events index for subscribe.
			{Keys: bson.D{
				{Key: "name", Value: 1},
				{Key: "acked", Value: 1},
				{Key: "created_at", Value: 1},
			}},
		},
		colWorkers: {
			{Keys: bson.D{{Key: "state", Value: 1}}},
			{Keys: bson.D{{Key: "is_leader", Value: 1}}},
			{Keys: bson.D{
				{Key: "state", Value: 1},
				{Key: "last_seen", Value: 1},
			}},
		},
	}
}
