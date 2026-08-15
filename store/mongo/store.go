package mongo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/mongodriver"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// Collection name constants.
const (
	colJobs          = "dispatch_jobs"
	colWorkflowRuns  = "dispatch_workflow_runs"
	colCheckpoints   = "dispatch_checkpoints"
	colCronEntries   = "dispatch_cron_entries"
	colDLQ           = "dispatch_dlq"
	colEvents        = "dispatch_events"
	colWorkers       = "dispatch_workers"
	colArtifacts     = "dispatch_artifacts"
	colArtifactLinks = "dispatch_artifact_links"
)

// Ensure Store implements all subsystem interfaces at compile time.
var (
	_ job.Store      = (*Store)(nil)
	_ workflow.Store = (*Store)(nil)
	_ cron.Store     = (*Store)(nil)
	_ dlq.Store      = (*Store)(nil)
	_ event.Store    = (*Store)(nil)
	_ cluster.Store  = (*Store)(nil)
	_ artifact.Store = (*Store)(nil)
	_ job.LeaseStore = (*Store)(nil)
)

// Store is a grove ORM implementation of store.Store using MongoDB driver.
// The caller owns the *grove.DB lifecycle; Store never closes it.
type Store struct {
	db     *grove.DB
	mdb    *mongodriver.MongoDB
	logger log.Logger
}

// Option configures the Store.
type Option func(*Store)

// WithLogger sets the logger for the store.
func WithLogger(logger log.Logger) Option {
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
		logger: log.NewNoopLogger(),
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

// Migrate creates indexes for all dispatch collections and adopts jobs
// left running by a pre-lease build.
//
// CreateMany is itself idempotent — mongo silently no-ops indexes that already
// exist with matching specs — so this is safe to call on every boot.
//
// Note this is the whole of the mongo backend's migration path: the grove
// migration group in migrations.go is not run from anywhere, so anything
// that must happen on upgrade belongs here rather than there.
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

	return s.backfillRunningJobLeases(ctx)
}

// backfillRunningJobLeases gives a lease expiry to every job that was
// already running when this fleet upgraded to a lease-aware build.
//
// Without it those jobs are stranded permanently. The lease feature added
// lease_expires_at, ReclaimExpiredLeases requires it to be non-null, and
// job.Lease.IsExpired deliberately reports false for a zero expiry — a
// zero value means "never leased" rather than "expired", so the reaper
// cannot steal jobs that were never leased. The pool no longer calls
// ReapStaleJobs for a store implementing job.LeaseStore, and dequeue
// claims only pending and retrying rows. A job running at the instant of
// the upgrade is therefore invisible to every recovery path and holds its
// slot forever.
//
// The filter tests lease_expires_at against null rather than using
// $exists, because this collection holds both shapes for the same absent
// value — see the comment on jobModel.ResourceRequests for why the insert
// and update paths disagree — and a plain null equality is the one test
// that matches both. It is also what makes this safe to re-run: after a
// pass the affected rows have a non-null expiry, so a second call matches
// nothing.
//
// The seeded value is deliberately in the past, which hands these jobs to
// the normal reclaim path on the very next sweep. The consequence worth
// naming: a job an old pod is still actively running is evicted and
// retried elsewhere, because an old binary's heartbeats do not push an
// expiry it does not know about. That is within the at-least-once
// contract, and it matches what the postgres and sqlite backfills do.
func (s *Store) backfillRunningJobLeases(ctx context.Context) error {
	// A bound time.Time, never a formatted string: the driver writes it as
	// a BSON date, which is what the reclaim filter's $lte compares
	// against. The equivalent sqlite backfill was first written with
	// strftime and silently wrote every row into the future, because there
	// the comparison is on text.
	t := now()

	filter := bson.M{
		"state":            string(job.StateRunning),
		"lease_expires_at": nil,
	}
	// A pipeline update, not a plain $set: the value is copied from
	// another field of the same document, which $set alone cannot express.
	update := mongod.Pipeline{
		{{Key: "$set", Value: bson.M{
			"lease_expires_at": bson.M{"$ifNull": bson.A{
				"$heartbeat_at",
				bson.M{"$ifNull": bson.A{"$started_at", t}},
			}},
			"updated_at": t,
		}}},
	}

	err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		_, updErr := s.mdb.Collection(colJobs).UpdateMany(ctx, filter, update)

		return updErr
	})
	if err != nil {
		return fmt.Errorf("dispatch/mongo: backfill running job leases: %w", err)
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
		colArtifacts: {
			// Partial unique index on the storage coordinates: only live
			// rows collide, so a purged key becomes reusable.
			{
				Keys: bson.D{
					{Key: "backend", Value: 1},
					{Key: "bucket", Value: 1},
					{Key: "key", Value: 1},
				},
				Options: options.Index().
					SetName("dispatch_artifacts_unique_live_key").
					SetUnique(true).
					SetPartialFilterExpression(bson.M{"deleted_at": bson.M{"$eq": nil}}),
			},
			{Keys: bson.D{{Key: "lifecycle", Value: 1}, {Key: "created_at", Value: 1}}},
			{Keys: bson.D{{Key: "deleted_at", Value: 1}}},
			{Keys: bson.D{{Key: "content_hash", Value: 1}}},
			{Keys: bson.D{
				{Key: "scope_app_id", Value: 1},
				{Key: "scope_org_id", Value: 1},
			}},
		},
		colArtifactLinks: {
			{
				Keys: bson.D{
					{Key: "artifact_id", Value: 1},
					{Key: "owner_kind", Value: 1},
					{Key: "owner_id", Value: 1},
					{Key: "name", Value: 1},
					{Key: "attempt", Value: 1},
				},
				Options: options.Index().
					SetName("dispatch_artifact_links_unique").
					SetUnique(true),
			},
			{Keys: bson.D{{Key: "owner_kind", Value: 1}, {Key: "owner_id", Value: 1}}},
			{Keys: bson.D{{Key: "artifact_id", Value: 1}}},
		},
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
			// Lease index for the expired-lease reclaim scan.
			{Keys: bson.D{{Key: "state", Value: 1}, {Key: "lease_expires_at", Value: 1}}},
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
			// Partial unique index lets AcquireLeadership rely on the index
			// itself to enforce single-leader (any conflicting write returns
			// E11000), reducing the election from 3 round-trips to 2.
			{
				Keys: bson.D{{Key: "is_leader", Value: 1}},
				Options: options.Index().
					SetName("dispatch_workers_unique_leader").
					SetUnique(true).
					SetPartialFilterExpression(bson.M{"is_leader": true}),
			},
			// TTL index. Worker docs whose last_seen falls behind the
			// expireAfterSeconds window get auto-deleted by mongo's TTL
			// sweeper. Set generously (5 minutes) so a transient
			// heartbeat hiccup doesn't evict a live worker; the
			// startup sweep in DeleteStaleWorkers handles the more
			// aggressive immediate cleanup. Live workers heartbeat
			// every HeartbeatInterval (default 10s) which keeps their
			// last_seen well within the window.
			{
				Keys: bson.D{{Key: "last_seen", Value: 1}},
				Options: options.Index().
					SetName("dispatch_workers_last_seen_ttl").
					SetExpireAfterSeconds(int32(300)),
			},
		},
	}
}
