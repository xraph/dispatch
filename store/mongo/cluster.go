package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
)

// RegisterWorker adds a new worker to the cluster registry.
// Uses upsert to handle re-registration.
func (s *Store) RegisterWorker(ctx context.Context, w *cluster.Worker) error {
	m := toWorkerModel(w)
	col := s.mdb.Collection(colWorkers)

	_, err := col.UpdateOne(ctx,
		bson.M{"_id": m.ID},
		bson.M{"$set": bson.M{
			"hostname":     m.Hostname,
			"queues":       m.Queues,
			"concurrency":  m.Concurrency,
			"state":        m.State,
			"is_leader":    m.IsLeader,
			"leader_until": m.LeaderUntil,
			"last_seen":    m.LastSeen,
			"metadata":     m.Metadata,
			"created_at":   m.CreatedAt,
		}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: register worker: %w", err)
	}
	return nil
}

// DeregisterWorker removes a worker from the cluster registry.
func (s *Store) DeregisterWorker(ctx context.Context, workerID id.WorkerID) error {
	col := s.mdb.Collection(colWorkers)
	res, err := col.DeleteOne(ctx, bson.M{"_id": workerID.String()})
	if err != nil {
		return fmt.Errorf("dispatch/mongo: deregister worker: %w", err)
	}
	if res.DeletedCount == 0 {
		return dispatch.ErrWorkerNotFound
	}
	return nil
}

// HeartbeatWorker updates the last-seen timestamp for a worker.
func (s *Store) HeartbeatWorker(ctx context.Context, workerID id.WorkerID) error {
	col := s.mdb.Collection(colWorkers)
	t := now()
	var res *mongod.UpdateResult
	err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		var inner error
		res, inner = col.UpdateOne(ctx,
			bson.M{"_id": workerID.String()},
			bson.M{"$set": bson.M{"last_seen": t}},
		)
		return inner
	})
	if err != nil {
		return fmt.Errorf("dispatch/mongo: heartbeat worker: %w", err)
	}
	if res.MatchedCount == 0 {
		return dispatch.ErrWorkerNotFound
	}
	return nil
}

// HeartbeatWorkers updates last-seen for many workers in one round-trip via
// BulkWrite. This is an opt-in fast path callers can reach via type assertion;
// the cluster.Store interface only requires HeartbeatWorker.
func (s *Store) HeartbeatWorkers(ctx context.Context, workerIDs []id.WorkerID) error {
	if len(workerIDs) == 0 {
		return nil
	}
	if len(workerIDs) == 1 {
		return s.HeartbeatWorker(ctx, workerIDs[0])
	}

	t := now()
	models := make([]mongod.WriteModel, 0, len(workerIDs))
	for _, w := range workerIDs {
		models = append(models,
			mongod.NewUpdateOneModel().
				SetFilter(bson.M{"_id": w.String()}).
				SetUpdate(bson.M{"$set": bson.M{"last_seen": t}}),
		)
	}

	bulkOpts := options.BulkWrite().SetOrdered(false)
	if _, err := s.mdb.Collection(colWorkers).BulkWrite(ctx, models, bulkOpts); err != nil {
		return fmt.Errorf("dispatch/mongo: heartbeat workers bulk: %w", err)
	}
	return nil
}

// ListWorkers returns all registered workers.
func (s *Store) ListWorkers(ctx context.Context) ([]*cluster.Worker, error) {
	col := s.mdb.Collection(colWorkers)

	findOpts := options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}})
	cursor, err := col.Find(ctx, bson.M{}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list workers: %w", err)
	}
	defer cursor.Close(ctx)

	var models []workerModel
	if err := cursor.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: list workers decode: %w", err)
	}

	workers := make([]*cluster.Worker, 0, len(models))
	for i := range models {
		w, convErr := fromWorkerModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: list workers convert: %w", convErr)
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// ReapDeadWorkers returns workers whose last-seen timestamp is older than
// the given threshold.
func (s *Store) ReapDeadWorkers(ctx context.Context, threshold time.Duration) ([]*cluster.Worker, error) {
	cutoff := now().Add(-threshold)
	col := s.mdb.Collection(colWorkers)

	cursor, err := col.Find(ctx, bson.M{
		"last_seen": bson.M{"$lt": cutoff},
	})
	if err != nil {
		return nil, fmt.Errorf("dispatch/mongo: reap dead workers: %w", err)
	}
	defer cursor.Close(ctx)

	var models []workerModel
	if err := cursor.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("dispatch/mongo: reap dead workers decode: %w", err)
	}

	workers := make([]*cluster.Worker, 0, len(models))
	for i := range models {
		w, convErr := fromWorkerModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/mongo: reap dead workers convert: %w", convErr)
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// DeleteStaleWorkers removes worker rows whose last-seen timestamp is older
// than the threshold. Used at startup to evict rows from previous instances
// that crashed before deregistering. Without this, a stale row with
// is_leader=true blocks the partial-unique leader index and the new
// instance can't claim leadership until mongo's TTL sweeper catches up.
func (s *Store) DeleteStaleWorkers(ctx context.Context, threshold time.Duration) (int64, error) {
	cutoff := now().Add(-threshold)
	col := s.mdb.Collection(colWorkers)

	var deleted int64
	err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		res, e := col.DeleteMany(ctx, bson.M{
			"last_seen": bson.M{"$lt": cutoff},
		})
		if e != nil {
			return e
		}
		deleted = res.DeletedCount
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("dispatch/mongo: delete stale workers: %w", err)
	}
	return deleted, nil
}

// AcquireLeadership attempts to become the cluster leader.
//
// Relies on the partial unique index on is_leader=true (created by Migrate)
// so the index itself enforces single-leader. Two round-trips: (1) clear
// expired leaders, (2) atomic UpdateOne to claim. A duplicate-key error on
// step 2 means another worker already holds leadership.
func (s *Store) AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()
	t := now()
	until := t.Add(ttl)
	col := s.mdb.Collection(colWorkers)

	// Pin a single mongo session across both round-trips so they share one
	// pooled connection instead of grabbing two implicit sessions.
	session, sErr := s.mdb.Client().StartSession()
	if sErr != nil {
		return false, fmt.Errorf("dispatch/mongo: start session: %w", sErr)
	}
	defer session.EndSession(ctx)

	var (
		acquired bool
		runErr   error
	)
	runErr = mongod.WithSession(ctx, session, func(sctx context.Context) error {
		ok, err := s.acquireLeadershipInSession(sctx, col, wID, t, until)
		acquired = ok
		return err
	})
	return acquired, runErr
}

// acquireLeadershipInSession runs the two leadership round-trips against a
// pre-bound session context so they share a connection.
func (s *Store) acquireLeadershipInSession(ctx context.Context, col *mongod.Collection, wID string, t, until time.Time) (bool, error) {
	// RT1: clear any expired leader so the partial unique index frees up.
	if err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		_, e := col.UpdateMany(ctx,
			bson.M{
				"is_leader":    true,
				"leader_until": bson.M{"$lt": t},
			},
			bson.M{
				"$set":   bson.M{"is_leader": false},
				"$unset": bson.M{"leader_until": ""},
			},
		)
		return e
	}); err != nil {
		return false, fmt.Errorf("dispatch/mongo: clear expired leader: %w", err)
	}

	// RT2: atomic claim. The partial unique index rejects this if another
	// worker is already leader (E11000). MatchedCount=0 means our worker
	// doc isn't registered yet.
	var res *mongod.UpdateResult
	err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		var inner error
		res, inner = col.UpdateOne(ctx,
			bson.M{"_id": wID},
			bson.M{"$set": bson.M{
				"is_leader":    true,
				"leader_until": until,
			}},
		)
		return inner
	})
	if err != nil {
		if isDuplicateKey(err) {
			return false, nil
		}
		return false, fmt.Errorf("dispatch/mongo: claim leadership: %w", err)
	}
	if res.MatchedCount == 0 {
		return false, nil
	}
	return true, nil
}

// RenewLeadership extends the leader's hold.
func (s *Store) RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	until := now().Add(ttl)
	col := s.mdb.Collection(colWorkers)

	var res *mongod.UpdateResult
	err := withRetry(ctx, defaultRetry, func(ctx context.Context) error {
		var inner error
		res, inner = col.UpdateOne(ctx,
			bson.M{
				"_id":       workerID.String(),
				"is_leader": true,
			},
			bson.M{"$set": bson.M{
				"leader_until": until,
			}},
		)
		return inner
	})
	if err != nil {
		return false, fmt.Errorf("dispatch/mongo: renew leadership: %w", err)
	}
	if res.MatchedCount == 0 {
		return false, nil
	}
	return true, nil
}

// GetLeader returns the current cluster leader, or nil if there is no leader.
func (s *Store) GetLeader(ctx context.Context) (*cluster.Worker, error) {
	t := now()
	col := s.mdb.Collection(colWorkers)

	var m workerModel
	err := col.FindOne(ctx, bson.M{
		"is_leader":    true,
		"leader_until": bson.M{"$gte": t},
	}).Decode(&m)
	if err != nil {
		if isNoDocuments(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("dispatch/mongo: get leader: %w", err)
	}
	return fromWorkerModel(&m)
}
