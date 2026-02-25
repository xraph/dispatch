package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
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
	res, err := col.UpdateOne(ctx,
		bson.M{"_id": workerID.String()},
		bson.M{"$set": bson.M{"last_seen": t}},
	)
	if err != nil {
		return fmt.Errorf("dispatch/mongo: heartbeat worker: %w", err)
	}
	if res.MatchedCount == 0 {
		return dispatch.ErrWorkerNotFound
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

// AcquireLeadership attempts to become the cluster leader.
// Uses a multi-step approach: clear expired, check active, claim.
func (s *Store) AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()
	t := now()
	until := t.Add(ttl)
	col := s.mdb.Collection(colWorkers)

	// Step 1: Clear any expired leader.
	_, err := col.UpdateMany(ctx,
		bson.M{
			"is_leader":    true,
			"leader_until": bson.M{"$lt": t},
		},
		bson.M{"$set": bson.M{
			"is_leader": false,
		}, "$unset": bson.M{
			"leader_until": "",
		}},
	)
	if err != nil {
		return false, fmt.Errorf("dispatch/mongo: clear expired leader: %w", err)
	}

	// Step 2: Check if there's already an active leader that isn't us.
	var activeLeader workerModel
	err = col.FindOne(ctx, bson.M{
		"is_leader":    true,
		"leader_until": bson.M{"$gte": t},
	}).Decode(&activeLeader)
	if err != nil {
		if !isNoDocuments(err) {
			return false, fmt.Errorf("dispatch/mongo: check leader: %w", err)
		}
		// No active leader -- proceed to claim.
	} else if activeLeader.ID != wID {
		return false, nil
	}

	// Step 3: Claim or re-claim leadership.
	res, err := col.UpdateOne(ctx,
		bson.M{"_id": wID},
		bson.M{"$set": bson.M{
			"is_leader":    true,
			"leader_until": until,
		}},
	)
	if err != nil {
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

	res, err := col.UpdateOne(ctx,
		bson.M{
			"_id":       workerID.String(),
			"is_leader": true,
		},
		bson.M{"$set": bson.M{
			"leader_until": until,
		}},
	)
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
