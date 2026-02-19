package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
)

// RegisterWorker adds a new worker to the cluster registry.
func (s *Store) RegisterWorker(ctx context.Context, w *cluster.Worker) error {
	wID := w.ID.String()
	key := workerKey(wID)

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key, workerToMap(w))
	pipe.SAdd(ctx, workerIDsKey, wID)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: register worker: %w", err)
	}
	return nil
}

// DeregisterWorker removes a worker from the cluster registry.
func (s *Store) DeregisterWorker(ctx context.Context, workerID id.WorkerID) error {
	wID := workerID.String()
	key := workerKey(wID)

	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: deregister exists: %w", err)
	}
	if exists == 0 {
		return dispatch.ErrWorkerNotFound
	}

	pipe := s.client.TxPipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, workerIDsKey, wID)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/redis: deregister worker: %w", err)
	}
	return nil
}

// HeartbeatWorker updates the last-seen timestamp for a worker.
func (s *Store) HeartbeatWorker(ctx context.Context, workerID id.WorkerID) error {
	key := workerKey(workerID.String())
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: heartbeat exists: %w", err)
	}
	if exists == 0 {
		return dispatch.ErrWorkerNotFound
	}

	_, err = s.client.HSet(ctx, key,
		"last_seen", time.Now().UTC().Format(time.RFC3339Nano),
	).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: heartbeat worker: %w", err)
	}
	return nil
}

// ListWorkers returns all registered workers.
func (s *Store) ListWorkers(ctx context.Context) ([]*cluster.Worker, error) {
	ids, err := s.client.SMembers(ctx, workerIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list workers: %w", err)
	}

	workers := make([]*cluster.Worker, 0, len(ids))
	for _, wID := range ids {
		vals, getErr := s.client.HGetAll(ctx, workerKey(wID)).Result()
		if getErr != nil || len(vals) == 0 {
			continue
		}
		w, convErr := mapToWorker(vals)
		if convErr != nil {
			continue
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// ReapDeadWorkers returns workers whose last-seen timestamp is older than the threshold.
func (s *Store) ReapDeadWorkers(ctx context.Context, threshold time.Duration) ([]*cluster.Worker, error) {
	cutoff := time.Now().UTC().Add(-threshold)

	ids, err := s.client.SMembers(ctx, workerIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: reap smembers: %w", err)
	}

	var dead []*cluster.Worker
	for _, wID := range ids {
		vals, getErr := s.client.HGetAll(ctx, workerKey(wID)).Result()
		if getErr != nil || len(vals) == 0 {
			continue
		}
		w, convErr := mapToWorker(vals)
		if convErr != nil {
			continue
		}
		if w.LastSeen.Before(cutoff) {
			dead = append(dead, w)
		}
	}
	return dead, nil
}

// AcquireLeadership attempts to become the cluster leader.
func (s *Store) AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()
	wKey := workerKey(wID)

	// Check worker exists.
	exists, err := s.client.Exists(ctx, wKey).Result()
	if err != nil {
		return false, fmt.Errorf("dispatch/redis: acquire leadership exists: %w", err)
	}
	if exists == 0 {
		return false, dispatch.ErrWorkerNotFound
	}

	// Try SET NX with TTL (atomic acquire).
	ok, err := s.client.SetNX(ctx, leaderKey, wID, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("dispatch/redis: acquire leadership setnx: %w", err)
	}
	if ok {
		// We got the lock — update worker fields.
		until := time.Now().UTC().Add(ttl)
		if _, hErr := s.client.HSet(ctx, wKey,
			"is_leader", "1",
			"leader_until", until.Format(time.RFC3339Nano),
		).Result(); hErr != nil {
			s.logger.Warn("failed to update leader fields", "error", hErr)
		}
		return true, nil
	}

	// Check if we already hold it.
	current, err := s.client.Get(ctx, leaderKey).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return false, fmt.Errorf("dispatch/redis: acquire leadership get: %w", err)
	}
	if current == wID {
		// Re-acquire: extend TTL.
		if eErr := s.client.Expire(ctx, leaderKey, ttl).Err(); eErr != nil {
			s.logger.Warn("failed to expire leader key", "error", eErr)
		}
		until := time.Now().UTC().Add(ttl)
		if _, hErr := s.client.HSet(ctx, wKey,
			"is_leader", "1",
			"leader_until", until.Format(time.RFC3339Nano),
		).Result(); hErr != nil {
			s.logger.Warn("failed to update leader fields", "error", hErr)
		}
		return true, nil
	}

	return false, nil
}

// RenewLeadership extends the leader's hold.
func (s *Store) RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()

	current, err := s.client.Get(ctx, leaderKey).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return false, nil // no leader
		}
		return false, fmt.Errorf("dispatch/redis: renew leadership get: %w", err)
	}
	if current != wID {
		return false, nil // not the leader
	}

	if eErr := s.client.Expire(ctx, leaderKey, ttl).Err(); eErr != nil {
		s.logger.Warn("failed to expire leader key", "error", eErr)
	}
	until := time.Now().UTC().Add(ttl)
	if _, hErr := s.client.HSet(ctx, workerKey(wID),
		"leader_until", until.Format(time.RFC3339Nano),
	).Result(); hErr != nil {
		s.logger.Warn("failed to update leader fields", "error", hErr)
	}
	return true, nil
}

// GetLeader returns the current cluster leader, or nil if there is no leader.
func (s *Store) GetLeader(ctx context.Context) (*cluster.Worker, error) {
	wID, err := s.client.Get(ctx, leaderKey).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil // no leader
		}
		return nil, fmt.Errorf("dispatch/redis: get leader: %w", err)
	}

	vals, err := s.client.HGetAll(ctx, workerKey(wID)).Result()
	if err != nil || len(vals) == 0 {
		return nil, nil // leader key exists but worker gone
	}
	return mapToWorker(vals)
}

// ── helpers ──

func workerToMap(w *cluster.Worker) map[string]interface{} {
	m := map[string]interface{}{
		"id":          w.ID.String(),
		"hostname":    w.Hostname,
		"queues":      marshalJSON(w.Queues),
		"concurrency": strconv.Itoa(w.Concurrency),
		"state":       string(w.State),
		"is_leader":   boolToStr(w.IsLeader),
		"last_seen":   w.LastSeen.Format(time.RFC3339Nano),
		"metadata":    marshalJSON(w.Metadata),
		"created_at":  w.CreatedAt.Format(time.RFC3339Nano),
	}
	if w.LeaderUntil != nil {
		m["leader_until"] = w.LeaderUntil.Format(time.RFC3339Nano)
	}
	return m
}

func mapToWorker(m map[string]string) (*cluster.Worker, error) {
	wID, err := id.ParseWorkerID(m["id"])
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse worker id: %w", err)
	}

	concurrency, _ := strconv.Atoi(m["concurrency"])              //nolint:errcheck // best-effort parse from trusted Redis data
	lastSeen, _ := time.Parse(time.RFC3339Nano, m["last_seen"])   //nolint:errcheck // best-effort parse from trusted Redis data
	createdAt, _ := time.Parse(time.RFC3339Nano, m["created_at"]) //nolint:errcheck // best-effort parse from trusted Redis data

	w := &cluster.Worker{
		ID:          wID,
		Hostname:    m["hostname"],
		Queues:      unmarshalStrings(m["queues"]),
		Concurrency: concurrency,
		State:       cluster.WorkerState(m["state"]),
		IsLeader:    m["is_leader"] == "1",
		LastSeen:    lastSeen,
		Metadata:    unmarshalMap(m["metadata"]),
		CreatedAt:   createdAt,
	}

	if v := m["leader_until"]; v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v) //nolint:errcheck // best-effort parse from trusted Redis data
		w.LeaderUntil = &t
	}
	return w, nil
}

func boolToStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
