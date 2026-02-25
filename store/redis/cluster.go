package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
)

// ── JSON model for KV storage ──

type workerEntity struct {
	ID          string            `json:"id"`
	Hostname    string            `json:"hostname"`
	Queues      []string          `json:"queues"`
	Concurrency int               `json:"concurrency"`
	State       string            `json:"state"`
	IsLeader    bool              `json:"is_leader"`
	LeaderUntil *time.Time        `json:"leader_until,omitempty"`
	LastSeen    time.Time         `json:"last_seen"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

func toWorkerEntity(w *cluster.Worker) *workerEntity {
	return &workerEntity{
		ID:          w.ID.String(),
		Hostname:    w.Hostname,
		Queues:      w.Queues,
		Concurrency: w.Concurrency,
		State:       string(w.State),
		IsLeader:    w.IsLeader,
		LeaderUntil: w.LeaderUntil,
		LastSeen:    w.LastSeen,
		Metadata:    w.Metadata,
		CreatedAt:   w.CreatedAt,
	}
}

func fromWorkerEntity(e *workerEntity) (*cluster.Worker, error) {
	wID, err := id.ParseWorkerID(e.ID)
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: parse worker id: %w", err)
	}

	return &cluster.Worker{
		ID:          wID,
		Hostname:    e.Hostname,
		Queues:      e.Queues,
		Concurrency: e.Concurrency,
		State:       cluster.WorkerState(e.State),
		IsLeader:    e.IsLeader,
		LeaderUntil: e.LeaderUntil,
		LastSeen:    e.LastSeen,
		Metadata:    e.Metadata,
		CreatedAt:   e.CreatedAt,
	}, nil
}

// RegisterWorker adds a new worker to the cluster registry.
func (s *Store) RegisterWorker(ctx context.Context, w *cluster.Worker) error {
	wID := w.ID.String()
	key := workerKey(wID)

	e := toWorkerEntity(w)
	if err := s.setEntity(ctx, key, e); err != nil {
		return fmt.Errorf("dispatch/redis: register worker set: %w", err)
	}

	if err := s.rdb.SAdd(ctx, workerIDsKey, wID).Err(); err != nil {
		return fmt.Errorf("dispatch/redis: register worker index: %w", err)
	}
	return nil
}

// DeregisterWorker removes a worker from the cluster registry.
func (s *Store) DeregisterWorker(ctx context.Context, workerID id.WorkerID) error {
	wID := workerID.String()
	key := workerKey(wID)

	exists, err := s.entityExists(ctx, key)
	if err != nil {
		return fmt.Errorf("dispatch/redis: deregister exists: %w", err)
	}
	if !exists {
		return dispatch.ErrWorkerNotFound
	}

	pipe := s.rdb.TxPipeline()
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

	var e workerEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return dispatch.ErrWorkerNotFound
		}
		return fmt.Errorf("dispatch/redis: heartbeat get: %w", err)
	}

	e.LastSeen = now()
	return s.setEntity(ctx, key, &e)
}

// ListWorkers returns all registered workers.
func (s *Store) ListWorkers(ctx context.Context) ([]*cluster.Worker, error) {
	ids, err := s.rdb.SMembers(ctx, workerIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: list workers: %w", err)
	}

	workers := make([]*cluster.Worker, 0, len(ids))
	for _, wID := range ids {
		var e workerEntity
		if getErr := s.getEntity(ctx, workerKey(wID), &e); getErr != nil {
			continue
		}
		w, convErr := fromWorkerEntity(&e)
		if convErr != nil {
			continue
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// ReapDeadWorkers returns workers whose last-seen timestamp is older than the threshold.
func (s *Store) ReapDeadWorkers(ctx context.Context, threshold time.Duration) ([]*cluster.Worker, error) {
	cutoff := now().Add(-threshold)

	ids, err := s.rdb.SMembers(ctx, workerIDsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: reap smembers: %w", err)
	}

	var dead []*cluster.Worker
	for _, wID := range ids {
		var e workerEntity
		if getErr := s.getEntity(ctx, workerKey(wID), &e); getErr != nil {
			continue
		}
		if e.LastSeen.Before(cutoff) {
			w, convErr := fromWorkerEntity(&e)
			if convErr != nil {
				continue
			}
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
	exists, err := s.entityExists(ctx, wKey)
	if err != nil {
		return false, fmt.Errorf("dispatch/redis: acquire leadership exists: %w", err)
	}
	if !exists {
		return false, dispatch.ErrWorkerNotFound
	}

	// Try SET NX with TTL (atomic acquire).
	ok, err := s.rdb.SetNX(ctx, leaderKey, wID, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("dispatch/redis: acquire leadership setnx: %w", err)
	}
	if ok {
		// We got the lock -- update worker fields.
		until := now().Add(ttl)
		var e workerEntity
		if getErr := s.getEntity(ctx, wKey, &e); getErr == nil {
			e.IsLeader = true
			e.LeaderUntil = &until
			_ = s.setEntity(ctx, wKey, &e) //nolint:errcheck // best-effort update
		}
		return true, nil
	}

	// Check if we already hold it.
	current, err := s.rdb.Get(ctx, leaderKey).Result()
	if err != nil && !isRedisNil(err) {
		return false, fmt.Errorf("dispatch/redis: acquire leadership get: %w", err)
	}
	if current == wID {
		// Re-acquire: extend TTL.
		_ = s.rdb.Expire(ctx, leaderKey, ttl).Err() //nolint:errcheck // best-effort
		until := now().Add(ttl)
		var e workerEntity
		if getErr := s.getEntity(ctx, wKey, &e); getErr == nil {
			e.IsLeader = true
			e.LeaderUntil = &until
			_ = s.setEntity(ctx, wKey, &e) //nolint:errcheck // best-effort update
		}
		return true, nil
	}

	return false, nil
}

// RenewLeadership extends the leader's hold.
func (s *Store) RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()

	current, err := s.rdb.Get(ctx, leaderKey).Result()
	if err != nil {
		if isRedisNil(err) {
			return false, nil // no leader
		}
		return false, fmt.Errorf("dispatch/redis: renew leadership get: %w", err)
	}
	if current != wID {
		return false, nil // not the leader
	}

	_ = s.rdb.Expire(ctx, leaderKey, ttl).Err() //nolint:errcheck // best-effort
	until := now().Add(ttl)
	var e workerEntity
	if getErr := s.getEntity(ctx, workerKey(wID), &e); getErr == nil {
		e.LeaderUntil = &until
		_ = s.setEntity(ctx, workerKey(wID), &e) //nolint:errcheck // best-effort update
	}
	return true, nil
}

// GetLeader returns the current cluster leader, or nil if there is no leader.
func (s *Store) GetLeader(ctx context.Context) (*cluster.Worker, error) {
	wID, err := s.rdb.Get(ctx, leaderKey).Result()
	if err != nil {
		if isRedisNil(err) {
			return nil, nil // no leader
		}
		return nil, fmt.Errorf("dispatch/redis: get leader: %w", err)
	}

	var e workerEntity
	if getErr := s.getEntity(ctx, workerKey(wID), &e); getErr != nil {
		return nil, nil // leader key exists but worker gone
	}
	return fromWorkerEntity(&e)
}
