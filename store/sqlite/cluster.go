package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
)

// RegisterWorker adds a new worker to the cluster registry.
// Uses ON CONFLICT to upsert if the worker already exists.
func (s *Store) RegisterWorker(ctx context.Context, w *cluster.Worker) error {
	m, err := toWorkerModel(w)
	if err != nil {
		return err
	}
	_, err = s.sdb.NewInsert(m).
		OnConflict("(id) DO UPDATE").
		Set("hostname = EXCLUDED.hostname").
		Set("queues = EXCLUDED.queues").
		Set("concurrency = EXCLUDED.concurrency").
		Set("state = EXCLUDED.state").
		Set("capacity = EXCLUDED.capacity").
		Set("last_seen = EXCLUDED.last_seen").
		Set("metadata = EXCLUDED.metadata").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: register worker: %w", err)
	}
	return nil
}

// DeregisterWorker removes a worker from the cluster registry.
func (s *Store) DeregisterWorker(ctx context.Context, workerID id.WorkerID) error {
	res, err := s.sdb.NewDelete((*workerModel)(nil)).
		Where("id = ?", workerID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: deregister worker: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 { //nolint:errcheck // driver always returns nil
		return dispatch.ErrWorkerNotFound
	}
	return nil
}

// HeartbeatWorker updates the last-seen timestamp for a worker.
func (s *Store) HeartbeatWorker(ctx context.Context, workerID id.WorkerID) error {
	now := time.Now().UTC()
	res, err := s.sdb.NewUpdate((*workerModel)(nil)).
		Set("last_seen = ?", now).
		Where("id = ?", workerID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: heartbeat worker: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 { //nolint:errcheck // driver always returns nil
		return dispatch.ErrWorkerNotFound
	}
	return nil
}

// GetWorker returns one registered worker.
func (s *Store) GetWorker(ctx context.Context, workerID id.WorkerID) (*cluster.Worker, error) {
	m := new(workerModel)
	err := s.sdb.NewSelect(m).
		Where("id = ?", workerID.String()).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrWorkerNotFound
		}
		return nil, fmt.Errorf("dispatch/sqlite: get worker: %w", err)
	}
	return fromWorkerModel(m)
}

// ListWorkers returns all registered workers.
func (s *Store) ListWorkers(ctx context.Context) ([]*cluster.Worker, error) {
	var models []workerModel
	err := s.sdb.NewSelect(&models).
		OrderExpr("created_at ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: list workers: %w", err)
	}

	workers := make([]*cluster.Worker, 0, len(models))
	for i := range models {
		w, convErr := fromWorkerModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: list workers convert: %w", convErr)
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// DeleteStaleWorkers removes worker rows whose last-seen timestamp is older
// than the given threshold. Returns the number of rows deleted.
func (s *Store) DeleteStaleWorkers(ctx context.Context, threshold time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-threshold)
	res, err := s.sdb.NewDelete((*workerModel)(nil)).
		Where("last_seen < ?", cutoff).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: delete stale workers: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("dispatch/sqlite: delete stale workers rows affected: %w", err)
	}
	return n, nil
}

// ReapDeadWorkers returns workers whose last-seen timestamp is older than
// the given threshold.
func (s *Store) ReapDeadWorkers(ctx context.Context, threshold time.Duration) ([]*cluster.Worker, error) {
	cutoff := time.Now().UTC().Add(-threshold)
	var models []workerModel
	err := s.sdb.NewSelect(&models).
		Where("last_seen < ?", cutoff).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/sqlite: reap dead workers: %w", err)
	}

	workers := make([]*cluster.Worker, 0, len(models))
	for i := range models {
		w, convErr := fromWorkerModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: reap dead workers convert: %w", convErr)
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// AcquireLeadership attempts to become the cluster leader.
// The unique leader index protects the claim after the expiry check.
func (s *Store) AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()
	now := time.Now().UTC()
	until := now.Add(ttl)

	leader, err := s.readLeader(ctx)
	if err != nil {
		return false, err
	}
	if leader != nil {
		if leader.LeaderUntil != nil && !leader.LeaderUntil.Before(now) {
			if leader.ID != workerID {
				return false, nil
			}
		} else {
			// Compare parsed instants in Go. leader_until stores RFC 3339 text,
			// while a bound time.Time uses the driver's different text format.
			clearQuery := s.sdb.NewUpdate((*workerModel)(nil)).
				Set("is_leader = ?", false).
				Set("leader_until = NULL").
				Where("id = ? AND is_leader = ?", leader.ID.String(), true)
			if leader.LeaderUntil == nil {
				clearQuery = clearQuery.Where("leader_until IS NULL")
			} else {
				clearQuery = clearQuery.Where("leader_until = ?", leader.LeaderUntil.UTC().Format(time.RFC3339Nano))
			}
			result, clearErr := clearQuery.Exec(ctx)
			if clearErr != nil {
				return false, fmt.Errorf("dispatch/sqlite: clear expired leader: %w", clearErr)
			}
			n, rowsErr := result.RowsAffected()
			if rowsErr != nil {
				return false, fmt.Errorf("dispatch/sqlite: clear expired leader rows affected: %w", rowsErr)
			}
			if n == 0 {
				return false, nil // The observed lease changed before it could be cleared.
			}
		}
	}

	// Claim or re-claim leadership.
	untilStr := until.UTC().Format(time.RFC3339Nano)
	res, claimErr := s.sdb.NewUpdate((*workerModel)(nil)).
		Set("is_leader = ?", true).
		Set("leader_until = ?", untilStr).
		Where("id = ?", wID).
		Exec(ctx)
	if claimErr != nil {
		return false, fmt.Errorf("dispatch/sqlite: claim leadership: %w", claimErr)
	}
	if rows, _ := res.RowsAffected(); rows == 0 { //nolint:errcheck // driver always returns nil
		return false, nil
	}

	return true, nil
}

// RenewLeadership extends the leader's hold.
func (s *Store) RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	until := time.Now().UTC().Add(ttl)
	untilStr := until.Format(time.RFC3339Nano)

	res, err := s.sdb.NewUpdate((*workerModel)(nil)).
		Set("leader_until = ?", untilStr).
		Where("id = ? AND is_leader = ?", workerID.String(), true).
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("dispatch/sqlite: renew leadership: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 { //nolint:errcheck // driver always returns nil
		return false, nil
	}
	return true, nil
}

// GetLeader returns the current cluster leader, or nil if there is no leader.
func (s *Store) GetLeader(ctx context.Context) (*cluster.Worker, error) {
	leader, err := s.readLeader(ctx)
	if err != nil {
		return nil, err
	}
	if leader == nil || leader.LeaderUntil == nil || leader.LeaderUntil.Before(time.Now()) {
		return nil, nil
	}
	return leader, nil
}

func (s *Store) readLeader(ctx context.Context) (*cluster.Worker, error) {
	m := new(workerModel)
	err := s.sdb.NewSelect(m).
		Where("is_leader = ?", true).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("dispatch/sqlite: get leader: %w", err)
	}
	if m.LeaderUntil != nil {
		if _, parseErr := time.Parse(time.RFC3339Nano, *m.LeaderUntil); parseErr != nil {
			return nil, fmt.Errorf("dispatch/sqlite: parse leader expiry: %w", parseErr)
		}
	}
	return fromWorkerModel(m)
}
