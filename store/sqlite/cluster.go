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
	m := toWorkerModel(w)
	_, err := s.sdb.NewInsert(m).
		OnConflict("(id) DO UPDATE").
		Set("hostname = EXCLUDED.hostname").
		Set("queues = EXCLUDED.queues").
		Set("concurrency = EXCLUDED.concurrency").
		Set("state = EXCLUDED.state").
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
// Uses a multi-step approach: clear expired, check active, claim.
func (s *Store) AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()
	now := time.Now().UTC()
	until := now.Add(ttl)

	// Step 1: Clear any expired leader.
	_, err := s.sdb.NewUpdate((*workerModel)(nil)).
		Set("is_leader = ?", false).
		Set("leader_until = NULL").
		Where("is_leader = ? AND leader_until < ?", true, now).
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("dispatch/sqlite: clear expired leader: %w", err)
	}

	// Step 2: Check if there's already an active leader that isn't us.
	var activeLeaderID string
	err = s.sdb.NewSelect().
		TableExpr("dispatch_workers").
		Column("id").
		Where("is_leader = ? AND leader_until >= ?", true, now).
		Limit(1).
		Scan(ctx, &activeLeaderID)
	if err != nil {
		if !isNoRows(err) {
			return false, fmt.Errorf("dispatch/sqlite: check leader: %w", err)
		}
		// No active leader -- proceed to claim.
	} else if activeLeaderID != wID {
		return false, nil
	}

	// Step 3: Claim or re-claim leadership.
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
	now := time.Now().UTC()
	m := new(workerModel)
	err := s.sdb.NewSelect(m).
		Where("is_leader = ? AND leader_until >= ?", true, now).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("dispatch/sqlite: get leader: %w", err)
	}
	return fromWorkerModel(m)
}
