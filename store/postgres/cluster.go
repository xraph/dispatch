package postgres

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
	_, err := s.pgdb.NewInsert(m).
		OnConflict("(id) DO UPDATE").
		Set("hostname = EXCLUDED.hostname").
		Set("queues = EXCLUDED.queues").
		Set("concurrency = EXCLUDED.concurrency").
		Set("state = EXCLUDED.state").
		Set("last_seen = EXCLUDED.last_seen").
		Set("metadata = EXCLUDED.metadata").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: register worker: %w", err)
	}
	return nil
}

// DeregisterWorker removes a worker from the cluster registry.
func (s *Store) DeregisterWorker(ctx context.Context, workerID id.WorkerID) error {
	res, err := s.pgdb.NewDelete((*workerModel)(nil)).
		Where("id = ?", workerID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: deregister worker: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 { //nolint:errcheck // driver always returns nil
		return dispatch.ErrWorkerNotFound
	}
	return nil
}

// HeartbeatWorker updates the last-seen timestamp for a worker.
func (s *Store) HeartbeatWorker(ctx context.Context, workerID id.WorkerID) error {
	res, err := s.pgdb.NewUpdate((*workerModel)(nil)).
		Set("last_seen = NOW()").
		Where("id = ?", workerID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/bun: heartbeat worker: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 { //nolint:errcheck // driver always returns nil
		return dispatch.ErrWorkerNotFound
	}
	return nil
}

// ListWorkers returns all registered workers.
func (s *Store) ListWorkers(ctx context.Context) ([]*cluster.Worker, error) {
	var models []workerModel
	err := s.pgdb.NewSelect(&models).
		OrderExpr("created_at ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: list workers: %w", err)
	}

	workers := make([]*cluster.Worker, 0, len(models))
	for i := range models {
		w, convErr := fromWorkerModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/bun: list workers convert: %w", convErr)
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// ReapDeadWorkers returns workers whose last-seen timestamp is older than
// the given threshold.
func (s *Store) ReapDeadWorkers(ctx context.Context, threshold time.Duration) ([]*cluster.Worker, error) {
	var models []workerModel
	err := s.pgdb.NewSelect(&models).
		Where("last_seen < NOW() - ?::interval", threshold.String()).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch/bun: reap dead workers: %w", err)
	}

	workers := make([]*cluster.Worker, 0, len(models))
	for i := range models {
		w, convErr := fromWorkerModel(&models[i])
		if convErr != nil {
			return nil, fmt.Errorf("dispatch/bun: reap dead workers convert: %w", convErr)
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// AcquireLeadership attempts to become the cluster leader.
// Uses a multi-step approach: clear expired → check active → claim.
func (s *Store) AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()
	until := time.Now().UTC().Add(ttl)

	// Step 1: Clear any expired leader.
	_, err := s.pgdb.NewUpdate((*workerModel)(nil)).
		Set("is_leader = FALSE").
		Set("leader_until = NULL").
		Where("is_leader = TRUE AND leader_until < NOW()").
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("dispatch/bun: clear expired leader: %w", err)
	}

	// Step 2: Check if there's already an active leader that isn't us.
	var activeLeaderID string
	err = s.pgdb.NewSelect().
		TableExpr("dispatch_workers").
		Column("id").
		Where("is_leader = TRUE AND leader_until >= NOW()").
		Limit(1).
		Scan(ctx, &activeLeaderID)
	if err != nil {
		if !isNoRows(err) {
			return false, fmt.Errorf("dispatch/bun: check leader: %w", err)
		}
		// No active leader — proceed to claim.
	} else if activeLeaderID != wID {
		return false, nil
	}

	// Step 3: Claim or re-claim leadership.
	res, claimErr := s.pgdb.NewUpdate((*workerModel)(nil)).
		Set("is_leader = TRUE").
		Set("leader_until = ?", until).
		Where("id = ?", wID).
		Exec(ctx)
	if claimErr != nil {
		return false, fmt.Errorf("dispatch/bun: claim leadership: %w", claimErr)
	}
	if rows, _ := res.RowsAffected(); rows == 0 { //nolint:errcheck // driver always returns nil
		return false, nil
	}

	return true, nil
}

// RenewLeadership extends the leader's hold.
func (s *Store) RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	until := time.Now().UTC().Add(ttl)

	res, err := s.pgdb.NewUpdate((*workerModel)(nil)).
		Set("leader_until = ?", until).
		Where("id = ? AND is_leader = TRUE", workerID.String()).
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("dispatch/bun: renew leadership: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 { //nolint:errcheck // driver always returns nil
		return false, nil
	}
	return true, nil
}

// GetLeader returns the current cluster leader, or nil if there is no leader.
func (s *Store) GetLeader(ctx context.Context) (*cluster.Worker, error) {
	m := new(workerModel)
	err := s.pgdb.NewSelect(m).
		Where("is_leader = TRUE AND leader_until >= NOW()").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("dispatch/bun: get leader: %w", err)
	}
	return fromWorkerModel(m)
}
