package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
)

// RegisterWorker adds a new worker to the cluster registry.
func (s *Store) RegisterWorker(ctx context.Context, w *cluster.Worker) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dispatch_workers (
			id, hostname, queues, concurrency, state,
			is_leader, leader_until, last_seen, metadata, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			hostname = EXCLUDED.hostname,
			queues = EXCLUDED.queues,
			concurrency = EXCLUDED.concurrency,
			state = EXCLUDED.state,
			last_seen = EXCLUDED.last_seen,
			metadata = EXCLUDED.metadata`,
		w.ID.String(), w.Hostname, w.Queues, w.Concurrency,
		string(w.State), w.IsLeader, w.LeaderUntil,
		w.LastSeen, w.Metadata, w.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: register worker: %w", err)
	}
	return nil
}

// DeregisterWorker removes a worker from the cluster registry.
func (s *Store) DeregisterWorker(ctx context.Context, workerID id.WorkerID) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM dispatch_workers WHERE id = $1`,
		workerID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: deregister worker: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrWorkerNotFound
	}
	return nil
}

// HeartbeatWorker updates the last-seen timestamp for a worker.
func (s *Store) HeartbeatWorker(ctx context.Context, workerID id.WorkerID) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE dispatch_workers SET last_seen = NOW() WHERE id = $1`,
		workerID.String(),
	)
	if err != nil {
		return fmt.Errorf("dispatch/postgres: heartbeat worker: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return dispatch.ErrWorkerNotFound
	}
	return nil
}

// ListWorkers returns all registered workers.
func (s *Store) ListWorkers(ctx context.Context) ([]*cluster.Worker, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			id, hostname, queues, concurrency, state,
			is_leader, leader_until, last_seen, metadata, created_at
		FROM dispatch_workers
		ORDER BY created_at ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: list workers: %w", err)
	}
	defer rows.Close()

	var workers []*cluster.Worker
	for rows.Next() {
		w, scanErr := scanWorker(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("dispatch/postgres: scan worker row: %w", scanErr)
		}
		workers = append(workers, w)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("dispatch/postgres: iterate worker rows: %w", err)
	}
	return workers, nil
}

// ReapDeadWorkers returns workers whose last-seen timestamp is older than
// the given threshold.
func (s *Store) ReapDeadWorkers(ctx context.Context, threshold time.Duration) ([]*cluster.Worker, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			id, hostname, queues, concurrency, state,
			is_leader, leader_until, last_seen, metadata, created_at
		FROM dispatch_workers
		WHERE last_seen < NOW() - $1::interval`,
		threshold.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("dispatch/postgres: reap dead workers: %w", err)
	}
	defer rows.Close()

	var workers []*cluster.Worker
	for rows.Next() {
		w, scanErr := scanWorker(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("dispatch/postgres: scan dead worker: %w", scanErr)
		}
		workers = append(workers, w)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("dispatch/postgres: iterate dead workers: %w", err)
	}
	return workers, nil
}

// AcquireLeadership attempts to become the cluster leader.
// Uses a single atomic UPDATE to claim leadership when no valid leader exists
// or the current leader's TTL has expired.
func (s *Store) AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	wID := workerID.String()
	until := time.Now().UTC().Add(ttl)

	// Step 1: Clear any expired leader.
	_, err := s.pool.Exec(ctx, `
		UPDATE dispatch_workers
		SET is_leader = FALSE, leader_until = NULL
		WHERE is_leader = TRUE AND leader_until < NOW()`,
	)
	if err != nil {
		return false, fmt.Errorf("dispatch/postgres: clear expired leader: %w", err)
	}

	// Step 2: Check if there's already an active leader that isn't us.
	var activeLeaderID *string
	err = s.pool.QueryRow(ctx, `
		SELECT id FROM dispatch_workers
		WHERE is_leader = TRUE AND leader_until >= NOW()
		LIMIT 1`,
	).Scan(&activeLeaderID)
	if err != nil && !isNoRows(err) {
		return false, fmt.Errorf("dispatch/postgres: check leader: %w", err)
	}

	if activeLeaderID != nil && *activeLeaderID != wID {
		return false, nil
	}

	// Step 3: Claim or re-claim leadership.
	tag, claimErr := s.pool.Exec(ctx, `
		UPDATE dispatch_workers
		SET is_leader = TRUE, leader_until = $2
		WHERE id = $1`,
		wID, until,
	)
	if claimErr != nil {
		return false, fmt.Errorf("dispatch/postgres: claim leadership: %w", claimErr)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}

	return true, nil
}

// RenewLeadership extends the leader's hold.
func (s *Store) RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error) {
	until := time.Now().UTC().Add(ttl)

	tag, err := s.pool.Exec(ctx, `
		UPDATE dispatch_workers
		SET leader_until = $2
		WHERE id = $1 AND is_leader = TRUE`,
		workerID.String(), until,
	)
	if err != nil {
		return false, fmt.Errorf("dispatch/postgres: renew leadership: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	return true, nil
}

// GetLeader returns the current cluster leader, or nil if there is no leader.
func (s *Store) GetLeader(ctx context.Context) (*cluster.Worker, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			id, hostname, queues, concurrency, state,
			is_leader, leader_until, last_seen, metadata, created_at
		FROM dispatch_workers
		WHERE is_leader = TRUE AND leader_until >= NOW()
		LIMIT 1`,
	)

	w, err := scanWorker(row)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("dispatch/postgres: get leader: %w", err)
	}
	return w, nil
}

// scanWorker scans a single worker row.
func scanWorker(row pgx.Row) (*cluster.Worker, error) {
	var (
		w        cluster.Worker
		idStr    string
		stateStr string
	)
	err := row.Scan(
		&idStr, &w.Hostname, &w.Queues, &w.Concurrency, &stateStr,
		&w.IsLeader, &w.LeaderUntil, &w.LastSeen, &w.Metadata, &w.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	w.State = cluster.WorkerState(stateStr)

	parsedID, parseErr := id.ParseWorkerID(idStr)
	if parseErr != nil {
		return nil, fmt.Errorf("dispatch/postgres: parse worker id %q: %w", idStr, parseErr)
	}
	w.ID = parsedID

	return &w, nil
}
