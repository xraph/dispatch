package cluster

import (
	"context"
	"time"

	"github.com/xraph/dispatch/id"
)

// Store defines the persistence contract for cluster worker management.
type Store interface {
	// RegisterWorker adds a new worker to the cluster registry.
	RegisterWorker(ctx context.Context, w *Worker) error

	// DeregisterWorker removes a worker from the cluster registry.
	DeregisterWorker(ctx context.Context, workerID id.WorkerID) error

	// HeartbeatWorker updates the last-seen timestamp for a worker,
	// indicating it is still alive.
	HeartbeatWorker(ctx context.Context, workerID id.WorkerID) error

	// ListWorkers returns all registered workers.
	ListWorkers(ctx context.Context) ([]*Worker, error)

	// ReapDeadWorkers returns workers whose last-seen timestamp is older than
	// the given threshold, indicating they may have crashed.
	ReapDeadWorkers(ctx context.Context, threshold time.Duration) ([]*Worker, error)

	// AcquireLeadership attempts to become the cluster leader.
	// Returns true if this worker is now leader. The leadership
	// expires after ttl if not renewed.
	AcquireLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error)

	// RenewLeadership extends the leader's hold. Must be called
	// before the TTL expires.
	RenewLeadership(ctx context.Context, workerID id.WorkerID, ttl time.Duration) (bool, error)

	// GetLeader returns the current cluster leader, or nil if there
	// is no leader.
	GetLeader(ctx context.Context) (*Worker, error)
}
