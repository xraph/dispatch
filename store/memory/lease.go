package memory

import (
	"context"
	"sort"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Compile-time check that the memory store provides the lease capability.
var _ job.LeaseStore = (*Store)(nil)

// DequeueLeased claims up to limit ready jobs and grants each a lease.
func (m *Store) DequeueLeased(
	_ context.Context,
	queues []string,
	limit int,
	workerID id.WorkerID,
	leaseUntil time.Time,
) ([]*job.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	queueSet := make(map[string]struct{}, len(queues))
	for _, q := range queues {
		queueSet[q] = struct{}{}
	}

	now := time.Now().UTC()

	candidates := make([]*job.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		if j.State != job.StatePending && j.State != job.StateRetrying {
			continue
		}
		if !j.RunAt.IsZero() && j.RunAt.After(now) {
			continue
		}
		if len(queueSet) > 0 {
			if _, ok := queueSet[j.Queue]; !ok {
				continue
			}
		}
		candidates = append(candidates, j)
	}

	sort.Slice(candidates, func(i, k int) bool {
		if candidates[i].Priority != candidates[k].Priority {
			return candidates[i].Priority > candidates[k].Priority
		}

		return candidates[i].RunAt.Before(candidates[k].RunAt)
	})

	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}

	result := make([]*job.Job, len(candidates))
	for i, j := range candidates {
		started := now
		until := leaseUntil

		j.State = job.StateRunning
		j.StartedAt = &started
		j.WorkerID = workerID
		j.LeaseEpoch++
		j.LeaseExpiresAt = &until
		j.UpdatedAt = now

		result[i] = cloneJob(j)
	}

	return result, nil
}

// RenewLease extends the lease only if the caller still holds it.
func (m *Store) RenewLease(
	_ context.Context,
	jobID id.JobID,
	workerID id.WorkerID,
	epoch int,
	leaseUntil time.Time,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[jobID.String()]
	if !ok {
		return job.ErrLeaseLost
	}
	if j.State != job.StateRunning || j.WorkerID != workerID || j.LeaseEpoch != epoch {
		return job.ErrLeaseLost
	}

	now := time.Now().UTC()
	until := leaseUntil
	beat := now

	j.LeaseExpiresAt = &until
	j.HeartbeatAt = &beat
	j.UpdatedAt = now

	return nil
}

// ReclaimExpiredLeases returns expired-lease jobs to pending, fencing
// their previous holders.
func (m *Store) ReclaimExpiredLeases(_ context.Context, limit int) ([]*job.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()

	reclaimed := make([]*job.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		if limit > 0 && len(reclaimed) >= limit {
			break
		}
		if j.State != job.StateRunning {
			continue
		}
		lease := job.Lease{Epoch: j.LeaseEpoch}
		if j.LeaseExpiresAt != nil {
			lease.ExpiresAt = *j.LeaseExpiresAt
		}
		if !lease.IsExpired(now) {
			continue
		}

		j.State = job.StatePending
		j.RunAt = now
		j.WorkerID = id.WorkerID{}
		j.StartedAt = nil
		j.HeartbeatAt = nil
		j.LeaseExpiresAt = nil
		j.LeaseEpoch++
		j.EvictCount++
		j.UpdatedAt = now

		reclaimed = append(reclaimed, cloneJob(j))
	}

	return reclaimed, nil
}
