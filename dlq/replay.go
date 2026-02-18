package dlq

import (
	"context"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Replay re-enqueues a DLQ entry as a new pending job and marks the
// entry as replayed. The new job gets a fresh ID, zero retry count,
// and runs immediately.
func (s *Service) Replay(ctx context.Context, entryID id.DLQID) (*job.Job, error) {
	entry, err := s.store.GetDLQ(ctx, entryID)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       entry.JobName,
		Queue:      entry.Queue,
		Payload:    entry.Payload,
		State:      job.StatePending,
		MaxRetries: entry.MaxRetries,
		ScopeAppID: entry.ScopeAppID,
		ScopeOrgID: entry.ScopeOrgID,
		RunAt:      now,
	}

	if err := s.jobStore.EnqueueJob(ctx, j); err != nil {
		return nil, err
	}

	if err := s.store.ReplayDLQ(ctx, entryID); err != nil {
		// The job is already enqueued. Log but don't fail.
		return j, err
	}

	return j, nil
}
