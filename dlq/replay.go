package dlq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// Replay re-enqueues a DLQ entry as a new pending job and marks the
// entry as replayed. The new job gets a fresh ID, zero retry count,
// and runs immediately. See ReplayEntry for how a replay is made safe
// to run twice.
func (s *Service) Replay(ctx context.Context, entryID id.DLQID) (*job.Job, error) {
	entry, err := s.store.GetDLQ(ctx, entryID)
	if err != nil {
		return nil, err
	}

	return s.ReplayEntry(ctx, entry)
}

// ReplayEntry is Replay for an entry the caller has already read.
//
// The entry is claimed before anything is enqueued: the new job's ID is
// minted first and ClaimReplay records it on the entry only if nobody
// replayed or retried the entry before. Of two concurrent replays,
// exactly one gets past the claim and the other gets an error wrapping
// dispatch.ErrDLQAlreadyReplayed, so one failure is never turned back
// into two jobs. If the enqueue then fails, the claim is released and
// the entry can be replayed again.
//
// A process that dies between the claim and the enqueue leaves the entry
// marked replayed with no job behind it. That is the safe side to fail
// on: the operator sees an entry replayed into a job that does not
// exist, which is visible, where the opposite order could run the job
// twice, which is not.
func (s *Service) ReplayEntry(ctx context.Context, entry *Entry) (*job.Job, error) {
	j := replayJob(entry)

	if err := s.store.ClaimReplay(ctx, entry.ID, j.ID); err != nil {
		return nil, fmt.Errorf("replay dlq entry %s: %w", entry.ID, err)
	}

	if err := s.enqueue(ctx, j); err != nil {
		// Detached, so an enqueue that failed because ctx ended does not
		// also strand the claim.
		relCtx := context.WithoutCancel(ctx)
		if relErr := s.store.ReleaseReplay(relCtx, entry.ID, j.ID); relErr != nil {
			return nil, errors.Join(
				fmt.Errorf("replay dlq entry %s: %w", entry.ID, err),
				fmt.Errorf("release replay claim: %w", relErr),
			)
		}

		return nil, fmt.Errorf("replay dlq entry %s: %w", entry.ID, err)
	}

	return j, nil
}

// replayJob builds the job a replay of entry enqueues: a fresh pending
// job carrying everything the failed one ran with.
func replayJob(entry *Entry) *job.Job {
	now := time.Now().UTC()

	return &job.Job{
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
		// Restored from the failed job. Nothing on the replay path
		// re-derives any of these from the definition: whatever is not
		// set here silently becomes a default, and for LeaseTTL that
		// default is short enough to make a long job unrunnable. See the
		// Entry doc.
		Priority:         entry.Priority,
		Timeout:          entry.Timeout,
		LeaseTTL:         entry.LeaseTTL,
		ArtifactBindings: entry.ArtifactBindings,
		Resources:        entry.Resources,
		ResourceLimits:   entry.ResourceLimits,
		ResourceClass:    entry.ResourceClass,
		InputBytes:       entry.InputBytes,
		PrimaryInputHash: entry.PrimaryInputHash,
	}
}
