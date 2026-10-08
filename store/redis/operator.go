package redis

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

var (
	_ dlq.ReplayClaimer    = (*Store)(nil)
	_ cron.TargetedUpdater = (*Store)(nil)
	_ workflow.Reopener    = (*Store)(nil)
)

// ClaimReplay marks an unreplayed entry replayed by jobID. The decision
// and the write are one compare-and-set (updateEntity), so of two
// concurrent claims exactly one wins and the other re-reads the entry,
// finds it claimed, and gets dispatch.ErrDLQAlreadyReplayed.
func (s *Store) ClaimReplay(ctx context.Context, entryID id.DLQID, jobID id.JobID) error {
	return updateEntity(ctx, s, s.keys.dlq(entryID.String()), dispatch.ErrDLQNotFound,
		func(e *dlqEntity) error {
			if e.ReplayedAt != nil {
				return dispatch.ErrDLQAlreadyReplayed
			}

			t := now()
			e.ReplayedAt = &t
			e.ReplayedJobID = jobID.String()

			return nil
		})
}

// ReleaseReplay undoes a claim, but only the one jobID made. On any other
// entry it writes nothing and returns nil.
func (s *Store) ReleaseReplay(ctx context.Context, entryID id.DLQID, jobID id.JobID) error {
	return updateEntity(ctx, s, s.keys.dlq(entryID.String()), dispatch.ErrDLQNotFound,
		func(e *dlqEntity) error {
			if e.ReplayedJobID != jobID.String() {
				return errSkipWrite
			}

			e.ReplayedAt = nil
			e.ReplayedJobID = ""

			return nil
		})
}

// GetDLQByJobID returns the newest entry, by ID, for a failed job. It
// reads the job's dlqByJob set, so it costs a few round trips however
// large the dead letter queue is, once ensureDLQJobIndex has nothing left
// to add.
func (s *Store) GetDLQByJobID(ctx context.Context, jobID id.JobID) (*dlq.Entry, error) {
	if err := s.ensureDLQJobIndex(ctx); err != nil {
		return nil, err
	}

	members, err := s.rdb.SMembers(ctx, s.keys.dlqByJob(jobID.String())).Result()
	if err != nil {
		return nil, fmt.Errorf("dispatch/redis: read dlq entries of job %s: %w", jobID, err)
	}

	// Entry IDs of one prefix sort by creation as strings, the order
	// ListDLQPage uses. Newest first.
	slices.SortFunc(members, func(a, b string) int { return strings.Compare(b, a) })

	for _, m := range members {
		var e dlqEntity
		if getErr := s.getEntity(ctx, s.keys.dlq(m), &e); getErr != nil {
			if isNotFound(getErr) {
				// A member whose entry is gone: a delete raced a backfill.
				// The next newest is the answer.
				continue
			}
			return nil, fmt.Errorf("dispatch/redis: get dlq %s: %w", m, getErr)
		}

		return fromDLQEntity(&e)
	}

	return nil, dispatch.ErrDLQNotFound
}

// ensureDLQJobIndex puts every DLQ entry that is not yet in its job's
// dlqByJob set into it. A Redis written by the release before the per-job
// index, or by a process still on that release during a rolling upgrade,
// holds entries that only dlqIDs knows about; without this, GetDLQByJobID
// would report them not found and a retry would skip their replay claim.
//
// It follows ensureBackfilled (listindex.go): both counts are read in one
// MULTI, and when dlqIDs holds no more members than dlqJobIndexed there is
// nothing to do, which is every call once all rows are indexed. Otherwise
// it reads the difference, looks up each entry's job, and adds the entry
// to its job's set and to dlqJobIndexed in one MULTI per entry. An entry
// whose blob is gone is marked indexed anyway, since it has no job to be
// found under, so it does not make every later call look again.
//
// The leftovers are the same kind ensureBackfilled describes. A delete
// that lands between this read and its write leaves a dlqByJob member
// with no entry, which GetDLQByJobID skips, and a dlqJobIndexed member
// with no dlqIDs member. During a rolling upgrade, each such extra member
// can hide one entry an old process pushes, until the counts next differ.
func (s *Store) ensureDLQJobIndex(ctx context.Context) error {
	idsKey, indexedKey := s.keys.dlqIDs(), s.keys.dlqJobIndexed()

	pipe := s.rdb.TxPipeline()
	setCount := pipe.SCard(ctx, idsKey)
	indexedCount := pipe.SCard(ctx, indexedKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("dispatch/redis: count dlq ids and job index: %w", err)
	}
	if setCount.Val() <= indexedCount.Val() {
		return nil
	}

	missing, err := s.rdb.SDiff(ctx, idsKey, indexedKey).Result()
	if err != nil {
		return fmt.Errorf("dispatch/redis: read unindexed dlq ids: %w", err)
	}

	for _, m := range missing {
		var e dlqEntity
		getErr := s.getEntity(ctx, s.keys.dlq(m), &e)
		if getErr != nil && !isNotFound(getErr) {
			return fmt.Errorf("dispatch/redis: backfill dlq job index get %s: %w", m, getErr)
		}

		add := s.rdb.TxPipeline()
		if getErr == nil {
			add.SAdd(ctx, s.keys.dlqByJob(e.JobID), m)
		}
		add.SAdd(ctx, indexedKey, m)
		if _, execErr := add.Exec(ctx); execErr != nil {
			return fmt.Errorf("dispatch/redis: backfill dlq job index %s: %w", m, execErr)
		}
	}

	return nil
}

// DeleteDLQ hard-deletes one entry together with every index that points
// at it, in one MULTI. The entry is read first only to learn its job;
// whether this call deleted it is decided by the DEL inside the MULTI, so
// of two concurrent deletes exactly one returns nil.
func (s *Store) DeleteDLQ(ctx context.Context, entryID id.DLQID) error {
	eID := entryID.String()
	key := s.keys.dlq(eID)

	var e dlqEntity
	if err := s.getEntity(ctx, key, &e); err != nil {
		if isNotFound(err) {
			return dispatch.ErrDLQNotFound
		}
		return fmt.Errorf("dispatch/redis: delete dlq get: %w", err)
	}

	pipe := s.rdb.TxPipeline()
	deleted := pipe.Del(ctx, key)
	pipe.SRem(ctx, s.keys.dlqIDs(), eID)
	pipe.ZRem(ctx, s.keys.byCreated(entityDLQ), eID)
	pipe.SRem(ctx, s.keys.dlqByJob(e.JobID), eID)
	pipe.SRem(ctx, s.keys.dlqJobIndexed(), eID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("dispatch/redis: delete dlq: %w", err)
	}
	if deleted.Val() == 0 {
		return dispatch.ErrDLQNotFound
	}

	return nil
}

// ReopenRun moves a finished run back to running. The decision and the
// write are one compare-and-set (updateEntity), so of two concurrent
// reopens exactly one wins and the other re-reads the run, finds it
// running, and is refused.
func (s *Store) ReopenRun(ctx context.Context, runID id.RunID) error {
	return updateEntity(ctx, s, s.keys.run(runID.String()), dispatch.ErrRunNotFound,
		func(e *runEntity) error {
			if workflow.RunState(e.State) == workflow.RunStateRunning {
				return fmt.Errorf("%w: run %s is %s", dispatch.ErrInvalidState, runID, e.State)
			}

			e.State = string(workflow.RunStateRunning)
			e.Error = ""
			e.CompletedAt = nil
			e.UpdatedAt = now()

			return nil
		})
}
