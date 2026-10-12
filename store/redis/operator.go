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

// countTwoSetsScript reads SCARD of KEYS[1] and KEYS[2] in one atomic
// step and returns both.
const countTwoSetsScript = `
return {redis.call('SCARD', KEYS[1]), redis.call('SCARD', KEYS[2])}
`

// setDiffScript returns the members of KEYS[1] that are not in KEYS[2].
const setDiffScript = `
return redis.call('SDIFF', KEYS[1], KEYS[2])
`

// deleteDLQScript removes a DLQ entry and every index that points at it.
//
// KEYS[1] the entry key, KEYS[2] dlqIDs, KEYS[3] the DLQ created-order
// index, KEYS[4] the job's dlqByJob set, KEYS[5] dlqJobIndexed. ARGV[1] the
// entry ID. Returns 1 when this call deleted the entry, 0 when it was
// already gone, in which case nothing is touched.
const deleteDLQScript = `
if redis.call('DEL', KEYS[1]) == 0 then
  return 0
end
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('SREM', KEYS[4], ARGV[1])
redis.call('SREM', KEYS[5], ARGV[1])
return 1
`

// scriptStrings reads a script reply that is a list of strings.
func scriptStrings(v any) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	items, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected script reply %T", v)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		str, isStr := item.(string)
		if !isStr {
			return nil, fmt.Errorf("unexpected script reply element %T", item)
		}
		out = append(out, str)
	}
	return out, nil
}

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

	members, err := s.kv.SMembers(ctx, s.keys.dlqByJob(jobID.String()))
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
// script, and when dlqIDs holds no more members than dlqJobIndexed there is
// nothing to do, which is every call once all rows are indexed. Otherwise
// it reads the difference, looks up each entry's job, and adds the entry
// to its job's set and then to dlqJobIndexed. A crash between the two
// leaves the entry unmarked, so the next call indexes it again, and SADD
// of a present member changes nothing. An entry whose blob is gone is
// marked indexed anyway, since it has no job to be found under, so it does
// not make every later call look again.
//
// The leftovers are the same kind ensureBackfilled describes. A delete
// that lands between this read and its write leaves a dlqByJob member
// with no entry, which GetDLQByJobID skips, and a dlqJobIndexed member
// with no dlqIDs member. During a rolling upgrade, each such extra member
// can hide one entry an old process pushes, until the counts next differ.
func (s *Store) ensureDLQJobIndex(ctx context.Context) error {
	idsKey, indexedKey := s.keys.dlqIDs(), s.keys.dlqJobIndexed()

	raw, err := s.kv.Eval(ctx, countTwoSetsScript, []string{idsKey, indexedKey})
	if err != nil {
		return fmt.Errorf("dispatch/redis: count dlq ids and job index: %w", err)
	}
	counts, ok := raw.([]any)
	if !ok || len(counts) != 2 {
		return fmt.Errorf("dispatch/redis: count dlq ids and job index: unexpected reply %T", raw)
	}
	if scriptInt(counts[0]) <= scriptInt(counts[1]) {
		return nil
	}

	diff, err := s.kv.Eval(ctx, setDiffScript, []string{idsKey, indexedKey})
	if err != nil {
		return fmt.Errorf("dispatch/redis: read unindexed dlq ids: %w", err)
	}
	missing, err := scriptStrings(diff)
	if err != nil {
		return fmt.Errorf("dispatch/redis: read unindexed dlq ids: %w", err)
	}

	for _, m := range missing {
		var e dlqEntity
		getErr := s.getEntity(ctx, s.keys.dlq(m), &e)
		if getErr != nil && !isNotFound(getErr) {
			return fmt.Errorf("dispatch/redis: backfill dlq job index get %s: %w", m, getErr)
		}

		if getErr == nil {
			if _, addErr := s.kv.SAdd(ctx, s.keys.dlqByJob(e.JobID), m); addErr != nil {
				return fmt.Errorf("dispatch/redis: backfill dlq job index %s: %w", m, addErr)
			}
		}
		if _, addErr := s.kv.SAdd(ctx, indexedKey, m); addErr != nil {
			return fmt.Errorf("dispatch/redis: backfill dlq job index %s: %w", m, addErr)
		}
	}

	return nil
}

// DeleteDLQ hard-deletes one entry together with every index that points
// at it, in one script. The entry is read first only to learn its job;
// whether this call deleted it is decided by the DEL inside the script, so
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

	deleted, err := s.kv.Eval(ctx, deleteDLQScript,
		[]string{
			key,
			s.keys.dlqIDs(),
			s.keys.byCreated(entityDLQ),
			s.keys.dlqByJob(e.JobID),
			s.keys.dlqJobIndexed(),
		},
		eID,
	)
	if err != nil {
		return fmt.Errorf("dispatch/redis: delete dlq: %w", err)
	}
	if scriptInt(deleted) == 0 {
		return dispatch.ErrDLQNotFound
	}

	return nil
}

// ReopenRun moves a finished run back to running. The decision and the
// write are one compare-and-set (updateEntity), so of two concurrent
// reopens exactly one wins and the other re-reads the run, finds it
// running, and is refused.
func (s *Store) ReopenRun(ctx context.Context, runID id.RunID, expectedGeneration int64) error {
	return updateEntity(ctx, s, s.keys.run(runID.String()), dispatch.ErrRunNotFound,
		func(e *runEntity) error {
			if workflow.RunState(e.State) == workflow.RunStateRunning || e.ReplayGeneration != expectedGeneration {
				return fmt.Errorf("%w: run %s is %s or its replay generation changed", dispatch.ErrInvalidState, runID, e.State)
			}

			e.State = string(workflow.RunStateRunning)
			e.ReplayGeneration++
			e.Error = ""
			e.CompletedAt = nil
			e.UpdatedAt = now()

			return nil
		})
}
