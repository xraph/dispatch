package sqlite

import (
	"context"
	"fmt"
	"time"

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

// The claim, the release and the reopen below are each one conditional
// UPDATE. SQLite holds a single write lock for the whole database, so the
// condition and the write cannot interleave with another writer, and of
// two concurrent claims exactly one changes a row. The conditional form
// is still what decides the winner: without it the loser would overwrite
// the winner as soon as the lock came free.
//
// Those three run under withBusyRetry because they are the writes callers
// race on. Grove's sqlitedriver sets no busy_timeout, so the loser of the
// lock fails at once with SQLITE_BUSY instead of waiting, and without the
// retry a refused claim would surface as a driver error rather than as
// ErrDLQAlreadyReplayed or ErrInvalidState.
//
// Every time is bound as a time.Time, never formatted, for the reason on
// ReclaimExpiredLeases: the driver writes its own text form, and a column
// holding a mix of forms no longer sorts or compares correctly.

// ClaimReplay marks an unreplayed entry replayed by jobID.
func (s *Store) ClaimReplay(ctx context.Context, entryID id.DLQID, jobID id.JobID) error {
	now := time.Now().UTC()

	var rows int64
	err := withBusyRetry(ctx, func() error {
		res, execErr := s.sdb.NewUpdate((*dlqEntryModel)(nil)).
			Set("replayed_at = ?", now).
			Set("replayed_job_id = ?", jobID.String()).
			Where("id = ?", entryID.String()).
			Where("replayed_at IS NULL").
			Exec(ctx)
		if execErr != nil {
			return execErr
		}
		rows, _ = res.RowsAffected() //nolint:errcheck // driver always returns nil
		return nil
	})
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: claim replay: %w", err)
	}
	if rows == 1 {
		return nil
	}

	// No row changed: either there is no such entry, or it was already
	// replayed. The entry cannot come back once gone, so reading after the
	// write still tells the two apart.
	exists, err := s.dlqExists(ctx, entryID)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: claim replay: %w", err)
	}
	if !exists {
		return dispatch.ErrDLQNotFound
	}

	return dispatch.ErrDLQAlreadyReplayed
}

// ReleaseReplay undoes a claim, but only the one jobID made.
func (s *Store) ReleaseReplay(ctx context.Context, entryID id.DLQID, jobID id.JobID) error {
	var rows int64
	err := withBusyRetry(ctx, func() error {
		res, execErr := s.sdb.NewUpdate((*dlqEntryModel)(nil)).
			Set("replayed_at = NULL").
			Set("replayed_job_id = NULL").
			Where("id = ?", entryID.String()).
			Where("replayed_job_id = ?", jobID.String()).
			Exec(ctx)
		if execErr != nil {
			return execErr
		}
		rows, _ = res.RowsAffected() //nolint:errcheck // driver always returns nil
		return nil
	})
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: release replay: %w", err)
	}
	if rows == 1 {
		return nil
	}

	// Somebody else's claim, or no claim at all, is a no-op. Only a
	// missing entry is an error.
	exists, err := s.dlqExists(ctx, entryID)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: release replay: %w", err)
	}
	if !exists {
		return dispatch.ErrDLQNotFound
	}

	return nil
}

// dlqExists reports whether the entry is in the dead letter queue.
func (s *Store) dlqExists(ctx context.Context, entryID id.DLQID) (bool, error) {
	count, err := s.sdb.NewSelect((*dlqEntryModel)(nil)).
		Where("id = ?", entryID.String()).
		Count(ctx)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// GetDLQByJobID returns the newest entry, by ID, for a failed job.
// idx_dispatch_dlq_job_id holds the entries for one job in ID order, so
// this is one index probe.
func (s *Store) GetDLQByJobID(ctx context.Context, jobID id.JobID) (*dlq.Entry, error) {
	m := new(dlqEntryModel)
	err := s.sdb.NewSelect(m).
		Where("job_id = ?", jobID.String()).
		OrderExpr("id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrDLQNotFound
		}
		return nil, fmt.Errorf("dispatch/sqlite: get dlq by job id: %w", err)
	}
	return fromDLQModel(m)
}

// DeleteDLQ removes one entry.
func (s *Store) DeleteDLQ(ctx context.Context, entryID id.DLQID) error {
	res, err := s.sdb.NewDelete((*dlqEntryModel)(nil)).
		Where("id = ?", entryID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: delete dlq: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrDLQNotFound
	}
	return nil
}

// SetCronEnabled sets enabled, and next_run_at when one is given.
func (s *Store) SetCronEnabled(ctx context.Context, entryID id.CronID, enabled bool, nextRunAt *time.Time) error {
	q := s.sdb.NewUpdate((*cronEntryModel)(nil)).
		Set("enabled = ?", enabled).
		Set("updated_at = ?", time.Now().UTC())
	if nextRunAt != nil {
		q = q.Set("next_run_at = ?", *nextRunAt)
	}

	res, err := q.Where("id = ?", entryID.String()).Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: set cron enabled: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// UpdateCronNextRun sets next_run_at and nothing else the scheduler does
// not own. In particular it never writes enabled, which is what lets an
// operator's disable survive a fire that read the entry before it.
func (s *Store) UpdateCronNextRun(ctx context.Context, entryID id.CronID, nextRunAt time.Time) error {
	res, err := s.sdb.NewUpdate((*cronEntryModel)(nil)).
		Set("next_run_at = ?", nextRunAt).
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", entryID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: update cron next run: %w", err)
	}
	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// ReopenRun moves a finished run back to running, for exactly one caller.
func (s *Store) ReopenRun(ctx context.Context, runID id.RunID, expectedGeneration int64) error {
	now := time.Now().UTC()

	var rows int64
	err := withBusyRetry(ctx, func() error {
		res, execErr := s.sdb.NewUpdate((*workflowRunModel)(nil)).
			Set("state = ?", string(workflow.RunStateRunning)).
			Set("error = ?", "").
			Set("completed_at = NULL").
			Set("replay_generation = replay_generation + 1").
			Set("updated_at = ?", now).
			Where("id = ?", runID.String()).
			Where("state <> ?", string(workflow.RunStateRunning)).
			Where("replay_generation = ?", expectedGeneration).
			Exec(ctx)
		if execErr != nil {
			return execErr
		}
		rows, _ = res.RowsAffected() //nolint:errcheck // driver always returns nil
		return nil
	})
	if err != nil {
		return fmt.Errorf("dispatch/sqlite: reopen run: %w", err)
	}
	if rows == 1 {
		return nil
	}

	// No row changed: the run is missing or already running. Read the
	// state to say which, and to name it in the refusal.
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return err
	}

	return fmt.Errorf("%w: run %s is %s or its replay generation changed", dispatch.ErrInvalidState, runID, run.State)
}
