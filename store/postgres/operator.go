package postgres

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

// Each conditional write below is one UPDATE whose WHERE clause carries
// the condition. Under READ COMMITTED a second UPDATE of the same row
// waits for the first to commit, then re-checks its WHERE against the
// row the first one wrote, so of two concurrent calls exactly one
// matches. When none matches, a follow-up count tells a missing row from
// a refused one. A row that exists but did not match failed the condition
// when the UPDATE looked at it, so the refusal is reported from that fact
// and not from a second read, which could see a state that came later.

// dlqExists reports whether a dead letter entry is present.
func (s *Store) dlqExists(ctx context.Context, entryID id.DLQID) (bool, error) {
	n, err := s.pgdb.NewSelect((*dlqEntryModel)(nil)).
		Where("id = ?", entryID.String()).
		Count(ctx)
	if err != nil {
		return false, fmt.Errorf(errPrefix+"check dlq exists: %w", err)
	}
	return n > 0, nil
}

// ClaimReplay marks an unreplayed entry replayed by jobID.
func (s *Store) ClaimReplay(ctx context.Context, entryID id.DLQID, jobID id.JobID) error {
	res, err := s.pgdb.NewUpdate((*dlqEntryModel)(nil)).
		Set("replayed_at = NOW()").
		Set("replayed_job_id = ?", jobID.String()).
		Where("id = ?", entryID.String()).
		Where("replayed_at IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"claim dlq replay: %w", err)
	}

	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 1 {
		return nil
	}

	exists, err := s.dlqExists(ctx, entryID)
	if err != nil {
		return err
	}
	if !exists {
		return dispatch.ErrDLQNotFound
	}
	return dispatch.ErrDLQAlreadyReplayed
}

// ReleaseReplay undoes a claim, but only the one jobID made.
func (s *Store) ReleaseReplay(ctx context.Context, entryID id.DLQID, jobID id.JobID) error {
	res, err := s.pgdb.NewUpdate((*dlqEntryModel)(nil)).
		Set("replayed_at = NULL").
		Set("replayed_job_id = NULL").
		Where("id = ?", entryID.String()).
		Where("replayed_job_id = ?", jobID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"release dlq replay: %w", err)
	}

	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 1 {
		return nil
	}

	exists, err := s.dlqExists(ctx, entryID)
	if err != nil {
		return err
	}
	if !exists {
		return dispatch.ErrDLQNotFound
	}
	return nil
}

// GetDLQByJobID returns the newest entry, by ID, for a failed job. The
// dlq_replayed_job_id migration's (job_id, id) index answers it by
// reading one row from the end of that job's range.
func (s *Store) GetDLQByJobID(ctx context.Context, jobID id.JobID) (*dlq.Entry, error) {
	m := new(dlqEntryModel)
	err := s.pgdb.NewSelect(m).
		Where("job_id = ?", jobID.String()).
		OrderExpr("id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, dispatch.ErrDLQNotFound
		}
		return nil, fmt.Errorf(errPrefix+"get dlq by job id: %w", err)
	}
	return fromDLQModel(m)
}

// DeleteDLQ removes one entry.
func (s *Store) DeleteDLQ(ctx context.Context, entryID id.DLQID) error {
	res, err := s.pgdb.NewDelete((*dlqEntryModel)(nil)).
		Where("id = ?", entryID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"delete dlq: %w", err)
	}

	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrDLQNotFound
	}
	return nil
}

// SetCronEnabled sets enabled, and next_run_at when one is given.
func (s *Store) SetCronEnabled(ctx context.Context, entryID id.CronID, enabled bool, nextRunAt *time.Time) error {
	q := s.pgdb.NewUpdate((*cronEntryModel)(nil)).
		Set("enabled = ?", enabled).
		Set("updated_at = NOW()")
	if nextRunAt != nil {
		q = q.Set("next_run_at = ?", *nextRunAt)
	}

	res, err := q.Where("id = ?", entryID.String()).Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"set cron enabled: %w", err)
	}

	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// UpdateCronNextRun sets next_run_at, and never enabled.
func (s *Store) UpdateCronNextRun(ctx context.Context, entryID id.CronID, nextRunAt time.Time) error {
	res, err := s.pgdb.NewUpdate((*cronEntryModel)(nil)).
		Set("next_run_at = ?", nextRunAt).
		Set("updated_at = NOW()").
		Where("id = ?", entryID.String()).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"update cron next run: %w", err)
	}

	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 0 {
		return dispatch.ErrCronNotFound
	}
	return nil
}

// ReopenRun moves a finished run back to running.
func (s *Store) ReopenRun(ctx context.Context, runID id.RunID) error {
	running := string(workflow.RunStateRunning)

	res, err := s.pgdb.NewUpdate((*workflowRunModel)(nil)).
		Set("state = ?", running).
		Set("error = ''").
		Set("completed_at = NULL").
		Set("updated_at = NOW()").
		Where("id = ?", runID.String()).
		Where("state <> ?", running).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"reopen run: %w", err)
	}

	rows, _ := res.RowsAffected() //nolint:errcheck // driver always returns nil
	if rows == 1 {
		return nil
	}

	n, err := s.pgdb.NewSelect((*workflowRunModel)(nil)).
		Where("id = ?", runID.String()).
		Count(ctx)
	if err != nil {
		return fmt.Errorf(errPrefix+"check run exists: %w", err)
	}
	if n == 0 {
		return dispatch.ErrRunNotFound
	}
	return fmt.Errorf("%w: run %s is %s", dispatch.ErrInvalidState, runID, workflow.RunStateRunning)
}
