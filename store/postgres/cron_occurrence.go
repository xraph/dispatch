package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/dispatch/job"
)

// EnqueueCronOccurrence advances the schedule and writes its job in one transaction.
func (s *Store) EnqueueCronOccurrence(ctx context.Context, j *job.Job, f job.CronOccurrence) error {
	if !f.Next.After(f.Due) {
		return fmt.Errorf("cron next occurrence must follow due time")
	}
	model, err := toJobModel(j)
	if err != nil {
		return err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() //nolint:errcheck // rollback after commit is harmless
	result, err := tx.Exec(ctx, `UPDATE dispatch_cron_entries
 SET next_run_at=$1, last_run_at=$2, updated_at=clock_timestamp()
 WHERE id=$3 AND enabled AND next_run_at=$4 AND locked_by=$5
 AND locked_until>clock_timestamp() AND schedule=$6`, f.Next, f.FiredAt, f.CronID.String(), f.Due, f.WorkerID.String(), f.Schedule)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return job.ErrCronOccurrenceLost
	}
	query, args, err := s.pgdb.NewInsert(model).Build()
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, query, args...); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notifyWake(ctx)
	return nil
}
