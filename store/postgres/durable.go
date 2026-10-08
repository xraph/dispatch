package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	log "github.com/xraph/go-utils/log"
	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

var _ durable.Store = (*Store)(nil)

const executionColumns = `namespace, workflow_id, run_id, workflow_type, build_id,
    state, revision, last_sequence, input, output, created_at, updated_at`

func scanExecution(row driver.Row) (durable.Execution, error) {
	var e durable.Execution
	err := row.Scan(&e.Namespace, &e.WorkflowID, &e.RunID, &e.WorkflowType, &e.BuildID,
		&e.State, &e.Revision, &e.LastSequence, &e.Input, &e.Output, &e.CreatedAt, &e.UpdatedAt)
	if isNoRows(err) {
		return durable.Execution{}, durable.ErrNotFound
	}
	return e, err
}

// GetExecution reads the persisted projection for one namespace-qualified run.
func (s *Store) GetExecution(ctx context.Context, key durable.Key) (durable.Execution, error) {
	if err := key.Validate(); err != nil {
		return durable.Execution{}, err
	}
	return scanExecution(s.pgdb.QueryRow(ctx, `SELECT `+executionColumns+`
        FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`,
		key.Namespace, key.WorkflowID, key.RunID))
}

// ReadHistory returns an ordered page without loading the entire history.
func (s *Store) ReadHistory(ctx context.Context, key durable.Key, after int64, limit int) ([]durable.Event, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if after < 0 || limit < 1 || limit > 1000 {
		return nil, durable.ErrInvalid
	}
	if _, err := s.GetExecution(ctx, key); err != nil {
		return nil, err
	}
	rows, err := s.pgdb.Query(ctx, `SELECT sequence, type, payload, occurred_at
        FROM dispatch_execution_events WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3
        AND sequence > $4 ORDER BY sequence LIMIT $5`, key.Namespace, key.WorkflowID, key.RunID, after, limit)
	if err != nil {
		return nil, fmt.Errorf(errPrefix+"read execution history: %w", err)
	}
	defer rows.Close()
	events := make([]durable.Event, 0, limit)
	for rows.Next() {
		var e durable.Event
		if scanErr := rows.Scan(&e.Sequence, &e.Type, &e.Payload, &e.Time); scanErr != nil {
			return nil, fmt.Errorf(errPrefix+"scan execution history: %w", scanErr)
		}
		events = append(events, e)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf(errPrefix+"execution history rows: %w", err)
	}
	return events, nil
}

const taskColumns = `t.namespace, t.workflow_id, t.run_id, t.task_id, t.kind, t.queue,
    t.payload, t.available_at, t.owner, t.epoch, t.attempt, t.lease_until`

func scanTask(row driver.Row) (*durable.Task, error) {
	var task durable.Task
	var until sql.NullTime
	err := row.Scan(&task.Namespace, &task.WorkflowID, &task.RunID, &task.ID, &task.Kind,
		&task.Queue, &task.Payload, &task.AvailableAt, &task.Owner, &task.Epoch, &task.Attempt, &until)
	if err != nil {
		return nil, err
	}
	task.LeaseUntil = until.Time
	return &task, nil
}

// ClaimTask atomically claims one eligible task, skipping other pollers' locks.
func (s *Store) ClaimTask(ctx context.Context, r durable.ClaimRequest) (*durable.Task, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	task, err := scanTask(s.pgdb.QueryRow(ctx, `WITH candidate AS (
        SELECT t.namespace, t.workflow_id, t.run_id, t.task_id
        FROM dispatch_execution_tasks t JOIN dispatch_executions e
          USING (namespace, workflow_id, run_id)
        WHERE t.namespace=$1 AND t.queue=$2 AND t.kind=$3 AND NOT t.done
          AND e.state='running' AND t.available_at <= clock_timestamp()
          AND (t.lease_until IS NULL OR t.lease_until <= clock_timestamp())
        ORDER BY t.available_at, t.workflow_id, t.run_id, t.task_id
        FOR UPDATE OF t SKIP LOCKED LIMIT 1
    ) UPDATE dispatch_execution_tasks t SET owner=$4, epoch=t.epoch+1,
        attempt=t.attempt+1, lease_until=clock_timestamp()+($5 * interval '1 microsecond')
      FROM candidate c WHERE t.namespace=c.namespace AND t.workflow_id=c.workflow_id
        AND t.run_id=c.run_id AND t.task_id=c.task_id
      RETURNING `+taskColumns, r.Namespace, r.Queue, string(r.Kind), r.Owner, r.LeaseDuration.Microseconds()))
	if isNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf(errPrefix+"claim execution task: %w", err)
	}
	return task, nil
}

// RenewTask extends a live grant without shortening its current deadline.
func (s *Store) RenewTask(ctx context.Context, key durable.Key, token durable.TaskToken, ttl time.Duration) (time.Time, error) {
	if err := key.Validate(); err != nil {
		return time.Time{}, err
	}
	if err := token.Validate(); err != nil {
		return time.Time{}, err
	}
	if err := durable.ValidateLease(ttl); err != nil {
		return time.Time{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, err
	}
	defer s.rollbackExecution(tx)
	// Match the completion lock order. An UPDATE predicate can be evaluated
	// before a row-lock wait; expiry must be checked after both locks are held.
	execution, err := scanExecution(tx.QueryRow(ctx, `SELECT `+executionColumns+`
        FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`,
		key.Namespace, key.WorkflowID, key.RunID))
	if err != nil {
		return time.Time{}, err
	}
	if execution.State != durable.StateRunning {
		return time.Time{}, durable.ErrLeaseLost
	}
	task, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM dispatch_execution_tasks t
        WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id=$4 AND NOT done FOR UPDATE`,
		key.Namespace, key.WorkflowID, key.RunID, token.TaskID))
	if isNoRows(err) {
		return time.Time{}, durable.ErrLeaseLost
	}
	if err != nil {
		return time.Time{}, err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return time.Time{}, err
	}
	if leaseErr := durable.CheckLease(*task, token, now); leaseErr != nil {
		return time.Time{}, leaseErr
	}
	until := durable.Timestamp(now.Add(ttl))
	if task.LeaseUntil.After(until) {
		until = task.LeaseUntil
	}
	_, err = tx.Exec(ctx, `UPDATE dispatch_execution_tasks SET lease_until=$5
        WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id=$4`,
		key.Namespace, key.WorkflowID, key.RunID, token.TaskID, until)
	if err != nil {
		return time.Time{}, err
	}
	if err = tx.Commit(); err != nil {
		return time.Time{}, fmt.Errorf(errPrefix+"renew execution task: %w", err)
	}
	return until, nil
}

func (s *Store) rollbackExecution(tx driver.Tx) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, pgx.ErrTxClosed) && !errors.Is(err, sql.ErrTxDone) {
		s.logger.Warn("execution transaction rollback failed", log.String("error", err.Error()))
	}
}

func executionTime(ctx context.Context, tx driver.Tx) (time.Time, error) {
	var now time.Time
	err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now, err
}

func executionBytes(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}
