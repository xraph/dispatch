package postgres

import (
	"context"
	"database/sql"
	"math"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

const executionTimeoutColumns = `namespace,workflow_id,run_id,
 CASE WHEN run_deadline_at IS NOT NULL AND (execution_deadline_at IS NULL OR run_deadline_at<execution_deadline_at) THEN 'run' ELSE 'execution' END,
 LEAST(run_deadline_at,execution_deadline_at),timeout_owner,timeout_epoch,timeout_attempt,timeout_lease_until`

func scanExecutionTimeout(row driver.Row) (*durable.ExecutionTimeoutTask, error) {
	var task durable.ExecutionTimeoutTask
	var deadline, lease sql.NullTime
	err := row.Scan(&task.Namespace, &task.WorkflowID, &task.RunID, &task.Kind, &deadline, &task.Owner, &task.Epoch, &task.Attempt, &lease)
	if err != nil {
		return nil, err
	}
	task.DeadlineAt, task.LeaseUntil = deadline.Time, lease.Time
	return &task, nil
}

// ClaimExecutionTimeout skips owned rows and grants expiry across workflow builds.
func (s *Store) ClaimExecutionTimeout(ctx context.Context, r durable.ExecutionTimeoutClaimRequest) (result *durable.ExecutionTimeoutTask, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return nil, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer s.rollbackExecution(tx)
	grant, err := scanExecutionTimeout(tx.QueryRow(ctx, `SELECT `+executionTimeoutColumns+` FROM dispatch_executions
 WHERE namespace=$1 AND state='running' AND LEAST(run_deadline_at,execution_deadline_at)<=clock_timestamp()
 AND (timeout_lease_until IS NULL OR timeout_lease_until<=clock_timestamp())
 ORDER BY LEAST(run_deadline_at,execution_deadline_at),workflow_id,run_id FOR UPDATE SKIP LOCKED LIMIT 1`, r.Namespace))
	if isNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	if grant.DeadlineAt.IsZero() || grant.DeadlineAt.After(now) || grant.LeaseUntil.After(now) {
		return nil, nil
	}
	if grant.Epoch == math.MaxInt64 || grant.Attempt == math.MaxInt64 {
		return nil, durable.ErrInvalid
	}
	grant.Owner = r.Owner
	grant.Epoch++
	grant.Attempt++
	grant.LeaseUntil = durable.Timestamp(now.Add(r.LeaseDuration))
	_, err = tx.Exec(ctx, `UPDATE dispatch_executions SET timeout_owner=$4,timeout_epoch=$5,timeout_attempt=$6,timeout_lease_until=$7
 WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, grant.Namespace, grant.WorkflowID, grant.RunID, grant.Owner, grant.Epoch, grant.Attempt, grant.LeaseUntil)
	if err != nil {
		return nil, err
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return nil, commitErr
	}
	return grant, nil
}

// ApplyExecutionTimeout records closure and all lifecycle consequences atomically.
func (s *Store) ApplyExecutionTimeout(ctx context.Context, r durable.ExecutionTimeoutRequest) (result durable.Receipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	digest, err := durable.Fingerprint("execution-timeout", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.Receipt{}, err
	}
	defer s.rollbackExecution(tx)
	if receipt, found, readErr := readExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest); readErr != nil || found {
		return receipt, readErr
	}
	if lockErr := lockSignalWorkflow(ctx, tx, r.Namespace, r.WorkflowID); lockErr != nil {
		return durable.Receipt{}, lockErr
	}
	current, err := scanExecution(tx.QueryRow(ctx, `SELECT `+executionColumns+` FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`, r.Namespace, r.WorkflowID, r.RunID))
	if err != nil {
		return durable.Receipt{}, err
	}
	if receipt, found, readErr := readExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest); readErr != nil || found {
		return receipt, readErr
	}
	if current.State != durable.StateRunning {
		return durable.Receipt{}, durable.ErrClosed
	}
	grant, err := scanExecutionTimeout(tx.QueryRow(ctx, `SELECT `+executionTimeoutColumns+` FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, r.Namespace, r.WorkflowID, r.RunID))
	if err != nil {
		return durable.Receipt{}, err
	}
	_, err = tx.Exec(ctx, `SELECT 1 FROM dispatch_execution_tasks WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND NOT done FOR UPDATE`, r.Namespace, r.WorkflowID, r.RunID)
	if err != nil {
		return durable.Receipt{}, err
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return durable.Receipt{}, err
	}
	if leaseErr := durable.CheckExecutionTimeoutLease(*grant, r, now); leaseErr != nil {
		return durable.Receipt{}, leaseErr
	}
	next, event, receipt, err := durable.PrepareExecutionTimeout(current, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	var batch *durable.ContinuationBatch
	if current.RetryPolicy != nil {
		history, readErr := loadSuccessorHistory(ctx, tx, current.Key)
		if readErr != nil {
			return durable.Receipt{}, readErr
		}
		var queue string
		if queueErr := tx.QueryRow(ctx, `SELECT queue FROM dispatch_execution_tasks WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id='workflow:1'`, r.Namespace, r.WorkflowID, r.RunID).Scan(&queue); queueErr != nil {
			return durable.Receipt{}, queueErr
		}
		batch, err = durable.PrepareWorkflowRetry(current, durable.StateTimedOut, []durable.EventInput{event.EventInput}, history, queue, now)
		if err != nil {
			return durable.Receipt{}, err
		}
		if batch != nil {
			next.NextRunID = batch.Execution.RunID
		}
	}
	inputs, err := durable.AddWorkflowRetryEvent(&next, &receipt, []durable.EventInput{event.EventInput}, batch)
	if err != nil {
		return durable.Receipt{}, err
	}
	deliveries, err := prepareChildDeliveries(ctx, tx, next, durable.CommitRequest{Key: r.Key, Events: []durable.EventInput{event.EventInput}}, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	for i, input := range inputs {
		if eventErr := insertExecutionEvent(ctx, tx, r.Key, durable.Event{EventInput: input, Sequence: receipt.FirstSequence + int64(i), Time: now}); eventErr != nil {
			return durable.Receipt{}, eventErr
		}
	}
	_, err = tx.Exec(ctx, `UPDATE dispatch_executions SET state='timed_out',timeout_owner='',timeout_lease_until=NULL,revision=$4,last_sequence=$5,output=$6,updated_at=$7,next_run_id=$8 WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, r.Namespace, r.WorkflowID, r.RunID, next.Revision, next.LastSequence, []byte{}, now, next.NextRunID)
	if err != nil {
		return durable.Receipt{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE dispatch_execution_tasks SET done=TRUE,version=version+1 WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND NOT done`, r.Namespace, r.WorkflowID, r.RunID)
	if err != nil {
		return durable.Receipt{}, err
	}
	if deliveryErr := insertChildDeliveries(ctx, tx, deliveries); deliveryErr != nil {
		return durable.Receipt{}, deliveryErr
	}
	if receiptErr := saveExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest, "", "execution.timeout", receipt); receiptErr != nil {
		return durable.Receipt{}, receiptErr
	}
	if successorErr := insertContinuation(ctx, tx, batch, nil); successorErr != nil {
		return durable.Receipt{}, successorErr
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return durable.Receipt{}, commitErr
	}
	return receipt, nil
}
