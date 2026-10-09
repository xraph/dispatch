package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/dispatch/durable"
)

// RecordHeartbeat commits task progress and its immutable request receipt.
func (s *Store) RecordHeartbeat(ctx context.Context, r durable.HeartbeatRequest) (result durable.Receipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	digest, err := durable.Fingerprint("heartbeat", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.Receipt{}, err
	}
	defer s.rollbackExecution(tx)
	execution, err := scanExecution(tx.QueryRow(ctx, `SELECT `+executionColumns+`
        FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`, r.Namespace, r.WorkflowID, r.RunID))
	if err != nil {
		return durable.Receipt{}, err
	}
	prior, found, err := readExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest)
	if err != nil || found {
		return prior, err
	}
	if execution.State != durable.StateRunning {
		return durable.Receipt{}, durable.ErrClosed
	}
	task, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM dispatch_execution_tasks t
        WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id=$4 FOR UPDATE`, r.Namespace, r.WorkflowID, r.RunID, r.Token.TaskID))
	if isNoRows(err) {
		return durable.Receipt{}, durable.ErrLeaseLost
	}
	if err != nil {
		return durable.Receipt{}, err
	}
	// Row-lock waits can expire the lease or heartbeat deadline.
	now, err := executionTime(ctx, tx)
	if err != nil {
		return durable.Receipt{}, err
	}
	if deadlineErr := durable.CheckExecutionDeadline(execution, now); deadlineErr != nil {
		return durable.Receipt{}, deadlineErr
	}
	next, err := durable.ApplyHeartbeat(*task, r, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	if saveErr := saveExecutionTaskState(ctx, tx, next); saveErr != nil {
		return durable.Receipt{}, saveErr
	}
	receipt := durable.Receipt{Revision: execution.Revision}
	if receiptErr := saveExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest, "", receipt); receiptErr != nil {
		return durable.Receipt{}, receiptErr
	}
	if err = tx.Commit(); err != nil {
		return durable.Receipt{}, fmt.Errorf(errPrefix+"commit heartbeat: %w", err)
	}
	return receipt, nil
}
