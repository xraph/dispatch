package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

func readExecutionReceipt(ctx context.Context, tx driver.Tx, key durable.Key, requestID, digest string) (durable.Receipt, bool, error) {
	var receipt durable.Receipt
	var storedDigest string
	err := tx.QueryRow(ctx, `SELECT digest, revision, first_sequence, last_sequence
        FROM dispatch_execution_receipts WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND request_id=$4`,
		key.Namespace, key.WorkflowID, key.RunID, requestID).
		Scan(&storedDigest, &receipt.Revision, &receipt.FirstSequence, &receipt.LastSequence)
	if isNoRows(err) {
		return durable.Receipt{}, false, nil
	}
	if err != nil {
		return durable.Receipt{}, false, err
	}
	if storedDigest != digest {
		return durable.Receipt{}, true, durable.ErrRequestConflict
	}
	return receipt, true, nil
}

func saveExecutionReceipt(ctx context.Context, tx driver.Tx, key durable.Key, requestID, digest, intent string, receipt durable.Receipt) error {
	_, err := tx.Exec(ctx, `INSERT INTO dispatch_execution_receipts
        (namespace, workflow_id, run_id, request_id, digest, revision, first_sequence, last_sequence, intent_digest)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, key.Namespace, key.WorkflowID, key.RunID, requestID, digest,
		receipt.Revision, receipt.FirstSequence, receipt.LastSequence, intent)
	return err
}

func insertExecutionEvent(ctx context.Context, tx driver.Tx, key durable.Key, event durable.Event) error {
	_, err := tx.Exec(ctx, `INSERT INTO dispatch_execution_events
        (namespace, workflow_id, run_id, sequence, type, payload, occurred_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7)`, key.Namespace, key.WorkflowID, key.RunID,
		event.Sequence, event.Type, executionBytes(event.Payload), event.Time)
	return err
}

func insertExecutionTask(ctx context.Context, tx driver.Tx, key durable.Key, spec durable.TaskSpec, now time.Time) error {
	task, err := durable.NewTask(key, spec, now)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO dispatch_execution_tasks
        (namespace, workflow_id, run_id, task_id, kind, queue, payload, available_at, version, deadline_at, progress)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, key.Namespace, key.WorkflowID, key.RunID,
		task.ID, string(task.Kind), task.Queue, executionBytes(task.Payload), task.AvailableAt,
		task.Version, taskNullableTime(task.DeadlineAt), executionBytes(task.Progress))
	if isDuplicateKey(err) {
		return durable.ErrExists
	}
	return err
}

// StartExecution persists a run, its first event, task and receipt together.
func (s *Store) StartExecution(ctx context.Context, r durable.StartRequest) (durable.Receipt, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	digest, err := durable.Fingerprint("start", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.Receipt{}, err
	}
	defer s.rollbackExecution(tx)
	if lockErr := lockSignalWorkflow(ctx, tx, r.Namespace, r.WorkflowID); lockErr != nil {
		return durable.Receipt{}, lockErr
	}
	now, err := executionTime(ctx, tx)
	if err != nil {
		return durable.Receipt{}, err
	}
	created, err := insertStartedExecution(ctx, tx, r, digest, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	if !created {
		receipt, found, readErr := readExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest)
		if readErr != nil || found {
			return receipt, readErr
		}
		return durable.Receipt{}, durable.ErrExists
	}
	if err = tx.Commit(); err != nil {
		return durable.Receipt{}, fmt.Errorf(errPrefix+"commit execution start: %w", err)
	}
	return durable.Receipt{Revision: 1, FirstSequence: 1, LastSequence: 1}, nil
}

// CommitTransition serializes a run's mutations and fences task completion.
func (s *Store) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	digest, err := durable.Fingerprint("commit", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.Receipt{}, err
	}
	defer s.rollbackExecution(tx)
	// A committed child decision is independent of later ownership of its child
	// identities. Recover it before waiting on those identities again.
	if len(r.Children) != 0 {
		accepted, exists, receiptErr := readExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest)
		if receiptErr != nil || exists {
			return accepted, receiptErr
		}
	}
	if lockErr := lockChildIdentities(ctx, tx, r.Children); lockErr != nil {
		return durable.Receipt{}, lockErr
	}
	current, err := scanExecution(tx.QueryRow(ctx, `SELECT `+executionColumns+`
        FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`,
		r.Namespace, r.WorkflowID, r.RunID))
	if err != nil {
		return durable.Receipt{}, err
	}
	prior, found, err := readExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest)
	if err != nil || found {
		return prior, err
	}
	if current.State != durable.StateRunning {
		return durable.Receipt{}, durable.ErrClosed
	}
	task, err := scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM dispatch_execution_tasks t
        WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id=$4 AND NOT done FOR UPDATE`,
		r.Namespace, r.WorkflowID, r.RunID, r.Token.TaskID))
	if isNoRows(err) {
		return durable.Receipt{}, durable.ErrLeaseLost
	}
	if err != nil {
		return durable.Receipt{}, err
	}
	conditions, err := lockTaskConditions(ctx, tx, r)
	if err != nil {
		return durable.Receipt{}, err
	}
	if r.CancelPendingTasks || r.State != "" && r.State != durable.StateRunning {
		// Closure cancels all pending tasks, even those without explicit
		// conditions. Acquire their locks before validating the source deadline.
		_, err = tx.Exec(ctx, `SELECT 1 FROM dispatch_execution_tasks
            WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND NOT done FOR UPDATE`,
			r.Namespace, r.WorkflowID, r.RunID)
		if err != nil {
			return durable.Receipt{}, err
		}
	}
	// Read time after acquiring every task lock. A lock wait can expire the
	// source grant or make a target deadline eligible for cancellation.
	now, err := executionTime(ctx, tx)
	if err != nil {
		return durable.Receipt{}, err
	}
	if sourceErr := durable.ValidateChildSource(*task, r.Children); sourceErr != nil {
		return durable.Receipt{}, sourceErr
	}
	next, receipt, err := durable.Advance(current, *task, r, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	for _, condition := range r.Conditions {
		target, exists := conditions[condition.TaskID]
		if !exists || durable.CheckTaskCondition(target, condition, now) != nil {
			return durable.Receipt{}, durable.ErrTaskConflict
		}
	}
	updated, err := durable.UpdateTask(*task, r.TaskUpdate, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	if childErr := insertChildExecutions(ctx, tx, r.Key, r.Children, now); childErr != nil {
		return durable.Receipt{}, childErr
	}
	childEvents, eventErr := durable.ChildStartEvents(r.Children)
	if eventErr != nil {
		return durable.Receipt{}, eventErr
	}
	events := append(append([]durable.EventInput(nil), r.Events...), childEvents...)
	for i, input := range events {
		if eventErr := insertExecutionEvent(ctx, tx, r.Key, durable.Event{
			EventInput: input, Sequence: receipt.FirstSequence + int64(i), Time: now,
		}); eventErr != nil {
			return durable.Receipt{}, eventErr
		}
	}
	_, err = tx.Exec(ctx, `UPDATE dispatch_executions SET state=$4, revision=$5,
        last_sequence=$6, output=$7, updated_at=$8 WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`,
		r.Namespace, r.WorkflowID, r.RunID, string(next.State), next.Revision, next.LastSequence, executionBytes(next.Output), next.UpdatedAt)
	if err != nil {
		return durable.Receipt{}, err
	}
	if r.CancelPendingTasks {
		_, err = tx.Exec(ctx, `UPDATE dispatch_execution_tasks SET done=TRUE, version=version+1
 WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND task_id<>$4 AND NOT done`, r.Namespace, r.WorkflowID, r.RunID, r.Token.TaskID)
		if err != nil {
			return durable.Receipt{}, err
		}
	}
	for _, spec := range r.Tasks {
		if taskErr := insertExecutionTask(ctx, tx, r.Key, spec, now); taskErr != nil {
			return durable.Receipt{}, taskErr
		}
	}
	if taskErr := saveExecutionTaskState(ctx, tx, updated); taskErr != nil {
		return durable.Receipt{}, taskErr
	}
	for _, taskID := range r.CancelTasks {
		cancelled, cancelErr := durable.UpdateTask(conditions[taskID], nil, now)
		if cancelErr != nil {
			return durable.Receipt{}, cancelErr
		}
		if saveErr := saveExecutionTaskState(ctx, tx, cancelled); saveErr != nil {
			return durable.Receipt{}, saveErr
		}
	}
	if next.State != durable.StateRunning {
		_, err = tx.Exec(ctx, `UPDATE dispatch_execution_tasks SET done=TRUE, version=version+1
            WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 AND NOT done`, r.Namespace, r.WorkflowID, r.RunID)
		if err != nil {
			return durable.Receipt{}, err
		}
	}
	if receiptErr := saveExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest, r.IntentDigest, receipt); receiptErr != nil {
		return durable.Receipt{}, receiptErr
	}
	if err = tx.Commit(); err != nil {
		return durable.Receipt{}, fmt.Errorf(errPrefix+"commit execution transition: %w", err)
	}
	return receipt, nil
}
