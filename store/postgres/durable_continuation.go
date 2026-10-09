package postgres

import (
	"context"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

func prepareContinuation(ctx context.Context, tx driver.Tx, current durable.Execution, task durable.Task, r durable.CommitRequest, now time.Time) (*durable.ContinuationBatch, error) {
	if r.Continuation == nil && (current.RetryPolicy == nil || r.State != durable.StateFailed) {
		return nil, nil
	}
	history, err := loadSuccessorHistory(ctx, tx, current.Key)
	if err != nil {
		return nil, err
	}
	return durable.PrepareTransitionSuccessor(current, task, r, history, now)
}

func loadSuccessorHistory(ctx context.Context, tx driver.Tx, key durable.Key) ([]durable.Event, error) {
	rows, err := tx.Query(ctx, `SELECT sequence,type,payload,occurred_at FROM dispatch_execution_events WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 ORDER BY sequence LIMIT 100001`, key.Namespace, key.WorkflowID, key.RunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var history []durable.Event
	for rows.Next() {
		var e durable.Event
		if scanErr := rows.Scan(&e.Sequence, &e.Type, &e.Payload, &e.Time); scanErr != nil {
			return nil, scanErr
		}
		history = append(history, e)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, rowsErr
	}
	return history, nil
}

func insertContinuation(ctx context.Context, tx driver.Tx, batch *durable.ContinuationBatch, spec *durable.ContinueSpec) error {
	if batch == nil || batch.RetrySuppressed {
		return nil
	}
	e := batch.Execution
	if spec == nil {
		spec = &batch.Spec
	}
	policy, err := encodeWorkflowRetryPolicy(e.RetryPolicy)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO dispatch_executions (`+executionColumns+`)
 VALUES($1,$2,$3,$4,$5,'running',1,$6,$7,$8,$9,$9,$10,$11,$12,$13,'',$14,$15,$16,$17,$18,$19)`, e.Namespace, e.WorkflowID, e.RunID, e.WorkflowType, e.BuildID, e.LastSequence, executionBytes(e.Input), []byte{}, e.CreatedAt, taskNullableTime(e.RunDeadlineAt), taskNullableTime(e.ExecutionDeadlineAt), e.FirstRunID, e.PreviousRunID, e.RunNumber, e.FirstStartedAt, int64(e.RunTimeout), policy, e.RetryAttempt, e.AvailableAt())
	if isDuplicateKey(err) {
		return durable.ErrExists
	}
	if err != nil {
		return err
	}
	for _, event := range batch.History {
		if eventErr := insertExecutionEvent(ctx, tx, e.Key, event); eventErr != nil {
			return eventErr
		}
	}
	return insertExecutionTask(ctx, tx, e.Key, durable.TaskSpec{ID: "workflow:1", Kind: durable.TaskWorkflow, Queue: spec.Queue, AvailableAt: e.AvailableAt()}, e.CreatedAt)
}
