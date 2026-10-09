package postgres

import (
	"context"
	"time"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

// insertStartedExecution requires the workflow identity lock and creates all
// initial rows. A conflict never adopts an existing run.
func insertStartedExecution(ctx context.Context, tx driver.Tx, r durable.StartRequest, digest string, now time.Time) (bool, error) {
	execution, err := durable.NewExecution(r, now)
	if err != nil {
		return false, err
	}
	policy, err := encodeWorkflowRetryPolicy(execution.RetryPolicy)
	if err != nil {
		return false, err
	}
	result, err := tx.Exec(ctx, `INSERT INTO dispatch_executions (`+executionColumns+`)
 VALUES($1,$2,$3,$4,$5,'running',1,1,$6,$7,$8,$8,$9,$10,$3,'','',1,$8,$11,$12,1,$8) ON CONFLICT DO NOTHING`, r.Namespace, r.WorkflowID, r.RunID, r.WorkflowType, r.BuildID, executionBytes(r.Input), []byte{}, now, taskNullableTime(execution.RunDeadlineAt), taskNullableTime(execution.ExecutionDeadlineAt), int64(execution.RunTimeout), policy)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil || count == 0 {
		return false, err
	}
	if eventErr := insertExecutionEvent(ctx, tx, r.Key, durable.Event{EventInput: durable.EventInput{Type: "execution.started", Payload: r.Input}, Sequence: 1, Time: now}); eventErr != nil {
		return false, eventErr
	}
	if taskErr := insertExecutionTask(ctx, tx, r.Key, durable.TaskSpec{ID: "workflow:1", Kind: durable.TaskWorkflow, Queue: r.Queue}, now); taskErr != nil {
		return false, taskErr
	}
	receipt := durable.Receipt{Revision: 1, FirstSequence: 1, LastSequence: 1}
	if receiptErr := saveExecutionReceipt(ctx, tx, r.Key, r.RequestID, digest, "", "execution.start", receipt); receiptErr != nil {
		return false, receiptErr
	}
	return true, nil
}
