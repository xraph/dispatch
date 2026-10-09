package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/driver"

	"github.com/xraph/dispatch/durable"
)

// RequestCancelExecution deduplicates before choosing the explicit or open run.
func (s *Store) RequestCancelExecution(ctx context.Context, r durable.CancelExecutionRequest) (result durable.CancelExecutionReceipt, resultErr error) {
	defer func() { resultErr = normalizeExecutionError(resultErr) }()
	if err := r.Validate(); err != nil {
		return durable.CancelExecutionReceipt{}, err
	}
	digest, err := durable.Fingerprint("cancel-execution", r)
	if err != nil {
		return durable.CancelExecutionReceipt{}, err
	}
	payload, err := json.Marshal(durable.ExecutionCancellation{Version: 1, RequestID: r.RequestID, Reason: r.Reason})
	if err != nil {
		return durable.CancelExecutionReceipt{}, err
	}
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return durable.CancelExecutionReceipt{}, err
	}
	defer s.rollbackExecution(tx)
	if lockErr := lockSignalWorkflow(ctx, tx, r.Namespace, r.WorkflowID); lockErr != nil {
		return durable.CancelExecutionReceipt{}, lockErr
	}
	prior, found, err := readCancellationReceipt(ctx, tx, r.Key, r.RequestID, digest)
	if err != nil || found {
		return prior, err
	}
	var current durable.Execution
	if r.RunID == "" {
		current, err = lockOpenSignalRun(ctx, tx, r.Namespace, r.WorkflowID)
	} else {
		current, err = scanExecution(tx.QueryRow(ctx, `SELECT `+executionColumns+` FROM dispatch_executions WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3 FOR UPDATE`, r.Namespace, r.WorkflowID, r.RunID))
	}
	if err != nil {
		return durable.CancelExecutionReceipt{}, err
	}
	accepted, err := appendWorkflowInput(ctx, tx, current, r.BuildID, payload, durable.EventCancellationRequested, "cancel-request")
	if err != nil {
		return durable.CancelExecutionReceipt{}, err
	}
	receipt := durable.CancelExecutionReceipt{Key: accepted.Key, Receipt: accepted.Receipt}
	if intentErr := prepareReceiptIntent(ctx, tx, accepted.Key, "cancellation_receipt", r.RequestID, "accepted", "execution.cancel"); intentErr != nil {
		return durable.CancelExecutionReceipt{}, intentErr
	}
	_, err = tx.Exec(ctx, `INSERT INTO dispatch_cancellation_receipts(namespace,workflow_id,request_id,run_id,digest,revision,first_sequence,last_sequence)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, receipt.Namespace, receipt.WorkflowID, r.RequestID, receipt.RunID, digest, receipt.Revision, receipt.FirstSequence, receipt.LastSequence)
	if err != nil {
		return durable.CancelExecutionReceipt{}, err
	}
	if err = tx.Commit(); err != nil {
		return durable.CancelExecutionReceipt{}, fmt.Errorf(errPrefix+"commit cancellation acceptance: %w", err)
	}
	return receipt, nil
}

func readCancellationReceipt(ctx context.Context, tx driver.Tx, key durable.Key, requestID, digest string) (durable.CancelExecutionReceipt, bool, error) {
	result := durable.CancelExecutionReceipt{Key: durable.Key{Namespace: key.Namespace, WorkflowID: key.WorkflowID}}
	var stored string
	err := tx.QueryRow(ctx, `SELECT run_id,digest,revision,first_sequence,last_sequence FROM dispatch_cancellation_receipts WHERE namespace=$1 AND workflow_id=$2 AND request_id=$3`, key.Namespace, key.WorkflowID, requestID).Scan(&result.RunID, &stored, &result.Revision, &result.FirstSequence, &result.LastSequence)
	if isNoRows(err) {
		return durable.CancelExecutionReceipt{}, false, nil
	}
	if err != nil {
		return durable.CancelExecutionReceipt{}, false, err
	}
	if stored != digest {
		return durable.CancelExecutionReceipt{}, true, durable.ErrRequestConflict
	}
	return result, true, nil
}
