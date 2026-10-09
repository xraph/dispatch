package memory

import (
	"context"
	"encoding/json"
	"time"

	"github.com/xraph/dispatch/durable"
)

type cancellationReceiptRecord struct {
	digest  string
	receipt durable.CancelExecutionReceipt
}

// RequestCancelExecution accepts cancellation and its wakeup atomically.
func (m *Store) requestCancelExecution(ctx context.Context, r durable.CancelExecutionRequest) (durable.CancelExecutionReceipt, error) {
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if contextErr := ctx.Err(); contextErr != nil {
		return durable.CancelExecutionReceipt{}, contextErr
	}
	id := signalReceiptKey{r.Namespace, r.WorkflowID, r.RequestID}
	if prior, found := m.cancellationReceipts[id]; found {
		if prior.digest != digest {
			return durable.CancelExecutionReceipt{}, durable.ErrRequestConflict
		}
		return prior.receipt, nil
	}
	var record *executionRecord
	if r.RunID != "" {
		record = m.executions[r.Key]
	} else {
		record = m.openSignalRun(r.Namespace, r.WorkflowID)
	}
	if record == nil {
		return durable.CancelExecutionReceipt{}, durable.ErrNotFound
	}
	accepted, err := m.appendWorkflowInput(record, r.BuildID, payload, durable.Timestamp(time.Now()), durable.EventCancellationRequested, "cancel-request")
	if err != nil {
		return durable.CancelExecutionReceipt{}, err
	}
	receipt := durable.CancelExecutionReceipt{Key: accepted.Key, Receipt: accepted.Receipt}
	m.cancellationReceipts[id] = cancellationReceiptRecord{digest: digest, receipt: receipt}
	return receipt, nil
}
