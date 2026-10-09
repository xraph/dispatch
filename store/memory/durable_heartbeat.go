package memory

import (
	"context"
	"time"

	"github.com/xraph/dispatch/durable"
)

// RecordHeartbeat persists progress and its receipt under the execution lock.
func (m *Store) recordHeartbeat(ctx context.Context, r durable.HeartbeatRequest) (durable.Receipt, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	digest, err := durable.Fingerprint("heartbeat", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if contextErr := ctx.Err(); contextErr != nil {
		return durable.Receipt{}, contextErr
	}
	record, exists := m.executions[r.Key]
	if !exists {
		return durable.Receipt{}, durable.ErrNotFound
	}
	if receipt, found := record.receipts[r.RequestID]; found {
		return replayReceipt(receipt, digest)
	}
	if record.execution.State != durable.StateRunning {
		return durable.Receipt{}, durable.ErrClosed
	}
	task, exists := record.tasks[r.Token.TaskID]
	if !exists {
		return durable.Receipt{}, durable.ErrLeaseLost
	}
	now := durable.Timestamp(time.Now())
	if deadlineErr := durable.CheckExecutionDeadline(record.execution, now); deadlineErr != nil {
		return durable.Receipt{}, deadlineErr
	}
	next, err := durable.ApplyHeartbeat(task.Task, r, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	receipt := durable.Receipt{Revision: record.execution.Revision}
	task.Task = next
	record.receipts[r.RequestID] = durableReceipt{action: "activity.heartbeat", digest: digest, value: receipt}
	return receipt, nil
}
