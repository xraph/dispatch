package memory

import (
	"context"
	"math"
	"time"

	"github.com/xraph/dispatch/durable"
)

// ClaimExecutionTimeout grants an expired run independently of its tasks/build.
func (m *Store) ClaimExecutionTimeout(ctx context.Context, r durable.ExecutionTimeoutClaimRequest) (*durable.ExecutionTimeoutTask, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := durable.Timestamp(time.Now())
	var selected *executionRecord
	for key, record := range m.executions {
		if key.Namespace != r.Namespace || record.execution.State != durable.StateRunning || durable.CheckExecutionDeadline(record.execution, now) == nil || record.timeout.LeaseUntil.After(now) {
			continue
		}
		_, deadline := record.execution.Deadline()
		if selected == nil {
			selected = record
			continue
		}
		_, prior := selected.execution.Deadline()
		if deadline.Before(prior) {
			selected = record
		}
	}
	if selected == nil {
		return nil, nil
	}
	prior := selected.timeout
	if prior.Epoch == math.MaxInt64 || prior.Attempt == math.MaxInt64 {
		return nil, durable.ErrInvalid
	}
	kind, deadline := selected.execution.Deadline()
	selected.timeout = durable.ExecutionTimeoutTask{Key: selected.execution.Key, Kind: kind, DeadlineAt: deadline, Owner: r.Owner, Epoch: prior.Epoch + 1, Attempt: prior.Attempt + 1, LeaseUntil: durable.Timestamp(now.Add(r.LeaseDuration))}
	result := selected.timeout
	return &result, nil
}

// ApplyExecutionTimeout stages every fallible change before publishing closure.
func (m *Store) ApplyExecutionTimeout(ctx context.Context, r durable.ExecutionTimeoutRequest) (durable.Receipt, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	digest, err := durable.Fingerprint("execution-timeout", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if contextErr := ctx.Err(); contextErr != nil {
		return durable.Receipt{}, contextErr
	}
	record, ok := m.executions[r.Key]
	if !ok {
		return durable.Receipt{}, durable.ErrNotFound
	}
	if receipt, found := record.receipts[r.RequestID]; found {
		return replayReceipt(receipt, digest)
	}
	if record.execution.State != durable.StateRunning {
		return durable.Receipt{}, durable.ErrClosed
	}
	now := durable.Timestamp(time.Now())
	if leaseErr := durable.CheckExecutionTimeoutLease(record.timeout, r, now); leaseErr != nil {
		return durable.Receipt{}, leaseErr
	}
	next, event, receipt, err := durable.PrepareExecutionTimeout(record.execution, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	deliveries, err := m.prepareChildDeliveries(next, durable.CommitRequest{Key: r.Key, Events: []durable.EventInput{event.EventInput}}, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	changes := make(map[string]durable.Task)
	for id, task := range record.tasks {
		if task.Done {
			continue
		}
		cancelled, changeErr := durable.UpdateTask(task.Task, nil, now)
		if changeErr != nil {
			return durable.Receipt{}, changeErr
		}
		changes[id] = cancelled
	}
	for id, task := range changes {
		record.tasks[id] = &durableTask{Task: task}
	}
	record.timeout.Owner = ""
	record.timeout.LeaseUntil = time.Time{}
	record.execution = next
	record.history = append(record.history, event)
	record.receipts[r.RequestID] = durableReceipt{digest: digest, value: receipt}
	m.saveChildDeliveries(deliveries)
	return receipt, nil
}
