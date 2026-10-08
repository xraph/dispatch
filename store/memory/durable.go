package memory

import (
	"context"
	"time"

	"github.com/xraph/dispatch/durable"
)

var _ durable.Store = (*Store)(nil)

type executionRecord struct {
	execution durable.Execution
	history   []durable.Event
	tasks     map[string]*durableTask
	receipts  map[string]durableReceipt
}

type durableTask struct {
	durable.Task
	done bool
}

type durableReceipt struct {
	digest string
	value  durable.Receipt
}

// StartExecution creates history and its initial task under one mutation lock.
func (m *Store) StartExecution(ctx context.Context, r durable.StartRequest) (durable.Receipt, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	digest, err := durable.Fingerprint("start", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return durable.Receipt{}, ctxErr
	}
	if record, exists := m.executions[r.Key]; exists {
		if receipt, found := record.receipts[r.RequestID]; found {
			return replayReceipt(receipt, digest)
		}
		return durable.Receipt{}, durable.ErrExists
	}
	for key, record := range m.executions {
		if key.Namespace == r.Namespace && key.WorkflowID == r.WorkflowID && record.execution.State == durable.StateRunning {
			return durable.Receipt{}, durable.ErrExists
		}
	}
	now := durable.Timestamp(time.Now())
	receipt := durable.Receipt{Revision: 1, FirstSequence: 1, LastSequence: 1}
	m.executions[r.Key] = &executionRecord{
		execution: durable.Execution{Key: r.Key, WorkflowType: r.WorkflowType, BuildID: r.BuildID,
			State: durable.StateRunning, Revision: 1, LastSequence: 1, Input: cloneBytes(r.Input), CreatedAt: now, UpdatedAt: now},
		history: []durable.Event{{EventInput: durable.EventInput{Type: "execution.started", Payload: cloneBytes(r.Input)}, Sequence: 1, Time: now}},
		tasks: map[string]*durableTask{"workflow:1": {Task: durable.Task{Key: r.Key,
			TaskSpec: durable.TaskSpec{ID: "workflow:1", Kind: durable.TaskWorkflow, Queue: r.Queue, AvailableAt: now}}}},
		receipts: map[string]durableReceipt{r.RequestID: {digest: digest, value: receipt}},
	}
	return receipt, nil
}

func replayReceipt(receipt durableReceipt, digest string) (durable.Receipt, error) {
	if receipt.digest != digest {
		return durable.Receipt{}, durable.ErrRequestConflict
	}
	return receipt.value, nil
}

// GetExecution returns a copy of the namespace-qualified execution.
func (m *Store) GetExecution(ctx context.Context, key durable.Key) (durable.Execution, error) {
	if err := key.Validate(); err != nil {
		return durable.Execution{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.Execution{}, err
	}
	record, ok := m.executions[key]
	if !ok {
		return durable.Execution{}, durable.ErrNotFound
	}
	result := record.execution
	result.Input, result.Output = cloneBytes(result.Input), cloneBytes(result.Output)
	return result, nil
}

// ReadHistory reads a bounded page after an exclusive sequence cursor.
func (m *Store) ReadHistory(ctx context.Context, key durable.Key, after int64, limit int) ([]durable.Event, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if after < 0 || limit < 1 || limit > 1000 {
		return nil, durable.ErrInvalid
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	record, ok := m.executions[key]
	if !ok {
		return nil, durable.ErrNotFound
	}
	result := make([]durable.Event, 0, limit)
	for _, evt := range record.history {
		if evt.Sequence > after {
			evt.Payload = cloneBytes(evt.Payload)
			result = append(result, evt)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

// ClaimTask grants or reclaims one eligible task using the store's clock.
func (m *Store) ClaimTask(ctx context.Context, r durable.ClaimRequest) (*durable.Task, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := durable.Timestamp(time.Now())
	var selected *durableTask
	for key, record := range m.executions {
		if key.Namespace != r.Namespace || record.execution.State != durable.StateRunning ||
			(r.BuildID != "" && record.execution.BuildID != r.BuildID) {
			continue
		}
		for _, task := range record.tasks {
			if task.done || task.Queue != r.Queue || task.Kind != r.Kind || task.AvailableAt.After(now) || task.LeaseUntil.After(now) {
				continue
			}
			if selected == nil || task.AvailableAt.Before(selected.AvailableAt) {
				selected = task
			}
		}
	}
	if selected == nil {
		return nil, nil
	}
	selected.Owner, selected.LeaseUntil = r.Owner, durable.Timestamp(now.Add(r.LeaseDuration))
	selected.Epoch++
	selected.Attempt++
	result := selected.Task
	result.Payload = cloneBytes(result.Payload)
	return &result, nil
}

// RenewTask extends only a current, unexpired task lease.
func (m *Store) RenewTask(ctx context.Context, key durable.Key, token durable.TaskToken, ttl time.Duration) (time.Time, error) {
	if err := key.Validate(); err != nil {
		return time.Time{}, err
	}
	if err := token.Validate(); err != nil {
		return time.Time{}, err
	}
	if err := durable.ValidateLease(ttl); err != nil {
		return time.Time{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	record, ok := m.executions[key]
	if !ok {
		return time.Time{}, durable.ErrNotFound
	}
	task, ok := record.tasks[token.TaskID]
	now := durable.Timestamp(time.Now())
	if !ok || task.done || record.execution.State != durable.StateRunning || durable.CheckLease(task.Task, token, now) != nil {
		return time.Time{}, durable.ErrLeaseLost
	}
	until := durable.Timestamp(now.Add(ttl))
	if until.After(task.LeaseUntil) {
		task.LeaseUntil = until
	}
	return task.LeaseUntil, nil
}

// CommitTransition applies all state changes after validating the whole batch.
func (m *Store) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	digest, err := durable.Fingerprint("commit", r)
	if err != nil {
		return durable.Receipt{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return durable.Receipt{}, ctxErr
	}
	record, ok := m.executions[r.Key]
	if !ok {
		return durable.Receipt{}, durable.ErrNotFound
	}
	if receipt, exists := record.receipts[r.RequestID]; exists {
		return replayReceipt(receipt, digest)
	}
	if record.execution.State != durable.StateRunning {
		return durable.Receipt{}, durable.ErrClosed
	}
	task, ok := record.tasks[r.Token.TaskID]
	if !ok || task.done {
		return durable.Receipt{}, durable.ErrLeaseLost
	}
	now := durable.Timestamp(time.Now())
	next, receipt, err := durable.Advance(record.execution, task.Task, r, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	for _, spec := range r.Tasks {
		if _, exists := record.tasks[spec.ID]; exists {
			return durable.Receipt{}, durable.ErrExists
		}
	}
	for i, evt := range r.Events {
		evt.Payload = cloneBytes(evt.Payload)
		record.history = append(record.history, durable.Event{EventInput: evt, Sequence: receipt.FirstSequence + int64(i), Time: now})
	}
	for _, spec := range r.Tasks {
		spec.Payload = cloneBytes(spec.Payload)
		if spec.AvailableAt.IsZero() {
			spec.AvailableAt = now
		} else {
			spec.AvailableAt = durable.Timestamp(spec.AvailableAt)
		}
		record.tasks[spec.ID] = &durableTask{Task: durable.Task{Key: r.Key, TaskSpec: spec}}
	}
	task.done = true
	if next.State != durable.StateRunning {
		for _, pending := range record.tasks {
			pending.done = true
		}
	}
	record.execution = next
	record.receipts[r.RequestID] = durableReceipt{digest: digest, value: receipt}
	return receipt, nil
}

func cloneBytes(b []byte) []byte { return append([]byte(nil), b...) }
