package memory

import (
	"context"
	"math"
	"time"

	"github.com/xraph/dispatch/durable"
)

var _ durable.Store = (*Store)(nil)

type executionRecord struct {
	execution durable.Execution
	history   []durable.Event
	tasks     map[string]*durableTask
	receipts  map[string]durableReceipt
	children  map[string]durable.Key
}

type durableTask struct {
	durable.Task
}

type durableReceipt struct {
	digest string
	intent string
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
	record := newExecutionRecord(r, now)
	record.receipts[r.RequestID] = durableReceipt{digest: digest, value: receipt}
	m.installExecution(record)
	return receipt, nil
}

func newExecutionRecord(r durable.StartRequest, now time.Time) *executionRecord {
	return &executionRecord{
		execution: durable.Execution{Key: r.Key, WorkflowType: r.WorkflowType, BuildID: r.BuildID,
			State: durable.StateRunning, Revision: 1, LastSequence: 1, Input: cloneBytes(r.Input), CreatedAt: now, UpdatedAt: now},
		history: []durable.Event{{EventInput: durable.EventInput{Type: "execution.started", Payload: cloneBytes(r.Input)}, Sequence: 1, Time: now}},
		tasks: map[string]*durableTask{"workflow:1": {Task: durable.Task{Key: r.Key, Version: 1,
			TaskSpec: durable.TaskSpec{ID: "workflow:1", Kind: durable.TaskWorkflow, Queue: r.Queue, AvailableAt: now}}}},
		receipts: make(map[string]durableReceipt),
		children: make(map[string]durable.Key),
	}
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
			if task.Done || task.LeaseKind == durable.LeaseAsync || task.Queue != r.Queue || task.Kind != r.Kind || task.AvailableAt.After(now) || task.LeaseUntil.After(now) ||
				(!task.DeadlineAt.IsZero() && !task.DeadlineAt.After(now)) {
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
	if selected.Version == math.MaxInt64 || selected.Epoch == math.MaxInt64 || selected.Attempt == math.MaxInt64 {
		return nil, durable.ErrInvalid
	}
	selected.Owner, selected.LeaseUntil, selected.LeaseKind = r.Owner, durable.Timestamp(now.Add(r.LeaseDuration)), ""
	if !selected.DeadlineAt.IsZero() && selected.LeaseUntil.After(selected.DeadlineAt) {
		selected.LeaseUntil = selected.DeadlineAt
	}
	selected.Epoch++
	selected.Attempt++
	selected.Version++
	result := selected.Task
	result.Payload, result.Progress = cloneBytes(result.Payload), cloneBytes(result.Progress)
	return &result, nil
}

// ClaimTimeoutTask fences execution and grants independent timeout processing.
func (m *Store) ClaimTimeoutTask(ctx context.Context, r durable.TimeoutClaimRequest) (*durable.Task, error) {
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
		if key.Namespace != r.Namespace || record.execution.State != durable.StateRunning || (r.BuildID != "" && record.execution.BuildID != r.BuildID) {
			continue
		}
		for _, task := range record.tasks {
			if task.Done || task.Kind != durable.TaskActivity || task.DeadlineAt.IsZero() || task.DeadlineAt.After(now) || (task.LeaseKind == durable.LeaseTimeout && task.LeaseUntil.After(now)) {
				continue
			}
			if selected == nil || task.DeadlineAt.Before(selected.DeadlineAt) {
				selected = task
			}
		}
	}
	if selected == nil {
		return nil, nil
	}
	if selected.Version == math.MaxInt64 || selected.Epoch == math.MaxInt64 {
		return nil, durable.ErrInvalid
	}
	selected.Owner, selected.LeaseKind, selected.LeaseUntil = r.Owner, durable.LeaseTimeout, durable.Timestamp(now.Add(r.LeaseDuration))
	selected.Epoch++
	selected.Version++
	result := selected.Task
	result.Payload, result.Progress = cloneBytes(result.Payload), cloneBytes(result.Progress)
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
	if token.LeaseKind == durable.LeaseAsync {
		return time.Time{}, durable.ErrInvalid
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
	if !ok || task.Done || record.execution.State != durable.StateRunning {
		return time.Time{}, durable.ErrLeaseLost
	}
	if leaseErr := durable.CheckLease(task.Task, token, now); leaseErr != nil {
		return time.Time{}, leaseErr
	}
	until := durable.Timestamp(now.Add(ttl))
	if task.LeaseKind != durable.LeaseTimeout && !task.DeadlineAt.IsZero() && until.After(task.DeadlineAt) {
		until = task.DeadlineAt
	}
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
	if !ok || task.Done {
		return durable.Receipt{}, durable.ErrLeaseLost
	}
	now := durable.Timestamp(time.Now())
	next, receipt, err := durable.Advance(record.execution, task.Task, r, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	for _, condition := range r.Conditions {
		target, found := record.tasks[condition.TaskID]
		if !found || durable.CheckTaskCondition(target.Task, condition, now) != nil {
			return durable.Receipt{}, durable.ErrTaskConflict
		}
	}
	if sourceErr := durable.ValidateChildSource(task.Task, r.Children); sourceErr != nil {
		return durable.Receipt{}, sourceErr
	}
	if sourceErr := durable.ValidateChildCancellationSource(task.Task, r.CancelChildren); sourceErr != nil {
		return durable.Receipt{}, sourceErr
	}
	deliveries, deliveryErr := m.prepareChildDeliveries(next, r, now)
	if deliveryErr != nil {
		return durable.Receipt{}, deliveryErr
	}
	children, childErr := m.prepareChildren(record, r, now)
	if childErr != nil {
		return durable.Receipt{}, childErr
	}
	childEvents, eventErr := durable.ChildStartEvents(r.Children)
	if eventErr != nil {
		return durable.Receipt{}, eventErr
	}
	updated, err := durable.UpdateTask(task.Task, r.TaskUpdate, now)
	if err != nil {
		return durable.Receipt{}, err
	}
	changes := map[string]durable.Task{task.ID: updated}
	for _, taskID := range r.CancelTasks {
		cancelled, cancelErr := durable.UpdateTask(record.tasks[taskID].Task, nil, now)
		if cancelErr != nil {
			return durable.Receipt{}, cancelErr
		}
		changes[taskID] = cancelled
	}
	if next.State != durable.StateRunning || r.CancelPendingTasks {
		for taskID, pending := range record.tasks {
			if _, changed := changes[taskID]; changed || pending.Done {
				continue
			}
			cancelled, cancelErr := durable.UpdateTask(pending.Task, nil, now)
			if cancelErr != nil {
				return durable.Receipt{}, cancelErr
			}
			changes[taskID] = cancelled
		}
	}
	for _, spec := range r.Tasks {
		if _, exists := record.tasks[spec.ID]; exists {
			return durable.Receipt{}, durable.ErrExists
		}
		created, createErr := durable.NewTask(r.Key, spec, now)
		if createErr != nil {
			return durable.Receipt{}, createErr
		}
		changes[spec.ID] = created
	}
	events := append(append([]durable.EventInput(nil), r.Events...), childEvents...)
	for i, evt := range events {
		evt.Payload = cloneBytes(evt.Payload)
		record.history = append(record.history, durable.Event{EventInput: evt, Sequence: receipt.FirstSequence + int64(i), Time: now})
	}
	for taskID, change := range changes {
		record.tasks[taskID] = &durableTask{Task: change}
	}
	for _, child := range r.Children {
		m.installExecution(children[child.Start.Key])
		child.Start.Input = cloneBytes(child.Start.Input)
		m.childParents[child.Start.Key] = durable.ChildExecution{Parent: r.Key, ChildStartSpec: child, CreatedAt: now}
		record.children[child.CommandID] = child.Start.Key
	}
	m.saveChildDeliveries(deliveries)
	record.execution = next
	record.receipts[r.RequestID] = durableReceipt{digest: digest, intent: r.IntentDigest, value: receipt}
	return receipt, nil
}

func cloneBytes(b []byte) []byte { return append([]byte(nil), b...) }

// GetTask returns a copy of a pending or completed task in one execution.
func (m *Store) GetTask(ctx context.Context, key durable.Key, taskID string) (durable.Task, error) {
	if err := key.Validate(); err != nil {
		return durable.Task{}, err
	}
	if err := durable.ValidateTaskID(taskID); err != nil {
		return durable.Task{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.Task{}, err
	}
	record, exists := m.executions[key]
	if !exists {
		return durable.Task{}, durable.ErrNotFound
	}
	task, exists := record.tasks[taskID]
	if !exists {
		return durable.Task{}, durable.ErrNotFound
	}
	result := task.Task
	result.Payload, result.Progress = cloneBytes(result.Payload), cloneBytes(result.Progress)
	return result, nil
}
