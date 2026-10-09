package memory

import (
	"context"
	"encoding/json"
	"maps"
	"math"
	"sort"
	"time"

	"github.com/xraph/dispatch/durable"
)

type childDeliveryKey struct {
	source durable.Key
	id     string
}
type childDeliveryReceiptKey struct {
	childDeliveryKey
	requestID string
}
type childDeliveryReceiptRecord struct {
	digest  string
	receipt durable.ChildDeliveryReceipt
}

func (m *Store) prepareChildDeliveries(next durable.Execution, r durable.CommitRequest, now time.Time) ([]durable.ChildDelivery, error) {
	b := durable.ChildDeliveryBatch{Next: next, Request: r}
	for _, key := range m.executions[next.Key].children {
		b.Children = append(b.Children, m.childProjection(key))
	}
	if parent, ok := m.childParents[next.Key]; ok {
		b.Parent = &parent
		b.ParentBuildID = m.executions[parent.Parent].execution.BuildID
	}
	deliveries, err := durable.PrepareChildDeliveries(b, now)
	if err != nil {
		return nil, err
	}
	return deliveries, m.checkChildDeliveries(deliveries)
}

func (m *Store) checkChildDeliveries(deliveries []durable.ChildDelivery) error {
	seen := make(map[childDeliveryKey]bool, len(deliveries))
	for _, d := range deliveries {
		key := childDeliveryKey{d.Source, d.ID}
		if _, exists := m.childDeliveries[key]; exists || seen[key] {
			return durable.ErrExists
		}
		if validationErr := d.Validate(); validationErr != nil {
			return validationErr
		}
		seen[key] = true
	}
	return nil
}

func (m *Store) saveChildDeliveries(deliveries []durable.ChildDelivery) {
	for _, d := range deliveries {
		m.childDeliveries[childDeliveryKey{d.Source, d.ID}] = d.Clone()
	}
}

// ClaimChildDelivery polls messages even when their source execution has closed.
func (m *Store) ClaimChildDelivery(ctx context.Context, r durable.ChildDeliveryClaimRequest) (*durable.ChildDelivery, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := durable.Timestamp(time.Now())
	var selected *durable.ChildDelivery
	for _, d := range m.childDeliveries {
		if d.Source.Namespace != r.Namespace || d.Done || r.BuildID != "" && d.TargetBuildID != r.BuildID || d.AvailableAt.After(now) || d.LeaseUntil.After(now) {
			continue
		}
		if selected == nil || d.AvailableAt.Before(selected.AvailableAt) {
			selected = &d
		}
	}
	if selected == nil {
		return nil, nil
	}
	if selected.Epoch == math.MaxInt64 || selected.Attempt == math.MaxInt64 {
		return nil, durable.ErrInvalid
	}
	selected.Owner = r.Owner
	selected.Epoch++
	selected.Attempt++
	selected.LeaseUntil = durable.Timestamp(now.Add(r.LeaseDuration))
	m.childDeliveries[childDeliveryKey{selected.Source, selected.ID}] = *selected
	result := selected.Clone()
	return &result, nil
}

// GetChildDelivery returns an isolated view including its final disposition.
func (m *Store) GetChildDelivery(ctx context.Context, source durable.Key, id string) (durable.ChildDelivery, error) {
	if err := source.Validate(); err != nil {
		return durable.ChildDelivery{}, err
	}
	if err := durable.ValidateTaskID(id); err != nil {
		return durable.ChildDelivery{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.ChildDelivery{}, err
	}
	d, ok := m.childDeliveries[childDeliveryKey{source, id}]
	if !ok {
		return durable.ChildDelivery{}, durable.ErrNotFound
	}
	return d.Clone(), nil
}

// ListChildDeliveries pages by exclusive delivery ID, including completed rows.
func (m *Store) ListChildDeliveries(ctx context.Context, source durable.Key, after string, limit int) ([]durable.ChildDelivery, error) {
	if err := source.Validate(); err != nil {
		return nil, err
	}
	if after != "" && durable.ValidateTaskID(after) != nil || limit < 1 || limit > 1000 {
		return nil, durable.ErrInvalid
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := m.executions[source]; !ok {
		return nil, durable.ErrNotFound
	}
	result := make([]durable.ChildDelivery, 0)
	for key, d := range m.childDeliveries {
		if key.source == source && key.id > after {
			result = append(result, d.Clone())
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func cloneChildTarget(record *executionRecord) *executionRecord {
	candidate := *record
	candidate.history = append([]durable.Event(nil), record.history...)
	candidate.receipts = maps.Clone(record.receipts)
	candidate.tasks = make(map[string]*durableTask, len(record.tasks))
	for id, task := range record.tasks {
		copyTask := *task
		candidate.tasks[id] = &copyTask
	}
	return &candidate
}

// ApplyChildDelivery changes one target and its receipt atomically.
func (m *Store) ApplyChildDelivery(ctx context.Context, r durable.ChildDeliveryRequest) (durable.ChildDeliveryReceipt, error) {
	if err := r.Validate(); err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	digest, err := durable.Fingerprint("child-delivery", r)
	if err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return durable.ChildDeliveryReceipt{}, ctxErr
	}
	key := childDeliveryKey{r.Source, r.DeliveryID}
	receiptKey := childDeliveryReceiptKey{key, r.RequestID}
	if prior, ok := m.childDeliveryReceipts[receiptKey]; ok {
		if prior.digest != digest {
			return durable.ChildDeliveryReceipt{}, durable.ErrRequestConflict
		}
		return prior.receipt, nil
	}
	d, ok := m.childDeliveries[key]
	if !ok {
		return durable.ChildDeliveryReceipt{}, durable.ErrNotFound
	}
	now := durable.Timestamp(time.Now())
	if leaseErr := d.CheckLease(r, now); leaseErr != nil {
		return durable.ChildDeliveryReceipt{}, leaseErr
	}
	if validationErr := d.Validate(); validationErr != nil {
		return durable.ChildDeliveryReceipt{}, validationErr
	}
	current, ok := m.executions[d.Target]
	if !ok {
		return durable.ChildDeliveryReceipt{}, durable.ErrNotFound
	}
	if current.execution.BuildID != d.TargetBuildID || current.tasks["workflow:1"].Queue != d.TargetQueue {
		return durable.ChildDeliveryReceipt{}, durable.ErrInvalid
	}
	candidate := cloneChildTarget(current)
	receipt, generated, cancelReceipt, err := m.applyChildTarget(candidate, d, now)
	if err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	if err := m.checkChildDeliveries(generated); err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	m.executions[d.Target] = candidate
	m.saveChildDeliveries(generated)
	if cancelReceipt != nil {
		cr := d.CancellationRequest()
		m.cancellationReceipts[signalReceiptKey{cr.Namespace, cr.WorkflowID, cr.RequestID}] = *cancelReceipt
	}
	d.Done, d.Disposition = true, receipt.Disposition
	m.childDeliveries[key] = d
	m.childDeliveryReceipts[receiptKey] = childDeliveryReceiptRecord{digest, receipt}
	return receipt, nil
}

func (m *Store) applyChildTarget(target *executionRecord, d durable.ChildDelivery, now time.Time) (durable.ChildDeliveryReceipt, []durable.ChildDelivery, *cancellationReceiptRecord, error) {
	receipt := durable.ChildDeliveryReceipt{Target: d.Target, Receipt: durable.Receipt{Revision: target.execution.Revision, LastSequence: target.execution.LastSequence}, Disposition: durable.ChildDeliveryIgnoredClosed}
	var generated []durable.ChildDelivery
	var cancelReceipt *cancellationReceiptRecord
	if target.execution.State == durable.StateRunning && durable.CheckExecutionDeadline(target.execution, now) != nil {
		receipt.Disposition = durable.ChildDeliveryIgnoredExpired
	} else if target.execution.State == durable.StateRunning {
		var err error
		receipt.Disposition = durable.ChildDeliveryApplied
		switch d.Kind {
		case durable.ChildDeliveryResult, durable.ChildDeliveryCancelAck:
			payload, encodeErr := json.Marshal(d.Message)
			if encodeErr != nil {
				return durable.ChildDeliveryReceipt{}, nil, nil, encodeErr
			}
			eventType := durable.EventChildCompleted
			if d.Kind == durable.ChildDeliveryCancelAck {
				eventType = durable.EventChildCancellationAcknowledged
			}
			accepted, appendErr := m.appendWorkflowInput(target, d.TargetBuildID, payload, now, eventType, "child")
			err = appendErr
			receipt.Receipt = accepted.Receipt
		case durable.ChildDeliveryClose:
			if d.Message.Policy == durable.ParentCloseTerminate {
				receipt.Receipt, generated, err = m.terminateChildTarget(target, d, now)
			} else {
				receipt.Receipt, cancelReceipt, err = m.cancelChildTarget(target, d, now)
			}
		case durable.ChildDeliveryCancel:
			receipt.Receipt, cancelReceipt, err = m.cancelChildTarget(target, d, now)
		}
		if err != nil {
			return durable.ChildDeliveryReceipt{}, nil, nil, err
		}
	}
	if d.Kind == durable.ChildDeliveryCancel && d.Message.CancellationID != "" {
		link, ok := m.childParents[d.Target]
		if !ok || link.Parent != d.Source || link.CommandID != d.Message.CommandID {
			return durable.ChildDeliveryReceipt{}, nil, nil, durable.ErrInvalid
		}
		ack, err := d.CancellationAcknowledgment(m.executions[link.Parent].execution.BuildID, link.ParentQueue, receipt.Disposition, now)
		if err != nil {
			return durable.ChildDeliveryReceipt{}, nil, nil, err
		}
		generated = append(generated, ack)
	}
	return receipt, generated, cancelReceipt, nil
}

func (m *Store) cancelChildTarget(target *executionRecord, d durable.ChildDelivery, now time.Time) (durable.Receipt, *cancellationReceiptRecord, error) {
	r := d.CancellationRequest()
	digest, err := durable.Fingerprint("cancel-execution", r)
	if err != nil {
		return durable.Receipt{}, nil, err
	}
	if prior, ok := m.cancellationReceipts[signalReceiptKey{r.Namespace, r.WorkflowID, r.RequestID}]; ok {
		if prior.digest != digest {
			return durable.Receipt{}, nil, durable.ErrRequestConflict
		}
		return prior.receipt.Receipt, nil, nil
	}
	payload, err := json.Marshal(durable.ExecutionCancellation{Version: 1, RequestID: r.RequestID, Reason: r.Reason})
	if err != nil {
		return durable.Receipt{}, nil, err
	}
	accepted, err := m.appendWorkflowInput(target, r.BuildID, payload, now, durable.EventCancellationRequested, "cancel-request")
	if err != nil {
		return durable.Receipt{}, nil, err
	}
	stored := &cancellationReceiptRecord{digest: digest, receipt: durable.CancelExecutionReceipt{Key: accepted.Key, Receipt: accepted.Receipt}}
	return accepted.Receipt, stored, nil
}

func (m *Store) terminateChildTarget(target *executionRecord, d durable.ChildDelivery, now time.Time) (durable.Receipt, []durable.ChildDelivery, error) {
	current := target.execution
	if current.Revision == math.MaxInt64 || current.LastSequence == math.MaxInt64 {
		return durable.Receipt{}, nil, durable.ErrInvalid
	}
	payload, err := json.Marshal(durable.ExecutionTermination{Version: 1, RequestID: d.CancellationRequest().RequestID, Reason: "parent closed"})
	if err != nil {
		return durable.Receipt{}, nil, err
	}
	event := durable.EventInput{Type: durable.EventWorkflowTerminated, Payload: payload}
	for _, task := range target.tasks {
		if task.Done {
			continue
		}
		updated, updateErr := durable.UpdateTask(task.Task, nil, now)
		if updateErr != nil {
			return durable.Receipt{}, nil, updateErr
		}
		task.Task = updated
	}
	target.execution.Revision++
	target.execution.LastSequence++
	target.execution.State = durable.StateTerminated
	target.execution.Output = nil
	target.execution.UpdatedAt = now
	target.history = append(target.history, durable.Event{EventInput: event, Sequence: target.execution.LastSequence, Time: now})
	generated, err := m.prepareChildDeliveries(target.execution, durable.CommitRequest{Key: d.Target, Events: []durable.EventInput{event}}, now)
	if err != nil {
		return durable.Receipt{}, nil, err
	}
	return durable.Receipt{Revision: target.execution.Revision, FirstSequence: target.execution.LastSequence, LastSequence: target.execution.LastSequence}, generated, nil
}
