package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/xraph/dispatch/durable"
)

type signalReceiptKey struct{ namespace, workflowID, requestID string }
type signalReceiptRecord struct {
	digest  string
	receipt durable.SignalReceipt
}

// SignalExecution atomically accepts a message and schedules its workflow.
func (m *Store) SignalExecution(ctx context.Context, r durable.SignalRequest) (durable.SignalReceipt, error) {
	r.Input = bytes.Clone(r.Input)
	if err := r.Validate(); err != nil {
		return durable.SignalReceipt{}, err
	}
	digest, err := durable.Fingerprint("signal", r)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	message := durable.Signal{Version: 1, ID: r.RequestID, Name: r.Name, Input: r.Input}
	payload, err := json.Marshal(message)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if contextErr := ctx.Err(); contextErr != nil {
		return durable.SignalReceipt{}, contextErr
	}
	id := signalReceiptKey{r.Namespace, r.WorkflowID, r.RequestID}
	if prior, found := m.signalReceipts[id]; found {
		return replaySignal(prior, digest)
	}
	var record *executionRecord
	if r.RunID != "" {
		record = m.executions[r.Key]
	} else {
		record = m.openSignalRun(r.Namespace, r.WorkflowID)
	}
	if record == nil {
		return durable.SignalReceipt{}, durable.ErrNotFound
	}
	receipt, err := m.appendSignal(record, r.BuildID, payload, durable.Timestamp(time.Now()))
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	m.signalReceipts[id] = signalReceiptRecord{digest: digest, receipt: receipt}
	return receipt, nil
}

// SignalWithStart deduplicates before selecting or creating an open run.
func (m *Store) SignalWithStart(ctx context.Context, r durable.SignalWithStartRequest) (durable.SignalReceipt, error) {
	r.Input, r.Start.Input = bytes.Clone(r.Input), bytes.Clone(r.Start.Input)
	if err := r.Validate(); err != nil {
		return durable.SignalReceipt{}, err
	}
	digest, err := durable.Fingerprint("signal-with-start", r)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	payload, err := json.Marshal(durable.Signal{Version: 1, ID: r.Start.RequestID, Name: r.Name, Input: r.Input})
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if contextErr := ctx.Err(); contextErr != nil {
		return durable.SignalReceipt{}, contextErr
	}
	id := signalReceiptKey{r.Start.Namespace, r.Start.WorkflowID, r.Start.RequestID}
	if prior, found := m.signalReceipts[id]; found {
		return replaySignal(prior, digest)
	}
	now := durable.Timestamp(time.Now())
	var receipt durable.SignalReceipt
	if record := m.openSignalRun(r.Start.Namespace, r.Start.WorkflowID); record != nil {
		receipt, err = m.appendSignal(record, r.Start.BuildID, payload, now)
		if err != nil {
			return durable.SignalReceipt{}, err
		}
	} else {
		if _, exists := m.executions[r.Start.Key]; exists {
			return durable.SignalReceipt{}, durable.ErrExists
		}
		record := newExecutionRecord(r.Start, now)
		record.execution.LastSequence = 2
		record.history = append(record.history, durable.Event{EventInput: durable.EventInput{Type: durable.EventSignalReceived, Payload: payload}, Sequence: 2, Time: now})
		receipt = durable.SignalReceipt{Key: r.Start.Key, Receipt: durable.Receipt{Revision: 1, FirstSequence: 1, LastSequence: 2}, Started: true}
		m.executions[r.Start.Key] = record
	}
	m.signalReceipts[id] = signalReceiptRecord{digest: digest, receipt: receipt}
	return receipt, nil
}

func replaySignal(prior signalReceiptRecord, digest string) (durable.SignalReceipt, error) {
	if prior.digest != digest {
		return durable.SignalReceipt{}, durable.ErrRequestConflict
	}
	return prior.receipt, nil
}

// The caller holds m.mu throughout selection and publication.
func (m *Store) openSignalRun(namespace, workflowID string) *executionRecord {
	for key, record := range m.executions {
		if key.Namespace == namespace && key.WorkflowID == workflowID && record.execution.State == durable.StateRunning {
			return record
		}
	}
	return nil
}

func (m *Store) appendSignal(record *executionRecord, build string, payload []byte, now time.Time) (durable.SignalReceipt, error) {
	current := record.execution
	if current.State != durable.StateRunning {
		return durable.SignalReceipt{}, durable.ErrClosed
	}
	if current.BuildID != build {
		return durable.SignalReceipt{}, durable.ErrInvalid
	}
	if current.Revision == math.MaxInt64 || current.LastSequence == math.MaxInt64 {
		return durable.SignalReceipt{}, durable.ErrInvalid
	}
	initial := record.tasks["workflow:1"]
	if initial == nil || initial.Kind != durable.TaskWorkflow {
		return durable.SignalReceipt{}, durable.ErrInvalid
	}
	current.Revision++
	current.LastSequence++
	current.UpdatedAt = now
	wake, err := durable.NewTask(current.Key, durable.TaskSpec{ID: fmt.Sprintf("workflow:signal:%d", current.Revision), Kind: durable.TaskWorkflow, Queue: initial.Queue}, now)
	if err != nil {
		return durable.SignalReceipt{}, err
	}
	if _, exists := record.tasks[wake.ID]; exists {
		return durable.SignalReceipt{}, durable.ErrExists
	}
	receipt := durable.SignalReceipt{Key: current.Key, Receipt: durable.Receipt{Revision: current.Revision, FirstSequence: current.LastSequence, LastSequence: current.LastSequence}}
	record.execution = current
	record.history = append(record.history, durable.Event{EventInput: durable.EventInput{Type: durable.EventSignalReceived, Payload: payload}, Sequence: current.LastSequence, Time: now})
	record.tasks[wake.ID] = &durableTask{Task: wake}
	return receipt, nil
}
