package memory

import (
	"github.com/xraph/dispatch/durable"
)

func (m *Store) prepareContinuationRecord(batch *durable.ContinuationBatch, spec *durable.ContinueSpec) (*executionRecord, error) {
	if batch == nil {
		return nil, nil
	}
	if spec == nil {
		spec = &batch.Spec
	}
	if _, exists := m.executions[batch.Execution.Key]; exists {
		return nil, durable.ErrExists
	}
	return &executionRecord{execution: batch.Execution, history: batch.History,
		tasks:    map[string]*durableTask{"workflow:1": {Task: durable.Task{Key: batch.Execution.Key, Version: 1, TaskSpec: durable.TaskSpec{ID: "workflow:1", Kind: durable.TaskWorkflow, Queue: spec.Queue, AvailableAt: batch.Execution.AvailableAt()}}}},
		receipts: make(map[string]durableReceipt), children: make(map[string]durable.Key)}, nil
}

func (m *Store) childRoot(key durable.Key) durable.Key {
	if record := m.executions[key]; record != nil {
		key.RunID = record.execution.FirstRunID
	}
	return key
}

func (m *Store) currentChildKey(root durable.Key) durable.Key {
	key := root
	for {
		record := m.executions[key]
		if record == nil || record.execution.NextRunID == "" {
			return key
		}
		key.RunID = record.execution.NextRunID
	}
}
