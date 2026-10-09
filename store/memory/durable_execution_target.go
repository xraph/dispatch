package memory

import (
	"context"

	"github.com/xraph/dispatch/durable"
)

// installExecution publishes a new execution and its latest pointer together.
// The caller holds m.mu and has finished all validation that can fail.
func (m *Store) installExecution(record *executionRecord) {
	key := record.execution.Key
	m.executions[key] = record
	workflow := key
	workflow.RunID = ""
	m.executionHeads[workflow] = key
}

// ResolveExecution copies one execution snapshot under the store read lock.
func (m *Store) ResolveExecution(ctx context.Context, r durable.ExecutionTarget) (durable.Execution, error) {
	if err := r.Validate(); err != nil {
		return durable.Execution{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.Execution{}, err
	}
	var record *executionRecord
	switch r.Selection {
	case durable.RunExplicit:
		record = m.executions[r.Key]
	case durable.RunCurrent:
		record = m.openSignalRun(r.Namespace, r.WorkflowID)
	case durable.RunLatest:
		if key, ok := m.executionHeads[r.Key]; ok {
			record = m.executions[key]
		}
	}
	if record == nil {
		return durable.Execution{}, durable.ErrNotFound
	}
	result := record.execution
	result.Input, result.Output = cloneBytes(result.Input), cloneBytes(result.Output)
	return result, nil
}
