package memory

import (
	"context"
	"sort"
	"time"

	"github.com/xraph/dispatch/durable"
)

func (m *Store) prepareChildren(parent *executionRecord, r durable.CommitRequest, now time.Time) (map[durable.Key]*executionRecord, error) {
	children := make(map[durable.Key]*executionRecord, len(r.Children))
	for _, child := range r.Children {
		_, commandExists := parent.children[child.CommandID]
		_, runExists := m.executions[child.Start.Key]
		if commandExists || runExists || m.openSignalRun(child.Start.Namespace, child.Start.WorkflowID) != nil {
			return nil, &durable.ChildStartError{CommandID: child.CommandID, Err: durable.ErrExists}
		}
		digest, err := durable.Fingerprint("start", child.Start)
		if err != nil {
			return nil, err
		}
		created := newExecutionRecord(child.Start, now)
		created.receipts[child.Start.RequestID] = durableReceipt{digest: digest, value: durable.Receipt{Revision: 1, FirstSequence: 1, LastSequence: 1}}
		children[child.Start.Key] = created
	}
	return children, nil
}

func (m *Store) childProjection(key durable.Key) durable.ChildExecution {
	link := m.childParents[key]
	child := m.executions[key].execution
	link.State, link.UpdatedAt = child.State, child.UpdatedAt
	link.Start.Input = cloneBytes(link.Start.Input)
	return link
}

// GetChildExecution reads one parent's immutable child identity and current state.
func (m *Store) GetChildExecution(ctx context.Context, parent durable.Key, commandID string) (durable.ChildExecution, error) {
	if err := parent.Validate(); err != nil {
		return durable.ChildExecution{}, err
	}
	if err := durable.ValidateTaskID(commandID); err != nil {
		return durable.ChildExecution{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.ChildExecution{}, err
	}
	record, exists := m.executions[parent]
	if !exists {
		return durable.ChildExecution{}, durable.ErrNotFound
	}
	key, found := record.children[commandID]
	if !found {
		return durable.ChildExecution{}, durable.ErrNotFound
	}
	return m.childProjection(key), nil
}

// GetParentExecution reads a child's unique original parent relationship.
func (m *Store) GetParentExecution(ctx context.Context, child durable.Key) (durable.ChildExecution, error) {
	if err := child.Validate(); err != nil {
		return durable.ChildExecution{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.ChildExecution{}, err
	}
	if _, exists := m.childParents[child]; !exists {
		return durable.ChildExecution{}, durable.ErrNotFound
	}
	return m.childProjection(child), nil
}

// ListChildExecutions pages by command ID, with an exclusive cursor.
func (m *Store) ListChildExecutions(ctx context.Context, parent durable.Key, after string, limit int) ([]durable.ChildExecution, error) {
	if err := parent.Validate(); err != nil {
		return nil, err
	}
	if (after != "" && durable.ValidateTaskID(after) != nil) || limit < 1 || limit > 1000 {
		return nil, durable.ErrInvalid
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	record, exists := m.executions[parent]
	if !exists {
		return nil, durable.ErrNotFound
	}
	ids := make([]string, 0, len(record.children))
	for id := range record.children {
		if id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	result := make([]durable.ChildExecution, 0, len(ids))
	for _, id := range ids {
		result = append(result, m.childProjection(record.children[id]))
	}
	return result, nil
}
