package memory

import (
	"cmp"
	"context"
	"slices"

	"github.com/xraph/dispatch/durable"
)

var _ durable.ReadStore = (*Store)(nil)

func (m *Store) ListExecutions(ctx context.Context, r durable.ExecutionList) ([]durable.Execution, string, error) {
	p, err := r.Position()
	if err != nil {
		return nil, "", err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, "", contextErr
	}
	rows := []durable.Execution{}
	for _, record := range m.executions {
		e := record.execution
		if e.Namespace != r.Namespace || (r.WorkflowID != "" && e.WorkflowID != r.WorkflowID) || (r.WorkflowType != "" && e.WorkflowType != r.WorkflowType) || (r.BuildID != "" && e.BuildID != r.BuildID) || (r.State != "" && e.State != r.State) {
			continue
		}
		if !p.CreatedAt.IsZero() && (e.CreatedAt.After(p.CreatedAt) || (e.CreatedAt.Equal(p.CreatedAt) && (e.WorkflowID > p.WorkflowID || (e.WorkflowID == p.WorkflowID && e.RunID >= p.ID)))) {
			continue
		}
		rows = append(rows, e.Clone())
	}
	slices.SortFunc(rows, func(a, b durable.Execution) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		if c := cmp.Compare(b.WorkflowID, a.WorkflowID); c != 0 {
			return c
		}
		return cmp.Compare(b.RunID, a.RunID)
	})
	next := ""
	if len(rows) > r.Limit {
		rows = rows[:r.Limit]
		next, err = r.Next(rows[len(rows)-1])
	}
	return rows, next, err
}
func (m *Store) ListTasks(ctx context.Context, r durable.TaskList) ([]durable.Task, string, error) {
	p, err := r.Position()
	if err != nil {
		return nil, "", err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, "", contextErr
	}
	record, ok := m.executions[r.Key]
	if !ok {
		return nil, "", durable.ErrNotFound
	}
	rows := []durable.Task{}
	for _, task := range record.tasks {
		if task.ID > p.ID && (r.Kind == "" || task.Kind == r.Kind) {
			t := task.Task
			t.Payload = cloneBytes(t.Payload)
			t.Progress = cloneBytes(t.Progress)
			rows = append(rows, t)
		}
	}
	slices.SortFunc(rows, func(a, b durable.Task) int { return cmp.Compare(a.ID, b.ID) })
	next := ""
	if len(rows) > r.Limit {
		rows = rows[:r.Limit]
		next, err = r.Next(rows[len(rows)-1])
	}
	return rows, next, err
}
func (m *Store) ReadBuildFacts(ctx context.Context, namespace, build string) (durable.BuildFacts, error) {
	var out durable.BuildFacts
	if durable.ValidateBuildRead(namespace, build) != nil {
		return out, durable.ErrInvalid
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return out, err
	}
	for _, record := range m.executions {
		if record.execution.Namespace == namespace && record.execution.BuildID == build {
			out.Executions++
			if record.execution.State == durable.StateRunning {
				out.Running++
			}
			for _, t := range record.tasks {
				if !t.Done {
					out.PendingTasks++
				}
			}
		}
	}
	return out, nil
}
func (m *Store) ReadDeliveryStatus(ctx context.Context, r durable.ScopedDeliveryStatus) (durable.DeliveryStatus, error) {
	if err := r.Validate(); err != nil {
		return durable.DeliveryStatus{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.DeliveryStatus{}, err
	}
	out := durable.DeliveryStatus{Records: []durable.DeliveryRecord{}}
	for _, d := range m.outbox {
		if !inDeliveryScope(d, r.DeliveryScope) || d.Delivery.Namespace != r.Namespace || (r.WorkflowID != "" && (d.Delivery.WorkflowID != r.WorkflowID || d.Delivery.RunID != r.RunID)) {
			continue
		}
		if d.DeliveredAt.IsZero() {
			out.Pending++
			if d.Blocked() {
				out.Blocked++
			}
			if out.OldestAcceptedAt.IsZero() || d.AcceptedAt.Before(out.OldestAcceptedAt) {
				out.OldestAcceptedAt = d.AcceptedAt
			}
		}
		if d.Delivery.ID > r.After {
			out.Records = append(out.Records, d)
		}
	}
	slices.SortFunc(out.Records, func(a, b durable.DeliveryRecord) int { return cmp.Compare(a.Delivery.ID, b.Delivery.ID) })
	if len(out.Records) > r.Limit {
		out.Records = out.Records[:r.Limit]
	}
	return out, nil
}
