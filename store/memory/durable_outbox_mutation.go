package memory

import (
	"context"
	"maps"
	"sort"
	"time"

	"github.com/xraph/dispatch/durable"
)

// mutateAudited stages the affected namespace while holding the activation lock.
// No candidate writes or failures become visible until the entire intent batch
// is prepared. Memory is a development backend: index copying is O(store size),
// and deep copying is O(affected namespace state).
func mutateAudited[T any](ctx context.Context, m *Store, namespace string, fn func(*Store) (T, error)) (T, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	n, registered := m.namespaces[namespace]
	c := m.durableCandidate(namespace)
	result, err := fn(c)
	if err != nil {
		return zero, err
	}
	var intents []durable.Delivery
	if registered {
		sources := m.newDeliverySources(ctx, c, namespace)
		for _, source := range sources {
			destinations := []durable.Destination{}
			if n.RequireAudit {
				destinations = append(destinations, durable.DestinationChronicle)
			}
			if n.RequireHooks && source.Kind == "event" {
				destinations = append(destinations, durable.DestinationRelay)
			}
			for _, destination := range destinations {
				d, prepareErr := durable.NewDelivery(n, destination, source)
				if prepareErr != nil {
					return zero, prepareErr
				}
				if old, ok := m.outbox[d.ID]; ok && old.Delivery.Fingerprint != d.Fingerprint {
					return zero, durable.ErrRequestConflict
				}
				if m.outboxPrepare != nil {
					if prepareErr := m.outboxPrepare(d); prepareErr != nil {
						return zero, prepareErr
					}
				}
				intents = append(intents, d)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	m.queryRuntimes = c.queryRuntimes
	m.taskDeferrals = c.taskDeferrals
	m.taskDeferralReceipts = c.taskDeferralReceipts
	m.retirementNamespaces = c.retirementNamespaces
	m.buildAdmissions = c.buildAdmissions
	m.lifecycleReceipts = c.lifecycleReceipts
	m.executions = c.executions
	m.executionHeads = c.executionHeads
	m.childParents = c.childParents
	m.childDeliveries = c.childDeliveries
	m.childDeliveryReceipts = c.childDeliveryReceipts
	m.signalReceipts = c.signalReceipts
	m.cancellationReceipts = c.cancellationReceipts
	now := time.Now().UTC()
	for _, d := range intents {
		m.outbox[d.ID] = durable.DeliveryRecord{Delivery: d, AcceptedAt: now}
	}
	return result, nil
}
func (m *Store) durableCandidate(namespace string) *Store {
	c := &Store{queryRuntimes: maps.Clone(m.queryRuntimes), taskDeferrals: maps.Clone(m.taskDeferrals), taskDeferralReceipts: maps.Clone(m.taskDeferralReceipts), namespaces: m.namespaces, retirementNamespaces: maps.Clone(m.retirementNamespaces), buildAdmissions: maps.Clone(m.buildAdmissions), lifecycleReceipts: maps.Clone(m.lifecycleReceipts), executions: maps.Clone(m.executions), executionHeads: maps.Clone(m.executionHeads), childParents: maps.Clone(m.childParents), childDeliveries: maps.Clone(m.childDeliveries), childDeliveryReceipts: maps.Clone(m.childDeliveryReceipts), signalReceipts: maps.Clone(m.signalReceipts), cancellationReceipts: maps.Clone(m.cancellationReceipts)}
	for key, r := range c.executions {
		if key.Namespace != namespace {
			continue
		}
		v := cloneChildTarget(r)
		v.execution = r.execution.Clone()
		v.children = maps.Clone(r.children)
		for i := range v.history {
			v.history[i].Payload = cloneBytes(v.history[i].Payload)
		}
		for _, t := range v.tasks {
			t.Payload = cloneBytes(t.Payload)
			t.Progress = cloneBytes(t.Progress)
			if t.DeadlineLimit != nil {
				v := *t.DeadlineLimit
				t.DeadlineLimit = &v
			}
		}
		c.executions[key] = v
	}
	for key, v := range c.childParents {
		if key.Namespace == namespace {
			v.Start = v.Start.Clone()
			c.childParents[key] = v
		}
	}
	for key, v := range c.childDeliveries {
		if key.source.Namespace == namespace {
			c.childDeliveries[key] = v.Clone()
		}
	}
	return c
}
func (m *Store) newDeliverySources(ctx context.Context, c *Store, namespace string) []durable.DeliverySource {
	sources := []durable.DeliverySource{}
	now := time.Now().UTC()
	metadata := durable.AuditMetadataFromContext(ctx)
	receipt := func(key durable.Key, kind, id, outcome, action string) {
		if kind != "child_receipt" {
			id = durable.ReceiptSourceID(id)
		}
		sources = append(sources, durable.DeliverySource{Key: key, Kind: kind, ID: id, OccurredAt: now, Action: action, Outcome: outcome, Metadata: metadata})
	}
	for key, r := range c.executions {
		if key.Namespace != namespace {
			continue
		}
		old := m.executions[key]
		first := 0
		if old != nil {
			first = len(old.history)
		}
		for _, event := range r.history[first:] {
			sources = append(sources, durable.EventDeliverySource(ctx, key, event))
		}
		for id, accepted := range r.receipts {
			if old != nil {
				if _, exists := old.receipts[id]; exists {
					continue
				}
			}
			receipt(key, "execution_receipt", id, "accepted", accepted.action)
		}
	}
	for key, r := range c.signalReceipts {
		if key.namespace == namespace {
			if _, exists := m.signalReceipts[key]; !exists {
				receipt(r.receipt.Key, "signal_receipt", key.requestID, "accepted", "execution.signal")
			}
		}
	}
	for key, r := range c.cancellationReceipts {
		if key.namespace == namespace {
			if _, exists := m.cancellationReceipts[key]; !exists {
				receipt(r.receipt.Key, "cancellation_receipt", key.requestID, "accepted", "execution.cancel")
			}
		}
	}
	for key, r := range c.childDeliveryReceipts {
		if key.source.Namespace == namespace {
			if _, exists := m.childDeliveryReceipts[key]; !exists {
				receipt(key.source, "child_receipt", durable.ReceiptSourceID(key.id, key.requestID), r.receipt.Disposition, "child.deliver")
			}
		}
	}
	for key, r := range c.lifecycleReceipts {
		if key.namespace == namespace {
			if _, exists := m.lifecycleReceipts[key]; !exists {
				sources = append(sources, durable.LifecycleDeliverySource(ctx, r))
			}
		}
	}
	sort.Slice(sources, func(i, j int) bool {
		a, b := sources[i], sources[j]
		if a.Key.WorkflowID != b.Key.WorkflowID {
			return a.Key.WorkflowID < b.Key.WorkflowID
		}
		if a.Key.RunID != b.Key.RunID {
			return a.Key.RunID < b.Key.RunID
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.ID < b.ID
	})
	return sources
}

func (m *Store) StartExecution(ctx context.Context, r durable.StartRequest) (durable.Receipt, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.Receipt, error) { return c.startExecution(ctx, r) })
}

func (m *Store) CommitTransition(ctx context.Context, r durable.CommitRequest) (durable.Receipt, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.Receipt, error) { return c.commitTransition(ctx, r) })
}

func (m *Store) SignalExecution(ctx context.Context, r durable.SignalRequest) (durable.SignalReceipt, error) {
	if err := r.Validate(); err != nil {
		return durable.SignalReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.SignalReceipt, error) { return c.signalExecution(ctx, r) })
}

func (m *Store) SignalWithStart(ctx context.Context, r durable.SignalWithStartRequest) (durable.SignalReceipt, error) {
	if err := r.Validate(); err != nil {
		return durable.SignalReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Start.Namespace, func(c *Store) (durable.SignalReceipt, error) { return c.signalWithStart(ctx, r) })
}

// SignalWithStartOutcome preserves acceptance provenance within the audited lock.
func (m *Store) SignalWithStartOutcome(ctx context.Context, r durable.SignalWithStartRequest) (durable.SignalStartOutcome, error) {
	if err := r.Validate(); err != nil {
		return durable.SignalStartOutcome{}, err
	}
	return mutateAudited(ctx, m, r.Start.Namespace, func(c *Store) (durable.SignalStartOutcome, error) {
		_, recovered := c.signalReceipts[signalReceiptKey{r.Start.Namespace, r.Start.WorkflowID, r.Start.RequestID}]
		receipt, err := c.signalWithStart(ctx, r)
		if err != nil {
			return durable.SignalStartOutcome{}, err
		}
		return durable.SignalStartOutcome{Receipt: receipt, Recovered: recovered}, nil
	})
}

func (m *Store) RequestCancelExecution(ctx context.Context, r durable.CancelExecutionRequest) (durable.CancelExecutionReceipt, error) {
	if err := r.Validate(); err != nil {
		return durable.CancelExecutionReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.CancelExecutionReceipt, error) { return c.requestCancelExecution(ctx, r) })
}

func (m *Store) RecordHeartbeat(ctx context.Context, r durable.HeartbeatRequest) (durable.Receipt, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.Receipt, error) { return c.recordHeartbeat(ctx, r) })
}

func (m *Store) ApplyExecutionTimeout(ctx context.Context, r durable.ExecutionTimeoutRequest) (durable.Receipt, error) {
	if err := r.Validate(); err != nil {
		return durable.Receipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.Receipt, error) { return c.applyExecutionTimeout(ctx, r) })
}

func (m *Store) ApplyChildDelivery(ctx context.Context, r durable.ChildDeliveryRequest) (durable.ChildDeliveryReceipt, error) {
	if err := r.Validate(); err != nil {
		return durable.ChildDeliveryReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Source.Namespace, func(c *Store) (durable.ChildDeliveryReceipt, error) { return c.applyChildDelivery(ctx, r) })
}
