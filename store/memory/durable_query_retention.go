package memory

import (
	"context"
	"sort"
	"time"

	"github.com/xraph/dispatch/durable"
)

type queryRuntimeKey struct{ namespace, id string }

var _ durable.QueryRuntimeStore = (*Store)(nil)

func (m *Store) queryBindings(target durable.BuildTarget) (int64, []durable.QueryRuntimeBinding) {
	var retained int64
	for k, e := range m.executions {
		if k.Namespace == target.Namespace && e.execution.BuildID == target.BuildID {
			retained++
		}
	}
	bindings := []durable.QueryRuntimeBinding{}
	for _, b := range m.queryRuntimes {
		if b.BuildTarget == target {
			bindings = append(bindings, b)
		}
	}
	return retained, bindings
}
func (m *Store) InspectQueryRetention(ctx context.Context, target durable.BuildTarget) (durable.QueryRetentionFacts, error) {
	if checkErr := target.Validate(); checkErr != nil {
		return durable.QueryRetentionFacts{}, checkErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if checkErr := ctx.Err(); checkErr != nil {
		return durable.QueryRetentionFacts{}, checkErr
	}
	if checkErr := m.checkLifecycleTarget(target.NamespaceTarget); checkErr != nil {
		return durable.QueryRetentionFacts{}, checkErr
	}
	b, ok := m.buildAdmissions[target]
	if !ok {
		return durable.QueryRetentionFacts{}, durable.ErrNotFound
	}
	retained, bindings := m.queryBindings(target)
	return durable.QueryRetention(b, retained, bindings, durable.Timestamp(time.Now())), nil
}
func (m *Store) ListQueryRuntimes(ctx context.Context, r durable.QueryRuntimeList) (durable.QueryRuntimePage, error) {
	var page durable.QueryRuntimePage
	if checkErr := r.Validate(); checkErr != nil {
		return page, checkErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if checkErr := ctx.Err(); checkErr != nil {
		return page, checkErr
	}
	if checkErr := m.checkLifecycleTarget(r.NamespaceTarget); checkErr != nil {
		return page, checkErr
	}
	page.Items = []durable.QueryRuntimeBinding{}
	page.ObservedAt = durable.Timestamp(time.Now())
	for _, b := range m.queryRuntimes {
		if b.NamespaceTarget == r.NamespaceTarget && b.RuntimeID > r.After && (r.InstanceID == "" || b.InstanceID == r.InstanceID) && (r.BuildID == "" || b.BuildID == r.BuildID) {
			page.Items = append(page.Items, b)
		}
	}
	sort.Slice(page.Items, func(i, j int) bool { return page.Items[i].RuntimeID < page.Items[j].RuntimeID })
	if len(page.Items) > r.Limit {
		page.Items = page.Items[:r.Limit]
		page.Next = page.Items[len(page.Items)-1].RuntimeID
	}
	return page, nil
}
func (m *Store) mutateQueryRuntime(ctx context.Context, target durable.QueryRuntimeTarget, operation durable.LifecycleOperation, id, digest, command string, settlement *durable.QueryRemovalAbortEvidence, apply func(*Store, durable.BuildAdmission, durable.QueryRuntimeBinding, bool, time.Time) (durable.QueryRuntimeBinding, error)) (durable.LifecycleReceipt, error) {
	return mutateAudited(ctx, m, target.Namespace, func(c *Store) (durable.LifecycleReceipt, error) {
		key := lifecycleReceiptKey{target.Namespace, operation, id}
		q := durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: operation, RequestID: id, RequestDigest: digest}
		if saved, ok := c.lifecycleReceipts[key]; ok {
			return saved.Clone(), saved.Match(q)
		}
		if checkErr := c.checkLifecycleTarget(target.NamespaceTarget); checkErr != nil {
			return durable.LifecycleReceipt{}, checkErr
		}
		build, ok := c.buildAdmissions[target.BuildTarget]
		if !ok {
			return durable.LifecycleReceipt{}, durable.ErrNotFound
		}
		runtimeKey := queryRuntimeKey{target.Namespace, target.RuntimeID}
		prior, exists := c.queryRuntimes[runtimeKey]
		if exists && prior.BuildTarget != target.BuildTarget {
			return durable.LifecycleReceipt{}, durable.ErrRequestConflict
		}
		now := durable.Timestamp(time.Now())
		next, err := apply(c, build, prior, exists, now)
		if err != nil {
			return durable.LifecycleReceipt{}, err
		}
		if checkErr := next.Validate(); checkErr != nil {
			return durable.LifecycleReceipt{}, checkErr
		}
		receipt := durable.LifecycleReceipt{NamespaceTarget: target.NamespaceTarget, Operation: operation, RequestID: id, RequestDigest: digest, CommandDigest: command, ResponseVersion: 1, AcceptedAt: next.ChangedAt, QueryRuntime: &next, QueryAbort: settlement}
		delivery, err := durable.NewDelivery(c.namespaces[target.Namespace], durable.DestinationChronicle, durable.LifecycleDeliverySource(ctx, receipt))
		if err != nil {
			return durable.LifecycleReceipt{}, err
		}
		receipt.DeliveryID = delivery.ID
		c.queryRuntimes[runtimeKey] = next
		c.lifecycleReceipts[key] = receipt
		return receipt.Clone(), nil
	})
}
func (m *Store) RegisterQueryRuntime(ctx context.Context, r durable.RegisterQueryRuntimeRequest) (durable.LifecycleReceipt, error) {
	if checkErr := r.Validate(); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	digest, err := durable.Fingerprint(string(durable.OperationRegisterQueryRuntime), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return m.mutateQueryRuntime(ctx, r.Identity.QueryRuntimeTarget, durable.OperationRegisterQueryRuntime, r.RequestID, digest, r.CommandDigest, nil, func(_ *Store, build durable.BuildAdmission, _ durable.QueryRuntimeBinding, exists bool, now time.Time) (durable.QueryRuntimeBinding, error) {
		if exists {
			return durable.QueryRuntimeBinding{}, durable.ErrRequestConflict
		}
		if build.QueryIdentity != r.Identity.BuildIdentity || build.QueryIdentity.Validate() != nil {
			return durable.QueryRuntimeBinding{}, durable.ErrQueryRetention
		}
		return durable.QueryRuntimeBinding{QueryRuntimeIdentity: r.Identity, State: durable.QueryRuntimeActive, Version: 1, CreatedAt: now, ChangedAt: now}, nil
	})
}
func (m *Store) RecordQueryRuntimeVerification(ctx context.Context, r durable.VerifyQueryRuntimeRequest) (durable.LifecycleReceipt, error) {
	if checkErr := r.Validate(); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	operation := durable.OperationVerifyQueryRuntime
	digest, err := durable.Fingerprint(string(operation), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return m.mutateQueryRuntime(ctx, r.QueryRuntimeTarget, operation, r.RequestID, digest, r.CommandDigest, nil, func(_ *Store, _ durable.BuildAdmission, b durable.QueryRuntimeBinding, exists bool, now time.Time) (durable.QueryRuntimeBinding, error) {
		if !exists {
			return b, durable.ErrNotFound
		}
		return durable.VerifyQueryBinding(b, r, now)
	})
}
func (m *Store) AbortQueryRuntimeRemoval(ctx context.Context, r durable.AbortQueryRemovalRequest) (durable.LifecycleReceipt, error) {
	if checkErr := r.Validate(); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	operation := durable.OperationAbortQueryRemoval
	digest, err := durable.Fingerprint(string(operation), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return m.mutateQueryRuntime(ctx, r.QueryRuntimeTarget, operation, r.RequestID, digest, r.CommandDigest, &durable.QueryRemovalAbortEvidence{Fence: r.Fence, Settlement: r.Settlement}, func(_ *Store, _ durable.BuildAdmission, b durable.QueryRuntimeBinding, exists bool, now time.Time) (durable.QueryRuntimeBinding, error) {
		if !exists {
			return b, durable.ErrNotFound
		}
		return durable.AbortQueryRemoval(b, r, now)
	})
}
func (m *Store) BeginQueryRuntimeRemoval(ctx context.Context, r durable.BeginQueryRemovalRequest) (durable.LifecycleReceipt, error) {
	if checkErr := r.Validate(); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	digest, err := durable.Fingerprint(string(durable.OperationBeginQueryRemoval), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return m.mutateQueryRuntime(ctx, r.QueryRuntimeTarget, durable.OperationBeginQueryRemoval, r.RequestID, digest, "", nil, func(c *Store, build durable.BuildAdmission, b durable.QueryRuntimeBinding, exists bool, _ time.Time) (durable.QueryRuntimeBinding, error) {
		if !exists {
			return b, durable.ErrNotFound
		}
		retained, bindings := c.queryBindings(r.BuildTarget)
		return durable.ReserveQueryRemoval(b, r, build, retained, bindings, durable.Timestamp(time.Now()))
	})
}
func (m *Store) FinishQueryRuntimeRemoval(ctx context.Context, r durable.FinishQueryRemovalRequest) (durable.LifecycleReceipt, error) {
	if checkErr := r.Validate(); checkErr != nil {
		return durable.LifecycleReceipt{}, checkErr
	}
	digest, err := durable.Fingerprint(string(durable.OperationFinishQueryRemoval), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return m.mutateQueryRuntime(ctx, r.Fence.Candidate.QueryRuntimeTarget, durable.OperationFinishQueryRemoval, r.RequestID, digest, "", nil, func(_ *Store, _ durable.BuildAdmission, b durable.QueryRuntimeBinding, exists bool, now time.Time) (durable.QueryRuntimeBinding, error) {
		if !exists {
			return b, durable.ErrNotFound
		}
		return durable.FinishQueryRemoval(b, r, now)
	})
}
func (m *Store) CheckQueryRuntimeRemoval(ctx context.Context, f durable.QueryRemovalFence) (durable.QueryRemovalFacts, error) {
	var result durable.QueryRemovalFacts
	if checkErr := f.Candidate.Validate(); checkErr != nil {
		return result, checkErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if checkErr := ctx.Err(); checkErr != nil {
		return result, checkErr
	}
	if checkErr := m.checkLifecycleTarget(f.Candidate.NamespaceTarget); checkErr != nil {
		return result, checkErr
	}
	candidate, ok := m.queryRuntimes[queryRuntimeKey{f.Candidate.Namespace, f.Candidate.RuntimeID}]
	if !ok {
		return result, durable.ErrNotFound
	}
	build, ok := m.buildAdmissions[f.Candidate.BuildTarget]
	if !ok {
		return result, durable.ErrNotFound
	}
	retained, bindings := m.queryBindings(f.Candidate.BuildTarget)
	now := durable.Timestamp(time.Now())
	if checkErr := durable.CheckQueryRemoval(candidate, f, build, retained, bindings, now); checkErr != nil {
		return result, checkErr
	}
	return durable.QueryRemovalFacts{Fence: f, CheckedAt: now}, nil
}
