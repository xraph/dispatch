package memory

import (
	"context"
	"sort"
	"time"

	"github.com/xraph/dispatch/durable"
)

var _ durable.RetirementEnrollmentStore = (*Store)(nil)

type lifecycleReceiptKey struct {
	namespace string
	operation durable.LifecycleOperation
	requestID string
}

func (m *Store) InspectCompatibility(ctx context.Context, target durable.NamespaceTarget) (durable.CompatibilityFacts, error) {
	if err := target.Validate(); err != nil {
		return durable.CompatibilityFacts{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.CompatibilityFacts{}, err
	}
	if n, ok := m.namespaces[target.Namespace]; !ok || n.InstallationID != target.InstallationID {
		return durable.CompatibilityFacts{}, durable.ErrNotFound
	}
	f := m.retirementNamespaces[target.Namespace]
	f.NamespaceTarget = target
	f.ObservedAt = durable.Timestamp(time.Now())
	return f, nil
}

func (m *Store) LookupLifecycleReceipt(ctx context.Context, q durable.LifecycleReceiptLookup) (durable.LifecycleReceipt, error) {
	if err := q.Validate(); err != nil {
		return durable.LifecycleReceipt{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.LifecycleReceipt{}, err
	}
	r, ok := m.lifecycleReceipts[lifecycleReceiptKey{q.Namespace, q.Operation, q.RequestID}]
	if !ok || r.InstallationID != q.InstallationID {
		return durable.LifecycleReceipt{}, durable.ErrNotFound
	}
	return r.Clone(), r.Match(q)
}

func (m *Store) EnrollRetirement(ctx context.Context, r durable.RetirementEnrollmentRequest) (durable.LifecycleReceipt, error) {
	if err := r.Validate(); err != nil {
		return durable.LifecycleReceipt{}, err
	}
	digest, err := durable.Fingerprint(string(durable.OperationEnrollRetirement), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.LifecycleReceipt, error) {
		n, ok := c.namespaces[r.Namespace]
		if !ok || n.InstallationID != r.InstallationID {
			return durable.LifecycleReceipt{}, durable.ErrNotFound
		}
		key := lifecycleReceiptKey{r.Namespace, durable.OperationEnrollRetirement, r.RequestID}
		if saved, found := c.lifecycleReceipts[key]; found {
			return saved.Clone(), saved.Match(durable.LifecycleReceiptLookup{NamespaceTarget: r.NamespaceTarget, Operation: key.operation, RequestID: r.RequestID, RequestDigest: digest})
		}
		if !n.RequireAudit {
			return durable.LifecycleReceipt{}, durable.ErrInvalid
		}
		if c.retirementNamespaces[r.Namespace].Enrolled {
			return durable.LifecycleReceipt{}, durable.ErrRequestConflict
		}
		builds, inventoryErr := c.historicalBuilds(ctx, r.Namespace)
		if inventoryErr != nil {
			return durable.LifecycleReceipt{}, inventoryErr
		}
		now := durable.Timestamp(time.Now())
		for _, build := range builds {
			target := durable.BuildTarget{NamespaceTarget: r.NamespaceTarget, BuildID: build}
			c.buildAdmissions[target] = durable.BuildAdmission{BuildTarget: target, State: "accepting", Epoch: 1, Version: 1, ChangedAt: now}
		}
		facts := durable.CompatibilityFacts{NamespaceTarget: r.NamespaceTarget, Enrolled: true, SchemaVersion: 1, WriterProtocol: 1, Version: 1, EnrolledAt: now, ObservedAt: now}
		c.retirementNamespaces[r.Namespace] = facts
		setDigest, digestErr := durable.Fingerprint("retirement-historical-builds.v1", builds)
		if digestErr != nil {
			return durable.LifecycleReceipt{}, digestErr
		}
		receipt := durable.LifecycleReceipt{NamespaceTarget: r.NamespaceTarget, Operation: key.operation, RequestID: r.RequestID, RequestDigest: digest, ResponseVersion: 1, AcceptedAt: now, Enrollment: &durable.RetirementEnrollment{Compatibility: facts, HistoricalBuildCount: int64(len(builds)), HistoricalBuildDigest: setDigest}}
		delivery, deliveryErr := durable.NewDelivery(n, durable.DestinationChronicle, durable.LifecycleDeliverySource(ctx, receipt))
		if deliveryErr != nil {
			return durable.LifecycleReceipt{}, deliveryErr
		}
		receipt.DeliveryID = delivery.ID
		c.lifecycleReceipts[key] = receipt
		return receipt.Clone(), nil
	})
}

func (m *Store) historicalBuilds(ctx context.Context, namespace string) ([]string, error) {
	set := map[string]bool{}
	for key, r := range m.executions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if key.Namespace != namespace {
			continue
		}
		if durable.ValidateBuildID(r.execution.BuildID) != nil {
			return nil, durable.ErrInvalid
		}
		first := key
		first.RunID = r.execution.FirstRunID
		if m.executions[first] == nil {
			return nil, durable.ErrInvalid
		}
		if r.execution.PreviousRunID != "" {
			previous := key
			previous.RunID = r.execution.PreviousRunID
			p := m.executions[previous]
			if p == nil || p.execution.NextRunID != key.RunID || p.execution.RunNumber != r.execution.RunNumber-1 {
				return nil, durable.ErrInvalid
			}
		}
		if r.execution.NextRunID != "" {
			next := key
			next.RunID = r.execution.NextRunID
			n := m.executions[next]
			if n == nil || n.execution.PreviousRunID != key.RunID || n.execution.RunNumber != r.execution.RunNumber+1 {
				return nil, durable.ErrInvalid
			}
		}
		set[r.execution.BuildID] = true
		for _, task := range r.tasks {
			if task.Key != key {
				return nil, durable.ErrInvalid
			}
		}
	}
	for key, c := range m.childParents {
		if key.Namespace != namespace {
			continue
		}
		p, e := m.executions[c.Parent], m.executions[key]
		if p == nil || e == nil || c.Start.Key != key || c.Start.BuildID != e.execution.BuildID || c.Start.WorkflowType != e.execution.WorkflowType || c.Validate(c.Parent) != nil {
			return nil, durable.ErrInvalid
		}
	}
	for key, d := range m.childDeliveries {
		if key.source.Namespace != namespace {
			continue
		}
		if m.executions[d.Source] == nil || m.executions[d.Target] == nil || m.executions[d.Target].execution.BuildID != d.TargetBuildID || durable.ValidateBuildID(d.TargetBuildID) != nil {
			return nil, durable.ErrInvalid
		}
		set[d.TargetBuildID] = true
	}
	result := make([]string, 0, len(set))
	for build := range set {
		result = append(result, build)
	}
	sort.Strings(result)
	return result, nil
}
