package memory

import (
	"context"
	"time"

	"github.com/xraph/dispatch/durable"
)

var _ durable.LifecycleStore = (*Store)(nil)

func (m *Store) RegisterBuild(ctx context.Context, r durable.RegisterBuildRequest) (durable.LifecycleReceipt, error) {
	if r.Identity != nil {
		identity := *r.Identity
		r.Identity = &identity
	}
	if err := r.Validate(); err != nil {
		return durable.LifecycleReceipt{}, err
	}
	digest, err := durable.Fingerprint(string(durable.OperationRegisterBuild), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.LifecycleReceipt, error) {
		key := lifecycleReceiptKey{r.Namespace, durable.OperationRegisterBuild, r.RequestID}
		if saved, ok := c.lifecycleReceipts[key]; ok {
			return saved.Clone(), saved.Match(durable.LifecycleReceiptLookup{NamespaceTarget: r.NamespaceTarget, Operation: key.operation, RequestID: r.RequestID, RequestDigest: digest})
		}
		if err := c.checkLifecycleTarget(r.NamespaceTarget); err != nil {
			return durable.LifecycleReceipt{}, err
		}
		prior, exists := c.buildAdmissions[r.BuildTarget]
		build, registerErr := durable.RegisterBuildIdentity(prior, exists, r, durable.Timestamp(time.Now()))
		if registerErr != nil {
			return durable.LifecycleReceipt{}, registerErr
		}
		c.buildAdmissions[r.BuildTarget] = build
		return c.saveBuildReceipt(ctx, key, digest, r.CommandDigest, build)
	})
}
func (m *Store) checkLifecycleTarget(t durable.NamespaceTarget) error {
	if n, ok := m.namespaces[t.Namespace]; !ok || n.InstallationID != t.InstallationID {
		return durable.ErrNotFound
	}
	if !m.retirementNamespaces[t.Namespace].Enrolled {
		return durable.ErrWriterCompatibility
	}
	return nil
}
func (m *Store) saveBuildReceipt(ctx context.Context, key lifecycleReceiptKey, digest, command string, build durable.BuildAdmission) (durable.LifecycleReceipt, error) {
	r := durable.LifecycleReceipt{NamespaceTarget: build.NamespaceTarget, Operation: key.operation, RequestID: key.requestID, RequestDigest: digest, CommandDigest: command, ResponseVersion: 1, AcceptedAt: build.ChangedAt, Build: &build}
	delivery, err := durable.NewDelivery(m.namespaces[key.namespace], durable.DestinationChronicle, durable.LifecycleDeliverySource(ctx, r))
	if err != nil {
		return r, err
	}
	r.DeliveryID = delivery.ID
	m.lifecycleReceipts[key] = r
	return r.Clone(), nil
}
func (m *Store) InspectBuildLifecycle(ctx context.Context, target durable.BuildTarget) (durable.BuildLifecycleFacts, error) {
	if err := target.Validate(); err != nil {
		return durable.BuildLifecycleFacts{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return durable.BuildLifecycleFacts{}, err
	}
	if err := m.checkLifecycleTarget(target.NamespaceTarget); err != nil {
		return durable.BuildLifecycleFacts{}, err
	}
	return m.buildLifecycleFacts(target, durable.Timestamp(time.Now()))
}
func (m *Store) buildLifecycleFacts(target durable.BuildTarget, now time.Time) (durable.BuildLifecycleFacts, error) {
	build, ok := m.buildAdmissions[target]
	if !ok {
		return durable.BuildLifecycleFacts{}, durable.ErrNotFound
	}
	f := durable.BuildLifecycleFacts{Admission: build, ObservationVersion: durable.ObservationVersion{CompatibilityVersion: m.retirementNamespaces[target.Namespace].Version, BuildVersion: build.Version, ObservedAt: now}}
	for key, r := range m.executions {
		if key.Namespace != target.Namespace || r.execution.BuildID != target.BuildID {
			continue
		}
		if r.execution.State == durable.StateRunning {
			f.Blockers.OpenExecutions++
			if r.execution.RunAvailableAt.After(now) {
				f.Blockers.DelayedRuns++
			}
		}
		for _, task := range r.tasks {
			if !task.Done {
				f.Blockers.PendingTasks++
				if task.LeaseKind == durable.LeaseAsync {
					f.Blockers.AsyncCallbacks++
				}
			}
		}
	}
	for key, d := range m.childDeliveries {
		if key.source.Namespace != target.Namespace || d.Done {
			continue
		}
		buildID := d.TargetBuildID
		if d.Kind == durable.ChildDeliveryClose || d.Kind == durable.ChildDeliveryCancel {
			if current := m.executions[m.currentChildKey(d.Target)]; current != nil {
				buildID = current.execution.BuildID
			}
		}
		if buildID == target.BuildID {
			f.Blockers.PendingChildDeliveries++
		}
		if d.Kind == durable.ChildDeliveryCancel && d.Message.CancellationID != "" {
			parent := m.executions[d.Source]
			if parent == nil {
				return f, durable.ErrInvalid
			}
			if parent.execution.BuildID == target.BuildID {
				f.Blockers.ChildObligations++
			}
		}
	}
	for child, link := range m.childParents {
		if child.Namespace != target.Namespace {
			continue
		}
		p, c := m.executions[link.Parent], m.executions[m.currentChildKey(child)]
		if p == nil || c == nil {
			return f, durable.ErrInvalid
		}
		if p.execution.BuildID == target.BuildID && c.execution.State == durable.StateRunning {
			f.Blockers.ChildObligations++
		}
	}
	retained, bindings := m.queryBindings(target)
	now = durable.Timestamp(time.Now())
	f.ObservationVersion.ObservedAt = now
	f.QueryRetention = durable.QueryRetention(build, retained, bindings, now)
	return f, nil
}
func (m *Store) BeginBuildRetirement(ctx context.Context, r durable.BuildRetirementRequest) (durable.LifecycleReceipt, error) {
	return m.mutateBuildLifecycle(ctx, r, durable.OperationBeginRetirement)
}
func (m *Store) FinalizeBuildRetirement(ctx context.Context, r durable.BuildRetirementRequest) (durable.LifecycleReceipt, error) {
	return m.mutateBuildLifecycle(ctx, r, durable.OperationFinalizeRetirement)
}
func (m *Store) AbortBuildRetirement(ctx context.Context, r durable.BuildRetirementRequest) (durable.LifecycleReceipt, error) {
	return m.mutateBuildLifecycle(ctx, r, durable.OperationAbortRetirement)
}
func (m *Store) mutateBuildLifecycle(ctx context.Context, r durable.BuildRetirementRequest, operation durable.LifecycleOperation) (durable.LifecycleReceipt, error) {
	if err := r.Validate(); err != nil {
		return durable.LifecycleReceipt{}, err
	}
	digest, err := durable.Fingerprint(string(operation), r)
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	return mutateAudited(ctx, m, r.Namespace, func(c *Store) (durable.LifecycleReceipt, error) {
		key := lifecycleReceiptKey{r.Namespace, operation, r.RequestID}
		if saved, ok := c.lifecycleReceipts[key]; ok {
			return saved.Clone(), saved.Match(durable.LifecycleReceiptLookup{NamespaceTarget: r.NamespaceTarget, Operation: operation, RequestID: r.RequestID, RequestDigest: digest})
		}
		if err := c.checkLifecycleTarget(r.NamespaceTarget); err != nil {
			return durable.LifecycleReceipt{}, err
		}
		now := durable.Timestamp(time.Now())
		f, readErr := c.buildLifecycleFacts(r.BuildTarget, now)
		if readErr != nil {
			return durable.LifecycleReceipt{}, readErr
		}
		if operation == durable.OperationFinalizeRetirement && f.Blockers.Empty() && f.QueryRetention.RetainedExecutions > 0 && f.QueryRetention.VerifiedBindings == 0 {
			return durable.LifecycleReceipt{}, durable.ErrQueryRetention
		}
		build, transitionErr := durable.TransitionBuild(f.Admission, r, operation, f.Blockers, now)
		if transitionErr != nil {
			return durable.LifecycleReceipt{}, transitionErr
		}
		c.buildAdmissions[r.BuildTarget] = build
		return c.saveBuildReceipt(ctx, key, digest, "", build)
	})
}
func (m *Store) admitExecution(execution, source *durable.Execution, kind, command string) error {
	n, active := m.retirementNamespaces[execution.Namespace]
	if !active {
		return nil
	}
	target := durable.BuildTarget{NamespaceTarget: n.NamespaceTarget, BuildID: execution.BuildID}
	build, ok := m.buildAdmissions[target]
	if !ok {
		build = durable.BuildAdmission{BuildTarget: target, State: "unregistered"}
	}
	epoch, err := durable.AdmissionEpoch(build, source, kind, command)
	if err != nil {
		return err
	}
	execution.AdmissionEpoch = epoch
	return nil
}
