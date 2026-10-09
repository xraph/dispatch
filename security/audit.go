package security

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/xraph/dispatch/durable"
)

// AuditService is a shared activation handle. Route copies keep this pointer;
// Activate verifies its binding after migrations. The extension enables remote
// admission only after the engine starts successfully.
// A zero or inactive handle fails closed.
type AuditService struct {
	mu       sync.RWMutex
	binding  *auditBinding
	failures atomic.Uint64
}
type auditBinding struct {
	store     durable.OutboxStore
	catalog   durable.NamespaceStore
	resource  Resource
	namespace string
	fallback  bool
}

// Activate verifies the persisted host binding; caller input never selects it.
func (a *AuditService) Activate(ctx context.Context, store durable.OutboxStore, catalog durable.NamespaceStore, resource Resource, namespace string, allowNamespaceFallback bool) error {
	if a == nil || store == nil || catalog == nil {
		return ErrUnavailable
	}
	n, err := catalog.GetNamespace(ctx, resource.InstallationID, namespace)
	if err != nil || n.InstallationID != resource.InstallationID || n.TenantID != resource.PolicyTenant || !n.RequireAudit {
		return ErrUnavailable
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.binding != nil {
		return errors.New("dispatch: audit already active")
	}
	a.binding = &auditBinding{store: store, catalog: catalog, resource: resource, namespace: namespace, fallback: allowNamespaceFallback}
	return nil
}
func (a *AuditService) Deactivate() {
	if a != nil {
		a.mu.Lock()
		a.binding = nil
		a.mu.Unlock()
	}
}
func (a *AuditService) bound(resource Resource) *auditBinding {
	if a == nil {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.binding == nil || a.binding.resource != resource {
		return nil
	}
	return a.binding
}

// Metadata retains the verified machine actor kind. It never copies raw claims.
func Metadata(p Principal) durable.AuditMetadata {
	if p.Validate() != nil {
		return durable.AuditMetadata{ActorKind: "anonymous"}
	}
	return durable.AuditMetadata{ActorKind: p.Kind, ActorID: p.Subject}
}

func (b Boundary) audit(ctx context.Context, p Principal, op Operation, outcome, namespace string) (auditErr error) {
	defer func() {
		if auditErr != nil && b.Audit != nil {
			b.Audit.failures.Add(1)
		}
	}()
	binding := b.Audit.bound(b.Resource)
	if binding == nil {
		return ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, CheckTimeout)
	defer cancel()
	auditNamespace := binding.namespace
	if namespace != "" && outcome == "allowed" {
		record, err := binding.catalog.GetNamespace(ctx, b.Resource.InstallationID, namespace)
		if err == nil && record.InstallationID == b.Resource.InstallationID && record.TenantID == b.Resource.PolicyTenant && record.RequireAudit {
			auditNamespace = record.Namespace
		} else if !binding.fallback {
			return ErrUnavailable
		}
	}
	action := op.auditAction()
	if action == "" {
		action = "dispatch.unknown"
	}
	target := op.Target
	if target == "" {
		target = namespace
	}
	if target != "" && !durable.DeliveryIdentifier(target) {
		target = "invalid-target"
	}
	audit, err := durable.CaptureSecurityAudit(b.Resource.InstallationID, auditNamespace, action, outcome, target, Metadata(p))
	if err == nil {
		_, err = binding.store.AppendSecurityAudit(ctx, audit)
	}
	if err != nil {
		return ErrUnavailable
	}
	return nil
}

// AuthenticationDenied records anonymous admission without retaining credentials
// or provider errors. Audit failure never changes the denial into authorization.
func (b Boundary) AuthenticationDenied(ctx context.Context, op Operation) error {
	if err := b.audit(ctx, Principal{}, op, "unauthenticated", ""); err != nil {
		return errors.Join(ErrUnauthenticated, err)
	}
	return ErrUnauthenticated
}

// CommandAttempt identifies durable evidence accepted before legacy execution.
type CommandAttempt struct{ Record durable.LegacyAttempt }

// OutcomeUnconfirmedError means execution may have succeeded. Retrying the command
// is unsafe; reconcile the retained attempt instead.
type OutcomeUnconfirmedError struct{ AttemptID string }

func (e *OutcomeUnconfirmedError) Error() string {
	return "dispatch: command outcome unconfirmed; attempt " + e.AttemptID
}
func (e *OutcomeUnconfirmedError) Unwrap() error { return ErrUnavailable }

func (b Boundary) BeginCommand(ctx context.Context, p Principal, op Operation) (attempt CommandAttempt, auditErr error) {
	defer func() {
		if auditErr != nil && b.Audit != nil {
			b.Audit.failures.Add(1)
		}
	}()
	if !op.Mutating() {
		return CommandAttempt{}, nil
	}
	binding := b.Audit.bound(b.Resource)
	if binding == nil {
		return CommandAttempt{}, ErrUnavailable
	}
	store, ok := binding.store.(durable.LegacyAuditStore)
	if !ok {
		return CommandAttempt{}, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, CheckTimeout)
	defer cancel()
	audit, err := durable.CaptureSecurityAudit(b.Resource.InstallationID, binding.namespace, op.auditAction(), "attempted", op.Target, Metadata(p))
	if err != nil {
		return CommandAttempt{}, ErrUnavailable
	}
	record, err := store.BeginLegacyAttempt(ctx, audit)
	if err != nil {
		return CommandAttempt{}, ErrUnavailable
	}
	return CommandAttempt{Record: record}, nil
}

// CaptureOutcome allocates one immutable outcome for safe persistence retries.
func (a CommandAttempt) CaptureOutcome(success bool) (durable.LegacyOutcome, error) {
	outcome := "returned_error"
	if success {
		outcome = "returned_success"
	}
	d := a.Record.Attempt
	audit, err := durable.CaptureSecurityAudit(d.InstallationID, d.Namespace, d.Action, outcome, d.Target, d.Metadata)
	return durable.LegacyOutcome{AttemptID: d.ID, Audit: audit}, err
}
func (b Boundary) AcceptOutcome(ctx context.Context, o durable.LegacyOutcome) error {
	binding := b.Audit.bound(b.Resource)
	if binding != nil && o.Audit.InstallationID == b.Resource.InstallationID && o.Audit.Namespace == binding.namespace {
		if store, ok := binding.store.(durable.LegacyAuditStore); ok {
			auditCtx, cancel := context.WithTimeout(ctx, CheckTimeout)
			defer cancel()
			if err := store.CompleteLegacyAttempt(auditCtx, o); err == nil {
				return nil
			}
		}
	}
	if b.Audit != nil {
		b.Audit.failures.Add(1)
	}
	return &OutcomeUnconfirmedError{AttemptID: o.AttemptID}
}
func (b Boundary) FinishCommand(ctx context.Context, a CommandAttempt, success bool) error {
	if a.Record.Attempt.ID == "" {
		return nil
	}
	o, err := a.CaptureOutcome(success)
	if err != nil {
		return &OutcomeUnconfirmedError{AttemptID: a.Record.Attempt.ID}
	}
	// The mutation can finish at the request deadline. Give its separate outcome
	// acceptance a bounded chance without inheriting an already canceled request.
	return b.AcceptOutcome(context.WithoutCancel(ctx), o)
}
func (op Operation) Mutating() bool { return op.Action == OperatorWrite || op.Action == Federation }

// AcceptanceFailures counts local audit acceptance failures, without provider
// text or recursive audit events. Persisted unresolved attempts survive restart.
func (a *AuditService) AcceptanceFailures() uint64 {
	if a == nil {
		return 0
	}
	return a.failures.Load()
}

func (op Operation) auditAction() string {
	if op.AuditAction != "" {
		return op.AuditAction
	}
	return op.Action
}

// RecordDurableRead accepts denial and sensitive-read audit before protected data
// leaves the operator service. Successful scope comes from the persisted catalog.
func (b Boundary) RecordDurableRead(ctx context.Context, p Principal, action, outcome, namespace string, targets ...durable.Key) error {
	binding := b.Audit.bound(b.Resource)
	if binding == nil {
		return ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, CheckTimeout)
	defer cancel()
	auditNamespace := binding.namespace
	if outcome == "allowed" {
		n, err := binding.catalog.GetNamespace(ctx, b.Resource.InstallationID, namespace)
		if err != nil || !n.RequireAudit {
			return ErrUnavailable
		}
		auditNamespace = n.Namespace
	}
	record, err := durable.CaptureSecurityAudit(b.Resource.InstallationID, auditNamespace, action, outcome, namespace, Metadata(p))
	if len(targets) > 1 {
		return ErrUnavailable
	}
	if len(targets) == 1 {
		key := targets[0]
		if key.Validate() != nil || key.Namespace != auditNamespace {
			return ErrUnavailable
		}
		record.WorkflowID = key.WorkflowID
		record.RunID = key.RunID
	}
	if err == nil {
		_, err = binding.store.AppendSecurityAudit(ctx, record)
	}
	if err != nil {
		b.Audit.failures.Add(1)
		return ErrUnavailable
	}
	return nil
}

// DurableReadsReady preserves the extension's post-start admission barrier.
func (b Boundary) DurableReadsReady() bool { return b.Audit.bound(b.Resource) != nil }
