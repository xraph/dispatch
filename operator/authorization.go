// Package operator exposes authorized, payload-safe durable execution reads.
package operator

import (
	"context"
	"errors"

	"github.com/xraph/warden"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

const (
	Discover       = "dispatch.namespace.discover"
	ListExecutions = "dispatch.execution.list"
	ReadExecution  = "dispatch.execution.read"
	ReadHistory    = "dispatch.history.read"
	ReadTasks      = "dispatch.task.read"
	ReadChain      = "dispatch.chain.read"
	ReadPayload    = "dispatch.payload.read"
	QueryWorkflow  = "dispatch.workflow.query"
	ReadAudit      = "dispatch.audit.read"
	ReadHooks      = "dispatch.hook.read"
)

func KnownAction(action string) bool {
	switch action {
	case Discover, ListExecutions, ReadExecution, ReadHistory, ReadTasks, ReadChain, ReadPayload, QueryWorkflow, ReadAudit, ReadHooks:
		return true
	default:
		return false
	}
}

type Resource struct{ InstallationID, Namespace, AppID, TenantID, WorkflowID, RunID string }
type Authorizer interface {
	Authorize(context.Context, security.Principal, string, Resource) error
}
type AuthorizerFunc func(context.Context, security.Principal, string, Resource) error

func (f AuthorizerFunc) Authorize(c context.Context, p security.Principal, a string, r Resource) error {
	return f(c, p, a, r)
}

type WardenAuthorizer struct {
	Engine func() (*warden.Engine, error)
}

func (a *WardenAuthorizer) Authorize(ctx context.Context, p security.Principal, action string, r Resource) error {
	if p.Validate() != nil {
		return security.ErrUnauthenticated
	}
	if !KnownAction(action) || !durable.DeliveryIdentifier(r.Namespace) {
		return security.ErrForbidden
	}
	if a == nil || a.Engine == nil {
		return security.ErrUnavailable
	}
	engine, err := a.Engine()
	if err != nil || engine == nil {
		return security.ErrUnavailable
	}
	ctx = warden.WithNamespace(warden.WithTenant(ctx, r.AppID, r.TenantID), r.Namespace)
	result, err := engine.Check(ctx, &warden.CheckRequest{TenantID: r.TenantID, Subject: warden.Subject{Kind: warden.SubjectKind(p.Kind), ID: p.Subject, Attributes: map[string]any{"principal_kind": p.Kind}}, Action: warden.Action{Name: action}, Resource: warden.Resource{Type: "dispatch_namespace", ID: r.Namespace, Attributes: map[string]any{"installation_id": r.InstallationID, "namespace": r.Namespace, "app_id": r.AppID, "tenant_id": r.TenantID, "workflow_id": r.WorkflowID, "run_id": r.RunID}}})
	if err != nil {
		return security.ErrUnavailable
	}
	if result == nil || !result.Allowed || len(result.Obligations) > 0 {
		return security.ErrForbidden
	}
	return nil
}
func (s *Service) check(ctx context.Context, p security.Principal, action string, key durable.Key) error {
	if err := p.Validate(); err != nil {
		return s.denied(ctx, p, action, err)
	}
	if !KnownAction(action) || !durable.DeliveryIdentifier(key.Namespace) {
		return s.denied(ctx, p, action, security.ErrForbidden)
	}
	if !s.audit.DurableReadsReady() {
		return security.ErrUnavailable
	}
	n, err := s.catalog.GetNamespace(ctx, s.installation, key.Namespace)
	if err != nil {
		if errors.Is(err, durable.ErrNotFound) || errors.Is(err, durable.ErrInvalid) {
			err = security.ErrForbidden
		} else {
			err = security.ErrUnavailable
		}
		return s.denied(ctx, p, action, err)
	}
	err = s.authorizer.Authorize(ctx, p, action, Resource{InstallationID: n.InstallationID, Namespace: n.Namespace, AppID: n.AppID, TenantID: n.TenantID, WorkflowID: key.WorkflowID, RunID: key.RunID})
	if err != nil {
		if !errors.Is(err, security.ErrForbidden) {
			err = security.ErrUnavailable
		}
		return s.denied(ctx, p, action, err)
	}
	return nil
}
func (s *Service) denied(ctx context.Context, p security.Principal, action string, cause error) error {
	if !KnownAction(action) {
		action = "dispatch.unknown"
	}
	if err := s.audit.RecordDurableRead(ctx, p, action, "denied", ""); err != nil {
		return errors.Join(cause, security.ErrUnavailable)
	}
	return cause
}
