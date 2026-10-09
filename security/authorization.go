// Package security defines the authorization boundary for remote Dispatch operators.
package security

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xraph/dispatch/durable"
)

var (
	ErrUnauthenticated = errors.New("dispatch: authentication required")
	ErrForbidden       = errors.New("dispatch: access denied")
	ErrUnavailable     = errors.New("dispatch: authorization unavailable")
)

const CheckTimeout = 5 * time.Second
const StreamLifetime = 5 * time.Minute

// Principal is a verified server identity. Request filters never establish authority.
type Principal struct{ Subject, Kind string }

func (p Principal) Validate() error {
	if !durable.DeliveryIdentifier(p.Subject) {
		return ErrUnauthenticated
	}
	switch p.Kind {
	case "user", "service", "api_key", "service_acct":
		return nil
	default:
		return ErrUnauthenticated
	}
}

type Resource struct{ InstallationID, PolicyTenant string }
type Authorizer interface {
	Authorize(context.Context, Principal, string, Resource) error
}
type AuthorizerFunc func(context.Context, Principal, string, Resource) error

func (f AuthorizerFunc) Authorize(ctx context.Context, p Principal, a string, r Resource) error {
	return f(ctx, p, a, r)
}

type Authenticator interface {
	Authenticate(context.Context, *http.Request) (Principal, error)
}
type AuthenticatorFunc func(context.Context, *http.Request) (Principal, error)

func (f AuthenticatorFunc) Authenticate(ctx context.Context, r *http.Request) (Principal, error) {
	return f(ctx, r)
}

// Boundary always denies incomplete configuration and bounds policy work.
type Boundary struct {
	Resource   Resource
	Authorizer Authorizer
	Audit      *AuditService
}

func (b Boundary) Check(ctx context.Context, p Principal, op Operation) error {
	return b.CheckNamespace(ctx, p, op, "")
}

// CheckNamespace uses an already resolved authorized namespace for successful
// read auditing. Unresolved and denied attempts always use the host binding.
func (b Boundary) CheckNamespace(ctx context.Context, p Principal, op Operation, namespace string) error {
	err := b.authorize(ctx, p, op)
	outcome := "allowed"
	switch {
	case errors.Is(err, ErrUnauthenticated):
		outcome = "unauthenticated"
	case errors.Is(err, ErrForbidden):
		outcome = "denied"
	case err != nil:
		outcome = "unavailable"
	}
	auditErr := b.audit(ctx, p, op, outcome, namespace)
	if err != nil {
		return err
	}
	return auditErr
}
func (b Boundary) authorize(ctx context.Context, p Principal, op Operation) error {
	if err := p.Validate(); err != nil {
		return err
	}
	switch op.Action {
	case OperatorRead, OperatorWrite, Subscribe, Federation:
	default:
		return ErrForbidden
	}
	if b.Authorizer == nil || strings.TrimSpace(b.Resource.InstallationID) == "" || strings.TrimSpace(b.Resource.PolicyTenant) == "" {
		return ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, CheckTimeout)
	defer cancel()
	actions := []string{op.Action}
	if op.Payload {
		actions = append(actions, PayloadRead)
	}
	for _, action := range actions {
		err := b.Authorizer.Authorize(ctx, p, action, b.Resource)
		if err != nil {
			if errors.Is(err, ErrForbidden) {
				return ErrForbidden
			}
			return ErrUnavailable
		}
	}
	return nil
}

const (
	OperatorRead  = "dispatch.installation.read"
	OperatorWrite = "dispatch.installation.write"
	PayloadRead   = "dispatch.installation.payload.read"
	Subscribe     = "dispatch.installation.subscribe"
	Federation    = "dispatch.installation.federation"
)

type Operation struct {
	Action  string
	Payload bool
}

// RESTOperation uses the registered method and template, never a request label.
func RESTOperation(method, path string) Operation {
	key := method + " " + path
	switch key {
	case "GET /jobs", "GET /jobs/:jobId", "GET /workflows/runs", "GET /workflows/runs/:runId", "GET /workflows/runs/:runId/replay", "GET /dlq", "GET /dlq/:entryId", "GET /crons", "GET /crons/:cronId":
		return Operation{OperatorRead, true}
	case "GET /jobs/counts", "GET /workflows", "GET /dlq/count", "GET /stats":
		return Operation{Action: OperatorRead}
	case "POST /jobs/:jobId/cancel", "POST /jobs/:jobId/retry", "DELETE /dlq/:entryId", "POST /dlq/replay-all", "POST /dlq/purge", "DELETE /crons/:cronId":
		return Operation{Action: OperatorWrite}
	case "POST /workflows/runs/:runId/replay", "POST /dlq/:entryId/replay", "POST /crons/:cronId/enable", "POST /crons/:cronId/disable", "POST /crons/:cronId/trigger":
		return Operation{OperatorWrite, true}
	default:
		return Operation{}
	}
}
func ContractOperation(intent string) Operation {
	switch intent {
	case "artifacts.list", "artifacts.get", "artifacts.forJob", "workers.list", "workers.get", "queues.list", "queues.get", "handlers.list", "handlers.get", "overview.summary", "jobs.counts", "dlq.counts", "dlq.purgePreview":
		return Operation{Action: OperatorRead}
	case "artifacts.presign", "engine.config", "workflows.list", "workflows.get", "workflows.replayPreview", "jobs.list", "jobs.get", "dlq.list", "dlq.get", "crons.list", "crons.get":
		return Operation{OperatorRead, true}
	case "jobs.cancel", "jobs.retry", "dlq.replayAll", "dlq.delete", "dlq.purge", "crons.delete":
		return Operation{Action: OperatorWrite}
	case "workflows.replayFrom", "dlq.replay", "crons.enable", "crons.disable", "crons.runNow":
		return Operation{OperatorWrite, true}
	default:
		return Operation{}
	}
}
func DWPOperation(method string) Operation {
	switch method {
	case "job.get", "workflow.get", "workflow.timeline":
		return Operation{OperatorRead, true}
	case "stats":
		return Operation{Action: OperatorRead}
	case "job.enqueue", "job.cancel", "workflow.start", "workflow.event":
		return Operation{Action: OperatorWrite}
	case "subscribe", "unsubscribe":
		return Operation{Subscribe, true}
	case "federation.enqueue", "federation.event", "federation.heartbeat":
		return Operation{Action: Federation}
	default:
		return Operation{}
	}
}
