package contract

import (
	"context"
	"errors"

	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/security"
)

const DurableWardenName = "dispatchDurableOperator"

// DurableAction is a closed descriptor shared by admission and typed handlers.
func DurableAction(intent string) string {
	if action := lifecycleDurableAction(intent); action != "" {
		return action
	}
	switch intent {
	case "durable.start":
		return operator.StartWorkflow
	case "durable.signal":
		return operator.SignalWorkflow
	case "durable.signalStart":
		return operator.SignalStartWorkflow
	case "durable.cancel":
		return operator.CancelWorkflow
	case "durable.query":
		return operator.QueryWorkflow
	case "durable.capabilities":
		return operator.Discover
	case "durable.namespaces":
		return operator.Discover
	case "durable.executions":
		return operator.ListExecutions
	case "durable.execution":
		return operator.ReadExecution
	case "durable.history":
		return operator.ReadHistory
	case "durable.tasks":
		return operator.ReadTasks
	case "durable.chain", "durable.children":
		return operator.ReadChain
	case "durable.payload":
		return operator.ReadPayload
	case "durable.audit":
		return operator.ReadAudit
	case "durable.hooks":
		return operator.ReadHooks
	default:
		return ""
	}
}
func durableKind(intent string) fc.Kind {
	if kind := lifecycleDurableKind(intent); kind != "" {
		return kind
	}
	switch intent {
	case "durable.start", "durable.signal", "durable.signalStart", "durable.cancel":
		return fc.KindCommand
	default:
		if DurableAction(intent) != "" {
			return fc.KindQuery
		}
		return ""
	}
}
func durableError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, security.ErrUnauthenticated):
		return fc.ErrUnauthenticated
	case errors.Is(err, security.ErrForbidden):
		return fc.ErrPermissionDenied
	case errors.Is(err, durable.ErrInvalid):
		return badRequest("invalid durable scope, filter or continuation")
	case errors.Is(err, operator.ErrRuntimeUnavailable):
		return &fc.Error{Code: fc.CodeUnavailable, Message: "compatible runtime unavailable", Retryable: true}
	case errors.Is(err, durable.ErrWriterCompatibility), errors.Is(err, durable.ErrLifecycleBusy), errors.Is(err, durable.ErrBuildAdmission), errors.Is(err, durable.ErrRetirementBlocked), errors.Is(err, durable.ErrQueryRetention), errors.Is(err, durable.ErrQueryFence), errors.Is(err, operator.ErrBuildMismatch), errors.Is(err, operator.ErrHistoryIncompatible), errors.Is(err, durable.ErrRequestConflict), errors.Is(err, durable.ErrClosed), errors.Is(err, durable.ErrExists), errors.Is(err, durable.ErrRevisionConflict):
		return &fc.Error{Code: fc.CodeConflict, Message: err.Error()}
	case errors.Is(err, drt.ErrQueryMutation):
		return badRequest("workflow SDK mutation is forbidden in a query")
	case errors.Is(err, drt.ErrQueryNotFound):
		return notFound("query handler unavailable at this snapshot")
	case errors.Is(err, durable.ErrNotFound):
		return notFound("durable resource not found")
	default:
		return fc.ErrUnavailable
	}
}
func durableHandle[I, O any](deps Deps, intent string, fn func(context.Context, *operator.Service, security.Principal, I) (O, error)) func(context.Context, I, fc.Principal) (O, error) {
	return func(ctx context.Context, in I, identity fc.Principal) (O, error) {
		if writer := dashauth.ResponseWriterFromContext(ctx); writer != nil {
			writer.Header().Set("Cache-Control", "no-store")
		}
		var zero O
		if DurableAction(intent) == "" {
			_ = deps.Security.RecordDurableRead(ctx, security.Principal{}, "dispatch.unknown", "denied", "") //nolint:errcheck // Denial remains final when required audit acceptance fails.
			return zero, fc.ErrPermissionDenied
		}
		p, err := security.FromContract(identity)
		if err != nil {
			_ = deps.Security.RecordDurableRead(ctx, p, DurableAction(intent), "denied", "") //nolint:errcheck // Denial remains final when required audit acceptance fails.
			return zero, fc.ErrUnauthenticated
		}
		if deps.Durable == nil {
			return zero, fc.ErrUnavailable
		}
		out, err := fn(ctx, deps.Durable, p, in)
		return out, durableError(err)
	}
}
func durableBindings(deps Deps) []binding {
	return append([]binding{
		query("durable.capabilities", durableHandle(deps, "durable.capabilities", func(c context.Context, s *operator.Service, p security.Principal, i operator.CapabilitiesInput) (operator.Capabilities, error) {
			return s.Capabilities(c, p, i)
		})),
		command(deps, "durable.start", durableHandle(deps, "durable.start", func(c context.Context, s *operator.Service, p security.Principal, i operator.StartInput) (operator.Acceptance, error) {
			return s.Start(c, p, i)
		})),
		command(deps, "durable.signal", durableHandle(deps, "durable.signal", func(c context.Context, s *operator.Service, p security.Principal, i durable.SignalRequest) (operator.Acceptance, error) {
			return s.Signal(c, p, i)
		})),
		command(deps, "durable.signalStart", durableHandle(deps, "durable.signalStart", func(c context.Context, s *operator.Service, p security.Principal, i operator.SignalStartInput) (operator.Acceptance, error) {
			return s.SignalStart(c, p, i)
		})),
		command(deps, "durable.cancel", durableHandle(deps, "durable.cancel", func(c context.Context, s *operator.Service, p security.Principal, i durable.CancelExecutionRequest) (operator.Acceptance, error) {
			return s.Cancel(c, p, i)
		})),
		query("durable.query", durableHandle(deps, "durable.query", func(c context.Context, s *operator.Service, p security.Principal, i drt.QueryRequest) (operator.QueryResult, error) {
			return s.Query(c, p, i)
		})),
		query("durable.namespaces", durableHandle(deps, "durable.namespaces", func(c context.Context, s *operator.Service, p security.Principal, i operator.NamespaceInput) (operator.Page[operator.Namespace], error) {
			return s.Namespaces(c, p, i)
		})),
		query("durable.executions", durableHandle(deps, "durable.executions", func(c context.Context, s *operator.Service, p security.Principal, i durable.ExecutionList) (operator.Page[operator.Execution], error) {
			return s.Executions(c, p, i)
		})),
		query("durable.execution", durableHandle(deps, "durable.execution", func(c context.Context, s *operator.Service, p security.Principal, i durable.Key) (operator.Detail, error) {
			return s.Detail(c, p, i)
		})),
		query("durable.history", durableHandle(deps, "durable.history", func(c context.Context, s *operator.Service, p security.Principal, i operator.RunInput) (operator.Page[operator.Event], error) {
			return s.History(c, p, i)
		})),
		query("durable.tasks", durableHandle(deps, "durable.tasks", func(c context.Context, s *operator.Service, p security.Principal, i durable.TaskList) (operator.Page[operator.Task], error) {
			return s.Tasks(c, p, i)
		})),
		query("durable.chain", durableHandle(deps, "durable.chain", func(c context.Context, s *operator.Service, p security.Principal, i operator.RunInput) (operator.Chain, error) {
			return s.Chain(c, p, i)
		})),
		query("durable.children", durableHandle(deps, "durable.children", func(c context.Context, s *operator.Service, p security.Principal, i operator.RunInput) (operator.Children, error) {
			return s.Children(c, p, i)
		})),
		query("durable.payload", durableHandle(deps, "durable.payload", func(c context.Context, s *operator.Service, p security.Principal, i durable.Key) (operator.Payload, error) {
			return s.Payload(c, p, i)
		})),
		query("durable.audit", deliveryBinding(deps, "durable.audit", durable.DestinationChronicle)),
		query("durable.hooks", deliveryBinding(deps, "durable.hooks", durable.DestinationRelay)),
	}, lifecycleDurableBindings(deps)...)
}
func deliveryBinding(deps Deps, intent string, destination durable.Destination) func(context.Context, operator.RunInput, fc.Principal) (operator.Deliveries, error) {
	return durableHandle(deps, intent, func(c context.Context, s *operator.Service, p security.Principal, i operator.RunInput) (operator.Deliveries, error) {
		return s.Deliveries(c, p, operator.DeliveryInput{Key: i.Key, Destination: destination, Cursor: i.Cursor, Limit: i.Limit})
	})
}

type durableWarden struct{ deps Deps }

func (w durableWarden) Authorize(ctx context.Context, p fc.Principal, a fc.Action) (fc.Decision, error) {
	principal, err := security.FromContract(p)
	if a.Contributor != ContributorName || a.Kind != durableKind(a.Intent) || DurableAction(a.Intent) == "" {
		_ = w.deps.Security.RecordDurableRead(ctx, principal, "dispatch.unknown", "denied", "") //nolint:errcheck // Denial remains final when required audit acceptance fails.
		return fc.Decision{}, fc.ErrPermissionDenied
	}
	if err != nil {
		_ = w.deps.Security.RecordDurableRead(ctx, principal, DurableAction(a.Intent), "denied", "") //nolint:errcheck // Denial remains final when required audit acceptance fails.
		return fc.Decision{}, fc.ErrUnauthenticated
	}
	// Admission knows identity and descriptor only. Decoded payload targets are
	// authorized in the shared service, including direct dispatcher calls.
	return fc.Decision{Allow: true}, nil
}
