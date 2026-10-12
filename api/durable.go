package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/security"
)

// WithDurableCallbacks mounts machine callback routes through the shared service.
// Supply the stock explicit-token ForgeAuthenticator configured with Authsome.
func WithDurableCallbacks(service *operator.Service, authenticator *security.ForgeAuthenticator) Option {
	return func(a *API) { a.durable = service; a.durableAuth = authenticator }
}

type CompleteDurableActivityRequest operator.CompletionInput
type HeartbeatDurableActivityRequest operator.HeartbeatInput
type durablePrincipalKey struct{}

func (a *API) registerDurableRoutes(router forge.Router) error {
	if a.durable == nil {
		return nil
	}
	group := router.Group("/v1/durable/activities", forge.WithGroupTags("durable"))
	return errors.Join(
		group.POST("/complete", a.completeDurableActivity, forge.WithMiddleware(a.durableGate(operator.CompleteActivity)), forge.WithOperationID("completeDurableActivity"), forge.WithRequestSchema(CompleteDurableActivityRequest{}), forge.WithResponseSchema(http.StatusOK, "Accepted completion", operator.Acceptance{}), forge.WithErrorResponses()),
		group.POST("/heartbeat", a.heartbeatDurableActivity, forge.WithMiddleware(a.durableGate(operator.HeartbeatActivity)), forge.WithOperationID("heartbeatDurableActivity"), forge.WithRequestSchema(HeartbeatDurableActivityRequest{}), forge.WithResponseSchema(http.StatusOK, "Accepted heartbeat", operator.Acceptance{}), forge.WithErrorResponses()),
	)
}
func (a *API) durableGate(action string) forge.Middleware {
	return func(next forge.Handler) forge.Handler {
		return func(ctx forge.Context) error {
			ctx.Response().Header().Set("Cache-Control", "no-store")
			if a.durableAuth == nil || (action != operator.CompleteActivity && action != operator.HeartbeatActivity) {
				return ctx.JSON(http.StatusUnauthorized, ErrorResponse{Error: "authentication required"})
			}
			p, err := a.durableAuth.Authenticate(ctx.Context(), ctx.Request())
			if err != nil {
				_ = a.boundary.AuthenticationDenied(ctx.Context(), security.Operation{AuditAction: action}) //nolint:errcheck // Authentication remains denied if audit is unavailable.
				return ctx.JSON(http.StatusUnauthorized, ErrorResponse{Error: "authentication required"})
			}
			ctx.Request().Body = http.MaxBytesReader(ctx.Response(), ctx.Request().Body, 2<<20)
			ctx.WithContext(context.WithValue(ctx.Context(), durablePrincipalKey{}, p))
			return next(ctx)
		}
	}
}
func (a *API) completeDurableActivity(ctx forge.Context, in *CompleteDurableActivityRequest) (*operator.Acceptance, error) {
	p, ok := ctx.Context().Value(durablePrincipalKey{}).(security.Principal)
	if !ok {
		return nil, forge.Unauthorized("authentication required")
	}
	out, err := a.durable.Complete(ctx.Context(), p, operator.CompletionInput(*in))
	if err != nil {
		return nil, durableAPIError(err)
	}
	return &out, nil
}
func (a *API) heartbeatDurableActivity(ctx forge.Context, in *HeartbeatDurableActivityRequest) (*operator.Acceptance, error) {
	p, ok := ctx.Context().Value(durablePrincipalKey{}).(security.Principal)
	if !ok {
		return nil, forge.Unauthorized("authentication required")
	}
	out, err := a.durable.Heartbeat(ctx.Context(), p, operator.HeartbeatInput(*in))
	if err != nil {
		return nil, durableAPIError(err)
	}
	return &out, nil
}
func durableAPIError(err error) error {
	switch {
	case errors.Is(err, security.ErrUnauthenticated):
		return forge.Unauthorized("authentication required")
	case errors.Is(err, security.ErrForbidden):
		return forge.Forbidden("access denied")
	case errors.Is(err, durable.ErrInvalid):
		return forge.BadRequest("invalid callback request")
	case errors.Is(err, durable.ErrNotFound):
		return forge.NotFound("durable resource not found")
	case errors.Is(err, durable.ErrRequestConflict), errors.Is(err, durable.ErrLeaseLost), errors.Is(err, durable.ErrClosed), errors.Is(err, durable.ErrTaskDeadline), errors.Is(err, durable.ErrExecutionDeadline), errors.Is(err, operator.ErrBuildMismatch):
		return forge.NewHTTPError(http.StatusConflict, "callback conflicts with the saved attempt or request")
	default:
		return forge.NewHTTPError(http.StatusServiceUnavailable, "callback unavailable; retry the identical request")
	}
}
