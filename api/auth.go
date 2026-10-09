package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/security"
)

type Option func(*API)

// WithSecurity supplies the verified HTTP identity and installation policy boundary.
func WithSecurity(auth security.Authenticator, boundary security.Boundary) Option {
	return func(a *API) { a.auth = auth; a.boundary = boundary }
}
func (a *API) guard(method, path string) forge.Middleware {
	op := security.RESTOperation(method, path)
	return func(next forge.Handler) forge.Handler {
		return func(ctx forge.Context) error {
			if a.auth == nil {
				return ctx.JSON(http.StatusUnauthorized, ErrorResponse{Error: "authentication required"})
			}
			checkCtx, cancel := context.WithTimeout(ctx.Context(), security.CheckTimeout)
			defer cancel()
			p, err := a.auth.Authenticate(checkCtx, ctx.Request())
			if err == nil {
				err = a.boundary.Check(checkCtx, p, op)
			}
			if err != nil {
				code := http.StatusServiceUnavailable
				message := "authorization unavailable"
				if errors.Is(err, security.ErrUnauthenticated) {
					code = http.StatusUnauthorized
					message = "authentication required"
				}
				if errors.Is(err, security.ErrForbidden) {
					code = http.StatusForbidden
					message = "access denied"
				}
				return ctx.JSON(code, ErrorResponse{Error: message})
			}
			ctx.WithContext(ext.WithActor(ctx.Context(), p.Subject))
			return next(ctx)
		}
	}
}
