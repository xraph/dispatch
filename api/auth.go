package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/security"
)

type Option func(*API)

// WithSecurity supplies the verified HTTP identity and installation policy boundary.
func WithSecurity(auth security.Authenticator, boundary security.Boundary) Option {
	return func(a *API) { a.auth = auth; a.boundary = boundary }
}
func (a *API) guard(method, path string) forge.Middleware {
	return func(next forge.Handler) forge.Handler {
		return func(ctx forge.Context) error {
			op := security.RESTOperation(method, path)
			target, targetErr := auditTarget(ctx, path)
			op.Target = target
			if a.auth == nil {
				_ = a.boundary.AuthenticationDenied(ctx.Context(), op) //nolint:errcheck // The response remains unauthorized even if local audit fails.
				return ctx.JSON(http.StatusUnauthorized, ErrorResponse{Error: "authentication required"})
			}
			checkCtx, cancel := context.WithTimeout(ctx.Context(), security.CheckTimeout)
			defer cancel()
			p, err := a.auth.Authenticate(checkCtx, ctx.Request())
			if err == nil {
				err = a.boundary.Check(checkCtx, p, op)
			} else {
				err = a.boundary.AuthenticationDenied(checkCtx, op)
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
			if targetErr != nil {
				return ctx.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid operation target"})
			}
			attempt, err := a.boundary.BeginCommand(ctx.Context(), p, op)
			if err != nil {
				return ctx.JSON(http.StatusServiceUnavailable, ErrorResponse{Error: "audit acceptance unavailable"})
			}
			ctx.WithContext(durable.WithAuditMetadata(ext.WithActor(ctx.Context(), p.Subject), security.Metadata(p)))
			if !op.Mutating() {
				return next(ctx)
			}
			buffered := &commandResponse{responseContext: ctx, writer: &commandWriter{header: ctx.Response().Header().Clone()}}
			err = next(buffered)
			outcomeErr := a.boundary.FinishCommand(ctx.Context(), attempt, err == nil && buffered.writer.code < 400 && !buffered.writer.overflow)
			if outcomeErr != nil {
				return ctx.JSON(http.StatusServiceUnavailable, ErrorResponse{Error: outcomeErr.Error()})
			}
			if err != nil {
				return err
			}
			if buffered.writer.overflow {
				return ctx.JSON(http.StatusServiceUnavailable, ErrorResponse{Error: "command response unavailable; reconcile attempt " + attempt.Record.Attempt.ID})
			}
			return buffered.flush()
		}
	}
}

// Forge adapts handlers through http.ResponseWriter and creates a new Context.
// Capture that writer as well as direct JSON/no-content calls, before any status
// reaches the client. Mutation responses are bounded to four MiB.
type responseContext = forge.Context
type commandResponse struct {
	responseContext
	writer *commandWriter
}

func (c *commandResponse) Response() http.ResponseWriter { return c.writer }
func (c *commandResponse) JSON(code int, value any) error {
	c.writer.Header().Set("Content-Type", "application/json")
	c.writer.WriteHeader(code)
	return json.NewEncoder(c.writer).Encode(value)
}
func (c *commandResponse) NoContent(code int) error { c.writer.WriteHeader(code); return nil }
func (c *commandResponse) flush() error {
	for key, values := range c.writer.header {
		c.responseContext.Response().Header()[key] = values
	}
	code := c.writer.code
	if code == 0 {
		code = http.StatusOK
	}
	c.responseContext.Response().WriteHeader(code)
	_, err := c.responseContext.Response().Write(c.writer.body.Bytes())
	return err
}

type commandWriter struct {
	header   http.Header
	code     int
	body     bytes.Buffer
	overflow bool
}

func (w *commandWriter) Header() http.Header { return w.header }
func (w *commandWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
		return
	}
	// Forge can discover a serialization error after selecting a success status.
	// Nothing has reached the client yet, so discard partial response bytes and
	// preserve the handler adapter's final error status.
	if code >= 400 && w.code < 400 {
		w.code = code
		w.body.Reset()
	}
}
func (w *commandWriter) Write(data []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	if w.body.Len()+len(data) > 4<<20 {
		w.overflow = true
		return 0, fmt.Errorf("dispatch: command response exceeds limit")
	}
	return w.body.Write(data)
}
