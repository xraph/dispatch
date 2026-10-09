package dwp

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/stream"
)

type RejectAuthenticator struct{}

func (*RejectAuthenticator) Authenticate(context.Context, string) (*Identity, error) {
	return nil, ErrUnauthorized
}

// ForgeAuthenticator carries the actual HTTP request so proof validation retains
// the original method, URL and context. Token-only callers fail closed.
type ForgeAuthenticator struct{ Auth security.Authenticator }

func (a *ForgeAuthenticator) Authenticate(context.Context, string) (*Identity, error) {
	return nil, ErrUnauthorized
}
func (a *ForgeAuthenticator) AuthenticateRequest(ctx context.Context, r *http.Request, token string) (*Identity, error) {
	if a == nil || a.Auth == nil || r == nil {
		return nil, ErrUnauthorized
	}
	request := r.Clone(ctx)
	if token != "" {
		fields := strings.Fields(token)
		if len(fields) == 1 {
			token = "Bearer " + token
		}
		request.Header.Set("Authorization", token)
	}
	p, err := a.Auth.Authenticate(ctx, request)
	if err != nil {
		return nil, ErrUnauthorized
	}
	return &Identity{Subject: p.Subject, Kind: p.Kind}, nil
}

type requestAuthenticator interface {
	AuthenticateRequest(context.Context, *http.Request, string) (*Identity, error)
}

func (s *Server) authenticate(ctx context.Context, r *http.Request, token string) (*Identity, error) {
	ctx, cancel := context.WithTimeout(ctx, security.CheckTimeout)
	defer cancel()
	var identity *Identity
	var err error
	if auth, ok := s.auth.(requestAuthenticator); ok {
		identity, err = auth.AuthenticateRequest(ctx, r, token)
	} else {
		identity, err = s.auth.Authenticate(ctx, token)
	}
	if err != nil || identity == nil || identity.principal().Validate() != nil {
		return nil, ErrUnauthorized
	}
	clone := *identity
	clone.Scopes = append([]string(nil), identity.Scopes...)
	return &clone, nil
}
func (id *Identity) principal() security.Principal {
	if id == nil {
		return security.Principal{}
	}
	kind := id.Kind
	if kind == "" {
		kind = "user"
	}
	return security.Principal{Subject: id.Subject, Kind: kind}
}
func (h *Handler) authorize(ctx context.Context, id *Identity, method string) error {
	return h.security.Check(ctx, id.principal(), security.DWPOperation(method))
}
func authorizationCode(err error) int {
	if errors.Is(err, security.ErrUnauthenticated) {
		return ErrCodeUnauthorized
	}
	if errors.Is(err, security.ErrForbidden) {
		return ErrCodeForbidden
	}
	return 503
}

// WithSecurity configures a policy boundary shared by admission, direct dispatch
// and event forwarding. Scope strings do not grant operator access.
func WithSecurity(boundary security.Boundary) Option {
	return func(s *Server) { s.handler.security = boundary }
}

type sseIdentityKey struct{}

// admitSSE runs before Forge commits the event-stream response headers.
func (s *Server) admitSSE(next forge.Handler) forge.Handler {
	return func(ctx forge.Context) error {
		identity, err := s.authenticate(ctx.Context(), ctx.Request(), ctx.Header("Authorization"))
		if err != nil {
			return ctx.JSON(http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		}
		if err := s.handler.authorize(ctx.Context(), identity, MethodSubscribe); err != nil {
			return ctx.JSON(authorizationCode(err), map[string]string{"error": "access unavailable or denied"})
		}
		if err := stream.ValidateTopic(ctx.Query("channel")); err != nil {
			return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "invalid channel"})
		}
		ctx.WithContext(context.WithValue(ctx.Context(), sseIdentityKey{}, identity))
		return next(ctx)
	}
}
