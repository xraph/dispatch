// Package scope provides helpers to capture and restore multi-tenant
// execution context (app and org identity) from/to context.Context.
//
// When the forge framework is available, scope is carried via
// forge.WithScope / forge.ScopeFrom. These helpers bridge between
// the Job entity's ScopeAppID/ScopeOrgID fields and the context.
package scope

import (
	"context"

	"github.com/xraph/forge"
)

// Capture extracts the app and org identifiers from the context.
// Returns empty strings if no scope is present.
func Capture(ctx context.Context) (appID, orgID string) {
	s, ok := forge.ScopeFrom(ctx)
	if !ok {
		return "", ""
	}
	return s.AppID(), s.OrgID()
}

// Restore attaches a scope to the context using the given app and org IDs.
// If both are empty, the context is returned unchanged (no-op).
func Restore(ctx context.Context, appID, orgID string) context.Context {
	if appID == "" && orgID == "" {
		return ctx
	}
	var s forge.Scope
	if orgID != "" {
		s = forge.NewOrgScope(appID, orgID)
	} else {
		s = forge.NewAppScope(appID)
	}
	return forge.WithScope(ctx, s)
}
