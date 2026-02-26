package dwp

import (
	"context"
	"testing"
)

func TestAPIKeyAuthenticator(t *testing.T) {
	t.Parallel()

	auth := NewAPIKeyAuthenticator(
		APIKeyEntry{
			Token: "dk_test_123",
			Identity: Identity{
				Subject: "user-1",
				AppID:   "app-1",
				OrgID:   "org-1",
				Scopes:  []string{ScopeJobWrite, ScopeWorkflowRead},
			},
		},
		APIKeyEntry{
			Token: "dk_admin_456",
			Identity: Identity{
				Subject: "admin-1",
				Scopes:  []string{ScopeAll},
			},
		},
	)

	ctx := context.Background()

	t.Run("valid token", func(t *testing.T) {
		id, err := auth.Authenticate(ctx, "dk_test_123")
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if id.Subject != "user-1" {
			t.Errorf("Subject = %q, want %q", id.Subject, "user-1")
		}
		if id.AppID != "app-1" {
			t.Errorf("AppID = %q, want %q", id.AppID, "app-1")
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		_, err := auth.Authenticate(ctx, "invalid")
		if err == nil {
			t.Error("expected error for invalid token")
		}
	})
}

func TestIdentityHasScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		scopes   []string
		check    string
		expected bool
	}{
		{"exact match", []string{"job:write"}, "job:write", true},
		{"no match", []string{"job:write"}, "job:read", false},
		{"wildcard", []string{"*"}, "anything", true},
		{"multiple scopes", []string{"job:read", "job:write"}, "job:write", true},
		{"empty scopes", nil, "job:read", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := &Identity{Subject: "test", Scopes: tt.scopes}
			if got := id.HasScope(tt.check); got != tt.expected {
				t.Errorf("HasScope(%q) = %v, want %v", tt.check, got, tt.expected)
			}
		})
	}
}

func TestRequiredScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method   string
		expected string
	}{
		{MethodAuth, ""},
		{MethodJobEnqueue, ScopeJobWrite},
		{MethodJobGet, ScopeJobRead},
		{MethodJobCancel, ScopeJobWrite},
		{MethodJobList, ScopeJobRead},
		{MethodWorkflowStart, ScopeWorkflowWrite},
		{MethodWorkflowGet, ScopeWorkflowRead},
		{MethodWorkflowTimeline, ScopeWorkflowRead},
		{MethodSubscribe, ScopeSubscribe},
		{MethodUnsubscribe, ScopeSubscribe},
		{MethodStats, ScopeStatsRead},
		{MethodFederationEnqueue, ScopeAdmin},
		{MethodFederationEvent, ScopeAdmin},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			got := RequiredScope(tt.method)
			if got != tt.expected {
				t.Errorf("RequiredScope(%q) = %q, want %q", tt.method, got, tt.expected)
			}
		})
	}
}

func TestNoopAuthenticator(t *testing.T) {
	t.Parallel()

	auth := &NoopAuthenticator{}
	id, err := auth.Authenticate(context.Background(), "any-token")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.Subject != "anonymous" {
		t.Errorf("Subject = %q, want %q", id.Subject, "anonymous")
	}
	if !id.HasScope(ScopeAll) {
		t.Error("NoopAuthenticator should grant wildcard scope")
	}
}

func TestCompositeAuthenticator(t *testing.T) {
	t.Parallel()

	apiKey := NewAPIKeyAuthenticator(
		APIKeyEntry{
			Token:    "dk_first",
			Identity: Identity{Subject: "first"},
		},
	)

	second := NewAPIKeyAuthenticator(
		APIKeyEntry{
			Token:    "dk_second",
			Identity: Identity{Subject: "second"},
		},
	)

	composite := NewCompositeAuthenticator(apiKey, second)
	ctx := context.Background()

	t.Run("first authenticator matches", func(t *testing.T) {
		id, err := composite.Authenticate(ctx, "dk_first")
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if id.Subject != "first" {
			t.Errorf("Subject = %q, want %q", id.Subject, "first")
		}
	})

	t.Run("second authenticator matches", func(t *testing.T) {
		id, err := composite.Authenticate(ctx, "dk_second")
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if id.Subject != "second" {
			t.Errorf("Subject = %q, want %q", id.Subject, "second")
		}
	})

	t.Run("none match", func(t *testing.T) {
		_, err := composite.Authenticate(ctx, "unknown")
		if err == nil {
			t.Error("expected error when no authenticator matches")
		}
	})
}
