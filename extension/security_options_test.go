package extension

import "testing"

func TestOperatorProviderAllowlistCapturedAndCopied(t *testing.T) {
	providers := []string{"session"}
	option := WithOperatorSecurity(SecurityConfig{InstallationID: "installation", PolicyTenant: "tenant", Providers: providers})
	providers[0] = "unexpected"
	first := New(option)
	second := New(option)
	if first.config.Security.Providers[0] != "session" || second.config.Security.Providers[0] != "session" {
		t.Fatal("caller changed allowlist")
	}
	first.config.Security.Providers[0] = "changed"
	if second.config.Security.Providers[0] != "session" {
		t.Fatal("extension allowlists aliased")
	}
}
