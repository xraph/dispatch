package redis

import "testing"

func TestKeys_full(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		suffix string
		want   string
	}{
		{name: "no prefix keeps the historical key", prefix: "", suffix: "job:1", want: "dispatch:job:1"},
		{name: "tenant prefix wraps the namespace", prefix: "ws_acme:", suffix: "job:1", want: "ws_acme:dispatch:job:1"},
		{name: "wake channel follows the same rule", prefix: "ws_acme:", suffix: "jobs:wake", want: "ws_acme:dispatch:jobs:wake"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newKeys(tt.prefix).full(tt.suffix); got != tt.want {
				t.Fatalf("full(%q) with prefix %q = %q, want %q", tt.suffix, tt.prefix, got, tt.want)
			}
		})
	}
}

// Usage keys arrived after WithKeyPrefix and must not slip back to the
// unprefixed namespace: a shared Redis would then mix two tenants' usage
// history into one estimator read.
func TestKeys_usageCarriesPrefix(t *testing.T) {
	k := newKeys("ws_acme:")
	for got, want := range map[string]string{
		k.usage("u1"):         "ws_acme:dispatch:usage:u1",
		k.usageIndex():        "ws_acme:dispatch:usage_index",
		k.usageName("resize"): "ws_acme:dispatch:usage_name:resize",
	} {
		if got != want {
			t.Fatalf("usage key = %q, want %q", got, want)
		}
	}
}
