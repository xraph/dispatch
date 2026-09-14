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
