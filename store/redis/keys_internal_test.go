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

// The created-order indexes are new keys on data that already exists in
// production, so their exact shape is pinned: renaming one would orphan
// every index already built and quietly backfill a new one.
func TestKeys_createdIndexes(t *testing.T) {
	tests := []struct {
		entity string
		want   string
	}{
		{entityJob, "ws_acme:dispatch:job_by_created"},
		{entityRun, "ws_acme:dispatch:run_by_created"},
		{entityDLQ, "ws_acme:dispatch:dlq_by_created"},
		{entityArtifact, "ws_acme:dispatch:artifact_by_created"},
	}
	k := newKeys("ws_acme:")
	for _, tt := range tests {
		t.Run(tt.entity, func(t *testing.T) {
			if got := k.byCreated(tt.entity); got != tt.want {
				t.Errorf("byCreated(%q) = %q, want %q", tt.entity, got, tt.want)
			}
		})
	}

	if got := newKeys("").byCreated(entityJob); got != "dispatch:job_by_created" {
		t.Errorf("unprefixed byCreated(job) = %q, want %q", got, "dispatch:job_by_created")
	}
}
