package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
)

// TestEnqueueRaw_CarriesLeaseTTL verifies that EnqueueRaw copies the declared
// LeaseTTL from job options to both the returned job and the persisted store row.
func TestEnqueueRaw_CarriesLeaseTTL(t *testing.T) {
	tests := []struct {
		name     string
		leaseTTL time.Duration
	}{
		{
			name:     "zero LeaseTTL",
			leaseTTL: 0,
		},
		{
			name:     "30 second LeaseTTL",
			leaseTTL: 30 * time.Second,
		},
		{
			name:     "6 hour LeaseTTL",
			leaseTTL: 6 * time.Hour,
		},
		{
			name:     "1 minute LeaseTTL",
			leaseTTL: 1 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng, store, _ := newWorkflowEngine(t)

			// Enqueue a job with the specified LeaseTTL.
			returned, err := eng.EnqueueRaw(
				context.Background(),
				"test-job",
				[]byte(`{}`),
				job.WithLeaseTTL(tt.leaseTTL),
			)
			if err != nil {
				t.Fatalf("EnqueueRaw: %v", err)
			}

			// Assert the returned job carries the declared LeaseTTL.
			if returned.LeaseTTL != tt.leaseTTL {
				t.Errorf("returned job LeaseTTL = %v, want %v", returned.LeaseTTL, tt.leaseTTL)
			}

			// Assert the persisted row in the store also has the LeaseTTL.
			persisted, err := store.GetJob(context.Background(), returned.ID)
			if err != nil {
				t.Fatalf("GetJob: %v", err)
			}
			if persisted.LeaseTTL != tt.leaseTTL {
				t.Errorf("persisted job LeaseTTL = %v, want %v", persisted.LeaseTTL, tt.leaseTTL)
			}
		})
	}
}

// TestEnqueueRaw_DefinitionLeaseTTL covers the other half of the
// precedence chain: a definition declares, an enqueue overrides.
//
// The definition half was unreachable before this test existed.
// job.WithLeaseTTL set Options.LeaseTTL, RegisterDefinition never read
// it, and EnqueueRaw built its options from the enqueue site alone — so a
// definition asking for a six-hour lease silently got the pool's default
// and its jobs were reclaimed mid-run. Per-definition TTLs are the point
// of putting lease_ttl on the row at all, so this asserts the value
// reaches the persisted row, not merely the returned struct.
func TestEnqueueRaw_DefinitionLeaseTTL(t *testing.T) {
	const (
		declared = 6 * time.Hour
		override = 90 * time.Second
	)

	tests := []struct {
		name string
		// defTTL is what the definition declares; zero declares nothing.
		defTTL time.Duration
		// enqueueOpts are the options passed to the enqueue call.
		enqueueOpts []job.Option
		want        time.Duration
	}{
		{
			name:   "definition declaration reaches the row",
			defTTL: declared,
			want:   declared,
		},
		{
			name:        "enqueue overrides the definition",
			defTTL:      declared,
			enqueueOpts: []job.Option{job.WithLeaseTTL(override)},
			want:        override,
		},
		{
			name: "neither declares, so the pool default applies",
			want: 0,
		},
		{
			name:        "enqueue alone still works with no declaration",
			enqueueOpts: []job.Option{job.WithLeaseTTL(override)},
			want:        override,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng, store, _ := newWorkflowEngine(t)

			var defOpts []job.Option
			if tt.defTTL > 0 {
				defOpts = append(defOpts, job.WithLeaseTTL(tt.defTTL))
			}

			engine.Register(eng, job.NewDefinition("leased-job",
				func(_ context.Context, _ struct{}) error { return nil },
				defOpts...))

			returned, err := eng.EnqueueRaw(
				context.Background(), "leased-job", []byte(`{}`), tt.enqueueOpts...)
			if err != nil {
				t.Fatalf("EnqueueRaw: %v", err)
			}

			if returned.LeaseTTL != tt.want {
				t.Errorf("returned job LeaseTTL = %v, want %v", returned.LeaseTTL, tt.want)
			}

			persisted, err := store.GetJob(context.Background(), returned.ID)
			if err != nil {
				t.Fatalf("GetJob: %v", err)
			}

			if persisted.LeaseTTL != tt.want {
				t.Errorf("persisted job LeaseTTL = %v, want %v", persisted.LeaseTTL, tt.want)
			}
		})
	}
}
