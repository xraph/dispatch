package engine_test

import (
	"context"
	"testing"
	"time"

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
