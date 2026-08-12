//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

func TestPostgresRoundTripsResources(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	j := &job.Job{
		Entity:  dispatch.NewEntity(),
		ID:      id.NewJobID(),
		Name:    "tessellate.model",
		Queue:   "default",
		State:   job.StatePending,
		Payload: []byte(`{}`),
		Resources: resource.Set{
			resource.CPU: 4000, resource.Memory: 16 << 30, "fpga": 2,
		},
		ResourceLimits:   resource.Set{resource.Memory: 16 << 30},
		ResourceClass:    "heavy",
		InputBytes:       4 << 30,
		PrimaryInputHash: "blake3:9f2a",
	}

	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob() error = %v", err)
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}

	if got.Resources[resource.CPU] != 4000 {
		t.Errorf("cpu = %d, want 4000", got.Resources[resource.CPU])
	}
	if got.Resources["fpga"] != 2 {
		t.Errorf("custom key lost in round-trip: %v", got.Resources)
	}
	if got.ResourceLimits[resource.Memory] != 16<<30 {
		t.Errorf("limits = %v", got.ResourceLimits)
	}
	if got.ResourceClass != "heavy" {
		t.Errorf("class = %q, want heavy", got.ResourceClass)
	}
	if got.InputBytes != 4<<30 || got.PrimaryInputHash != "blake3:9f2a" {
		t.Errorf("input signal lost: bytes=%d hash=%q",
			got.InputBytes, got.PrimaryInputHash)
	}
}

func TestPostgresJobWithNoResourcesRoundTrips(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	j := &job.Job{
		Entity:  dispatch.NewEntity(),
		ID:      id.NewJobID(),
		Name:    "notify.user",
		Queue:   "default",
		State:   job.StatePending,
		Payload: []byte(`{}`),
	}

	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob() error = %v", err)
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if !got.Resources.IsZero() {
		t.Errorf("Resources = %v, want zero", got.Resources)
	}
}
