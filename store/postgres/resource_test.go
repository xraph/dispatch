//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

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

// TestPostgresJobWithNoResourcesRoundTrips pins the backward-compatibility
// contract, and it asserts the STORED value rather than only the decoded
// one.
//
// Resources.IsZero() is true for a nil Set, for Set{}, and for
// Set{"memory": 0} alike, so a decoded-side assertion cannot tell an
// undeclared job from one that stored an empty JSON document. The
// difference is not cosmetic: NULL is what a worker running the
// pre-resource code reads as "no requirement", and '{}' is what a
// half-migrated fleet would have to interpret. Redis and Mongo already
// assert on the raw stored value; this is the SQL analogue, and it is
// the column NULL itself that a rolling deploy depends on.
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

	var (
		requestsNull bool
		limitsNull   bool
		cpu          int64
		customKeys   string
		class        string
		inputBytes   int64
	)

	if err = pgdriver.Unwrap(s.DB()).QueryRow(ctx, `
		SELECT resource_requests IS NULL,
		       resource_limits IS NULL,
		       req_cpu_milli,
		       req_custom_keys,
		       resource_class,
		       input_bytes
		FROM dispatch_jobs WHERE id = $1`, j.ID.String()).
		Scan(&requestsNull, &limitsNull, &cpu, &customKeys, &class, &inputBytes); err != nil {
		t.Fatalf("raw column read: %v", err)
	}

	if !requestsNull {
		t.Error("resource_requests is not NULL for an undeclared job; " +
			"an empty JSONB document decodes to the same zero Set but is a different row")
	}

	if !limitsNull {
		t.Error("resource_limits is not NULL for an undeclared job")
	}

	// The scalar columns are NOT NULL DEFAULT, so they must be present
	// and zero — that is what lets the dequeue comparisons be bare
	// rather than COALESCEd.
	if cpu != 0 || customKeys != "" || class != "" || inputBytes != 0 {
		t.Errorf("scalar columns = cpu %d, keys %q, class %q, input %d; want all zero/empty",
			cpu, customKeys, class, inputBytes)
	}
}
