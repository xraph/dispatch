package sqlite_test

import (
	"context"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// TestSqliteRoundTripsResources mirrors store/postgres's resource round-trip
// test: a job with cpu/memory/a custom key/limits/class/input signal must
// come back identical, and the custom key specifically must survive since
// it only lives in the JSON column, not the scalar columns.
func TestSqliteRoundTripsResources(t *testing.T) {
	s := openSqliteStore(t)
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

// TestSqliteJobWithNoResourcesRoundTrips pins the backward-compatibility
// contract: a job enqueued without any resource declaration must come back
// with a zero Set, not a Set containing zero-valued canonical keys -- those
// are indistinguishable to a caller checking IsZero(), but the row-level
// NULL-vs-"{}" distinction is what a rolling deploy depends on.
//
// So this asserts the STORED value, not just the decoded one. IsZero() is
// true for nil, for Set{} and for Set{"memory": 0} alike, which means the
// decoded-side check the comment above describes cannot actually see the
// distinction it names. Reading the column back raw can: NULL is what a
// worker still running the pre-resource code reads as "no requirement".
// Redis and Mongo already assert on their raw stored value; this is
// SQLite's.
func TestSqliteJobWithNoResourcesRoundTrips(t *testing.T) {
	s, drv, _ := openMigratedWithDriver(t)
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
		requestsNull int
		limitsNull   int
		cpu          int64
		customKeys   string
		class        string
		inputBytes   int64
	)

	row := drv.QueryRow(ctx, `
		SELECT resource_requests IS NULL,
		       resource_limits IS NULL,
		       req_cpu_milli,
		       req_custom_keys,
		       resource_class,
		       input_bytes
		FROM dispatch_jobs WHERE id = ?`, j.ID.String())

	if err = row.Scan(&requestsNull, &limitsNull, &cpu,
		&customKeys, &class, &inputBytes); err != nil {
		t.Fatalf("raw column read: %v", err)
	}

	if requestsNull != 1 {
		t.Error(`resource_requests is not NULL for an undeclared job; a stored "{}" decodes ` +
			`to the same zero Set but is a different row, and NULL is what a worker running ` +
			`the pre-resource code reads as "no requirement"`)
	}

	if limitsNull != 1 {
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
