//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// TestRedisRoundTripsResources mirrors store/postgres's and store/sqlite's
// resource round-trip test: a job with cpu/memory/a custom key/limits/
// class/input signal must come back identical, and the custom key
// specifically must survive since it only lives in the full-fidelity
// resource_requests JSON, not the scalar fields.
func TestRedisRoundTripsResources(t *testing.T) {
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

// TestRedisJobWithNoResourcesRoundTrips pins the backward-compatibility
// contract: a job enqueued without any resource declaration must come back
// with a zero Set, not a Set containing zero-valued canonical keys.
func TestRedisJobWithNoResourcesRoundTrips(t *testing.T) {
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

// TestRedisUndeclaredJobHasNoResourceKeyInRawJSON closes the gap a
// Resources.IsZero() assertion cannot: decoding an absent JSON key and
// decoding an empty "{}" both yield IsZero() == true, so that assertion
// alone does not prove the stored value is genuinely absent rather than
// an empty document. This test reads the raw JSON blob straight from the
// KV store, bypassing jobEntity entirely, and asserts the
// resource_requests/resource_limits keys are not present at all -- the
// Redis analogue of the SQL backends' NULL-column proof.
func TestRedisUndeclaredJobHasNoResourceKeyInRawJSON(t *testing.T) {
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

	// "dispatch:job:<id>" mirrors the unexported jobKey format in
	// keys.go (keyPrefix + "job:" + id) -- there is no other way to
	// read the raw entity from outside the package.
	raw, err := s.KV().GetRaw(ctx, "dispatch:job:"+j.ID.String())
	if err != nil {
		t.Fatalf("GetRaw() error = %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("unmarshal raw entity: %v", err)
	}

	if _, ok := fields["resource_requests"]; ok {
		t.Errorf("resource_requests present in raw JSON: %s", fields["resource_requests"])
	}
	if _, ok := fields["resource_limits"]; ok {
		t.Errorf("resource_limits present in raw JSON: %s", fields["resource_limits"])
	}

	// The scalar/class/hash fields are always-present in the SQL
	// backends (NOT NULL DEFAULT), so their Redis analogues should
	// still be written -- only the full-fidelity JSON is
	// absent-when-zero.
	if v, ok := fields["req_cpu_milli"]; !ok || string(v) != "0" {
		t.Errorf("req_cpu_milli = %s, want present and 0", v)
	}
	if v, ok := fields["req_custom_keys"]; !ok || string(v) != `""` {
		t.Errorf("req_custom_keys = %s, want present and empty", v)
	}
}
