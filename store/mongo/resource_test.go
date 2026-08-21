package mongo_test

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

// TestMongoRoundTripsResources mirrors store/postgres's and store/sqlite's
// resource round-trip test: a job with cpu/memory/a custom key/limits/
// class/input signal must come back identical, and the custom key
// specifically must survive since it only lives in the full-fidelity
// resource_requests subdocument, not the scalar fields.
func TestMongoRoundTripsResources(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
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

// TestMongoJobWithNoResourcesRoundTrips pins the backward-compatibility
// contract: a job enqueued without any resource declaration must come back
// with a zero Set, not a Set containing zero-valued canonical keys.
func TestMongoJobWithNoResourcesRoundTrips(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
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

// TestMongoUndeclaredJobHasNoResourceSubdocument closes the gap a
// Resources.IsZero() assertion cannot: decoding an absent
// resource_requests key and decoding an empty {} subdocument both yield
// IsZero() == true, so that assertion alone does not prove the stored
// value is genuinely null rather than an empty subdocument. This test
// reads the raw BSON document straight from the driver, bypassing
// jobModel entirely, and asserts resource_requests/resource_limits are
// BSON null -- never an empty subdocument -- which is Mongo's exact
// analogue of the SQL backends' NULL column.
//
// It asserts null rather than "key absent", which is a deliberate,
// empirically-verified choice: EnqueueJob writes through grove's
// mongodriver.NewInsert, whose structToMapInsert builds the document by
// reflecting over every grove-tagged field and always assigning
// doc[column] = value.Interface() -- it has no concept of the field's
// bson struct tag, so "omitempty" on ResourceRequests/ResourceLimits
// has no effect on this path and a nil resource.Set is written as an
// explicit BSON null, not omitted. (The bson tag DOES take effect on
// UpdateJob, which calls the raw driver's ReplaceOne(ctx, filter, m)
// directly -- there a nil Set genuinely drops the key. The two write
// paths are asymmetric in this one respect; both leave the field
// non-existent-or-null, never "{}", so IsZero() on read is unaffected
// either way.)
func TestMongoUndeclaredJobHasNoResourceSubdocument(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	rawDB := rawDatabase(t, uri)
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

	var raw bson.M
	if err := rawDB.Collection("dispatch_jobs").
		FindOne(ctx, bson.M{"_id": j.ID.String()}).
		Decode(&raw); err != nil {
		t.Fatalf("raw FindOne() error = %v", err)
	}

	reqVal, reqOK := raw["resource_requests"]
	if !reqOK {
		t.Error("resource_requests key missing from raw document, want present-and-null")
	} else if reqVal != nil {
		t.Errorf("resource_requests = %#v (%T), want BSON null, not an empty subdocument", reqVal, reqVal)
	}

	limitsVal, limitsOK := raw["resource_limits"]
	if !limitsOK {
		t.Error("resource_limits key missing from raw document, want present-and-null")
	} else if limitsVal != nil {
		t.Errorf("resource_limits = %#v (%T), want BSON null, not an empty subdocument", limitsVal, limitsVal)
	}

	// The scalar/class/hash fields are always-present columns in the
	// SQL backends (NOT NULL DEFAULT), so their Mongo analogues should
	// still be written -- only the full-fidelity subdocuments are
	// null-when-zero.
	if v, ok := raw["req_cpu_milli"]; !ok || toInt64(v) != 0 {
		t.Errorf("req_cpu_milli = %v, want present and 0", raw["req_cpu_milli"])
	}
	if v, ok := raw["req_custom_keys"]; !ok || v != "" {
		t.Errorf("req_custom_keys = %v, want present and empty", raw["req_custom_keys"])
	}
}

// TestMongoUpdateJobDropsResourceKeyEntirely documents the asymmetry
// called out above: UpdateJob's write path (ReplaceOne with the raw
// driver, not grove's insert helper) DOES honor the bson "omitempty"
// tag, so replacing a job down to a zero Set drops the key from the
// document entirely rather than leaving it null. Both representations
// -- absent key, or present-and-null -- decode back to a nil Set, so
// this is a proof of the write path's actual behavior, not a
// requirement the contract imposes.
func TestMongoUpdateJobDropsResourceKeyEntirely(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	rawDB := rawDatabase(t, uri)
	ctx := context.Background()

	j := &job.Job{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewJobID(),
		Name:      "notify.user",
		Queue:     "default",
		State:     job.StatePending,
		Payload:   []byte(`{}`),
		Resources: resource.Set{resource.CPU: 1000},
	}
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob() error = %v", err)
	}

	j.Resources = nil
	if err := s.UpdateJob(ctx, j); err != nil {
		t.Fatalf("UpdateJob() error = %v", err)
	}

	var raw bson.M
	if err := rawDB.Collection("dispatch_jobs").
		FindOne(ctx, bson.M{"_id": j.ID.String()}).
		Decode(&raw); err != nil {
		t.Fatalf("raw FindOne() error = %v", err)
	}

	if v, ok := raw["resource_requests"]; ok {
		t.Errorf("resource_requests = %#v, want key entirely absent after ReplaceOne", v)
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if !got.Resources.IsZero() {
		t.Errorf("Resources = %v, want zero after clearing", got.Resources)
	}
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int32:
		return int64(n)
	case int64:
		return n
	default:
		return -1
	}
}
