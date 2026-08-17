package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/resource"
)

// DLQStore is the capability this suite exercises.
type DLQStore interface {
	dlq.Store
}

// RunDLQSuite asserts the DLQ contract every backend has to satisfy.
//
// newStore may return a shared store, so every case works on entries it
// created itself and never asserts on the total count.
func RunDLQSuite(t *testing.T, newStore func(t *testing.T) DLQStore) {
	t.Helper()

	t.Run("PushDLQPreservesExecutionFields", func(t *testing.T) {
		testDLQPreservesExecutionFields(t, newStore(t))
	})

	t.Run("PushDLQPreservesAbsentResourceSets", func(t *testing.T) {
		testDLQPreservesAbsentResourceSets(t, newStore(t))
	})
}

// testDLQPreservesExecutionFields is the round trip that stops a backend
// from silently dropping a column.
//
// dlq.Replay rebuilds a job from the stored entry and enqueues it
// directly, without going back through the engine, so nothing re-derives
// any of these values for it. A backend that fails to persist one does not
// report an error: the entry reads back with a zero in that field and the
// replayed job quietly runs with a default instead. For LeaseTTL that
// default is short enough to make a long job unrunnable, since its lease
// lapses mid-run and reclamation restarts it forever.
//
// Every value below is deliberately non-zero and distinct, so a mapper
// that drops a field, or crosses two of them, cannot produce a passing
// result by accident.
func testDLQPreservesExecutionFields(t *testing.T, s DLQStore) {
	t.Helper()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	want := &dlq.Entry{
		ID:         id.NewDLQID(),
		JobID:      id.NewJobID(),
		JobName:    "long-render",
		Queue:      "dlq-fidelity",
		Payload:    []byte(`{"frame":42}`),
		Error:      "handler exploded",
		RetryCount: 3,
		MaxRetries: 3,
		ScopeAppID: "app_1",
		ScopeOrgID: "org_1",
		FailedAt:   now,
		CreatedAt:  now,

		Priority: 7,
		Timeout:  90 * time.Minute,
		// Six hours: the case the per-job TTL exists for, and far enough
		// from any pool default that a dropped value is unmistakable.
		LeaseTTL:         6 * time.Hour,
		ArtifactBindings: []byte(`{"input":"artifact_abc"}`),
		Resources:        resource.Set{"cpu_milli": 2000, "memory_bytes": 1 << 30},
		ResourceLimits:   resource.Set{"cpu_milli": 4000, "memory_bytes": 2 << 30},
		ResourceClass:    "gpu-large",
		InputBytes:       4096,
		PrimaryInputHash: "sha256:deadbeef",
	}

	if err := s.PushDLQ(ctx, want); err != nil {
		t.Fatalf("PushDLQ: %v", err)
	}

	got, err := s.GetDLQ(ctx, want.ID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}

	if got.Priority != want.Priority {
		t.Errorf("Priority = %d, want %d", got.Priority, want.Priority)
	}
	if got.Timeout != want.Timeout {
		t.Errorf("Timeout = %v, want %v", got.Timeout, want.Timeout)
	}
	if got.LeaseTTL != want.LeaseTTL {
		t.Errorf("LeaseTTL = %v, want %v: a replayed job would fall back to the "+
			"pool default and be reclaimed mid-run forever", got.LeaseTTL, want.LeaseTTL)
	}
	if string(got.ArtifactBindings) != string(want.ArtifactBindings) {
		t.Errorf("ArtifactBindings = %q, want %q", got.ArtifactBindings, want.ArtifactBindings)
	}
	if !resourceSetEqual(got.Resources, want.Resources) {
		t.Errorf("Resources = %v, want %v", got.Resources, want.Resources)
	}
	if !resourceSetEqual(got.ResourceLimits, want.ResourceLimits) {
		t.Errorf("ResourceLimits = %v, want %v", got.ResourceLimits, want.ResourceLimits)
	}
	if got.ResourceClass != want.ResourceClass {
		t.Errorf("ResourceClass = %q, want %q", got.ResourceClass, want.ResourceClass)
	}
	if got.InputBytes != want.InputBytes {
		t.Errorf("InputBytes = %d, want %d", got.InputBytes, want.InputBytes)
	}
	if got.PrimaryInputHash != want.PrimaryInputHash {
		t.Errorf("PrimaryInputHash = %q, want %q", got.PrimaryInputHash, want.PrimaryInputHash)
	}

	// ListDLQ decodes through the same mapper but a different query, and
	// on at least one backend that is a genuinely separate code path.
	listed, err := s.ListDLQ(ctx, dlq.ListOpts{Queue: want.Queue, Limit: 50})
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}

	var found *dlq.Entry
	for _, e := range listed {
		if e.ID == want.ID {
			found = e

			break
		}
	}
	if found == nil {
		t.Fatalf("ListDLQ did not return the pushed entry")
	}
	if found.LeaseTTL != want.LeaseTTL {
		t.Errorf("ListDLQ LeaseTTL = %v, want %v", found.LeaseTTL, want.LeaseTTL)
	}
}

// testDLQPreservesAbsentResourceSets pins that a job which declared no
// resources reads back with none, rather than with an empty set.
//
// The distinction is not cosmetic on the SQL backends: resource.
// EncodeSetString writes NULL for a zero Set specifically so the two stay
// distinguishable, and a mapper that turns absent into empty would make
// every replayed job look like it had declared an explicit empty
// requirement.
func testDLQPreservesAbsentResourceSets(t *testing.T, s DLQStore) {
	t.Helper()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	bare := &dlq.Entry{
		ID:         id.NewDLQID(),
		JobID:      id.NewJobID(),
		JobName:    "plain",
		Queue:      "dlq-fidelity-bare",
		Payload:    []byte(`{}`),
		Error:      "boom",
		MaxRetries: 3,
		FailedAt:   now,
		CreatedAt:  now,
	}

	if err := s.PushDLQ(ctx, bare); err != nil {
		t.Fatalf("PushDLQ: %v", err)
	}

	got, err := s.GetDLQ(ctx, bare.ID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}

	if len(got.Resources) != 0 {
		t.Errorf("Resources = %v, want none", got.Resources)
	}
	if len(got.ResourceLimits) != 0 {
		t.Errorf("ResourceLimits = %v, want none", got.ResourceLimits)
	}
	if got.LeaseTTL != 0 {
		t.Errorf("LeaseTTL = %v, want 0", got.LeaseTTL)
	}
}

// resourceSetEqual compares two sets by content, treating nil and empty as
// the same thing.
func resourceSetEqual(a, b resource.Set) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}

	return true
}
