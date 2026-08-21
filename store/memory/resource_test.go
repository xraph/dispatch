package memory_test

import (
	"context"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
)

func newResourceJob(t *testing.T) *job.Job {
	t.Helper()

	return &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       "tessellate.model",
		Queue:      "default",
		State:      job.StatePending,
		Payload:    []byte(`{}`),
		Resources:  resource.Set{resource.CPU: 4000, resource.Memory: 16 << 30},
		InputBytes: 4 << 30,
	}
}

func TestMemoryStoreRoundTripsResources(t *testing.T) {
	st := memory.New()
	ctx := context.Background()

	j := newResourceJob(t)
	if err := st.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob() error = %v", err)
	}

	got, err := st.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}

	if got.Resources[resource.Memory] != 16<<30 {
		t.Errorf("memory = %d, want 16 GiB", got.Resources[resource.Memory])
	}
	if got.InputBytes != 4<<30 {
		t.Errorf("InputBytes = %d, want 4 GiB", got.InputBytes)
	}
}

func TestMemoryStoreDoesNotAliasResourceMap(t *testing.T) {
	st := memory.New()
	ctx := context.Background()

	j := newResourceJob(t)
	if err := st.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob() error = %v", err)
	}

	// A caller mutating its own copy must not rewrite the stored job.
	j.Resources[resource.Memory] = 1

	got, err := st.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if got.Resources[resource.Memory] != 16<<30 {
		t.Fatalf("the store aliased the caller's map: memory = %d",
			got.Resources[resource.Memory])
	}

	// And a caller mutating what it read must not rewrite it either.
	got.Resources[resource.Memory] = 2

	again, err := st.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if again.Resources[resource.Memory] != 16<<30 {
		t.Fatalf("the store returned an aliased map: memory = %d",
			again.Resources[resource.Memory])
	}
}
