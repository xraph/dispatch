package k8s

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
)

// The shared cluster suite cannot run here: RegisterWorker annotates an
// existing Pod named after the worker's Hostname, and the suite's
// workers have no Pod. These two cases pin GetWorker directly.

func TestGetWorker(t *testing.T) {
	p, _ := newTestProvider(t, makeWorkerPod("get-pod"), makeWorkerPod("other-pod"))
	ctx := context.Background()

	want := makeWorker(t, "get-pod")
	if err := p.RegisterWorker(ctx, want); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if err := p.RegisterWorker(ctx, makeWorker(t, "other-pod")); err != nil {
		t.Fatalf("RegisterWorker other: %v", err)
	}

	got, err := p.GetWorker(ctx, want.ID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got.ID.String() != want.ID.String() || got.Hostname != "get-pod" {
		t.Fatalf("GetWorker = %s on %q, want %s on %q", got.ID, got.Hostname, want.ID, "get-pod")
	}
	if got.Metadata["zone"] != "us-east-1" {
		t.Errorf("Metadata = %v, want zone us-east-1", got.Metadata)
	}
}

func TestGetWorker_NotFound(t *testing.T) {
	p, _ := newTestProvider(t, makeWorkerPod("lonely-pod"))

	_, err := p.GetWorker(context.Background(), id.NewWorkerID())
	if !errors.Is(err, dispatch.ErrWorkerNotFound) {
		t.Fatalf("GetWorker(unknown) error = %v, want dispatch.ErrWorkerNotFound", err)
	}
}
