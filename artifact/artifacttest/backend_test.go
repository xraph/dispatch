package artifacttest_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
)

func TestBackendRoundTrip(t *testing.T) {
	ctx := context.Background()
	b := artifacttest.NewBackend()
	b.Put("models", "tower.ifc", []byte("hello"))

	ref := artifact.Ref{Backend: b.Name(), Bucket: "models", Key: "tower.ifc"}

	rc, err := b.Open(ctx, ref)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	got, err := io.ReadAll(rc)
	if cerr := rc.Close(); cerr != nil {
		t.Fatalf("Close: %v", cerr)
	}

	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("read %q, want %q", got, "hello")
	}

	if b.Opens() != 1 {
		t.Fatalf("Opens() = %d, want 1", b.Opens())
	}
}

func TestBackendOpenMissing(t *testing.T) {
	_, err := artifacttest.NewBackend().Open(context.Background(),
		artifact.Ref{Bucket: "models", Key: "nope"})
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Open(missing) = %v, want ErrNotFound", err)
	}
}

func TestBackendStat(t *testing.T) {
	b := artifacttest.NewBackend()
	b.Put("models", "tower.ifc", []byte("0123456789"))

	info, err := b.Stat(context.Background(),
		artifact.Ref{Bucket: "models", Key: "tower.ifc"})
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if info.Size != 10 {
		t.Fatalf("info.Size = %d, want 10", info.Size)
	}

	_, err = b.Stat(context.Background(), artifact.Ref{Bucket: "models", Key: "nope"})
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Stat(missing) = %v, want ErrNotFound", err)
	}
}

func TestBackendWriterCommitThenAbortIsNoOp(t *testing.T) {
	ctx := context.Background()
	b := artifacttest.NewBackend()

	w, err := b.Create(ctx, "models", "mesh.glb")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, werr := w.Write([]byte("meshdata")); werr != nil {
		t.Fatalf("Write: %v", werr)
	}

	info, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if info.Size != 8 {
		t.Fatalf("info.Size = %d, want 8", info.Size)
	}

	if err := w.Abort(); err != nil {
		t.Fatalf("Abort after Commit must be a no-op, got %v", err)
	}

	if !b.Has("models", "mesh.glb") {
		t.Fatal("committed object is missing")
	}
}

func TestBackendWriterAbortPublishesNothing(t *testing.T) {
	ctx := context.Background()
	b := artifacttest.NewBackend()

	w, err := b.Create(ctx, "models", "aborted.glb")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := w.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	if _, err := b.Open(ctx, artifact.Ref{Bucket: "models", Key: "aborted.glb"}); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("aborted object is readable; Open = %v, want ErrNotFound", err)
	}
}

func TestBackendDeleteMissingIsNotAnError(t *testing.T) {
	err := artifacttest.NewBackend().Delete(context.Background(),
		artifact.Ref{Bucket: "models", Key: "absent"})
	if err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}
