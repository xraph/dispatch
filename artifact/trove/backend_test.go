package trove_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	trovelib "github.com/xraph/trove"
	"github.com/xraph/trove/drivers/memdriver"

	"github.com/xraph/dispatch/artifact"
	troveadapter "github.com/xraph/dispatch/artifact/trove"
)

const testBucket = "dispatch"

func newBackend(t *testing.T) artifact.Backend {
	t.Helper()

	ctx := context.Background()

	drv := memdriver.New()
	if err := drv.Open(ctx, "mem://"); err != nil {
		t.Fatalf("driver open: %v", err)
	}

	tr, err := trovelib.Open(drv, trovelib.WithDefaultBucket(testBucket))
	if err != nil {
		t.Fatalf("trove open: %v", err)
	}

	t.Cleanup(func() {
		if cerr := tr.Close(ctx); cerr != nil {
			t.Errorf("trove close: %v", cerr)
		}
	})

	if err := tr.CreateBucket(ctx, testBucket); err != nil &&
		!errors.Is(err, trovelib.ErrBucketExists) {
		t.Fatalf("create bucket: %v", err)
	}

	return troveadapter.New(tr)
}

func writeObject(t *testing.T, b artifact.Backend, key string, data []byte) {
	t.Helper()

	ctx := context.Background()

	w, err := b.Create(ctx, testBucket, key)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, werr := w.Write(data); werr != nil {
		t.Fatalf("Write: %v", werr)
	}

	if _, cerr := w.Commit(ctx); cerr != nil {
		t.Fatalf("Commit: %v", cerr)
	}
}

func TestTroveRoundTrip(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	w, err := b.Create(ctx, testBucket, "mesh.glb")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, werr := w.Write([]byte("meshbytes")); werr != nil {
		t.Fatalf("Write: %v", werr)
	}

	info, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if info.Size != 9 {
		t.Fatalf("info.Size = %d, want 9 (logical bytes written)", info.Size)
	}

	ref := artifact.Ref{Backend: b.Name(), Bucket: testBucket, Key: "mesh.glb"}

	rc, err := b.Open(ctx, ref)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	got, rerr := io.ReadAll(rc)
	if cerr := rc.Close(); cerr != nil {
		t.Fatalf("Close: %v", cerr)
	}

	if rerr != nil {
		t.Fatalf("ReadAll: %v", rerr)
	}

	if !bytes.Equal(got, []byte("meshbytes")) {
		t.Fatalf("read %q, want %q", got, "meshbytes")
	}
}

func TestTroveOpenMissingMapsToErrNotFound(t *testing.T) {
	_, err := newBackend(t).Open(context.Background(),
		artifact.Ref{Bucket: testBucket, Key: "absent"})
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Open(missing) = %v, want ErrNotFound", err)
	}
}

func TestTroveStat(t *testing.T) {
	b := newBackend(t)
	writeObject(t, b, "stat.bin", []byte("0123456789"))

	info, err := b.Stat(context.Background(),
		artifact.Ref{Bucket: testBucket, Key: "stat.bin"})
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if info.Size != 10 {
		t.Fatalf("info.Size = %d, want 10", info.Size)
	}
}

func TestTroveStatMissingMapsToErrNotFound(t *testing.T) {
	_, err := newBackend(t).Stat(context.Background(),
		artifact.Ref{Bucket: testBucket, Key: "absent"})
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Stat(missing) = %v, want ErrNotFound", err)
	}
}

// TestTroveDeleteMissingIsNotAnError matters for the purge pass: it
// deletes bytes then the row, and must be safe to retry after a partial
// failure.
func TestTroveDeleteMissingIsNotAnError(t *testing.T) {
	err := newBackend(t).Delete(context.Background(),
		artifact.Ref{Bucket: testBucket, Key: "absent"})
	if err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}

func TestTroveDelete(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)
	writeObject(t, b, "doomed.bin", []byte("bye"))

	ref := artifact.Ref{Bucket: testBucket, Key: "doomed.bin"}

	if err := b.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := b.Open(ctx, ref); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Open after Delete = %v, want ErrNotFound", err)
	}
}

func TestTroveAbortPublishesNothing(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	w, err := b.Create(ctx, testBucket, "aborted.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, werr := w.Write([]byte("partial")); werr != nil {
		t.Fatalf("Write: %v", werr)
	}

	if aerr := w.Abort(); aerr != nil {
		t.Fatalf("Abort: %v", aerr)
	}

	if _, err := b.Open(ctx, artifact.Ref{Bucket: testBucket, Key: "aborted.bin"}); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("aborted object is readable; Open = %v, want ErrNotFound", err)
	}
}

func TestTroveAbortAfterCommitIsNoOp(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	w, err := b.Create(ctx, testBucket, "committed.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, werr := w.Write([]byte("data")); werr != nil {
		t.Fatalf("Write: %v", werr)
	}

	if _, cerr := w.Commit(ctx); cerr != nil {
		t.Fatalf("Commit: %v", cerr)
	}

	if aerr := w.Abort(); aerr != nil {
		t.Fatalf("Abort after Commit must be a no-op, got %v", aerr)
	}

	if _, err := b.Open(ctx, artifact.Ref{Bucket: testBucket, Key: "committed.bin"}); err != nil {
		t.Fatalf("Abort after Commit destroyed the object: %v", err)
	}
}

func TestTroveOpenRange(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)
	writeObject(t, b, "ranged.bin", []byte("0123456789"))

	rr, ok := b.(artifact.RangeReader)
	if !ok {
		t.Fatal("trove backend does not implement RangeReader")
	}

	rc, err := rr.OpenRange(ctx,
		artifact.Ref{Bucket: testBucket, Key: "ranged.bin"}, 2, 3)
	if errors.Is(err, troveadapter.ErrRangeUnsupported) {
		t.Skipf("driver does not support range reads: %v", err)
	}

	if err != nil {
		t.Fatalf("OpenRange: %v", err)
	}

	got, rerr := io.ReadAll(rc)
	if cerr := rc.Close(); cerr != nil {
		t.Fatalf("Close: %v", cerr)
	}

	if rerr != nil {
		t.Fatalf("ReadAll: %v", rerr)
	}

	if !bytes.Equal(got, []byte("234")) {
		t.Fatalf("range read = %q, want %q", got, "234")
	}
}

func TestTroveBackendName(t *testing.T) {
	if got := newBackend(t).Name(); got != "trove" {
		t.Fatalf("Name() = %q, want %q", got, "trove")
	}
}
