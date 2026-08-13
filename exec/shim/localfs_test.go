package shim_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/exec/shim"
)

func TestLocalFS_CreateThenOpen(t *testing.T) {
	root := t.TempDir()
	be := shim.NewLocalFS(root)
	ctx := context.Background()

	w, err := be.Create(ctx, "b", "ephemeral/job/j1/0/mesh.glb")
	if err != nil {
		t.Fatalf("Create() = %v", err)
	}
	if _, werr := io.WriteString(w, "hello"); werr != nil {
		t.Fatalf("Write() = %v", werr)
	}
	if _, cerr := w.Commit(ctx); cerr != nil {
		t.Fatalf("Commit() = %v", cerr)
	}

	// The bytes must be a real file under root, at the key's path, so the
	// parent can collect outputs without cooperating with the child.
	onDisk := filepath.Join(root, "ephemeral/job/j1/0/mesh.glb")
	got, err := os.ReadFile(onDisk)
	if err != nil {
		t.Fatalf("expected a file at %s: %v", onDisk, err)
	}
	if string(got) != "hello" {
		t.Errorf("file = %q, want %q", got, "hello")
	}

	rc, err := be.Open(ctx, artifact.Ref{Backend: "localfs", Bucket: "b", Key: "ephemeral/job/j1/0/mesh.glb"})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	defer rc.Close()
	back, _ := io.ReadAll(rc)
	if string(back) != "hello" {
		t.Errorf("Open() = %q, want %q", back, "hello")
	}
}

func TestLocalFS_OpenMissingIsNotFound(t *testing.T) {
	be := shim.NewLocalFS(t.TempDir())
	_, err := be.Open(context.Background(), artifact.Ref{Bucket: "b", Key: "nope"})
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Open(missing) = %v, want ErrNotFound", err)
	}
}

func TestLocalFS_AbortLeavesNoFile(t *testing.T) {
	root := t.TempDir()
	be := shim.NewLocalFS(root)

	w, err := be.Create(context.Background(), "b", "k")
	if err != nil {
		t.Fatalf("Create() = %v", err)
	}
	if _, err := io.WriteString(w, "partial"); err != nil {
		t.Fatalf("Write() = %v", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("Abort() = %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, "k")); !os.IsNotExist(err) {
		t.Error("Abort() left a file behind; an aborted write must be invisible")
	}
}

func TestLocalFS_RejectsEscapingKey(t *testing.T) {
	// A key is attacker-influenced in the general case. It must never
	// resolve outside root. A key that lexically collapses to root itself
	// ("", ".", "a/..", "a/b/../..") is equally dangerous even though it
	// never leaves root: Create would write into root's parent (Dir of
	// root), and Delete would remove the whole scratch directory instead
	// of one object.
	be := shim.NewLocalFS(t.TempDir())
	ctx := context.Background()
	keys := []string{
		"../escape", "a/../../escape", "/absolute",
		"", ".", "a/..", "a/b/../..",
	}
	for _, key := range keys {
		if _, err := be.Create(ctx, "b", key); err == nil {
			t.Errorf("Create(%q) = nil, want a rejection", key)
		}
		if err := be.Delete(ctx, artifact.Ref{Bucket: "b", Key: key}); err == nil {
			t.Errorf("Delete(%q) = nil, want a rejection", key)
		}
	}
}

func TestLocalFS_Name(t *testing.T) {
	be := shim.NewLocalFS(t.TempDir())
	if got := be.Name(); got != "localfs" {
		t.Errorf("Name() = %q, want %q", got, "localfs")
	}
}

func TestLocalFS_DeleteMissingIsNotError(t *testing.T) {
	be := shim.NewLocalFS(t.TempDir())
	err := be.Delete(context.Background(), artifact.Ref{Bucket: "b", Key: "nope"})
	if err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}

func TestLocalFS_StatReportsSize(t *testing.T) {
	root := t.TempDir()
	be := shim.NewLocalFS(root)
	ctx := context.Background()

	w, err := be.Create(ctx, "b", "k")
	if err != nil {
		t.Fatalf("Create() = %v", err)
	}
	if _, werr := io.WriteString(w, "hello world"); werr != nil {
		t.Fatalf("Write() = %v", werr)
	}
	info, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() = %v", err)
	}
	if info.Size != int64(len("hello world")) {
		t.Errorf("Commit() size = %d, want %d", info.Size, len("hello world"))
	}
	if info.ETag == "" {
		t.Errorf("Commit() ETag is empty, want a content hash")
	}

	stat, err := be.Stat(ctx, artifact.Ref{Bucket: "b", Key: "k"})
	if err != nil {
		t.Fatalf("Stat() = %v", err)
	}
	if stat.Size != int64(len("hello world")) {
		t.Errorf("Stat() size = %d, want %d", stat.Size, len("hello world"))
	}
}

func TestLocalFS_CommitAbortIsNoOp(t *testing.T) {
	root := t.TempDir()
	be := shim.NewLocalFS(root)
	ctx := context.Background()

	w, err := be.Create(ctx, "b", "k")
	if err != nil {
		t.Fatalf("Create() = %v", err)
	}
	if _, err := io.WriteString(w, "hello"); err != nil {
		t.Fatalf("Write() = %v", err)
	}
	if _, err := w.Commit(ctx); err != nil {
		t.Fatalf("Commit() = %v", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("Abort() after Commit() = %v, want nil", err)
	}

	// The committed file must still be present; Abort after Commit must
	// not remove it.
	if _, err := os.Stat(filepath.Join(root, "k")); err != nil {
		t.Errorf("file gone after post-commit Abort(): %v", err)
	}
}

func TestLocalFS_FileModeIsNotWorldReadable(t *testing.T) {
	root := t.TempDir()
	be := shim.NewLocalFS(root)
	ctx := context.Background()

	w, err := be.Create(ctx, "b", "k")
	if err != nil {
		t.Fatalf("Create() = %v", err)
	}
	if _, werr := io.WriteString(w, "hello"); werr != nil {
		t.Fatalf("Write() = %v", werr)
	}
	if _, cerr := w.Commit(ctx); cerr != nil {
		t.Fatalf("Commit() = %v", cerr)
	}

	info, err := os.Stat(filepath.Join(root, "k"))
	if err != nil {
		t.Fatalf("Stat() = %v", err)
	}
	if info.Mode().Perm()&0o007 != 0 {
		t.Errorf("file mode = %v, must not be world-accessible", info.Mode().Perm())
	}
}
