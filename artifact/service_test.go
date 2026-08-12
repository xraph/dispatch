package artifact_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/memory"
)

func newService(t *testing.T) (*artifact.Service, *artifacttest.Backend, artifact.Store) {
	t.Helper()

	b := artifacttest.NewBackend()
	st := memory.New()
	svc := artifact.NewService(st, b,
		artifact.WithEphemeralPrefix("ephemeral"),
		artifact.WithDefaultBucket("dispatch"))

	return svc, b, st
}

func newJobOwner() artifact.OwnerRef {
	return artifact.OwnerRef{Kind: artifact.OwnerJob, ID: id.NewJobID().String()}
}

func TestRegisterDurable(t *testing.T) {
	ctx := context.Background()
	svc, b, _ := newService(t)
	b.Put("models", "tower.ifc", []byte("0123456789"))

	ref, err := svc.Register(ctx, "models", "tower.ifc")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if ref.Size != 10 {
		t.Fatalf("ref.Size = %d, want 10 (Register must Stat)", ref.Size)
	}

	if ref.ID.Prefix() != id.PrefixArtifact {
		t.Fatalf("ref.ID prefix = %q, want %q", ref.ID.Prefix(), id.PrefixArtifact)
	}

	if ref.ContentHash != "" {
		t.Fatal("Register must not hash — hashing is deferred to first staging")
	}
}

func TestRegisterMissingObject(t *testing.T) {
	svc, _, _ := newService(t)

	_, err := svc.Register(context.Background(), "models", "nope.ifc")
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("Register(missing) = %v, want ErrNotFound", err)
	}
}

func TestRegisterIsIdempotent(t *testing.T) {
	ctx := context.Background()
	svc, b, _ := newService(t)
	b.Put("models", "same.ifc", []byte("abc"))

	first, err := svc.Register(ctx, "models", "same.ifc")
	if err != nil {
		t.Fatalf("first Register: %v", err)
	}

	second, err := svc.Register(ctx, "models", "same.ifc")
	if err != nil {
		t.Fatalf("second Register: %v", err)
	}

	if first.ID != second.ID {
		t.Fatalf("Register not idempotent: %v then %v", first.ID, second.ID)
	}
}

func TestCreateCommitLinksOutput(t *testing.T) {
	ctx := context.Background()
	svc, _, st := newService(t)
	owner := newJobOwner()

	w, err := svc.Create(ctx, owner, 0, "mesh.glb",
		artifact.ContentType("model/gltf-binary"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, cerr := io.Copy(w, strings.NewReader("meshbytes")); cerr != nil {
		t.Fatalf("Copy: %v", cerr)
	}

	ref, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if ref.Size != 9 {
		t.Fatalf("ref.Size = %d, want 9", ref.Size)
	}

	if !strings.Contains(ref.Key, "/0/mesh.glb") {
		t.Fatalf("ephemeral key %q must embed the attempt", ref.Key)
	}

	links, err := st.ListLinks(ctx, owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}

	if len(links) != 1 || links[0].Role != artifact.RoleOutput {
		t.Fatalf("links = %+v, want one output link", links)
	}

	a, err := st.GetArtifact(ctx, ref.ID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}

	if a.Lifecycle != artifact.Ephemeral {
		t.Fatalf("created artifact lifecycle = %q, want ephemeral", a.Lifecycle)
	}
}

func TestCreateKeysDifferPerAttempt(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newService(t)
	owner := newJobOwner()

	keys := make(map[string]bool)

	for attempt := range 3 {
		w, err := svc.Create(ctx, owner, attempt, "mesh.glb")
		if err != nil {
			t.Fatalf("Create attempt %d: %v", attempt, err)
		}

		if _, werr := w.Write([]byte("x")); werr != nil {
			t.Fatalf("Write attempt %d: %v", attempt, werr)
		}

		ref, err := w.Commit(ctx)
		if err != nil {
			t.Fatalf("Commit attempt %d: %v", attempt, err)
		}

		if keys[ref.Key] {
			t.Fatalf("attempt %d reused key %q — the unique constraint would fire", attempt, ref.Key)
		}

		keys[ref.Key] = true
	}
}

func TestCreateIfAbsentFindsPriorAttempt(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newService(t)
	owner := newJobOwner()

	w, err := svc.Create(ctx, owner, 0, "page-317.png")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, werr := w.Write([]byte("pixels")); werr != nil {
		t.Fatalf("Write: %v", werr)
	}

	first, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if _, aerr := svc.Create(ctx, owner, 1, "page-317.png", artifact.IfAbsent()); !errors.Is(aerr, artifact.ErrExists) {
		t.Fatalf("IfAbsent on attempt 1 = %v, want ErrExists", aerr)
	}

	existing, err := svc.FindExisting(ctx, owner, "page-317.png")
	if err != nil {
		t.Fatalf("FindExisting: %v", err)
	}

	if existing.ID != first.ID {
		t.Fatalf("FindExisting = %v, want the attempt-0 artifact %v", existing.ID, first.ID)
	}
}

func TestCreateIfAbsentAllowsNewName(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newService(t)
	owner := newJobOwner()

	w, err := svc.Create(ctx, owner, 1, "page-318.png", artifact.IfAbsent())
	if err != nil {
		t.Fatalf("Create(IfAbsent) on a fresh name: %v", err)
	}

	if err := w.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
}

func TestAbortLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()
	svc, b, st := newService(t)
	owner := newJobOwner()

	w, err := svc.Create(ctx, owner, 0, "partial.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, werr := w.Write([]byte("half")); werr != nil {
		t.Fatalf("Write: %v", werr)
	}

	if aerr := w.Abort(); aerr != nil {
		t.Fatalf("Abort: %v", aerr)
	}

	links, err := st.ListLinks(ctx, owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}

	if len(links) != 0 {
		t.Fatalf("aborted write left %d links, want 0", len(links))
	}

	if b.Creates() != 1 {
		t.Fatalf("Creates() = %d, want 1", b.Creates())
	}
}

func TestAbortAfterCommitIsNoOp(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newService(t)
	owner := newJobOwner()

	w, err := svc.Create(ctx, owner, 0, "out.bin")
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
}

func TestCreateRejectsUnsafeNames(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newService(t)
	owner := newJobOwner()

	tests := []struct {
		name string
		arg  string
	}{
		{"empty", ""},
		{"slash", "a/b.png"},
		{"backslash", `a\b.png`},
		{"parent traversal", "../escape.png"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := svc.Create(ctx, owner, 0, tt.arg); err == nil {
				t.Fatalf("Create(%q) succeeded, want an error", tt.arg)
			}
		})
	}
}

func TestRetainSetsExpiry(t *testing.T) {
	ctx := context.Background()
	svc, _, st := newService(t)
	owner := newJobOwner()

	w, err := svc.Create(ctx, owner, 0, "temp.bin", artifact.Retain(time.Hour))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, werr := w.Write([]byte("x")); werr != nil {
		t.Fatalf("Write: %v", werr)
	}

	ref, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	a, err := st.GetArtifact(ctx, ref.ID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}

	if a.ExpiresAt == nil {
		t.Fatal("Retain did not set ExpiresAt")
	}
}

func TestDisabledServiceReturnsErrNoBackend(t *testing.T) {
	ctx := context.Background()
	svc := artifact.NewService(memory.New(), nil)

	if svc.Enabled() {
		t.Fatal("service with a nil backend reports enabled")
	}

	if _, err := svc.Register(ctx, "b", "k"); !errors.Is(err, artifact.ErrNoBackend) {
		t.Fatalf("Register = %v, want ErrNoBackend", err)
	}

	if _, err := svc.Create(ctx, newJobOwner(), 0, "x.bin"); !errors.Is(err, artifact.ErrNoBackend) {
		t.Fatalf("Create = %v, want ErrNoBackend", err)
	}
}

func TestLinkInput(t *testing.T) {
	ctx := context.Background()
	svc, b, st := newService(t)
	b.Put("models", "in.ifc", []byte("data"))

	ref, err := svc.Register(ctx, "models", "in.ifc")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	owner := newJobOwner()
	if lerr := svc.Link(ctx, ref, owner, artifact.RoleInput, "model", 0); lerr != nil {
		t.Fatalf("Link: %v", lerr)
	}

	arts, err := st.ListArtifactsByOwner(ctx, owner, artifact.RoleInput)
	if err != nil {
		t.Fatalf("ListArtifactsByOwner: %v", err)
	}

	if len(arts) != 1 || arts[0].ID != ref.ID {
		t.Fatalf("ListArtifactsByOwner = %+v, want the registered input", arts)
	}
}
