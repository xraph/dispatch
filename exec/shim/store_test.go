package shim

// This file is package shim (internal), not shim_test, deliberately
// breaking the external-tests-only convention the rest of this package
// follows -- the same exception internal_test.go documents. memStore and
// newMemStore are unexported: the point of these tests is the sandbox's
// own persistence semantics, not just what artifact.Service exposes
// through them, so there is no way to reach the type under test from an
// external package.

import (
	"context"
	"errors"
	osexec "os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

func newTestArtifact(key string) *artifact.Artifact {
	return &artifact.Artifact{
		ID:        id.NewArtifactID(),
		Backend:   "memory",
		Bucket:    "b",
		Key:       key,
		Lifecycle: artifact.Ephemeral,
		CreatedAt: time.Now().UTC(),
	}
}

func newTestOwner() artifact.OwnerRef {
	return artifact.OwnerRef{Kind: artifact.OwnerJob, ID: "job-1"}
}

func newTestLink(artifactID id.ArtifactID, owner artifact.OwnerRef, name string, attempt int) *artifact.Link {
	return &artifact.Link{
		ArtifactID: artifactID,
		OwnerKind:  owner.Kind,
		OwnerID:    owner.ID,
		Role:       artifact.RoleOutput,
		Name:       name,
		Attempt:    attempt,
		CreatedAt:  time.Now().UTC(),
	}
}

func TestMemStore_CreateArtifact(t *testing.T) {
	tests := []struct {
		name    string
		seed    bool
		wantErr error
	}{
		{name: "insert succeeds"},
		{name: "duplicate backend/bucket/key returns ErrExists", seed: true, wantErr: artifact.ErrExists},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newMemStore()
			ctx := context.Background()
			owner := newTestOwner()
			a := newTestArtifact("mesh.glb")

			if tt.seed {
				seed := newTestArtifact("mesh.glb")
				if err := s.CreateArtifact(ctx, seed, nil); err != nil {
					t.Fatalf("seed CreateArtifact() error = %v, want nil", err)
				}
			}

			link := newTestLink(a.ID, owner, "mesh", 0)

			err := s.CreateArtifact(ctx, a, link)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CreateArtifact() error = %v, want %v", err, tt.wantErr)
			}

			if tt.wantErr != nil {
				return
			}

			got, gerr := s.GetArtifact(ctx, a.ID)
			if gerr != nil {
				t.Fatalf("GetArtifact() error = %v, want nil", gerr)
			}

			if got.Key != a.Key {
				t.Errorf("GetArtifact().Key = %q, want %q", got.Key, a.Key)
			}
		})
	}
}

func TestMemStore_GetArtifact(t *testing.T) {
	tests := []struct {
		name    string
		lookup  func(seeded id.ArtifactID) id.ArtifactID
		wantErr error
	}{
		{
			name:   "known id returns the artifact",
			lookup: func(seeded id.ArtifactID) id.ArtifactID { return seeded },
		},
		{
			name:    "unknown id returns ErrNotFound",
			lookup:  func(id.ArtifactID) id.ArtifactID { return id.NewArtifactID() },
			wantErr: artifact.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newMemStore()
			ctx := context.Background()
			a := newTestArtifact("mesh.glb")

			if err := s.CreateArtifact(ctx, a, nil); err != nil {
				t.Fatalf("CreateArtifact() error = %v, want nil", err)
			}

			_, err := s.GetArtifact(ctx, tt.lookup(a.ID))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("GetArtifact() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestMemStore_FindArtifactByKey(t *testing.T) {
	tests := []struct {
		name      string
		lookupKey string
		wantErr   error
	}{
		{name: "finds what CreateArtifact inserted", lookupKey: "mesh.glb"},
		{name: "unknown key returns ErrNotFound", lookupKey: "missing.glb", wantErr: artifact.ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newMemStore()
			ctx := context.Background()
			a := newTestArtifact("mesh.glb")

			if err := s.CreateArtifact(ctx, a, nil); err != nil {
				t.Fatalf("CreateArtifact() error = %v, want nil", err)
			}

			got, err := s.FindArtifactByKey(ctx, a.Backend, a.Bucket, tt.lookupKey)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("FindArtifactByKey() error = %v, want %v", err, tt.wantErr)
			}

			if tt.wantErr != nil {
				return
			}

			if got.ID != a.ID {
				t.Errorf("FindArtifactByKey().ID = %v, want %v", got.ID, a.ID)
			}
		})
	}
}

func TestMemStore_FindLinkByName(t *testing.T) {
	t.Run("returns the highest attempt when two links share a name", func(t *testing.T) {
		s := newMemStore()
		ctx := context.Background()
		owner := newTestOwner()

		older := newTestArtifact("v0.glb")
		newer := newTestArtifact("v1.glb")

		if err := s.CreateArtifact(ctx, older, newTestLink(older.ID, owner, "mesh", 0)); err != nil {
			t.Fatalf("CreateArtifact(older) error = %v, want nil", err)
		}

		if err := s.CreateArtifact(ctx, newer, newTestLink(newer.ID, owner, "mesh", 1)); err != nil {
			t.Fatalf("CreateArtifact(newer) error = %v, want nil", err)
		}

		got, err := s.FindLinkByName(ctx, owner, "mesh")
		if err != nil {
			t.Fatalf("FindLinkByName() error = %v, want nil", err)
		}

		if got.ArtifactID != newer.ID {
			t.Errorf("FindLinkByName().ArtifactID = %v, want %v", got.ArtifactID, newer.ID)
		}

		if got.Attempt != 1 {
			t.Errorf("FindLinkByName().Attempt = %d, want 1", got.Attempt)
		}
	})

	t.Run("no attempt has produced the name returns ErrNotFound", func(t *testing.T) {
		s := newMemStore()

		_, err := s.FindLinkByName(context.Background(), newTestOwner(), "mesh")
		if !errors.Is(err, artifact.ErrNotFound) {
			t.Fatalf("FindLinkByName() error = %v, want %v", err, artifact.ErrNotFound)
		}
	})
}

// TestMemStore_ConcurrentCreateArtifact proves memStore's mutex actually
// guards the map: several goroutines each creating a distinct artifact
// must not corrupt the store or trip the race detector, and every
// artifact created must be readable afterward.
func TestMemStore_ConcurrentCreateArtifact(t *testing.T) {
	s := newMemStore()
	ctx := context.Background()
	owner := newTestOwner()

	const n = 32

	artifacts := make([]*artifact.Artifact, n)
	for i := range artifacts {
		artifacts[i] = newTestArtifact("out-" + strconv.Itoa(i) + ".glb")
	}

	var wg sync.WaitGroup

	errs := make([]error, n)

	for i := range artifacts {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			link := newTestLink(artifacts[i].ID, owner, artifacts[i].Key, 0)
			errs[i] = s.CreateArtifact(ctx, artifacts[i], link)
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("CreateArtifact(%d) error = %v, want nil", i, err)
		}
	}

	for i, a := range artifacts {
		if _, err := s.GetArtifact(ctx, a.ID); err != nil {
			t.Errorf("GetArtifact(%d) error = %v, want nil", i, err)
		}
	}
}

// TestShimLinksNoInfrastructure fails if the sandbox binary gains an
// import that could reach a credential, a socket, or a config file. The
// phase's central claim is that this process holds none of those, and
// that claim should be checkable by inspection rather than by tracing
// which package-level variables happen not to be constructed.
func TestShimLinksNoInfrastructure(t *testing.T) {
	out, err := osexec.CommandContext(context.Background(), "go", "list", "-deps", "github.com/xraph/dispatch/exec/shim").Output()
	if err != nil {
		t.Skipf("go list unavailable: %v", err)
	}

	forbidden := []string{"go-redis", "client-go", "confy", "xraph/forge"}

	for _, dep := range strings.Split(string(out), "\n") {
		for _, bad := range forbidden {
			if strings.Contains(dep, bad) {
				t.Errorf("exec/shim links %q, which must not be reachable from a sandbox", dep)
			}
		}
	}
}
