package artifacttest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
)

// RunStoreSuite exercises the artifact.Store contract. newStore must
// return a fresh, empty store on every call.
func RunStoreSuite(t *testing.T, newStore func() artifact.Store) {
	t.Helper()

	tests := []struct {
		name string
		fn   func(*testing.T, artifact.Store)
	}{
		{"CreateAndGet", testCreateAndGet},
		{"CreateDuplicateKey", testCreateDuplicateKey},
		{"GetMissing", testGetMissing},
		{"FindByKey", testFindByKey},
		{"UpdateHash", testUpdateHash},
		{"LinkAndList", testLinkAndList},
		{"LinkIdempotent", testLinkIdempotent},
		{"FindLinkByNameAcrossAttempts", testFindLinkAcrossAttempts},
		{"ListArtifacts", testListArtifacts},
		{"SweepNeverTouchesDurable", testSweepNeverTouchesDurable},
		{"SweepOrphans", testSweepOrphans},
		{"SweepOrphansSkipsLinked", testSweepOrphansSkipsLinked},
		{"SweepOrphansDryRun", testSweepOrphansRespectsLimit},
		{"PurgeFlow", testPurgeFlow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t, newStore())
		})
	}
}

func newArtifact(key string, lc artifact.Lifecycle) *artifact.Artifact {
	return &artifact.Artifact{
		ID:        id.NewArtifactID(),
		Backend:   "primary",
		Bucket:    "models",
		Key:       key,
		Size:      1024,
		Lifecycle: lc,
		CreatedAt: time.Now().UTC(),
	}
}

func newOwner() artifact.OwnerRef {
	return artifact.OwnerRef{Kind: artifact.OwnerJob, ID: id.NewJobID().String()}
}

func testCreateAndGet(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("tower.ifc", artifact.Durable)

	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	got, err := s.GetArtifact(ctx, a.ID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}

	if got.Key != a.Key || got.Size != a.Size || got.Lifecycle != a.Lifecycle {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, a)
	}
}

func testCreateDuplicateKey(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("dup.ifc", artifact.Durable)

	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("first CreateArtifact: %v", err)
	}

	b := newArtifact("dup.ifc", artifact.Durable)
	if err := s.CreateArtifact(ctx, b, nil); !errors.Is(err, artifact.ErrExists) {
		t.Fatalf("duplicate key error = %v, want ErrExists", err)
	}
}

func testGetMissing(t *testing.T, s artifact.Store) {
	if _, err := s.GetArtifact(context.Background(), id.NewArtifactID()); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("GetArtifact(missing) = %v, want ErrNotFound", err)
	}
}

func testFindByKey(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("find.ifc", artifact.Durable)

	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	got, err := s.FindArtifactByKey(ctx, "primary", "models", "find.ifc")
	if err != nil {
		t.Fatalf("FindArtifactByKey: %v", err)
	}

	if got.ID != a.ID {
		t.Fatalf("FindArtifactByKey ID = %v, want %v", got.ID, a.ID)
	}

	_, err = s.FindArtifactByKey(ctx, "primary", "models", "nope.ifc")
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("FindArtifactByKey(missing) = %v, want ErrNotFound", err)
	}
}

func testUpdateHash(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("hash.ifc", artifact.Durable)

	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	a.ContentHash = "blake3:9f2a"
	a.Size = 4096

	if err := s.UpdateArtifact(ctx, a); err != nil {
		t.Fatalf("UpdateArtifact: %v", err)
	}

	got, err := s.GetArtifact(ctx, a.ID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}

	if got.ContentHash != "blake3:9f2a" || got.Size != 4096 {
		t.Fatalf("update not persisted: hash=%q size=%d", got.ContentHash, got.Size)
	}
}

func testLinkAndList(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("linked.ifc", artifact.Ephemeral)
	owner := newOwner()
	link := &artifact.Link{
		ArtifactID: a.ID,
		OwnerKind:  owner.Kind,
		OwnerID:    owner.ID,
		Role:       artifact.RoleOutput,
		Name:       "mesh.glb",
		CreatedAt:  time.Now().UTC(),
	}

	if err := s.CreateArtifact(ctx, a, link); err != nil {
		t.Fatalf("CreateArtifact with link: %v", err)
	}

	links, err := s.ListLinks(ctx, owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}

	if len(links) != 1 || links[0].Name != "mesh.glb" {
		t.Fatalf("ListLinks = %+v, want one link named mesh.glb", links)
	}

	arts, err := s.ListArtifactsByOwner(ctx, owner, artifact.RoleOutput)
	if err != nil {
		t.Fatalf("ListArtifactsByOwner: %v", err)
	}

	if len(arts) != 1 || arts[0].ID != a.ID {
		t.Fatalf("ListArtifactsByOwner = %+v, want artifact %v", arts, a.ID)
	}

	none, err := s.ListArtifactsByOwner(ctx, owner, artifact.RoleInput)
	if err != nil {
		t.Fatalf("ListArtifactsByOwner(input): %v", err)
	}

	if len(none) != 0 {
		t.Fatalf("ListArtifactsByOwner(input) = %+v, want empty", none)
	}
}

func testLinkIdempotent(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	a := newArtifact("idem.ifc", artifact.Ephemeral)

	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	owner := newOwner()
	link := &artifact.Link{
		ArtifactID: a.ID,
		OwnerKind:  owner.Kind,
		OwnerID:    owner.ID,
		Role:       artifact.RoleOutput,
		Name:       "out.bin",
		CreatedAt:  time.Now().UTC(),
	}

	for i := range 2 {
		if err := s.LinkArtifact(ctx, link); err != nil {
			t.Fatalf("LinkArtifact call %d: %v", i, err)
		}
	}

	links, err := s.ListLinks(ctx, owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}

	if len(links) != 1 {
		t.Fatalf("ListLinks returned %d links, want 1 (link must be idempotent)", len(links))
	}
}

func testFindLinkAcrossAttempts(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	owner := newOwner()

	for attempt := range 3 {
		a := newArtifact(fmt.Sprintf("page-317-attempt-%d.png", attempt), artifact.Ephemeral)
		link := &artifact.Link{
			ArtifactID: a.ID,
			OwnerKind:  owner.Kind,
			OwnerID:    owner.ID,
			Role:       artifact.RoleOutput,
			Name:       "page-317.png",
			Attempt:    attempt,
			CreatedAt:  time.Now().UTC(),
		}

		if err := s.CreateArtifact(ctx, a, link); err != nil {
			t.Fatalf("CreateArtifact attempt %d: %v", attempt, err)
		}
	}

	got, err := s.FindLinkByName(ctx, owner, "page-317.png")
	if err != nil {
		t.Fatalf("FindLinkByName: %v", err)
	}

	if got.Attempt != 2 {
		t.Fatalf("FindLinkByName attempt = %d, want 2 (highest)", got.Attempt)
	}

	_, err = s.FindLinkByName(ctx, owner, "never-made.png")
	if !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("FindLinkByName(missing) = %v, want ErrNotFound", err)
	}
}

func testListArtifacts(t *testing.T, s artifact.Store) {
	ctx := context.Background()

	for i := range 3 {
		a := newArtifact(fmt.Sprintf("dur-%d.ifc", i), artifact.Durable)
		if err := s.CreateArtifact(ctx, a, nil); err != nil {
			t.Fatalf("CreateArtifact durable %d: %v", i, err)
		}
	}

	eph := newArtifact("eph.bin", artifact.Ephemeral)
	if err := s.CreateArtifact(ctx, eph, nil); err != nil {
		t.Fatalf("CreateArtifact ephemeral: %v", err)
	}

	all, err := s.ListArtifacts(ctx, artifact.ListOpts{})
	if err != nil {
		t.Fatalf("ListArtifacts: %v", err)
	}

	if len(all) != 4 {
		t.Fatalf("ListArtifacts returned %d, want 4", len(all))
	}

	durable, err := s.ListArtifacts(ctx, artifact.ListOpts{Lifecycle: artifact.Durable})
	if err != nil {
		t.Fatalf("ListArtifacts(durable): %v", err)
	}

	if len(durable) != 3 {
		t.Fatalf("ListArtifacts(durable) returned %d, want 3", len(durable))
	}

	limited, err := s.ListArtifacts(ctx, artifact.ListOpts{Limit: 2})
	if err != nil {
		t.Fatalf("ListArtifacts(limit 2): %v", err)
	}

	if len(limited) != 2 {
		t.Fatalf("ListArtifacts(limit 2) returned %d, want 2", len(limited))
	}
}

// testSweepNeverTouchesDurable is the safety invariant of the whole
// design. A durable artifact must be unreachable from any sweep path,
// regardless of age, links, or owner state.
func testSweepNeverTouchesDurable(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	long := time.Now().UTC().Add(-365 * 24 * time.Hour)

	durable := newArtifact("customer-upload.ifc", artifact.Durable)
	durable.CreatedAt = long

	if err := s.CreateArtifact(ctx, durable, nil); err != nil {
		t.Fatalf("CreateArtifact durable: %v", err)
	}

	// A durable artifact linked to an owner that no longer exists is the
	// most tempting sweep candidate there is. It must still survive.
	linked := newArtifact("customer-upload-2.ifc", artifact.Durable)
	linked.CreatedAt = long
	owner := newOwner()
	link := &artifact.Link{
		ArtifactID: linked.ID,
		OwnerKind:  owner.Kind,
		OwnerID:    owner.ID,
		Role:       artifact.RoleInput,
		Name:       "model",
		CreatedAt:  long,
	}

	if err := s.CreateArtifact(ctx, linked, link); err != nil {
		t.Fatalf("CreateArtifact linked durable: %v", err)
	}

	swept, err := s.SweepEphemeral(ctx, artifact.SweepOpts{Retention: 0, Limit: 100})
	if err != nil {
		t.Fatalf("SweepEphemeral: %v", err)
	}

	orphaned, err := s.SweepOrphans(ctx, time.Now().UTC(), 100)
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	for _, a := range append(swept, orphaned...) {
		if a.Lifecycle == artifact.Durable {
			t.Fatalf("a sweep marked DURABLE artifact %v — safety invariant violated", a.ID)
		}
	}

	for _, want := range []*artifact.Artifact{durable, linked} {
		got, err := s.GetArtifact(ctx, want.ID)
		if err != nil {
			t.Fatalf("durable artifact %v not retrievable after sweeps: %v", want.ID, err)
		}

		if got.IsDeleted() {
			t.Fatalf("durable artifact %v was soft-deleted — safety invariant violated", want.ID)
		}
	}
}

func testSweepOrphans(t *testing.T, s artifact.Store) {
	ctx := context.Background()

	old := newArtifact("orphan.bin", artifact.Ephemeral)
	old.CreatedAt = time.Now().UTC().Add(-48 * time.Hour)

	if err := s.CreateArtifact(ctx, old, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	fresh := newArtifact("fresh.bin", artifact.Ephemeral)
	if err := s.CreateArtifact(ctx, fresh, nil); err != nil {
		t.Fatalf("CreateArtifact fresh: %v", err)
	}

	cutoff := time.Now().UTC().Add(-24 * time.Hour)

	swept, err := s.SweepOrphans(ctx, cutoff, 100)
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	if len(swept) != 1 || swept[0].ID != old.ID {
		t.Fatalf("SweepOrphans = %+v, want only the 48h-old orphan", swept)
	}

	got, err := s.GetArtifact(ctx, fresh.ID)
	if err != nil {
		t.Fatalf("fresh orphan was swept: %v", err)
	}

	if got.IsDeleted() {
		t.Fatal("fresh orphan soft-deleted before its grace window elapsed")
	}
}

func testSweepOrphansSkipsLinked(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	long := time.Now().UTC().Add(-48 * time.Hour)

	a := newArtifact("has-link.bin", artifact.Ephemeral)
	a.CreatedAt = long
	owner := newOwner()
	link := &artifact.Link{
		ArtifactID: a.ID,
		OwnerKind:  owner.Kind,
		OwnerID:    owner.ID,
		Role:       artifact.RoleOutput,
		Name:       "out.bin",
		CreatedAt:  long,
	}

	if err := s.CreateArtifact(ctx, a, link); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	swept, err := s.SweepOrphans(ctx, time.Now().UTC(), 100)
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	for _, got := range swept {
		if got.ID == a.ID {
			t.Fatal("SweepOrphans marked a linked artifact — it is not an orphan")
		}
	}
}

func testSweepOrphansRespectsLimit(t *testing.T, s artifact.Store) {
	ctx := context.Background()
	long := time.Now().UTC().Add(-48 * time.Hour)

	for i := range 5 {
		a := newArtifact(fmt.Sprintf("orphan-%d.bin", i), artifact.Ephemeral)
		a.CreatedAt = long

		if err := s.CreateArtifact(ctx, a, nil); err != nil {
			t.Fatalf("CreateArtifact %d: %v", i, err)
		}
	}

	swept, err := s.SweepOrphans(ctx, time.Now().UTC(), 2)
	if err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	if len(swept) != 2 {
		t.Fatalf("SweepOrphans with limit 2 returned %d", len(swept))
	}
}

func testPurgeFlow(t *testing.T, s artifact.Store) {
	ctx := context.Background()

	a := newArtifact("purge.bin", artifact.Ephemeral)
	a.CreatedAt = time.Now().UTC().Add(-72 * time.Hour)

	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatalf("CreateArtifact: %v", err)
	}

	if _, err := s.SweepOrphans(ctx, time.Now().UTC().Add(-24*time.Hour), 100); err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	// A soft-deleted artifact is no longer served.
	if _, err := s.GetArtifact(ctx, a.ID); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("GetArtifact after soft delete = %v, want ErrNotFound", err)
	}

	// With a grace window longer than its age, it is not yet purgeable.
	notYet, err := s.ListPurgeable(ctx, time.Hour, 100)
	if err != nil {
		t.Fatalf("ListPurgeable(1h grace): %v", err)
	}

	if len(notYet) != 0 {
		t.Fatalf("ListPurgeable(1h grace) = %+v, want empty", notYet)
	}

	purgeable, err := s.ListPurgeable(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ListPurgeable: %v", err)
	}

	if len(purgeable) != 1 || purgeable[0].ID != a.ID {
		t.Fatalf("ListPurgeable = %+v, want the swept artifact", purgeable)
	}

	if perr := s.PurgeArtifact(ctx, a.ID); perr != nil {
		t.Fatalf("PurgeArtifact: %v", perr)
	}

	if _, gerr := s.GetArtifact(ctx, a.ID); !errors.Is(gerr, artifact.ErrNotFound) {
		t.Fatalf("GetArtifact after purge = %v, want ErrNotFound", gerr)
	}

	after, err := s.ListPurgeable(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ListPurgeable after purge: %v", err)
	}

	if len(after) != 0 {
		t.Fatalf("ListPurgeable after purge = %+v, want empty", after)
	}
}
