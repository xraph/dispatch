package contract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
)

func runArtifactRecordReads(t *testing.T, s store.Store) {
	t.Helper()
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	reader, ok := s.(interface {
		GetArtifactRecord(context.Context, id.ArtifactID) (*artifact.Artifact, error)
	})
	if !ok {
		t.Fatal("store does not inspect deleted artifact metadata")
	}
	expires := time.Now().UTC().Add(time.Hour)
	a := &artifact.Artifact{ID: id.NewArtifactID(), Backend: "memory", Bucket: "inputs", Key: "retained", Size: 42, Lifecycle: artifact.Ephemeral, CreatedAt: time.Now().Add(-time.Hour), ExpiresAt: &expires}
	if err := s.CreateArtifact(ctx, a, nil); err != nil {
		t.Fatal(err)
	}
	first, err := reader.GetArtifactRecord(ctx, a.ID)
	if err != nil || first.ID != a.ID || first.Size != 42 {
		t.Fatalf("live=%+v, %v", first, err)
	}
	first.Size = 999
	*first.ExpiresAt = expires.Add(time.Hour)
	again, err := reader.GetArtifactRecord(ctx, a.ID)
	if err != nil || again.Size != 42 || again.ExpiresAt == nil || again.ExpiresAt.Sub(expires).Abs() > time.Millisecond {
		t.Fatalf("aliased metadata=%+v, %v", again, err)
	}
	swept, err := s.SweepOrphans(ctx, time.Now(), 10)
	if err != nil || len(swept) != 1 {
		t.Fatalf("sweep=%v, %v", swept, err)
	}
	deleted, err := reader.GetArtifactRecord(ctx, a.ID)
	if err != nil || deleted.DeletedAt == nil {
		t.Fatalf("deleted=%+v, %v", deleted, err)
	}
	if _, readErr := s.GetArtifact(ctx, a.ID); !errors.Is(readErr, artifact.ErrNotFound) {
		t.Fatalf("deleted artifact served: %v", readErr)
	}
	if err := s.PurgeArtifact(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, readErr := reader.GetArtifactRecord(ctx, a.ID); !errors.Is(readErr, artifact.ErrNotFound) {
		t.Fatalf("purged metadata=%v", readErr)
	}
	if _, readErr := reader.GetArtifactRecord(ctx, id.NewArtifactID()); !errors.Is(readErr, artifact.ErrNotFound) {
		t.Fatalf("missing metadata=%v", readErr)
	}
}
func TestArtifactRecordReadsMemoryAndSQLite(t *testing.T) {
	t.Run("memory", func(t *testing.T) { runArtifactRecordReads(t, memory.New()) })
	t.Run("sqlite", func(t *testing.T) { runArtifactRecordReads(t, sqliteContractStore(t)) })
}
