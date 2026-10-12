//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/grove/kv"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	redisstore "github.com/xraph/dispatch/store/redis"
	"github.com/xraph/dispatch/store/storetest"
	"github.com/xraph/dispatch/workflow"
)

// assertIndexed checks that member sits in the created-order index at key
// with the millisecond minted into its ID as its score.
func assertIndexed(t *testing.T, kvStore *kv.Store, key string, member id.ID) {
	t.Helper()

	score, ok, err := kvStore.ZScore(context.Background(), key, member.String())
	if err != nil {
		t.Fatalf("ZSCORE %s %s: %v", key, member, err)
	}
	if !ok {
		t.Fatalf("%s is not in %s", member, key)
	}
	if want := float64(member.Time().UnixMilli()); score != want {
		t.Fatalf("%s scored %v in %s, want %v", member, score, key, want)
	}
}

func assertNotIndexed(t *testing.T, kvStore *kv.Store, key string, member id.ID) {
	t.Helper()

	_, ok, err := kvStore.ZScore(context.Background(), key, member.String())
	if err != nil {
		t.Fatalf("ZSCORE %s %s: %v", key, member, err)
	}
	if ok {
		t.Fatalf("%s is still in %s after its row was deleted", member, key)
	}
}

// Every write that creates or removes a job, run, dead letter or artifact
// keeps that entity's created-order index in step. The key names are
// spelled out so a rename shows up here as well as in the keys test.
func TestCreatedIndex_followsEveryCreateAndDelete(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	s := redisstore.New(kvStore)

	t.Run("jobs", func(t *testing.T) {
		j := storetest.PendingJob("indexed", "created-index", 0)
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("EnqueueJob: %v", err)
		}
		assertIndexed(t, kvStore, "dispatch:job_by_created", j.ID)

		if err := s.DeleteJob(ctx, j.ID); err != nil {
			t.Fatalf("DeleteJob: %v", err)
		}
		assertNotIndexed(t, kvStore, "dispatch:job_by_created", j.ID)
	})

	t.Run("runs", func(t *testing.T) {
		r := &workflow.Run{
			Entity:    dispatch.NewEntity(),
			ID:        id.NewRunID(),
			Name:      "indexed",
			State:     workflow.RunStateRunning,
			StartedAt: time.Now().UTC(),
		}
		if err := s.CreateRun(ctx, r); err != nil {
			t.Fatalf("CreateRun: %v", err)
		}
		assertIndexed(t, kvStore, "dispatch:run_by_created", r.ID)
	})

	t.Run("dead letters", func(t *testing.T) {
		failed := time.Now().UTC().Add(-time.Hour)
		e := &dlq.Entry{
			ID:        id.NewDLQID(),
			JobID:     id.NewJobID(),
			JobName:   "indexed",
			Queue:     "created-index",
			Payload:   []byte(`{}`),
			Error:     "boom",
			FailedAt:  failed,
			CreatedAt: failed,
		}
		if err := s.PushDLQ(ctx, e); err != nil {
			t.Fatalf("PushDLQ: %v", err)
		}
		assertIndexed(t, kvStore, "dispatch:dlq_by_created", e.ID)

		if _, err := s.PurgeDLQ(ctx, time.Now().UTC()); err != nil {
			t.Fatalf("PurgeDLQ: %v", err)
		}
		assertNotIndexed(t, kvStore, "dispatch:dlq_by_created", e.ID)
	})

	t.Run("artifacts", func(t *testing.T) {
		artID := id.NewArtifactID()
		a := &artifact.Artifact{
			ID:        artID,
			Backend:   "mem",
			Bucket:    "created-index",
			Key:       artID.String(),
			Size:      1,
			Lifecycle: artifact.Durable,
			CreatedAt: time.Now().UTC(),
		}
		if err := s.CreateArtifact(ctx, a, nil); err != nil {
			t.Fatalf("CreateArtifact: %v", err)
		}
		assertIndexed(t, kvStore, "dispatch:artifact_by_created", a.ID)

		if err := s.PurgeArtifact(ctx, a.ID); err != nil {
			t.Fatalf("PurgeArtifact: %v", err)
		}
		assertNotIndexed(t, kvStore, "dispatch:artifact_by_created", a.ID)
	})
}
