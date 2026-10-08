package mongo_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/storetest"
	"github.com/xraph/dispatch/workflow"
)

// The three operator suites run the way TestListSuite does: one container
// and one migrated store per suite, shared by every case. Each case works
// on rows it created under names nobody else uses, which the suites
// document as safe. Migrate runs first so GetDLQByJobID reads through the
// index production has.

func TestDLQReplayConformance(t *testing.T) {
	uri := startMongo(t)
	shared := openStore(t, uri)

	if err := shared.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	storetest.RunDLQReplaySuite(t, func(t *testing.T) storetest.DLQReplayStore {
		t.Helper()

		return shared
	})
}

func TestCronConformance(t *testing.T) {
	uri := startMongo(t)
	shared := openStore(t, uri)

	if err := shared.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	storetest.RunCronSuite(t, func(t *testing.T) storetest.CronStore {
		t.Helper()

		return shared
	})
}

func TestWorkflowConformance(t *testing.T) {
	uri := startMongo(t)
	shared := openStore(t, uri)

	if err := shared.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	storetest.RunWorkflowSuite(t, func(t *testing.T) storetest.WorkflowStore {
		t.Helper()

		return shared
	})
}

func TestReopenLegacyRunWithoutGeneration(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	ctx := context.Background()
	run := &workflow.Run{Entity: dispatch.NewEntity(), ID: id.NewRunID(), Name: "legacy", State: workflow.RunStateCompleted, StartedAt: time.Now().UTC()}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	col := rawDatabase(t, uri).Collection("dispatch_workflow_runs")
	if _, err := col.UpdateOne(ctx, bson.M{"_id": run.ID.String()}, bson.M{"$unset": bson.M{"replay_generation": ""}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReopenRun(ctx, run.ID, 0); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ReplayGeneration != 1 {
		t.Fatalf("generation = %d, want 1", got.ReplayGeneration)
	}
	got.State = workflow.RunStateCompleted
	if err := s.UpdateRun(ctx, got); err != nil {
		t.Fatal(err)
	}
	if err := s.ReopenRun(ctx, run.ID, 0); !errors.Is(err, dispatch.ErrInvalidState) {
		t.Fatalf("stale claim = %v", err)
	}
}

// TestClaimReplayMatchesBothUnreplayedShapes covers the document shape the
// suite cannot produce. PushDLQ goes through grove's insert, which writes
// replayed_at as an explicit null; an entry written by the raw driver, or
// one ReleaseReplay has cleared, has no replayed_at key at all. The claim
// filter has to treat both as unreplayed, and a claimed entry as claimed.
func TestClaimReplayMatchesBothUnreplayedShapes(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	col := rawDatabase(t, uri).Collection("dispatch_dlq")
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	entry := func(name string) *dlq.Entry {
		return &dlq.Entry{
			ID:        id.NewDLQID(),
			JobID:     id.NewJobID(),
			JobName:   name,
			Queue:     "claim-shapes",
			Payload:   []byte(`{}`),
			Error:     "boom",
			FailedAt:  now,
			CreatedAt: now,
		}
	}

	nullKey, absentKey := entry("null-shape"), entry("absent-shape")
	for _, e := range []*dlq.Entry{nullKey, absentKey} {
		if err := s.PushDLQ(ctx, e); err != nil {
			t.Fatalf("push %s: %v", e.JobName, err)
		}
	}

	if _, err := col.UpdateOne(ctx,
		bson.M{"_id": absentKey.ID.String()},
		bson.M{"$unset": bson.M{"replayed_at": "", "replayed_job_id": ""}},
	); err != nil {
		t.Fatalf("unset replay fields: %v", err)
	}

	// The two shapes really are different, or the rest proves nothing.
	var nullDoc, absentDoc bson.M
	if err := col.FindOne(ctx, bson.M{"_id": nullKey.ID.String()}).Decode(&nullDoc); err != nil {
		t.Fatalf("read null doc: %v", err)
	}
	if v, ok := nullDoc["replayed_at"]; !ok || v != nil {
		t.Fatalf("pushed replayed_at = %#v (present=%t), want present and null", v, ok)
	}
	if err := col.FindOne(ctx, bson.M{"_id": absentKey.ID.String()}).Decode(&absentDoc); err != nil {
		t.Fatalf("read absent doc: %v", err)
	}
	if v, ok := absentDoc["replayed_at"]; ok {
		t.Fatalf("unset replayed_at = %#v, want key absent", v)
	}

	for _, e := range []*dlq.Entry{nullKey, absentKey} {
		jobID := id.NewJobID()
		if err := s.ClaimReplay(ctx, e.ID, jobID); err != nil {
			t.Fatalf("ClaimReplay(%s): %v", e.JobName, err)
		}

		got, err := s.GetDLQ(ctx, e.ID)
		if err != nil {
			t.Fatalf("GetDLQ(%s): %v", e.JobName, err)
		}
		if got.ReplayedAt == nil || got.ReplayedJobID == nil || *got.ReplayedJobID != jobID {
			t.Errorf("%s after claim: ReplayedAt %v, ReplayedJobID %v, want both set to the claim",
				e.JobName, got.ReplayedAt, got.ReplayedJobID)
		}

		if err := s.ClaimReplay(ctx, e.ID, id.NewJobID()); !errors.Is(err, dispatch.ErrDLQAlreadyReplayed) {
			t.Errorf("second ClaimReplay(%s) error = %v, want ErrDLQAlreadyReplayed", e.JobName, err)
		}
	}
}

// TestRunVersionRoundTrips covers what the workflow suite cannot see: it
// compares one store read with another, so a model that drops Version
// reads back 0 on both sides and passes. The runner resumes a run on the
// definition version stamped on it, so a dropped version would resume a
// version 3 run on version 1.
func TestRunVersionRoundTrips(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	ctx := context.Background()

	run := &workflow.Run{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewRunID(),
		Name:      "versioned",
		State:     workflow.RunStateRunning,
		StartedAt: time.Now().UTC().Truncate(time.Millisecond),
		Version:   3,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.Version != 3 {
		t.Fatalf("Version = %d after a round trip, want 3", got.Version)
	}
}
