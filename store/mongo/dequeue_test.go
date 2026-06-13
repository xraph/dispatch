package mongo_test

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/mongodriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	mongostore "github.com/xraph/dispatch/store/mongo"
)

const testDBName = "dispatch_dequeue_test"

// startMongo launches a disposable mongod and returns its URI. Skips when
// -short is set or no container runtime is available.
func startMongo(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed integration test in -short mode")
	}

	ctx := context.Background()
	ctr, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}
	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(ctr); termErr != nil {
			t.Errorf("terminate mongo container: %v", termErr)
		}
	})

	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("mongo connection string: %v", err)
	}
	return uri
}

func openStore(t *testing.T, uri string) *mongostore.Store {
	t.Helper()
	ctx := context.Background()

	drv := mongodriver.New()
	if err := drv.Open(ctx, uri, mongodriver.WithDatabase(testDBName)); err != nil {
		t.Fatalf("open mongodriver: %v", err)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatalf("grove open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return mongostore.New(db)
}

func rawDatabase(t *testing.T, uri string) *mongod.Database {
	t.Helper()
	client, err := mongod.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("raw mongo connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client.Database(testDBName)
}

// countJobWriteCommands returns the number of profiled write operations
// against the jobs collection.
func countJobWriteCommands(t *testing.T, mdb *mongod.Database) int {
	t.Helper()
	ctx := context.Background()

	cursor, err := mdb.Collection("system.profile").Find(ctx, bson.M{
		"$or": bson.A{
			bson.M{"op": bson.M{"$in": bson.A{"update", "insert", "remove"}}},
			bson.M{"command.findAndModify": bson.M{"$exists": true}},
			bson.M{"command.findandmodify": bson.M{"$exists": true}},
		},
		"ns": testDBName + ".dispatch_jobs",
	})
	if err != nil {
		t.Fatalf("query system.profile: %v", err)
	}
	var entries []bson.M
	if err := cursor.All(ctx, &entries); err != nil {
		t.Fatalf("decode system.profile: %v", err)
	}
	return len(entries)
}

// TestDequeueJobsIdleIssuesNoWriteCommands locks in the read-gate: polling
// empty queues must not send write commands (findAndModify) to the server,
// otherwise idle worker pools generate constant write traffic.
func TestDequeueJobsIdleIssuesNoWriteCommands(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	mdb := rawDatabase(t, uri)
	ctx := context.Background()

	if err := mdb.RunCommand(ctx, bson.D{{Key: "profile", Value: 2}}).Err(); err != nil {
		t.Fatalf("enable profiling: %v", err)
	}

	for range 5 {
		jobs, err := s.DequeueJobs(ctx, []string{"default"}, 4)
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if len(jobs) != 0 {
			t.Fatalf("expected empty dequeue, got %d", len(jobs))
		}
	}

	if n := countJobWriteCommands(t, mdb); n != 0 {
		t.Fatalf("idle DequeueJobs issued %d write commands against jobs; want 0", n)
	}
}

// TestDequeueJobsClaimsPendingJob proves the gate doesn't break the claim
// path.
func TestDequeueJobsClaimsPendingJob(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	ctx := context.Background()

	now := time.Now().UTC()
	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       "claim-me",
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      now.Add(-time.Second),
	}
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	jobs, err := s.DequeueJobs(ctx, []string{"default"}, 4)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 dequeued job, got %d", len(jobs))
	}
	if jobs[0].ID.String() != j.ID.String() {
		t.Fatalf("dequeued wrong job: %s", jobs[0].ID)
	}
	if jobs[0].State != job.StateRunning {
		t.Fatalf("expected state running, got %s", jobs[0].State)
	}
}
