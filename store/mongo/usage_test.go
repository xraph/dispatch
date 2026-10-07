package mongo_test

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/job/jobtest"
)

// TestUsageRecorderConformance runs the shared job.UsageRecorder suite
// against MongoDB. One container serves every subtest; the collection is
// emptied between them because the suite asserts absolute counts.
func TestUsageRecorderConformance(t *testing.T) {
	ctx := context.Background()
	uri := startMongo(t)
	store := openStore(t, uri)

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	client, err := mongod.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect raw mongo client: %v", err)
	}

	t.Cleanup(func() {
		if derr := client.Disconnect(ctx); derr != nil {
			t.Errorf("disconnect: %v", derr)
		}
	})

	col := client.Database(testDBName).Collection("dispatch_job_usage")

	jobtest.RunUsageRecorderSuite(t, func() job.UsageRecorder {
		if _, derr := col.DeleteMany(ctx, bson.M{}); derr != nil {
			t.Fatalf("clear usage collection: %v", derr)
		}

		return store
	})
}
