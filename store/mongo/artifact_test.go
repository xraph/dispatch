package mongo_test

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
)

// TestArtifactStoreConformance runs the shared artifact.Store suite
// against MongoDB.
//
// One container serves every subtest; the artifact collections are
// emptied between them because the suite asserts absolute counts.
// Documents are deleted rather than the collections dropped so the
// indexes created by Migrate — in particular the partial unique index on
// live keys — stay in force for every subtest.
func TestArtifactStoreConformance(t *testing.T) {
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
			t.Errorf("disconnect raw mongo client: %v", derr)
		}
	})

	db := client.Database(testDBName)

	artifacttest.RunStoreSuite(t, func(t *testing.T) artifact.Store {
		for _, col := range []string{"dispatch_artifact_links", "dispatch_artifacts"} {
			if _, derr := db.Collection(col).DeleteMany(ctx, bson.M{}); derr != nil {
				t.Fatalf("clear %s: %v", col, derr)
			}
		}

		return store
	})
}
