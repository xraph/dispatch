package mongo_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	mongod "go.mongodb.org/mongo-driver/v2/mongo"
)

// indexKeys returns every index on a collection as its key pattern
// written out, "state:1,_id:-1", so a test can compare patterns without
// caring about the numeric type the server echoes back.
func indexKeys(t *testing.T, mdb *mongod.Database, col string) []string {
	t.Helper()

	ctx := context.Background()

	cur, err := mdb.Collection(col).Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes on %s: %v", col, err)
	}

	var specs []struct {
		Key bson.D `bson:"key"`
	}
	if err = cur.All(ctx, &specs); err != nil {
		t.Fatalf("decode indexes on %s: %v", col, err)
	}

	out := make([]string, 0, len(specs))
	for _, spec := range specs {
		parts := make([]string, len(spec.Key))
		for i, e := range spec.Key {
			parts[i] = fmt.Sprintf("%s:%v", e.Key, e.Value)
		}
		out = append(out, strings.Join(parts, ","))
	}

	return out
}

// TestListIndexesExistAfterMigrate checks the compound indexes the paged
// lists read through: one per exact-match filter, each ending in _id
// descending so a filtered page comes back newest first without a sort.
//
// Migrate runs twice first, as it does on every process start.
// CreateMany with an identical key pattern and options is a no-op, so the
// second run must succeed and leave the same indexes.
func TestListIndexesExistAfterMigrate(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)

	for range 2 {
		if err := s.Migrate(context.Background()); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}

	mdb := rawDatabase(t, uri)

	for _, want := range []struct{ col, keys string }{
		{"dispatch_jobs", "state:1,_id:-1"},
		{"dispatch_jobs", "queue:1,_id:-1"},
		{"dispatch_dlq", "queue:1,_id:-1"},
		{"dispatch_workflow_runs", "state:1,_id:-1"},
		{"dispatch_artifacts", "scope_app_id:1,_id:-1"},
	} {
		if got := indexKeys(t, mdb, want.col); !slices.Contains(got, want.keys) {
			t.Errorf("%s has no {%s} index; has %v", want.col, want.keys, got)
		}
	}
}
