package mongo_test

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/storetest"
)

func TestCheckpointOrderConformance(t *testing.T) {
	uri := startMongo(t)
	s := openStore(t, uri)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	col := rawDatabase(t, uri).Collection("dispatch_checkpoints")
	storetest.RunCheckpointOrderSuite(t, s, func(ctx context.Context, runID id.RunID, step string, at time.Time) error {
		_, err := col.UpdateOne(ctx, bson.M{"run_id": runID.String(), "step_name": step}, bson.M{"$set": bson.M{"created_at": at}})
		return err
	})
}
