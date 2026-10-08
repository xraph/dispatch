//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch/id"
	redisstore "github.com/xraph/dispatch/store/redis"
	"github.com/xraph/dispatch/store/storetest"
)

func TestCheckpointOrderConformance(t *testing.T) {
	kvStore := setupTestKV(t)
	s := redisstore.New(kvStore)
	client := redisdriver.UnwrapClient(kvStore)
	storetest.RunCheckpointOrderSuite(t, s, func(ctx context.Context, runID id.RunID, step string, at time.Time) error {
		key := "dispatch:checkpoint:" + runID.String() + ":" + step
		raw, err := client.Get(ctx, key).Bytes()
		if err != nil {
			return err
		}
		var record map[string]json.RawMessage
		if decodeErr := json.Unmarshal(raw, &record); decodeErr != nil {
			return decodeErr
		}
		encodedTime, err := json.Marshal(at)
		if err != nil {
			return err
		}
		record["created_at"] = encodedTime
		updated, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return client.Set(ctx, key, updated, 0).Err()
	})
}
