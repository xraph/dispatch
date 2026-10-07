//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
	redisstore "github.com/xraph/dispatch/store/redis"
	"github.com/xraph/dispatch/store/storetest"
)

func TestClusterSuite(t *testing.T) {
	// One container for every case: the suite isolates by worker ID.
	shared := setupTestStore(t)

	storetest.RunClusterSuite(t, func(t *testing.T) cluster.Store {
		t.Helper()

		return shared
	})
}

// TestHeartbeatWorker_restoresIndexMembership covers the state a stale
// sweep racing a heartbeat leaves behind. HeartbeatWorker reads the
// entity and writes it back; DeleteStaleWorkers deletes the entity and
// its worker_ids member. If the sweep lands between the heartbeat's read
// and write, the entity comes back without its member, and from then on
// the heartbeat succeeds against a row ListWorkers cannot see, so the
// engine never re-registers and the worker drops off the dashboard while
// it is still running. The heartbeat restores the member, which makes
// that state heal on the next beat.
func TestHeartbeatWorker_restoresIndexMembership(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	s := redisstore.New(kvStore)

	w := &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    "raced",
		Queues:      []string{"default"},
		Concurrency: 1,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("register: %v", err)
	}

	// The legacy, unprefixed key: see TestStore_KeyPrefix_emptyPrefixKeepsLegacyKeys.
	client := redisdriver.UnwrapClient(kvStore)
	if err := client.SRem(ctx, "dispatch:worker_ids", w.ID.String()).Err(); err != nil {
		t.Fatalf("drop index member: %v", err)
	}

	if err := s.HeartbeatWorker(ctx, w.ID); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, got := range workers {
		if got.ID.String() == w.ID.String() {
			return
		}
	}
	t.Fatalf("worker %s missing from ListWorkers after a heartbeat; the index member was not restored", w.ID)
}
