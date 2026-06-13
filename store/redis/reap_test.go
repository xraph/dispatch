package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	redismodule "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	redisstore "github.com/xraph/dispatch/store/redis"
)

// startRedis launches a disposable redis container and returns its
// connection string. Skips when -short is set or no container runtime is
// available. Named distinctly from the integration-tagged setupTestStore so
// both build under -tags integration.
func startRedis(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed integration test in -short mode")
	}

	ctx := context.Background()
	ctr, err := redismodule.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}
	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(ctr); termErr != nil {
			t.Errorf("terminate redis container: %v", termErr)
		}
	})

	connStr, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis connection string: %v", err)
	}
	return connStr
}

func openRedisStore(t *testing.T, connStr string) *redisstore.Store {
	t.Helper()
	ctx := context.Background()

	rdb := redisdriver.New()
	if err := rdb.Open(ctx, connStr); err != nil {
		t.Fatalf("open redisdriver: %v", err)
	}
	kvStore, err := kv.Open(rdb)
	if err != nil {
		t.Fatalf("kv open: %v", err)
	}
	t.Cleanup(func() { _ = kvStore.Close() })

	return redisstore.New(kvStore)
}

func openReapRedis(t *testing.T) *redisstore.Store {
	t.Helper()
	return openRedisStore(t, startRedis(t))
}

func runningJob(name string, startedAgo time.Duration) *job.Job {
	now := time.Now().UTC()
	started := now.Add(-startedAgo)
	return &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StateRunning,
		MaxRetries: 3,
		RunAt:      now.Add(-startedAgo),
		StartedAt:  &started,
	}
}

// TestReapStaleJobs_OrphanNilHeartbeat covers the crashed-worker window: a
// job claimed (running, started_at set) whose worker died before the first
// heartbeat has a nil heartbeat and must still be reaped once its start
// time passes the threshold. A freshly claimed job must not be.
func TestReapStaleJobs_OrphanNilHeartbeat(t *testing.T) {
	s := openReapRedis(t)
	ctx := context.Background()

	orphan := runningJob("orphan", time.Minute)
	if err := s.EnqueueJob(ctx, orphan); err != nil {
		t.Fatalf("enqueue orphan: %v", err)
	}
	fresh := runningJob("fresh", 0)
	if err := s.EnqueueJob(ctx, fresh); err != nil {
		t.Fatalf("enqueue fresh: %v", err)
	}

	// Precondition: the store persisted the claimed-but-unheartbeated state.
	got, err := s.GetJob(ctx, orphan.ID)
	if err != nil {
		t.Fatalf("get orphan: %v", err)
	}
	if got.State != job.StateRunning || got.HeartbeatAt != nil {
		t.Fatalf("precondition: want running with nil heartbeat, got state=%s heartbeat=%v", got.State, got.HeartbeatAt)
	}

	stale, err := s.ReapStaleJobs(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 orphaned stale job, got %d", len(stale))
	}
	if stale[0].ID.String() != orphan.ID.String() {
		t.Fatalf("reaped wrong job: %s", stale[0].Name)
	}
}
