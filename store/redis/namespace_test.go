//go:build integration

package redis_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/drivers/redisdriver"
	"github.com/xraph/grove/kv/middleware"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	redisstore "github.com/xraph/dispatch/store/redis"
)

// A host can hand Dispatch a kv store that already carries a namespace
// hook. Every operation now goes through kv.Store, so every key has to
// pass through that hook too: sorted sets, sets, hashes, scripts and
// streams as well as plain gets and sets. grove kv before v1.7.0 applied
// the hook to some of those and not others, which split one store across
// two keyspaces. This drives each kind of operation and then reads Redis
// raw to prove nothing landed outside the namespace.
func TestStore_NamespaceHook_keepsEveryKeyInside(t *testing.T) {
	ctx := context.Background()
	connStr := startRedis(t)

	drv := redisdriver.New()
	if err := drv.Open(ctx, connStr); err != nil {
		t.Fatalf("open redisdriver: %v", err)
	}
	kvStore, err := kv.Open(drv, kv.WithHook(middleware.NewNamespace("tenant_x")))
	if err != nil {
		t.Fatalf("kv open: %v", err)
	}
	t.Cleanup(func() { _ = kvStore.Close() })

	s := redisstore.New(kvStore)
	now := time.Now().UTC()

	// Sets, sorted sets and plain entities.
	pending := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       "ns-pending",
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      now,
	}
	if err := s.EnqueueJob(ctx, pending); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := s.DequeueJobs(ctx, job.DequeueOpts{Queues: []string{"default"}, Limit: 10})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("dequeued %d jobs, want the 1 just enqueued: the queue and the entity disagree on where they live", len(got))
	}

	// The reclaim path runs a Lua script over the job key.
	if err := s.EnqueueJob(ctx, runningJob("ns-stale", time.Hour)); err != nil {
		t.Fatalf("enqueue running: %v", err)
	}
	if _, err := s.ReclaimExpiredLeases(ctx, 10); err != nil {
		t.Fatalf("reclaim: %v", err)
	}

	// Streams, hashes-backed cluster state, cron names, usage indexes.
	if err := s.PublishEvent(ctx, &event.Event{ID: id.NewEventID(), Name: "ns-evt", CreatedAt: now}); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	w := &cluster.Worker{ID: id.NewWorkerID(), Hostname: "h", Queues: []string{"default"}, State: cluster.WorkerActive, LastSeen: now, CreatedAt: now}
	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := s.AcquireLeadership(ctx, w.ID, time.Minute); err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}
	next := now.Add(time.Hour)
	if err := s.RegisterCron(ctx, &cron.Entry{
		Entity: dispatch.NewEntity(), ID: id.NewCronID(), Name: "ns-cron",
		Schedule: "0 0 * * *", JobName: "x", Payload: []byte(`{}`), Enabled: true, NextRunAt: &next,
	}); err != nil {
		t.Fatalf("register cron: %v", err)
	}
	if err := s.RecordJobUsage(ctx, &job.Usage{
		ID: id.NewUsageID(), JobID: pending.ID, Name: pending.Name, Queue: "default",
		Status: job.StateCompleted, WallTime: time.Second, RecordedAt: now,
	}); err != nil {
		t.Fatalf("record usage: %v", err)
	}

	raw := redisdriver.UnwrapClient(kvStore)
	keys, err := raw.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatalf("raw keys: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("no keys written at all")
	}
	for _, k := range keys {
		if !strings.HasPrefix(k, "tenant_x:") {
			t.Errorf("key %q escaped the namespace hook", k)
		}
	}
}
