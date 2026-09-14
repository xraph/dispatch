//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	redisstore "github.com/xraph/dispatch/store/redis"
)

// Two tenants sharing one Redis must never observe each other's dispatch
// state. Each assertion below fails on an unprefixed store because every
// key would collapse onto the same "dispatch:" namespace.

func TestStore_KeyPrefix_jobsAreIsolated(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	a := redisstore.New(kvStore, redisstore.WithKeyPrefix("ws_a:"))
	b := redisstore.New(kvStore, redisstore.WithKeyPrefix("ws_b:"))

	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       "tenant-a-only",
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      time.Now().UTC(),
	}
	if err := a.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue on a: %v", err)
	}

	if _, err := b.GetJob(ctx, j.ID); err == nil {
		t.Fatalf("tenant b read tenant a's job %s by id", j.ID)
	}

	got, err := b.DequeueJobs(ctx, job.DequeueOpts{Queues: []string{"default"}, Limit: 10})
	if err != nil {
		t.Fatalf("dequeue on b: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("tenant b dequeued %d of tenant a's jobs", len(got))
	}

	n, err := b.CountJobs(ctx, job.CountOpts{State: job.StatePending})
	if err != nil {
		t.Fatalf("count on b: %v", err)
	}
	if n != 0 {
		t.Fatalf("tenant b counted %d pending jobs, want 0", n)
	}

	got, err = a.DequeueJobs(ctx, job.DequeueOpts{Queues: []string{"default"}, Limit: 10})
	if err != nil {
		t.Fatalf("dequeue on a: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("tenant a dequeued %d jobs, want its own 1", len(got))
	}
}

func TestStore_KeyPrefix_cronNamesAreIsolated(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	a := redisstore.New(kvStore, redisstore.WithKeyPrefix("ws_a:"))
	b := redisstore.New(kvStore, redisstore.WithKeyPrefix("ws_b:"))

	entry := func() *cron.Entry {
		next := time.Now().Add(time.Hour).UTC()
		return &cron.Entry{
			Entity:    dispatch.NewEntity(),
			ID:        id.NewCronID(),
			Name:      "nightly",
			Schedule:  "0 0 * * *",
			JobName:   "rescore",
			Payload:   []byte(`{}`),
			Enabled:   true,
			NextRunAt: &next,
		}
	}
	if err := a.RegisterCron(ctx, entry()); err != nil {
		t.Fatalf("register on a: %v", err)
	}
	// Same cron name in another tenant is a different cron, not a duplicate.
	if err := b.RegisterCron(ctx, entry()); err != nil {
		t.Fatalf("register on b collided with tenant a: %v", err)
	}
}

func TestStore_KeyPrefix_leadershipIsIsolated(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	a := redisstore.New(kvStore, redisstore.WithKeyPrefix("ws_a:"))
	b := redisstore.New(kvStore, redisstore.WithKeyPrefix("ws_b:"))

	leaderA := registerWorker(t, a, "leader-a")
	ok, err := a.AcquireLeadership(ctx, leaderA, time.Minute)
	if err != nil {
		t.Fatalf("acquire on a: %v", err)
	}
	if !ok {
		t.Fatal("tenant a could not take leadership of an empty cluster")
	}

	leaderB := registerWorker(t, b, "leader-b")
	ok, err = b.AcquireLeadership(ctx, leaderB, time.Minute)
	if err != nil {
		t.Fatalf("acquire on b: %v", err)
	}
	if !ok {
		t.Fatal("tenant b was blocked by tenant a's leader")
	}
}

// registerWorker enrols a worker on the store, since leadership can only be
// taken by a registered worker.
func registerWorker(t *testing.T, s *redisstore.Store, hostname string) id.WorkerID {
	t.Helper()
	w := &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    hostname,
		Queues:      []string{"default"},
		Concurrency: 1,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.RegisterWorker(context.Background(), w); err != nil {
		t.Fatalf("register worker %s: %v", hostname, err)
	}
	return w.ID
}

func TestStore_KeyPrefix_emptyPrefixKeepsLegacyKeys(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	legacy := redisstore.New(kvStore)
	explicit := redisstore.New(kvStore, redisstore.WithKeyPrefix(""))

	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       "shared",
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      time.Now().UTC(),
	}
	if err := legacy.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := explicit.GetJob(ctx, j.ID); err != nil {
		t.Fatalf("empty prefix must address the same keys as no prefix: %v", err)
	}
}
