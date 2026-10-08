//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	redisstore "github.com/xraph/dispatch/store/redis"
)

func TestSummaryReadsPropagateOutagesAndCorruptRecords(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	s := redisstore.New(kvStore)
	client := redisdriver.UnwrapClient(kvStore)
	hook := &workflowReadFault{}
	hook.key.Store("")
	client.AddHook(hook)
	worker := &cluster.Worker{ID: id.NewWorkerID(), State: cluster.WorkerActive, LastSeen: time.Now(), CreatedAt: time.Now()}
	if err := s.RegisterWorker(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.AcquireLeadership(ctx, worker.ID, time.Minute); err != nil || !ok {
		t.Fatalf("leader=%v, %v", ok, err)
	}
	entry := &cron.Entry{Entity: dispatch.NewEntity(), ID: id.NewCronID(), Name: "summary", Schedule: "@every 1h", JobName: "summary", Enabled: true}
	if err := s.RegisterCron(ctx, entry); err != nil {
		t.Fatal(err)
	}
	j := &job.Job{Entity: dispatch.NewEntity(), ID: id.NewJobID(), Name: "summary", Queue: "default", State: job.StatePending, RunAt: time.Now()}
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	reads := map[string]struct {
		key, otherID string
		call         func() error
	}{
		"workers":   {"dispatch:worker:" + worker.ID.String(), id.NewWorkerID().String(), func() error { _, err := s.ListWorkers(ctx); return err }},
		"worker":    {"dispatch:worker:" + worker.ID.String(), id.NewWorkerID().String(), func() error { _, err := s.GetWorker(ctx, worker.ID); return err }},
		"leader":    {"dispatch:worker:" + worker.ID.String(), id.NewWorkerID().String(), func() error { _, err := s.GetLeader(ctx); return err }},
		"crons":     {"dispatch:cron:" + entry.ID.String(), id.NewCronID().String(), func() error { _, err := s.ListCrons(ctx); return err }},
		"cron":      {"dispatch:cron:" + entry.ID.String(), id.NewCronID().String(), func() error { _, err := s.GetCron(ctx, entry.ID); return err }},
		"job-count": {"dispatch:job:" + j.ID.String(), id.NewJobID().String(), func() error { _, err := s.CountJobs(ctx, job.CountOpts{}); return err }},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			original, readErr := client.Get(ctx, read.key).Bytes()
			if readErr != nil {
				t.Fatal(readErr)
			}
			for _, mode := range []string{"outage", "json", "invalid-id", "mismatched-id"} {
				t.Run(mode, func(t *testing.T) {
					t.Cleanup(func() {
						hook.key.Store("")
						if restoreErr := client.Set(ctx, read.key, original, 0).Err(); restoreErr != nil {
							t.Error(restoreErr)
						}
					})
					if mode == "outage" {
						hook.key.Store(read.key)
					} else {
						data := []byte("{broken")
						if mode != "json" {
							var record map[string]json.RawMessage
							if decodeErr := json.Unmarshal(original, &record); decodeErr != nil {
								t.Fatal(decodeErr)
							}
							identity := "broken"
							if mode == "mismatched-id" {
								identity = read.otherID
							}
							record["id"], _ = json.Marshal(identity)
							var encodeErr error
							data, encodeErr = json.Marshal(record)
							if encodeErr != nil {
								t.Fatal(encodeErr)
							}
						}
						if setErr := client.Set(ctx, read.key, data, 0).Err(); setErr != nil {
							t.Fatal(setErr)
						}
					}
					if callErr := read.call(); callErr == nil {
						t.Fatal("unreadable or mismatched row became a successful summary")
					}
				})
			}
		})
	}
	// Dangling valid members still represent absent rows, not read failures.
	for key, member := range map[string]string{"dispatch:worker_ids": id.NewWorkerID().String(), "dispatch:cron_ids": id.NewCronID().String(), "dispatch:job_ids": id.NewJobID().String()} {
		if err := client.SAdd(ctx, key, member).Err(); err != nil {
			t.Fatal(err)
		}
	}
	workers, err := s.ListWorkers(ctx)
	if err != nil || len(workers) != 1 {
		t.Fatalf("workers=%v, %v", workers, err)
	}
	entries, err := s.ListCrons(ctx)
	if err != nil || len(entries) != 1 {
		t.Fatalf("crons=%v, %v", entries, err)
	}
	count, err := s.CountJobs(ctx, job.CountOpts{})
	if err != nil || count != 1 {
		t.Fatalf("count=%v, %v", count, err)
	}
	if deleteErr := client.Del(ctx, "dispatch:worker:"+worker.ID.String()).Err(); deleteErr != nil {
		t.Fatal(deleteErr)
	}
	leader, err := s.GetLeader(ctx)
	if err != nil || leader != nil {
		t.Fatalf("missing leader row=%+v, %v", leader, err)
	}
}
