//go:build integration

package bunstore_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgmodule "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	bunstore "github.com/xraph/dispatch/store/bun"
	"github.com/xraph/dispatch/workflow"
)

// setupTestStore creates a Postgres container and returns a connected Bun Store.
func setupTestStore(t *testing.T) *bunstore.Store {
	t.Helper()

	ctx := context.Background()

	container, err := pgmodule.Run(ctx,
		"postgres:16-alpine",
		pgmodule.WithDatabase("dispatch_test"),
		pgmodule.WithUsername("test"),
		pgmodule.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if termErr := container.Terminate(ctx); termErr != nil {
			t.Logf("terminate container: %v", termErr)
		}
	})

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}

	// Create Bun DB from pgdriver.
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(connStr)))
	db := bun.NewDB(sqldb, pgdialect.New())

	t.Cleanup(func() {
		_ = db.Close()
	})

	store := bunstore.New(db, bunstore.WithLogger(slog.Default()))

	if migErr := store.Migrate(ctx); migErr != nil {
		t.Fatalf("migrate: %v", migErr)
	}

	return store
}

// ──────────────────────────────────────────────────
// Lifecycle tests
// ──────────────────────────────────────────────────

func TestStore_Ping(t *testing.T) {
	s := setupTestStore(t)
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func TestStore_MigrateIdempotent(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	// Second migrate should be a no-op.
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second migrate failed: %v", err)
	}
}

// ──────────────────────────────────────────────────
// Job Store tests
// ──────────────────────────────────────────────────

func TestJobStore_EnqueueAndGet(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       "test-job",
		Queue:      "default",
		Payload:    []byte(`{"key":"value"}`),
		State:      job.StatePending,
		Priority:   5,
		MaxRetries: 3,
		RunAt:      time.Now().UTC(),
	}

	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Duplicate should fail.
	if dupErr := s.EnqueueJob(ctx, j); !errors.Is(dupErr, dispatch.ErrJobAlreadyExists) {
		t.Fatalf("expected ErrJobAlreadyExists, got: %v", dupErr)
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "test-job" {
		t.Fatalf("expected name test-job, got %s", got.Name)
	}
	if got.Priority != 5 {
		t.Fatalf("expected priority 5, got %d", got.Priority)
	}
}

func TestJobStore_DequeueSkipLocked(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Enqueue 3 jobs with different priorities.
	for i := 0; i < 3; i++ {
		j := &job.Job{
			Entity:     dispatch.NewEntity(),
			ID:         id.NewJobID(),
			Name:       fmt.Sprintf("job-%d", i),
			Queue:      "default",
			Payload:    []byte(`{}`),
			State:      job.StatePending,
			Priority:   i, // 0, 1, 2
			MaxRetries: 3,
			RunAt:      time.Now().UTC(),
		}
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue job-%d: %v", i, err)
		}
	}

	// Dequeue 2 — should get highest priority first.
	dequeued, err := s.DequeueJobs(ctx, []string{"default"}, 2)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(dequeued) != 2 {
		t.Fatalf("expected 2 dequeued, got %d", len(dequeued))
	}
	if dequeued[0].Priority != 2 {
		t.Fatalf("expected first dequeued priority 2, got %d", dequeued[0].Priority)
	}
	if dequeued[1].Priority != 1 {
		t.Fatalf("expected second dequeued priority 1, got %d", dequeued[1].Priority)
	}

	// Dequeue remaining — should get 1 job.
	remaining, err := s.DequeueJobs(ctx, []string{"default"}, 10)
	if err != nil {
		t.Fatalf("dequeue remaining: %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining, got %d", len(remaining))
	}
}

func TestJobStore_UpdateAndDelete(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       "update-test",
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      time.Now().UTC(),
	}
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	j.State = job.StateCompleted
	now := time.Now().UTC()
	j.CompletedAt = &now
	if err := s.UpdateJob(ctx, j); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got.State != job.StateCompleted {
		t.Fatalf("expected completed, got %s", got.State)
	}

	if err = s.DeleteJob(ctx, j.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, getErr := s.GetJob(ctx, j.ID)
	if !errors.Is(getErr, dispatch.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got: %v", getErr)
	}
}

func TestJobStore_ListByState(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		state := job.StatePending
		if i >= 3 {
			state = job.StateCompleted
		}
		j := &job.Job{
			Entity:     dispatch.NewEntity(),
			ID:         id.NewJobID(),
			Name:       fmt.Sprintf("list-job-%d", i),
			Queue:      "default",
			Payload:    []byte(`{}`),
			State:      state,
			MaxRetries: 3,
			RunAt:      time.Now().UTC(),
		}
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	pending, err := s.ListJobsByState(ctx, job.StatePending, job.ListOpts{})
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending, got %d", len(pending))
	}
}

func TestJobStore_HeartbeatAndReap(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       "heartbeat-test",
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StateRunning,
		MaxRetries: 3,
		RunAt:      time.Now().UTC(),
	}
	now := time.Now().UTC()
	j.StartedAt = &now
	old := now.Add(-2 * time.Minute)
	j.HeartbeatAt = &old

	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Reap with 1-minute threshold — should find the stale job.
	stale, err := s.ReapStaleJobs(ctx, 1*time.Minute)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale, got %d", len(stale))
	}

	// Heartbeat — update to fresh.
	if err = s.HeartbeatJob(ctx, j.ID, id.NewWorkerID()); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	// Reap again — should find 0.
	stale, err = s.ReapStaleJobs(ctx, 1*time.Minute)
	if err != nil {
		t.Fatalf("reap after heartbeat: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("expected 0 stale after heartbeat, got %d", len(stale))
	}
}

func TestJobStore_CountJobs(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		j := &job.Job{
			Entity:     dispatch.NewEntity(),
			ID:         id.NewJobID(),
			Name:       "count-test",
			Queue:      "default",
			Payload:    []byte(`{}`),
			State:      job.StatePending,
			MaxRetries: 3,
			RunAt:      time.Now().UTC(),
		}
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	count, err := s.CountJobs(ctx, job.CountOpts{State: job.StatePending})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}
}

// ──────────────────────────────────────────────────
// Workflow Store tests
// ──────────────────────────────────────────────────

func TestWorkflowStore_CreateAndGetRun(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	run := &workflow.Run{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewRunID(),
		Name:      "test-workflow",
		State:     workflow.RunStateRunning,
		Input:     []byte(`{"input":"data"}`),
		StartedAt: time.Now().UTC(),
	}

	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Name != "test-workflow" {
		t.Fatalf("expected name test-workflow, got %s", got.Name)
	}
	if got.State != workflow.RunStateRunning {
		t.Fatalf("expected running, got %s", got.State)
	}
}

func TestWorkflowStore_UpdateRun(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	run := &workflow.Run{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewRunID(),
		Name:      "update-workflow",
		State:     workflow.RunStateRunning,
		StartedAt: time.Now().UTC(),
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}

	run.State = workflow.RunStateCompleted
	now := time.Now().UTC()
	run.CompletedAt = &now
	run.Output = []byte(`{"result":"done"}`)

	if err := s.UpdateRun(ctx, run); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != workflow.RunStateCompleted {
		t.Fatalf("expected completed, got %s", got.State)
	}
}

func TestWorkflowStore_ListRuns(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		run := &workflow.Run{
			Entity:    dispatch.NewEntity(),
			ID:        id.NewRunID(),
			Name:      fmt.Sprintf("list-wf-%d", i),
			State:     workflow.RunStateRunning,
			StartedAt: time.Now().UTC(),
		}
		if err := s.CreateRun(ctx, run); err != nil {
			t.Fatalf("create run %d: %v", i, err)
		}
	}

	runs, err := s.ListRuns(ctx, workflow.ListOpts{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3, got %d", len(runs))
	}
}

func TestWorkflowStore_Checkpoints(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	runID := id.NewRunID()
	run := &workflow.Run{
		Entity:    dispatch.NewEntity(),
		ID:        runID,
		Name:      "checkpoint-wf",
		State:     workflow.RunStateRunning,
		StartedAt: time.Now().UTC(),
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Save checkpoint.
	if err := s.SaveCheckpoint(ctx, runID, "step-1", []byte("data-1")); err != nil {
		t.Fatalf("save checkpoint 1: %v", err)
	}
	if err := s.SaveCheckpoint(ctx, runID, "step-2", []byte("data-2")); err != nil {
		t.Fatalf("save checkpoint 2: %v", err)
	}

	// Get checkpoint.
	data, err := s.GetCheckpoint(ctx, runID, "step-1")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if string(data) != "data-1" {
		t.Fatalf("expected data-1, got %s", string(data))
	}

	// Get non-existent checkpoint — should return nil.
	data, err = s.GetCheckpoint(ctx, runID, "missing")
	if err != nil {
		t.Fatalf("get missing checkpoint: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %v", data)
	}

	// List checkpoints.
	cps, err := s.ListCheckpoints(ctx, runID)
	if err != nil {
		t.Fatalf("list checkpoints: %v", err)
	}
	if len(cps) != 2 {
		t.Fatalf("expected 2, got %d", len(cps))
	}

	// Overwrite checkpoint — upsert.
	if err = s.SaveCheckpoint(ctx, runID, "step-1", []byte("data-1-updated")); err != nil {
		t.Fatalf("overwrite checkpoint: %v", err)
	}
	data, err = s.GetCheckpoint(ctx, runID, "step-1")
	if err != nil {
		t.Fatalf("get updated checkpoint: %v", err)
	}
	if string(data) != "data-1-updated" {
		t.Fatalf("expected data-1-updated, got %s", string(data))
	}
}

// ──────────────────────────────────────────────────
// Cron Store tests
// ──────────────────────────────────────────────────

func TestCronStore_RegisterAndGet(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	next := time.Now().Add(1 * time.Hour).UTC()
	entry := &cron.Entry{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewCronID(),
		Name:      "test-cron",
		Schedule:  "*/5 * * * *",
		JobName:   "process-batch",
		Payload:   []byte(`{"batch_size":100}`),
		Enabled:   true,
		NextRunAt: &next,
	}

	if err := s.RegisterCron(ctx, entry); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Duplicate name should fail.
	dup := &cron.Entry{
		Entity:   dispatch.NewEntity(),
		ID:       id.NewCronID(),
		Name:     "test-cron",
		Schedule: "*/10 * * * *",
		JobName:  "other-job",
		Enabled:  true,
	}
	if dupErr := s.RegisterCron(ctx, dup); !errors.Is(dupErr, dispatch.ErrDuplicateCron) {
		t.Fatalf("expected ErrDuplicateCron, got: %v", dupErr)
	}

	got, err := s.GetCron(ctx, entry.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "test-cron" {
		t.Fatalf("expected test-cron, got %s", got.Name)
	}
	if !got.Enabled {
		t.Fatal("expected enabled")
	}
}

func TestCronStore_ListAndDelete(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		entry := &cron.Entry{
			Entity:   dispatch.NewEntity(),
			ID:       id.NewCronID(),
			Name:     fmt.Sprintf("cron-%d", i),
			Schedule: "*/5 * * * *",
			JobName:  "test-job",
			Enabled:  true,
		}
		if err := s.RegisterCron(ctx, entry); err != nil {
			t.Fatalf("register cron-%d: %v", i, err)
		}
	}

	entries, err := s.ListCrons(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3, got %d", len(entries))
	}

	// Delete one.
	if err = s.DeleteCron(ctx, entries[0].ID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	entries, err = s.ListCrons(ctx)
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2, got %d", len(entries))
	}
}

func TestCronStore_LockAndRelease(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	entry := &cron.Entry{
		Entity:   dispatch.NewEntity(),
		ID:       id.NewCronID(),
		Name:     "lock-test",
		Schedule: "*/5 * * * *",
		JobName:  "test-job",
		Enabled:  true,
	}
	if err := s.RegisterCron(ctx, entry); err != nil {
		t.Fatalf("register: %v", err)
	}

	worker1 := id.NewWorkerID()
	worker2 := id.NewWorkerID()

	// Worker1 acquires lock.
	acquired, err := s.AcquireCronLock(ctx, entry.ID, worker1, 30*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquired")
	}

	// Worker2 cannot acquire (lock held by worker1).
	acquired, err = s.AcquireCronLock(ctx, entry.ID, worker2, 30*time.Second)
	if err != nil {
		t.Fatalf("acquire by worker2: %v", err)
	}
	if acquired {
		t.Fatal("expected not acquired by worker2")
	}

	// Worker1 can re-acquire (idempotent).
	acquired, err = s.AcquireCronLock(ctx, entry.ID, worker1, 30*time.Second)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if !acquired {
		t.Fatal("expected re-acquired by worker1")
	}

	// Release.
	if err = s.ReleaseCronLock(ctx, entry.ID, worker1); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Now worker2 can acquire.
	acquired, err = s.AcquireCronLock(ctx, entry.ID, worker2, 30*time.Second)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquired by worker2 after release")
	}
}

func TestCronStore_UpdateEntry(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	entry := &cron.Entry{
		Entity:   dispatch.NewEntity(),
		ID:       id.NewCronID(),
		Name:     "update-cron",
		Schedule: "*/5 * * * *",
		JobName:  "test-job",
		Enabled:  true,
	}
	if err := s.RegisterCron(ctx, entry); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry.Enabled = false
	if err := s.UpdateCronEntry(ctx, entry); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := s.GetCron(ctx, entry.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Enabled {
		t.Fatal("expected disabled")
	}
}

func TestCronStore_UpdateLastRun(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	entry := &cron.Entry{
		Entity:   dispatch.NewEntity(),
		ID:       id.NewCronID(),
		Name:     "lastrun-cron",
		Schedule: "*/5 * * * *",
		JobName:  "test-job",
		Enabled:  true,
	}
	if err := s.RegisterCron(ctx, entry); err != nil {
		t.Fatalf("register: %v", err)
	}

	now := time.Now().UTC()
	if err := s.UpdateCronLastRun(ctx, entry.ID, now); err != nil {
		t.Fatalf("update last run: %v", err)
	}

	got, err := s.GetCron(ctx, entry.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.LastRunAt == nil {
		t.Fatal("expected last_run_at to be set")
	}
}

// ──────────────────────────────────────────────────
// DLQ Store tests
// ──────────────────────────────────────────────────

func TestDLQStore_PushAndGet(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	entry := &dlq.Entry{
		ID:         id.NewDLQID(),
		JobID:      id.NewJobID(),
		JobName:    "failed-job",
		Queue:      "default",
		Payload:    []byte(`{"key":"value"}`),
		Error:      "something went wrong",
		RetryCount: 3,
		FailedAt:   time.Now().UTC(),
		CreatedAt:  time.Now().UTC(),
	}

	if err := s.PushDLQ(ctx, entry); err != nil {
		t.Fatalf("push: %v", err)
	}

	got, err := s.GetDLQ(ctx, entry.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.JobName != "failed-job" {
		t.Fatalf("expected failed-job, got %s", got.JobName)
	}
	if got.Error != "something went wrong" {
		t.Fatalf("expected error message, got %s", got.Error)
	}
}

func TestDLQStore_ListAndPurge(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		entry := &dlq.Entry{
			ID:         id.NewDLQID(),
			JobID:      id.NewJobID(),
			JobName:    fmt.Sprintf("dlq-job-%d", i),
			Queue:      "default",
			Payload:    []byte(`{}`),
			Error:      "error",
			RetryCount: 3,
			FailedAt:   time.Now().UTC().Add(-time.Duration(i) * time.Hour),
			CreatedAt:  time.Now().UTC(),
		}
		if err := s.PushDLQ(ctx, entry); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}

	entries, err := s.ListDLQ(ctx, dlq.ListOpts{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5, got %d", len(entries))
	}

	// Purge entries older than 2 hours.
	purged, err := s.PurgeDLQ(ctx, time.Now().UTC().Add(-2*time.Hour))
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if purged != 3 {
		t.Fatalf("expected 3 purged, got %d", purged)
	}

	remaining, err := s.ListDLQ(ctx, dlq.ListOpts{})
	if err != nil {
		t.Fatalf("list after purge: %v", err)
	}
	if len(remaining) != 2 {
		t.Fatalf("expected 2, got %d", len(remaining))
	}
}

func TestDLQStore_Replay(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	entry := &dlq.Entry{
		ID:         id.NewDLQID(),
		JobID:      id.NewJobID(),
		JobName:    "replay-job",
		Queue:      "default",
		Payload:    []byte(`{}`),
		Error:      "error",
		RetryCount: 3,
		FailedAt:   time.Now().UTC(),
		CreatedAt:  time.Now().UTC(),
	}
	if err := s.PushDLQ(ctx, entry); err != nil {
		t.Fatalf("push: %v", err)
	}

	if err := s.ReplayDLQ(ctx, entry.ID); err != nil {
		t.Fatalf("replay: %v", err)
	}

	got, err := s.GetDLQ(ctx, entry.ID)
	if err != nil {
		t.Fatalf("get after replay: %v", err)
	}
	if got.ReplayedAt == nil {
		t.Fatal("expected replayed_at to be set")
	}
}

func TestDLQStore_Count(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		entry := &dlq.Entry{
			ID:         id.NewDLQID(),
			JobID:      id.NewJobID(),
			JobName:    "count-test",
			Queue:      "default",
			Payload:    []byte(`{}`),
			Error:      "error",
			RetryCount: 3,
			FailedAt:   time.Now().UTC(),
			CreatedAt:  time.Now().UTC(),
		}
		if err := s.PushDLQ(ctx, entry); err != nil {
			t.Fatalf("push: %v", err)
		}
	}

	count, err := s.CountDLQ(ctx)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}
}

// ──────────────────────────────────────────────────
// Event Store tests
// ──────────────────────────────────────────────────

func TestEventStore_PublishAndSubscribe(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	evt := &event.Event{
		ID:        id.NewEventID(),
		Name:      "order.completed",
		Payload:   []byte(`{"order_id":"123"}`),
		CreatedAt: time.Now().UTC(),
	}

	if err := s.PublishEvent(ctx, evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Subscribe should find it.
	got, err := s.SubscribeEvent(ctx, "order.completed", 5*time.Second)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if got == nil {
		t.Fatal("expected event, got nil")
	}
	if got.Name != "order.completed" {
		t.Fatalf("expected order.completed, got %s", got.Name)
	}
}

func TestEventStore_AckEvent(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	evt := &event.Event{
		ID:        id.NewEventID(),
		Name:      "ack-test",
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.PublishEvent(ctx, evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if err := s.AckEvent(ctx, evt.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// After ack, subscribe should timeout (no unacked events).
	got, err := s.SubscribeEvent(ctx, "ack-test", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("subscribe after ack: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil after ack, got event")
	}
}

func TestEventStore_SubscribeTimeout(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Subscribe to non-existent event — should timeout.
	got, err := s.SubscribeEvent(ctx, "nonexistent", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil on timeout")
	}
}

// ──────────────────────────────────────────────────
// Cluster Store tests
// ──────────────────────────────────────────────────

func TestClusterStore_RegisterAndList(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	w := &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    "worker-1",
		Queues:      []string{"default", "high"},
		Concurrency: 10,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		Metadata:    map[string]string{"version": "1.0"},
		CreatedAt:   time.Now().UTC(),
	}

	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("register: %v", err)
	}

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("expected 1, got %d", len(workers))
	}
	if workers[0].Hostname != "worker-1" {
		t.Fatalf("expected worker-1, got %s", workers[0].Hostname)
	}
}

func TestClusterStore_Deregister(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	w := &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    "ephemeral",
		Queues:      []string{"default"},
		Concurrency: 5,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := s.DeregisterWorker(ctx, w.ID); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(workers) != 0 {
		t.Fatalf("expected 0, got %d", len(workers))
	}
}

func TestClusterStore_HeartbeatAndReap(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	w := &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    "stale-worker",
		Queues:      []string{"default"},
		Concurrency: 5,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC().Add(-5 * time.Minute),
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Should be reaped with 1-minute threshold.
	dead, err := s.ReapDeadWorkers(ctx, 1*time.Minute)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(dead) != 1 {
		t.Fatalf("expected 1 dead, got %d", len(dead))
	}

	// Heartbeat refreshes.
	if err = s.HeartbeatWorker(ctx, w.ID); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	dead, err = s.ReapDeadWorkers(ctx, 1*time.Minute)
	if err != nil {
		t.Fatalf("reap after heartbeat: %v", err)
	}
	if len(dead) != 0 {
		t.Fatalf("expected 0 dead after heartbeat, got %d", len(dead))
	}
}

func TestClusterStore_Leadership(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	w1 := &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    "leader-1",
		Queues:      []string{"default"},
		Concurrency: 10,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}
	w2 := &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    "leader-2",
		Queues:      []string{"default"},
		Concurrency: 10,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}

	for _, w := range []*cluster.Worker{w1, w2} {
		if err := s.RegisterWorker(ctx, w); err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	// w1 acquires leadership.
	acquired, err := s.AcquireLeadership(ctx, w1.ID, 30*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquired")
	}

	// w2 cannot acquire.
	acquired, err = s.AcquireLeadership(ctx, w2.ID, 30*time.Second)
	if err != nil {
		t.Fatalf("acquire by w2: %v", err)
	}
	if acquired {
		t.Fatal("expected not acquired by w2")
	}

	// GetLeader returns w1.
	leader, err := s.GetLeader(ctx)
	if err != nil {
		t.Fatalf("get leader: %v", err)
	}
	if leader == nil {
		t.Fatal("expected leader")
	}
	if leader.ID.String() != w1.ID.String() {
		t.Fatalf("expected w1 as leader, got %s", leader.ID.String())
	}

	// w1 renews.
	renewed, err := s.RenewLeadership(ctx, w1.ID, 30*time.Second)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !renewed {
		t.Fatal("expected renewed")
	}

	// w2 cannot renew (not leader).
	renewed, err = s.RenewLeadership(ctx, w2.ID, 30*time.Second)
	if err != nil {
		t.Fatalf("renew by w2: %v", err)
	}
	if renewed {
		t.Fatal("expected not renewed by w2")
	}
}

func TestClusterStore_LeaderExpiry(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	w1 := &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    "expiring-leader",
		Queues:      []string{"default"},
		Concurrency: 10,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}
	w2 := &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    "new-leader",
		Queues:      []string{"default"},
		Concurrency: 10,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}

	for _, w := range []*cluster.Worker{w1, w2} {
		if err := s.RegisterWorker(ctx, w); err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	// w1 acquires with very short TTL.
	acquired, err := s.AcquireLeadership(ctx, w1.ID, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquired")
	}

	// Wait for TTL to expire.
	time.Sleep(50 * time.Millisecond)

	// w2 should now be able to acquire.
	acquired, err = s.AcquireLeadership(ctx, w2.ID, 30*time.Second)
	if err != nil {
		t.Fatalf("acquire by w2: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquired by w2 after expiry")
	}
}
