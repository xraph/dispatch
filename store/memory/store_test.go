package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/event"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/workflow"
)

// ──────────────────────────────────────────────────
// Lifecycle tests
// ──────────────────────────────────────────────────

func TestLifecycle(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Migrate", func() error { return s.Migrate(ctx) }},
		{"Ping", func() error { return s.Ping(ctx) }},
		{"Close", func() error { return s.Close() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(); err != nil {
				t.Fatalf("%s returned error: %v", tt.name, err)
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Job Store tests
// ──────────────────────────────────────────────────

func newJob(name, queue string, state job.State, priority int) *job.Job {
	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      queue,
		Payload:    []byte(`{"test":true}`),
		State:      state,
		Priority:   priority,
		MaxRetries: 3,
		RunAt:      time.Now().UTC().Add(-time.Second), // eligible immediately
	}
	return j
}

func TestJobEnqueueAndGet(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	j := newJob("test-job", "default", job.StatePending, 0)

	tests := []struct {
		name    string
		fn      func() error
		wantErr error
	}{
		{
			name:    "enqueue new job",
			fn:      func() error { return s.EnqueueJob(ctx, j) },
			wantErr: nil,
		},
		{
			name:    "enqueue duplicate job",
			fn:      func() error { return s.EnqueueJob(ctx, j) },
			wantErr: dispatch.ErrJobAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got error %v, want %v", err, tt.wantErr)
			}
		})
	}

	// Verify Get.
	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Name != j.Name {
		t.Fatalf("got name %q, want %q", got.Name, j.Name)
	}

	// Get non-existent.
	_, err = s.GetJob(ctx, id.NewJobID())
	if !errors.Is(err, dispatch.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestJobDequeue(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	// Enqueue jobs with different priorities and queues.
	j1 := newJob("low", "default", job.StatePending, 1)
	j2 := newJob("high", "default", job.StatePending, 10)
	j3 := newJob("other-queue", "critical", job.StatePending, 5)

	for _, j := range []*job.Job{j1, j2, j3} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("EnqueueJob: %v", err)
		}
	}

	tests := []struct {
		name      string
		queues    []string
		limit     int
		wantCount int
		wantFirst string // expected first job name (highest priority)
	}{
		{
			name:      "dequeue from default queue",
			queues:    []string{"default"},
			limit:     10,
			wantCount: 2,
			wantFirst: "high",
		},
		{
			name:      "dequeue from critical queue",
			queues:    []string{"critical"},
			limit:     10,
			wantCount: 1,
			wantFirst: "other-queue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs, err := s.DequeueJobs(ctx, tt.queues, tt.limit)
			if err != nil {
				t.Fatalf("DequeueJobs: %v", err)
			}
			if len(jobs) != tt.wantCount {
				t.Fatalf("got %d jobs, want %d", len(jobs), tt.wantCount)
			}
			if len(jobs) > 0 && jobs[0].Name != tt.wantFirst {
				t.Fatalf("first job name = %q, want %q", jobs[0].Name, tt.wantFirst)
			}
			for _, j := range jobs {
				if j.State != job.StateRunning {
					t.Fatalf("dequeued job state = %q, want %q", j.State, job.StateRunning)
				}
			}
		})
	}
}

func TestJobDequeueLimitAndRunAt(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	// Job in the future — should not be dequeued.
	jFuture := newJob("future", "default", job.StatePending, 1)
	jFuture.RunAt = time.Now().UTC().Add(time.Hour)

	jReady := newJob("ready", "default", job.StatePending, 1)

	for _, j := range []*job.Job{jFuture, jReady} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatalf("EnqueueJob: %v", err)
		}
	}

	jobs, err := s.DequeueJobs(ctx, []string{"default"}, 10)
	if err != nil {
		t.Fatalf("DequeueJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1 (future job should be excluded)", len(jobs))
	}
	if jobs[0].Name != "ready" {
		t.Fatalf("dequeued job = %q, want %q", jobs[0].Name, "ready")
	}
}

func TestJobUpdate(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	j := newJob("update-me", "default", job.StatePending, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatal(err)
	}

	j.State = job.StateCompleted
	if err := s.UpdateJob(ctx, j); err != nil {
		t.Fatal(err)
	}

	got, _ := s.GetJob(ctx, j.ID)
	if got.State != job.StateCompleted {
		t.Fatalf("state = %q, want %q", got.State, job.StateCompleted)
	}

	// Update non-existent.
	missing := newJob("missing", "default", job.StatePending, 0)
	if err := s.UpdateJob(ctx, missing); !errors.Is(err, dispatch.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestJobDelete(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	j := newJob("delete-me", "default", job.StatePending, 0)
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteJob(ctx, j.ID); err != nil {
		t.Fatal(err)
	}

	_, err := s.GetJob(ctx, j.ID)
	if !errors.Is(err, dispatch.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound after delete, got %v", err)
	}

	// Delete non-existent.
	if err := s.DeleteJob(ctx, id.NewJobID()); !errors.Is(err, dispatch.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestJobListByState(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	j1 := newJob("pending1", "default", job.StatePending, 0)
	j2 := newJob("pending2", "default", job.StatePending, 0)
	j3 := newJob("running1", "default", job.StateRunning, 0)

	for _, j := range []*job.Job{j1, j2, j3} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name      string
		state     job.State
		opts      job.ListOpts
		wantCount int
	}{
		{"all pending", job.StatePending, job.ListOpts{}, 2},
		{"all running", job.StateRunning, job.ListOpts{}, 1},
		{"pending with limit", job.StatePending, job.ListOpts{Limit: 1}, 1},
		{"pending with offset", job.StatePending, job.ListOpts{Offset: 1}, 1},
		{"completed (none)", job.StateCompleted, job.ListOpts{}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs, err := s.ListJobsByState(ctx, tt.state, tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != tt.wantCount {
				t.Fatalf("got %d, want %d", len(jobs), tt.wantCount)
			}
		})
	}
}

func TestJobHeartbeatAndReapStale(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	j := newJob("heartbeat-job", "default", job.StateRunning, 0)
	old := time.Now().UTC().Add(-time.Minute)
	j.HeartbeatAt = &old

	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatal(err)
	}

	// Before heartbeat, should be stale.
	stale, err := s.ReapStaleJobs(ctx, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale job, got %d", len(stale))
	}

	// Heartbeat.
	err = s.HeartbeatJob(ctx, j.ID, id.NewWorkerID())
	if err != nil {
		t.Fatal(err)
	}

	// After heartbeat, should not be stale.
	stale, err = s.ReapStaleJobs(ctx, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 0 {
		t.Fatalf("expected 0 stale jobs after heartbeat, got %d", len(stale))
	}
}

func TestJobCount(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	j1 := newJob("count1", "default", job.StatePending, 0)
	j2 := newJob("count2", "critical", job.StatePending, 0)
	j3 := newJob("count3", "default", job.StateRunning, 0)

	for _, j := range []*job.Job{j1, j2, j3} {
		if err := s.EnqueueJob(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name string
		opts job.CountOpts
		want int64
	}{
		{"all", job.CountOpts{}, 3},
		{"default queue", job.CountOpts{Queue: "default"}, 2},
		{"pending state", job.CountOpts{State: job.StatePending}, 2},
		{"default+pending", job.CountOpts{Queue: "default", State: job.StatePending}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := s.CountJobs(ctx, tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			if count != tt.want {
				t.Fatalf("count = %d, want %d", count, tt.want)
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Workflow Store tests
// ──────────────────────────────────────────────────

func newRun(name string, state workflow.RunState) *workflow.Run {
	return &workflow.Run{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewRunID(),
		Name:      name,
		State:     state,
		Input:     []byte(`{"input":true}`),
		StartedAt: time.Now().UTC(),
	}
}

func TestWorkflowCreateAndGetRun(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	r := newRun("test-wf", workflow.RunStateRunning)
	if err := s.CreateRun(ctx, r); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetRun(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != r.Name {
		t.Fatalf("name = %q, want %q", got.Name, r.Name)
	}

	// Not found.
	_, err = s.GetRun(ctx, id.NewRunID())
	if !errors.Is(err, dispatch.ErrRunNotFound) {
		t.Fatalf("expected ErrRunNotFound, got %v", err)
	}
}

func TestWorkflowUpdateRun(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	r := newRun("update-wf", workflow.RunStateRunning)
	if err := s.CreateRun(ctx, r); err != nil {
		t.Fatal(err)
	}

	r.State = workflow.RunStateCompleted
	now := time.Now().UTC()
	r.CompletedAt = &now
	if err := s.UpdateRun(ctx, r); err != nil {
		t.Fatal(err)
	}

	got, _ := s.GetRun(ctx, r.ID)
	if got.State != workflow.RunStateCompleted {
		t.Fatalf("state = %q, want %q", got.State, workflow.RunStateCompleted)
	}

	// Update non-existent.
	missing := newRun("missing", workflow.RunStateRunning)
	if err := s.UpdateRun(ctx, missing); !errors.Is(err, dispatch.ErrRunNotFound) {
		t.Fatalf("expected ErrRunNotFound, got %v", err)
	}
}

func TestWorkflowListRuns(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	r1 := newRun("wf1", workflow.RunStateRunning)
	r2 := newRun("wf2", workflow.RunStateCompleted)
	r3 := newRun("wf3", workflow.RunStateRunning)

	for _, r := range []*workflow.Run{r1, r2, r3} {
		if err := s.CreateRun(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name      string
		opts      workflow.ListOpts
		wantCount int
	}{
		{"all", workflow.ListOpts{}, 3},
		{"running only", workflow.ListOpts{State: workflow.RunStateRunning}, 2},
		{"completed only", workflow.ListOpts{State: workflow.RunStateCompleted}, 1},
		{"with limit", workflow.ListOpts{Limit: 1}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runs, err := s.ListRuns(ctx, tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			if len(runs) != tt.wantCount {
				t.Fatalf("got %d, want %d", len(runs), tt.wantCount)
			}
		})
	}
}

func TestWorkflowCheckpoints(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	runID := id.NewRunID()
	data1 := []byte(`{"step":"one"}`)
	data2 := []byte(`{"step":"two"}`)

	// Save checkpoints.
	if err := s.SaveCheckpoint(ctx, runID, "step-1", data1); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(ctx, runID, "step-2", data2); err != nil {
		t.Fatal(err)
	}

	// Get specific checkpoint.
	got, err := s.GetCheckpoint(ctx, runID, "step-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data1) {
		t.Fatalf("data = %q, want %q", got, data1)
	}

	// Get non-existent checkpoint.
	got, err = s.GetCheckpoint(ctx, runID, "step-99")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing checkpoint, got %q", got)
	}

	// List all checkpoints for run.
	cps, err := s.ListCheckpoints(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(cps) != 2 {
		t.Fatalf("got %d checkpoints, want 2", len(cps))
	}

	// Overwrite checkpoint.
	newData := []byte(`{"step":"one-updated"}`)
	if err := s.SaveCheckpoint(ctx, runID, "step-1", newData); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetCheckpoint(ctx, runID, "step-1")
	if string(got) != string(newData) {
		t.Fatalf("overwritten data = %q, want %q", got, newData)
	}
}

// ──────────────────────────────────────────────────
// Cron Store tests
// ──────────────────────────────────────────────────

func newCronEntry(name, schedule string) *cron.Entry {
	return &cron.Entry{
		Entity:   dispatch.NewEntity(),
		ID:       id.NewCronID(),
		Name:     name,
		Schedule: schedule,
		JobName:  "test-job",
		Enabled:  true,
	}
}

func TestCronRegisterAndGet(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	e := newCronEntry("every-minute", "* * * * *")
	if err := s.RegisterCron(ctx, e); err != nil {
		t.Fatal(err)
	}

	// Duplicate name.
	e2 := newCronEntry("every-minute", "*/5 * * * *")
	if err := s.RegisterCron(ctx, e2); !errors.Is(err, dispatch.ErrDuplicateCron) {
		t.Fatalf("expected ErrDuplicateCron, got %v", err)
	}

	got, err := s.GetCron(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != e.Name {
		t.Fatalf("name = %q, want %q", got.Name, e.Name)
	}

	// Not found.
	_, err = s.GetCron(ctx, id.NewCronID())
	if !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Fatalf("expected ErrCronNotFound, got %v", err)
	}
}

func TestCronListAndDelete(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	e1 := newCronEntry("cron-a", "* * * * *")
	e2 := newCronEntry("cron-b", "*/5 * * * *")

	for _, e := range []*cron.Entry{e1, e2} {
		if err := s.RegisterCron(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	list, err := s.ListCrons(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("got %d, want 2", len(list))
	}

	// Delete.
	if err := s.DeleteCron(ctx, e1.ID); err != nil {
		t.Fatal(err)
	}
	list, _ = s.ListCrons(ctx)
	if len(list) != 1 {
		t.Fatalf("after delete: got %d, want 1", len(list))
	}

	// Delete non-existent.
	if err := s.DeleteCron(ctx, id.NewCronID()); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Fatalf("expected ErrCronNotFound, got %v", err)
	}
}

func TestCronLocking(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	e := newCronEntry("lockable", "* * * * *")
	if err := s.RegisterCron(ctx, e); err != nil {
		t.Fatal(err)
	}

	w1 := id.NewWorkerID()
	w2 := id.NewWorkerID()
	ttl := 5 * time.Minute

	// Worker 1 acquires lock.
	ok, err := s.AcquireCronLock(ctx, e.ID, w1, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected lock to be acquired")
	}

	// Worker 2 cannot acquire lock.
	ok, err = s.AcquireCronLock(ctx, e.ID, w2, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected lock to fail for worker 2")
	}

	// Worker 1 can re-acquire (extend).
	ok, err = s.AcquireCronLock(ctx, e.ID, w1, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected worker 1 to re-acquire lock")
	}

	// Release.
	err = s.ReleaseCronLock(ctx, e.ID, w1)
	if err != nil {
		t.Fatal(err)
	}

	// Worker 2 can now acquire.
	ok, err = s.AcquireCronLock(ctx, e.ID, w2, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected worker 2 to acquire after release")
	}
}

func TestCronUpdateLastRun(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	e := newCronEntry("last-run", "* * * * *")
	if err := s.RegisterCron(ctx, e); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if err := s.UpdateCronLastRun(ctx, e.ID, now); err != nil {
		t.Fatal(err)
	}

	got, _ := s.GetCron(ctx, e.ID)
	if got.LastRunAt == nil || !got.LastRunAt.Equal(now) {
		t.Fatalf("LastRunAt = %v, want %v", got.LastRunAt, now)
	}

	// Non-existent.
	if err := s.UpdateCronLastRun(ctx, id.NewCronID(), now); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Fatalf("expected ErrCronNotFound, got %v", err)
	}
}

// ──────────────────────────────────────────────────
// DLQ Store tests
// ──────────────────────────────────────────────────

func newDLQEntry(queue string, failedAt time.Time) *dlq.Entry {
	return &dlq.Entry{
		ID:         id.NewDLQID(),
		JobID:      id.NewJobID(),
		JobName:    "failed-job",
		Queue:      queue,
		Payload:    []byte(`{"fail":true}`),
		Error:      "something went wrong",
		RetryCount: 3,
		FailedAt:   failedAt,
		CreatedAt:  time.Now().UTC(),
	}
}

func TestDLQPushAndGet(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	e := newDLQEntry("default", time.Now().UTC())
	if err := s.PushDLQ(ctx, e); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetDLQ(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.JobName != e.JobName {
		t.Fatalf("job name = %q, want %q", got.JobName, e.JobName)
	}

	// Not found.
	_, err = s.GetDLQ(ctx, id.NewDLQID())
	if !errors.Is(err, dispatch.ErrDLQNotFound) {
		t.Fatalf("expected ErrDLQNotFound, got %v", err)
	}
}

func TestDLQList(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	e1 := newDLQEntry("default", time.Now().UTC())
	e2 := newDLQEntry("critical", time.Now().UTC())
	e3 := newDLQEntry("default", time.Now().UTC())

	for _, e := range []*dlq.Entry{e1, e2, e3} {
		if err := s.PushDLQ(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name      string
		opts      dlq.ListOpts
		wantCount int
	}{
		{"all", dlq.ListOpts{}, 3},
		{"default queue", dlq.ListOpts{Queue: "default"}, 2},
		{"critical queue", dlq.ListOpts{Queue: "critical"}, 1},
		{"with limit", dlq.ListOpts{Limit: 1}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entries, err := s.ListDLQ(ctx, tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != tt.wantCount {
				t.Fatalf("got %d, want %d", len(entries), tt.wantCount)
			}
		})
	}
}

func TestDLQReplay(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	e := newDLQEntry("default", time.Now().UTC())
	if err := s.PushDLQ(ctx, e); err != nil {
		t.Fatal(err)
	}

	if err := s.ReplayDLQ(ctx, e.ID); err != nil {
		t.Fatal(err)
	}

	got, _ := s.GetDLQ(ctx, e.ID)
	if got.ReplayedAt == nil {
		t.Fatal("ReplayedAt should be set after replay")
	}

	// Replay non-existent.
	if err := s.ReplayDLQ(ctx, id.NewDLQID()); !errors.Is(err, dispatch.ErrDLQNotFound) {
		t.Fatalf("expected ErrDLQNotFound, got %v", err)
	}
}

func TestDLQPurge(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	old := time.Now().UTC().Add(-24 * time.Hour)
	recent := time.Now().UTC()

	e1 := newDLQEntry("default", old)
	e2 := newDLQEntry("default", recent)

	for _, e := range []*dlq.Entry{e1, e2} {
		if err := s.PushDLQ(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	cutoff := time.Now().UTC().Add(-time.Hour)
	purged, err := s.PurgeDLQ(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 1 {
		t.Fatalf("purged = %d, want 1", purged)
	}

	count, _ := s.CountDLQ(ctx)
	if count != 1 {
		t.Fatalf("remaining = %d, want 1", count)
	}
}

func TestDLQCount(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	count, err := s.CountDLQ(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("empty store count = %d, want 0", count)
	}

	if err := s.PushDLQ(ctx, newDLQEntry("default", time.Now().UTC())); err != nil {
		t.Fatal(err)
	}

	count, _ = s.CountDLQ(ctx)
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

// ──────────────────────────────────────────────────
// Event Store tests
// ──────────────────────────────────────────────────

func TestEventPublishAndSubscribe(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	evt := &event.Event{
		ID:        id.NewEventID(),
		Name:      "user.created",
		Payload:   []byte(`{"user_id":"123"}`),
		CreatedAt: time.Now().UTC(),
	}

	if err := s.PublishEvent(ctx, evt); err != nil {
		t.Fatal(err)
	}

	// Subscribe should find the event.
	got, err := s.SubscribeEvent(ctx, "user.created", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected event, got nil")
	}
	if got.ID.String() != evt.ID.String() {
		t.Fatalf("event ID mismatch")
	}

	// Subscribe for non-existent event should timeout.
	got, err = s.SubscribeEvent(ctx, "order.placed", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing event, got %v", got)
	}
}

func TestEventAck(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	evt := &event.Event{
		ID:        id.NewEventID(),
		Name:      "test.ack",
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.PublishEvent(ctx, evt); err != nil {
		t.Fatal(err)
	}

	if err := s.AckEvent(ctx, evt.ID); err != nil {
		t.Fatal(err)
	}

	// After ack, subscribe should not find it (acked events are excluded).
	got, err := s.SubscribeEvent(ctx, "test.ack", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatal("expected nil after ack, got event")
	}

	// Ack non-existent.
	if err := s.AckEvent(ctx, id.NewEventID()); !errors.Is(err, dispatch.ErrEventNotFound) {
		t.Fatalf("expected ErrEventNotFound, got %v", err)
	}
}

func TestEventSubscribeContextCancel(t *testing.T) {
	t.Parallel()
	s := New()

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay.
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	_, err := s.SubscribeEvent(ctx, "never-published", 5*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// ──────────────────────────────────────────────────
// Cluster Store tests
// ──────────────────────────────────────────────────

func newWorker(hostname string) *cluster.Worker {
	return &cluster.Worker{
		ID:          id.NewWorkerID(),
		Hostname:    hostname,
		Queues:      []string{"default"},
		Concurrency: 10,
		State:       cluster.WorkerActive,
		LastSeen:    time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}
}

func TestClusterRegisterAndList(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	w1 := newWorker("node-1")
	w2 := newWorker("node-2")

	for _, w := range []*cluster.Worker{w1, w2} {
		if err := s.RegisterWorker(ctx, w); err != nil {
			t.Fatal(err)
		}
	}

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 2 {
		t.Fatalf("got %d workers, want 2", len(workers))
	}
}

func TestClusterDeregister(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	w := newWorker("deregister-me")
	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatal(err)
	}

	if err := s.DeregisterWorker(ctx, w.ID); err != nil {
		t.Fatal(err)
	}

	workers, _ := s.ListWorkers(ctx)
	if len(workers) != 0 {
		t.Fatalf("expected 0 workers after deregister, got %d", len(workers))
	}

	// Deregister non-existent.
	if err := s.DeregisterWorker(ctx, id.NewWorkerID()); !errors.Is(err, dispatch.ErrWorkerNotFound) {
		t.Fatalf("expected ErrWorkerNotFound, got %v", err)
	}
}

func TestClusterHeartbeatAndReap(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	w := newWorker("heartbeat-worker")
	w.LastSeen = time.Now().UTC().Add(-time.Minute)
	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatal(err)
	}

	// Before heartbeat, should be dead.
	dead, err := s.ReapDeadWorkers(ctx, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 {
		t.Fatalf("expected 1 dead worker, got %d", len(dead))
	}

	// Heartbeat.
	err = s.HeartbeatWorker(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}

	// After heartbeat, should not be dead.
	dead, err = s.ReapDeadWorkers(ctx, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 0 {
		t.Fatalf("expected 0 dead workers after heartbeat, got %d", len(dead))
	}

	// Heartbeat non-existent.
	if err := s.HeartbeatWorker(ctx, id.NewWorkerID()); !errors.Is(err, dispatch.ErrWorkerNotFound) {
		t.Fatalf("expected ErrWorkerNotFound, got %v", err)
	}
}

func TestClusterLeadership(t *testing.T) {
	t.Parallel()
	s := New()
	ctx := context.Background()

	w1 := newWorker("leader-1")
	w2 := newWorker("leader-2")

	for _, w := range []*cluster.Worker{w1, w2} {
		if err := s.RegisterWorker(ctx, w); err != nil {
			t.Fatal(err)
		}
	}

	ttl := 5 * time.Minute

	// No leader initially.
	leader, err := s.GetLeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if leader != nil {
		t.Fatal("expected no leader initially")
	}

	// Worker 1 acquires leadership.
	ok, err := s.AcquireLeadership(ctx, w1.ID, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected worker 1 to acquire leadership")
	}

	leader, _ = s.GetLeader(ctx)
	if leader == nil || leader.ID.String() != w1.ID.String() {
		t.Fatal("leader should be worker 1")
	}

	// Worker 2 cannot acquire while worker 1 holds.
	ok, err = s.AcquireLeadership(ctx, w2.ID, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected worker 2 to fail acquiring leadership")
	}

	// Worker 1 renews.
	ok, err = s.RenewLeadership(ctx, w1.ID, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected worker 1 to renew")
	}

	// Worker 2 cannot renew (not leader).
	ok, err = s.RenewLeadership(ctx, w2.ID, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected worker 2 renewal to fail")
	}
}
