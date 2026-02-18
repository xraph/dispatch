package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"

	"github.com/xraph/forge"
)

// ──────────────────────────────────────────────────
// Test payloads
// ──────────────────────────────────────────────────

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// ──────────────────────────────────────────────────
// End-to-end: Register → Enqueue → Process
// ──────────────────────────────────────────────────

func TestEngine_EndToEnd_RegisterEnqueueProcess(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(
		dispatch.WithStore(s),
		dispatch.WithConcurrency(2),
		dispatch.WithQueues([]string{"default"}),
	)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	var processed atomic.Bool
	var gotPayload emailPayload
	def := job.NewDefinition("send-email", func(_ context.Context, p emailPayload) error {
		gotPayload = p
		processed.Store(true)
		return nil
	})
	engine.Register(eng, def)

	// Enqueue.
	j, err := engine.Enqueue(context.Background(), eng, "send-email", emailPayload{
		To:      "alice@example.com",
		Subject: "Hello from Dispatch",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if j.Name != "send-email" {
		t.Errorf("job.Name = %q, want %q", j.Name, "send-email")
	}
	if j.State != job.StatePending {
		t.Errorf("job.State = %q, want %q", j.State, job.StatePending)
	}

	// Start processing.
	if startErr := eng.Start(context.Background()); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	// Wait for processing.
	deadline := time.After(5 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job to be processed")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Verify payload.
	if gotPayload.To != "alice@example.com" {
		t.Errorf("payload.To = %q, want %q", gotPayload.To, "alice@example.com")
	}
	if gotPayload.Subject != "Hello from Dispatch" {
		t.Errorf("payload.Subject = %q, want %q", gotPayload.Subject, "Hello from Dispatch")
	}

	// Verify job state in store.
	got, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.State != job.StateCompleted {
		t.Errorf("job.State = %q, want %q", got.State, job.StateCompleted)
	}

	// Stop.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := eng.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ──────────────────────────────────────────────────
// Extension lifecycle events
// ──────────────────────────────────────────────────

type lifecycleTracker struct {
	enqueued      atomic.Bool
	started       atomic.Bool
	completed     atomic.Bool
	failed        atomic.Bool
	shutdown      atomic.Bool
	retryingCount atomic.Int32
	dlq           atomic.Bool

	// Workflow hooks.
	wfStarted            atomic.Bool
	wfCompleted          atomic.Bool
	wfFailed             atomic.Bool
	wfStepCompletedCount atomic.Int32
	wfStepFailedCount    atomic.Int32

	// Cron hooks.
	cronFired      atomic.Bool
	cronFiredEntry atomic.Value // stores string
	cronFiredJobID atomic.Value // stores id.JobID
}

func (e *lifecycleTracker) Name() string { return "lifecycle-tracker" }

func (e *lifecycleTracker) OnJobEnqueued(_ context.Context, _ *job.Job) error {
	e.enqueued.Store(true)
	return nil
}

func (e *lifecycleTracker) OnJobStarted(_ context.Context, _ *job.Job) error {
	e.started.Store(true)
	return nil
}

func (e *lifecycleTracker) OnJobCompleted(_ context.Context, _ *job.Job, _ time.Duration) error {
	e.completed.Store(true)
	return nil
}

func (e *lifecycleTracker) OnJobFailed(_ context.Context, _ *job.Job, _ error) error {
	e.failed.Store(true)
	return nil
}

func (e *lifecycleTracker) OnJobRetrying(_ context.Context, _ *job.Job, _ int, _ time.Time) error {
	e.retryingCount.Add(1)
	return nil
}

func (e *lifecycleTracker) OnJobDLQ(_ context.Context, _ *job.Job, _ error) error {
	e.dlq.Store(true)
	return nil
}

func (e *lifecycleTracker) OnShutdown(_ context.Context) error {
	e.shutdown.Store(true)
	return nil
}

func (e *lifecycleTracker) OnWorkflowStarted(_ context.Context, _ *workflow.Run) error {
	e.wfStarted.Store(true)
	return nil
}

func (e *lifecycleTracker) OnWorkflowCompleted(_ context.Context, _ *workflow.Run, _ time.Duration) error {
	e.wfCompleted.Store(true)
	return nil
}

func (e *lifecycleTracker) OnWorkflowFailed(_ context.Context, _ *workflow.Run, _ error) error {
	e.wfFailed.Store(true)
	return nil
}

func (e *lifecycleTracker) OnWorkflowStepCompleted(_ context.Context, _ *workflow.Run, _ string, _ time.Duration) error {
	e.wfStepCompletedCount.Add(1)
	return nil
}

func (e *lifecycleTracker) OnWorkflowStepFailed(_ context.Context, _ *workflow.Run, _ string, _ error) error {
	e.wfStepFailedCount.Add(1)
	return nil
}

func (e *lifecycleTracker) OnCronFired(_ context.Context, entryName string, jobID id.JobID) error {
	e.cronFired.Store(true)
	e.cronFiredEntry.Store(entryName)
	e.cronFiredJobID.Store(jobID)
	return nil
}

func TestEngine_ExtensionLifecycleEvents(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	tracker := &lifecycleTracker{}
	eng, err := engine.Build(d, engine.WithExtension(tracker))
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	var processed atomic.Bool
	engine.Register(eng, job.NewDefinition("tracked-job", func(_ context.Context, _ struct{}) error {
		processed.Store(true)
		return nil
	}))

	// Enqueue fires OnJobEnqueued.
	_, err = engine.Enqueue(context.Background(), eng, "tracked-job", struct{}{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !tracker.enqueued.Load() {
		t.Error("expected OnJobEnqueued to fire on enqueue")
	}

	// Start and wait for processing.
	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Give extensions a moment to fire.
	time.Sleep(50 * time.Millisecond)

	if !tracker.started.Load() {
		t.Error("expected OnJobStarted to fire")
	}
	if !tracker.completed.Load() {
		t.Error("expected OnJobCompleted to fire")
	}

	// Stop fires OnShutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := eng.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if !tracker.shutdown.Load() {
		t.Error("expected OnShutdown to fire on stop")
	}
}

// ──────────────────────────────────────────────────
// Failed job triggers OnJobFailed
// ──────────────────────────────────────────────────

func TestEngine_FailedJobExtension(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	tracker := &lifecycleTracker{}
	eng, err := engine.Build(d, engine.WithExtension(tracker))
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	var processed atomic.Bool
	engine.Register(eng, job.NewDefinition("failing-job", func(_ context.Context, _ struct{}) error {
		processed.Store(true)
		return errors.New("intentional failure")
	}))

	// MaxRetries=0 so the job goes directly to DLQ/failed with no retries.
	if _, err := engine.Enqueue(context.Background(), eng, "failing-job", struct{}{},
		job.WithMaxRetries(0),
	); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Give extensions a moment to fire.
	time.Sleep(50 * time.Millisecond)

	if !tracker.failed.Load() {
		t.Error("expected OnJobFailed to fire for failing job")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := eng.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ──────────────────────────────────────────────────
// Scope capture and restore
// ──────────────────────────────────────────────────

func TestEngine_ScopePassthrough(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	var gotAppID, gotOrgID string
	var processed atomic.Bool
	engine.Register(eng, job.NewDefinition("scoped-job", func(ctx context.Context, _ struct{}) error {
		sc, ok := forge.ScopeFrom(ctx)
		if ok {
			gotAppID = sc.AppID()
			gotOrgID = sc.OrgID()
		}
		processed.Store(true)
		return nil
	}))

	// Enqueue with scope in context.
	ctx := forge.WithScope(context.Background(), forge.NewOrgScope("app_123", "org_456"))
	if _, err := engine.Enqueue(ctx, eng, "scoped-job", struct{}{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if gotAppID != "app_123" {
		t.Errorf("appID = %q, want %q", gotAppID, "app_123")
	}
	if gotOrgID != "org_456" {
		t.Errorf("orgID = %q, want %q", gotOrgID, "org_456")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := eng.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ──────────────────────────────────────────────────
// Graceful shutdown drains queue
// ──────────────────────────────────────────────────

func TestEngine_GracefulShutdown(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s), dispatch.WithConcurrency(4))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	engine.Register(eng, job.NewDefinition("noop", func(_ context.Context, _ struct{}) error {
		return nil
	}))

	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Let the pool start.
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ──────────────────────────────────────────────────
// Enqueue with options
// ──────────────────────────────────────────────────

func TestEngine_EnqueueWithOptions(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	engine.Register(eng, job.NewDefinition("priority-job", func(_ context.Context, _ struct{}) error {
		return nil
	}))

	scheduled := time.Now().Add(1 * time.Hour)
	j, err := engine.Enqueue(context.Background(), eng, "priority-job", struct{}{},
		job.WithQueue("critical"),
		job.WithPriority(10),
		job.WithMaxRetries(5),
		job.WithRunAt(scheduled),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if j.Queue != "critical" {
		t.Errorf("Queue = %q, want %q", j.Queue, "critical")
	}
	if j.Priority != 10 {
		t.Errorf("Priority = %d, want %d", j.Priority, 10)
	}
	if j.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want %d", j.MaxRetries, 5)
	}
	if !j.RunAt.Equal(scheduled) {
		t.Errorf("RunAt = %v, want %v", j.RunAt, scheduled)
	}
}

// ──────────────────────────────────────────────────
// Build errors
// ──────────────────────────────────────────────────

func TestEngine_BuildNoStore(t *testing.T) {
	d, err := dispatch.New()
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	_, err = engine.Build(d)
	if !errors.Is(err, dispatch.ErrNoStore) {
		t.Fatalf("expected ErrNoStore, got: %v", err)
	}
}

// badStore only implements Storer but not job.Store.
type badStore struct{}

func (badStore) Migrate(_ context.Context) error { return nil }
func (badStore) Ping(_ context.Context) error    { return nil }
func (badStore) Close() error                    { return nil }

func TestEngine_BuildBadStore(t *testing.T) {
	d, err := dispatch.New(dispatch.WithStore(badStore{}))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	_, err = engine.Build(d)
	if err == nil {
		t.Fatal("expected error for store that doesn't implement job.Store")
	}
}

// ──────────────────────────────────────────────────
// Multiple jobs processed concurrently
// ──────────────────────────────────────────────────

func TestEngine_ConcurrentJobs(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(
		dispatch.WithStore(s),
		dispatch.WithConcurrency(4),
		dispatch.WithLogger(slog.Default()),
	)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	var count atomic.Int32
	engine.Register(eng, job.NewDefinition("counter", func(_ context.Context, _ struct{}) error {
		count.Add(1)
		time.Sleep(10 * time.Millisecond) // Simulate work.
		return nil
	}))

	// Enqueue 20 jobs.
	for range 20 {
		if _, err := engine.Enqueue(context.Background(), eng, "counter", struct{}{}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for all jobs.
	deadline := time.After(10 * time.Second)
	for count.Load() < 20 {
		select {
		case <-deadline:
			t.Fatalf("timed out: only %d/20 jobs processed", count.Load())
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := eng.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if got := count.Load(); got != 20 {
		t.Errorf("processed %d jobs, want 20", got)
	}
}

// ──────────────────────────────────────────────────
// Phase 3: Retry, Backoff & DLQ
// ──────────────────────────────────────────────────

func TestEngine_RetryThenSucceed(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	tracker := &lifecycleTracker{}
	eng, err := engine.Build(d,
		engine.WithExtension(tracker),
		engine.WithBackoff(backoff.NewConstant(10*time.Millisecond)),
	)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	// Handler fails first 2 calls, succeeds on 3rd.
	var attempts atomic.Int32
	var processed atomic.Bool
	engine.Register(eng, job.NewDefinition("retry-succeed", func(_ context.Context, _ struct{}) error {
		n := attempts.Add(1)
		if n <= 2 {
			return errors.New("transient error")
		}
		processed.Store(true)
		return nil
	}))

	j, err := engine.Enqueue(context.Background(), eng, "retry-succeed", struct{}{},
		job.WithMaxRetries(3),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if startErr := eng.Start(context.Background()); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	deadline := time.After(10 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job to succeed after retries")
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	// Give extensions a moment to fire.
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if stopErr := eng.Stop(ctx); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	// Verify job state.
	got, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.State != job.StateCompleted {
		t.Errorf("job state = %q, want %q", got.State, job.StateCompleted)
	}
	if got.RetryCount != 2 {
		t.Errorf("RetryCount = %d, want 2", got.RetryCount)
	}

	// Verify extensions.
	if tracker.retryingCount.Load() != 2 {
		t.Errorf("retrying events = %d, want 2", tracker.retryingCount.Load())
	}
	if tracker.dlq.Load() {
		t.Error("expected no DLQ event")
	}
	if !tracker.completed.Load() {
		t.Error("expected OnJobCompleted to fire")
	}
}

func TestEngine_ExhaustRetriesToDLQ(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	tracker := &lifecycleTracker{}
	eng, err := engine.Build(d,
		engine.WithExtension(tracker),
		engine.WithBackoff(backoff.NewConstant(10*time.Millisecond)),
	)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	// Handler always fails.
	var attempts atomic.Int32
	engine.Register(eng, job.NewDefinition("always-fail", func(_ context.Context, _ struct{}) error {
		attempts.Add(1)
		return errors.New("permanent error")
	}))

	j, err := engine.Enqueue(context.Background(), eng, "always-fail", struct{}{},
		job.WithMaxRetries(2),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if startErr := eng.Start(context.Background()); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	// Wait for the job to exhaust retries and hit DLQ.
	deadline := time.After(10 * time.Second)
	for !tracker.dlq.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job to reach DLQ")
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	// Give extensions a moment to fire.
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if stopErr := eng.Stop(ctx); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	// Verify job state.
	got, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.State != job.StateFailed {
		t.Errorf("job state = %q, want %q", got.State, job.StateFailed)
	}

	// Verify DLQ.
	dlqCount, err := s.CountDLQ(context.Background())
	if err != nil {
		t.Fatalf("CountDLQ: %v", err)
	}
	if dlqCount != 1 {
		t.Errorf("DLQ count = %d, want 1", dlqCount)
	}

	// Verify extensions.
	if !tracker.failed.Load() {
		t.Error("expected OnJobFailed to fire")
	}
	if !tracker.dlq.Load() {
		t.Error("expected OnJobDLQ to fire")
	}
	if tracker.retryingCount.Load() != 2 {
		t.Errorf("retrying events = %d, want 2", tracker.retryingCount.Load())
	}
}

func TestEngine_DLQReplay(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	tracker := &lifecycleTracker{}
	eng, err := engine.Build(d,
		engine.WithExtension(tracker),
		engine.WithBackoff(backoff.NewConstant(10*time.Millisecond)),
	)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	// Phase 1: Handler always fails to send job to DLQ.
	var attempts atomic.Int32
	var succeeded atomic.Bool
	engine.Register(eng, job.NewDefinition("replay-job", func(_ context.Context, _ struct{}) error {
		n := attempts.Add(1)
		if n <= 1 {
			return errors.New("initial failure")
		}
		succeeded.Store(true)
		return nil
	}))

	_, err = engine.Enqueue(context.Background(), eng, "replay-job", struct{}{},
		job.WithMaxRetries(0), // No retries — go straight to DLQ.
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if startErr := eng.Start(context.Background()); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	// Wait for DLQ.
	deadline := time.After(10 * time.Second)
	for !tracker.dlq.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job to reach DLQ")
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	// Give it a moment for store updates.
	time.Sleep(50 * time.Millisecond)

	// Find the DLQ entry.
	dlqStore := eng.DLQService().DLQStore()
	entries, listErr := dlqStore.ListDLQ(context.Background(), dlq.ListOpts{})
	if listErr != nil {
		t.Fatalf("ListDLQ: %v", listErr)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(entries))
	}

	// Phase 2: Replay the DLQ entry. The handler will succeed on the 2nd attempt.
	replayedJob, replayErr := eng.DLQService().Replay(context.Background(), entries[0].ID)
	if replayErr != nil {
		t.Fatalf("Replay: %v", replayErr)
	}

	// Wait for the replayed job to succeed.
	deadline = time.After(10 * time.Second)
	for !succeeded.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for replayed job to succeed")
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	// Give store time to update.
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if stopErr := eng.Stop(ctx); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	// Verify replayed job state.
	got, err := s.GetJob(context.Background(), replayedJob.ID)
	if err != nil {
		t.Fatalf("GetJob for replayed job: %v", err)
	}
	if got.State != job.StateCompleted {
		t.Errorf("replayed job state = %q, want %q", got.State, job.StateCompleted)
	}

	// Verify DLQ entry has ReplayedAt set.
	entry, err := dlqStore.GetDLQ(context.Background(), entries[0].ID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if entry.ReplayedAt == nil {
		t.Error("expected DLQ entry ReplayedAt to be set after replay")
	}
}

func TestEngine_ZeroRetries_DirectToDLQ(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	tracker := &lifecycleTracker{}
	eng, err := engine.Build(d,
		engine.WithExtension(tracker),
		engine.WithBackoff(backoff.NewConstant(10*time.Millisecond)),
	)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	var processed atomic.Bool
	engine.Register(eng, job.NewDefinition("zero-retry-job", func(_ context.Context, _ struct{}) error {
		processed.Store(true)
		return errors.New("immediate failure")
	}))

	_, err = engine.Enqueue(context.Background(), eng, "zero-retry-job", struct{}{},
		job.WithMaxRetries(0),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if startErr := eng.Start(context.Background()); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	// Wait for DLQ event.
	deadline := time.After(10 * time.Second)
	for !tracker.dlq.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for DLQ event")
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if stopErr := eng.Stop(ctx); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	// No retries should have fired.
	if tracker.retryingCount.Load() != 0 {
		t.Errorf("retrying events = %d, want 0", tracker.retryingCount.Load())
	}

	// DLQ and failed should have fired.
	if !tracker.dlq.Load() {
		t.Error("expected OnJobDLQ to fire")
	}
	if !tracker.failed.Load() {
		t.Error("expected OnJobFailed to fire")
	}

	// Verify DLQ count.
	dlqCount, err := s.CountDLQ(context.Background())
	if err != nil {
		t.Fatalf("CountDLQ: %v", err)
	}
	if dlqCount != 1 {
		t.Errorf("DLQ count = %d, want 1", dlqCount)
	}
}

// ──────────────────────────────────────────────────
// Phase 4: Workflows & Step Functions
// ──────────────────────────────────────────────────

// newWorkflowEngine creates an engine with a lifecycleTracker extension.
func newWorkflowEngine(t *testing.T) (*engine.Engine, *memory.Store, *lifecycleTracker) {
	t.Helper()
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}
	tracker := &lifecycleTracker{}
	eng, err := engine.Build(d, engine.WithExtension(tracker))
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}
	return eng, s, tracker
}

func TestEngine_WorkflowMultiStep(t *testing.T) {
	eng, s, tracker := newWorkflowEngine(t)

	var step1, step2, step3 atomic.Bool
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("multi-step", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("step-1", func(_ context.Context) error { step1.Store(true); return nil }); err != nil {
			return err
		}
		if err := wf.Step("step-2", func(_ context.Context) error { step2.Store(true); return nil }); err != nil {
			return err
		}
		return wf.Step("step-3", func(_ context.Context) error { step3.Store(true); return nil })
	}))

	run, err := engine.StartWorkflow(context.Background(), eng, "multi-step", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}

	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}
	if !step1.Load() || !step2.Load() || !step3.Load() {
		t.Errorf("steps: 1=%v 2=%v 3=%v, want all true", step1.Load(), step2.Load(), step3.Load())
	}

	// Verify 3 checkpoints.
	cps, cpErr := s.ListCheckpoints(context.Background(), run.ID)
	if cpErr != nil {
		t.Fatalf("ListCheckpoints: %v", cpErr)
	}
	if len(cps) != 3 {
		t.Errorf("checkpoints = %d, want 3", len(cps))
	}

	// Verify extension hooks.
	if !tracker.wfStarted.Load() {
		t.Error("expected OnWorkflowStarted")
	}
	if !tracker.wfCompleted.Load() {
		t.Error("expected OnWorkflowCompleted")
	}
	if tracker.wfStepCompletedCount.Load() != 3 {
		t.Errorf("step completed events = %d, want 3", tracker.wfStepCompletedCount.Load())
	}
}

func TestEngine_WorkflowCrashResume(t *testing.T) {
	eng, s, _ := newWorkflowEngine(t)

	var step1Calls, step2Calls atomic.Int32
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("resume-wf", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("step-1", func(_ context.Context) error {
			step1Calls.Add(1)
			return nil
		}); err != nil {
			return err
		}
		return wf.Step("step-2", func(_ context.Context) error {
			step2Calls.Add(1)
			return nil
		})
	}))

	// First run — both steps execute.
	run, err := engine.StartWorkflow(context.Background(), eng, "resume-wf", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if step1Calls.Load() != 1 || step2Calls.Load() != 1 {
		t.Fatalf("initial: step1=%d step2=%d, want 1/1", step1Calls.Load(), step2Calls.Load())
	}

	// Simulate crash: set back to running.
	step1Calls.Store(0)
	step2Calls.Store(0)
	run.State = workflow.RunStateRunning
	run.CompletedAt = nil
	if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
		t.Fatalf("UpdateRun: %v", updateErr)
	}

	// Resume — both steps should be skipped (checkpointed).
	runner := eng.WorkflowRunner()
	if resumeErr := runner.Resume(context.Background(), run.ID); resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}

	if step1Calls.Load() != 0 {
		t.Errorf("step1 after resume = %d, want 0 (skipped)", step1Calls.Load())
	}
	if step2Calls.Load() != 0 {
		t.Errorf("step2 after resume = %d, want 0 (skipped)", step2Calls.Load())
	}
}

func TestEngine_WorkflowStepWithResult(t *testing.T) {
	eng, s, _ := newWorkflowEngine(t)

	type computeResult struct {
		Value string
		Score int
	}

	var gotResult computeResult
	var computeCalls atomic.Int32
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("result-wf", func(wf *workflow.Workflow, _ struct{}) error {
		r, err := workflow.StepWithResult[computeResult](wf, "compute", func(_ context.Context) (computeResult, error) {
			computeCalls.Add(1)
			return computeResult{Value: "gold", Score: 100}, nil
		})
		if err != nil {
			return err
		}
		gotResult = r
		return nil
	}))

	run, err := engine.StartWorkflow(context.Background(), eng, "result-wf", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}
	if gotResult.Value != "gold" || gotResult.Score != 100 {
		t.Errorf("result = %+v, want {gold, 100}", gotResult)
	}

	// Resume to verify gob round-trip.
	computeCalls.Store(0)
	gotResult = computeResult{}
	run.State = workflow.RunStateRunning
	run.CompletedAt = nil
	if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
		t.Fatalf("UpdateRun: %v", updateErr)
	}

	if resumeErr := eng.WorkflowRunner().Resume(context.Background(), run.ID); resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}
	if computeCalls.Load() != 0 {
		t.Errorf("compute calls after resume = %d, want 0", computeCalls.Load())
	}
	if gotResult.Value != "gold" || gotResult.Score != 100 {
		t.Errorf("result after resume = %+v, want {gold, 100}", gotResult)
	}
}

func TestEngine_WorkflowParallel(t *testing.T) {
	eng, _, tracker := newWorkflowEngine(t)

	var a, b, c atomic.Bool
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("parallel-wf", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Parallel("group",
			func(_ context.Context) error { a.Store(true); return nil },
			func(_ context.Context) error { b.Store(true); return nil },
			func(_ context.Context) error { c.Store(true); return nil },
		)
	}))

	run, err := engine.StartWorkflow(context.Background(), eng, "parallel-wf", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}
	if !a.Load() || !b.Load() || !c.Load() {
		t.Errorf("parallel: a=%v b=%v c=%v, want all true", a.Load(), b.Load(), c.Load())
	}
	if tracker.wfStepCompletedCount.Load() != 3 {
		t.Errorf("step completed = %d, want 3", tracker.wfStepCompletedCount.Load())
	}
}

func TestEngine_WorkflowParallelFailure(t *testing.T) {
	eng, _, tracker := newWorkflowEngine(t)

	engine.RegisterWorkflow(eng, workflow.NewWorkflow("parallel-fail-wf", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Parallel("failing",
			func(_ context.Context) error { return nil },
			func(_ context.Context) error { return errors.New("step 2 failed") },
			func(_ context.Context) error { return nil },
		)
	}))

	run, err := engine.StartWorkflow(context.Background(), eng, "parallel-fail-wf", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if run.State != workflow.RunStateFailed {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateFailed)
	}
	if !tracker.wfFailed.Load() {
		t.Error("expected OnWorkflowFailed")
	}
}

func TestEngine_WorkflowWaitForEvent(t *testing.T) {
	eng, _, _ := newWorkflowEngine(t)

	type wfResult struct {
		run *workflow.Run
		err error
	}

	var gotPayload atomic.Value // stores string
	waiting := make(chan struct{})
	resultCh := make(chan wfResult, 1)

	engine.RegisterWorkflow(eng, workflow.NewWorkflow("event-wf", func(wf *workflow.Workflow, _ struct{}) error {
		// Signal that we're about to block on WaitForEvent.
		close(waiting)
		evt, err := wf.WaitForEvent("payment.received", 5*time.Second)
		if err != nil {
			return err
		}
		if evt != nil {
			gotPayload.Store(string(evt.Payload))
		}
		return nil
	}))

	// Start workflow in a goroutine since it blocks on WaitForEvent.
	go func() {
		run, err := engine.StartWorkflow(context.Background(), eng, "event-wf", struct{}{})
		resultCh <- wfResult{run, err}
	}()

	// Wait for the workflow to enter WaitForEvent.
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for workflow to start")
	}

	// Brief pause to ensure SubscribeEvent poll loop is running.
	time.Sleep(50 * time.Millisecond)

	// Publish the event.
	bus := eng.EventBus()
	_, pubErr := bus.Publish(context.Background(), "payment.received", []byte(`{"amount":99}`), "", "")
	if pubErr != nil {
		t.Fatalf("Publish: %v", pubErr)
	}

	// Wait for workflow to complete.
	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("StartWorkflow: %v", res.err)
		}
		if res.run.State != workflow.RunStateCompleted {
			t.Errorf("state = %q, want %q, error = %q", res.run.State, workflow.RunStateCompleted, res.run.Error)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for workflow to complete")
	}

	if p, ok := gotPayload.Load().(string); !ok || p != `{"amount":99}` {
		t.Errorf("payload = %v, want %q", gotPayload.Load(), `{"amount":99}`)
	}
}

func TestEngine_WorkflowSleep(t *testing.T) {
	eng, s, _ := newWorkflowEngine(t)

	engine.RegisterWorkflow(eng, workflow.NewWorkflow("sleep-wf", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Sleep("brief", 1*time.Millisecond)
	}))

	run, err := engine.StartWorkflow(context.Background(), eng, "sleep-wf", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}

	// Verify checkpoint.
	data, chkErr := s.GetCheckpoint(context.Background(), run.ID, "sleep:brief")
	if chkErr != nil {
		t.Fatalf("GetCheckpoint: %v", chkErr)
	}
	if data == nil {
		t.Fatal("expected sleep checkpoint")
	}

	// Resume should skip sleep instantly.
	run.State = workflow.RunStateRunning
	run.CompletedAt = nil
	if updateErr := s.UpdateRun(context.Background(), run); updateErr != nil {
		t.Fatalf("UpdateRun: %v", updateErr)
	}

	start := time.Now()
	if resumeErr := eng.WorkflowRunner().Resume(context.Background(), run.ID); resumeErr != nil {
		t.Fatalf("Resume: %v", resumeErr)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("resumed sleep took %v, expected near-instant", elapsed)
	}
}

func TestEngine_WorkflowExtensionLifecycle(t *testing.T) {
	eng, _, tracker := newWorkflowEngine(t)

	engine.RegisterWorkflow(eng, workflow.NewWorkflow("lifecycle-wf", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("s1", func(_ context.Context) error { return nil }); err != nil {
			return err
		}
		return wf.Step("s2", func(_ context.Context) error { return nil })
	}))

	run, err := engine.StartWorkflow(context.Background(), eng, "lifecycle-wf", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}

	if !tracker.wfStarted.Load() {
		t.Error("expected OnWorkflowStarted")
	}
	if tracker.wfStepCompletedCount.Load() != 2 {
		t.Errorf("step completed = %d, want 2", tracker.wfStepCompletedCount.Load())
	}
	if !tracker.wfCompleted.Load() {
		t.Error("expected OnWorkflowCompleted")
	}
	if tracker.wfFailed.Load() {
		t.Error("unexpected OnWorkflowFailed")
	}
}

func TestEngine_WorkflowFailedExtensions(t *testing.T) {
	eng, _, tracker := newWorkflowEngine(t)

	engine.RegisterWorkflow(eng, workflow.NewWorkflow("fail-ext-wf", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Step("bad-step", func(_ context.Context) error {
			return errors.New("step error")
		})
	}))

	run, err := engine.StartWorkflow(context.Background(), eng, "fail-ext-wf", struct{}{})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}

	if run.State != workflow.RunStateFailed {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateFailed)
	}
	if tracker.wfStepFailedCount.Load() != 1 {
		t.Errorf("step failed events = %d, want 1", tracker.wfStepFailedCount.Load())
	}
	if !tracker.wfFailed.Load() {
		t.Error("expected OnWorkflowFailed")
	}
	if tracker.wfCompleted.Load() {
		t.Error("unexpected OnWorkflowCompleted")
	}
}

// Ensure StartWorkflow JSON round-trips the typed input.
func TestEngine_WorkflowTypedInput(t *testing.T) {
	eng, _, _ := newWorkflowEngine(t)

	type orderInput struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
	}

	var gotInput orderInput
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("typed-input-wf", func(_ *workflow.Workflow, input orderInput) error {
		gotInput = input
		return nil
	}))

	run, err := engine.StartWorkflow(context.Background(), eng, "typed-input-wf", orderInput{
		OrderID: "ord_42",
		Amount:  250,
	})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if run.State != workflow.RunStateCompleted {
		t.Errorf("state = %q, want %q", run.State, workflow.RunStateCompleted)
	}
	if gotInput.OrderID != "ord_42" {
		t.Errorf("OrderID = %q, want %q", gotInput.OrderID, "ord_42")
	}
	if gotInput.Amount != 250 {
		t.Errorf("Amount = %d, want %d", gotInput.Amount, 250)
	}

	// Verify stored input is valid JSON.
	var stored orderInput
	if jsonErr := json.Unmarshal(run.Input, &stored); jsonErr != nil {
		t.Fatalf("unmarshal stored input: %v", jsonErr)
	}
	if stored.OrderID != "ord_42" {
		t.Errorf("stored OrderID = %q, want %q", stored.OrderID, "ord_42")
	}
}

// ──────────────────────────────────────────────────
// Phase 5: Cron Scheduling & Leader Election
// ──────────────────────────────────────────────────

type cronPayload struct {
	Report string `json:"report"`
}

func TestEngine_CronFiresAndEnqueuesJob(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	// Register the job handler that the cron will enqueue.
	var processed atomic.Bool
	var gotPayload atomic.Value
	engine.Register(eng, job.NewDefinition("daily-report", func(_ context.Context, p cronPayload) error {
		gotPayload.Store(p)
		processed.Store(true)
		return nil
	}))

	// Register a cron that fires every second.
	ctx := context.Background()
	err = engine.RegisterCron(ctx, eng, &cron.Definition[cronPayload]{
		Name:     "daily-report-cron",
		Schedule: "@every 1s",
		JobName:  "daily-report",
		Payload:  cronPayload{Report: "sales"},
	})
	if err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}

	// Start the engine (starts pool + scheduler).
	if startErr := eng.Start(ctx); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	// Wait for the job handler to be invoked (cron fires → enqueues → pool processes).
	deadline := time.After(10 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for cron-enqueued job to be processed")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if stopErr := eng.Stop(stopCtx); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	// Verify payload round-tripped correctly.
	payload, ok := gotPayload.Load().(cronPayload)
	if !ok {
		t.Fatal("expected cronPayload to be stored")
	}
	if payload.Report != "sales" {
		t.Errorf("payload.Report = %q, want %q", payload.Report, "sales")
	}

	// Verify cron entry was updated.
	entries, err := s.ListCrons(context.Background())
	if err != nil {
		t.Fatalf("ListCrons: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 cron entry, got %d", len(entries))
	}
	if entries[0].LastRunAt == nil {
		t.Error("expected LastRunAt to be set after cron fired")
	}
}

func TestEngine_CronDisabledSkipped(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	var processed atomic.Bool
	engine.Register(eng, job.NewDefinition("disabled-job", func(_ context.Context, _ struct{}) error {
		processed.Store(true)
		return nil
	}))

	ctx := context.Background()

	// Register a cron entry.
	err = engine.RegisterCron(ctx, eng, &cron.Definition[struct{}]{
		Name:     "disabled-cron",
		Schedule: "@every 1s",
		JobName:  "disabled-job",
		Payload:  struct{}{},
	})
	if err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}

	// Disable it before starting.
	entries, err := s.ListCrons(ctx)
	if err != nil {
		t.Fatalf("ListCrons: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 cron entry, got %d", len(entries))
	}
	entries[0].Enabled = false
	if updateErr := s.UpdateCronEntry(ctx, entries[0]); updateErr != nil {
		t.Fatalf("UpdateCronEntry: %v", updateErr)
	}

	if startErr := eng.Start(ctx); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	// Wait a bit — disabled cron should not fire.
	time.Sleep(2 * time.Second)

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if stopErr := eng.Stop(stopCtx); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	if processed.Load() {
		t.Error("disabled cron should not have fired, but job was processed")
	}
}

func TestEngine_CronExtensionHookFires(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	tracker := &lifecycleTracker{}
	eng, err := engine.Build(d, engine.WithExtension(tracker))
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	engine.Register(eng, job.NewDefinition("hook-job", func(_ context.Context, _ struct{}) error {
		return nil
	}))

	ctx := context.Background()
	err = engine.RegisterCron(ctx, eng, &cron.Definition[struct{}]{
		Name:     "hook-cron",
		Schedule: "@every 1s",
		JobName:  "hook-job",
		Payload:  struct{}{},
	})
	if err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}

	if startErr := eng.Start(ctx); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	// Wait for the cron hook to fire.
	deadline := time.After(5 * time.Second)
	for !tracker.cronFired.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for OnCronFired hook")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if stopErr := eng.Stop(stopCtx); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	// Verify the hook received the correct entry name.
	entryName, ok := tracker.cronFiredEntry.Load().(string)
	if !ok || entryName != "hook-cron" {
		t.Errorf("OnCronFired entry = %q, want %q", entryName, "hook-cron")
	}
}

func TestEngine_RegisterCronIdempotent(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	engine.Register(eng, job.NewDefinition("idempotent-job", func(_ context.Context, _ struct{}) error {
		return nil
	}))

	ctx := context.Background()
	def := &cron.Definition[struct{}]{
		Name:     "idempotent-cron",
		Schedule: "@every 1s",
		JobName:  "idempotent-job",
		Payload:  struct{}{},
	}

	// First registration.
	if regErr := engine.RegisterCron(ctx, eng, def); regErr != nil {
		t.Fatalf("first RegisterCron: %v", regErr)
	}

	// Second registration should be idempotent.
	if regErr := engine.RegisterCron(ctx, eng, def); regErr != nil {
		t.Fatalf("second RegisterCron should be idempotent: %v", regErr)
	}

	// Verify only one entry exists.
	entries, err := s.ListCrons(ctx)
	if err != nil {
		t.Fatalf("ListCrons: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 cron entry after idempotent registration, got %d", len(entries))
	}
}

func TestEngine_RegisterCronInvalidSchedule(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	err = engine.RegisterCron(context.Background(), eng, &cron.Definition[struct{}]{
		Name:     "bad-cron",
		Schedule: "not-a-valid-schedule",
		JobName:  "noop",
		Payload:  struct{}{},
	})
	if err == nil {
		t.Fatal("expected error for invalid cron schedule")
	}
}
