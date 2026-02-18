package cron_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// stubEmitter records EmitCronFired calls.
type stubEmitter struct {
	mu    sync.Mutex
	calls []cronFiredCall
}

type cronFiredCall struct {
	EntryName string
	JobID     id.JobID
}

func (e *stubEmitter) EmitCronFired(_ context.Context, entryName string, jobID id.JobID) {
	e.mu.Lock()
	e.calls = append(e.calls, cronFiredCall{EntryName: entryName, JobID: jobID})
	e.mu.Unlock()
}

func (e *stubEmitter) getCalls() []cronFiredCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]cronFiredCall, len(e.calls))
	copy(out, e.calls)
	return out
}

// enqueueSpy tracks enqueue calls with thread safety.
type enqueueSpy struct {
	mu    sync.Mutex
	calls []enqueueCall
}

type enqueueCall struct {
	Name    string
	Payload []byte
}

func (e *enqueueSpy) Fn() cron.EnqueueFunc {
	return func(_ context.Context, name string, payload []byte, _ ...job.Option) (id.JobID, error) {
		e.mu.Lock()
		e.calls = append(e.calls, enqueueCall{Name: name, Payload: payload})
		e.mu.Unlock()
		return id.NewJobID(), nil
	}
}

func (e *enqueueSpy) Count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func (e *enqueueSpy) Names() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.calls))
	for i, c := range e.calls {
		out[i] = c.Name
	}
	return out
}

func registerDueEntry(t *testing.T, s *memory.Store, name, jobName string) *cron.Entry {
	t.Helper()

	past := time.Now().UTC().Add(-1 * time.Second)
	entry := &cron.Entry{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewCronID(),
		Name:      name,
		Schedule:  "@every 1s",
		JobName:   jobName,
		Payload:   []byte(`{}`),
		NextRunAt: &past,
		Enabled:   true,
	}

	if err := s.RegisterCron(context.Background(), entry); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}
	return entry
}

// newTestScheduler creates a working scheduler with leadership acquired.
func newTestScheduler(t *testing.T) (
	*cron.Scheduler,
	*memory.Store,
	*stubEmitter,
	*enqueueSpy,
) {
	t.Helper()

	s := memory.New()
	emitter := &stubEmitter{}
	workerID := id.NewWorkerID()
	spy := &enqueueSpy{}

	ctx := context.Background()

	// Register this worker and acquire leadership.
	w := &cluster.Worker{
		ID:        workerID,
		Hostname:  "test-host",
		State:     cluster.WorkerActive,
		LastSeen:  time.Now().UTC(),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	acquired, err := s.AcquireLeadership(ctx, workerID, 30*time.Second)
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	if !acquired {
		t.Fatal("failed to acquire leadership")
	}

	sched := cron.NewScheduler(
		s, s, spy.Fn(), emitter, workerID, nil,
		cron.WithTickInterval(50*time.Millisecond),
		cron.WithLeaderTTL(10*time.Second),
	)

	return sched, s, emitter, spy
}

// ──────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────

func TestScheduler_FiresOnSchedule(t *testing.T) {
	sched, s, emitter, spy := newTestScheduler(t)

	registerDueEntry(t, s, "every-second", "send-email")

	if err := sched.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for at least one fire.
	deadline := time.After(3 * time.Second)
	for spy.Count() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for cron to fire")
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	if err := sched.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	names := spy.Names()
	if len(names) == 0 {
		t.Fatal("expected at least one enqueue call")
	}
	if names[0] != "send-email" {
		t.Errorf("enqueued job name = %q, want %q", names[0], "send-email")
	}

	// Verify emitter was called.
	calls := emitter.getCalls()
	if len(calls) == 0 {
		t.Error("expected at least one EmitCronFired call")
	}
	if len(calls) > 0 && calls[0].EntryName != "every-second" {
		t.Errorf("emitter entry name = %q, want %q", calls[0].EntryName, "every-second")
	}
}

func TestScheduler_SkipsDisabled(t *testing.T) {
	sched, s, _, spy := newTestScheduler(t)

	entry := registerDueEntry(t, s, "disabled-cron", "noop-job")

	// Disable the entry.
	entry.Enabled = false
	if err := s.UpdateCronEntry(context.Background(), entry); err != nil {
		t.Fatalf("UpdateCronEntry: %v", err)
	}

	if err := sched.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait a bit — should NOT fire.
	time.Sleep(300 * time.Millisecond)

	if err := sched.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if spy.Count() != 0 {
		t.Errorf("expected 0 enqueue calls for disabled entry, got %d", spy.Count())
	}
}

func TestScheduler_NonLeaderSkips(t *testing.T) {
	s := memory.New()
	emitter := &stubEmitter{}
	spy := &enqueueSpy{}

	nonLeaderID := id.NewWorkerID()
	leaderID := id.NewWorkerID()

	ctx := context.Background()

	// Register both workers, but make leaderID the leader.
	for _, wid := range []id.WorkerID{leaderID, nonLeaderID} {
		w := &cluster.Worker{
			ID:        wid,
			Hostname:  "test",
			State:     cluster.WorkerActive,
			LastSeen:  time.Now().UTC(),
			CreatedAt: time.Now().UTC(),
		}
		if err := s.RegisterWorker(ctx, w); err != nil {
			t.Fatalf("RegisterWorker: %v", err)
		}
	}
	acquired, err := s.AcquireLeadership(ctx, leaderID, 30*time.Second)
	if err != nil || !acquired {
		t.Fatalf("AcquireLeadership: acquired=%v err=%v", acquired, err)
	}

	// Create scheduler for the non-leader worker.
	sched := cron.NewScheduler(
		s, s, spy.Fn(), emitter, nonLeaderID, nil,
		cron.WithTickInterval(50*time.Millisecond),
		cron.WithLeaderTTL(10*time.Second),
	)

	registerDueEntry(t, s, "leader-only", "test-job")

	if startErr := sched.Start(context.Background()); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	// Wait a bit — non-leader should not fire.
	time.Sleep(300 * time.Millisecond)

	if stopErr := sched.Stop(context.Background()); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	if spy.Count() != 0 {
		t.Errorf("non-leader should not fire crons, got %d calls", spy.Count())
	}
}

func TestScheduler_ComputesNextRunAt(t *testing.T) {
	sched, s, _, spy := newTestScheduler(t)

	entry := registerDueEntry(t, s, "update-next", "compute-job")
	entryID := entry.ID

	if err := sched.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for at least one fire.
	deadline := time.After(3 * time.Second)
	for spy.Count() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for cron to fire")
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	if err := sched.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Verify NextRunAt was updated to a future time.
	updated, err := s.GetCron(context.Background(), entryID)
	if err != nil {
		t.Fatalf("GetCron: %v", err)
	}
	if updated.NextRunAt == nil {
		t.Fatal("expected NextRunAt to be set")
	}
	// NextRunAt should be in the future (or very recent past due to timing).
	if updated.NextRunAt.Before(time.Now().UTC().Add(-2 * time.Second)) {
		t.Errorf("NextRunAt = %v, expected recent/future time", updated.NextRunAt)
	}

	// Verify LastRunAt was set.
	if updated.LastRunAt == nil {
		t.Error("expected LastRunAt to be set after firing")
	}
}

func TestScheduler_LockPreventsDoubleFire(t *testing.T) {
	s := memory.New()
	emitter := &stubEmitter{}
	spy := &enqueueSpy{}
	workerID := id.NewWorkerID()

	ctx := context.Background()

	// Register worker and acquire leadership.
	w := &cluster.Worker{
		ID:        workerID,
		Hostname:  "test",
		State:     cluster.WorkerActive,
		LastSeen:  time.Now().UTC(),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	acquired, err := s.AcquireLeadership(ctx, workerID, 30*time.Second)
	if err != nil || !acquired {
		t.Fatalf("AcquireLeadership: acquired=%v err=%v", acquired, err)
	}

	entry := registerDueEntry(t, s, "locked-entry", "locked-job")

	// Pre-acquire the lock for this entry with a different worker.
	otherWorkerID := id.NewWorkerID()
	locked, lockErr := s.AcquireCronLock(ctx, entry.ID, otherWorkerID, 30*time.Second)
	if lockErr != nil {
		t.Fatalf("AcquireCronLock: %v", lockErr)
	}
	if !locked {
		t.Fatal("expected to acquire cron lock")
	}

	sched := cron.NewScheduler(
		s, s, spy.Fn(), emitter, workerID, nil,
		cron.WithTickInterval(50*time.Millisecond),
		cron.WithLeaderTTL(10*time.Second),
	)

	if startErr := sched.Start(ctx); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	// Wait — scheduler should try but fail to acquire the lock.
	time.Sleep(300 * time.Millisecond)

	if stopErr := sched.Stop(ctx); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	if spy.Count() != 0 {
		t.Errorf("expected 0 fires with pre-locked entry, got %d", spy.Count())
	}
}

func TestParseSchedule(t *testing.T) {
	// Descriptor format.
	sched, err := cron.ParseSchedule("@every 30s")
	if err != nil {
		t.Fatalf("ParseSchedule(@every 30s): %v", err)
	}
	now := time.Now().UTC()
	next := sched.Next(now)
	if !next.After(now) {
		t.Errorf("Next(%v) = %v, expected future time", now, next)
	}

	// Standard 5-field expression.
	sched2, err := cron.ParseSchedule("*/5 * * * *")
	if err != nil {
		t.Fatalf("ParseSchedule(*/5 * * * *): %v", err)
	}
	next2 := sched2.Next(now)
	if !next2.After(now) {
		t.Errorf("Next(%v) = %v, expected future time", now, next2)
	}

	// Invalid expression.
	_, err = cron.ParseSchedule("not-a-cron")
	if err == nil {
		t.Error("expected error for invalid cron expression")
	}
}
