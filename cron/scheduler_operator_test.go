package cron_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// startLeader starts a scheduler on s that already holds leadership, with
// a 20ms tick and a cron cache that never refreshes on its own, so every
// tick runs against what the scheduler listed at startup unless something
// invalidates it. That is the view a leader has of an entry an operator
// changed through another node. The scheduler stops when the test ends.
func startLeader(t *testing.T, s *memory.Store, enqueue cron.EnqueueFunc, logger log.Logger) {
	t.Helper()
	ctx := context.Background()
	workerID := id.NewWorkerID()

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
	if ok, err := s.AcquireLeadership(ctx, workerID, 30*time.Second); err != nil || !ok {
		t.Fatalf("AcquireLeadership: ok=%v err=%v", ok, err)
	}

	sched := cron.NewScheduler(
		s, s, enqueue, &stubEmitter{}, workerID, logger,
		cron.WithTickInterval(20*time.Millisecond),
		cron.WithLeaderTTL(10*time.Second),
		cron.WithCronRefreshInterval(time.Hour),
	)
	if err := sched.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sched.Stop(context.Background()) })
}

func waitForFirstFire(t *testing.T, spy *enqueueSpy) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for spy.Count() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the entry to fire")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestScheduler_StaleCacheDoesNotFireDisabledEntry is the regression for
// a disabled cron coming back. The entry is disabled in the store without
// touching this scheduler's cache, as an operator on another node would.
// The scheduler then runs through the entry's next fire time: it must not
// enqueue, and its post-fire write must not put enabled back.
func TestScheduler_StaleCacheDoesNotFireDisabledEntry(t *testing.T) {
	s := memory.New()
	spy := &enqueueSpy{}
	entry := registerDueEntry(t, s, "nightly", "report") // @every 1s, due now
	startLeader(t, s, spy.Fn(), nil)

	waitForFirstFire(t, spy)

	if err := s.SetCronEnabled(context.Background(), entry.ID, false, nil); err != nil {
		t.Fatalf("SetCronEnabled(false): %v", err)
	}
	atDisable := spy.Count()

	// The entry's next fire is at most a second after the first one.
	time.Sleep(1500 * time.Millisecond)

	if got := spy.Count(); got != atDisable {
		t.Errorf("enqueues after disable = %d, want 0", got-atDisable)
	}
	got, err := s.GetCron(context.Background(), entry.ID)
	if err != nil {
		t.Fatalf("GetCron: %v", err)
	}
	if got.Enabled {
		t.Error("entry is enabled again after the scheduler ran past its fire time")
	}
}

// TestScheduler_DisableDuringFireSticks lands the operator's disable
// between the scheduler's read of the entry and its post-fire write, the
// window the whole-row UpdateCronEntry write-back used to reopen.
func TestScheduler_DisableDuringFireSticks(t *testing.T) {
	s := memory.New()
	entry := registerDueEntry(t, s, "nightly", "report")

	spy := &enqueueSpy{}
	inner := spy.Fn()
	enqueue := func(ctx context.Context, name string, payload []byte, opts ...job.Option) (id.JobID, error) {
		jobID, err := inner(ctx, name, payload, opts...)
		if disErr := s.SetCronEnabled(ctx, entry.ID, false, nil); disErr != nil {
			t.Errorf("SetCronEnabled(false): %v", disErr)
		}
		return jobID, err
	}
	startLeader(t, s, enqueue, nil)

	waitForFirstFire(t, spy)
	time.Sleep(1500 * time.Millisecond)

	if got := spy.Count(); got != 1 {
		t.Errorf("enqueues = %d, want exactly the one that raced the disable", got)
	}
	got, err := s.GetCron(context.Background(), entry.ID)
	if err != nil {
		t.Fatalf("GetCron: %v", err)
	}
	if got.Enabled {
		t.Error("the post-fire write put enabled back")
	}
	if got.LastRunAt == nil {
		t.Error("LastRunAt not recorded for the fire")
	}
	if got.NextRunAt == nil || !got.NextRunAt.After(*entry.NextRunAt) {
		t.Errorf("NextRunAt = %v, want it moved past the fire", got.NextRunAt)
	}
}

// TestScheduler_EntryDeletedDuringFire deletes the entry between the
// enqueue and the post-fire writes. The scheduler logs that at debug and
// carries on; it is not an error.
func TestScheduler_EntryDeletedDuringFire(t *testing.T) {
	s := memory.New()
	entry := registerDueEntry(t, s, "nightly", "report")

	spy := &enqueueSpy{}
	inner := spy.Fn()
	var once sync.Once
	enqueue := func(ctx context.Context, name string, payload []byte, opts ...job.Option) (id.JobID, error) {
		jobID, err := inner(ctx, name, payload, opts...)
		once.Do(func() {
			if delErr := s.DeleteCron(ctx, entry.ID); delErr != nil {
				t.Errorf("DeleteCron: %v", delErr)
			}
		})
		return jobID, err
	}
	logger := log.NewTestLogger()
	startLeader(t, s, enqueue, logger)

	waitForFirstFire(t, spy)
	time.Sleep(200 * time.Millisecond)

	if got := spy.Count(); got != 1 {
		t.Errorf("enqueues = %d, want 1", got)
	}
	if _, err := s.GetCron(context.Background(), entry.ID); !errors.Is(err, dispatch.ErrCronNotFound) {
		t.Fatalf("GetCron after delete: %v, want ErrCronNotFound", err)
	}
	tl, ok := logger.(*log.TestLogger)
	if !ok {
		t.Fatalf("logger is %T, want *log.TestLogger", logger)
	}
	if n := tl.CountLogs("ERROR"); n != 0 {
		t.Errorf("scheduler logged %d errors for an entry deleted mid-fire: %v", n, tl.GetLogsByLevel("ERROR"))
	}
	if n := tl.CountLogs("DEBUG"); n == 0 {
		t.Error("no debug log for the entry deleted mid-fire")
	}
}

// TestScheduler_FiresOncePerDueTime checks that the scheduler does not
// fire again on the next tick while its cache still holds the old
// NextRunAt: one due time, one enqueue.
func TestScheduler_FiresOncePerDueTime(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	past := time.Now().UTC().Add(-time.Second)
	entry := &cron.Entry{
		Entity:    dispatch.NewEntity(),
		ID:        id.NewCronID(),
		Name:      "hourly",
		Schedule:  "@every 1h",
		JobName:   "report",
		NextRunAt: &past,
		Enabled:   true,
	}
	if err := s.RegisterCron(ctx, entry); err != nil {
		t.Fatalf("RegisterCron: %v", err)
	}
	spy := &enqueueSpy{}
	startLeader(t, s, spy.Fn(), nil)

	waitForFirstFire(t, spy)
	time.Sleep(300 * time.Millisecond) // ~15 more ticks

	if got := spy.Count(); got != 1 {
		t.Errorf("enqueues = %d, want 1", got)
	}
}
