package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
)

// pushFailedNamed is pushFailed for a job with its own name, so a test
// can tell entries apart by the job they replay into.
func pushFailedNamed(t *testing.T, eng *engine.Engine, s *memory.Store, name string) *dlq.Entry {
	t.Helper()
	ctx := context.Background()

	j, err := eng.EnqueueRaw(ctx, name, []byte(`{"n":1}`))
	if err != nil {
		t.Fatalf("EnqueueRaw: %v", err)
	}
	j.State = job.StateFailed
	if updErr := s.UpdateJob(ctx, j); updErr != nil {
		t.Fatalf("UpdateJob: %v", updErr)
	}
	if pushErr := eng.DLQService().Push(ctx, j, errors.New("boom")); pushErr != nil {
		t.Fatalf("Push: %v", pushErr)
	}

	entry, err := s.GetDLQByJobID(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetDLQByJobID: %v", err)
	}

	return entry
}

func countPending(t *testing.T, s *memory.Store) int64 {
	t.Helper()

	n, err := s.CountJobs(context.Background(), job.CountOpts{State: job.StatePending})
	if err != nil {
		t.Fatalf("CountJobs: %v", err)
	}

	return n
}

func TestReplayDLQ_EnqueuesAndClaims(t *testing.T) {
	s := memory.New()
	eng, rec := newJobOpsEngine(t, s)
	failed, entryID := pushFailed(t, eng, s)

	j, err := eng.ReplayDLQ(ext.WithActor(context.Background(), "dave"), entryID)
	if err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	if j.ID == failed.ID || j.Name != failed.Name || string(j.Payload) != string(failed.Payload) {
		t.Errorf("replayed job = %s %q %s, want a new ID for %q with the same payload",
			j.ID, j.Name, j.Payload, failed.Name)
	}

	stored, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.State != job.StatePending {
		t.Errorf("stored state = %s, want pending", stored.State)
	}

	entry, err := s.GetDLQ(context.Background(), entryID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if entry.ReplayedJobID == nil || *entry.ReplayedJobID != j.ID {
		t.Errorf("ReplayedJobID = %v, want %s", entry.ReplayedJobID, j.ID)
	}

	actions, _, _ := rec.snapshot()
	if len(actions) != 1 {
		t.Fatalf("got %d operator actions, want 1", len(actions))
	}
	a := actions[0]
	if a.Kind != ext.ActionDLQReplayed || a.DLQID != entryID || a.JobID != failed.ID ||
		a.NewJobID != j.ID || a.Actor != "dave" {
		t.Errorf("action = %+v, want dlq.replayed of %s from %s into %s by dave", a, entryID, failed.ID, j.ID)
	}
}

func TestReplayDLQ_ConcurrentReplaysMakeOneJob(t *testing.T) {
	s := memory.New()
	eng, rec := newJobOpsEngine(t, s)
	_, entryID := pushFailed(t, eng, s)

	const callers = 8
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		won       int
		conflicts int
		other     []error
	)
	for range callers {
		wg.Go(func() {
			_, err := eng.ReplayDLQ(context.Background(), entryID)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won++
			case errors.Is(err, dispatch.ErrDLQAlreadyReplayed):
				conflicts++
			default:
				other = append(other, err)
			}
		})
	}
	wg.Wait()

	if won != 1 || conflicts != callers-1 || len(other) != 0 {
		t.Fatalf("won %d, conflicts %d, other %v; want 1, %d, none", won, conflicts, other, callers-1)
	}
	if n := countPending(t, s); n != 1 {
		t.Errorf("pending jobs = %d, want exactly 1 new job", n)
	}
	if actions, _, _ := rec.snapshot(); len(actions) != 1 {
		t.Errorf("got %d operator actions, want 1", len(actions))
	}
}

func TestReplayDLQ_ThenRetryIsRefused(t *testing.T) {
	s := memory.New()
	eng, _ := newJobOpsEngine(t, s)
	failed, entryID := pushFailed(t, eng, s)

	if _, err := eng.ReplayDLQ(context.Background(), entryID); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}

	_, err := eng.RetryJob(context.Background(), failed.ID)
	if !errors.Is(err, dispatch.ErrDLQAlreadyReplayed) {
		t.Fatalf("RetryJob after a replay: error = %v, want ErrDLQAlreadyReplayed", err)
	}

	stored, err := s.GetJob(context.Background(), failed.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.State != job.StateFailed {
		t.Errorf("failed job state = %s, want it left failed", stored.State)
	}
}

func TestRetryJob_ThenReplayIsRefused(t *testing.T) {
	s := memory.New()
	eng, _ := newJobOpsEngine(t, s)
	failed, entryID := pushFailed(t, eng, s)

	if _, err := eng.RetryJob(context.Background(), failed.ID); err != nil {
		t.Fatalf("RetryJob: %v", err)
	}

	_, err := eng.ReplayDLQ(context.Background(), entryID)
	if !errors.Is(err, dispatch.ErrDLQAlreadyReplayed) {
		t.Fatalf("ReplayDLQ after a retry: error = %v, want ErrDLQAlreadyReplayed", err)
	}
	if n := countPending(t, s); n != 1 {
		t.Errorf("pending jobs = %d, want only the retried job", n)
	}
}

// failEnqueueStore fails EnqueueJob while fail is set, and for any job
// named poison once poison is set.
type failEnqueueStore struct {
	*memory.Store
	mu     sync.Mutex
	fail   bool
	poison string
}

var errOpsEnqueue = errors.New("enqueue refused by test")

func (f *failEnqueueStore) setPoison(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.poison = name
}

func (f *failEnqueueStore) setFail(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = v
}

func (f *failEnqueueStore) EnqueueJob(ctx context.Context, j *job.Job) error {
	f.mu.Lock()
	fail := f.fail || (f.poison != "" && j.Name == f.poison)
	f.mu.Unlock()
	if fail {
		return errOpsEnqueue
	}
	return f.Store.EnqueueJob(ctx, j)
}

func TestReplayDLQ_FailedEnqueueLeavesTheEntryReplayable(t *testing.T) {
	s := &failEnqueueStore{Store: memory.New()}
	eng, rec := newJobOpsEngine(t, s)
	_, entryID := pushFailed(t, eng, s.Store)

	s.setFail(true)
	if _, err := eng.ReplayDLQ(context.Background(), entryID); !errors.Is(err, errOpsEnqueue) {
		t.Fatalf("ReplayDLQ error = %v, want the store's enqueue error", err)
	}

	entry, err := s.GetDLQ(context.Background(), entryID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if entry.ReplayedAt != nil || entry.ReplayedJobID != nil {
		t.Errorf("entry after a failed replay: replayed_at %v replayed_job_id %v; want released",
			entry.ReplayedAt, entry.ReplayedJobID)
	}
	if actions, _, _ := rec.snapshot(); len(actions) != 0 {
		t.Errorf("a failed replay emitted %d actions, want 0", len(actions))
	}

	s.setFail(false)
	if _, err := eng.ReplayDLQ(context.Background(), entryID); err != nil {
		t.Fatalf("ReplayDLQ after the store recovered: %v", err)
	}
}

// TestReplayDLQ_UnschedulableIsRefused replays a job bigger than the
// declared fleet. The check runs on the carried Resources, and runs for
// DLQService().Replay too, because both enqueue through the engine.
func TestReplayDLQ_UnschedulableIsRefused(t *testing.T) {
	s := memory.New()
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}
	eng, err := engine.Build(d, engine.WithWorkerCapacity(resource.CPUs(2)))
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	j, err := eng.EnqueueRaw(context.Background(), "big-job", []byte(`{}`))
	if err != nil {
		t.Fatalf("EnqueueRaw: %v", err)
	}
	// Sized when the fleet was bigger than it is now.
	j.State = job.StateFailed
	j.Resources = resource.CPUs(8)
	if updErr := s.UpdateJob(context.Background(), j); updErr != nil {
		t.Fatalf("UpdateJob: %v", updErr)
	}
	if pushErr := eng.DLQService().Push(context.Background(), j, errors.New("boom")); pushErr != nil {
		t.Fatalf("Push: %v", pushErr)
	}
	entry, err := s.GetDLQByJobID(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetDLQByJobID: %v", err)
	}

	if _, replayErr := eng.ReplayDLQ(context.Background(), entry.ID); !errors.Is(replayErr, resource.ErrUnschedulable) {
		t.Fatalf("ReplayDLQ error = %v, want ErrUnschedulable", replayErr)
	}
	if _, replayErr := eng.DLQService().Replay(context.Background(), entry.ID); !errors.Is(replayErr, resource.ErrUnschedulable) {
		t.Fatalf("DLQService().Replay error = %v, want ErrUnschedulable", replayErr)
	}

	got, err := s.GetDLQ(context.Background(), entry.ID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if got.ReplayedAt != nil {
		t.Errorf("entry was left claimed after an unschedulable replay")
	}
}

// stealOnListStore claims the entries in steal right after listing them,
// as if another operator replayed them between ReplayAllDLQ's read and
// its own claim.
type stealOnListStore struct {
	*failEnqueueStore
	steal map[id.DLQID]bool
}

func (s *stealOnListStore) ListDLQPage(ctx context.Context, opts dlq.PageOpts) (dlq.Page, error) {
	page, err := s.Store.ListDLQPage(ctx, opts)
	if err != nil {
		return page, err
	}
	for _, e := range page.Entries {
		if s.steal[e.ID] {
			if claimErr := s.ClaimReplay(ctx, e.ID, id.NewJobID()); claimErr != nil {
				return page, claimErr
			}
		}
	}
	return page, nil
}

func TestReplayAllDLQ_Counts(t *testing.T) {
	s := &stealOnListStore{
		failEnqueueStore: &failEnqueueStore{Store: memory.New()},
		steal:            map[id.DLQID]bool{},
	}
	eng, rec := newJobOpsEngine(t, s)

	done := pushFailedNamed(t, eng, s.Store, "ops-done")
	stolen := pushFailedNamed(t, eng, s.Store, "ops-stolen")
	poison := pushFailedNamed(t, eng, s.Store, "ops-poison")
	for range 3 {
		pushFailedNamed(t, eng, s.Store, "ops-fine")
	}
	s.steal[stolen.ID] = true
	s.setPoison("ops-poison")

	// Replayed before the sweep, so the sweep never lists it.
	if _, err := eng.ReplayDLQ(context.Background(), done.ID); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}

	res, err := eng.ReplayAllDLQ(ext.WithActor(context.Background(), "erin"), engine.ReplayAllOpts{})
	if err != nil {
		t.Fatalf("ReplayAllDLQ: %v", err)
	}
	if res.Replayed != 3 || res.Conflicts != 1 || res.Failed != 1 || len(res.Errors) != 1 {
		t.Fatalf("result = %+v, want 3 replayed, 1 conflict, 1 failed with one message", res)
	}

	got, err := s.GetDLQ(context.Background(), poison.ID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if got.ReplayedAt != nil {
		t.Errorf("the entry whose replay failed was left claimed")
	}

	// One pending job from the earlier replay, three from the sweep.
	if n := countPending(t, s.Store); n != 4 {
		t.Errorf("pending jobs = %d, want 4", n)
	}

	actions, _, _ := rec.snapshot()
	if len(actions) != 2 {
		t.Fatalf("got %d operator actions, want the earlier replay's and one for the sweep", len(actions))
	}
	a := actions[1]
	if a.Kind != ext.ActionDLQReplayed || a.Count != 3 || !a.DLQID.IsNil() || a.Actor != "erin" {
		t.Errorf("sweep action = %+v, want dlq.replayed with count 3, no entry, by erin", a)
	}
}

func TestReplayAllDLQ_LimitTakesTheNewest(t *testing.T) {
	s := memory.New()
	eng, _ := newJobOpsEngine(t, s)

	entries := make([]*dlq.Entry, 0, 4)
	for range 4 {
		entries = append(entries, pushFailedNamed(t, eng, s, "ops-fine"))
		// IDs sort by creation time; a millisecond apart keeps the
		// order this test reads as "newest" unambiguous.
		time.Sleep(2 * time.Millisecond)
	}

	res, err := eng.ReplayAllDLQ(context.Background(), engine.ReplayAllOpts{Limit: 2})
	if err != nil {
		t.Fatalf("ReplayAllDLQ: %v", err)
	}
	if res.Replayed != 2 {
		t.Fatalf("Replayed = %d, want 2", res.Replayed)
	}

	for i, e := range entries {
		got, err := s.GetDLQ(context.Background(), e.ID)
		if err != nil {
			t.Fatalf("GetDLQ: %v", err)
		}
		wantReplayed := i >= 2 // the two pushed last have the highest IDs
		if (got.ReplayedAt != nil) != wantReplayed {
			t.Errorf("entry %d replayed = %v, want %v", i, got.ReplayedAt != nil, wantReplayed)
		}
	}
}

func TestReplayAllDLQ_FollowsCursorAcrossCompletePages(t *testing.T) {
	s := memory.New()
	eng, rec := newJobOpsEngine(t, s)
	entries := make([]*dlq.Entry, 0, 107)
	for range 107 {
		entries = append(entries, pushFailedNamed(t, eng, s, "ops-fine"))
	}

	res, err := eng.ReplayAllDLQ(context.Background(), engine.ReplayAllOpts{Limit: 105})
	if err != nil {
		t.Fatalf("ReplayAllDLQ: %v", err)
	}
	if res.Replayed != 105 || res.Conflicts != 0 || res.Failed != 0 {
		t.Fatalf("result = %+v, want 105 replayed across two pages", res)
	}
	claimed := 0
	for _, entry := range entries {
		got, getErr := s.GetDLQ(context.Background(), entry.ID)
		if getErr != nil {
			t.Fatalf("GetDLQ: %v", getErr)
		}
		if got.ReplayedJobID != nil {
			if _, getErr = s.GetJob(context.Background(), *got.ReplayedJobID); getErr != nil {
				t.Fatalf("replayed job for %s: %v", entry.ID, getErr)
			}
			claimed++
		}
	}
	if claimed != 105 {
		t.Errorf("claimed entries = %d, want 105", claimed)
	}
	actions, _, _ := rec.snapshot()
	if len(actions) != 1 || actions[0].Count != 105 {
		t.Errorf("actions = %+v, want one bulk action for 105 entries", actions)
	}
}

func TestDeleteDLQ(t *testing.T) {
	s := memory.New()
	eng, rec := newJobOpsEngine(t, s)
	_, entryID := pushFailed(t, eng, s)

	if err := eng.DeleteDLQ(ext.WithActor(context.Background(), "frank"), entryID); err != nil {
		t.Fatalf("DeleteDLQ: %v", err)
	}
	if _, err := s.GetDLQ(context.Background(), entryID); !errors.Is(err, dispatch.ErrDLQNotFound) {
		t.Errorf("GetDLQ after delete: error = %v, want ErrDLQNotFound", err)
	}

	if err := eng.DeleteDLQ(context.Background(), entryID); !errors.Is(err, dispatch.ErrDLQNotFound) {
		t.Errorf("second DeleteDLQ: error = %v, want ErrDLQNotFound", err)
	}

	actions, _, _ := rec.snapshot()
	if len(actions) != 1 || actions[0].Kind != ext.ActionDLQDeleted || actions[0].DLQID != entryID ||
		actions[0].Actor != "frank" {
		t.Errorf("actions = %+v, want one dlq.deleted of %s by frank", actions, entryID)
	}
}

func TestPurgeDLQ(t *testing.T) {
	s := memory.New()
	eng, rec := newJobOpsEngine(t, s)
	ctx := context.Background()

	now := time.Now().UTC()
	for _, failedAt := range []time.Time{now.Add(-72 * time.Hour), now.Add(-48 * time.Hour), now} {
		e := &dlq.Entry{
			ID:        id.NewDLQID(),
			JobID:     id.NewJobID(),
			JobName:   "ops-old",
			Queue:     "default",
			Error:     "boom",
			FailedAt:  failedAt,
			CreatedAt: failedAt,
		}
		if err := s.PushDLQ(ctx, e); err != nil {
			t.Fatalf("PushDLQ: %v", err)
		}
	}
	cutoff := now.Add(-24 * time.Hour)

	if _, err := eng.PurgeDLQ(ctx, time.Time{}); err == nil {
		t.Fatal("PurgeDLQ with a zero before: want an error")
	}
	if _, err := eng.CountDLQPurge(ctx, time.Time{}); err == nil {
		t.Fatal("CountDLQPurge with a zero before: want an error")
	}

	would, err := eng.CountDLQPurge(ctx, cutoff)
	if err != nil {
		t.Fatalf("CountDLQPurge: %v", err)
	}
	if would != 2 {
		t.Errorf("CountDLQPurge = %d, want 2", would)
	}
	if actions, _, _ := rec.snapshot(); len(actions) != 0 {
		t.Errorf("the dry run and the refusals emitted %d actions, want 0", len(actions))
	}

	purged, err := eng.PurgeDLQ(ext.WithActor(ctx, "gina"), cutoff)
	if err != nil {
		t.Fatalf("PurgeDLQ: %v", err)
	}
	if purged != would {
		t.Errorf("PurgeDLQ = %d, want the dry run's %d", purged, would)
	}

	left, err := s.CountDLQ(ctx)
	if err != nil {
		t.Fatalf("CountDLQ: %v", err)
	}
	if left != 1 {
		t.Errorf("entries left = %d, want 1", left)
	}

	actions, _, _ := rec.snapshot()
	if len(actions) != 1 || actions[0].Kind != ext.ActionDLQPurged || actions[0].Count != 2 ||
		actions[0].Actor != "gina" {
		t.Errorf("actions = %+v, want one dlq.purged with count 2 by gina", actions)
	}
}
