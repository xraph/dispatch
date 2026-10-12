package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// jobOpsRecorder records what the operator methods report: every
// OperatorAction, and which jobs reached OnJobCancelled and OnJobFailed.
type jobOpsRecorder struct {
	mu        sync.Mutex
	actions   []ext.Action
	cancelled []*job.Job
	failed    []id.JobID
}

func (r *jobOpsRecorder) Name() string { return "job-ops-recorder" }

func (r *jobOpsRecorder) OnOperatorAction(_ context.Context, a ext.Action) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, a)
	return nil
}

func (r *jobOpsRecorder) OnJobCancelled(_ context.Context, j *job.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancelled = append(r.cancelled, j)
	return nil
}

func (r *jobOpsRecorder) OnJobFailed(_ context.Context, j *job.Job, _ error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed = append(r.failed, j.ID)
	return nil
}

func (r *jobOpsRecorder) snapshot() (actions []ext.Action, cancelled []*job.Job, failed []id.JobID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ext.Action(nil), r.actions...),
		append([]*job.Job(nil), r.cancelled...),
		append([]id.JobID(nil), r.failed...)
}

// newJobOpsEngine builds an engine over st with the recorder registered
// and the pool not started.
func newJobOpsEngine(t *testing.T, st dispatch.Storer, opts ...dispatch.Option) (*engine.Engine, *jobOpsRecorder) {
	t.Helper()

	all := append([]dispatch.Option{dispatch.WithStore(st)}, opts...)
	d, err := dispatch.New(all...)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	rec := &jobOpsRecorder{}
	eng, err := engine.Build(d, engine.WithExtension(rec))
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	return eng, rec
}

// putJobInState enqueues a job and moves it straight to state, without
// a worker.
func putJobInState(t *testing.T, eng *engine.Engine, s *memory.Store, state job.State) *job.Job {
	t.Helper()

	j, err := eng.EnqueueRaw(context.Background(), "ops-job", []byte(`{}`))
	if err != nil {
		t.Fatalf("EnqueueRaw: %v", err)
	}
	if state == job.StatePending {
		return j
	}

	j.State = state
	j.LastError = "boom"
	j.RetryCount = 3
	if err := s.UpdateJob(context.Background(), j); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	return j
}

// pushFailed puts j in failed and gives it a dead letter entry, the way
// the runner leaves a job that ran out of retries.
func pushFailed(t *testing.T, eng *engine.Engine, s *memory.Store) (*job.Job, id.DLQID) {
	t.Helper()

	j := putJobInState(t, eng, s, job.StateFailed)
	if err := eng.DLQService().Push(context.Background(), j, errors.New("boom")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	entry, err := s.GetDLQByJobID(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetDLQByJobID: %v", err)
	}

	return j, entry.ID
}

func TestCancelJob_PendingAndRetrying(t *testing.T) {
	for _, state := range []job.State{job.StatePending, job.StateRetrying} {
		t.Run(string(state), func(t *testing.T) {
			s := memory.New()
			eng, rec := newJobOpsEngine(t, s)
			j := putJobInState(t, eng, s, state)

			ctx := ext.WithActor(context.Background(), "alice")
			got, err := eng.CancelJob(ctx, j.ID)
			if err != nil {
				t.Fatalf("CancelJob: %v", err)
			}
			if got.State != job.StateCancelled || got.CompletedAt == nil {
				t.Errorf("returned job: state %s, completed_at %v; want cancelled with a time",
					got.State, got.CompletedAt)
			}

			stored, err := s.GetJob(context.Background(), j.ID)
			if err != nil {
				t.Fatalf("GetJob: %v", err)
			}
			if stored.State != job.StateCancelled {
				t.Errorf("stored state = %s, want cancelled", stored.State)
			}

			actions, cancelled, failed := rec.snapshot()
			if len(cancelled) != 1 || cancelled[0].ID != j.ID {
				t.Errorf("OnJobCancelled saw %d jobs, want exactly %s", len(cancelled), j.ID)
			}
			if len(failed) != 0 {
				t.Errorf("OnJobFailed fired %d times, want 0", len(failed))
			}
			if len(actions) != 1 {
				t.Fatalf("got %d operator actions, want 1", len(actions))
			}
			a := actions[0]
			if a.Kind != ext.ActionJobCancelled || a.JobID != j.ID || a.Actor != "alice" || a.At.IsZero() {
				t.Errorf("action = %+v, want job.cancelled for %s by alice with a time", a, j.ID)
			}
		})
	}
}

func TestCancelJob_TerminalStatesAreRefused(t *testing.T) {
	for _, state := range []job.State{job.StateCompleted, job.StateFailed, job.StateCancelled} {
		t.Run(string(state), func(t *testing.T) {
			s := memory.New()
			eng, rec := newJobOpsEngine(t, s)
			j := putJobInState(t, eng, s, state)

			_, err := eng.CancelJob(context.Background(), j.ID)
			if !errors.Is(err, dispatch.ErrInvalidState) {
				t.Fatalf("CancelJob error = %v, want ErrInvalidState", err)
			}

			stored, err := s.GetJob(context.Background(), j.ID)
			if err != nil {
				t.Fatalf("GetJob: %v", err)
			}
			if stored.State != state {
				t.Errorf("stored state = %s, want it left at %s", stored.State, state)
			}

			actions, cancelled, _ := rec.snapshot()
			if len(actions) != 0 || len(cancelled) != 0 {
				t.Errorf("a refused cancel emitted %d actions and %d cancellations, want none",
					len(actions), len(cancelled))
			}
		})
	}
}

func TestCancelJob_UnknownJob(t *testing.T) {
	eng, rec := newJobOpsEngine(t, memory.New())

	_, err := eng.CancelJob(context.Background(), id.NewJobID())
	if !errors.Is(err, dispatch.ErrJobNotFound) {
		t.Fatalf("CancelJob error = %v, want ErrJobNotFound", err)
	}
	if actions, _, _ := rec.snapshot(); len(actions) != 0 {
		t.Errorf("got %d operator actions for an unknown job, want 0", len(actions))
	}
}

// TestCancelJob_Running cancels a job a real pool is running. The handler
// blocks until its context ends, so the only way out is the cancel
// reaching it through the lease.
func TestCancelJob_Running(t *testing.T) {
	s := memory.New()
	eng, rec := newJobOpsEngine(t, s,
		dispatch.WithConcurrency(1),
		dispatch.WithQueues([]string{"default"}),
		dispatch.WithPollInterval(10*time.Millisecond),
		dispatch.WithHeartbeatInterval(20*time.Millisecond),
	)

	started := make(chan struct{})
	causes := make(chan error, 1)
	engine.Register(eng, job.NewDefinition("ops-block", func(ctx context.Context, _ struct{}) error {
		close(started)
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return ctx.Err()
	}))

	j, err := eng.EnqueueRaw(context.Background(), "ops-block", []byte(`{}`))
	if err != nil {
		t.Fatalf("EnqueueRaw: %v", err)
	}

	if startErr := eng.Start(context.Background()); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	got, err := eng.CancelJob(ext.WithActor(context.Background(), "bob"), j.ID)
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if got.State != job.StateCancelled {
		t.Errorf("returned state = %s, want cancelled", got.State)
	}

	select {
	case cause := <-causes:
		if !errors.Is(cause, job.ErrLeaseLost) {
			t.Errorf("handler context cause = %v, want job.ErrLeaseLost", cause)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler's context was not cancelled after CancelJob")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, cancelled, _ := rec.snapshot()
		if len(cancelled) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("OnJobCancelled never fired for the running job")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Several heartbeats later the row must still say cancelled: the
	// worker's own terminal write was refused, not merely late.
	time.Sleep(100 * time.Millisecond)

	stored, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.State != job.StateCancelled || stored.CompletedAt == nil {
		t.Errorf("stored job: state %s, completed_at %v; want cancelled with a time",
			stored.State, stored.CompletedAt)
	}

	actions, cancelled, failed := rec.snapshot()
	if len(cancelled) != 1 || cancelled[0].ID != j.ID || cancelled[0].State != job.StateCancelled {
		t.Errorf("OnJobCancelled saw %d jobs, want one, %s, in state cancelled", len(cancelled), j.ID)
	}
	if len(failed) != 0 {
		t.Errorf("OnJobFailed fired for %v, want never: a cancel is not a failure", failed)
	}
	if len(actions) != 1 || actions[0].Kind != ext.ActionJobCancelled || actions[0].Actor != "bob" {
		t.Errorf("actions = %+v, want one job.cancelled by bob", actions)
	}
}

func TestRetryJob_ClaimsTheDLQEntry(t *testing.T) {
	s := memory.New()
	eng, rec := newJobOpsEngine(t, s)
	j, entryID := pushFailed(t, eng, s)

	got, err := eng.RetryJob(ext.WithActor(context.Background(), "carol"), j.ID)
	if err != nil {
		t.Fatalf("RetryJob: %v", err)
	}

	stored, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	for _, c := range []*job.Job{got, stored} {
		if c.State != job.StatePending || c.RetryCount != 0 || c.LastError != "" ||
			c.CompletedAt != nil || c.StartedAt != nil || !c.WorkerID.IsNil() {
			t.Errorf("job after retry = state %s retries %d error %q completed %v started %v worker %s; "+
				"want a clean pending job", c.State, c.RetryCount, c.LastError, c.CompletedAt, c.StartedAt, c.WorkerID)
		}
	}

	entry, err := s.GetDLQ(context.Background(), entryID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if entry.ReplayedAt == nil || entry.ReplayedJobID == nil || *entry.ReplayedJobID != j.ID {
		t.Errorf("entry after retry: replayed_at %v replayed_job_id %v; want claimed for %s",
			entry.ReplayedAt, entry.ReplayedJobID, j.ID)
	}

	actions, _, _ := rec.snapshot()
	if len(actions) != 1 {
		t.Fatalf("got %d operator actions, want 1", len(actions))
	}
	a := actions[0]
	if a.Kind != ext.ActionJobRetried || a.JobID != j.ID || a.DLQID != entryID || a.Actor != "carol" {
		t.Errorf("action = %+v, want job.retried for %s with entry %s by carol", a, j.ID, entryID)
	}
}

func TestRetryJob_WithoutADLQEntry(t *testing.T) {
	s := memory.New()
	eng, rec := newJobOpsEngine(t, s)
	j := putJobInState(t, eng, s, job.StateFailed)

	if _, err := eng.RetryJob(context.Background(), j.ID); err != nil {
		t.Fatalf("RetryJob: %v", err)
	}

	stored, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.State != job.StatePending {
		t.Errorf("stored state = %s, want pending", stored.State)
	}

	actions, _, _ := rec.snapshot()
	if len(actions) != 1 || actions[0].Kind != ext.ActionJobRetried || !actions[0].DLQID.IsNil() {
		t.Errorf("actions = %+v, want one job.retried with no entry", actions)
	}
}

func TestRetryJob_OnlyFailedJobs(t *testing.T) {
	for _, state := range []job.State{
		job.StatePending, job.StateRunning, job.StateRetrying, job.StateCompleted, job.StateCancelled,
	} {
		t.Run(string(state), func(t *testing.T) {
			s := memory.New()
			eng, rec := newJobOpsEngine(t, s)
			j := putJobInState(t, eng, s, state)

			_, err := eng.RetryJob(context.Background(), j.ID)
			if !errors.Is(err, dispatch.ErrInvalidState) {
				t.Fatalf("RetryJob error = %v, want ErrInvalidState", err)
			}
			if actions, _, _ := rec.snapshot(); len(actions) != 0 {
				t.Errorf("a refused retry emitted %d actions, want 0", len(actions))
			}
		})
	}
}

func TestRetryJob_TwiceIsRefusedByTheClaim(t *testing.T) {
	s := memory.New()
	eng, rec := newJobOpsEngine(t, s)
	j, _ := pushFailed(t, eng, s)

	if _, err := eng.RetryJob(context.Background(), j.ID); err != nil {
		t.Fatalf("first RetryJob: %v", err)
	}

	// Fail it again by hand without a new dead letter entry, so the only
	// thing standing between it and a second retry is the old claim.
	j.State = job.StateFailed
	if err := s.UpdateJob(context.Background(), j); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	_, err := eng.RetryJob(context.Background(), j.ID)
	if !errors.Is(err, dispatch.ErrDLQAlreadyReplayed) {
		t.Fatalf("second RetryJob error = %v, want ErrDLQAlreadyReplayed", err)
	}
	if actions, _, _ := rec.snapshot(); len(actions) != 1 {
		t.Errorf("got %d operator actions, want only the first retry's", len(actions))
	}
}

// failUpdateStore fails UpdateJob while fail is set.
type failUpdateStore struct {
	*memory.Store
	mu   sync.Mutex
	fail bool
}

var errOpsUpdate = errors.New("update refused by test")

func (f *failUpdateStore) setFail(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = v
}

func (f *failUpdateStore) UpdateJob(ctx context.Context, j *job.Job) error {
	f.mu.Lock()
	fail := f.fail
	f.mu.Unlock()
	if fail {
		return errOpsUpdate
	}
	return f.Store.UpdateJob(ctx, j)
}

func TestRetryJob_FailedWriteReleasesTheClaim(t *testing.T) {
	s := &failUpdateStore{Store: memory.New()}
	eng, rec := newJobOpsEngine(t, s)
	j, entryID := pushFailed(t, eng, s.Store)

	s.setFail(true)
	_, err := eng.RetryJob(context.Background(), j.ID)
	if !errors.Is(err, errOpsUpdate) {
		t.Fatalf("RetryJob error = %v, want the store's update error", err)
	}

	entry, err := s.GetDLQ(context.Background(), entryID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if entry.ReplayedAt != nil || entry.ReplayedJobID != nil {
		t.Errorf("entry after a failed retry: replayed_at %v replayed_job_id %v; want released",
			entry.ReplayedAt, entry.ReplayedJobID)
	}
	if actions, _, _ := rec.snapshot(); len(actions) != 0 {
		t.Errorf("a failed retry emitted %d actions, want 0", len(actions))
	}

	s.setFail(false)
	if _, err := eng.RetryJob(context.Background(), j.ID); err != nil {
		t.Fatalf("RetryJob after the store recovered: %v", err)
	}
}
