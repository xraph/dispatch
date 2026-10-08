package worker_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/worker"
)

// cancelTracker records which jobs reached OnJobCancelled and OnJobFailed.
type cancelTracker struct {
	mu        sync.Mutex
	cancelled []*job.Job
	failed    []id.JobID
}

func (c *cancelTracker) Name() string { return "cancel-tracker" }

func (c *cancelTracker) OnJobCancelled(_ context.Context, j *job.Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelled = append(c.cancelled, j)
	return nil
}

func (c *cancelTracker) OnJobFailed(_ context.Context, j *job.Job, _ error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failed = append(c.failed, j.ID)
	return nil
}

func (c *cancelTracker) counts() (cancelled []*job.Job, failed int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*job.Job(nil), c.cancelled...), len(c.failed)
}

// ctxStore is the memory store made to honour a cancelled context on
// the calls a runner makes, the way every persistent backend does. The
// memory store ignores ctx, which is how a lost-lease write through a
// cancelled context went unnoticed. It also counts job writes.
type ctxStore struct {
	*memory.Store
	writes atomic.Int32
}

func (s *ctxStore) GetJob(ctx context.Context, jobID id.JobID) (*job.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.Store.GetJob(ctx, jobID)
}

func (s *ctxStore) UpdateJob(ctx context.Context, j *job.Job) error {
	s.writes.Add(1)
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Store.UpdateJob(ctx, j)
}

func (s *ctxStore) UpdateLeasedJob(ctx context.Context, j *job.Job, workerID id.WorkerID, epoch int) error {
	s.writes.Add(1)
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Store.UpdateLeasedJob(ctx, j, workerID, epoch)
}

// claimLeased enqueues one job named name and claims it with a lease,
// returning the claimed copy and the worker that holds it.
func claimLeased(t *testing.T, s *memory.Store, name string) (*job.Job, id.WorkerID) {
	t.Helper()
	ctx := context.Background()

	now := time.Now().UTC()
	j := &job.Job{
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      now,
	}
	j.CreatedAt = now
	j.UpdatedAt = now
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}

	workerID := id.NewWorkerID()
	claimed, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{"default"},
		Limit:      1,
		WorkerID:   workerID,
		LeaseUntil: now.Add(time.Hour),
	})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("DequeueJobs: %v (n=%d)", err, len(claimed))
	}

	return claimed[0], workerID
}

// setStoredState rewrites the stored row's state the way an operator
// cancel (cancelled) or a reclaim (pending) would leave it.
func setStoredState(t *testing.T, s *memory.Store, jobID id.JobID, state job.State) {
	t.Helper()

	row, err := s.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	row.State = state
	if state == job.StateCancelled {
		now := time.Now().UTC()
		row.CompletedAt = &now
	}
	if err := s.UpdateJob(context.Background(), row); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
}

func newCancelRunner(t *testing.T, st job.Store, name string, handler func(context.Context, struct{}) error) (*worker.Runner, *cancelTracker) {
	t.Helper()

	reg := job.NewRegistry()
	job.NewDefinition(name, handler).Register(reg)

	extensions := ext.NewRegistry(log.NewNoopLogger())
	tracker := &cancelTracker{}
	extensions.Register(tracker)

	runner := worker.NewRunner(
		reg, extensions, st, nil,
		backoff.NewConstant(time.Millisecond), nil, log.NewNoopLogger(),
	)

	return runner, tracker
}

// TestRunner_LeaseLostToACancelledRow_ReportsCancelled is an operator
// cancel landing while the handler runs and the handler finishing anyway:
// its terminal write is refused by the fence, and the runner must report
// the cancel it finds in the row, not a failure.
func TestRunner_LeaseLostToACancelledRow_ReportsCancelled(t *testing.T) {
	s := memory.New()
	j, workerID := claimLeased(t, s, "finishes.job")
	setStoredState(t, s, j.ID, job.StateCancelled)

	runner, tracker := newCancelRunner(t, s, "finishes.job",
		func(context.Context, struct{}) error { return nil })

	ctx := worker.WithLeaseFenceForTest(context.Background(), s, workerID, j.LeaseEpoch)
	if err := runner.Execute(ctx, j); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("Execute() = %v, want job.ErrLeaseLost", err)
	}

	cancelled, failed := tracker.counts()
	if len(cancelled) != 1 || cancelled[0].ID != j.ID || cancelled[0].State != job.StateCancelled {
		t.Errorf("OnJobCancelled saw %d jobs, want one, %s, in state cancelled", len(cancelled), j.ID)
	}
	if failed != 0 {
		t.Errorf("OnJobFailed fired %d times, want 0", failed)
	}

	stored, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.State != job.StateCancelled {
		t.Errorf("stored state = %s, want cancelled left alone", stored.State)
	}
}

// TestRunner_AttemptCancelledForALostLease_WritesNothing is the path the
// pool takes: a renewal came back ErrLeaseLost, so the attempt's context
// is cancelled with that cause before the handler returns. Through a
// store that honours the context, the old terminal write failed as
// context.Canceled and reported nothing at all.
func TestRunner_AttemptCancelledForALostLease_WritesNothing(t *testing.T) {
	tests := []struct {
		name          string
		rowState      job.State
		wantCancelled bool
	}{
		{name: "cancelled by an operator", rowState: job.StateCancelled, wantCancelled: true},
		{name: "reclaimed by the reaper", rowState: job.StatePending, wantCancelled: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ctxStore{Store: memory.New()}
			j, workerID := claimLeased(t, s.Store, "blocks.job")
			setStoredState(t, s.Store, j.ID, tt.rowState)

			runner, tracker := newCancelRunner(t, s, "blocks.job",
				func(ctx context.Context, _ struct{}) error {
					<-ctx.Done()
					return ctx.Err()
				})

			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(job.ErrLeaseLost)
			ctx = worker.WithLeaseFenceForTest(ctx, s, workerID, j.LeaseEpoch)

			if err := runner.Execute(ctx, j); !errors.Is(err, job.ErrLeaseLost) {
				t.Fatalf("Execute() = %v, want job.ErrLeaseLost", err)
			}
			if n := s.writes.Load(); n != 0 {
				t.Errorf("job writes = %d, want 0: the lease is known lost", n)
			}

			cancelled, failed := tracker.counts()
			if tt.wantCancelled {
				if len(cancelled) != 1 || failed != 0 {
					t.Errorf("cancelled %d, failed %d; want 1 and 0", len(cancelled), failed)
				}
				return
			}
			if len(cancelled) != 0 || failed != 1 {
				t.Errorf("cancelled %d, failed %d; want 0 and 1", len(cancelled), failed)
			}
		})
	}
}
