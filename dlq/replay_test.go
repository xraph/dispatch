package dlq_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/dispatch"
	dispatchDLQ "github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// pushOne pushes one failed job and returns its entry.
func pushOne(t *testing.T, s *memory.Store, svc *dispatchDLQ.Service) *dispatchDLQ.Entry {
	t.Helper()

	j := newTestJob("replay-claim", []byte(`{}`))
	if err := svc.Push(context.Background(), j, errors.New("boom")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	entry, err := s.GetDLQByJobID(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("GetDLQByJobID: %v", err)
	}

	return entry
}

func TestService_Replay_SecondReplayIsRefused(t *testing.T) {
	s := memory.New()
	svc := dispatchDLQ.NewService(s, s)
	entry := pushOne(t, s, svc)

	first, err := svc.Replay(context.Background(), entry.ID)
	if err != nil {
		t.Fatalf("first Replay: %v", err)
	}

	_, err = svc.Replay(context.Background(), entry.ID)
	if !errors.Is(err, dispatch.ErrDLQAlreadyReplayed) {
		t.Fatalf("second Replay error = %v, want ErrDLQAlreadyReplayed", err)
	}

	pending, err := s.CountJobs(context.Background(), job.CountOpts{State: job.StatePending})
	if err != nil {
		t.Fatalf("CountJobs: %v", err)
	}
	if pending != 1 {
		t.Errorf("pending jobs = %d, want 1: the second replay must not enqueue", pending)
	}

	got, err := s.GetDLQ(context.Background(), entry.ID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if got.ReplayedJobID == nil || *got.ReplayedJobID != first.ID {
		t.Errorf("ReplayedJobID = %v, want the first replay's job %s", got.ReplayedJobID, first.ID)
	}
}

func TestService_Replay_EnqueuesThroughTheEnqueuer(t *testing.T) {
	s := memory.New()

	var seen []*job.Job
	svc := dispatchDLQ.NewService(s, s, dispatchDLQ.WithEnqueuer(func(ctx context.Context, j *job.Job) error {
		seen = append(seen, j)
		return s.EnqueueJob(ctx, j)
	}))
	entry := pushOne(t, s, svc)

	replayed, err := svc.Replay(context.Background(), entry.ID)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(seen) != 1 || seen[0].ID != replayed.ID {
		t.Fatalf("enqueuer saw %d jobs, want exactly the replayed one", len(seen))
	}
}

func TestService_Replay_FailedEnqueueReleasesTheClaim(t *testing.T) {
	s := memory.New()

	refuse := errors.New("enqueue refused by test")
	fail := true
	svc := dispatchDLQ.NewService(s, s, dispatchDLQ.WithEnqueuer(func(ctx context.Context, j *job.Job) error {
		if fail {
			return refuse
		}
		return s.EnqueueJob(ctx, j)
	}))
	entry := pushOne(t, s, svc)

	if _, err := svc.Replay(context.Background(), entry.ID); !errors.Is(err, refuse) {
		t.Fatalf("Replay error = %v, want the enqueuer's error", err)
	}

	got, err := s.GetDLQ(context.Background(), entry.ID)
	if err != nil {
		t.Fatalf("GetDLQ: %v", err)
	}
	if got.ReplayedAt != nil || got.ReplayedJobID != nil {
		t.Errorf("entry after a failed enqueue: replayed_at %v replayed_job_id %v; want released",
			got.ReplayedAt, got.ReplayedJobID)
	}

	fail = false
	if _, err := svc.Replay(context.Background(), entry.ID); err != nil {
		t.Fatalf("Replay after the enqueuer recovered: %v", err)
	}
}
