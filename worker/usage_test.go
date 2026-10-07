package worker_test

import (
	"context"
	"errors"
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

// newUsageRunner builds a Runner over a memory store, which implements
// job.UsageRecorder.
func newUsageRunner(t *testing.T, reg *job.Registry) (*worker.Runner, *memory.Store) {
	t.Helper()

	s := memory.New()

	r := worker.NewRunner(
		reg,
		ext.NewRegistry(log.NewNoopLogger()),
		s,
		nil,
		backoff.NewExponential(time.Millisecond, time.Second),
		nil,
		log.NewNoopLogger(),
	)

	return r, s
}

func enqueued(t *testing.T, s *memory.Store, name string) *job.Job {
	t.Helper()

	j := &job.Job{
		ID:         id.NewJobID(),
		Name:       name,
		Queue:      "default",
		State:      job.StateRunning,
		MaxRetries: 3,
		RunAt:      time.Now().UTC(),
	}

	if err := s.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}

	return j
}

// TestUsageRecordedOnSuccess is the loop this feature exists to close:
// an attempt runs, and what it consumed survives it.
func TestUsageRecordedOnSuccess(t *testing.T) {
	ctx := context.Background()

	reg := job.NewRegistry()
	job.RegisterDefinition(reg, job.NewDefinition("ok",
		func(context.Context, struct{}) error {
			time.Sleep(2 * time.Millisecond)

			return nil
		}))

	r, s := newUsageRunner(t, reg)
	j := enqueued(t, s, "ok")

	if err := r.Execute(ctx, j); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got, err := s.ListJobUsage(ctx, job.UsageListOpts{})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d usage records, want 1", len(got))
	}

	rec := got[0]

	if rec.JobID != j.ID || rec.Name != "ok" {
		t.Fatalf("record does not identify the job: %+v", rec)
	}

	if rec.Status != job.StateCompleted {
		t.Fatalf("Status = %q, want completed", rec.Status)
	}

	if rec.WallTime <= 0 {
		t.Fatal("WallTime was not measured")
	}
}

// TestUsageRecordedOnFailure matters more than the success case: a job
// that died is the strongest evidence about what it needed.
func TestUsageRecordedOnFailure(t *testing.T) {
	ctx := context.Background()

	reg := job.NewRegistry()
	job.RegisterDefinition(reg, job.NewDefinition("boom",
		func(context.Context, struct{}) error {
			return errors.New("exploded")
		}))

	r, s := newUsageRunner(t, reg)
	j := enqueued(t, s, "boom")

	// Execute returns the handler error on a retryable failure.
	_ = r.Execute(ctx, j)

	got, err := s.ListJobUsage(ctx, job.UsageListOpts{})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d usage records for a failed attempt, want 1", len(got))
	}

	if got[0].Status != job.StateFailed {
		t.Fatalf("Status = %q, want failed", got[0].Status)
	}
}

// TestUsageRecordsAttemptNotNextAttempt guards the ordering bug this is
// easy to introduce: handleFailure increments RetryCount on its way to
// scheduling the retry, so recording after it would misattribute every
// measurement by one attempt.
func TestUsageRecordsAttemptNotNextAttempt(t *testing.T) {
	ctx := context.Background()

	reg := job.NewRegistry()
	job.RegisterDefinition(reg, job.NewDefinition("boom",
		func(context.Context, struct{}) error {
			return errors.New("exploded")
		}))

	r, s := newUsageRunner(t, reg)
	j := enqueued(t, s, "boom")
	j.RetryCount = 2

	_ = r.Execute(ctx, j)

	got, err := s.ListJobUsage(ctx, job.UsageListOpts{})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}

	if got[0].Attempt != 2 {
		t.Fatalf("Attempt = %d, want 2 (the attempt that ran, not the next one)", got[0].Attempt)
	}
}

// TestUsageRecordingSurvivesCancelledContext pins the case that would
// otherwise lose exactly the attempts worth studying: a job cancelled by
// timeout still has to leave its measurements behind.
func TestUsageRecordingSurvivesCancelledContext(t *testing.T) {
	reg := job.NewRegistry()
	job.RegisterDefinition(reg, job.NewDefinition("slow",
		func(ctx context.Context, _ struct{}) error {
			<-ctx.Done()

			return ctx.Err()
		}))

	r, s := newUsageRunner(t, reg)
	j := enqueued(t, s, "slow")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_ = r.Execute(ctx, j)

	got, err := s.ListJobUsage(context.Background(), job.UsageListOpts{})
	if err != nil {
		t.Fatalf("ListJobUsage: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("a cancelled attempt recorded %d rows, want 1", len(got))
	}
}

// TestNoUsageStoreIsNotAnError proves the capability is genuinely
// optional: a backend without it runs jobs unchanged.
func TestNoUsageStoreIsNotAnError(t *testing.T) {
	ctx := context.Background()

	reg := job.NewRegistry()
	job.RegisterDefinition(reg, job.NewDefinition("ok",
		func(context.Context, struct{}) error { return nil }))

	s := memory.New()

	// storeWithoutUsage hides the memory store's UsageRecorder methods.
	r := worker.NewRunner(
		reg,
		ext.NewRegistry(log.NewNoopLogger()),
		storeWithoutUsage{s},
		nil,
		backoff.NewExponential(time.Millisecond, time.Second),
		nil,
		log.NewNoopLogger(),
	)

	j := enqueued(t, s, "ok")

	if err := r.Execute(ctx, j); err != nil {
		t.Fatalf("Execute against a store without usage support: %v", err)
	}
}

// storeWithoutUsage embeds the job.Store interface, so it satisfies
// job.Store by promotion while deliberately not satisfying
// job.UsageRecorder — whatever the concrete value beneath it can do.
type storeWithoutUsage struct {
	job.Store
}
