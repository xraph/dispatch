package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/worker"
)

// TestPool_LeaseTTLFor exercises the TTL precedence chain: a job's own
// LeaseTTL, then the pool's WithDefaultLeaseTTL, then the legacy
// WithStaleJobThreshold, then job.DefaultLeaseTTL when nothing at all is
// configured.
func TestPool_LeaseTTLFor(t *testing.T) {
	const (
		jobTTL      = 5 * time.Second
		poolDefault = 45 * time.Second
		staleThresh = 90 * time.Second
	)

	tests := []struct {
		name string
		opts []worker.PoolOption
		job  *job.Job
		want time.Duration
	}{
		{
			name: "job's own TTL wins",
			opts: []worker.PoolOption{
				worker.WithDefaultLeaseTTL(poolDefault),
				worker.WithStaleJobThreshold(staleThresh),
			},
			job:  &job.Job{LeaseTTL: jobTTL},
			want: jobTTL,
		},
		{
			name: "pool default when the job declares none",
			opts: []worker.PoolOption{
				worker.WithDefaultLeaseTTL(poolDefault),
				worker.WithStaleJobThreshold(staleThresh),
			},
			job:  &job.Job{},
			want: poolDefault,
		},
		{
			name: "stale-job threshold when the pool default is unset",
			opts: []worker.PoolOption{
				worker.WithStaleJobThreshold(staleThresh),
			},
			job:  &job.Job{},
			want: staleThresh,
		},
		{
			name: "job.DefaultLeaseTTL when nothing is configured",
			opts: nil,
			job:  &job.Job{},
			want: job.DefaultLeaseTTL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := worker.NewPool(memory.New(), nil, nil, log.NewNoopLogger(), tt.opts...)

			got := pool.LeaseTTLFor(tt.job)
			if got != tt.want {
				t.Errorf("LeaseTTLFor() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestLeaseFencing is the store-level proof the pool's renewal path
// depends on: a lease granted at claim time, expired and reclaimed by
// another worker, must fence the original holder's next renewal with
// job.ErrLeaseLost.
func TestLeaseFencing(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	workerA := id.NewWorkerID()

	j := &job.Job{
		ID:         id.NewJobID(),
		Name:       "fenced",
		Queue:      "default",
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      time.Now().UTC(),
	}
	j.CreatedAt = time.Now().UTC()
	j.UpdatedAt = j.CreatedAt

	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Claim it with a lease that is already expired, so the very next
	// ReclaimExpiredLeases sweep picks it up.
	claimed, err := s.DequeueJobs(ctx, job.DequeueOpts{
		Queues:     []string{"default"},
		Limit:      1,
		WorkerID:   workerA,
		LeaseUntil: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("dequeue error: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed = %d jobs, want 1", len(claimed))
	}
	grantedEpoch := claimed[0].LeaseEpoch

	reclaimed, err := s.ReclaimExpiredLeases(ctx, 10)
	if err != nil {
		t.Fatalf("reclaim error: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("reclaimed = %d jobs, want 1", len(reclaimed))
	}

	// Worker A, unaware it was reclaimed, tries to renew with the epoch
	// it was originally granted. That must be fenced.
	err = s.RenewLease(ctx, j.ID, workerA, grantedEpoch, time.Now().UTC().Add(time.Minute))
	if !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("RenewLease() error = %v, want job.ErrLeaseLost", err)
	}
}

// fakeLeaseStore embeds *memory.Store for every method except RenewLease,
// which it overrides to return a caller-configured error. This is what
// lets a test drive the pool's heartbeat/renewal path through a specific
// outcome — lease lost, or a transient store error — without needing a
// lease that has genuinely expired or a second worker to steal it.
type fakeLeaseStore struct {
	*memory.Store

	renewErr error
}

// RenewLease shadows the promoted *memory.Store method and always
// returns the configured error, ignoring every argument.
func (f *fakeLeaseStore) RenewLease(
	_ context.Context,
	_ id.JobID,
	_ id.WorkerID,
	_ int,
	_ time.Time,
) error {
	return f.renewErr
}

// TestPool_SendHeartbeats_CancelsOnLeaseLost is the pool-level half of
// the fencing proof: TestLeaseFencing above shows the STORE returns
// job.ErrLeaseLost when the caller no longer holds the lease; this shows
// the POOL reacts to that return by cancelling the job's context with
// job.ErrLeaseLost as the cause — the one behaviour this whole task
// exists to add. Asserting only ctx.Err() != nil would not distinguish
// this from a shutdown cancellation, so the assertion goes through
// context.Cause.
func TestPool_SendHeartbeats_CancelsOnLeaseLost(t *testing.T) {
	s := &fakeLeaseStore{Store: memory.New(), renewErr: job.ErrLeaseLost}
	pool := worker.NewPool(s, nil, nil, log.NewNoopLogger())

	jobID := id.NewJobID().String()
	ctx := pool.TrackJob(jobID, 3, 30*time.Second)

	pool.HeartbeatOnce(context.Background())

	if ctx.Err() == nil {
		t.Fatalf("job context was not cancelled after RenewLease returned job.ErrLeaseLost")
	}

	cause := context.Cause(ctx)
	if !errors.Is(cause, job.ErrLeaseLost) {
		t.Fatalf("context.Cause(ctx) = %v, want job.ErrLeaseLost", cause)
	}
}

// TestPool_SendHeartbeats_KeepsJobAliveOnTransientError is the negative
// case: a renewal failure that is NOT job.ErrLeaseLost — a transient
// store blip — must leave the job's context alive. A pool that cancelled
// on every renewal error would kill healthy jobs whenever the store
// hiccups, which is exactly what the WARN-not-cancel branch in
// sendHeartbeats exists to avoid.
func TestPool_SendHeartbeats_KeepsJobAliveOnTransientError(t *testing.T) {
	s := &fakeLeaseStore{Store: memory.New(), renewErr: context.DeadlineExceeded}
	pool := worker.NewPool(s, nil, nil, log.NewNoopLogger())

	jobID := id.NewJobID().String()
	ctx := pool.TrackJob(jobID, 3, 30*time.Second)

	pool.HeartbeatOnce(context.Background())

	if err := ctx.Err(); err != nil {
		t.Fatalf("job context was cancelled on a transient renewal error: cause = %v", context.Cause(ctx))
	}
}

// storeOnly wraps a job.Store behind the bare interface, so it satisfies
// job.Store without also satisfying job.LeaseStore even though the
// concrete memory.Store underneath implements both. Embedding the
// interface value rather than the concrete type is what hides the extra
// method set: storeOnly's own method set is exactly job.Store's.
type storeOnly struct {
	job.Store
}

// TestPool_CapabilityLessStore proves a custom backend that implements
// only job.Store — not job.LeaseStore — keeps working: construction logs
// rather than panics, and TTL resolution still falls all the way back to
// job.DefaultLeaseTTL.
func TestPool_CapabilityLessStore(t *testing.T) {
	s := storeOnly{Store: memory.New()}

	pool := worker.NewPool(s, nil, nil, log.NewNoopLogger())

	got := pool.LeaseTTLFor(&job.Job{})
	if got != job.DefaultLeaseTTL {
		t.Errorf("LeaseTTLFor() = %v, want job.DefaultLeaseTTL (%v)", got, job.DefaultLeaseTTL)
	}
}
