package worker_test

import (
	"context"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/worker"
)

// assignedJob builds a job that looks like it is currently assigned to a
// worker: a running job with WorkerID, StartedAt, HeartbeatAt, and
// LeaseExpiresAt all set, mirroring what a real claim leaves on the row.
func assignedJob(name string) *job.Job {
	started := time.Now().UTC().Add(-time.Minute)
	leaseExpires := time.Now().UTC().Add(time.Hour)

	return &job.Job{
		ID:             id.NewJobID(),
		Name:           name,
		Queue:          "default",
		State:          job.StateRunning,
		MaxRetries:     3,
		WorkerID:       id.NewWorkerID(),
		StartedAt:      &started,
		HeartbeatAt:    &started,
		LeaseExpiresAt: &leaseExpires,
	}
}

// wantAssignmentCleared fails the test unless every field that records a
// job's worker assignment has been cleared. This is the one property all
// four requeue paths tested below must share, even though each keeps its
// own distinct delay/retry behaviour.
func wantAssignmentCleared(t *testing.T, j *job.Job) {
	t.Helper()

	if j.WorkerID != (id.WorkerID{}) {
		t.Errorf("WorkerID = %s, want cleared", j.WorkerID)
	}
	if j.StartedAt != nil {
		t.Errorf("StartedAt = %v, want nil", j.StartedAt)
	}
	if j.HeartbeatAt != nil {
		t.Errorf("HeartbeatAt = %v, want nil", j.HeartbeatAt)
	}
	if j.LeaseExpiresAt != nil {
		t.Errorf("LeaseExpiresAt = %v, want nil", j.LeaseExpiresAt)
	}
	if j.State != job.StatePending {
		t.Errorf("State = %s, want %s", j.State, job.StatePending)
	}
}

// TestRequeueRateLimited_ClearsJobAssignment covers worker/pool.go's
// requeueRateLimited: before the fix it only touched State and RunAt,
// leaving a rate-limited job looking assigned to the worker that never
// even started it.
func TestRequeueRateLimited_ClearsJobAssignment(t *testing.T) {
	store := newFakeJobStore()
	pool := worker.NewPool(store, nil, nil, log.NewNoopLogger())

	j := assignedJob("rate-limited")
	pool.RequeueRateLimitedForTest(context.Background(), j)

	wantAssignmentCleared(t, j)
}

// TestRequeueUndispatched_ClearsJobAssignment covers requeueUndispatched,
// the best-effort shutdown path for a claimed-but-not-started job. Before
// the fix it cleared StartedAt but left WorkerID, HeartbeatAt, and
// LeaseExpiresAt in place.
func TestRequeueUndispatched_ClearsJobAssignment(t *testing.T) {
	store := newFakeJobStore()
	pool := worker.NewPool(store, nil, nil, log.NewNoopLogger())

	j := assignedJob("undispatched")
	pool.RequeueUndispatchedForTest(j)

	wantAssignmentCleared(t, j)
}

// TestReapStaleJobsLegacy_ClearsJobAssignment covers the legacy
// SELECT-then-UPDATE reap path a backend implementing only job.Store
// falls back to. Before the fix it cleared WorkerID/HeartbeatAt/StartedAt
// but not LeaseExpiresAt.
//
// storeOnly hides memory.Store's job.LeaseStore methods so the pool
// routes reapStaleJobs to the legacy path instead of
// reclaimExpiredLeases, exactly as it would against a real
// capability-less backend.
func TestReapStaleJobsLegacy_ClearsJobAssignment(t *testing.T) {
	ctx := context.Background()
	s := storeOnly{Store: memory.New()}

	j := assignedJob("stale-legacy")
	if err := s.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}

	pool := worker.NewPool(s, nil, nil, log.NewNoopLogger(),
		worker.WithStaleJobThreshold(time.Second))
	pool.ReclaimOnce(ctx)

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	wantAssignmentCleared(t, got)
}

// TestRequeueAfterLaunchFailure_ClearsJobAssignment covers
// Runner.requeueAfterLaunchFailure. Before the fix it only touched State
// and RunAt, so a job whose sandbox failed to launch went back to
// pending still carrying the WorkerID and lease fields of the attempt
// that never ran.
func TestRequeueAfterLaunchFailure_ClearsJobAssignment(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("launch.job", func(context.Context, struct{}) error {
		return &exec.Error{Status: exec.StatusLaunchFailed, Msg: "boom"}
	}).Register(reg)

	store := newFakeJobStore()
	runner := worker.NewRunner(
		reg, ext.NewRegistry(log.NewNoopLogger()), store, nil,
		backoff.NewConstant(time.Millisecond), nil, log.NewNoopLogger(),
	)

	j := assignedJob("launch.job")

	err := runner.Execute(context.Background(), j)
	if err == nil {
		t.Fatal("Execute() = nil, want the launch-failure error")
	}

	wantAssignmentCleared(t, j)
}
