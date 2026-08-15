package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/worker"
)

// fakeLeaseJobStore extends fakeJobStore (runner_test.go) with a
// scriptable UpdateLeasedJob and no-op RenewLease / ReclaimExpiredLeases
// stubs, so it satisfies job.LeaseStore. This is what lets a runner-level
// test drive the fenced path through a specific outcome — applied, or
// job.ErrLeaseLost — without a real store's timing, mirroring
// fakeLeaseStore in lease_test.go for the pool's own renewal path.
type fakeLeaseJobStore struct {
	*fakeJobStore

	leasedErr     error
	leasedUpdates int
	lastJob       *job.Job
	lastWorkerID  id.WorkerID
	lastEpoch     int
}

func (f *fakeLeaseJobStore) UpdateLeasedJob(_ context.Context, j *job.Job, workerID id.WorkerID, epoch int) error {
	f.leasedUpdates++
	f.lastJob = j
	f.lastWorkerID = workerID
	f.lastEpoch = epoch

	return f.leasedErr
}

func (f *fakeLeaseJobStore) RenewLease(context.Context, id.JobID, id.WorkerID, int, time.Time) error {
	return nil
}

func (f *fakeLeaseJobStore) ReclaimExpiredLeases(context.Context, int) ([]*job.Job, error) {
	return nil, nil
}

var _ job.LeaseStore = (*fakeLeaseJobStore)(nil)

func TestRunner_HandleSuccess_RoutesThroughFencedWriteWhenAttached(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("ok.job", func(context.Context, struct{}) error { return nil }).Register(reg)

	store := &fakeLeaseJobStore{fakeJobStore: newFakeJobStore()}
	runner := worker.NewRunner(
		reg, ext.NewRegistry(log.NewNoopLogger()), store, nil,
		backoff.NewExponential(time.Second, time.Hour), nil, log.NewNoopLogger(),
	)

	workerID := id.NewWorkerID()
	j := &job.Job{ID: id.NewJobID(), Name: "ok.job", MaxRetries: 3}
	ctx := worker.WithLeaseFenceForTest(context.Background(), store, workerID, 7)

	if err := runner.Execute(ctx, j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}

	if store.leasedUpdates != 1 {
		t.Errorf("UpdateLeasedJob calls = %d, want 1", store.leasedUpdates)
	}
	if store.updates != 0 {
		t.Errorf("UpdateJob calls = %d, want 0 — a fenced attempt must not also use the unfenced path",
			store.updates)
	}
	if store.lastWorkerID != workerID || store.lastEpoch != 7 {
		t.Errorf("fenced write used (worker=%s epoch=%d), want (worker=%s epoch=%d)",
			store.lastWorkerID, store.lastEpoch, workerID, 7)
	}
	if store.lastJob.State != job.StateCompleted {
		t.Errorf("fenced write's State = %s, want %s", store.lastJob.State, job.StateCompleted)
	}
}

func TestRunner_Execute_FallsBackToUnfencedWriteWithNoFenceAttached(t *testing.T) {
	// No context fence at all — a Runner used without a Pool (NewExecutor,
	// or any direct caller) must keep calling the plain UpdateJob it
	// always has. This is the backward-compatibility guarantee: adopting
	// job.LeaseStore on a backend must not become a hard requirement of
	// using Runner.
	reg := job.NewRegistry()
	job.NewDefinition("ok.job", func(context.Context, struct{}) error { return nil }).Register(reg)

	store := &fakeLeaseJobStore{fakeJobStore: newFakeJobStore()}
	runner := worker.NewRunner(
		reg, ext.NewRegistry(log.NewNoopLogger()), store, nil,
		backoff.NewExponential(time.Second, time.Hour), nil, log.NewNoopLogger(),
	)

	j := &job.Job{ID: id.NewJobID(), Name: "ok.job", MaxRetries: 3}

	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}

	if store.updates != 1 {
		t.Errorf("UpdateJob calls = %d, want 1", store.updates)
	}
	if store.leasedUpdates != 0 {
		t.Errorf("UpdateLeasedJob calls = %d, want 0 — no fence was attached", store.leasedUpdates)
	}
}

// TestRunner_TerminalWrites_AbandonOnLeaseLost exercises all four
// fenced call sites — success, retry, DLQ, and the launch-failure
// requeue — against a store scripted to return job.ErrLeaseLost, and
// checks the one behaviour the whole fix exists for: the runner does
// not retry, DLQ, or touch the row again, and the loss is observable
// through the extension registry with no new plumbing.
func TestRunner_TerminalWrites_AbandonOnLeaseLost(t *testing.T) {
	tests := []struct {
		name       string
		jobName    string
		handler    func(context.Context, struct{}) error
		maxRetries int
		retryCount int
	}{
		{
			name:    "handleSuccess",
			jobName: "ok.job",
			handler: func(context.Context, struct{}) error { return nil },
		},
		{
			name:       "scheduleRetry",
			jobName:    "retry.job",
			handler:    func(context.Context, struct{}) error { return errors.New("boom") },
			maxRetries: 3,
			retryCount: 0,
		},
		{
			name:       "sendToDLQ",
			jobName:    "dlq.job",
			handler:    func(context.Context, struct{}) error { return errors.New("boom") },
			maxRetries: 1,
			retryCount: 1, // already at the ceiling, so handleFailure routes straight to sendToDLQ
		},
		{
			// A launch failure never reaches the middleware chain's
			// ordinary error path — it is *exec.Error carrying a status
			// that CountsAgainstRetries() reports false for, which
			// handleFailure routes to requeueAfterLaunchFailure instead of
			// scheduleRetry. Before this fix that call went through the
			// plain, unfenced store.UpdateJob no matter what was attached
			// to ctx; this case only proves anything once it does not.
			name:    "requeueAfterLaunchFailure",
			jobName: "launch.job",
			handler: func(context.Context, struct{}) error {
				return &exec.Error{Status: exec.StatusLaunchFailed, Msg: "boom"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := job.NewRegistry()
			job.NewDefinition(tt.jobName, tt.handler).Register(reg)

			store := &fakeLeaseJobStore{fakeJobStore: newFakeJobStore(), leasedErr: job.ErrLeaseLost}
			extensions := ext.NewRegistry(log.NewNoopLogger())
			tracker := &trackingExt{}
			extensions.Register(tracker)

			runner := worker.NewRunner(
				reg, extensions, store, nil,
				backoff.NewConstant(time.Millisecond), nil, log.NewNoopLogger(),
			)

			workerID := id.NewWorkerID()
			j := &job.Job{
				ID:         id.NewJobID(),
				Name:       tt.jobName,
				MaxRetries: tt.maxRetries,
				RetryCount: tt.retryCount,
			}
			ctx := worker.WithLeaseFenceForTest(context.Background(), store, workerID, 5)

			err := runner.Execute(ctx, j)
			if !errors.Is(err, job.ErrLeaseLost) {
				t.Fatalf("Execute() error = %v, want job.ErrLeaseLost", err)
			}

			if store.leasedUpdates != 1 {
				t.Errorf("UpdateLeasedJob calls = %d, want exactly 1 — no retry of the write itself",
					store.leasedUpdates)
			}
			if store.updates != 0 {
				t.Errorf("UpdateJob calls = %d, want 0 — a lost lease must never fall back to the unfenced write",
					store.updates)
			}
			if !tracker.failed.Load() {
				t.Error("OnJobFailed did not fire — audit_hook/relay_hook would not observe the lost lease")
			}
		})
	}
}

// TestPool_LeaseLostDuringExecution_DoesNotClobberTheWinner reproduces
// the bug this whole track exists to close, end to end, through the real
// Pool + Runner + a real store: a worker's lease is reclaimed and handed
// to a second claimant while the first worker's handler is still running,
// and the first worker's handler then returns success. Before this fix
// that unfenced terminal write would still land — rolling lease_epoch
// backwards, marking the job completed, and fencing the legitimate new
// holder off its own job. After it, the write must be refused and the
// second claimant's row must survive completely untouched.
func TestPool_LeaseLostDuringExecution_DoesNotClobberTheWinner(t *testing.T) {
	logger := log.NewNoopLogger()
	s := memory.New()
	reg := job.NewRegistry()
	extensions := ext.NewRegistry(logger)
	tracker := &trackingExt{}
	extensions.Register(tracker)

	dlqSvc := dlq.NewService(s, s)
	bo := backoff.NewConstant(10 * time.Millisecond)

	entered := make(chan struct{})
	release := make(chan struct{})
	job.RegisterDefinition(reg, job.NewDefinition("long-job", func(_ context.Context, _ struct{}) error {
		close(entered)
		<-release

		return nil
	}))

	executor := worker.NewExecutor(reg, extensions, s, dlqSvc, bo, logger, middleware.Recover(logger))

	pool := worker.NewPool(s, executor, extensions, logger,
		worker.WithPoolConcurrency(1),
		worker.WithPollInterval(10*time.Millisecond),
		worker.WithPoolQueues([]string{"default"}),
		// A short TTL and no heartbeat loop: the pool grants a lease at
		// claim time but never renews it, so it's genuinely expired by
		// the time this test reclaims it below — not a timing fiction.
		worker.WithDefaultLeaseTTL(30*time.Millisecond),
	)

	j := &job.Job{
		ID:         id.NewJobID(),
		Name:       "long-job",
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      time.Now().UTC(),
	}
	j.CreatedAt = time.Now().UTC()
	j.UpdatedAt = j.CreatedAt

	if err := s.EnqueueJob(context.Background(), j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_ = pool.Stop(ctx)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	// The lease TTL (30ms) has certainly elapsed by now: entering the
	// handler required a full poll-and-claim round trip already. Reclaim
	// it out from under the running worker and hand it to a second
	// claimant, exactly as an operator's reaper would after a pause.
	time.Sleep(60 * time.Millisecond)

	reclaimed, err := s.ReclaimExpiredLeases(context.Background(), 10)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if !storetestContains(reclaimed, j.ID) {
		t.Fatalf("reclaimed set does not contain %s", j.ID)
	}

	winner := id.NewWorkerID()
	claimed, err := s.DequeueJobs(context.Background(), job.DequeueOpts{
		Queues:     []string{"default"},
		Limit:      1,
		WorkerID:   winner,
		LeaseUntil: time.Now().UTC().Add(time.Hour),
	})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("second claim: %v (n=%d)", err, len(claimed))
	}
	winnerEpoch := claimed[0].LeaseEpoch

	// Let the original (now-zombie) attempt finish successfully.
	close(release)

	deadline := time.After(3 * time.Second)
	for !tracker.failed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the zombie's fenced write to be refused")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	after, err := s.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.State != job.StateRunning {
		t.Errorf("State = %s, want %s — the zombie's write must not have applied",
			after.State, job.StateRunning)
	}
	if after.WorkerID != winner {
		t.Errorf("WorkerID = %s, want %s — the winner's claim must survive untouched",
			after.WorkerID, winner)
	}
	if after.LeaseEpoch != winnerEpoch {
		t.Errorf("LeaseEpoch = %d, want %d — must not have been rolled back", after.LeaseEpoch, winnerEpoch)
	}
	if after.CompletedAt != nil {
		t.Errorf("CompletedAt = %v, want nil — the job was never legitimately completed", after.CompletedAt)
	}
}

// storetestContains mirrors storetest.Contains without importing the
// storetest package's testing.T-shaped API into this one.
func storetestContains(jobs []*job.Job, jobID id.JobID) bool {
	for _, j := range jobs {
		if j.ID == jobID {
			return true
		}
	}

	return false
}
