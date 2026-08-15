package worker_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/worker"
)

// recordingExecutor captures the Request the runner built.
type recordingExecutor struct {
	got       *exec.Request
	result    *exec.Result
	err       error
	reclaimed int
	closed    int
}

func (r *recordingExecutor) Name() string      { return "recording" }
func (r *recordingExecutor) Level() exec.Level { return exec.LevelProcess }

func (r *recordingExecutor) Run(_ context.Context, req *exec.Request) (*exec.Result, error) {
	r.got = req
	if r.err != nil {
		return nil, r.err
	}
	if r.result != nil {
		return r.result, nil
	}

	return &exec.Result{Status: exec.StatusOK}, nil
}

func (r *recordingExecutor) Reclaim(context.Context, id.WorkerID) error {
	r.reclaimed++
	return nil
}

func (r *recordingExecutor) Close() error {
	r.closed++
	return nil
}

func newTestRunner(t *testing.T, reg *job.Registry, executors *exec.Registry) (*worker.Runner, *fakeJobStore) {
	t.Helper()

	store := newFakeJobStore()

	return worker.NewRunner(
		reg,
		ext.NewRegistry(log.NewNoopLogger()),
		store,
		nil,
		backoff.NewExponential(time.Second, time.Hour),
		executors,
		log.NewNoopLogger(),
	), store
}

func TestRunner_ExecuteBuildsRequestFromJob(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	rec := &recordingExecutor{}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, _ := newTestRunner(t, reg, executors)

	j := &job.Job{
		ID:             id.NewJobID(),
		Name:           "test.job",
		Payload:        []byte(`{"a":1}`),
		RetryCount:     2,
		MaxRetries:     3,
		ScopeAppID:     "app_1",
		ScopeOrgID:     "org_1",
		ResourceLimits: resource.Set{resource.Memory: 256 << 20},
	}

	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
	if rec.got == nil {
		t.Fatal("executor was not called")
	}
	if rec.got.Name != "test.job" {
		t.Errorf("Request.Name = %q, want %q", rec.got.Name, "test.job")
	}
	if rec.got.Attempt != 2 {
		t.Errorf("Request.Attempt = %d, want 2", rec.got.Attempt)
	}
	if rec.got.ScopeAppID != "app_1" || rec.got.ScopeOrgID != "org_1" {
		t.Errorf("Request scope = (%q, %q), want (app_1, org_1)", rec.got.ScopeAppID, rec.got.ScopeOrgID)
	}
	if rec.got.Policy.Level != exec.LevelProcess {
		t.Errorf("Request.Policy.Level = %v, want %v", rec.got.Policy.Level, exec.LevelProcess)
	}
	// job.WithResourceLimits' resolved ceiling must cross the execution
	// boundary intact — this is the only thing that lets an isolated
	// rung enforce a per-job limit rather than a deployment-wide one.
	if got := rec.got.ResourceLimits[resource.Memory]; got != 256<<20 {
		t.Errorf("Request.ResourceLimits[memory] = %d, want %d", got, 256<<20)
	}
}

func TestRunner_ExecuteRoutesByPolicy(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("plain.job", func(context.Context, struct{}) error { return nil }).Register(reg)

	rec := &recordingExecutor{}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, _ := newTestRunner(t, reg, executors)

	// No declared isolation, so this must go to the default executor and
	// never reach the recording one.
	j := &job.Job{ID: id.NewJobID(), Name: "plain.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
	if rec.got != nil {
		t.Error("a job with no declared isolation was routed to the isolated executor")
	}
}

func TestRunner_LaunchFailureDoesNotConsumeRetries(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	rec := &recordingExecutor{
		result: &exec.Result{Status: exec.StatusLaunchFailed, HandlerErr: "image pull backoff"},
	}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, store := newTestRunner(t, reg, executors)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	err := runner.Execute(context.Background(), j)
	if err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}
	if j.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0 — a launch failure is infrastructure", j.RetryCount)
	}
	if j.State != job.StatePending && j.State != job.StateRetrying {
		t.Errorf("State = %q, want the job requeued", j.State)
	}
	if store.updates == 0 {
		t.Error("the job was never persisted")
	}
}

func TestRunner_RunErrorIsTreatedAsLaunchFailure(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	rec := &recordingExecutor{err: errors.New("image pull backoff")}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, store := newTestRunner(t, reg, executors)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	err := runner.Execute(context.Background(), j)
	if err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}
	if j.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0 — a raw Run error is a launch failure", j.RetryCount)
	}
	if j.State != job.StatePending && j.State != job.StateRetrying {
		t.Errorf("State = %q, want the job requeued", j.State)
	}
	if store.updates == 0 {
		t.Error("the job was never persisted")
	}
}

func TestRunner_RunErrorWrappingInvalidRequestGoesToDLQ(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	rec := &recordingExecutor{err: fmt.Errorf("bad: %w", exec.ErrInvalidRequest)}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, _ := newTestRunner(t, reg, executors)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	err := runner.Execute(context.Background(), j)
	if err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}
	if j.State != job.StateFailed {
		t.Errorf("State = %q, want %q — an invalid request must not requeue forever", j.State, job.StateFailed)
	}
}

// TestRunner_UnsatisfiablePolicyReachesDLQ guards the fix for a job whose
// declared execution policy no configured executor can satisfy: before
// this fix, terminalFor's error from exec.Registry.Select came straight
// back out of Execute without ever reaching handleFailure, so the job
// was never marked failed, never retried, and never persisted — it sat
// at StateRunning until its lease expired and got reaped, then re-leased
// to discover the identical, permanently unsatisfiable policy again, in
// an unbounded loop. Mutation check: reverting Execute to `return err`
// for terminalFor's error (bypassing handleFailure) makes this test fail
// with store.updates == 0 and State still StateRunning.
func TestRunner_UnsatisfiablePolicyReachesDLQ(t *testing.T) {
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	// No process-level executor registered: only the in-process default,
	// which cannot satisfy exec.LevelProcess.
	executors := exec.NewRegistry(inproc.New(reg))

	runner, store := newTestRunner(t, reg, executors)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", State: job.StateRunning, MaxRetries: 3}
	err := runner.Execute(context.Background(), j)
	if err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}
	if !errors.Is(err, exec.ErrNoExecutor) {
		t.Errorf("Execute() = %v, want it to wrap %v", err, exec.ErrNoExecutor)
	}
	if !errors.Is(err, dispatch.ErrPermanent) {
		t.Errorf("Execute() = %v, want it to wrap %v", err, dispatch.ErrPermanent)
	}
	if j.State != job.StateFailed {
		t.Errorf("State = %q, want %q — an unsatisfiable policy must reach a terminal state, not loop forever",
			j.State, job.StateFailed)
	}
	if j.LastError == "" {
		t.Error("LastError was never recorded")
	}
	if store.updates == 0 {
		t.Error("the job was never persisted — it would sit at StateRunning until its lease expired, forever")
	}
}

func TestRunner_HandlerErrorConsumesRetries(t *testing.T) {
	sentinel := errors.New("bad file")

	reg := job.NewRegistry()
	job.NewDefinition("test.job", func(context.Context, struct{}) error { return sentinel }).Register(reg)

	runner, _ := newTestRunner(t, reg, exec.NewRegistry(inproc.New(reg)))

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}
	if j.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", j.RetryCount)
	}
}

func TestRunner_PermanentHandlerErrorSkipsRetriesThroughAnExecutor(t *testing.T) {
	// The regression this guards: routing the attempt through an executor
	// must not cost the handler's error its identity, or ErrPermanent stops
	// being honoured for every engine user.
	reg := job.NewRegistry()
	job.NewDefinition("test.job", func(context.Context, struct{}) error {
		return fmt.Errorf("malformed payload: %w", dispatch.ErrPermanent)
	}).Register(reg)

	runner, _ := newTestRunner(t, reg, exec.NewRegistry(inproc.New(reg)))

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	err := runner.Execute(context.Background(), j)
	if err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}
	if j.State != job.StateFailed {
		t.Errorf("State = %q, want %q on the first attempt", j.State, job.StateFailed)
	}
	if j.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1 — the backoff schedule must not be spent", j.RetryCount)
	}
	if got, want := j.LastError, "malformed payload: dispatch: permanent failure"; got != want {
		t.Errorf("LastError = %q, want %q", got, want)
	}
	if !errors.Is(err, dispatch.ErrPermanent) {
		t.Errorf("errors.Is(Execute(), ErrPermanent) = false, want true (err = %v)", err)
	}
}

func TestRunner_PermanentResultFlagSkipsRetries(t *testing.T) {
	// The out-of-process shape of the same thing: no error chain crossed
	// the boundary, only the flag.
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	rec := &recordingExecutor{result: &exec.Result{
		Status:     exec.StatusHandlerError,
		HandlerErr: "the input object was deleted",
		Permanent:  true,
	}}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, _ := newTestRunner(t, reg, executors)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}
	if j.State != job.StateFailed {
		t.Errorf("State = %q, want %q on the first attempt", j.State, job.StateFailed)
	}
	if j.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", j.RetryCount)
	}
}

func TestRunner_UserErrorIdentitySurvivesTheExecutor(t *testing.T) {
	// What extensions receive: EmitJobFailed is handed this error, and an
	// extension matching its own error type on it must still succeed.
	reg := job.NewRegistry()
	job.NewDefinition("test.job", func(context.Context, struct{}) error {
		return fmt.Errorf("parse: %w", &userError{code: 42})
	}).Register(reg)

	runner, _ := newTestRunner(t, reg, exec.NewRegistry(inproc.New(reg)))

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 0}
	err := runner.Execute(context.Background(), j)
	if err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}

	var target *userError
	if !errors.As(err, &target) {
		t.Fatalf("errors.As(%v, **userError) = false, want true", err)
	}
	if target.code != 42 {
		t.Errorf("target.code = %d, want 42", target.code)
	}
	if !errors.Is(err, exec.ErrHandler) {
		t.Errorf("errors.Is(%v, ErrHandler) = false, want true", err)
	}
}

// userError stands in for an error type a caller or extension owns.
type userError struct{ code int }

func (e *userError) Error() string { return fmt.Sprintf("user error %d", e.code) }

func TestRunner_LaunchFailuresAreBounded(t *testing.T) {
	// A launch failure does not consume the retry budget, so without a
	// bound a job that can never launch requeues about once a second
	// forever, costing a store write and a worker slot each cycle.
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	rec := &recordingExecutor{
		result: &exec.Result{Status: exec.StatusLaunchFailed, HandlerErr: "image pull backoff"},
	}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, _ := newTestRunner(t, reg, executors)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}

	const wantRequeues = 5
	for attempt := 1; attempt <= wantRequeues; attempt++ {
		if err := runner.Execute(context.Background(), j); err == nil {
			t.Fatalf("attempt %d: Execute() = nil, want a failure", attempt)
		}
		if j.State != job.StatePending {
			t.Fatalf("attempt %d: State = %q, want %q", attempt, j.State, job.StatePending)
		}
		if j.RetryCount != 0 {
			t.Fatalf("attempt %d: RetryCount = %d, want 0", attempt, j.RetryCount)
		}
	}

	if err := runner.Execute(context.Background(), j); err == nil {
		t.Fatal("Execute() = nil after the launch cap, want a failure")
	}
	if j.State != job.StateFailed {
		t.Errorf("State = %q, want %q once the launch cap is exceeded", j.State, job.StateFailed)
	}
	if !strings.Contains(j.LastError, "failed to launch") {
		t.Errorf("LastError = %q, want it to explain the launch cap", j.LastError)
	}
}

func TestRunner_LaunchCounterIsForgottenOnceTheJobRuns(t *testing.T) {
	// The counter must not accumulate across a job's lifetime, or a job
	// that occasionally fails to launch would eventually be dead-lettered
	// for it. It also must not outlive the job in memory.
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	rec := &recordingExecutor{
		result: &exec.Result{Status: exec.StatusLaunchFailed, HandlerErr: "no capacity"},
	}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, _ := newTestRunner(t, reg, executors)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	for range 4 {
		if err := runner.Execute(context.Background(), j); err == nil {
			t.Fatal("Execute() = nil, want a launch failure")
		}
	}

	// The sandbox comes up and the job succeeds, which clears the count.
	rec.result = &exec.Result{Status: exec.StatusOK}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}

	// It may now fail to launch a full cap's worth again without being
	// dead-lettered for the failures that preceded the success.
	rec.result = &exec.Result{Status: exec.StatusLaunchFailed, HandlerErr: "no capacity"}
	for attempt := range 5 {
		if err := runner.Execute(context.Background(), j); err == nil {
			t.Fatalf("attempt %d: Execute() = nil, want a launch failure", attempt+1)
		}
		if j.State != job.StatePending {
			t.Fatalf("attempt %d: State = %q, want %q — the count was not reset",
				attempt+1, j.State, job.StatePending)
		}
	}
}

func TestRunner_RequestCarriesTheHandlerSetFingerprint(t *testing.T) {
	// Drift protection is only as good as the fingerprint being populated:
	// a rung handed an empty one either rejects every job or skips the
	// check, and a stale image then runs an old handler and reports
	// success.
	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)
	job.NewDefinition("other.job", func(context.Context, struct{}) error { return nil }).Register(reg)

	rec := &recordingExecutor{}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, _ := newTestRunner(t, reg, executors)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
	if rec.got == nil {
		t.Fatal("executor was not called")
	}
	if want := exec.Fingerprint(reg.Names()); rec.got.Fingerprint != want {
		t.Errorf("Request.Fingerprint = %q, want %q", rec.got.Fingerprint, want)
	}
}

func TestRunner_ReclaimAndCloseReachEveryExecutor(t *testing.T) {
	// Nothing called these before, so a rung with children to kill would
	// have leaked them on every restart and shutdown.
	reg := job.NewRegistry()
	rec := &recordingExecutor{}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner, _ := newTestRunner(t, reg, executors)

	if err := runner.Reclaim(context.Background(), id.NewWorkerID()); err != nil {
		t.Errorf("Reclaim() = %v, want nil", err)
	}
	if rec.reclaimed != 1 {
		t.Errorf("executor reclaimed %d times, want 1", rec.reclaimed)
	}
	if err := runner.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
	if rec.closed != 1 {
		t.Errorf("executor closed %d times, want 1", rec.closed)
	}
}

func TestNewExecutor_StillCompilesAndRuns(t *testing.T) {
	// The deprecated constructor must keep working for existing callers.
	reg := job.NewRegistry()
	job.NewDefinition("test.job", func(context.Context, struct{}) error { return nil }).Register(reg)

	e := worker.NewExecutor(
		reg,
		ext.NewRegistry(log.NewNoopLogger()),
		newFakeJobStore(),
		nil,
		backoff.NewExponential(time.Second, time.Hour),
		log.NewNoopLogger(),
	)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := e.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
	if j.State != job.StateCompleted {
		t.Errorf("State = %q, want %q", j.State, job.StateCompleted)
	}
}

// fakeJobStore is a job.Store that records UpdateJob calls. Only the
// method the runner uses does anything.
//
// DequeueJobs takes job.DequeueOpts rather than the brief's original
// (queues []string, limit int) — the Store interface's dequeue contract
// was widened by a concurrent change (see job/store.go) after the brief
// was written; this stub matches the current interface.
type fakeJobStore struct {
	updates int
}

func newFakeJobStore() *fakeJobStore { return &fakeJobStore{} }

func (f *fakeJobStore) UpdateJob(context.Context, *job.Job) error {
	f.updates++
	return nil
}

func (f *fakeJobStore) EnqueueJob(context.Context, *job.Job) error { return nil }

func (f *fakeJobStore) DequeueJobs(context.Context, job.DequeueOpts) ([]*job.Job, error) {
	return nil, nil
}

func (f *fakeJobStore) GetJob(context.Context, id.JobID) (*job.Job, error) { return nil, nil }

func (f *fakeJobStore) DeleteJob(context.Context, id.JobID) error { return nil }

func (f *fakeJobStore) ListJobsByState(
	context.Context, job.State, job.ListOpts,
) ([]*job.Job, error) {
	return nil, nil
}

func (f *fakeJobStore) HeartbeatJob(context.Context, id.JobID, id.WorkerID) error { return nil }

func (f *fakeJobStore) ReapStaleJobs(context.Context, time.Duration) ([]*job.Job, error) {
	return nil, nil
}

func (f *fakeJobStore) CountJobs(context.Context, job.CountOpts) (int64, error) { return 0, nil }
