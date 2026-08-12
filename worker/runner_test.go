package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/worker"
)

// recordingExecutor captures the Request the runner built.
type recordingExecutor struct {
	got    *exec.Request
	result *exec.Result
	err    error
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

func (r *recordingExecutor) Reclaim(context.Context, id.WorkerID) error { return nil }
func (r *recordingExecutor) Close() error                               { return nil }

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
		ID:         id.NewJobID(),
		Name:       "test.job",
		Payload:    []byte(`{"a":1}`),
		RetryCount: 2,
		MaxRetries: 3,
		ScopeAppID: "app_1",
		ScopeOrgID: "org_1",
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
