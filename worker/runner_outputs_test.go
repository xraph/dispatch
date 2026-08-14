package worker_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/backoff"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/worker"
)

// scriptedExecutor is an exec.Executor whose Run writes preconfigured
// files into req.OutputDir and returns a preconfigured Result, without
// spawning any real subprocess or shim. It exists so the Runner's
// directory-lifecycle and output-committing plumbing can be tested
// without exec/subprocess or exec/shim in the loop.
type scriptedExecutor struct {
	level exec.Level

	// files are written into req.OutputDir before Run returns, keyed by
	// name with their content as the value.
	files map[string]string

	// claim becomes the returned Result.Outputs. It is deliberately
	// independent of files, so a test can make the sandbox lie: claim a
	// name it never wrote, omit one it did, or misreport a size.
	claim []exec.OutputFile

	status exec.Status

	got *exec.Request
}

func (e *scriptedExecutor) Name() string      { return "scripted" }
func (e *scriptedExecutor) Level() exec.Level { return e.level }

func (e *scriptedExecutor) Run(_ context.Context, req *exec.Request) (*exec.Result, error) {
	e.got = req

	for name, content := range e.files {
		if err := os.WriteFile(filepath.Join(req.OutputDir, name), []byte(content), 0o600); err != nil {
			return nil, err
		}
	}

	status := e.status
	if status == "" {
		status = exec.StatusOK
	}

	return &exec.Result{Status: status, Outputs: e.claim}, nil
}

func (e *scriptedExecutor) Reclaim(context.Context, id.WorkerID) error { return nil }
func (e *scriptedExecutor) Close() error                               { return nil }

// artifactPlane bundles the store, backend, and service a test wires
// into a Runner via WithArtifacts.
type artifactPlane struct {
	store   *memory.Store
	backend *artifacttest.Backend
	svc     *artifact.Service
}

func newArtifactPlane() *artifactPlane {
	s := memory.New()
	b := artifacttest.NewBackend()
	svc := artifact.NewService(s, b, artifact.WithDefaultBucket("dispatch"))

	return &artifactPlane{store: s, backend: b, svc: svc}
}

// seedPriorOutput records name as though attempt already committed it,
// so a later attempt's Runner.resolvePriorOutputs has something to find.
func (p *artifactPlane) seedPriorOutput(t *testing.T, jobID id.JobID, name string, attempt int) artifact.Ref {
	t.Helper()

	a := &artifact.Artifact{
		ID:        id.NewArtifactID(),
		Backend:   p.backend.Name(),
		Bucket:    "dispatch",
		Key:       "ephemeral/job/" + jobID.String() + "/" + strconv.Itoa(attempt) + "/" + name,
		Size:      int64(len(name)),
		Lifecycle: artifact.Ephemeral,
		CreatedAt: time.Now().UTC(),
	}
	link := &artifact.Link{
		ArtifactID: a.ID,
		OwnerKind:  artifact.OwnerJob,
		OwnerID:    jobID.String(),
		Role:       artifact.RoleOutput,
		Name:       name,
		Attempt:    attempt,
		CreatedAt:  time.Now().UTC(),
	}

	if err := p.store.CreateArtifact(context.Background(), a, link); err != nil {
		t.Fatalf("seed prior output %q: %v", name, err)
	}

	return a.Ref()
}

// newOutputsTestRunner builds a Runner wired to executors, with
// WithArtifacts applied only when plane is non-nil.
func newOutputsTestRunner(
	t *testing.T,
	reg *job.Registry,
	executors *exec.Registry,
	plane *artifactPlane,
) *worker.Runner {
	t.Helper()

	runner := worker.NewRunner(
		reg,
		ext.NewRegistry(log.NewNoopLogger()),
		newFakeJobStore(),
		nil,
		backoff.NewExponential(time.Second, time.Hour),
		executors,
		log.NewNoopLogger(),
	)

	if plane != nil {
		runner = runner.WithArtifacts(plane.svc, t.TempDir())
	}

	return runner
}

// isolatedJobRegistry registers "test.job", requiring LevelProcess
// isolation.
func isolatedJobRegistry(t *testing.T) *job.Registry {
	t.Helper()

	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	return reg
}

func TestRunner_OutOfProcessGetsFreshEmptyOutputDir(t *testing.T) {
	rec := &scriptedExecutor{level: exec.LevelProcess}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner := newOutputsTestRunner(t, reg, executors, nil)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}

	if rec.got == nil {
		t.Fatal("executor was not called")
	}
	if rec.got.OutputDir == "" {
		t.Fatal("Request.OutputDir was not set for an out-of-process rung")
	}
}

func TestRunner_InProcessGetsNoOutputDir(t *testing.T) {
	// A job with no declared isolation runs in-process and never reaches
	// an executor at all; nothing here should try to build it a scratch
	// directory.
	reg := job.NewRegistry()
	job.NewDefinition("plain.job", func(context.Context, struct{}) error { return nil }).Register(reg)

	runner := newOutputsTestRunner(t, reg, exec.NewRegistry(inproc.New(reg)), nil)

	j := &job.Job{ID: id.NewJobID(), Name: "plain.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
}

func TestRunner_PriorOutputsPopulatedWhenArtifactsEnabled(t *testing.T) {
	rec := &scriptedExecutor{level: exec.LevelProcess}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()

	jobID := id.NewJobID()
	wantA := plane.seedPriorOutput(t, jobID, "a.txt", 0)
	wantB := plane.seedPriorOutput(t, jobID, "b.txt", 0)
	// A second, later attempt re-committing "a.txt" must win over the
	// first — the same tie-break FindLinkByName applies for one name.
	wantALatest := plane.seedPriorOutput(t, jobID, "a.txt", 1)

	runner := newOutputsTestRunner(t, reg, executors, plane)

	j := &job.Job{ID: jobID, Name: "test.job", RetryCount: 2, MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}

	if rec.got == nil {
		t.Fatal("executor was not called")
	}

	got := make(map[string]artifact.Ref, len(rec.got.PriorOutputs))
	for _, po := range rec.got.PriorOutputs {
		got[po.Name] = po.Ref
	}

	if len(got) != 2 {
		t.Fatalf("PriorOutputs has %d entries, want 2: %+v", len(got), rec.got.PriorOutputs)
	}
	if got["a.txt"] != wantALatest {
		t.Errorf("PriorOutputs[a.txt] = %+v, want the attempt-1 ref %+v (not attempt-0 %+v)",
			got["a.txt"], wantALatest, wantA)
	}
	if got["b.txt"] != wantB {
		t.Errorf("PriorOutputs[b.txt] = %+v, want %+v", got["b.txt"], wantB)
	}
}

func TestRunner_PriorOutputsEmptyWhenArtifactsDisabled(t *testing.T) {
	rec := &scriptedExecutor{level: exec.LevelProcess}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	// No WithArtifacts call at all: the plane is off.
	runner := newOutputsTestRunner(t, reg, executors, nil)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}

	if rec.got == nil {
		t.Fatal("executor was not called")
	}
	if len(rec.got.PriorOutputs) != 0 {
		t.Errorf("PriorOutputs = %+v, want empty when the artifact plane is disabled", rec.got.PriorOutputs)
	}
}

func TestRunner_CommitsWhatIsActuallyOnDiskNotWhatIsClaimed(t *testing.T) {
	// The invariant this task exists to protect: the artifact store must
	// reflect the sandbox's actual filesystem, never its claims. "real.txt"
	// is written but under-claimed (the sandbox lies about its size);
	// "ghost.txt" is claimed but never written at all.
	rec := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{"real.txt": "hello world"},
		claim: []exec.OutputFile{
			{Name: "real.txt", Size: 999999},
			{Name: "ghost.txt", Size: 12},
		},
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()
	runner := newOutputsTestRunner(t, reg, executors, plane)

	jobID := id.NewJobID()
	j := &job.Job{ID: jobID, Name: "test.job", RetryCount: 0, MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()}
	links, err := plane.store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}

	byName := make(map[string]*artifact.Link, len(links))
	for _, l := range links {
		byName[l.Name] = l
	}

	if _, ok := byName["ghost.txt"]; ok {
		t.Error("an artifact row was created for \"ghost.txt\", which the sandbox claimed but never wrote")
	}

	link, ok := byName["real.txt"]
	if !ok {
		t.Fatal("no artifact row was created for \"real.txt\", which the sandbox actually wrote")
	}

	a, err := plane.svc.Get(context.Background(), link.ArtifactID)
	if err != nil {
		t.Fatalf("Get(%s): %v", link.ArtifactID, err)
	}
	if want := int64(len("hello world")); a.Size != want {
		t.Errorf("committed artifact Size = %d, want %d (the real content length, not the claimed 999999)", a.Size, want)
	}
	if !plane.backend.Has(a.Bucket, a.Key) {
		t.Error("the committed artifact's bytes are not present in the backend")
	}
}

func TestRunner_SkipsCommittingWhenArtifactsDisabled(t *testing.T) {
	rec := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{"real.txt": "hello"},
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	// No WithArtifacts call: nothing should be committed, and Execute
	// must not fail because of it.
	runner := newOutputsTestRunner(t, reg, executors, nil)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
}

func TestRunner_DoesNotCommitOutputsOnFailure(t *testing.T) {
	rec := &scriptedExecutor{
		level:  exec.LevelProcess,
		files:  map[string]string{"partial.txt": "unfinished"},
		status: exec.StatusHandlerError,
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()
	runner := newOutputsTestRunner(t, reg, executors, plane)

	jobID := id.NewJobID()
	j := &job.Job{ID: jobID, Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()}
	links, err := plane.store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}
	if len(links) != 0 {
		t.Errorf("ListLinks = %+v, want none — a failed attempt must not commit anything", links)
	}
}

func TestRunner_RemovesScratchDirOnSuccess(t *testing.T) {
	rec := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{"out.txt": "done"},
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner := newOutputsTestRunner(t, reg, executors, newArtifactPlane())

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}

	if rec.got == nil {
		t.Fatal("executor was not called")
	}
	if _, err := os.Stat(rec.got.OutputDir); !os.IsNotExist(err) {
		t.Errorf("OutputDir %q still exists after a successful attempt (stat err = %v)", rec.got.OutputDir, err)
	}
}

func TestRunner_RemovesScratchDirOnFailure(t *testing.T) {
	rec := &scriptedExecutor{
		level:  exec.LevelProcess,
		status: exec.StatusHandlerError,
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner := newOutputsTestRunner(t, reg, executors, nil)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err == nil {
		t.Fatal("Execute() = nil, want a failure")
	}

	if rec.got == nil {
		t.Fatal("executor was not called")
	}
	if _, err := os.Stat(rec.got.OutputDir); !os.IsNotExist(err) {
		t.Errorf("OutputDir %q still exists after a failed attempt (stat err = %v)", rec.got.OutputDir, err)
	}
}

func TestRunner_ScratchDirIsEmptyWhenHandedToTheExecutor(t *testing.T) {
	var sawEntries []string

	reg := job.NewRegistry()
	job.NewDefinition("test.job",
		func(context.Context, struct{}) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	).Register(reg)

	rec := &recordingEmptyDirExecutor{seen: &sawEntries}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner := newOutputsTestRunner(t, reg, executors, nil)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}

	if len(sawEntries) != 0 {
		t.Errorf("OutputDir contained %v when handed to the executor, want empty", sawEntries)
	}
}

// recordingEmptyDirExecutor records the directory entries it finds in
// req.OutputDir at the moment Run is called, into *seen.
type recordingEmptyDirExecutor struct {
	seen *[]string
}

func (e *recordingEmptyDirExecutor) Name() string      { return "recording-empty-dir" }
func (e *recordingEmptyDirExecutor) Level() exec.Level { return exec.LevelProcess }

func (e *recordingEmptyDirExecutor) Run(_ context.Context, req *exec.Request) (*exec.Result, error) {
	entries, err := os.ReadDir(req.OutputDir)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	*e.seen = names

	return &exec.Result{Status: exec.StatusOK}, nil
}

func (e *recordingEmptyDirExecutor) Reclaim(context.Context, id.WorkerID) error { return nil }
func (e *recordingEmptyDirExecutor) Close() error                               { return nil }
