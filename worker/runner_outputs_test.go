package worker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
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
	// name with their content as the value. Keys may contain "/" to
	// place a file in a subdirectory, which is created as needed.
	files map[string]string

	// symlinks are created in req.OutputDir before Run returns, keyed
	// by the link's name with its target as the value. The target is an
	// absolute path, standing in for anything on the worker's own
	// filesystem a compromised handler might point at — its own
	// config, a credential file — since a symlink's target is not
	// confined to OutputDir at all.
	symlinks map[string]string

	// claim becomes the returned Result.Outputs. It is deliberately
	// independent of files, so a test can make the sandbox lie: claim a
	// name it never wrote, omit one it did, or misreport a size.
	claim []exec.OutputFile

	status exec.Status

	// beforeReturn, when set, runs after files/symlinks are written but
	// before Run returns its Result — the point in time a real
	// out-of-process rung's Run has already finished but the worker has
	// not yet started committing. It exists so a test can simulate the
	// pool's heartbeat loop cancelling the job's context in that exact
	// window, the race commitOutputs' fence gate exists to catch.
	beforeReturn func()

	got *exec.Request
}

func (e *scriptedExecutor) Name() string      { return "scripted" }
func (e *scriptedExecutor) Level() exec.Level { return e.level }

func (e *scriptedExecutor) Run(_ context.Context, req *exec.Request) (*exec.Result, error) {
	e.got = req

	for name, content := range e.files {
		full := filepath.Join(req.OutputDir, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			return nil, err
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			return nil, err
		}
	}

	for name, target := range e.symlinks {
		if err := os.Symlink(target, filepath.Join(req.OutputDir, name)); err != nil {
			return nil, err
		}
	}

	if e.beforeReturn != nil {
		e.beforeReturn()
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

// flakyBackend wraps another artifact.Backend, failing Create for any
// key containing failSubstring a configurable number of times before
// behaving normally — a test double standing in for a transient
// backend outage that clears on its own.
type flakyBackend struct {
	inner         artifact.Backend
	failSubstring string
	failN         int
}

var _ artifact.Backend = (*flakyBackend)(nil)

func (b *flakyBackend) Name() string { return b.inner.Name() }

func (b *flakyBackend) Open(ctx context.Context, ref artifact.Ref) (io.ReadCloser, error) {
	return b.inner.Open(ctx, ref)
}

func (b *flakyBackend) Stat(ctx context.Context, ref artifact.Ref) (artifact.ObjectInfo, error) {
	return b.inner.Stat(ctx, ref)
}

func (b *flakyBackend) Delete(ctx context.Context, ref artifact.Ref) error {
	return b.inner.Delete(ctx, ref)
}

func (b *flakyBackend) Create(ctx context.Context, bucket, key string) (artifact.Writer, error) {
	if b.failN > 0 && strings.Contains(key, b.failSubstring) {
		b.failN--

		return nil, errors.New("simulated transient backend outage")
	}

	return b.inner.Create(ctx, bucket, key)
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

// TestRunner_SkipsSymlinksInOutputDir is C1: a compromised handler can
// place a symlink anywhere in its own writable OutputDir pointing at
// anything the worker process itself can read — its config, cloud
// credentials, a mounted service-account token. If the worker ever
// opened what that symlink resolves to and committed the bytes, a
// symlink to any file the worker can read becomes an ordinary,
// downloadable job output. This proves it does not: the secret's bytes
// never appear anywhere in the backend, and no artifact row is created
// for the symlink's name.
func TestRunner_SkipsSymlinksInOutputDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating a symlink requires elevated privilege on windows")
	}

	secretPath := filepath.Join(t.TempDir(), "credentials")
	const secret = "AKIA-SUPER-SECRET-WORKER-CREDENTIAL"
	if err := os.WriteFile(secretPath, []byte(secret), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}

	rec := &scriptedExecutor{
		level:    exec.LevelProcess,
		files:    map[string]string{"real.txt": "legitimate output"},
		symlinks: map[string]string{"innocent.txt": secretPath},
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()
	runner := newOutputsTestRunner(t, reg, executors, plane)

	jobID := id.NewJobID()
	j := &job.Job{ID: jobID, Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil — a symlink must be skipped, not fail the attempt", err)
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()}
	links, err := plane.store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}

	for _, l := range links {
		if l.Name == "innocent.txt" {
			t.Errorf("an artifact row was created for the symlink %q", l.Name)
		}

		a, getErr := plane.svc.Get(context.Background(), l.ArtifactID)
		if getErr != nil {
			t.Fatalf("Get(%s): %v", l.ArtifactID, getErr)
		}

		rc, openErr := plane.svc.Open(context.Background(), a.Ref())
		if openErr != nil {
			t.Fatalf("Open(%s): %v", l.ArtifactID, openErr)
		}
		buf := make([]byte, len(secret))
		_, _ = rc.Read(buf)
		_ = rc.Close()

		if strings.Contains(string(buf), secret) {
			t.Fatalf("the worker's secret leaked into committed artifact %q (name %q)", l.ArtifactID, l.Name)
		}
	}

	if len(links) != 1 || links[0].Name != "real.txt" {
		t.Errorf("committed links = %+v, want exactly [real.txt]", links)
	}
}

// TestRunner_SkipsNonRegularFilesInOutputDir is a cross-platform
// smoke test alongside the genuine hang reproduction for C2: platform-
// portable FIFO creation lives in the unix-only fifo_unix_test.go
// (TestRunner_OpeningAFIFOWouldHangSoItIsNeverOpened), which is where
// C2's actual measured hang is proven fixed. This one just confirms an
// ordinary, unremarkable attempt with a plain file still behaves,
// exercising the same code path this file's other tests changed.
func TestRunner_SkipsNonRegularFilesInOutputDir(t *testing.T) {
	rec := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{"real.txt": "kept"},
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()
	runner := newOutputsTestRunner(t, reg, executors, plane)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}
}

// TestRunner_HiddenDirectoryContentsAreNotCommitted is m7: a
// dot-prefixed entry must be skipped whole, directory contents
// included — returning bare nil for a hidden directory does not stop
// WalkDir from descending into it, only fs.SkipDir does, so a file
// living inside a hidden directory must not slip through and get
// committed under its own (non-hidden) name.
func TestRunner_HiddenDirectoryContentsAreNotCommitted(t *testing.T) {
	rec := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{
			"visible.txt":            "kept",
			".hidden/leaked.txt":     "must not be committed",
			".hidden/sub/deeper.txt": "must not be committed either",
		},
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()
	runner := newOutputsTestRunner(t, reg, executors, plane)

	jobID := id.NewJobID()
	j := &job.Job{ID: jobID, Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err != nil {
		t.Fatalf("Execute() = %v, want nil", err)
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()}
	links, err := plane.store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}

	for _, l := range links {
		if l.Name != "visible.txt" {
			t.Errorf("committed a link from inside the hidden directory: %+v", l)
		}
	}
	if len(links) != 1 {
		t.Errorf("committed links = %+v, want exactly [visible.txt]", links)
	}
}

// TestRunner_RejectsDuplicateOutputNamesWithoutPartialCommits is C4 and
// C5 together: two files at different paths that would both commit as
// the same base name ("report.csv", nested under "us/" and "eu/") must
// fail the attempt cleanly, with zero partial commits left behind, and
// without consuming the job's real retry budget — a deterministic
// naming collision fails identically on every attempt, so retrying it
// normally would only burn the whole schedule for nothing.
func TestRunner_RejectsDuplicateOutputNamesWithoutPartialCommits(t *testing.T) {
	rec := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{
			"us/report.csv": "us data",
			"eu/report.csv": "eu data",
		},
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()
	runner := newOutputsTestRunner(t, reg, executors, plane)

	jobID := id.NewJobID()
	j := &job.Job{ID: jobID, Name: "test.job", RetryCount: 0, MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err == nil {
		t.Fatal("Execute() = nil, want a failure for a duplicate output name")
	}

	if j.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0 — a structural naming collision must not consume the real retry budget",
			j.RetryCount)
	}
	if j.State != job.StatePending {
		t.Errorf("State = %q, want %q (requeued via the bounded launch-failure path)", j.State, job.StatePending)
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()}
	links, err := plane.store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}
	if len(links) != 0 {
		t.Errorf("ListLinks = %+v, want none — a rejected commit must leave nothing behind", links)
	}
}

// TestRunner_DoesNotCommitOutputsWhenLeaseFenceIsAlreadyLost is C3's
// gate half. It simulates the pool's heartbeat loop cancelling the
// job's context with job.ErrLeaseLost in the exact window C3 measured:
// after the sandbox has already finished and is about to be reported a
// success, but before the worker has committed anything. A fenced-out
// attempt must not commit outputs as though it still owned the job.
func TestRunner_DoesNotCommitOutputsWhenLeaseFenceIsAlreadyLost(t *testing.T) {
	baseCtx, cancel := context.WithCancelCause(context.Background())

	rec := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{"out.txt": "should never be committed"},
		beforeReturn: func() {
			// Exactly what Pool.sendHeartbeats does on a lost lease
			// (cancelJob), except fired here, mid-attempt, instead of
			// from a concurrent heartbeat goroutine — deterministic
			// rather than timing-dependent, testing the same race.
			cancel(job.ErrLeaseLost)
		},
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()
	leaseStore := &fakeLeaseJobStore{fakeJobStore: newFakeJobStore()}

	runner := worker.NewRunner(
		reg, ext.NewRegistry(log.NewNoopLogger()), leaseStore, nil,
		backoff.NewExponential(time.Second, time.Hour), executors, log.NewNoopLogger(),
	).WithArtifacts(plane.svc, t.TempDir())

	jobID := id.NewJobID()
	j := &job.Job{ID: jobID, Name: "test.job", MaxRetries: 3}
	ctx := worker.WithLeaseFenceForTest(baseCtx, leaseStore, id.NewWorkerID(), 5)

	if err := runner.Execute(ctx, j); err == nil {
		t.Fatal("Execute() = nil, want a failure once the lease fence is gone")
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()}
	links, err := plane.store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}
	if len(links) != 0 {
		t.Errorf("ListLinks = %+v, want none — a fenced-out attempt must not commit anything", links)
	}
}

// TestRunner_TwoHoldersSameAttempt_DifferentEpochsDoNotCollide is C3's
// key half. It reproduces the shape of the race directly: two workers
// each believe they hold the same job at the same RetryCount at once —
// a lease reclaim racing a zombie that has not yet noticed its lease
// expired — and both commit an output under the identical name. Before
// the fix this silently overwrote the earlier commit's backend bytes
// out from under its own still-valid artifact row (see the report for
// the mutation-tested proof); after it, distinct lease epochs give each
// holder its own backend object, so both commits survive with their own
// correct, uncorrupted bytes.
func TestRunner_TwoHoldersSameAttempt_DifferentEpochsDoNotCollide(t *testing.T) {
	plane := newArtifactPlane()
	leaseStore := &fakeLeaseJobStore{fakeJobStore: newFakeJobStore()}
	jobID := id.NewJobID()

	run := func(epoch int, content string) {
		t.Helper()

		reg := isolatedJobRegistry(t)
		rec := &scriptedExecutor{level: exec.LevelProcess, files: map[string]string{"out.txt": content}}
		executors := exec.NewRegistry(inproc.New(reg))
		executors.Add(rec)

		runner := worker.NewRunner(
			reg, ext.NewRegistry(log.NewNoopLogger()), leaseStore, nil,
			backoff.NewExponential(time.Second, time.Hour), executors, log.NewNoopLogger(),
		).WithArtifacts(plane.svc, t.TempDir())

		j := &job.Job{ID: jobID, Name: "test.job", RetryCount: 0, MaxRetries: 3}
		ctx := worker.WithLeaseFenceForTest(context.Background(), leaseStore, id.NewWorkerID(), epoch)

		if err := runner.Execute(ctx, j); err != nil {
			t.Fatalf("Execute() (epoch %d) = %v, want nil", epoch, err)
		}
	}

	// The winner claims a fresh epoch after reclaiming the job; the
	// zombie is still running under the epoch it was originally granted
	// and finishes afterward, unaware it has already been reclaimed —
	// order matches what C3 measured: the loser finishes second.
	run(6, "WINNER-BYTES")
	run(5, "LOSER-BYTES-THAT-WOULD-OVERWRITE-THE-WINNERS")

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()}
	links, err := plane.store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}

	outLinks := make([]*artifact.Link, 0, len(links))
	for _, l := range links {
		if l.Name == "out.txt" {
			outLinks = append(outLinks, l)
		}
	}
	if len(outLinks) != 2 {
		t.Fatalf("out.txt links = %d, want 2 — each holder's commit must survive as its own row", len(outLinks))
	}

	gotContents := make(map[string]bool)
	seenKeys := make(map[string]bool)
	for _, l := range outLinks {
		a, getErr := plane.svc.Get(context.Background(), l.ArtifactID)
		if getErr != nil {
			t.Fatalf("Get(%s): %v", l.ArtifactID, getErr)
		}

		if seenKeys[a.Key] {
			t.Errorf("two holders resolved to the identical backend key %q", a.Key)
		}
		seenKeys[a.Key] = true

		rc, openErr := plane.svc.Open(context.Background(), a.Ref())
		if openErr != nil {
			t.Fatalf("Open(%s): %v", l.ArtifactID, openErr)
		}
		buf := make([]byte, 128)
		n, _ := rc.Read(buf)
		_ = rc.Close()

		gotContents[string(buf[:n])] = true
	}

	if !gotContents["WINNER-BYTES"] {
		t.Error("the winner's bytes (epoch 6) were not found intact — something overwrote or lost them")
	}
	if !gotContents["LOSER-BYTES-THAT-WOULD-OVERWRITE-THE-WINNERS"] {
		t.Error("the zombie's bytes (epoch 5) were not found intact — its commit did not survive as its own row")
	}
}

// deadPID starts and waits out a trivial child process, returning its
// PID once the OS has reaped it — a PID this test can be confident
// names no running process, unlike a made-up small integer that might
// coincidentally belong to something on the test machine.
func deadPID(t *testing.T) int {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("no portable trivial no-op command on windows")
	}

	cmd := osexec.CommandContext(context.Background(), "true")
	if err := cmd.Run(); err != nil {
		t.Fatalf("run trivial child process: %v", err)
	}

	return cmd.Process.Pid
}

// TestRunner_ReclaimSweepsOnlyScratchDirsWithNoLiveOwner is finding 4
// from the second review round: age alone cannot tell a stale scratch
// directory from a live one, since a long attempt rewriting the files
// inside it never advances the directory's OWN mtime, and a sibling
// worker process sharing the same scratch root looks exactly like a
// dead one to a purely age-based sweep. Ownership — an embedded PID
// this process can ask the OS about — is what actually distinguishes
// them: a directory whose owning PID is provably gone is swept
// regardless of how old or new it looks; one whose owning PID is alive
// is left alone regardless of how old it looks.
func TestRunner_ReclaimSweepsOnlyScratchDirsWithNoLiveOwner(t *testing.T) {
	dead := deadPID(t)
	root := t.TempDir()

	// Owned by a PID that is definitely gone, and old — the case the
	// sweep exists for.
	staleDead := filepath.Join(root, fmt.Sprintf("dispatch-out-%d-job-abc-000000", dead))
	if err := os.Mkdir(staleDead, 0o750); err != nil {
		t.Fatalf("mkdir staleDead: %v", err)
	}
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(staleDead, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes staleDead: %v", err)
	}

	// Owned by THIS test process's own PID (definitely alive), but
	// backdated to look exactly as old as staleDead — proving liveness
	// overrides age, not merely that a fresh mtime is spared.
	oldButAlive := filepath.Join(root, fmt.Sprintf("dispatch-out-%d-job-def-111111", os.Getpid()))
	if err := os.Mkdir(oldButAlive, 0o750); err != nil {
		t.Fatalf("mkdir oldButAlive: %v", err)
	}
	if err := os.Chtimes(oldButAlive, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes oldButAlive: %v", err)
	}

	// Owned by a dead PID but too fresh to have cleared the courtesy
	// grace window — must survive even though its owner is gone.
	freshDead := filepath.Join(root, fmt.Sprintf("dispatch-out-%d-job-ghi-222222", dead))
	if err := os.Mkdir(freshDead, 0o750); err != nil {
		t.Fatalf("mkdir freshDead: %v", err)
	}

	// Does not match the expected name shape at all — never ours,
	// regardless of age.
	unrelated := filepath.Join(root, "not-ours-at-all")
	if err := os.Mkdir(unrelated, 0o750); err != nil {
		t.Fatalf("mkdir unrelated: %v", err)
	}
	if err := os.Chtimes(unrelated, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes unrelated: %v", err)
	}

	reg := job.NewRegistry()
	runner := worker.NewRunner(
		reg, ext.NewRegistry(log.NewNoopLogger()), newFakeJobStore(), nil,
		backoff.NewExponential(time.Second, time.Hour), exec.NewRegistry(inproc.New(reg)), log.NewNoopLogger(),
	).WithArtifacts(newArtifactPlane().svc, root)

	if err := runner.Reclaim(context.Background(), id.NewWorkerID()); err != nil {
		t.Fatalf("Reclaim() = %v, want nil", err)
	}

	if _, err := os.Stat(staleDead); !os.IsNotExist(err) {
		t.Errorf("dead-owner, stale scratch dir still exists after Reclaim (stat err = %v)", err)
	}
	if _, err := os.Stat(oldButAlive); err != nil {
		t.Errorf("live-owner scratch dir was removed by Reclaim despite an old mtime: %v", err)
	}
	if _, err := os.Stat(freshDead); err != nil {
		t.Errorf("dead-owner scratch dir was removed before clearing the grace window: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Errorf("an unrelated directory was removed by Reclaim: %v", err)
	}
}

// TestRunner_ReclaimSweepsWithoutAnArtifactPlaneToo pins the corrected
// behaviour from the comment/doc drift sweep: prepareOutputDir creates a
// scratch directory for ANY out-of-process attempt regardless of
// whether this Runner has an artifact plane configured (see
// terminalFor — only PriorOutputs and committing are gated on
// r.artifacts, not scratch-directory creation itself, exactly as
// WithArtifacts' own doc comment says). A Runner with no artifact plane
// therefore leaks the identical "dispatch-out-…" directories on a crash
// as one with a plane configured, and Reclaim's sweep has to run for it
// too or those directories are never reclaimed at all — the exact gap
// an earlier version of this test enshrined as intentional by asserting
// the opposite.
//
// scratchRoot only has one setter — WithArtifacts — so a Runner with no
// artifact plane at all necessarily also has no configured scratchRoot
// and falls back to the process's real os.TempDir(). That is the exact
// case this test exercises: nothing here is a fabricated test-only
// path, it is what sweepStaleScratchDirs actually resolves to when a
// bare Runner (no WithArtifacts) calls Reclaim. TMPDIR is redirected via
// t.Setenv so the test can observe it without touching the real system
// temp directory.
func TestRunner_ReclaimSweepsWithoutAnArtifactPlaneToo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("os.TempDir() does not read TMPDIR on windows")
	}

	dead := deadPID(t)
	root := t.TempDir()
	t.Setenv("TMPDIR", root)

	if got := os.TempDir(); got != root {
		t.Fatalf("os.TempDir() = %q after redirecting TMPDIR, want %q — test setup is not exercising what it thinks",
			got, root)
	}

	staleDead := filepath.Join(root, fmt.Sprintf("dispatch-out-%d-job-abc-000000", dead))
	if err := os.Mkdir(staleDead, 0o750); err != nil {
		t.Fatalf("mkdir staleDead: %v", err)
	}
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(staleDead, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes staleDead: %v", err)
	}

	// A live-owner directory in the same root must still survive even
	// with no artifact plane configured: ownership, not the presence of
	// an artifact plane, is what protects a sibling's in-use directory.
	oldButAlive := filepath.Join(root, fmt.Sprintf("dispatch-out-%d-job-def-111111", os.Getpid()))
	if err := os.Mkdir(oldButAlive, 0o750); err != nil {
		t.Fatalf("mkdir oldButAlive: %v", err)
	}
	if err := os.Chtimes(oldButAlive, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes oldButAlive: %v", err)
	}

	reg := job.NewRegistry()
	// No WithArtifacts call at all, so r.scratchRoot is unset and falls
	// back to os.TempDir() — exactly the configuration the sweep must
	// still reach into now.
	runner := worker.NewRunner(
		reg, ext.NewRegistry(log.NewNoopLogger()), newFakeJobStore(), nil,
		backoff.NewExponential(time.Second, time.Hour), exec.NewRegistry(inproc.New(reg)), log.NewNoopLogger(),
	)

	if err := runner.Reclaim(context.Background(), id.NewWorkerID()); err != nil {
		t.Fatalf("Reclaim() = %v, want nil", err)
	}

	if _, err := os.Stat(staleDead); !os.IsNotExist(err) {
		t.Errorf("a Runner with no artifact plane left a dead-owner scratch dir behind (stat err = %v) — "+
			"it leaks the identical directories a Runner with a plane does and must sweep them too", err)
	}
	if _, err := os.Stat(oldButAlive); err != nil {
		t.Errorf("a live-owner scratch dir was removed despite no artifact plane being configured: %v", err)
	}
}

// TestRunner_MissingOutputDirIsALoudFailureNotEmptySuccess is finding
// 4's third requirement, independent of the sweep itself: a vanished
// OutputDir must never be read as "the handler produced nothing." That
// distinction matters most exactly when something else — a sibling
// process's sweep, however it decides staleness — has removed a still
// in-use OutputDir out from under a running attempt: silently reporting
// success with zero artifacts would be worse than any error.
func TestRunner_MissingOutputDirIsALoudFailureNotEmptySuccess(t *testing.T) {
	rec := &scriptedExecutor{level: exec.LevelProcess}
	rec.beforeReturn = func() {
		// Simulates OutputDir having been removed by something else
		// entirely — a sibling process's sweep, or the sandbox itself
		// misbehaving — between the sandbox finishing and the worker
		// walking it.
		if err := os.RemoveAll(rec.got.OutputDir); err != nil {
			t.Fatalf("remove OutputDir: %v", err)
		}
	}

	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()
	runner := newOutputsTestRunner(t, reg, executors, plane)

	j := &job.Job{ID: id.NewJobID(), Name: "test.job", MaxRetries: 3}
	if err := runner.Execute(context.Background(), j); err == nil {
		t.Fatal("Execute() = nil, want a failure — a missing OutputDir must never look like success")
	}

	if j.State == job.StateCompleted {
		t.Errorf("State = %q, want anything but completed — nothing was actually committed", j.State)
	}
}

// TestRunner_PartialCommitSurvivesForRetryToConverge is findings 2 and
// 3 from the second review round, verified together: a transient
// backend failure partway through committing outputs must not roll
// back what already landed (finding 2 — a rollback would delete bytes
// PriorOutputs still points a retried handler at, actively telling it
// to skip regenerating data that no longer exists), and a retry of the
// SAME attempt — a commit failure is classified StatusLaunchFailed, so
// RetryCount does not advance and the retry reuses the identical
// (owner, attempt) key namespace — must actually converge once the
// failure clears, even when the handler itself is not careful enough
// to consult PriorOutputs and skip regenerating a name it already
// produced (finding 3's "any store without lease grants" case, where
// nothing about a retry changes the key at all).
func TestRunner_PartialCommitSurvivesForRetryToConverge(t *testing.T) {
	store := memory.New()
	flaky := &flakyBackend{inner: artifacttest.NewBackend(), failSubstring: "b-second.txt", failN: 1}
	svc := artifact.NewService(store, flaky, artifact.WithDefaultBucket("dispatch"))

	reg := isolatedJobRegistry(t)
	rec := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{"a-first.txt": "AAA", "b-second.txt": "BBB"},
	}
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	runner := worker.NewRunner(
		reg, ext.NewRegistry(log.NewNoopLogger()), newFakeJobStore(), nil,
		backoff.NewExponential(time.Second, time.Hour), executors, log.NewNoopLogger(),
	).WithArtifacts(svc, t.TempDir())

	jobID := id.NewJobID()
	j := &job.Job{ID: jobID, Name: "test.job", RetryCount: 0, MaxRetries: 3}

	// Pass 1: "a-first.txt" commits; "b-second.txt" hits the simulated
	// outage, failing the whole attempt. It must requeue via the bounded
	// launch-failure path — RetryCount must not advance.
	if err := runner.Execute(context.Background(), j); err == nil {
		t.Fatal("Execute() (pass 1) = nil, want a failure from the simulated outage")
	}
	if j.RetryCount != 0 {
		t.Fatalf("RetryCount after pass 1 = %d, want 0 (a commit failure must not consume the real retry budget)",
			j.RetryCount)
	}
	if j.State != job.StatePending {
		t.Fatalf("State after pass 1 = %q, want %q", j.State, job.StatePending)
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()}
	linksAfterPass1, err := store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks after pass 1: %v", err)
	}
	if len(linksAfterPass1) != 1 || linksAfterPass1[0].Name != "a-first.txt" {
		t.Fatalf("links after pass 1 = %+v, want exactly [a-first.txt] (not rolled back)", linksAfterPass1)
	}

	// Finding 2's specific check: a-first.txt's bytes must still be
	// genuinely openable — a rollback would have deleted them out from
	// under the very link a retried handler's PriorOutputs check relies
	// on.
	firstArtifact, err := svc.Get(context.Background(), linksAfterPass1[0].ArtifactID)
	if err != nil {
		t.Fatalf("Get(a-first.txt) after pass 1: %v", err)
	}
	rc, err := svc.Open(context.Background(), firstArtifact.Ref())
	if err != nil {
		t.Fatalf("Open(a-first.txt) after pass 1: %v — a rollback would have deleted these bytes", err)
	}
	buf := make([]byte, 16)
	n, _ := rc.Read(buf)
	_ = rc.Close()
	if got := string(buf[:n]); got != "AAA" {
		t.Errorf("a-first.txt bytes after pass 1 = %q, want %q", got, "AAA")
	}
	firstArtifactID := firstArtifact.ID

	// Pass 2 reuses the same job object (same RetryCount, same attempt).
	// The handler is deliberately NOT idempotent here — it rewrites BOTH
	// files again, including the one that already succeeded — to prove
	// convergence does not depend on the handler consulting PriorOutputs
	// on its own. The simulated outage has cleared (failN is exhausted).
	rec2 := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{"a-first.txt": "AAA", "b-second.txt": "BBB"},
	}
	executors.Add(rec2)

	if execErr := runner.Execute(context.Background(), j); execErr != nil {
		t.Fatalf("Execute() (pass 2) = %v, want nil — the outage cleared", execErr)
	}
	if j.State != job.StateCompleted {
		t.Errorf("State after pass 2 = %q, want %q", j.State, job.StateCompleted)
	}

	// The other half of finding 2: rec2 must have SEEN a-first.txt in
	// PriorOutputs, pointing at pass 1's own still-valid artifact.
	foundPrior := false
	for _, po := range rec2.got.PriorOutputs {
		if po.Name != "a-first.txt" {
			continue
		}
		foundPrior = true
		if po.Ref.ID != firstArtifactID {
			t.Errorf("PriorOutputs[a-first.txt].Ref.ID = %s, want %s (pass 1's own artifact)",
				po.Ref.ID, firstArtifactID)
		}
	}
	if !foundPrior {
		t.Error("PriorOutputs on pass 2 did not include a-first.txt from pass 1's partial success")
	}

	linksAfterPass2, err := store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks after pass 2: %v", err)
	}
	byName := make(map[string]*artifact.Link, len(linksAfterPass2))
	for _, l := range linksAfterPass2 {
		byName[l.Name] = l
	}
	if len(byName) != 2 {
		t.Fatalf("links after pass 2 = %+v, want exactly [a-first.txt, b-second.txt]", linksAfterPass2)
	}

	// Finding 3's core assertion: a-first.txt was recognised as
	// already-committed and skipped, not re-written under a fresh
	// artifact — the SAME artifact ID both times.
	if byName["a-first.txt"].ArtifactID != firstArtifactID {
		t.Errorf("a-first.txt was re-committed on pass 2 (new artifact %s), want the SAME artifact %s from pass 1",
			byName["a-first.txt"].ArtifactID, firstArtifactID)
	}
}

// TestRunner_PlainCancellationDuringCommitIsNotLeaseLoss is finding 1
// from the second review round: only a definitive job.ErrLeaseLost
// from the pool's heartbeat loop may be classified as lease-fence loss.
// A plain cancellation for an unrelated reason — a graceful shutdown
// mid-commit is the obvious one — must not be reported as "lease fence
// lost," since that label determines which retry path (and therefore
// which store write, fenced or not) the resulting failure takes.
func TestRunner_PlainCancellationDuringCommitIsNotLeaseLoss(t *testing.T) {
	baseCtx, cancel := context.WithCancelCause(context.Background())

	rec := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{"out.txt": "unrelated cancellation"},
		beforeReturn: func() {
			cancel(context.Canceled) // deliberately NOT job.ErrLeaseLost
		},
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()
	runner := newOutputsTestRunner(t, reg, executors, plane)

	jobID := id.NewJobID()
	j := &job.Job{ID: jobID, Name: "test.job", MaxRetries: 3}

	if err := runner.Execute(baseCtx, j); err != nil {
		t.Fatalf("Execute() = %v, want nil — a plain cancellation must not gate the commit", err)
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()}
	links, err := plane.store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}
	if len(links) != 1 || links[0].Name != "out.txt" {
		t.Errorf("links = %+v, want exactly [out.txt] — a plain cancellation wrongly gated on as lease loss "+
			"would have skipped committing it", links)
	}
}
