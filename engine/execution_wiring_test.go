package engine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
)

// scratchWritingExecutor is a minimal exec.LevelProcess executor. It does
// not launch a real OS subprocess — that round trip needs a re-exec'd
// shim binary and is already covered by exec/subprocess's own tests and
// exec/exectest's shared conformance suite — but it is a genuine
// exec.LevelProcess implementation, so it exercises the exact seam
// engine.Build's wiring controls: Runner only hands a rung above
// exec.LevelNone a real req.OutputDir, and only commits what lands there,
// once a *worker.Runner has an artifact plane (see runner.WithArtifacts).
type scratchWritingExecutor struct{}

func (scratchWritingExecutor) Name() string      { return "scratch-writer" }
func (scratchWritingExecutor) Level() exec.Level { return exec.LevelProcess }

func (scratchWritingExecutor) Run(_ context.Context, req *exec.Request) (*exec.Result, error) {
	if req.OutputDir == "" {
		return nil, errors.New("no OutputDir given — the executor was not wired for out-of-process output committing")
	}

	if err := os.WriteFile(filepath.Join(req.OutputDir, "result.txt"), []byte("wired"), 0o600); err != nil {
		return nil, err
	}

	return &exec.Result{Status: exec.StatusOK}, nil
}

func (scratchWritingExecutor) Reclaim(context.Context, id.WorkerID) error { return nil }
func (scratchWritingExecutor) Close() error                               { return nil }

// TestEngineBuild_WiresArtifactsIntoRunner is the test the review found
// missing: engine.Build's
//
//	if eng.artifacts != nil {
//	    runner.WithArtifacts(eng.artifacts, eng.scratchRoot)
//	}
//
// (engine/engine.go) is what gives an out-of-process rung its scratch
// directory, its PriorOutputs, and its output committing at all — it is
// the entire point of the commit this task exists to cover. Mutating
// that guard to `if eng.artifacts != nil && false` left the whole suite
// green before this test existed, including TestEndToEndStageAndCommit:
// that test's job declares no isolation (exec.LevelNone), so its
// staging/commit path runs entirely through the artifact-input staging
// middleware, never through runner.WithArtifacts at all. This test
// specifically declares exec.LevelProcess so it actually exercises that
// call, and fails without it.
//
// What this proves: a LevelProcess executor is handed a real, writable
// OutputDir by the Runner, and whatever regular file it leaves there is
// committed through the artifact plane and visible via
// ListArtifactsByOwner — the same observable TestEndToEndStageAndCommit
// checks for the in-process path.
//
// What this does NOT prove: it does not exercise exec/subprocess's own
// re-exec'd OS subprocess, its wire protocol, or its uid/rlimit
// enforcement — those are exec/subprocess's own tests' job. This test's
// fake executor stands in for "any exec.LevelProcess executor", which is
// the right level for pinning engine.Build's own wiring, since that
// wiring does not know or care which out-of-process executor is
// registered.
func TestEngineBuild_WiresArtifactsIntoRunner(t *testing.T) {
	ctx := context.Background()

	s := memory.New()
	backend := artifacttest.NewBackend()
	svc := artifact.NewService(s, backend,
		artifact.WithEphemeralPrefix("ephemeral"),
		artifact.WithDefaultBucket("dispatch"))

	c, err := cache.New(t.TempDir(), backend, cache.WithBudget(1<<20))
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() {
		if cerr := c.Close(); cerr != nil {
			t.Errorf("cache close: %v", cerr)
		}
	})

	d, err := dispatch.New(
		dispatch.WithStore(s),
		dispatch.WithConcurrency(1),
		dispatch.WithQueues([]string{"default"}),
	)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d,
		engine.WithArtifacts(svc, c),
		engine.WithExecutor(scratchWritingExecutor{}),
	)
	if err != nil {
		t.Fatalf("engine.Build: %v", err)
	}

	def := job.NewDefinition("out-of-process",
		func(context.Context, tessellateInput) error { return nil },
		job.WithExecution(exec.Isolate(exec.LevelProcess)),
	)

	if err = engine.RegisterChecked(eng, def); err != nil {
		t.Fatalf("RegisterChecked: %v", err)
	}

	j, err := engine.Enqueue(ctx, eng, "out-of-process", tessellateInput{Detail: 0.5})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if serr := eng.Start(ctx); serr != nil {
		t.Fatalf("engine.Start: %v", serr)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if serr := eng.Stop(stopCtx); serr != nil {
			t.Errorf("engine.Stop: %v", serr)
		}
	})

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: j.ID.String()}

	waitFor(t, 5*time.Second, func() bool {
		outputs, listErr := s.ListArtifactsByOwner(ctx, owner, artifact.RoleOutput)
		return listErr == nil && len(outputs) == 1
	})

	outputs, err := s.ListArtifactsByOwner(ctx, owner, artifact.RoleOutput)
	if err != nil {
		t.Fatalf("ListArtifactsByOwner: %v", err)
	}
	if len(outputs) != 1 {
		t.Fatalf("got %d committed outputs, want 1 — without engine.Build wiring runner.WithArtifacts, "+
			"the executor's output is discarded along with its scratch directory instead of committed",
			len(outputs))
	}
	if outputs[0].Size != int64(len("wired")) {
		t.Errorf("output size = %d, want %d", outputs[0].Size, len("wired"))
	}
}
