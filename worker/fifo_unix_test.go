//go:build unix

package worker_test

import (
	"context"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// TestRunner_OpeningAFIFOWouldHangSoItIsNeverOpened is C2's actual
// reproduction: a FIFO with no writer on the other end blocks a plain
// os.Open forever. Since the worker walks OutputDir strictly after the
// sandbox's own Run has already returned — well past whatever deadline
// governed the attempt itself — nothing about the walk is itself
// context-aware, so one mkfifo left in OutputDir would otherwise wedge
// a worker goroutine permanently, and enough of them would exhaust a
// pool. This drives Execute in a goroutine with a hard wall-clock
// timeout and fails loudly if it does not return well within it, rather
// than actually hanging the test suite the way the bug would hang a
// worker.
func TestRunner_OpeningAFIFOWouldHangSoItIsNeverOpened(t *testing.T) {
	rec := &scriptedExecutor{
		level: exec.LevelProcess,
		files: map[string]string{"real.txt": "kept"},
	}
	reg := isolatedJobRegistry(t)
	executors := exec.NewRegistry(inproc.New(reg))
	executors.Add(rec)

	plane := newArtifactPlane()
	runner := newOutputsTestRunner(t, reg, executors, plane)

	rec.beforeReturn = func() {
		fifoPath := filepath.Join(rec.got.OutputDir, "pipe")
		if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
			t.Fatalf("mkfifo: %v", err)
		}
		// Deliberately no writer is ever opened on the other end: that
		// absence is exactly what makes a blocking os.Open on this path
		// hang forever, which is the bug being proven fixed.
	}

	jobID := id.NewJobID()
	j := &job.Job{ID: jobID, Name: "test.job", MaxRetries: 3}

	done := make(chan error, 1)
	go func() {
		done <- runner.Execute(context.Background(), j)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute() = %v, want nil — a FIFO must be skipped, not fail the attempt", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HUNG: Execute() did not return within 3s — a FIFO in OutputDir was opened and blocked forever")
	}

	owner := artifact.OwnerRef{Kind: artifact.OwnerJob, ID: jobID.String()}
	links, err := plane.store.ListLinks(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListLinks: %v", err)
	}
	if len(links) != 1 || links[0].Name != "real.txt" {
		t.Errorf("committed links = %+v, want exactly [real.txt]", links)
	}
}
