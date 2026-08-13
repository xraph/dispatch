package subprocess_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/exectest"
	"github.com/xraph/dispatch/exec/subprocess"
	"github.com/xraph/dispatch/id"
)

func newExecutor(t *testing.T) *subprocess.Executor {
	t.Helper()

	return subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
	)
}

func request(t *testing.T, name string, payload any) *exec.Request {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return &exec.Request{
		JobID:     id.NewJobID(),
		Name:      name,
		Payload:   raw,
		OutputDir: t.TempDir(),
		Policy:    exec.NewPolicy(exec.GracePeriod(time.Second)),
	}
}

func TestIdentity(t *testing.T) {
	e := newExecutor(t)
	if e.Name() != "subprocess" {
		t.Errorf("Name() = %q, want %q", e.Name(), "subprocess")
	}
	if e.Level() != exec.LevelProcess {
		t.Errorf("Level() = %v, want %v", e.Level(), exec.LevelProcess)
	}
}

func TestRunSuccess(t *testing.T) {
	res, err := newExecutor(t).Run(context.Background(), request(t, exectest.JobOK, struct{}{}))
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusOK {
		t.Fatalf("Status = %q, want ok (err %q)", res.Status, res.HandlerErr)
	}
}

func TestRunHandlerErrorIsExitZero(t *testing.T) {
	res, err := newExecutor(t).Run(context.Background(), request(t, exectest.JobError, struct{}{}))
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusHandlerError {
		t.Fatalf("Status = %q, want handler_error", res.Status)
	}
	if res.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0 — a handler saying no is not the shim failing", res.ExitCode)
	}
}

func TestRunPanicIsKilled(t *testing.T) {
	// The child dies; the parent must report it as killed rather than
	// letting the panic reach the worker, which is the whole point.
	res, err := newExecutor(t).Run(context.Background(), request(t, exectest.JobPanic, struct{}{}))
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusKilled && res.Status != exec.StatusHandlerError {
		t.Errorf("Status = %q, want killed or handler_error", res.Status)
	}
	if res.Status == exec.StatusOK {
		t.Error("a panicking handler must not report success")
	}
}

func TestRunDeadlineKillsAHandlerThatIgnoresCancellation(t *testing.T) {
	req := request(t, exectest.JobSlow, exectest.SlowPayload{SleepMillis: 30000, IgnoreCtx: true})
	req.Deadline = time.Now().Add(500 * time.Millisecond)

	start := time.Now()
	res, err := newExecutor(t).Run(context.Background(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusTimeout {
		t.Errorf("Status = %q, want timeout", res.Status)
	}
	// This is the assertion the whole phase exists for: the handler asked
	// to sleep 30s and ignores cancellation, so anything near that means
	// the deadline was still advisory.
	if elapsed > 10*time.Second {
		t.Errorf("Run() took %v; the deadline was not enforced", elapsed)
	}
}

func TestRunUnknownHandlerIsLaunchFailure(t *testing.T) {
	res, err := newExecutor(t).Run(context.Background(), request(t, "subprocess.absent", struct{}{}))
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusLaunchFailed {
		t.Fatalf("Status = %q, want launch_failed", res.Status)
	}
	if res.Status.CountsAgainstRetries() {
		t.Error("an unknown handler must not consume the retry budget")
	}
}

func TestRunMissingBinaryIsLaunchFailure(t *testing.T) {
	e := subprocess.New(subprocess.WithBinary("/nonexistent/dispatch-worker"))
	res, err := e.Run(context.Background(), request(t, exectest.JobOK, struct{}{}))

	// Either shape is acceptable, but it must be classified as a launch
	// failure and must not consume a retry.
	switch {
	case err != nil:
		// A raw error from Run is treated as a launch failure by the Runner.
	case res.Status != exec.StatusLaunchFailed:
		t.Fatalf("Status = %q, want launch_failed", res.Status)
	}
}

// TestRunContextCancellationKillsChild is not one of the brief's listed
// cases, but it proves a constraint the brief states in prose: cancelling
// the caller's context must actually stop the child, not just make Run
// return while the process keeps running. IgnoreCtx makes the handler deaf
// to its own context, so the only way this finishes quickly is the parent
// killing the OS process from the outside.
func TestRunContextCancellationKillsChild(t *testing.T) {
	req := request(t, exectest.JobSlow, exectest.SlowPayload{SleepMillis: 30000, IgnoreCtx: true})

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	res, err := newExecutor(t).Run(ctx, req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status == exec.StatusOK {
		t.Error("a cancelled attempt must not report success")
	}
	if elapsed > 10*time.Second {
		t.Errorf("Run() took %v; context cancellation did not stop the child", elapsed)
	}
}
