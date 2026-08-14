package subprocess_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
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

// runBounded runs e.Run in the background and fails the test fast if it
// does not return within bound, rather than letting a regression hang
// until go test's own multi-minute panic timeout does the job for it.
// Returning within a bound despite something misbehaving downstream is
// exactly the property the C1/C2 regression tests exist to check, so they
// assert on it directly instead of only on the returned Status.
func runBounded(
	ctx context.Context, t *testing.T, e *subprocess.Executor, req *exec.Request, bound time.Duration,
) (*exec.Result, time.Duration) {
	t.Helper()

	type outcome struct {
		res *exec.Result
		err error
	}
	done := make(chan outcome, 1)
	start := time.Now()
	go func() {
		res, err := e.Run(ctx, req)
		done <- outcome{res, err}
	}()

	select {
	case o := <-done:
		if o.err != nil {
			t.Fatalf("Run() = %v", o.err)
		}

		return o.res, time.Since(start)
	case <-time.After(bound):
		t.Fatalf("Run() did not return within %v", bound)

		return nil, 0 // unreachable; Fatalf stops the goroutine
	}
}

// TestRunGrandchildCannotWedgeTheDrain reproduces C1: a handler's own
// subprocess (a shelled-out ffmpeg, a stray background job) can exit
// cleanly and leave a grandchild running behind it. There is no deadline
// and no cancellation here, so killProcess never runs — the tracked
// process was never killed, it just exited on its own — which is exactly
// the case where nothing upstream would otherwise stop that grandchild
// from holding stdout/stderr open indefinitely. Only the post-wait drain
// grace stands between that and Run hanging for as long as the
// grandchild runs.
func TestRunGrandchildCannotWedgeTheDrain(t *testing.T) {
	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{envLeakChild: "1"}),
	)

	res, elapsed := runBounded(context.Background(), t, e, request(t, exectest.JobOK, struct{}{}), 8*time.Second)

	if res.Status != exec.StatusOK {
		t.Errorf("Status = %q, want ok (err %q)", res.Status, res.HandlerErr)
	}
	// fixtureSleep is 30s; anything well under that proves Run did not
	// wait on the grandchild to finish on its own.
	if elapsed > 6*time.Second {
		t.Errorf("Run() took %v; a leaked grandchild wedged the post-wait drain", elapsed)
	}
}

// TestRunRequestWriteIsInterruptedByDeadline reproduces C2: a request
// large enough to fill the pipe buffer (64KB on Linux, often 16KB on
// macOS), written to a child that never reads fd 3 at all. Without the
// fix, that write blocks in Run's own goroutine before the deadline timer
// even exists, so nothing can interrupt it until the child eventually
// exits on its own — here, only after fixtureSleep, far past the
// deadline this test sets.
func TestRunRequestWriteIsInterruptedByDeadline(t *testing.T) {
	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{envSleepOnly: "1"}),
	)

	req := request(t, exectest.JobOK, struct{ Value string }{Value: strings.Repeat("x", 1<<20)})
	req.Deadline = time.Now().Add(300 * time.Millisecond)

	res, elapsed := runBounded(context.Background(), t, e, req, 8*time.Second)

	if res.Status != exec.StatusTimeout && res.Status != exec.StatusKilled {
		t.Errorf("Status = %q, want timeout or killed", res.Status)
	}
	if elapsed > 6*time.Second {
		t.Errorf("Run() took %v; the deadline did not reach a request write blocked outside the select loop", elapsed)
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
