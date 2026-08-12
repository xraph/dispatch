package exectest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/id"
)

// Capabilities describes what a rung can actually do, so the suite asserts
// enforcement only against rungs that provide it.
//
// It describes variation, not an opt-out. RunSuite cross-checks it against
// the executor's own Level: anything claiming LevelProcess or above must
// enforce deadlines and isolate panics, and saying otherwise fails the suite
// rather than skipping the tests that prove it.
type Capabilities struct {
	// Enforces means the rung can stop a handler that ignores its
	// deadline. Only out-of-process rungs can.
	Enforces bool

	// ReportsUsage means the rung measures CPU time and peak memory
	// rather than only wall time.
	ReportsUsage bool

	// IsolatesPanic means a panicking handler does not take the caller
	// down, so the rung reports it as a failed attempt rather than
	// relying on the worker's recover middleware.
	IsolatesPanic bool
}

// RunSuite runs the conformance suite against one executor implementation.
//
// newExecutor is called per subtest so each case gets a clean executor.
// The returned executor must already have the fixture Handlers registered.
func RunSuite(t *testing.T, name string, newExecutor func(*testing.T) exec.Executor, caps Capabilities) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		t.Run("CapabilitiesMatchLevel", func(t *testing.T) {
			testCapabilitiesMatchLevel(t, name, newExecutor, caps)
		})
		t.Run("Identity", func(t *testing.T) { testIdentity(t, newExecutor) })
		t.Run("Success", func(t *testing.T) { testSuccess(t, newExecutor) })
		t.Run("HandlerError", func(t *testing.T) { testHandlerError(t, newExecutor) })
		t.Run("UnknownHandler", func(t *testing.T) { testUnknownHandler(t, newExecutor) })
		t.Run("InvalidRequest", func(t *testing.T) { testInvalidRequest(t, newExecutor) })
		t.Run("PayloadRoundTrip", func(t *testing.T) { testPayloadRoundTrip(t, newExecutor) })
		t.Run("LargePayload", func(t *testing.T) { testLargePayload(t, newExecutor) })
		t.Run("Cancellation", func(t *testing.T) { testCancellation(t, newExecutor) })
		t.Run("WallTimeRecorded", func(t *testing.T) { testWallTime(t, newExecutor) })
		t.Run("Reclaim", func(t *testing.T) { testReclaim(t, newExecutor) })

		if caps.Enforces {
			t.Run("DeadlineEnforced", func(t *testing.T) { testDeadlineEnforced(t, newExecutor) })
		}
		if caps.IsolatesPanic {
			t.Run("PanicIsolated", func(t *testing.T) { testPanicIsolated(t, newExecutor) })
		}
		if caps.ReportsUsage {
			t.Run("UsageReported", func(t *testing.T) { testUsageReported(t, newExecutor) })
		}
	})
}

func request(name string, payload any) *exec.Request {
	// payload is always one of this file's fixture payload types, which
	// are all marshalable, so the error is unreachable in practice.
	raw, _ := json.Marshal(payload) //nolint:errcheck // fixture payload types always marshal

	return &exec.Request{
		JobID:       id.NewJobID(),
		Name:        name,
		Payload:     raw,
		Fingerprint: exec.Fingerprint(HandlerNames()),
		Policy:      exec.NewPolicy(),
	}
}

// CheckCapabilities reports whether the capabilities a rung claims are
// consistent with the isolation it advertises, returning nil when they are.
//
// Without this check Capabilities is an escape hatch: a rung that reports
// LevelProcess but sets Enforces false skips DeadlineEnforced — the only
// test proving it can stop a handler that ignores cancellation, which is the
// property the whole isolation ladder exists to provide — and still passes
// the suite clean. An executor may run handlers where it cannot kill them,
// or it may claim LevelProcess; it may not do both.
//
// RunSuite calls this. It is exported so a rung's own tests can assert the
// same consistency without running the full suite.
func CheckCapabilities(name string, level exec.Level, caps Capabilities) error {
	if level < exec.LevelProcess {
		return nil
	}

	var errs []error
	if !caps.Enforces {
		errs = append(errs, fmt.Errorf(
			"executor %q reports Level %s but Capabilities.Enforces = false: "+
				"a rung running handlers out of process must be able to kill one that ignores its deadline",
			name, level))
	}
	if !caps.IsolatesPanic {
		errs = append(errs, fmt.Errorf(
			"executor %q reports Level %s but Capabilities.IsolatesPanic = false: "+
				"a handler panicking in another address space cannot take the worker down",
			name, level))
	}

	return errors.Join(errs...)
}

func testCapabilitiesMatchLevel(
	t *testing.T,
	name string,
	newExecutor func(*testing.T) exec.Executor,
	caps Capabilities,
) {
	if err := CheckCapabilities(name, newExecutor(t).Level(), caps); err != nil {
		t.Errorf("CheckCapabilities() = %v, want nil", err)
	}
}

func testIdentity(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	e := newExecutor(t)
	if e.Name() == "" {
		t.Error("Name() is empty")
	}
	if err := e.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

func testSuccess(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(), request(JobOK, struct{}{}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Status != exec.StatusOK {
		t.Errorf("Status = %q, want %q (handler err: %q)", res.Status, exec.StatusOK, res.HandlerErr)
	}
	if res.Err() != nil {
		t.Errorf("Err() = %v, want nil", res.Err())
	}
}

func testHandlerError(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(), request(JobError, struct{}{}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Status != exec.StatusHandlerError {
		t.Fatalf("Status = %q, want %q", res.Status, exec.StatusHandlerError)
	}
	if res.HandlerErr != ErrIntentional.Error() {
		t.Errorf("HandlerErr = %q, want %q", res.HandlerErr, ErrIntentional.Error())
	}
	if !errors.Is(res.Err(), exec.ErrHandler) {
		t.Errorf("Err() = %v, want it to wrap ErrHandler", res.Err())
	}
}

func testUnknownHandler(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(), request("exectest.absent", struct{}{}))
	if err != nil {
		t.Fatalf("Run() error = %v, want a Result", err)
	}
	if res.Status != exec.StatusLaunchFailed {
		t.Fatalf("Status = %q, want %q", res.Status, exec.StatusLaunchFailed)
	}
	if res.Status.CountsAgainstRetries() {
		t.Error("an unknown handler must not consume the retry budget")
	}
}

func testInvalidRequest(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	_, err := newExecutor(t).Run(context.Background(), &exec.Request{})
	if !errors.Is(err, exec.ErrInvalidRequest) {
		t.Fatalf("Run() error = %v, want ErrInvalidRequest", err)
	}
}

func testPayloadRoundTrip(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	const want = "hello boundary"

	res, err := newExecutor(t).Run(context.Background(),
		request(JobEcho, EchoPayload{Value: want, Want: len(want)}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Status != exec.StatusOK {
		t.Fatalf("Status = %q, want %q (handler err: %q)", res.Status, exec.StatusOK, res.HandlerErr)
	}
}

func testLargePayload(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	// Large enough to exceed a pipe buffer, so any rung that frames the
	// request over a descriptor is exercised rather than accidentally
	// fitting in one write.
	big := make([]byte, 1<<20)
	for i := range big {
		big[i] = byte('a' + i%26)
	}

	res, err := newExecutor(t).Run(context.Background(),
		request(JobEcho, EchoPayload{Value: string(big), Want: len(big)}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	// The handler compares the received length against Want, so a rung
	// that truncated the payload in transit fails here rather than
	// silently passing.
	if res.Status != exec.StatusOK {
		t.Errorf("Status = %q, want %q (handler err: %q)", res.Status, exec.StatusOK, res.HandlerErr)
	}
}

func testCancellation(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	res, err := newExecutor(t).Run(ctx,
		request(JobSlow, SlowPayload{SleepMillis: 5000, IgnoreCtx: false}))
	elapsed := time.Since(start)

	// Whatever shape the failure takes, cancellation must actually cut the
	// attempt short. Asserting only "it failed" would pass for a rung that
	// ignored the cancel and let the handler run its full five seconds.
	if elapsed > 3*time.Second {
		t.Errorf("Run() took %v, want cancellation to cut it short", elapsed)
	}
	if err != nil {
		// An out-of-process rung may surface cancellation as a launch
		// error rather than a Result. Both shapes are acceptable.
		return
	}
	if res.Status == exec.StatusOK {
		t.Error("Status = ok, want a failure after cancellation")
	}
}

func testWallTime(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(),
		request(JobSlow, SlowPayload{SleepMillis: 20}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Usage.WallTime <= 0 {
		t.Errorf("Usage.WallTime = %v, want > 0", res.Usage.WallTime)
	}
}

func testReclaim(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	// Reclaim must be safe to call when there is nothing to reclaim,
	// because the pool calls it unconditionally at startup.
	if err := newExecutor(t).Reclaim(context.Background(), id.NewWorkerID()); err != nil {
		t.Errorf("Reclaim() = %v, want nil", err)
	}
}

func testDeadlineEnforced(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	req := request(JobSlow, SlowPayload{SleepMillis: 30000, IgnoreCtx: true})
	req.Deadline = time.Now().Add(300 * time.Millisecond)
	req.Policy = exec.NewPolicy(exec.GracePeriod(200 * time.Millisecond))

	start := time.Now()
	res, err := newExecutor(t).Run(context.Background(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Status != exec.StatusTimeout {
		t.Errorf("Status = %q, want %q", res.Status, exec.StatusTimeout)
	}
	// The handler asked to sleep 30s and ignores cancellation. Anything
	// close to that means the rung did not actually kill it.
	if elapsed > 10*time.Second {
		t.Errorf("Run() took %v, want the deadline to be enforced", elapsed)
	}
}

func testPanicIsolated(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	// Reaching this line at all is half the assertion: a rung claiming
	// IsolatesPanic must not let the handler's panic unwind into the
	// caller and fail the test binary.
	res, err := newExecutor(t).Run(context.Background(), request(JobPanic, struct{}{}))
	if err != nil {
		return // a launch-shaped error is acceptable
	}
	if res.Status != exec.StatusKilled && res.Status != exec.StatusHandlerError {
		t.Errorf("Status = %q, want killed or handler_error for a panicking handler", res.Status)
	}
}

func testUsageReported(t *testing.T, newExecutor func(*testing.T) exec.Executor) {
	res, err := newExecutor(t).Run(context.Background(),
		request(JobSlow, SlowPayload{SleepMillis: 50}))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Usage.PeakRSS <= 0 {
		t.Errorf("Usage.PeakRSS = %d, want > 0", res.Usage.PeakRSS)
	}
}
