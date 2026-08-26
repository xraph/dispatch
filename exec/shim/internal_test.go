//go:build unix

package shim

// This file is package shim (internal), not shim_test, deliberately
// breaking the external-tests-only convention the rest of this package
// follows. mainExitCode and fdFromEnv are unexported, and the only way to
// exercise mainExitCode's exit-code derivation and its signal-handling
// goroutine without forking a real subprocess — which os.Exit inside Main
// would otherwise force — is to call it directly. File descriptors are
// process-scoped, so os.Pipe plus t.Setenv reaches it in-process.
//
// The build tag is here because callMainExitCode below calls
// syscall.Dup, which does not exist in Go's syscall package on Windows —
// this file predates the rest of this package's build-tag split (it's
// from Task 3) and was missed when that split happened. TestFDFromEnv
// itself needs nothing platform-specific, but it lives in the same file
// as callMainExitCode's other callers, so it is unix-only along with
// them rather than split out on its own; this package has no real
// non-Unix target (see procattr_other.go / limits_other.go in
// exec/subprocess, which refuse the whole rung outside Unix), so the
// coverage this loses there is nothing this rung claims to provide
// anyway.

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/wire"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// internalReq builds a minimal request for name, with its own OutputDir so
// runs don't interfere with each other.
func internalReq(t *testing.T, name string) *exec.Request {
	t.Helper()

	return &exec.Request{
		JobID:     id.NewJobID(),
		Name:      name,
		Payload:   []byte("{}"),
		OutputDir: t.TempDir(),
	}
}

// callMainExitCode wires up a pair of in-process pipes standing in for fd
// 3 and fd 4, points EnvRequestFD/EnvResultFD at duplicates of them,
// encodes req into the request pipe, and returns whatever
// mainExitCode(defs) returns.
//
// The duplication matters. mainExitCode wraps whatever fd number it is
// given in its own *os.File via os.NewFile, and that wrapper carries a GC
// finalizer that closes the underlying fd once the wrapper becomes
// unreachable — confirmed empirically: wrapping a pipe fd, dropping the
// wrapper, and forcing a GC leaves the original os.Pipe halves reading
// "bad file descriptor". A real re-exec'd child does not hit this,
// because fd 3 and fd 4 there are the child's own fd-table entries
// (inherited via dup2 at exec time), independent of whatever fd number
// the parent's pipe end happened to have — closing them never touches the
// parent's descriptors. Handing mainExitCode this test's *own* reqR/resW
// fd numbers directly would violate that: a leftover wrapper from an
// earlier call in this same loop can get GC'd and finalize-close a fd
// number the OS has since reused for a *later* call's pipe. Dup'ing
// before the call gives mainExitCode an fd it exclusively owns, matching
// the real setup and making the finalizer harmless — it closes only the
// duplicate, never the original this function still holds.
//
// t.Setenv scopes the environment override to this test and restores it
// afterward. This function's own reqR/reqW/resR/resW are closed before it
// returns, so a test calling this in a loop does not exhaust descriptors;
// the dup'd fds handed to mainExitCode are left for its finalizer, which
// is exactly the ownership split described above.
func callMainExitCode(t *testing.T, defs []job.Registrable, req *exec.Request) int {
	t.Helper()

	reqR, reqW, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe() = %v", err)
	}
	defer reqR.Close()
	defer reqW.Close()

	resR, resW, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe() = %v", err)
	}
	defer resR.Close()
	defer resW.Close()

	if eerr := wire.Encode(reqW, &wire.Frame{Kind: wire.KindRequest, Request: req}); eerr != nil {
		t.Fatalf("Encode() = %v", eerr)
	}

	reqDup, err := syscall.Dup(int(reqR.Fd()))
	if err != nil {
		t.Fatalf("Dup() = %v", err)
	}

	resDup, err := syscall.Dup(int(resW.Fd()))
	if err != nil {
		t.Fatalf("Dup() = %v", err)
	}

	t.Setenv(EnvRequestFD, strconv.Itoa(reqDup))
	t.Setenv(EnvResultFD, strconv.Itoa(resDup))

	return mainExitCode(defs, false)
}

// TestMainExitCode pins the exit-code contract mainExitCode must satisfy:
// a handler outcome, success or failure, is exit 0 because the parent
// reads it from the Result frame; a launch failure — the shim never
// finding a matching handler — is exit non-zero because Run reports that
// case by writing a Result and returning a nil error, and mainExitCode
// has to look past that nil to the Result's Status to catch it. Before the
// fix this whole table would have reported 0.
func TestMainExitCode(t *testing.T) {
	defs := []job.Registrable{
		job.NewDefinition("internal.ok", func(context.Context, struct{}) error { return nil }),
		job.NewDefinition("internal.err", func(context.Context, struct{}) error {
			return errors.New("handler said no")
		}),
	}

	tests := []struct {
		name string
		job  string
		want int
	}{
		{"handler success is exit 0", "internal.ok", 0},
		{"handler error is exit 0 -- StatusHandlerError is a business outcome", "internal.err", 0},
		{"unknown handler is a launch failure and must be exit non-zero", "internal.absent", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := callMainExitCode(t, defs, internalReq(t, tt.job)); got != tt.want {
				t.Errorf("mainExitCode() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestMainExitCode_FingerprintMismatchIsNonZero covers the other launch
// failure path — a fingerprint the registry does not match — since it
// goes through a different branch of run than an unknown handler does.
func TestMainExitCode_FingerprintMismatchIsNonZero(t *testing.T) {
	defs := []job.Registrable{
		job.NewDefinition("internal.ok", func(context.Context, struct{}) error { return nil }),
	}

	r := internalReq(t, "internal.ok")
	r.Fingerprint = "not-the-right-fingerprint"

	if got := callMainExitCode(t, defs, r); got != 1 {
		t.Errorf("mainExitCode() = %d, want 1", got)
	}
}

// TestMainExitCode_NoGoroutineLeak proves the fix for the SIGTERM-handler
// goroutine leak: mainExitCode must not leave its signal-watching
// goroutine parked on <-sigCh forever when no signal ever arrives, which
// is the common case (Main's own os.Exit used to mask this by killing the
// process before a leak could accumulate). Called 20 times with no signal
// sent, the goroutine count must return to its starting point once each
// call's done channel unblocks the watcher.
func TestMainExitCode_NoGoroutineLeak(t *testing.T) {
	defs := []job.Registrable{
		job.NewDefinition("internal.ok", func(context.Context, struct{}) error { return nil }),
	}

	// One warm-up call first, outside the measured baseline: the Go
	// runtime lazily starts its own permanent signal-forwarding goroutine
	// the first time any code calls signal.Notify, and that goroutine
	// lives for the rest of the process. Counting goroutines before this
	// warm-up would mistake that one-time, one-goroutine runtime cost for
	// a leak on the very first iteration, when the thing under test is
	// growth across repeated calls, not that fixed cost.
	if got := callMainExitCode(t, defs, internalReq(t, "internal.ok")); got != 0 {
		t.Fatalf("mainExitCode() warm-up call = %d, want 0", got)
	}

	before := runtime.NumGoroutine()

	const calls = 20
	for i := 0; i < calls; i++ {
		if got := callMainExitCode(t, defs, internalReq(t, "internal.ok")); got != 0 {
			t.Fatalf("mainExitCode() call %d = %d, want 0", i, got)
		}
	}

	// The done channel closes as mainExitCode returns, but the goroutine
	// parked in select still needs a scheduler quantum to wake up and
	// exit. Poll with a deadline rather than a fixed sleep so the test is
	// both fast on the common path and not flaky under load.
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}

	if got := runtime.NumGoroutine(); got > before {
		t.Errorf("NumGoroutine() = %d after %d calls with no signal, want <= %d (leak in the SIGTERM handler goroutine)",
			got, calls, before)
	}
}

// TestFDFromEnv exercises fdFromEnv directly: an unset variable and an
// unparsable one both fall back to the default, and a valid one wins.
func TestFDFromEnv(t *testing.T) {
	t.Setenv(EnvRequestFD, "42")
	if got := fdFromEnv(EnvRequestFD, defaultRequestFD); got != 42 {
		t.Errorf("fdFromEnv() = %d, want 42", got)
	}

	t.Setenv(EnvRequestFD, "not-a-number")
	if got := fdFromEnv(EnvRequestFD, defaultRequestFD); got != defaultRequestFD {
		t.Errorf("fdFromEnv() = %d, want default %d", got, defaultRequestFD)
	}

	// A negative value parses fine but is not a descriptor, and callers convert
	// to uintptr, where it would wrap to an enormous bogus fd rather than fail.
	for _, v := range []string{"-1", "-99"} {
		t.Setenv(EnvRequestFD, v)
		if got := fdFromEnv(EnvRequestFD, defaultRequestFD); got != defaultRequestFD {
			t.Errorf("fdFromEnv(%q) = %d, want default %d", v, got, defaultRequestFD)
		}
	}

	if err := os.Unsetenv("DISPATCH_EXEC_SHIM_TEST_UNSET"); err != nil {
		t.Fatalf("Unsetenv() = %v", err)
	}
	if got := fdFromEnv("DISPATCH_EXEC_SHIM_TEST_UNSET", defaultResultFD); got != defaultResultFD {
		t.Errorf("fdFromEnv() = %d, want default %d", got, defaultResultFD)
	}
}
