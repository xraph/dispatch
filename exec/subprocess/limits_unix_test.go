//go:build unix

package subprocess_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/exectest"
	"github.com/xraph/dispatch/exec/subprocess"
)

// TestSameUserIsRefusedByDefault proves checkLaunch (limits_unix.go)
// refuses to start the child when the configured uid matches the
// worker's own: without this, the uid boundary this rung exists to
// provide is silently absent, since the child ends up able to read
// everything the worker itself can.
func TestSameUserIsRefusedByDefault(t *testing.T) {
	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
		subprocess.WithUser(os.Getuid(), os.Getgid()),
	)

	res, err := e.Run(context.Background(), request(t, exectest.JobOK, struct{}{}))
	switch {
	case err != nil: // acceptable: refused at launch
	case res.Status != exec.StatusLaunchFailed:
		t.Fatalf("Status = %q, want launch_failed — running as the worker's own UID guts this rung", res.Status)
	}
}

// TestSameUserAllowedExplicitly proves WithAllowSameUser lifts the refusal
// above and the run proceeds normally. Credential.NoSetGroups matters
// here specifically: without it, os/exec's setgroups(2) call requires
// privilege this test process (running as an ordinary, non-root user, as
// every dev machine and CI run here does) does not have, and this case
// would fail to launch even though the uid/gid themselves are unchanged.
func TestSameUserAllowedExplicitly(t *testing.T) {
	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
		subprocess.WithUser(os.Getuid(), os.Getgid()),
		subprocess.WithAllowSameUser(),
	)

	res, err := e.Run(context.Background(), request(t, exectest.JobOK, struct{}{}))
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusOK {
		t.Fatalf("Status = %q, want ok (err %q)", res.Status, res.HandlerErr)
	}
}

// TestRlimitsAreAppliedChildSide proves a configured Rlimits value
// actually reaches the child: RLIMIT_NOFILE is used rather than
// RLIMIT_AS or RLIMIT_CORE because it is the one limit in this set that
// (a) Darwin actually allows setrlimit to lower, unlike RLIMIT_AS (see
// the AddressSpace doc comment on Rlimits), and (b) this test process can
// still observe from the parent side: a child capped well below the
// number of file descriptors JobOK itself needs (fd 3 and 4 for the wire
// protocol, plus whatever the Go runtime and os/exec's own stdio setup
// already hold open) fails outright, which is a clear, portable signal
// that the limit landed rather than being silently ignored.
func TestRlimitsAreAppliedChildSide(t *testing.T) {
	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
		subprocess.WithRlimits(subprocess.Rlimits{NoFile: 3}),
	)

	res, err := e.Run(context.Background(), request(t, exectest.JobOK, struct{}{}))
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	// Lowering RLIMIT_NOFILE does not close or invalidate descriptors
	// already open at the time it takes effect — fd 0/1/2 (inherited)
	// and fd 3/4 (the wire protocol, opened by the parent before exec)
	// all survive a soft limit of 3 that comes later, in applyRlimits.
	// What a limit of 3 does is make the *next* open() the shim's own
	// runtime needs (a network poller fd, a temp file, anything) fail,
	// which is early and unconditional enough in a live Go program that
	// the shim cannot reach StatusOK afterwards. That is the actual
	// mechanism this asserts on: not descriptors 3/4 becoming unusable,
	// but nothing further being allocatable.
	if res.Status == exec.StatusOK {
		t.Error("Status = ok; a RLIMIT_NOFILE of 3 should have made the child unable to run at all, so the limit was not applied")
	}
}

// TestStrictRlimitsFailsLaunchOnUnexpectedFailure proves WithStrictRlimits
// turns an rlimit failure into StatusLaunchFailed end to end, through a
// real forked child rather than shim's own in-process unit tests (see
// exec/shim/rlimit_unix_test.go for why those stick to values that never
// reach a real Setrlimit call). NoFile: -1 is deliberately a value
// applyRlimits rejects before ever calling Setrlimit — see
// TestSameUserIsRefusedByDefault's neighbors for why a value that
// actually depends on kernel-specific hard limits would be less portable
// than this.
func TestStrictRlimitsFailsLaunchOnUnexpectedFailure(t *testing.T) {
	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
		subprocess.WithRlimits(subprocess.Rlimits{NoFile: -1}),
		subprocess.WithStrictRlimits(),
	)

	res, err := e.Run(context.Background(), request(t, exectest.JobOK, struct{}{}))
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusLaunchFailed {
		t.Errorf("Status = %q, want launch_failed", res.Status)
	}
	if !strings.Contains(res.HandlerErr, "RLIMIT_NOFILE") {
		t.Errorf("HandlerErr = %q, want it to name RLIMIT_NOFILE", res.HandlerErr)
	}
}

// TestStrictRlimitsToleratesKnownUnsupported proves WithStrictRlimits does
// not turn a platform's own structural refusal of a limit into a launch
// failure. The AddressSpace value here is deliberately generous (2GiB) so
// that on a platform where RLIMIT_AS is actually settable (Linux, in
// particular this rung's CI) it simply succeeds rather than crashing this
// small handler; on Darwin, syscall.Setrlimit(RLIMIT_AS, ...) fails with
// EINVAL unconditionally regardless of value, which isKnownUnsupported
// classifies as structural rather than a misconfiguration WithStrictRlimits
// should catch. Either way the expected outcome is the same: StatusOK.
func TestStrictRlimitsToleratesKnownUnsupported(t *testing.T) {
	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
		subprocess.WithRlimits(subprocess.Rlimits{AddressSpace: 2 << 30}),
		subprocess.WithStrictRlimits(),
	)

	res, err := e.Run(context.Background(), request(t, exectest.JobOK, struct{}{}))
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusOK {
		t.Fatalf("Status = %q, want ok (err %q) — a platform's own refusal to support RLIMIT_AS must not be treated as a WithStrictRlimits failure", res.Status, res.HandlerErr)
	}
}
