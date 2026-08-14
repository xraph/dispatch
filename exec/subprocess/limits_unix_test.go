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

// TestNoUserIsRefusedByDefault proves checkLaunch refuses to start when no
// uid is configured at all — not just when a configured uid happens to
// match the worker's own. Without this, `execution.subprocess.enabled:
// true` with no `user` set would run the child as the worker's own uid
// silently: it defeats the uid boundary in exactly the same way a
// configured same-uid does, so it is refused the same way, not merely
// warned about.
func TestNoUserIsRefusedByDefault(t *testing.T) {
	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
	)

	res, err := e.Run(context.Background(), request(t, exectest.JobOK, struct{}{}))
	switch {
	case err != nil: // acceptable: refused at launch
	case res.Status != exec.StatusLaunchFailed:
		t.Fatalf("Status = %q, want launch_failed — no uid configured guts this rung", res.Status)
	}
}

// TestNoUserAllowedExplicitly proves WithAllowSameUser lifts the no-uid
// refusal above too, not just the same-uid one: it is the single opt-out
// for both shapes of running this rung unisolated.
func TestNoUserAllowedExplicitly(t *testing.T) {
	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
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

// TestSameUserRefusedMatchesCheckLaunch pins subprocess.SameUserRefused
// to checkLaunch's own actual launch-time decision. checkLaunch calls
// SameUserRefused itself (see limits_unix.go), so in principle this test
// cannot fail unless a future edit gives the two functions independent
// logic again — which is exactly the drift extension.resolveExecutionOptions
// (extension/execution.go) depends on not happening: it calls
// SameUserRefused to fail fast at startup, and that check is only correct
// for as long as it asks the identical question checkLaunch asks at
// Run() time.
func TestSameUserRefusedMatchesCheckLaunch(t *testing.T) {
	tests := []struct {
		name          string
		allowSameUser bool
	}{
		{"refused without AllowSameUser", false},
		{"allowed with AllowSameUser", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predicted := subprocess.SameUserRefused(os.Getuid(), tt.allowSameUser)

			opts := []subprocess.Option{
				subprocess.WithBinary(os.Args[0]),
				subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
				subprocess.WithUser(os.Getuid(), os.Getgid()),
			}
			if tt.allowSameUser {
				opts = append(opts, subprocess.WithAllowSameUser())
			}

			e := subprocess.New(opts...)
			res, err := e.Run(context.Background(), request(t, exectest.JobOK, struct{}{}))

			refused := err != nil || res.Status == exec.StatusLaunchFailed
			if refused != predicted {
				t.Fatalf("SameUserRefused(%d, %v) = %v, but the actual launch's refusal = %v — "+
					"the two have drifted apart", os.Getuid(), tt.allowSameUser, predicted, refused)
			}
		})
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
		subprocess.WithAllowSameUser(), // CI cannot drop privileges; this test is not about the uid boundary
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
// applyRlimits rejects before ever calling Setrlimit — see that same
// file's TestApplyOneUnverifiedResourceIsAFailure and its neighbors for
// why a value that actually depends on kernel-specific hard limits (a
// real NoFile ceiling, say) would be less portable across this rung's
// supported platforms than a value rejected up front.
func TestStrictRlimitsFailsLaunchOnUnexpectedFailure(t *testing.T) {
	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
		subprocess.WithRlimits(subprocess.Rlimits{NoFile: -1}),
		subprocess.WithStrictRlimits(),
		subprocess.WithAllowSameUser(), // CI cannot drop privileges; this test is not about the uid boundary
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
		subprocess.WithAllowSameUser(), // CI cannot drop privileges; this test is not about the uid boundary
	)

	res, err := e.Run(context.Background(), request(t, exectest.JobOK, struct{}{}))
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusOK {
		t.Fatalf("Status = %q, want ok (err %q) — a platform's own refusal to support RLIMIT_AS must not be treated as a WithStrictRlimits failure", res.Status, res.HandlerErr)
	}
}
