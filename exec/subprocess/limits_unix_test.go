//go:build unix

package subprocess_test

import (
	"context"
	"os"
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
	// A NoFile limit of 3 is below what any real process can operate
	// under (fd 0/1/2 alone exhaust it before fd 3/4 for the wire
	// protocol are even reached), so the shim must fail somehow — either
	// it never gets far enough to report OK, or the parent classifies the
	// process ending badly as killed/launch_failed. What matters is that
	// StatusOK is unreachable, which it would not be if the limit had
	// been silently dropped.
	if res.Status == exec.StatusOK {
		t.Error("Status = ok; a RLIMIT_NOFILE of 3 should have made the child unable to run at all, so the limit was not applied")
	}
}
