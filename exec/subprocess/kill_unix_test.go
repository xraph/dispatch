//go:build unix

package subprocess_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/exectest"
	"github.com/xraph/dispatch/exec/subprocess"
)

// TestKillLadderReachesAHandlerIgnoringSIGTERM proves the ladder still
// bounds Run's return even when the handler ignores the SIGTERM half of
// it entirely: IgnoreCtx makes JobSlow deaf to context cancellation (and
// so, transitively, to the shim's own SIGTERM trap, which only cancels
// that context), so the only thing that can end this attempt within any
// reasonable bound is the SIGKILL that follows the grace period.
func TestKillLadderReachesAHandlerIgnoringSIGTERM(t *testing.T) {
	req := request(t, exectest.JobSlow, exectest.SlowPayload{SleepMillis: 60000, IgnoreCtx: true})
	req.Deadline = time.Now().Add(300 * time.Millisecond)
	req.Policy = exec.NewPolicy(exec.GracePeriod(300 * time.Millisecond))

	start := time.Now()
	res, err := newExecutor(t).Run(context.Background(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusTimeout {
		t.Errorf("Status = %q, want timeout", res.Status)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Run() took %v; SIGKILL did not follow the grace period", elapsed)
	}
}

// TestKillLadderKillsTheWholeProcessGroup is the assertion the brief asks
// for by name: that the process group is actually gone once Run returns,
// not merely the one process this package tracks directly. The
// envGroupKill fixture (main_test.go) ignores SIGTERM outright and forks
// a grandchild that does nothing but sleep, so the only way both ever die
// is the ladder's SIGKILL half reaching the whole group — exactly the
// case a missing Setpgid, or a kill aimed at the wrong target, would fail
// silently on: Run would still return (the leader dies either way), but
// the grandchild would be left running.
func TestKillLadderKillsTheWholeProcessGroup(t *testing.T) {
	req := request(t, exectest.JobOK, struct{}{})
	req.Deadline = time.Now().Add(300 * time.Millisecond)
	req.Policy = exec.NewPolicy(exec.GracePeriod(300 * time.Millisecond))

	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{envGroupKill: "1"}),
	)

	start := time.Now()
	_, err := e.Run(context.Background(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Run() took %v; SIGKILL did not follow the grace period", elapsed)
	}

	pidPath := filepath.Join(req.OutputDir, "grandchild.pid")
	raw, rerr := os.ReadFile(pidPath)
	if rerr != nil {
		t.Fatalf("read grandchild pid file: %v", rerr)
	}

	pid, perr := strconv.Atoi(strings.TrimSpace(string(raw)))
	if perr != nil {
		t.Fatalf("parse grandchild pid %q: %v", raw, perr)
	}

	// A signal-0 probe reports ESRCH once the kernel has no process left
	// at that pid to deliver to. This is the assertion that catches a
	// missing Setpgid or a kill sent to the wrong target: without the
	// process-group signal actually reaching the grandchild, this pid
	// would still be alive here, sleeping out fixtureSleep on its own.
	if kerr := syscall.Kill(pid, 0); kerr != syscall.ESRCH {
		t.Errorf("syscall.Kill(%d, 0) = %v, want ESRCH — grandchild pid %d is still alive", pid, kerr, pid)
	}
}
