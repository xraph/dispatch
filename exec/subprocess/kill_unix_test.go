//go:build unix

package subprocess_test

import (
	"context"
	"errors"
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
// is the ladder's SIGKILL half reaching the whole group.
//
// A missing Setpgid would not fail silently on just the grandchild: with
// no Setpgid the child stays a member of the worker's own process group
// rather than becoming its own group leader, so terminate's pgid — the
// child's pid — names no group at all. killGroup's SIGTERM send and the
// escalation's raw syscall.Kill(-pgid, SIGKILL) both then return ESRCH,
// waitGroupEmpty's very first probe reports the group already "empty",
// and terminate returns without ever escalating. Neither the leader nor
// the grandchild is signalled by this package at all in that case — Run
// still returns once the deadline's own bookkeeping decides the attempt,
// not because anything here reaped either process.
func TestKillLadderKillsTheWholeProcessGroup(t *testing.T) {
	req := request(t, exectest.JobOK, struct{}{})
	req.Deadline = time.Now().Add(300 * time.Millisecond)
	req.Policy = exec.NewPolicy(exec.GracePeriod(300 * time.Millisecond))

	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{envGroupKill: "1"}),
		subprocess.WithAllowSameUser(), // CI cannot drop privileges; this test is not about the uid boundary
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
	// would still be alive here, sleeping out longSleep on its own —
	// which is deliberately much longer than this test's own bound, so
	// that surviving would show up as "still alive," not as "happened to
	// exit on its own around the same time," see envLongSleep's doc
	// comment (main_test.go).
	if kerr := syscall.Kill(pid, 0); kerr != syscall.ESRCH {
		t.Errorf("syscall.Kill(%d, 0) = %v, want ESRCH — grandchild pid %d is still alive", pid, kerr, pid)
	}
}

// TestKillLadderReapsAHelperAfterACooperativeLeaderExits is the C1
// regression test: it pins the bug where terminate decided whether to
// escalate to SIGKILL by asking only whether the tracked *leader* had
// exited. The envLeaderExitsHelperSurvives fixture's leader exits almost
// immediately on SIGTERM (its default disposition, since this fixture
// installs no handler for it) while the helper it forks first ignores
// SIGTERM outright — so a leader-only liveness check reports "done"
// within a poll interval of the leader dying, milliseconds after SIGTERM
// is sent, and never reaches the SIGKILL that the surviving helper
// actually needs. Measured against the pre-fix implementation: terminate
// returned in ~12ms and the helper was still alive afterwards
// (syscall.Kill(pid, 0) == nil). The fix (waitGroupEmpty, kill_unix.go)
// asks whether the whole *group* is empty instead, which keeps waiting
// through the helper's presence and does send the group SIGKILL once
// grace elapses.
func TestKillLadderReapsAHelperAfterACooperativeLeaderExits(t *testing.T) {
	req := request(t, exectest.JobOK, struct{}{})
	req.Deadline = time.Now().Add(300 * time.Millisecond)
	req.Policy = exec.NewPolicy(exec.GracePeriod(time.Second))

	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{envLeaderExitsHelperSurvives: "1"}),
		subprocess.WithAllowSameUser(), // CI cannot drop privileges; this test is not about the uid boundary
	)

	start := time.Now()
	_, err := e.Run(context.Background(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	// The leader itself dies within a poll interval or two of SIGTERM
	// landing, well under 300ms after the deadline fires. Naively, a
	// regression back to leader-only liveness ought to make Run() return
	// that quickly too — deadline (300ms) plus a handful of milliseconds —
	// since it would treat the leader's own exit as "nothing left to wait
	// for" and never reach the group SIGKILL. In practice this lower bound
	// does NOT reliably catch that regression, and crediting it as the
	// thing that does (an earlier version of this comment did) is false:
	// Run() also waits, separately, on drainGrace (executor.go) for
	// stdout/stderr to close, and this fixture's surviving helper keeps
	// those descriptors open regardless of which way terminate's own
	// liveness check goes. Mutating waitGroupEmpty back to leader-only —
	// reintroducing the exact bug this test exists to catch — measured
	// elapsed at 3.31s here, held entirely by that unrelated 3s drainGrace
	// floor, comfortably clearing 1100ms despite the regression. The
	// assertion that actually catches it is the ESRCH check below: a
	// leader-only liveness check never sends the helper SIGKILL, so it is
	// still alive when Run() returns, which only that check observes. This
	// bound is kept anyway as a sanity check that grace is actually
	// awaited rather than skipped outright (see the upper bound just
	// below it) — it is just not this bug's regression net.
	if elapsed < 1100*time.Millisecond {
		t.Errorf("Run() took %v; returned before the group SIGKILL had a chance to run", elapsed)
	}
	if elapsed > 8*time.Second {
		t.Errorf("Run() took %v; grace was not bounded", elapsed)
	}

	pidPath := filepath.Join(req.OutputDir, "helper.pid")
	raw, rerr := os.ReadFile(pidPath)
	if rerr != nil {
		t.Fatalf("read helper pid file: %v", rerr)
	}

	pid, perr := strconv.Atoi(strings.TrimSpace(string(raw)))
	if perr != nil {
		t.Fatalf("parse helper pid %q: %v", raw, perr)
	}

	// The helper is a grandchild from this test process's point of view —
	// forked by the leader, not by us — so once the leader is gone it is
	// reparented and reaped by whatever subreaper the OS hands it to, not
	// necessarily promptly relative to Run() returning. syscall.Kill(pid,
	// 0) reports success, not ESRCH, for a zombie that has already died
	// to SIGKILL but not yet been reaped: the process table entry is
	// still there. A single check right after Run() returns raced that
	// reaping under parallel-package load and flaked once in CI despite
	// the helper genuinely having been killed. Polling for up to a
	// couple of seconds waits out the reap without weakening what this
	// asserts — the helper still has to actually be gone, just not
	// instantaneously.
	deadline := time.Now().Add(2 * time.Second)
	var kerr error
	for {
		kerr = syscall.Kill(pid, 0)
		if errors.Is(kerr, syscall.ESRCH) || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !errors.Is(kerr, syscall.ESRCH) {
		t.Errorf("syscall.Kill(%d, 0) = %v, want ESRCH — a leader that exits on SIGTERM must not let its own uncooperative helper survive", pid, kerr)
	}
}

// TestKillLadderSendsSIGTERMBeforeGraceElapses is the C3 regression test:
// TestKillLadderReachesAHandlerIgnoringSIGTERM and
// TestKillLadderKillsTheWholeProcessGroup both use fixtures that ignore
// SIGTERM, so both pass identically whether or not the ladder's SIGTERM
// rung runs at all — a build that skipped straight to SIGKILL after grace
// would pass them too. The envSigtermMarker fixture closes that gap: it
// carries no timer of its own and can only end by receiving and handling
// an actual SIGTERM, so the marker file existing, and Run() returning
// well inside the grace period rather than only after it, is evidence
// that could not be produced any other way.
func TestKillLadderSendsSIGTERMBeforeGraceElapses(t *testing.T) {
	req := request(t, exectest.JobOK, struct{}{})
	req.Deadline = time.Now().Add(300 * time.Millisecond)
	req.Policy = exec.NewPolicy(exec.GracePeriod(5 * time.Second))

	e := subprocess.New(
		subprocess.WithBinary(os.Args[0]),
		subprocess.WithEnv(map[string]string{envSigtermMarker: "1"}),
		subprocess.WithAllowSameUser(), // CI cannot drop privileges; this test is not about the uid boundary
	)

	start := time.Now()
	res, err := e.Run(context.Background(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	// The generous 5s grace period is deliberately not what bounds this:
	// if SIGTERM is actually sent and handled, this returns well under
	// it, not anywhere near it. A build that never sent SIGTERM and
	// instead waited for SIGKILL after the full grace period would take
	// close to 5.3s here — well past this bound — since the fixture,
	// having received no SIGTERM, would still be blocked on <-sigCh right
	// up until SIGKILL ends it. The margin below grace is kept wide
	// (rather than a tight multiple of the 300ms deadline) so a cold,
	// possibly race-detector-instrumented child's own startup latency
	// cannot make this test flaky.
	if elapsed > 4*time.Second {
		t.Errorf("Run() took %v; wanted well under the 5s grace period, suggesting SIGTERM was never sent", elapsed)
	}
	// A process ended by SIGKILL reports Signaled() == true; this fixture
	// only ever exits cleanly (os.Exit(0) after handling SIGTERM), so a
	// signalled result here would itself mean the marker-writing path was
	// never reached and SIGKILL ended it directly instead.
	if res.Signal != 0 {
		t.Errorf("Signal = %d, want 0 — the fixture exits cleanly after handling SIGTERM, it does not die by SIGKILL", res.Signal)
	}

	markerPath := filepath.Join(req.OutputDir, "sigterm-received")
	if _, serr := os.Stat(markerPath); serr != nil {
		t.Errorf("sigterm-received marker missing (%v) — the fixture only writes it after actually receiving SIGTERM", serr)
	}
}

// TestKillLadderClassifiesACooperativeTimeoutCorrectly is the C2
// regression test, package-local to exec/subprocess. Every OTHER timeout
// test in this file uses a handler that ignores cancellation (IgnoreCtx:
// true); this one does not, so within this file it is the only test that
// drives classify's timedOut-overrides-the-frame rule (executor.go)
// through a real process instead of the synthetic inputs
// internal_test.go uses. It is not the only such test in the module,
// though: exectest's conformance suite (exec/exectest/suite.go) runs the
// same shape twice more against this rung — testDeadlineEnforcedCooperative
// and testDeadlineEnforcedSwallowedCancellation — as part of
// TestSubprocessConformance. With IgnoreCtx: false, JobSlow honours
// ctx.Done() and returns promptly, the shim writes a Result frame and
// exits 0 — frameOK && !signaled — while the parent's own deadline still
// independently fires and sets timedOut, which classify must let win
// regardless of what the frame says.
func TestKillLadderClassifiesACooperativeTimeoutCorrectly(t *testing.T) {
	req := request(t, exectest.JobSlow, exectest.SlowPayload{SleepMillis: 60000, IgnoreCtx: false})
	req.Deadline = time.Now().Add(300 * time.Millisecond)
	// A generous grace period, not a tight one: this test's whole point is
	// that the handler wins the cooperative race and exits cleanly before
	// any SIGKILL, so it needs real headroom for a cold, possibly
	// race-detector-instrumented child to start up, decode the request,
	// notice cancellation, and write its Result frame — a tight grace
	// here does not make the test stricter, it just makes it flaky by
	// occasionally forcing the SIGKILL half of the ladder to win instead,
	// which asserts nothing about the timedOut-overrides-the-frame rule
	// this test exists to pin.
	req.Policy = exec.NewPolicy(exec.GracePeriod(5 * time.Second))

	res, err := newExecutor(t).Run(context.Background(), req)
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Status != exec.StatusTimeout {
		t.Errorf("Status = %q, want timeout — timedOut must override a clean frame even when the handler cooperates", res.Status)
	}
	if res.Signal != 0 {
		t.Errorf("Signal = %d, want 0 — a cooperative handler exits via the shim's own Result frame, not a signal", res.Signal)
	}
	if res.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0 — the shim exits 0 on a handler error, which ctx.Err() is", res.ExitCode)
	}
}
