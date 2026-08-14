package subprocess_test

import (
	"context"
	"os"
	osexec "os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/exectest"
	"github.com/xraph/dispatch/exec/shim"
	"github.com/xraph/dispatch/exec/wire"
)

// Env vars gating the fixture branches below. Each stands in for a shim
// this binary is not, so the C1/C2 regression tests can drive a real
// process tree without needing a real handler that misbehaves this way.
const (
	// envLeakChild selects a fixture that behaves like a shim whose
	// handler shelled out to something and left it running: it reads the
	// request, spawns a grandchild that inherits stdout, stderr, and both
	// wire descriptors and then just sleeps, writes a normal successful
	// result, and exits promptly. Reproduces C1: the tracked process
	// exits cleanly and quickly, but something it spawned is still
	// holding the pipes open.
	envLeakChild = "DISPATCH_EXEC_LEAK_CHILD_TEST"

	// envSleepOnly selects a fixture that does nothing but sleep and
	// exit. It never touches fd 3 or fd 4 at all, so used as the direct
	// child it reproduces C2: a child that is alive but has not reached
	// wire.Decode. Used as envLeakChild's grandchild, it is what keeps
	// C1's pipes open past the tracked process's own exit.
	envSleepOnly = "DISPATCH_EXEC_SLEEP_ONLY_TEST"

	// envGroupKill selects a fixture for the kill ladder's own test
	// (kill_unix_test.go): it ignores SIGTERM itself FIRST — before
	// forking anything, the safer order, closing the window where a
	// SIGTERM landing between fork and signal.Ignore would kill it
	// outright — then forks a grandchild that ignores SIGTERM and sleeps
	// far longer than this fixture's own SIGTERM-ignoring sleep
	// (envLongSleep, not envSleepOnly — see its own doc comment for why),
	// writes that grandchild's pid to a file in the request's OutputDir,
	// and sleeps. Only the ladder's SIGKILL half — sent to the whole
	// process group, not just this fixture — can end either process,
	// which is what makes this the fixture that catches a missing
	// Setpgid: without it, this process never becomes its own group
	// leader, terminate's pgid (its pid) names no real group, both the
	// SIGTERM and the escalating SIGKILL come back ESRCH, and NEITHER
	// process — not this one, not the grandchild — is ever signalled by
	// the ladder at all.
	envGroupKill = "DISPATCH_EXEC_GROUP_KILL_TEST"

	// envLongSleep selects a fixture that ignores SIGTERM and sleeps for
	// longSleep, far longer than fixtureSleep. It exists specifically as
	// envGroupKill's grandchild: that fixture's own leader also ignores
	// SIGTERM and sleeps fixtureSleep (30s), and under a hypothetical
	// Setpgid regression neither process is ever actually signalled by
	// the kill ladder at all, so both would simply run out their own
	// timers and exit on their own. If the grandchild's timer were also
	// fixtureSleep, it would exit at roughly the same wall-clock moment
	// the leader's own timer does — which is also roughly when Run()
	// finally returns in that broken scenario — making the test's ESRCH
	// check on the grandchild's pid pass for the wrong reason (it expired
	// on its own, not because any signal reached it) instead of catching
	// the regression. Giving it a much longer timer means that if the
	// kill ladder never actually reaches it, it is still provably alive
	// when the test checks, and the ESRCH assertion is load-bearing on
	// its own rather than riding on coincidental timing.
	envLongSleep = "DISPATCH_EXEC_LONG_SLEEP_TEST"

	// envIgnoreSigtermLongSleep selects a fixture like envLongSleep, but
	// used as a *helper* left behind by a cooperative leader
	// (envLeaderExitsHelperSurvives) rather than as envGroupKill's
	// grandchild. Functionally identical to envLongSleep; kept as a
	// separate fixture rather than reused so each test's intent reads
	// clearly from which env var its process tree is built out of.
	envIgnoreSigtermLongSleep = "DISPATCH_EXEC_IGNORE_SIGTERM_LONG_SLEEP_TEST"

	// envLeaderExitsHelperSurvives selects the fixture for the C1
	// regression test (kill_unix_test.go): it forks a helper that ignores
	// SIGTERM and sleeps far longer than any bound the test asserts on
	// (envIgnoreSigtermLongSleep), writes that helper's pid to a file in
	// the request's OutputDir, and then — unlike envGroupKill's own
	// fixture — does *not* ignore SIGTERM itself, so the signal's default
	// disposition ends this leader almost immediately once the ladder's
	// first rung arrives. This is the shape terminate's escalation used
	// to get wrong: a leader that exits promptly on SIGTERM (standing in
	// for the production shim's own cooperative shutdown) while something
	// it forked keeps running. A version of the ladder that decided
	// whether to escalate to SIGKILL by asking only "has the leader
	// exited" would answer yes almost immediately and never send it,
	// leaving the helper alive indefinitely.
	envLeaderExitsHelperSurvives = "DISPATCH_EXEC_LEADER_EXITS_HELPER_SURVIVES_TEST"

	// envSigtermMarker selects a fixture for the C3 regression test
	// (kill_unix_test.go): unlike every other fixture in this file, it
	// carries no timeout or sleep of its own at all — it reads the
	// request only to learn OutputDir, then blocks indefinitely until it
	// receives an actual SIGTERM, at which point it writes a marker file
	// and exits promptly. That is what makes it load-bearing evidence
	// that the ladder's SIGTERM rung actually ran: this process has no
	// other way to end quickly, so the marker existing (and Run()
	// returning well under the grace period) can only mean SIGTERM was
	// sent and handled — not, for instance, that a deadline embedded in
	// the request itself caused a cooperative exit through some unrelated
	// path, and not that the ladder skipped straight to SIGKILL, which
	// this fixture cannot trap or write a marker in response to.
	envSigtermMarker = "DISPATCH_EXEC_SIGTERM_MARKER_TEST"

	// fixtureSleep is deliberately much longer than any bound the C1/C2
	// tests assert on, so a regression is caught by the test's own
	// timeout rather than by this sleep ever completing.
	fixtureSleep = 30 * time.Second

	// longSleep is deliberately much longer than fixtureSleep — see
	// envLongSleep's own doc comment for why that gap matters.
	longSleep = 5 * time.Minute
)

// TestMain lets this test binary act as its own sandbox child. The
// subprocess rung re-execs the running binary, so under test the child is
// this binary again; when the marker env var is set it runs the shim and
// exits instead of running tests.
func TestMain(m *testing.M) {
	switch {
	case os.Getenv("DISPATCH_EXEC_SHIM_TEST") != "":
		shim.Main(exectest.Handlers()...)
		return // unreachable; Main exits
	case os.Getenv(envLeakChild) != "":
		runLeakChild()
		return // unreachable; runLeakChild exits
	case os.Getenv(envSleepOnly) != "":
		// Stands in for a grandchild left running by a handler's own
		// subprocess: it holds whatever fds it inherited open and does
		// nothing else.
		time.Sleep(fixtureSleep)
		os.Exit(0)
		return
	case os.Getenv(envGroupKill) != "":
		runGroupKillFixture()
		return // unreachable; runGroupKillFixture exits
	case os.Getenv(envLongSleep) != "":
		signal.Ignore(syscall.SIGTERM)
		time.Sleep(longSleep)
		os.Exit(0)
		return
	case os.Getenv(envIgnoreSigtermLongSleep) != "":
		signal.Ignore(syscall.SIGTERM)
		time.Sleep(longSleep)
		os.Exit(0)
		return
	case os.Getenv(envLeaderExitsHelperSurvives) != "":
		runLeaderExitsHelperSurvivesFixture()
		return // unreachable; runLeaderExitsHelperSurvivesFixture exits
	case os.Getenv(envSigtermMarker) != "":
		runSigtermMarkerFixture()
		return // unreachable; runSigtermMarkerFixture exits
	}

	os.Exit(m.Run())
}

// runLeakChild is the envLeakChild fixture body. See its doc comment
// above for what it reproduces.
func runLeakChild() {
	in := os.NewFile(uintptr(fdFromEnv(shim.EnvRequestFD, 3)), "dispatch-exec-request")
	out := os.NewFile(uintptr(fdFromEnv(shim.EnvResultFD, 4)), "dispatch-exec-result")

	// Drain the request so the parent's write does not block on this
	// fixture; the content itself does not matter here.
	_, _ = wire.Decode(in)

	// CommandContext with context.Background() rather than Command, purely
	// to satisfy noctx; this fixture never cancels the grandchild via ctx
	// — it is deliberately left running, see the Start comment below.
	grandchild := osexec.CommandContext(context.Background(), os.Args[0])
	// Deliberately NOT append(os.Environ(), ...): this process's own
	// environment still carries envLeakChild (it is inherited, not
	// consumed), and os.Environ() would carry it straight into the
	// grandchild too — which TestMain's switch checks first, so the
	// grandchild would decide it is another leak-child fixture and spawn
	// its own grandchild, and so on without ever bottoming out. An
	// explicit, minimal env keeps this fixture to exactly the two
	// generations it means to create.
	grandchild.Env = []string{envSleepOnly + "=1"}
	grandchild.Stdout = os.Stdout
	grandchild.Stderr = os.Stderr
	grandchild.ExtraFiles = []*os.File{in, out}
	// Deliberately not waited on: it must outlive this process to
	// reproduce C1, exactly as a detached background job a handler
	// started would.
	_ = grandchild.Start()

	res := &exec.Result{Status: exec.StatusOK}
	_ = wire.Encode(out, &wire.Frame{Kind: wire.KindResult, Result: res})

	os.Exit(0)
}

// runGroupKillFixture is the envGroupKill fixture body. See its doc
// comment above for what it reproduces.
func runGroupKillFixture() {
	in := os.NewFile(uintptr(fdFromEnv(shim.EnvRequestFD, 3)), "dispatch-exec-request")

	frame, err := wire.Decode(in)
	if err != nil || frame.Request == nil {
		os.Exit(1)
		return
	}

	// Unlike the shim, which traps SIGTERM to cancel its handler's
	// context and shut down cleanly, this fixture ignores it outright —
	// standing in for a handler process that does not cooperate with the
	// first rung of the ladder at all, so only the SIGKILL half can end
	// it, and only if that SIGKILL actually reaches this process's whole
	// group rather than just its leader.
	signal.Ignore(syscall.SIGTERM)

	// CommandContext with context.Background() rather than Command, purely
	// to satisfy noctx; this fixture never cancels the grandchild via ctx
	// — it must outlive this process's own SIGTERM handling, exactly as a
	// native library's forked helper would.
	grandchild := osexec.CommandContext(context.Background(), os.Args[0])
	// Deliberately NOT append(os.Environ(), ...) — see runLeakChild's own
	// comment on the same line for why: this process's environment still
	// carries envGroupKill, and inheriting it wholesale would make the
	// grandchild decide it is another instance of this same fixture.
	//
	// envLongSleep, not envSleepOnly: see envLongSleep's own doc comment
	// for why this grandchild needs a sleep clearly longer than this
	// fixture's own fixtureSleep.
	grandchild.Env = []string{envLongSleep + "=1"}
	if err := grandchild.Start(); err != nil {
		os.Exit(1)
		return
	}

	// The parent test process reads this file after Run returns to learn
	// which pid to probe for liveness — it has no other way to learn a
	// pid this deep in a process tree it does not control directly.
	pidPath := filepath.Join(frame.Request.OutputDir, "grandchild.pid")
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(grandchild.Process.Pid)), 0o600); err != nil {
		os.Exit(1)
		return
	}

	// No result frame is written: this fixture is killed before it gets
	// the chance to, which is the point — the parent's classify call has
	// nothing to trust here but the process's own wait status.
	time.Sleep(fixtureSleep)
	os.Exit(0)
}

// runLeaderExitsHelperSurvivesFixture is the envLeaderExitsHelperSurvives
// fixture body. See its doc comment above for what it reproduces.
func runLeaderExitsHelperSurvivesFixture() {
	in := os.NewFile(uintptr(fdFromEnv(shim.EnvRequestFD, 3)), "dispatch-exec-request")

	frame, err := wire.Decode(in)
	if err != nil || frame.Request == nil {
		os.Exit(1)
		return
	}

	// CommandContext with context.Background() rather than Command, purely
	// to satisfy noctx; this fixture never cancels the helper via ctx — it
	// must outlive this leader, exactly as a native library's forked
	// helper would.
	helper := osexec.CommandContext(context.Background(), os.Args[0])
	// Deliberately NOT append(os.Environ(), ...) — see runLeakChild's own
	// comment on the same line for why.
	helper.Env = []string{envIgnoreSigtermLongSleep + "=1"}
	if err := helper.Start(); err != nil {
		os.Exit(1)
		return
	}

	pidPath := filepath.Join(frame.Request.OutputDir, "helper.pid")
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(helper.Process.Pid)), 0o600); err != nil {
		os.Exit(1)
		return
	}

	// Deliberately no signal.Ignore call here, unlike runGroupKillFixture:
	// this leader lets SIGTERM's default disposition (terminate) end it
	// almost immediately, standing in for the production shim's own
	// cooperative shutdown. The exact mechanism does not matter to this
	// fixture, only that the leader exits promptly, well before grace
	// elapses, while the helper it just forked (which does ignore
	// SIGTERM) does not.
	time.Sleep(fixtureSleep)
	os.Exit(0)
}

// runSigtermMarkerFixture is the envSigtermMarker fixture body. See its
// doc comment above for what it reproduces.
func runSigtermMarkerFixture() {
	in := os.NewFile(uintptr(fdFromEnv(shim.EnvRequestFD, 3)), "dispatch-exec-request")

	frame, err := wire.Decode(in)
	if err != nil || frame.Request == nil {
		os.Exit(1)
		return
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)

	// Blocks until the parent's kill ladder sends SIGTERM. Nothing else
	// in this fixture can end it — no deadline, no fixed sleep — so
	// reaching the line below is itself proof that a real SIGTERM arrived
	// and was handled, not a side effect of some unrelated timer.
	<-sigCh

	markerPath := filepath.Join(frame.Request.OutputDir, "sigterm-received")
	_ = os.WriteFile(markerPath, []byte("1"), 0o600)

	os.Exit(0)
}

// fdFromEnv mirrors shim's own unexported helper of the same name: it
// reads a file descriptor number from the named environment variable,
// falling back to def when unset or unparsable. Duplicated here rather
// than imported because shim does not export it, and this fixture is not
// part of the shim package.
func fdFromEnv(name string, def int) int {
	v, ok := os.LookupEnv(name)
	if !ok {
		return def
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}

	return n
}
