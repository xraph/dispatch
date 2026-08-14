package subprocess_test

import (
	"context"
	"os"
	osexec "os/exec"
	"strconv"
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

	// fixtureSleep is deliberately much longer than any bound the C1/C2
	// tests assert on, so a regression is caught by the test's own
	// timeout rather than by this sleep ever completing.
	fixtureSleep = 30 * time.Second
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
