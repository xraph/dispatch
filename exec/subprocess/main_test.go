package subprocess_test

import (
	"os"
	"testing"

	"github.com/xraph/dispatch/exec/exectest"
	"github.com/xraph/dispatch/exec/shim"
)

// TestMain lets this test binary act as its own sandbox child. The
// subprocess rung re-execs the running binary, so under test the child is
// this binary again; when the marker env var is set it runs the shim and
// exits instead of running tests.
func TestMain(m *testing.M) {
	if os.Getenv("DISPATCH_EXEC_SHIM_TEST") != "" {
		shim.Main(exectest.Handlers()...)
		return // unreachable; Main exits
	}

	os.Exit(m.Run())
}
