package subprocess_test

import (
	"os"
	"testing"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/exectest"
	"github.com/xraph/dispatch/exec/subprocess"
)

// TestSubprocessConformance runs this rung through the shared conformance
// suite with enforcement claimed, not just inherited: unlike the in-process
// rung, this one can actually kill a handler that ignores cancellation and
// cannot let a handler's panic reach the caller, so it claims Enforces and
// IsolatesPanic and the suite's DeadlineEnforced and PanicIsolated cases
// (plus the cooperative-deadline cases gated on Enforces) run for real
// instead of being skipped. ReportsUsage stays false: cgroups and PeakRSS
// are Phase 3, and a capability flag that lies is worse than one that is
// absent, because later rungs (OCI, Kubernetes) copy this file as their
// starting point.
func TestSubprocessConformance(t *testing.T) {
	exectest.RunSuite(t, "subprocess", func(*testing.T) exec.Executor {
		return subprocess.New(
			subprocess.WithBinary(os.Args[0]),
			subprocess.WithEnv(map[string]string{"DISPATCH_EXEC_SHIM_TEST": "1"}),
			subprocess.WithAllowSameUser(), // CI cannot drop privileges
		)
	}, exectest.Capabilities{
		Enforces:      true,
		IsolatesPanic: true,
		ReportsUsage:  false, // cgroups and PeakRSS are Phase 3
	})
}
