package exectest_test

import (
	"testing"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/exectest"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/job"
)

func TestInProcessConformance(t *testing.T) {
	exectest.RunSuite(t, "inprocess", func(*testing.T) exec.Executor {
		r := job.NewRegistry()
		for _, d := range exectest.Handlers() {
			d.Register(r)
		}

		return inproc.New(r)
	}, exectest.Capabilities{
		// In-process enforces nothing: it cannot kill a handler that
		// ignores cancellation, it has no separate address space to
		// measure, and a panic propagates to the caller, which is what
		// the worker's recover middleware is for.
		Enforces:      false,
		ReportsUsage:  false,
		IsolatesPanic: false,
	})
}
