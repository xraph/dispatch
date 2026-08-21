package exectest_test

import (
	"strings"
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

func TestCheckCapabilities(t *testing.T) {
	// Capabilities describes variation between rungs, not an opt-out: a
	// rung claiming out-of-process isolation cannot also claim it is unable
	// to kill a handler, and so skip the test that proves it.
	tests := []struct {
		name    string
		level   exec.Level
		caps    exectest.Capabilities
		wantErr bool
	}{
		{
			name:  "in-process may claim nothing",
			level: exec.LevelNone,
			caps:  exectest.Capabilities{},
		},
		{
			name:  "out-of-process claiming both is consistent",
			level: exec.LevelProcess,
			caps:  exectest.Capabilities{Enforces: true, IsolatesPanic: true},
		},
		{
			name:    "out-of-process disclaiming enforcement is not",
			level:   exec.LevelProcess,
			caps:    exectest.Capabilities{Enforces: false, IsolatesPanic: true},
			wantErr: true,
		},
		{
			name:    "out-of-process disclaiming panic isolation is not",
			level:   exec.LevelProcess,
			caps:    exectest.Capabilities{Enforces: true, IsolatesPanic: false},
			wantErr: true,
		},
		{
			name:    "a sandboxed rung is held to the same bar",
			level:   exec.LevelSandboxed,
			caps:    exectest.Capabilities{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := exectest.CheckCapabilities("subject", tt.level, tt.caps)
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("CheckCapabilities() = %v, want error: %v", err, tt.wantErr)
			}
			if tt.wantErr && !strings.Contains(err.Error(), "subject") {
				t.Errorf("CheckCapabilities() = %q, want it to name the executor", err)
			}
		})
	}
}
