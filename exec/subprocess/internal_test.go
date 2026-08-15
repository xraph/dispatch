package subprocess

// This file is package subprocess (internal), not subprocess_test,
// deliberately breaking the external-tests-only convention the rest of
// this package follows — precedent already set by
// exec/shim/internal_test.go for the same reason: classify is unexported,
// and the regression this covers cannot be forced from outside the
// package. The race is in Go's own select scheduling (which of two
// simultaneously-ready channels it picks), not in anything a black-box
// caller of Run can control or observe reliably; a black-box repro of it
// against the pre-fix code took a 20-iteration loop to reproduce (first
// failing on the ninth run), so a single-shot black-box version would
// pass most of the time and be worse than no test at all. Calling
// classify directly with synthetic inputs pins its contract down instead.

import (
	"slices"
	"strings"
	"testing"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/shim"
	"github.com/xraph/dispatch/exec/wire"
	"github.com/xraph/dispatch/resource"
)

func TestClassifyTimedOutOverridesADecodedFrame(t *testing.T) {
	e := &Executor{}
	frame := &wire.Frame{
		Kind:   wire.KindResult,
		Result: &exec.Result{Status: exec.StatusOK},
	}

	tests := []struct {
		name     string
		timedOut bool
		want     exec.Status
	}{
		{
			// The regression itself: round 1 had classify trust an
			// unsignalled decoded frame over timedOut, which made
			// StatusTimeout unreachable for any cooperative handler —
			// the shim already traps SIGTERM and writes a Result frame
			// on its way out, and Task 6 adds the parent's SIGTERM half
			// of the kill ladder, so "the tracked process finishes
			// right as the deadline fires" is a live shape, not a
			// theoretical one. waitLoop now only ever sets timedOut
			// after confirming, non-blockingly, that waitCh had not
			// already delivered — see its comment in executor.go — so
			// by the time classify sees timedOut, it must win
			// regardless of what the frame says.
			name:     "timed out overrides a clean frame",
			timedOut: true,
			want:     exec.StatusTimeout,
		},
		{
			// Contrast case: without timedOut, the same frame is
			// trusted, proving the table above is exercising the
			// branch it claims to, not returning a constant.
			name:     "no timeout trusts the frame",
			timedOut: false,
			want:     exec.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// ps == nil is already tolerated by processOutcome (exit
			// code -1, signal 0, signalled false), so no synthesised
			// *os.ProcessState is needed to reach either branch here.
			res := e.classify(&exec.Request{}, frame, nil, nil, nil, tt.timedOut, false)
			if res.Status != tt.want {
				t.Errorf("Status = %q, want %q", res.Status, tt.want)
			}
		})
	}
}

// TestBuildEnvCarriesRlimits pins buildEnv's construction of the
// DISPATCH_RLIMIT_* variables directly, without spawning a real child —
// the child-side enforcement itself is covered end to end by
// TestRlimitsAreAppliedChildSide (limits_unix_test.go), but that test can
// only observe the *effect* of a limit landing, not which ones buildEnv
// actually decided to send. This is the regression net for the two rules
// that effect can't distinguish: RLIMIT_CORE always going out as "0"
// regardless of what's configured, and every other field being omitted
// entirely rather than sent as "0" when left at its zero value.
func TestBuildEnvCarriesRlimits(t *testing.T) {
	tests := []struct {
		name          string
		hasRlimits    bool
		rlimits       Rlimits
		requestLimits resource.Set
		wantHas       []string
		wantAbsent    []string
	}{
		{
			name:       "no WithRlimits call still forces core to zero",
			hasRlimits: false,
			wantHas:    []string{shim.EnvRlimitCore + "=0"},
			wantAbsent: []string{shim.EnvRlimitAS, shim.EnvRlimitNoFile, shim.EnvRlimitNProc, shim.EnvRlimitFSize},
		},
		{
			name:       "configured fields are sent, zero fields are omitted",
			hasRlimits: true,
			rlimits:    Rlimits{AddressSpace: 1 << 30, NoFile: 64, Core: 999},
			wantHas: []string{
				shim.EnvRlimitCore + "=0", // Core forced to zero even though 999 was configured
				shim.EnvRlimitAS + "=1073741824",
				shim.EnvRlimitNoFile + "=64",
			},
			wantAbsent: []string{shim.EnvRlimitNProc, shim.EnvRlimitFSize}, // left at zero, so omitted
		},
		{
			// job.WithResourceLimits(resource.MemoryBytes(...)) resolved at
			// enqueue and carried across on Request.ResourceLimits. Per-job
			// must win over the deployment-wide WithRlimits default.
			name:          "a job's own memory limit wins over the deployment-wide default",
			hasRlimits:    true,
			rlimits:       Rlimits{AddressSpace: 1 << 30},
			requestLimits: resource.Set{resource.Memory: 2 << 30},
			wantHas:       []string{shim.EnvRlimitAS + "=2147483648"},
		},
		{
			// The overwhelming majority of jobs declare nothing: the
			// deployment-wide default must still apply exactly as before.
			name:          "deployment-wide default still applies when the job declares nothing",
			hasRlimits:    true,
			rlimits:       Rlimits{AddressSpace: 1 << 30},
			requestLimits: nil,
			wantHas:       []string{shim.EnvRlimitAS + "=1073741824"},
		},
		{
			// A job's own limit must apply even with no deployment-wide
			// WithRlimits call at all — the per-job ceiling is not merely
			// an override of a configured default, it is enforced on its
			// own.
			name:          "a job's own memory limit applies with no deployment-wide default configured",
			hasRlimits:    false,
			requestLimits: resource.Set{resource.Memory: 512 << 20},
			wantHas:       []string{shim.EnvRlimitAS + "=536870912"},
		},
		{
			// resource.CPU has no clean rlimit mapping (see buildEnv's
			// comment) and must never leak into RLIMIT_AS.
			name:          "a CPU limit alone never produces RLIMIT_AS",
			hasRlimits:    false,
			requestLimits: resource.Set{resource.CPU: 2000},
			wantAbsent:    []string{shim.EnvRlimitAS},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{opts: options{rlimits: tt.rlimits, hasRlimits: tt.hasRlimits}}
			env := e.buildEnv(&exec.Request{ResourceLimits: tt.requestLimits})

			for _, want := range tt.wantHas {
				if !slices.Contains(env, want) {
					t.Errorf("buildEnv() = %v, want to contain %q", env, want)
				}
			}
			for _, prefix := range tt.wantAbsent {
				for _, kv := range env {
					if strings.HasPrefix(kv, prefix+"=") {
						t.Errorf("buildEnv() contains %q, want %s absent", kv, prefix)
					}
				}
			}
		})
	}
}

// TestBuildEnvDoesNotInheritTheParentEnvironment proves buildEnv's fixed
// PATH/HOME/TMPDIR allowlist (see its own doc comment, and spec §6's "the
// child's environment is constructed, not inherited") actually excludes
// everything else in the worker's own environment, not just everything
// this test happens to think of. A sentinel set in this test process's own
// environment stands in for a credential the worker might carry — a DSN,
// an API key — and must not reach the child's constructed environment.
//
// Nothing else in this package's suite would catch a regression here: the
// rlimit tests above never configure WithEnv and don't need to (rlimit
// vars are unconditional), TestRunSuccess and its neighbors do configure
// WithEnv but only ever assert on the attempt's Status, never on what
// buildEnv actually sent, and a child that received the entire parent
// os.Environ() would still run those fixtures to completion successfully.
// A review round demonstrated exactly that: replacing the allowlist with a
// full os.Environ() copy passed the whole phase test suite clean.
func TestBuildEnvDoesNotInheritTheParentEnvironment(t *testing.T) {
	const sentinel = "DISPATCH_TEST_PARENT_ONLY_SENTINEL"
	t.Setenv(sentinel, "leaked-credential-value")

	e := &Executor{}
	env := e.buildEnv(&exec.Request{})

	for _, kv := range env {
		if strings.HasPrefix(kv, sentinel+"=") {
			t.Fatalf("buildEnv() = %v, contains %q — the child's environment must be "+
				"constructed from an allowlist, not inherited from the worker's own os.Environ()",
				env, kv)
		}
	}
}
