//go:build unix

package shim

// package shim (internal), not shim_test — same rationale as
// internal_test.go: mainExitCode's strict-vs-warning routing and
// isKnownUnsupported/joinRlimitFailures are unexported. This file is
// unix-tagged, unlike internal_test.go, specifically so it can be more
// aggressive about exercising the real rlimit path (env vars,
// mainExitCode's early-return branch) without needing this test binary
// to also build on non-Unix platforms it does not target.
//
// What this file does NOT do: call applyRlimits with a value that would
// actually succeed against a real resource like RLIMIT_AS or
// RLIMIT_NOFILE. callMainExitCode (internal_test.go) runs mainExitCode
// in this test binary's own process, not a forked child — a rlimit that
// actually took effect here would permanently lower it for every
// subsequent test in this same test binary run, since rlimits can only
// be lowered without privilege, never raised back. Every case below uses
// a value that fails before syscall.Setrlimit is ever called (a negative
// number), which is safe by construction. The cases that need a real
// Setrlimit outcome — proving WithStrictRlimits doesn't fire for a
// platform's own structural refusal, or does fire for a real failure —
// are exec/subprocess's TestStrictRlimitsFailsLaunchOnUnexpectedFailure
// and TestStrictRlimitsToleratesKnownUnsupported (limits_unix_test.go),
// which fork a real child and so cannot pollute this process.

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"syscall"
	"testing"

	"github.com/xraph/dispatch/job"
)

func TestIsKnownUnsupported(t *testing.T) {
	tests := []struct {
		name  string
		label string
		err   error
		want  bool
	}{
		{
			name:  "darwin RLIMIT_AS EINVAL is known-unsupported",
			label: "RLIMIT_AS",
			err:   syscall.EINVAL,
			want:  isDarwin(),
		},
		{
			name:  "RLIMIT_AS EPERM is not known-unsupported, even on Darwin",
			label: "RLIMIT_AS",
			err:   syscall.EPERM,
			want:  false,
		},
		{
			name:  "a different label with EINVAL is not known-unsupported",
			label: "RLIMIT_NOFILE",
			err:   syscall.EINVAL,
			want:  false,
		},
		{
			name:  "a wrapped EINVAL still matches, via errors.Is",
			label: "RLIMIT_AS",
			err:   fmt.Errorf("setrlimit: %w", syscall.EINVAL),
			want:  isDarwin(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isKnownUnsupported(tt.label, tt.err); got != tt.want {
				t.Errorf("isKnownUnsupported(%q, %v) = %v, want %v", tt.label, tt.err, got, tt.want)
			}
		})
	}
}

func TestJoinRlimitFailures(t *testing.T) {
	tests := []struct {
		name     string
		failures []rlimitFailure
		want     string
	}{
		{name: "empty", failures: nil, want: ""},
		{
			name:     "one",
			failures: []rlimitFailure{{"RLIMIT_NOFILE", errors.New("value \"-1\" is invalid")}},
			want:     `RLIMIT_NOFILE: value "-1" is invalid`,
		},
		{
			name: "two, joined with semicolons",
			failures: []rlimitFailure{
				{"RLIMIT_NOFILE", errors.New("boom")},
				{"RLIMIT_FSIZE", errors.New("also boom")},
			},
			want: "RLIMIT_NOFILE: boom; RLIMIT_FSIZE: also boom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := joinRlimitFailures(tt.failures); got != tt.want {
				t.Errorf("joinRlimitFailures() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestMainExitCodeStrictRlimitFailsLaunch proves the EnvRlimitStrict
// wiring end to end through mainExitCode: an rlimit value that fails
// before Setrlimit is ever reached (invalid, safe to run in this
// process — see the file doc comment) becomes exit 1 with strict mode
// set, and stays exit 0 (a warning, per applyRlimits) without it.
func TestMainExitCodeStrictRlimitFailsLaunch(t *testing.T) {
	defs := []job.Registrable{
		job.NewDefinition("internal.ok", func(context.Context, struct{}) error { return nil }),
	}

	t.Run("default: invalid rlimit is a warning, launch proceeds", func(t *testing.T) {
		t.Setenv(EnvRlimitNoFile, "-1")

		if got := callMainExitCode(t, defs, internalReq(t, "internal.ok")); got != 0 {
			t.Errorf("mainExitCode() = %d, want 0", got)
		}
	})

	t.Run("strict: invalid rlimit fails the launch", func(t *testing.T) {
		t.Setenv(EnvRlimitNoFile, "-1")
		t.Setenv(EnvRlimitStrict, "1")

		if got := callMainExitCode(t, defs, internalReq(t, "internal.ok")); got != 1 {
			t.Errorf("mainExitCode() = %d, want 1", got)
		}
	})
}

func isDarwin() bool { return runtime.GOOS == "darwin" }
