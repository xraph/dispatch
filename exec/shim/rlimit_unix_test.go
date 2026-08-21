//go:build unix

package shim

// package shim (internal), not shim_test — same rationale as
// internal_test.go: mainExitCode's strict-vs-warning routing and
// isKnownUnsupported/joinRlimitFailures are unexported. This file is
// unix-tagged for the same practical reason internal_test.go is (see its
// own doc comment): the rlimit path this file exercises — env vars,
// mainExitCode's early-return branch, applyOne, isKnownUnsupported — is
// built from syscall.Setrlimit and the RLIMIT_* constants, which do not
// exist in Go's syscall package on Windows.
//
// What this file does NOT do: call applyRlimits, or applyOne directly,
// with a value that would actually succeed against a real resource like
// RLIMIT_AS or RLIMIT_NOFILE. callMainExitCode (internal_test.go) runs
// mainExitCode in this test binary's own process, not a forked child — a
// rlimit that actually took effect here would permanently lower it for
// every subsequent test in this same test binary run, since rlimits can
// only be lowered without privilege, never raised back. Most cases below
// use a value applyRlimits rejects before ever calling Setrlimit (a
// negative number); TestApplyOneUnverifiedResourceIsAFailure is the
// exception — it uses a positive, plausible-looking value ("12345") and
// is still safe, because its synthetic rlimitSpec sets resourceOK false,
// which makes applyOne return a failure without ever reaching Setrlimit
// regardless of what the value is. The cases that need a real Setrlimit
// outcome — proving WithStrictRlimits doesn't fire for a platform's own
// structural refusal, or does fire for a real failure — are
// exec/subprocess's TestStrictRlimitsFailsLaunchOnUnexpectedFailure and
// TestStrictRlimitsToleratesKnownUnsupported (limits_unix_test.go), which
// fork a real child and so cannot pollute this process.

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

// TestApplyOneUnverifiedResourceIsAFailure is the regression test for the
// gap the review round 2 found: resourceOK false used to log and
// `continue` without ever appending to the failures slice, which meant
// WithStrictRlimits gave no guarantee at all on a platform this package
// has not verified a resource number for — the one case an operator
// opting into strictness needs it most. A synthetic rlimitSpec is used
// rather than a real unverified platform (this rung's CI and dev machine
// are both verified for every field WithRlimits exposes today), which is
// exactly what applyOne being split out of applyRlimits is for.
func TestApplyOneUnverifiedResourceIsAFailure(t *testing.T) {
	spec := rlimitSpec{env: "DISPATCH_TEST_UNVERIFIED", resource: 0, resourceOK: false, label: "RLIMIT_TEST"}

	f, hasFailure := applyOne(spec, "12345")
	if !hasFailure {
		t.Fatal("applyOne() reported no failure for an unverified resource number, want a failure so WithStrictRlimits actually refuses the launch")
	}
	if f.label != "RLIMIT_TEST" {
		t.Errorf("failure label = %q, want %q", f.label, "RLIMIT_TEST")
	}
	if f.err == nil {
		t.Error("failure err = nil, want a non-nil reason")
	}
}

// A resourceOK-true / real-Setrlimit contrast case (proving
// isKnownUnsupported actually reaches applyOne's result rather than only
// being unit-tested in isolation) deliberately does not live here: doing
// that against RLIMIT_AS in-process, in this test binary, would risk
// exactly the pollution this file's doc comment describes avoiding — on
// Linux, a generous-enough value would not fail at all, and would then
// apply for real, for the rest of this test binary's life.
// TestStrictRlimitsToleratesKnownUnsupported (exec/subprocess,
// limits_unix_test.go) covers that case instead, through a real forked
// child, which cannot pollute anything once it exits.

func isDarwin() bool { return runtime.GOOS == "darwin" }
