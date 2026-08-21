//go:build unix

package shim

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"syscall"
)

// rlimitSpec pairs the environment variable the parent sets with the raw
// setrlimit(2) resource number and a label for diagnostics. resourceOK is
// false when this platform's exact resource number for the limit has not
// been verified — see rlimitAS and rlimitNProc — in which case
// applyRlimits skips the syscall.Setrlimit call entirely rather than risk
// handing it the wrong resource number, and treats that skip as a
// failure like any other: this is Dispatch not having checked the
// constant for this platform, a library gap, not the platform itself
// refusing the limit — see isKnownUnsupported for that distinct case,
// which only applies to an actual syscall.Setrlimit failure.
type rlimitSpec struct {
	env        string
	resource   int
	resourceOK bool
	label      string
}

// rlimitSpecs lists every limit applyRlimits knows how to apply.
//
// RLIMIT_CORE, RLIMIT_NOFILE, and RLIMIT_FSIZE are taken from the
// syscall package's own named constants, which is safe everywhere this
// file builds (verified against Go's generated zerrors tables for every
// platform under the "unix" build constraint: aix, darwin, dragonfly,
// freebsd, illumos/solaris, linux — including its mips/mips64 variants,
// which differ from every other linux arch — netbsd, and openbsd): the
// symbol is present on all of them, and Go resolves it to the correct
// platform- and arch-specific number automatically. RLIMIT_AS and
// RLIMIT_NPROC do not have that property — see rlimitAS and rlimitNProc
// for why each needs its own platform-aware resolution instead of a bare
// syscall.RLIMIT_* reference.
func rlimitSpecs() []rlimitSpec {
	as, asOK := rlimitAS()
	nproc, nprocOK := rlimitNProc()

	return []rlimitSpec{
		// Applied first: this is the one limit the parent always sends,
		// and it is the one the worker's own security promise depends
		// on most directly.
		{EnvRlimitCore, syscall.RLIMIT_CORE, true, "RLIMIT_CORE"},
		{EnvRlimitAS, as, asOK, "RLIMIT_AS"},
		{EnvRlimitNoFile, syscall.RLIMIT_NOFILE, true, "RLIMIT_NOFILE"},
		{EnvRlimitFSize, syscall.RLIMIT_FSIZE, true, "RLIMIT_FSIZE"},
		{EnvRlimitNProc, nproc, nprocOK, "RLIMIT_NPROC"},
	}
}

// rlimitNProc reports the raw RLIMIT_NPROC resource number for the
// current platform and GOARCH, and whether it is verified.
//
// Go's syscall package does not export RLIMIT_NPROC on any platform — it
// was trimmed from the generated zerrors tables along with
// RLIMIT_MEMLOCK and RLIMIT_RSS — so every value below is a raw resource
// number taken from each platform's own <sys/resource.h> layout, not a
// named constant. Getting this wrong is worse than getting RLIMIT_AS
// wrong: an incorrect resource number does not fail to compile or even
// fail at runtime, it just silently caps a *different* resource. This
// task's original version hardcoded 6 for every non-Darwin platform,
// which is only correct for Linux's "asm-generic" architectures — on
// Linux/mips and Linux/mips64, position 6 is RLIMIT_AS, not
// RLIMIT_NPROC, so a configured NProc limit would have silently become
// an AS limit there instead, killing the child on its first allocation
// while the logs claimed NProc was applied fine.
//
// Verified per (GOOS, GOARCH) against Go's own generated
// syscall/zerrors_*.go tables plus the reported values for platforms Go
// does not export the surrounding constants for at all:
//   - linux, non-mips (amd64, arm64, 386, arm, riscv64, ppc64, ppc64le,
//     s390x, loong64): 6, matching <asm-generic/resource.h> — the same
//     ordering behind Go's own AS=9/NOFILE=7 on every one of these arches.
//   - linux, mips family (mips, mipsle, mips64, mips64le): 8, matching
//     that family's own distinct resource.h layout (also the reason its
//     AS=6 and NOFILE=5 diverge from every other linux arch).
//   - darwin: 7.
//   - freebsd: 7.
//
// Every other platform under the "unix" build constraint — netbsd,
// openbsd, dragonfly, solaris/illumos, aix, android, ios — returns
// ok=false rather than a guessed number: this rung has not verified
// RLIMIT_NPROC's position for any of them, and a wrong guess here is a
// silent security regression, not a build failure or a loud one, so
// applyRlimits skips it with a warning instead of risking that.
func rlimitNProc() (resource int, ok bool) {
	switch runtime.GOOS {
	case "linux":
		switch runtime.GOARCH {
		case "mips", "mipsle", "mips64", "mips64le":
			return 8, true
		default:
			return 6, true
		}
	case "darwin":
		return 7, true
	case "freebsd":
		return 7, true
	default:
		return 0, false
	}
}

// applyRlimits reads every limit the parent set in the environment (see
// EnvRlimitAS and friends) and applies it via syscall.Setrlimit before the
// handler ever runs. It returns every failure that was not judged "the
// platform itself refuses this limit" — see isKnownUnsupported — for
// mainExitCode to act on when EnvRlimitStrict (subprocess.WithStrictRlimits)
// is set; regardless of that, every failure is always logged to stderr
// here, whether it counts toward strict mode or not.
//
// A limit whose env var is unset is left alone — that is how buildEnv
// says "no opinion" for anything but RLIMIT_CORE, which it always sends.
// A limit whose resource number is not verified for this platform
// (resourceOK false — see rlimitAS and rlimitNProc) is skipped without
// ever calling Setrlimit, since there is no safe resource number to
// pass — but this counts as a failure like any other, not as
// known-unsupported: it is Dispatch not having checked the constant for
// this platform, a library gap that an update to rlimitAS/rlimitNProc
// could close, which is a different thing from the platform's own kernel
// refusing the limit (isKnownUnsupported, below) — an operator who asked
// for WithStrictRlimits gets told either way, since "we haven't verified
// this" is exactly the kind of silent gap strict mode exists to surface.
// A malformed value the parent sent (which should never happen — buildEnv
// only ever writes strconv.FormatInt output — but is not trusted blindly
// here regardless) is the same: a failure, unconditionally.
//
// By default — without WithStrictRlimits — every failure here, of any
// kind, is logged and otherwise ignored rather than treated as a launch
// failure, on the reasoning that an individual rlimit is one layer among
// several this rung provides — the uid boundary and the process-group
// kill hold regardless — and because Darwin's kernel rejects
// setrlimit(RLIMIT_AS, ...) outright (EINVAL) no matter what value is
// requested, so treating every failure as fatal by default would make
// this rung unusable there in practice. WithStrictRlimits opts a specific
// Executor out of that default when an operator has decided a limit not
// applying — for any reason, verified-but-refused or simply unverified —
// is worse than the attempt not running at all.
//
// Every message here goes to stderr, not through the logger the request
// carries: WithLogger's own default is a no-op logger, so anything routed
// through it would be invisible unless the operator opted in, which is
// backwards for a message that exists specifically to warn the operator.
func applyRlimits() []rlimitFailure {
	var failures []rlimitFailure

	for _, s := range rlimitSpecs() {
		v, ok := os.LookupEnv(s.env)
		if !ok {
			continue
		}

		if f, hasFailure := applyOne(s, v); hasFailure {
			failures = append(failures, f)
		}
	}

	return failures
}

// applyOne applies a single spec whose env var is set to v, and reports
// the failure it produced, if any. Split out from applyRlimits so the
// resourceOK-false path — reachable in practice only on a platform this
// package has not verified any spec's resource number for, none of which
// this rung's CI or dev machine are — can be exercised directly by a
// test on any platform, by constructing a synthetic rlimitSpec, rather
// than requiring an actual unverified platform to prove it produces a
// failure instead of a silent skip.
func applyOne(s rlimitSpec, v string) (rlimitFailure, bool) {
	if !s.resourceOK {
		err := fmt.Errorf("this platform's resource number for %s is not verified by dispatch, refusing to guess", s.label)
		fmt.Fprintf(os.Stderr, "dispatch/exec/shim: %s=%s requested but %v, skipping\n", s.label, v, err)

		return rlimitFailure{s.label, err}, true
	}

	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		err = fmt.Errorf("value %q is invalid", v)
		fmt.Fprintf(os.Stderr, "dispatch/exec/shim: %s %v, skipping\n", s.label, err)

		return rlimitFailure{s.label, err}, true
	}

	if err := syscall.Setrlimit(s.resource, newRlimit(n)); err != nil {
		fmt.Fprintf(os.Stderr, "dispatch/exec/shim: setrlimit %s=%d failed, continuing without it: %v\n", s.label, n, err)

		if !isKnownUnsupported(s.label, err) {
			return rlimitFailure{s.label, err}, true
		}
	}

	return rlimitFailure{}, false
}

// isKnownUnsupported reports whether an actual syscall.Setrlimit failure
// — one that ran against a resource number this package has verified,
// via a resourceOK spec — is a structural, permanent fact about the
// current kernel rather than a misconfiguration. The standing example is
// Darwin, whose kernel rejects setrlimit(RLIMIT_AS, ...) with EINVAL
// unconditionally, for any value: the kernel is saying no, not Dispatch
// declining to guess. Anything else — most concretely EPERM because the
// requested value exceeds the process's own hard limit — is treated as
// unexpected, since on a platform that does support the limit, that
// shape of failure means the configured value itself is the problem.
//
// This function only ever sees a failure from a real Setrlimit call.
// resourceOK-false skips (rlimitSpecs, applyRlimits) never reach here at
// all — those are a library gap, not a platform refusal, and applyRlimits
// already counts them as failures unconditionally without asking this
// function anything.
func isKnownUnsupported(label string, err error) bool {
	return runtime.GOOS == "darwin" && label == "RLIMIT_AS" && errors.Is(err, syscall.EINVAL)
}
