//go:build !unix

package subprocess

import "errors"

// checkLaunch refuses unconditionally outside Unix. Process groups,
// Credential-based uid dropping, and rlimits are all POSIX concepts this
// rung does not emulate anywhere else — running the child unconfined would
// make Run look like it succeeded while providing none of the isolation
// this package exists for, which is worse than failing loudly. So it fails
// loudly instead, regardless of what o carries.
func checkLaunch(options) error {
	return errors.New("dispatch/exec/subprocess: the subprocess rung requires a Unix platform")
}

// SameUserRefused always reports false outside Unix. Available, below,
// already refuses the whole rung on this platform, so configuration code
// that calls Available first — as it must — never reaches this question
// in practice; it exists here only so callers that are not platform-
// specific themselves (extension.resolveExecutionOptions) compile on
// every platform this package supports. See checkLaunch and its Unix
// counterpart in limits_unix.go, the one that matters.
func SameUserRefused(int, bool) bool { return false }

// Available reports that this platform cannot run the subprocess rung at
// all, for the identical reason checkLaunch refuses above. Configuration
// code should call this before ever constructing an Executor: checkLaunch
// catches the same condition too, but only once Run is actually called
// for a job attempt, which turns a startup misconfiguration into a
// per-job launch failure discovered in production instead of a single
// clear error at boot.
func Available() error {
	return errors.New("dispatch/exec/subprocess: the subprocess rung requires a Unix platform")
}
