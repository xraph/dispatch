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
