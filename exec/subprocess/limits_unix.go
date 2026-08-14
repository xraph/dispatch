//go:build unix

package subprocess

import (
	"fmt"
	"os"
)

// checkLaunch runs at the top of Run, before any pipe or process is
// created, and refuses to launch when the configured options would gut
// this rung's isolation.
//
// On Unix the only thing that can go wrong here is a configured uid that
// matches the worker's own: running the child as the worker leaves it able
// to read ~/.aws, /var/run/secrets, and the Dispatch config, which removes
// most of the value of this rung, so it is refused unless the caller opts
// in explicitly via WithAllowSameUser. Rlimits have no equivalent launch-
// time check — they cannot fail until they are actually applied, which
// happens child-side in shim.Main (see EnvRlimitAS and friends in
// exec/shim), since Go cannot set a child's rlimits through SysProcAttr.
func checkLaunch(o options) error {
	if o.hasUser && SameUserRefused(o.uid, o.allowSameUser) {
		return fmt.Errorf(
			"dispatch/exec/subprocess: configured uid %d matches the worker's own uid; "+
				"running the child as the worker defeats this rung's isolation — pass WithAllowSameUser to allow it",
			o.uid,
		)
	}

	return nil
}

// SameUserRefused reports whether checkLaunch would refuse to launch a
// child configured with this uid and allowSameUser setting — the exact
// condition above, factored out so configuration code can ask the same
// question before ever constructing an Executor, without keeping a
// second, independent copy of checkLaunch's own logic that could drift
// from it silently. It does not check o.hasUser: a caller asking this
// question already knows whether a uid was configured at all.
func SameUserRefused(uid int, allowSameUser bool) bool {
	return !allowSameUser && uid == os.Getuid()
}

// Available reports whether this platform can run the subprocess rung at
// all — always true on Unix. It only answers the platform question;
// checkLaunch (and SameUserRefused, above) answers a different one — is
// THIS configuration's uid the worker's own — which only checkLaunch
// itself catches on the actual launch path, once Run is called for a job
// attempt. Configuration code should call Available before ever
// constructing an Executor, so a deployment on an unsupported platform
// fails once, loudly, at startup — not job by job, on every attempt's
// launch failure, once it is already running. Call SameUserRefused
// alongside it for the same reason, if a uid is configured.
func Available() error { return nil }
