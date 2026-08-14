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
	if o.hasUser && !o.allowSameUser && o.uid == os.Getuid() {
		return fmt.Errorf(
			"dispatch/exec/subprocess: configured uid %d matches the worker's own uid; "+
				"running the child as the worker defeats this rung's isolation — pass WithAllowSameUser to allow it",
			o.uid,
		)
	}

	return nil
}

// Available reports whether this platform can run the subprocess rung at
// all — always true on Unix. checkLaunch above catches the same class of
// problem (a launch that would gut this rung's isolation), but only once
// Run is actually called for a job attempt. Configuration code should
// call Available before ever constructing an Executor, so a deployment
// that asks for this rung fails once, loudly, at startup — not job by
// job, on every attempt's launch failure, once it is already running.
func Available() error { return nil }
