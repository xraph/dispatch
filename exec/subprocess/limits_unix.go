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
// On Unix, running the child as the worker's own uid — whether because no
// uid was configured at all, or because one was configured that happens to
// match the worker's own — leaves it able to read ~/.aws, /var/run/secrets,
// and the Dispatch config, which removes most of the value of this rung.
// Both shapes are refused unless the caller opts in explicitly via
// WithAllowSameUser: there is one switch for "I know this is unisolated,"
// not a separate one for each way of ending up unisolated. Rlimits have no
// equivalent launch-time check — they cannot fail until they are actually
// applied, which happens child-side in shim.Main (see EnvRlimitAS and
// friends in exec/shim), since Go cannot set a child's rlimits through
// SysProcAttr.
func checkLaunch(o options) error {
	if !o.hasUser {
		if o.allowSameUser {
			return nil
		}

		return fmt.Errorf(
			"dispatch/exec/subprocess: no uid configured; the child would run as the worker's own uid, " +
				"which defeats this rung's isolation — pass WithUser to configure a dedicated uid, or " +
				"WithAllowSameUser to accept running unisolated",
		)
	}

	if SameUserRefused(o.uid, o.allowSameUser) {
		return fmt.Errorf(
			"dispatch/exec/subprocess: configured uid %d matches the worker's own uid; "+
				"running the child as the worker defeats this rung's isolation — pass WithAllowSameUser to allow it",
			o.uid,
		)
	}

	return nil
}

// SameUserRefused reports whether checkLaunch would refuse to launch a
// child configured with this uid and allowSameUser setting because the uid
// matches the worker's own — one of the two conditions checkLaunch checks,
// factored out so configuration code can ask the same question before ever
// constructing an Executor, without keeping a second, independent copy of
// checkLaunch's own logic that could drift from it silently. It does not
// check o.hasUser, and so does not answer checkLaunch's other condition —
// no uid configured at all — which callers must check separately; see
// resolveExecutionOptions (extension/execution.go) for the shape that
// checking both looks like.
func SameUserRefused(uid int, allowSameUser bool) bool {
	return !allowSameUser && uid == os.Getuid()
}

// Available reports whether this platform can run the subprocess rung at
// all — always true on Unix. It only answers the platform question;
// checkLaunch (and SameUserRefused, above) answers different ones — does
// THIS configuration name a uid at all, and if so, is it the worker's own —
// which only checkLaunch itself catches on the actual launch path, once Run
// is called for a job attempt. Configuration code should call Available
// before ever constructing an Executor, so a deployment on an unsupported
// platform fails once, loudly, at startup — not job by job, on every
// attempt's launch failure, once it is already running. Check hasUser and
// call SameUserRefused alongside it for the same reason.
func Available() error { return nil }
