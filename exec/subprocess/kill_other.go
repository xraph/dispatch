//go:build !unix

package subprocess

import (
	osexec "os/exec"
	"time"
)

// terminate falls back to an immediate, non-graceful kill outside Unix,
// since there is neither a process group nor a SIGTERM to build a grace
// period ladder out of on this build. grace is accepted only for
// signature symmetry with the Unix build's terminate (kill_unix.go) and
// is otherwise unused. This path is never reached in practice —
// checkLaunch (limits_other.go) refuses to start this rung at all outside
// Unix — so it exists only to keep the package compiling here.
func terminate(cmd *osexec.Cmd, _ time.Duration) {
	if cmd.Process == nil {
		return
	}

	_ = killGroup(cmd, 0) //nolint:errcheck // best-effort; unreachable in practice, see doc comment above
}
