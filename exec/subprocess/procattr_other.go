//go:build !unix

package subprocess

import (
	osexec "os/exec"
	"syscall"
)

// sysProcAttr is a no-op outside Unix: process groups and Credential-based
// uid dropping are both POSIX concepts this rung does not emulate anywhere
// else, and this package does not otherwise claim to support a non-Unix
// platform. checkLaunch (limits_other.go) is what actually refuses Run on
// this platform, so this stub is never reached in practice — it exists
// only so the package still compiles here.
func sysProcAttr(options) *syscall.SysProcAttr { return nil }

// killGroup falls back to killing the process directly outside Unix,
// since there is no process group to address as a whole. sig exists only
// for signature symmetry with the Unix build's killGroup, which terminate
// (kill_unix.go) calls with both SIGTERM and SIGKILL: os.Process.Kill is
// the only process-ending call this build can make through os/exec
// regardless of which signal the ladder asked for, and this path is never
// reached in practice anyway — checkLaunch (limits_other.go) refuses to
// start this rung at all outside Unix.
func killGroup(cmd *osexec.Cmd, _ syscall.Signal) error {
	return cmd.Process.Kill()
}
