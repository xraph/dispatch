//go:build unix

package subprocess

import (
	osexec "os/exec"
	"syscall"
)

// sysProcAttr puts the child in its own process group. The kill ladder
// (Task 6) needs this to reach a native library's forked helpers, and
// killGroup below needs it right now: without it, killing the tracked
// process leaves anything it spawned running, which is the classic silent
// failure of this design — the deadline appears to have worked while the
// real work continues in the background. Task 5 extends this same
// SysProcAttr with a Credential for the dedicated low-privilege uid; this
// task only sets the process group.
func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// killGroup sends SIGKILL to the child's whole process group rather than
// just the child itself. Setpgid without an explicit Pgid makes the child
// its own group leader, so its pid doubles as its pgid, and signalling the
// negative pid — syscall.Kill(-pid, sig) — is how POSIX addresses a group
// rather than one process. This is a direct kill, not the graceful
// SIGTERM-then-grace-period-then-SIGKILL ladder; that sequencing is
// Task 6's job. What matters here is that whichever signal is sent reaches
// every descendant the tracked process forked, not only the process this
// package started directly.
func killGroup(cmd *osexec.Cmd) error {
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
