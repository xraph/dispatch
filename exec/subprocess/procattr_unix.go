//go:build unix

package subprocess

import (
	"errors"
	"os"
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
//
// The probe before the kill exists because a raw syscall.Kill(-pid, ...)
// has no idea whether pid still names the process this package started.
// os.Process.wait marks the process done (doRelease(statusDone)) and takes
// a write lock on its own sigMu *before* it calls wait4 — the syscall that
// actually lets the kernel hand the pid to someone else — specifically so
// that Process.Signal, which read-locks the same sigMu, cannot land on a
// reused pid once that has happened. Routing through cmd.Process.Signal
// first reuses that same fencing instead of re-deriving it, which
// syscall.Kill alone has none of. It does not close the window entirely —
// there is still a gap between this check succeeding and the raw
// syscall.Kill call below — but it narrows it from "however long the
// waitLoop select takes to notice the process exited" down to a couple of
// Go statements, and the waitLoop fix above (checking waitCh before
// setting timedOut) removes the specific interleaving that used to make
// that gap wide enough to matter in practice.
func killGroup(cmd *osexec.Cmd) error {
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}

		return err
	}

	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
