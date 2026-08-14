//go:build unix

package subprocess

import (
	"errors"
	"os"
	osexec "os/exec"
	"syscall"
	"time"
)

// pollInterval is how often waitExited re-probes the process during the
// grace period. Short enough that the common case — the child exits
// promptly once it has been asked to — is detected quickly rather than
// riding out the whole grace period; long enough not to matter as CPU
// overhead for the rare case where the child ignores SIGTERM and this
// polls for the full duration instead.
const pollInterval = 10 * time.Millisecond

// terminate runs the kill ladder: SIGTERM to the child's whole process
// group, then up to grace for it to exit on its own, escalating to
// SIGKILL — also to the whole group — only if grace elapses first.
//
// Signalling the negative pid (killGroup, procattr_unix.go) is what
// reaches a native library's forked helper as well as the tracked process
// itself, and that matters differently for each half of the ladder. A
// helper that does not trap SIGTERM simply ignores the first signal and
// rides out the grace period, the same as it would have under the old
// direct-SIGKILL behaviour minus the wait; it dies on the SIGKILL that
// follows regardless, because SIGKILL cannot be trapped or ignored by
// anything. Sending SIGTERM to the group rather than just the tracked
// process is what gives a *cooperative* helper — one that does trap
// SIGTERM, unlike the shim's own handler process — the same chance to
// shut down cleanly that the tracked process gets; addressing only the
// leader would leave such a helper to be killed outright a grace period
// later instead, for no reason beyond which process happened to fork it.
//
// grace is measured from here, when the SIGTERM is actually sent, not
// from whatever triggered this call — a deadline or a cancelled caller
// context. That makes Policy.GracePeriod additive on top of the deadline
// rather than carved out of it: an operator who configures a six-hour
// deadline and a 30-second grace period gets six hours of run time
// followed by up to 30 more seconds for a cooperative shutdown, not a
// grace period that eats into the six hours and races whatever caused the
// deadline to fire. Carving it out instead would shrink the handler's
// effective budget by an amount that has nothing to do with the
// handler's own behaviour, purely as an artifact of how the ladder
// happens to be implemented — the opposite of what "give it 30 seconds to
// shut down after six hours" is asking for.
func terminate(cmd *osexec.Cmd, grace time.Duration) {
	if cmd.Process == nil {
		return
	}

	// Errors from both signal sends are deliberately discarded, for the
	// same reason killGroup's own doc comment gives for falling through
	// on a non-ErrProcessDone probe error: this function's caller
	// (killProcess) has no way to make Run itself fail differently based
	// on whether a signal landed, and a kill that was attempted and
	// failed is still strictly better-off than one skipped entirely —
	// classify has the process's actual wait status to fall back on
	// either way.
	_ = killGroup(cmd, syscall.SIGTERM) //nolint:errcheck // best-effort; see comment above

	if waitExited(cmd, grace) {
		// The common case: the child (or its cooperative helpers) honoured
		// SIGTERM and exited before grace ran out, so there is nothing
		// left to escalate to SIGKILL.
		return
	}

	_ = killGroup(cmd, syscall.SIGKILL) //nolint:errcheck // best-effort; see comment above
}

// waitExited reports whether the tracked process has exited by the time
// grace elapses.
//
// It cannot use cmd.Wait() to find out: Run's own dedicated goroutine
// already owns the one call to Wait() this *osexec.Cmd will ever get, and
// os/exec panics if Wait is invoked a second time. Polling
// cmd.Process.Signal(syscall.Signal(0)) instead is safe to call
// concurrently with a Wait() running on another goroutine — it is the
// same liveness probe killGroup's own guard uses — and once that other
// goroutine's Wait() actually reaps the process, Go's os.Process marks
// itself done internally, so this starts reporting os.ErrProcessDone
// immediately on the next poll, with no syscall involved, rather than
// only once some fixed interval has passed. That is what lets the common
// case (the process exits soon after SIGTERM) return in roughly one
// pollInterval instead of always waiting out the full grace period.
func waitExited(cmd *osexec.Cmd, grace time.Duration) bool {
	deadline := time.Now().Add(grace)

	for {
		if err := cmd.Process.Signal(syscall.Signal(0)); err != nil && errors.Is(err, os.ErrProcessDone) {
			return true
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}

		if remaining < pollInterval {
			time.Sleep(remaining)
		} else {
			time.Sleep(pollInterval)
		}
	}
}
