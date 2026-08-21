//go:build unix

package subprocess

import (
	"errors"
	osexec "os/exec"
	"syscall"
	"time"
)

// pollInterval is how often waitGroupEmpty re-probes the process group
// during the grace period. Short enough that the common case — the group
// empties out promptly once it has been asked to — is detected quickly
// rather than riding out the whole grace period; long enough not to
// matter as CPU overhead for the case where something in the group
// ignores SIGTERM and this polls for the full duration instead.
const pollInterval = 10 * time.Millisecond

// terminate runs the kill ladder: SIGTERM to the child's whole process
// group, then up to grace for the group to empty out on its own,
// escalating to SIGKILL — again to the whole group — only if it has not
// by the time grace elapses.
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
//
// The escalation decision below is made about the *group*, not the
// leader — this is the fix for a real bug an earlier version of this
// function had. Setpgid without an explicit Pgid makes the tracked
// process its own group leader, so its pid also names the group; pgid is
// captured once here, up front, because — unlike the leader's own pid,
// which killGroup's probe treats as unsafe to reuse the instant the
// leader is reaped — a pgid stays valid, and safe to keep addressing,
// for as long as *any* member of the group remains alive. The production
// shape this rung exists for is a handler that forks a native helper
// (an OpenCASCADE-style worker, say) and then itself exits cleanly on
// SIGTERM: the leader is gone almost immediately, well inside grace,
// while the helper it left behind is not. A version of this function
// that asked only "has the leader exited" would read that as "done,
// nothing left to escalate to SIGKILL" — and it did, silently leaving
// the helper running for as long as it liked. Asking whether the *group*
// is empty instead keeps waiting exactly as long as anything is still in
// it, leader or not, which is what makes the final SIGKILL below
// actually fire for this case rather than being skipped as though there
// were nothing left to reach.
func terminate(cmd *osexec.Cmd, grace time.Duration) {
	if cmd.Process == nil {
		return
	}

	// Setpgid without an explicit Pgid (sysProcAttr, procattr_unix.go)
	// makes the tracked process its own group leader, so its pid is also
	// the group's pgid at the moment this call begins — captured once,
	// before either signal, so the rest of this function keeps addressing
	// the same group even once the leader itself has been reaped.
	pgid := cmd.Process.Pid

	// Errors from both signal sends are deliberately discarded, for the
	// same reason killGroup's own doc comment gives for falling through
	// on a non-ErrProcessDone probe error: this function's caller
	// (killProcess) has no way to make Run itself fail differently based
	// on whether a signal landed, and a kill that was attempted and
	// failed is still strictly better-off than one skipped entirely —
	// classify has the process's actual wait status to fall back on
	// either way.
	_ = killGroup(cmd, syscall.SIGTERM) //nolint:errcheck // best-effort; see comment above

	if waitGroupEmpty(pgid, grace) {
		// The common case: every process in the group — the tracked
		// leader and anything cooperative it forked — honoured SIGTERM
		// and exited before grace ran out, so there is nothing left to
		// escalate to SIGKILL.
		return
	}

	// Signalling raw here, rather than through killGroup, is deliberate
	// and is the other half of this function's fix: killGroup's own probe
	// is keyed to cmd.Process specifically and skips the send outright
	// once that process is reaped, which is exactly wrong for this call —
	// the tracked leader having already exited is the expected shape of
	// the bug this escalation exists to catch, not a reason to skip it.
	// waitGroupEmpty having just reported the group as non-empty stands in
	// for that probe instead: the kernel does not hand a pgid back out
	// for reuse while any process is still using it as its group, so a
	// non-ESRCH result from that same kind of check moments ago means
	// pgid was, at that moment, still this attempt's own group and not a
	// number the kernel had already recycled. That does not close the
	// window entirely — a last member could exit in the interval between
	// that check and this send, freeing the pgid for reuse before the
	// signal lands — the same kind of gap killGroup's own doc comment
	// already accepts for the single-process case, narrowed here to the
	// width of one syscall rather than removed.
	_ = syscall.Kill(-pgid, syscall.SIGKILL) //nolint:errcheck // best-effort; see comment above
}

// waitGroupEmpty reports whether every process in the group named by
// pgid has exited by the time grace elapses.
//
// It probes the group directly — syscall.Kill(-pgid, 0) — rather than
// asking only whether the tracked leader is still alive. That distinction
// is the point: syscall.Kill with a negative pid succeeds as long as the
// caller has permission to signal at least one member of the group, and
// only fails with ESRCH once none are left, so this notices a helper the
// leader forked and left running exactly the same way it would notice an
// uncooperative leader — checking the leader alone would report the
// group "empty" the moment a *cooperative* leader exits, even while
// something it forked is still very much alive.
//
// This cannot use cmd.Wait() to find out when the leader specifically has
// gone: Run's own dedicated goroutine already owns the one call to Wait()
// this *osexec.Cmd will ever get, and os/exec panics if Wait is invoked a
// second time. Polling the group directly sidesteps that entirely, since
// it never touches cmd.Process at all.
func waitGroupEmpty(pgid int, grace time.Duration) bool {
	deadline := time.Now().Add(grace)

	for {
		if err := syscall.Kill(-pgid, 0); errors.Is(err, syscall.ESRCH) {
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
