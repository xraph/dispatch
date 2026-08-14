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
// real work continues in the background.
//
// When a user is configured, this also sets Credential so the child drops
// to that uid/gid before exec. checkLaunch (limits_unix.go) is what
// refuses to reach here at all when the configured uid matches the
// worker's own without WithAllowSameUser; sysProcAttr itself does not
// re-derive that policy, it just builds the attribute struct.
//
// Credential.NoSetGroups is true only when both uid and gid already equal
// the caller's own — the WithAllowSameUser dev/CI path, where the
// Credential is otherwise a no-op. That is the only case where it is
// needed: os/exec calls setgroups(2) to clear supplementary groups
// whenever NoSetGroups is false and Credential.Groups is nil (see
// exec_linux.go and exec_libc2.go's shared "if !cred.NoSetGroups"
// guard), and setgroups requires privilege (CAP_SETGID on Linux, root on
// Darwin) independent of whether Uid/Gid actually change — so without
// this exception, even WithAllowSameUser's same-uid case would fail to
// launch whenever the worker itself is not root, which is every dev
// machine and every CI run here.
//
// Whenever uid or gid genuinely differ from the caller's own, this is
// false, so setgroups does run and the child's supplementary groups are
// actually cleared rather than silently inherited. That launch already
// requires the same privilege setgroups does (only root/CAP_SETUID can
// change to a different uid at all), so this does not introduce a new
// privilege requirement — it only skips the clear in the one case where
// the process doing the dropping has no such privilege to begin with,
// and dropping is a no-op anyway. Getting this backwards is a real
// containment gap, not a cosmetic one: a worker running as root with
// supplementary group "docker" (common for a systemd unit that also
// manages containers) would otherwise hand every "sandboxed" child that
// same group membership — and therefore group-write access to
// /var/run/docker.sock, i.e. root on the host — regardless of the uid it
// was dropped to.
func sysProcAttr(o options) *syscall.SysProcAttr {
	attr := &syscall.SysProcAttr{Setpgid: true}
	if o.hasUser {
		attr.Credential = &syscall.Credential{
			Uid:         uint32(o.uid), //nolint:gosec // G115: operator-configured via WithUser, never attacker input.
			Gid:         uint32(o.gid), //nolint:gosec // G115: operator-configured via WithUser, never attacker input.
			NoSetGroups: o.uid == os.Getuid() && o.gid == os.Getgid(),
		}
	}

	return attr
}

// killGroup sends sig to the child's whole process group rather than just
// the child itself. Setpgid without an explicit Pgid makes the child its
// own group leader, so its pid doubles as its pgid, and signalling the
// negative pid — syscall.Kill(-pid, sig) — is how POSIX addresses a group
// rather than one process. Whatever signal is sent reaches every
// descendant the tracked process forked, not only the process this
// package started directly.
//
// It takes the signal as a parameter, rather than being a SIGKILL-only
// function, so that the probe below — the part that is actually
// delicate — has one general-purpose home instead of being copied for
// each signal that might need it. terminate (kill_unix.go) calls this
// for the ladder's SIGTERM leg, sent right when a decision to terminate
// has just been made, which is exactly the situation this probe is
// suited to: the tracked process is overwhelmingly likely to still be
// the one this package started, and the only race worth guarding against
// is the few-Go-statements gap the doc comment below describes.
//
// terminate's SIGKILL leg deliberately does *not* come through here —
// see its own doc comment for why: by the time grace has elapsed, the
// tracked leader having already exited is the expected shape of the
// exact bug that escalation exists to catch, not a rare race, so gating
// that signal on cmd.Process specifically being still alive would silence
// it in precisely the case it is needed. This function's probe is
// correct for a signal sent at the *start* of a kill; it is the wrong
// tool for one decided after a wait, which is what changed between "this
// function used to be terminate's only signal path" and now.
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
//
// Two trades this makes, both deliberate:
//
// A non-ErrProcessDone probe error falls through to attempt the group
// signal anyway rather than returning it. waitLoop has no way to make
// progress other than waitCh eventually firing, so a killGroup that gives
// up here would not fail the attempt — it would hang Run indefinitely
// instead, waiting on a kill that was never sent to a process that is
// never going to exit on its own. That is strictly worse than attempting
// a signal that might itself fail: signalling this package's own child
// used to have no path that produces a non-ErrProcessDone error here, but
// sysProcAttr above now sets Credential with a dedicated uid when one is
// configured, and that uid boundary makes EPERM a real possibility. A
// failed signal is recoverable — classify still has the process's actual
// wait status to report from, whatever it turns out to be; a signal that
// was never attempted is not.
//
// An ErrProcessDone probe result returns immediately, without ever
// reaching the group signal below. For the SIGTERM call this function is
// actually used for today, that is a narrow, accepted gap: a leader
// reaped in the couple of Go statements between waitLoop's own check and
// this probe landing leaves any surviving grandchildren un-signalled by
// *this* call, where an unconditional syscall.Kill(-pid, ...) (pid ==
// pgid here) would still have reached them, since a pgid stays valid as
// long as any member of the group is still alive, leader or not.
// Signalling anyway in that case was considered and rejected: it would
// mean signalling a pgid derived from a pid the kernel may already have
// handed to an unrelated process group, which is the exact hazard this
// probe exists to avoid — reaching a stray grandchild on the SIGTERM leg
// is not worth reintroducing that, because it is not the last word:
// terminate's SIGKILL escalation (kill_unix.go) does not route through
// this function at all, specifically so that a leader reaped by the time
// grace elapses — the expected shape of a cooperative exit, not a rare
// race — cannot make the *final* signal a no-op the same way. This
// narrows an already-partial guarantee rather than removing a complete
// one: a grandchild left behind by a tracked process that exited cleanly
// on its own, before killProcess was ever called at all, was already
// unreachable by this function (see the
// drainGrace comment in Run).
func killGroup(cmd *osexec.Cmd, sig syscall.Signal) error {
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil && errors.Is(err, os.ErrProcessDone) {
		return nil
	}

	return syscall.Kill(-cmd.Process.Pid, sig)
}
