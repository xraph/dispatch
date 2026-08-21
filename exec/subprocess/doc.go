// Package subprocess runs job handlers in a re-exec'd child process.
//
// The worker launches its own binary again with argv[1] set to
// shim.ArgName, so the child ends up running shim.Main with the same
// handler registry the worker itself has, by construction — there is no
// second binary to build, ship, or keep in sync. The request crosses to
// the child on fd 3 and the result crosses back on fd 4, using the
// wire package's length-prefixed frames; stdout and stderr are left free
// for the handler's own output and for whatever a native library writes,
// and are streamed to the configured logger instead.
//
// This is Dispatch's exec.LevelProcess rung: a crash, a panic, or a
// memory-unsafe parser going off the rails takes down the child, not the
// worker, and the child does not receive the worker's environment
// wholesale — buildEnv constructs it from the request's own Env plus a
// small fixed allowlist (PATH, HOME, TMPDIR) copied from the worker's,
// never a plain pass-through of os.Environ() — so it cannot read a
// credential that lived only in a worker environment variable outside
// that allowlist. It is not a sandbox in the mount/network/seccomp sense
// — that is exec.LevelSandboxed, a stronger rung built on the same wire
// protocol.
//
// # The uid/gid boundary
//
// WithUser configures a dedicated, low-privilege uid/gid for the child.
// It is required, not advisory: without one, the child would run as the
// worker's own uid and could read anything the worker can — the Dispatch
// config file on disk among it, which is where database credentials
// typically live, not just the worker's environment — so Run refuses to
// start at all unless WithAllowSameUser opts into that explicitly. See
// WithUser and WithAllowSameUser.
//
// A genuine uid drop — WithUser naming anything other than the worker's
// own uid — clears supplementary groups along with it: dropping to a
// different uid or gid already requires the same privilege setgroups(2)
// itself needs (CAP_SETUID/CAP_SETGID on Linux, root on Darwin), so
// os/exec's own "clear supplementary groups whenever Credential is set
// and NoSetGroups is false" behaviour applies in full (see sysProcAttr,
// procattr_unix.go). A worker running as root with, say, "docker" in its
// supplementary groups (a common shape for a systemd unit that also
// manages containers) does not hand that membership to a genuinely
// dropped child — the child keeps only the low-privilege uid/gid's own
// groups.
//
// The one case where supplementary groups are NOT cleared is
// WithAllowSameUser's same-uid, same-gid path, where Credential is
// otherwise a no-op: NoSetGroups is set there specifically because
// setgroups needs privilege this process does not have when it is not
// already root (every dev machine and CI run), and there is nothing to
// clear anyway, since the child is running as the worker's own account.
// That path already defeats most of this rung's purpose for the uid/gid
// boundary itself — see WithUser and WithAllowSameUser — so its
// supplementary-group behaviour is the smaller of the two problems.
package subprocess
