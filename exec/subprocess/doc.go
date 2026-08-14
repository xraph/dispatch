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
// worker, and the child never receives the worker's environment, so it
// cannot read credentials it was never handed. It is not a sandbox in the
// mount/network/seccomp sense — that is exec.LevelSandboxed, a stronger
// rung built on the same wire protocol.
//
// # The uid/gid boundary
//
// WithUser configures a dedicated, low-privilege uid/gid for the child;
// without one, the child runs as the worker's own uid and can read
// anything the worker can, which defeats most of this rung's purpose —
// see WithUser and WithAllowSameUser.
//
// That boundary covers the primary uid and gid only. It does not touch
// supplementary group membership: the child keeps every supplementary
// group the worker's own OS account belongs to. A worker running as
// root with, say, "docker" in its supplementary groups (a common shape
// for a systemd unit that also manages containers) hands that same
// group membership to every child this package launches, dropped uid
// notwithstanding — including group-write access to a group-owned
// socket like /var/run/docker.sock, which is root on the host. Deployments
// where supplementary groups grant access worth withholding need to
// account for that outside this package — for example, by not putting
// the worker's own account in privileged groups in the first place.
package subprocess
