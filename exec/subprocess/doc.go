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
package subprocess
