//go:build freebsd || dragonfly

package shim

import "syscall"

// newRlimit builds a syscall.Rlimit with both Cur and Max set to n.
// FreeBSD and Dragonfly are the two platforms under the "unix" build
// constraint whose generated syscall.Rlimit uses int64 fields rather
// than uint64 — everywhere else newRlimit needs the uint64 conversion
// this file's build-tag complement (rlimit_value_unix.go) provides.
func newRlimit(n int64) *syscall.Rlimit {
	return &syscall.Rlimit{Cur: n, Max: n}
}
