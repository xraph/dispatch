//go:build unix && !freebsd && !dragonfly

package shim

import "syscall"

// newRlimit builds a syscall.Rlimit with both Cur and Max set to n.
// Everywhere this file builds, Rlimit's fields are uint64 (verified
// against Go's generated ztypes_*.go for aix, darwin, linux, netbsd,
// openbsd, and solaris/illumos) — see rlimit_value_bsd64.go for the two
// platforms where that is not true.
func newRlimit(n int64) *syscall.Rlimit {
	//nolint:gosec // G115: n is validated non-negative by applyRlimits before this is called.
	return &syscall.Rlimit{Cur: uint64(n), Max: uint64(n)}
}
