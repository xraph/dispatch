//go:build unix && !openbsd

package shim

import "syscall"

// rlimitAS reports the RLIMIT_AS resource number and whether it is
// usable on this platform. Everywhere this file builds, that is simply
// syscall.RLIMIT_AS: Go resolves the named constant to the correct
// number for the platform and architecture being compiled for, which is
// verified true for every unix platform except one — see
// rlimit_as_openbsd.go, the build-tag complement of this file, for why
// OpenBSD needs its own implementation instead of this one.
func rlimitAS() (resource int, ok bool) {
	return syscall.RLIMIT_AS, true
}
